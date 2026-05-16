#!/usr/bin/env bash
# translate_loop.sh — unattended per-file translator loop for redis-rs-port.
#
# Adapted from lua-rs-port/harness/implement_loop.sh, restructured for
# Phase A translation rather than Phase F debug. Each iteration:
#
#   1. Pick the next file from the queue (file-deps.tsv, phase-then-LoC order)
#   2. Invoke chassis fanout for that one file
#   3. Parse pilot.jsonl for outcome
#   4. Handle outcome (retry on API 500, skip on hook fail, etc.)
#   5. After every CHECK_EVERY successful translations: cargo check --workspace
#      (if broken: revert the offending commit, mark file as failed)
#   6. Loop
#
# Stop conditions:
#   - Cost cap (LOOP_COST_CAP)
#   - Max iterations (MAX_ITER)
#   - Queue empty (all in-scope files done)
#   - STUCK_LIMIT consecutive failures
#
# Safety:
#   - Each iteration is one fanout call → one auto-commit (chassis hook chain).
#   - Failed iterations don't leave uncommitted state (we git stash/reset).
#   - cargo check --workspace verifies the running impl after each batch;
#     broken state triggers a revert of the most recent agent commit.
#
# Usage:
#   nohup ./harness/translate_loop.sh > /tmp/translate_loop.log 2>&1 &
#
# Tuning via env:
#   MAX_ITER=50         number of files to attempt (default 30)
#   LOOP_COST_CAP=80    USD cap across all iterations (default 50)
#   CHECK_EVERY=3       run cargo check --workspace every N translations (default 3)
#   API_RETRY_LIMIT=3   times to retry on API 500 (default 3)
#   PHASES="pilot later"  which phases to process (default both)

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT_DIR="harness/loop"
mkdir -p "$OUT_DIR"
STATE="$OUT_DIR/state.jsonl"
LOG="$OUT_DIR/loop.log"
QUEUE="$OUT_DIR/queue.txt"
DONE="$OUT_DIR/done.txt"
FAILED="$OUT_DIR/failed.txt"
ARCHITECT_NEEDED="$OUT_DIR/needs_architect.txt"
touch "$STATE" "$LOG" "$DONE" "$FAILED" "$ARCHITECT_NEEDED"

MAX_ITER=${MAX_ITER:-30}
LOOP_COST_CAP=${LOOP_COST_CAP:-50.00}
CHECK_EVERY=${CHECK_EVERY:-3}
API_RETRY_LIMIT=${API_RETRY_LIMIT:-3}
STUCK_LIMIT=${STUCK_LIMIT:-5}
PHASES=${PHASES:-"pilot later"}

CHASSIS="$(cd "$ROOT/../port-harness" && pwd)"
PILOT_JSONL="$ROOT/harness/oracle/results/pilot.jsonl"

TOTAL_COST="0.00"
CONSEC_FAIL=0
SINCE_CHECK=0

emit() {
    local ts; ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[$ts] $*" | tee -a "$LOG"
}

record() {
    local action="$1" detail="${2:-}"
    local ts; ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    jq -c -n --arg ts "$ts" --arg action "$action" --arg detail "$detail" \
        --argjson cost "$TOTAL_COST" \
        '{ts: $ts, action: $action, detail: $detail, total_cost: $cost}' >> "$STATE"
}

# Build the initial queue: every .c file in the requested phases that
# (a) has a non-SKIP target and (b) isn't already a real port.
build_queue() {
    local phases_pattern; phases_pattern=$(echo "$PHASES" | tr ' ' '|')
    : > "$QUEUE"
    while IFS=$'\t' read -r cfile crate rust phase; do
        case "$cfile" in ''|'#'*) continue ;; esac
        [[ "$cfile" == *.c ]] || continue
        [[ "$crate" == "SKIP" ]] && continue
        [[ "$phase" =~ ^(${phases_pattern})$ ]] || continue
        local rust_full="crates/$crate/$rust"
        # Skip if real-ported already (trailer source references a .c file
        # and isn't the "(none — scaffolding placeholder)" marker)
        if [ -f "$rust_full" ] \
            && grep -qE '^//\s*source:.*\.[ch]\b' "$rust_full" \
            && ! grep -qE '^//\s*source:.*\(none' "$rust_full"; then
            continue
        fi
        # Compute LoC for sort (smaller files first → quick wins, validate flow)
        local loc=0
        [ -f "reference/valkey/src/$cfile" ] && loc=$(wc -l < "reference/valkey/src/$cfile" | tr -d ' ')
        # Already-failed entries get demoted; skip them on subsequent runs.
        grep -qx "$cfile" "$FAILED" 2>/dev/null && continue
        # Phase rank: pilot=0, later=1, defer=2
        local phase_rank=9
        case "$phase" in pilot) phase_rank=0 ;; later) phase_rank=1 ;; defer) phase_rank=2 ;; esac
        printf '%d\t%05d\t%s\n' "$phase_rank" "$loc" "$cfile"
    done < "harness/file-deps.tsv" | sort -k1,1n -k2,2n | awk '{print $3}' > "$QUEUE"
    emit "queue built: $(wc -l < "$QUEUE" | tr -d ' ') files"
}

# Run one translator iteration on the given .c file. Returns 0 on success,
# 1 on failure (caller decides whether to retry or skip).
run_one() {
    local cfile="$1"
    local before_cost="$TOTAL_COST"

    # Ensure clean tree before each iteration (so we can detect this iter's diff)
    if [ -n "$(git status --porcelain)" ]; then
        emit "  WARN: uncommitted state going into iter; stashing"
        git stash push -u -m "translate_loop: pre-iter stash $(date -u +%FT%T)" >/dev/null 2>&1 || true
    fi

    "$CHASSIS/fanout.sh" --files "$cfile" >>"$LOG" 2>&1
    local rc=$?

    # Extract this iter's row from pilot.jsonl (last matching line)
    local row; row=$(grep "\"file\":\"$cfile\"" "$PILOT_JSONL" | tail -1)
    if [ -z "$row" ]; then
        emit "  no row in pilot.jsonl for $cfile (rc=$rc)"
        return 1
    fi

    local status cost duration hooks_pass syntax_ok target
    status=$(echo "$row" | jq -r '.status')
    cost=$(echo "$row" | jq -r '.cost_usd // 0')
    duration=$(echo "$row" | jq -r '.duration_s // 0')
    hooks_pass=$(echo "$row" | jq -r '.hooks_pass // false')
    syntax_ok=$(echo "$row" | jq -r '.syntax_ok // false')
    target=$(echo "$row" | jq -r '.target // ""')

    TOTAL_COST=$(awk -v a="$TOTAL_COST" -v b="$cost" 'BEGIN { printf "%.4f", a + b }')
    emit "  result: status=$status cost=\$$cost duration=${duration}s hooks=$hooks_pass syntax=$syntax_ok"

    case "$status" in
        ok)
            # Check for TODO(architect) markers — translator may have flagged a blocker
            if [ -f "$target" ] && grep -q 'TODO(architect)' "$target"; then
                local n_arch; n_arch=$(grep -c 'TODO(architect)' "$target")
                emit "  ⚠ TODO(architect) markers in output ($n_arch) — file lands but needs architect review"
                echo "$cfile" >> "$ARCHITECT_NEEDED"
                record "ok_with_architect_todo" "file=$cfile target=$target n=$n_arch"
            else
                record "ok" "file=$cfile target=$target cost=$cost"
            fi
            echo "$cfile" >> "$DONE"
            return 0
            ;;
        already_ported)
            emit "  already-ported (skipping)"
            echo "$cfile" >> "$DONE"
            record "already_ported" "file=$cfile"
            return 0
            ;;
        no_output|syntax_failed|hooks_failed)
            # Check whether it was an API 500 (transient)
            local basename="${cfile%.*}"
            local out_json="harness/oracle/results/$basename.translator.json"
            if [ -f "$out_json" ] && jq -e '.api_error_status == 500' "$out_json" >/dev/null 2>&1; then
                emit "  API 500 detected"
                record "api_500" "file=$cfile"
                return 2  # special: caller retries
            fi
            emit "  failure mode: $status (hooks=$hooks_pass syntax=$syntax_ok)"
            record "fail" "file=$cfile status=$status hooks=$hooks_pass syntax=$syntax_ok cost=$cost"
            echo "$cfile" >> "$FAILED"
            return 1
            ;;
        no_mapping)
            emit "  no_mapping (data error)"
            record "no_mapping" "file=$cfile"
            echo "$cfile" >> "$FAILED"
            return 1
            ;;
        *)
            emit "  unknown status: $status"
            record "unknown_status" "file=$cfile status=$status"
            echo "$cfile" >> "$FAILED"
            return 1
            ;;
    esac
}

# Run cargo check --workspace; if broken, revert to last green commit
# and log. Returns 0 if green, 1 if broken-and-reverted.
check_workspace() {
    if cargo check --workspace 2>"$OUT_DIR/check.err" >/dev/null; then
        emit "  cargo check --workspace: clean ✓"
        return 0
    fi
    emit "  cargo check --workspace: BROKEN — last commits caused regression"
    # Look at the most recent agent commit; revert it.
    local last_agent_commit; last_agent_commit=$(git log --format='%H %s' -20 | grep -m1 'agent: auto-commit' | awk '{print $1}')
    if [ -n "$last_agent_commit" ]; then
        emit "  reverting commit $last_agent_commit"
        git revert --no-edit "$last_agent_commit" >>"$LOG" 2>&1 || {
            emit "  revert failed; resetting to HEAD~1"
            git reset --hard HEAD~1 >>"$LOG" 2>&1
        }
        record "workspace_broken_reverted" "commit=$last_agent_commit"
    else
        emit "  could not identify the offending commit"
        record "workspace_broken_no_revert" ""
    fi
    return 1
}

# ──────────────────────────────────────────────────────────────────────
# Pre-flight
# ──────────────────────────────────────────────────────────────────────

emit "═════════════════════════════════════════════════════════════════"
emit "redis-rs-port translate_loop start"
emit "  MAX_ITER=$MAX_ITER  LOOP_COST_CAP=\$$LOOP_COST_CAP  PHASES=$PHASES"
emit "  CHECK_EVERY=$CHECK_EVERY  STUCK_LIMIT=$STUCK_LIMIT"
emit "═════════════════════════════════════════════════════════════════"
record "run_start" "phases=$PHASES max_iter=$MAX_ITER cap=$LOOP_COST_CAP"

build_queue

if [ ! -s "$QUEUE" ]; then
    emit "queue empty — nothing to do (all in-scope files done or marked failed)"
    record "empty_queue" ""
    exit 0
fi

# Verify chassis fanout is reachable
if [ ! -x "$CHASSIS/fanout.sh" ]; then
    emit "FATAL: chassis fanout not found at $CHASSIS/fanout.sh"
    record "fatal" "chassis_missing"
    exit 2
fi

# Verify cargo check is currently clean before we start
if ! cargo check --workspace >/dev/null 2>&1; then
    emit "FATAL: cargo check --workspace already broken before loop start — fix first"
    record "fatal" "preexisting_workspace_break"
    exit 2
fi

emit "pre-flight green; processing $(wc -l < "$QUEUE" | tr -d ' ') files"

# ──────────────────────────────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────────────────────────────

ITER=0
while [ "$ITER" -lt "$MAX_ITER" ]; do
    ITER=$((ITER + 1))

    # Cost gate
    if awk -v t="$TOTAL_COST" -v cap="$LOOP_COST_CAP" 'BEGIN { exit !(t >= cap) }'; then
        emit "cost cap \$$LOOP_COST_CAP reached (\$$TOTAL_COST) — stopping"
        record "cost_cap" "spent=$TOTAL_COST"
        break
    fi

    # Pop head of queue
    cfile=$(head -1 "$QUEUE")
    if [ -z "$cfile" ]; then
        emit "queue exhausted — all files processed"
        record "queue_empty" "iter=$ITER"
        break
    fi
    sed -i '' '1d' "$QUEUE" 2>/dev/null || sed -i '1d' "$QUEUE" 2>/dev/null

    emit "─── iter $ITER  ($(wc -l < "$QUEUE" | tr -d ' ') remaining)  spent: \$$TOTAL_COST ───"
    emit "  file: $cfile"

    # Run, with API-500 retry
    attempt=0
    while [ "$attempt" -lt "$API_RETRY_LIMIT" ]; do
        attempt=$((attempt + 1))
        run_one "$cfile"
        rc=$?
        if [ "$rc" -ne 2 ]; then
            break  # not an API 500 — done with this file
        fi
        if [ "$attempt" -lt "$API_RETRY_LIMIT" ]; then
            backoff=$((60 * attempt))
            emit "  API 500 (attempt $attempt/$API_RETRY_LIMIT) — waiting ${backoff}s"
            sleep "$backoff"
        else
            emit "  API 500 ${API_RETRY_LIMIT}x in a row — giving up on $cfile"
            record "api_500_exhausted" "file=$cfile"
            echo "$cfile" >> "$FAILED"
            rc=1
        fi
    done

    if [ "$rc" -eq 0 ]; then
        CONSEC_FAIL=0
        SINCE_CHECK=$((SINCE_CHECK + 1))
        if [ "$SINCE_CHECK" -ge "$CHECK_EVERY" ]; then
            SINCE_CHECK=0
            check_workspace || true  # don't bail — the revert handles it
        fi
    else
        CONSEC_FAIL=$((CONSEC_FAIL + 1))
        if [ "$CONSEC_FAIL" -ge "$STUCK_LIMIT" ]; then
            emit "$STUCK_LIMIT consecutive failures — stopping"
            record "stuck" "consec_fail=$CONSEC_FAIL"
            break
        fi
    fi
done

emit "═════════════════════════════════════════════════════════════════"
emit "Loop end. iters=$ITER  total_cost=\$$TOTAL_COST"
emit "  done:             $(wc -l < "$DONE" | tr -d ' ') files"
emit "  failed:           $(wc -l < "$FAILED" | tr -d ' ') files"
emit "  needs_architect:  $(wc -l < "$ARCHITECT_NEEDED" | tr -d ' ') files"
emit "  queue remaining:  $(wc -l < "$QUEUE" | tr -d ' ') files"
emit "═════════════════════════════════════════════════════════════════"
record "run_end" "iters=$ITER total_cost=$TOTAL_COST done=$(wc -l < "$DONE" | tr -d ' ') failed=$(wc -l < "$FAILED" | tr -d ' ')"

# Final cargo check report
if cargo check --workspace >/dev/null 2>&1; then
    emit "final cargo check --workspace: clean ✓"
else
    emit "final cargo check --workspace: BROKEN (see harness/loop/check.err for last error)"
fi

exit 0
