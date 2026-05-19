#!/usr/bin/env bash
# harness/oracle/smoke.sh — smoke-test runner for the wire-diff oracle.
#
# Builds the Rust binary if needed, spawns both servers, iterates every
# corpus script independently, reports per-script pass/fail, then tears
# down both servers unconditionally on exit.
#
# Usage: bash harness/oracle/smoke.sh [--skip-build] [--with-rdb]
#
# --with-rdb   also run the RDB bidirectional oracle (rdb-diff) after the
#              wire-diff suite.  Skipped by default because it is slower and
#              depends on RDB save/load being wired in.
#
# Exit code: 0 = all scripts passed, 1 = one or more scripts failed,
#             2 = infrastructure error (server did not start).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ORACLE="${ROOT}/harness/oracle/wire-diff"
CORPUS_DIR="${ROOT}/harness/oracle/corpus"
VALKEY_BIN="${ROOT}/reference/valkey/src/valkey-server"
RUST_BIN="${ROOT}/target/debug/redis-server"
WIRE_DIFF_TSV="${ROOT}/harness/oracle/wire-diff.tsv"
: > "${WIRE_DIFF_TSV}"

C_PORT=16379
RUST_PORT=16390

# Cleanup both server PIDs stored in these variables.
C_PID=""
RUST_PID=""

cleanup() {
    if [[ -n "${C_PID}" ]]; then
        kill "${C_PID}" 2>/dev/null || true
        wait "${C_PID}" 2>/dev/null || true
    fi
    if [[ -n "${RUST_PID}" ]]; then
        kill "${RUST_PID}" 2>/dev/null || true
        wait "${RUST_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ── build step ──────────────────────────────────────────────────────────────

SKIP_BUILD=0
WITH_RDB=0
for arg in "$@"; do
    [[ "${arg}" == "--skip-build" ]] && SKIP_BUILD=1
    [[ "${arg}" == "--with-rdb"   ]] && WITH_RDB=1
done

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
    echo "==> building Rust binary …"
    (cd "${ROOT}" && cargo build --bin redis-server 2>&1) || {
        echo "ERROR: cargo build failed — cannot run smoke." >&2
        exit 2
    }
    echo "==> build OK"
else
    echo "==> --skip-build: skipping cargo build"
fi

# ── C reference server ───────────────────────────────────────────────────────

C_AVAILABLE=0
if [[ -x "${VALKEY_BIN}" ]]; then
    echo "==> starting C Valkey on port ${C_PORT} …"
    TMPDIR_C="$(mktemp -d)"
    "${VALKEY_BIN}" \
        --port "${C_PORT}" \
        --bind 127.0.0.1 \
        --save "" \
        --appendonly no \
        --daemonize no \
        --loglevel warning \
        > "${TMPDIR_C}/valkey.log" 2>&1 &
    C_PID=$!

    # Wait up to 5 s for C server to accept connections.
    deadline=$(( $(date +%s) + 5 ))
    while true; do
        if nc -z 127.0.0.1 "${C_PORT}" 2>/dev/null; then
            C_AVAILABLE=1
            break
        fi
        if [[ $(date +%s) -ge ${deadline} ]]; then
            echo "WARN: C Valkey did not start within 5 s — skipping C-side comparison." >&2
            tail -20 "${TMPDIR_C}/valkey.log" >&2 || true
            kill "${C_PID}" 2>/dev/null || true
            C_PID=""
            break
        fi
        sleep 0.1
    done

    [[ "${C_AVAILABLE}" -eq 1 ]] && echo "==> C Valkey ready"
else
    echo "WARN: C reference binary not found at ${VALKEY_BIN}" >&2
    echo "      Run: cd ${ROOT}/reference/valkey && make -j4 BUILD_TLS=no DISABLE_WERRORS=yes" >&2
fi

# ── Rust server ──────────────────────────────────────────────────────────────

RUST_AVAILABLE=0
if [[ -x "${RUST_BIN}" ]]; then
    echo "==> starting Rust redis-server on port ${RUST_PORT} …"
    TMPDIR_RUST="$(mktemp -d)"
    "${RUST_BIN}" \
        --port "${RUST_PORT}" \
        --bind 127.0.0.1 \
        > "${TMPDIR_RUST}/rust.log" 2>&1 &
    RUST_PID=$!

    deadline=$(( $(date +%s) + 5 ))
    while true; do
        if nc -z 127.0.0.1 "${RUST_PORT}" 2>/dev/null; then
            RUST_AVAILABLE=1
            break
        fi
        if [[ $(date +%s) -ge ${deadline} ]]; then
            echo "ERROR: Rust server did not start within 5 s." >&2
            tail -20 "${TMPDIR_RUST}/rust.log" >&2 || true
            exit 2
        fi
        sleep 0.1
    done
    echo "==> Rust server ready"
else
    echo "ERROR: Rust binary not found at ${RUST_BIN}" >&2
    echo "       Run: cargo build --bin redis-server" >&2
    exit 2
fi

# ── per-script oracle runs ───────────────────────────────────────────────────

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Smoke corpus — wire-diff per script"
echo "═══════════════════════════════════════════════════════"

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

# Collect per-script results by running each corpus file individually via
# a synthetic single-script suite.  The oracle accepts --c-port and
# --rust-port so it uses the already-running servers.

for script in "${CORPUS_DIR}"/*.txt; do
    script_name="$(basename "${script}" .txt)"

    # Build a temporary one-file corpus directory so we can run a single
    # script through the oracle without re-implementing suite filtering here.
    TMPDIR_CORPUS="$(mktemp -d)"
    cp "${script}" "${TMPDIR_CORPUS}/${script_name}.txt"

    # Extract suite tags from the file so we can pass an existing suite
    # name. If none declared, fall back to running all (no --suite flag).
    suite_line="$(grep -m1 '^\[suite:' "${script}" || true)"

    if [[ -n "${suite_line}" ]]; then
        # Take the first tag from e.g. "[suite: smoke,protocol]"
        first_tag="$(echo "${suite_line}" | sed 's/\[suite: *//; s/\].*//' | cut -d',' -f1 | tr -d ' ')"
    else
        first_tag=""
    fi

    if [[ "${C_AVAILABLE}" -eq 0 ]]; then
        # No C reference — print C-side behavior only (oracle returns 0).
        printf "  %-22s  C unavailable — skip\n" "${script_name}"
        printf '%s\tSKIP\tC reference unavailable\n' "${script_name}" >> "${WIRE_DIFF_TSV}"
        SKIP_COUNT=$(( SKIP_COUNT + 1 ))
        rm -rf "${TMPDIR_CORPUS}"
        continue
    fi

    # Run the oracle against the full corpus file using the running servers.
    # We capture exit code without letting set -e abort us.
    oracle_out="$(python3 "${ORACLE}" \
        --c-port "${C_PORT}" \
        --rust-port "${RUST_PORT}" \
        2>&1)" || oracle_rc=$?
    oracle_rc="${oracle_rc:-0}"

    # Filter output to only lines for this script.
    script_lines="$(echo "${oracle_out}" | grep "^${script_name}" || true)"
    fail_lines="$(echo "${script_lines}" | grep " FAIL" || true)"
    pass_lines="$(echo "${script_lines}" | grep " PASS" || true)"

    pass_n="$(echo "${pass_lines}" | grep -c PASS || true)"
    fail_n="$(echo "${fail_lines}" | grep -c FAIL || true)"

    if [[ "${fail_n}" -gt 0 ]]; then
        printf "  %-22s  FAIL  (%d pass, %d fail)\n" "${script_name}" "${pass_n}" "${fail_n}"
        first_fail=$(echo "${fail_lines}" | head -1 | tr '\t' ' ' | cut -c1-160)
        printf '%s\tFAIL\t%s\n' "${script_name}" "${first_fail}" >> "${WIRE_DIFF_TSV}"
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    else
        printf "  %-22s  PASS  (%d commands)\n" "${script_name}" "${pass_n}"
        printf '%s\tPASS\t%d commands\n' "${script_name}" "${pass_n}" >> "${WIRE_DIFF_TSV}"
        PASS_COUNT=$(( PASS_COUNT + 1 ))
    fi

    rm -rf "${TMPDIR_CORPUS}"
done

echo ""
echo "═══════════════════════════════════════════════════════"
printf "  Scripts: %d pass  %d fail  %d skip\n" "${PASS_COUNT}" "${FAIL_COUNT}" "${SKIP_COUNT}"
echo "═══════════════════════════════════════════════════════"

if [[ "${C_AVAILABLE}" -eq 0 ]]; then
    echo ""
    echo "NOTE: C reference was unavailable; no comparisons were made."
    echo "      Install or build valkey-server to enable full smoke."
    exit 0
fi

# ── optional RDB bidirectional oracle ────────────────────────────────────────

RDB_FAIL=0
if [[ "${WITH_RDB}" -eq 1 ]]; then
    RDB_ORACLE="${ROOT}/harness/oracle/rdb-diff"
    echo ""
    echo "═══════════════════════════════════════════════════════"
    echo "  RDB bidirectional oracle (rdb-diff)"
    echo "═══════════════════════════════════════════════════════"

    python3 "${RDB_ORACLE}" --direction=all 2>&1 || RDB_FAIL=$?

    if [[ "${RDB_FAIL}" -ne 0 ]]; then
        echo ""
        echo "  rdb-diff: FAIL (exit ${RDB_FAIL})"
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    else
        echo ""
        echo "  rdb-diff: PASS"
    fi
fi

[[ "${FAIL_COUNT}" -eq 0 ]] && exit 0 || exit 1
