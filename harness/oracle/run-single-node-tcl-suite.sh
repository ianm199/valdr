#!/usr/bin/env bash
# Canonical upstream Valkey TCL runner for the single-node unit/type surface.
#
# This wraps tcl-survey.py so the official local command is one stable entry
# point instead of a hand-built --files list.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

RUNNER_ID="single-node-external-official"
PROFILE="single-node-external"
TIMEOUT_S=180
BASEPORT=30000
PORTCOUNT=8000
SKIP_BUILD=0
ISOLATED=1
FILES=""
TIER="all"

usage() {
    cat <<'EOF'
Usage:
  bash harness/oracle/run-single-node-tcl-suite.sh [options]

Canonical full single-node unit/type survey:
  bash harness/oracle/run-single-node-tcl-suite.sh

Fast focused loop after building once:
  cargo build --bin redis-server
  bash harness/oracle/run-single-node-tcl-suite.sh --skip-build --files unit/maxmemory

Options:
  --files LIST              Comma-separated TCL files, e.g. unit/multi,unit/maxmemory.
                            Defaults to all unit/*.tcl and unit/type/*.tcl except
                            known non-macOS/non-single-node infra files.
  --list-files              Print the default file list and exit.
  --runner-id ID            Runner id stored in the RunnerResult JSON.
  --profile NAME            tcl-survey.py deny-tag profile. Default: single-node-external.
  --timeout-s N             Per-file timeout. Default: 180.
  --baseport N              Initial Valkey test port. Default: 30000.
  --portcount N             Port range size. Default: 8000.
  --skip-build              Reuse target/debug/redis-server.
  --no-isolated-tests-copy  Run directly in reference/valkey instead of a temp copy.
  -h, --help                Show this help.

The Valkey TCL helper probes port + 10000 for every candidate port, so high
baseports such as 56111 are invalid with a normal portcount. This wrapper
validates the range before starting.
EOF
}

discover_default_files() {
    python3 - <<'PY'
from pathlib import Path

base = Path("reference/valkey/tests")
deny = {
    "unit/tls",
    "unit/mptcp",
    "unit/io-threads",
    "unit/oom-score-adj",
}
sections = []
for path in sorted(base.glob("unit/*.tcl")) + sorted(base.glob("unit/type/*.tcl")):
    rel = str(path.relative_to(base))[:-4]
    if rel not in deny:
        sections.append(rel)
print(",".join(sections))
PY
}

count_files() {
    python3 - "$1" <<'PY'
import sys

items = [part for part in sys.argv[1].split(",") if part]
print(len(items))
PY
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --files)
            FILES="${2:?--files requires a comma-separated list}"
            shift 2
            ;;
        --list-files)
            discover_default_files
            exit 0
            ;;
        --runner-id)
            RUNNER_ID="${2:?--runner-id requires a value}"
            shift 2
            ;;
        --profile)
            PROFILE="${2:?--profile requires a value}"
            shift 2
            ;;
        --timeout-s)
            TIMEOUT_S="${2:?--timeout-s requires a value}"
            shift 2
            ;;
        --baseport)
            BASEPORT="${2:?--baseport requires a value}"
            shift 2
            ;;
        --portcount)
            PORTCOUNT="${2:?--portcount requires a value}"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD=1
            shift
            ;;
        --no-isolated-tests-copy)
            ISOLATED=0
            shift
            ;;
        --tier)
            TIER="${2:?--tier requires a value (all|fast)}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "ERROR: unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z "${FILES}" ]]; then
    FILES="$(discover_default_files)"
fi

if [[ "${TIER}" == "fast" ]]; then
    SLOW_PATH="${ROOT}/harness/oracle/SLOW_FILES.txt"
    if [[ -f "${SLOW_PATH}" ]]; then
        FILES="$(python3 - <<PY
import sys
from pathlib import Path
slow = set()
for line in Path("${SLOW_PATH}").read_text().splitlines():
    stripped = line.split("#", 1)[0].strip()
    if stripped:
        slow.add(stripped)
files = "${FILES}".split(",")
kept = [f for f in files if f and f not in slow]
print(",".join(kept))
PY
)"
        echo "==> tier=fast: filtered FILES list against SLOW_FILES.txt"
    else
        echo "WARN: SLOW_FILES.txt missing; tier=fast is a no-op" >&2
    fi
elif [[ "${TIER}" != "all" ]]; then
    echo "ERROR: --tier must be 'all' or 'fast' (got: ${TIER})" >&2
    exit 2
fi

if ! [[ "${BASEPORT}" =~ ^[0-9]+$ && "${PORTCOUNT}" =~ ^[0-9]+$ && "${TIMEOUT_S}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --baseport, --portcount, and --timeout-s must be positive integers." >&2
    exit 2
fi
if (( BASEPORT <= 32 || PORTCOUNT <= 0 || TIMEOUT_S <= 0 )); then
    echo "ERROR: invalid port or timeout settings." >&2
    exit 2
fi

MAX_PROBED_PORT=$((BASEPORT + PORTCOUNT - 1 + 10000))
CLIENT_MIN=$((BASEPORT - 32))
CLIENT_MAX=$((BASEPORT - 1))
if (( MAX_PROBED_PORT > 65535 )); then
    echo "ERROR: invalid port range." >&2
    echo "The TCL helper probes port + 10000, so baseport + portcount - 1 + 10000 must be <= 65535." >&2
    echo "Got baseport=${BASEPORT}, portcount=${PORTCOUNT}, max probed port=${MAX_PROBED_PORT}." >&2
    exit 2
fi

CMD=(
    python3 harness/oracle/tcl-survey.py
    --runner-id "${RUNNER_ID}"
    --profile "${PROFILE}"
    --timeout-s "${TIMEOUT_S}"
    --baseport "${BASEPORT}"
    --portcount "${PORTCOUNT}"
    --files "${FILES}"
)
if (( ISOLATED == 1 )); then
    CMD+=(--isolated-tests-copy)
fi
if (( SKIP_BUILD == 1 )); then
    CMD+=(--skip-build)
fi

FILE_COUNT="$(count_files "${FILES}")"
TMP_JSON="$(mktemp "${TMPDIR:-/tmp}/single-node-tcl.XXXXXX.json")"
trap 'rm -f "${TMP_JSON}"' EXIT

echo "==> upstream TCL single-node survey"
echo "    files: ${FILE_COUNT}"
echo "    profile: ${PROFILE}"
echo "    deny tags: see harness/oracle/tcl-survey.py profile ${PROFILE}"
echo "    timeout: ${TIMEOUT_S}s per file"
echo "    ports: ${CLIENT_MIN}-${CLIENT_MAX} for harness client, ${BASEPORT}-$((BASEPORT + PORTCOUNT - 1)) for servers"
echo "    isolated tests copy: ${ISOLATED}"
if (( SKIP_BUILD == 1 )); then
    echo "    build: skipped"
else
    echo "    build: enabled"
fi
echo ""

if ! "${CMD[@]}" >"${TMP_JSON}"; then
    cat "${TMP_JSON}" >&2 || true
    exit 1
fi

python3 - "${TMP_JSON}" "${ROOT}" <<'PY'
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

tmp = Path(sys.argv[1])
root = Path(sys.argv[2])
data = json.loads(tmp.read_text())
evidence = data.get("evidence", {})
files = evidence.get("files") or []
run_id = evidence.get("run_id")

if run_id:
    run_dir = root / "harness" / "oracle" / "results" / "tcl-survey" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    result_path = run_dir / "result.json"
    result_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
else:
    run_dir = None
    result_path = None

passed = sum(item.get("passed") or 0 for item in files)
failed = sum(item.get("failed") or 0 for item in files)
counted = passed + failed
timeouts = [item["test"] for item in files if item.get("timed_out")]
no_summary = [
    item["test"]
    for item in files
    if item.get("passed") is None or item.get("failed") is None
]
failed_files = [item for item in files if item.get("failed")]

print(data.get("summary", "TCL survey completed."))
if counted:
    print(f"Counted pass rate: {passed}/{counted} = {passed / counted * 100:.3f}%")
    failures_allowed_at_98 = math.floor(counted * 0.02) - failed
    if failures_allowed_at_98 >= 0:
        print(f"Distance to 98%: already there; {failures_allowed_at_98} more counted failures could be absorbed.")
    else:
        needed = math.ceil(counted * 0.98) - passed
        print(f"Distance to 98%: fix {needed} counted failures.")
    print(f"Distance to 100% counted: fix {failed} counted failures.")

print(f"Timeout files: {len(timeouts)}")
if timeouts:
    print("  " + ", ".join(timeouts))

print(f"No-summary files: {len(no_summary)}")
if no_summary:
    print("  " + ", ".join(no_summary))

if failed_files:
    print("Failing counted files:")
    for item in failed_files:
        print(f"  {item['test']}: {item.get('passed')}/{item.get('total')} passed, {item.get('failed')} failed")

if run_dir:
    print(f"Artifacts: {run_dir.relative_to(root)}")
if result_path:
    print(f"Run JSON: {result_path.relative_to(root)}")
PY
