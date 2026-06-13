#!/usr/bin/env bash
# Run the current in-scope dual-server replication TCL dashboard sequentially.
#
# This is a telemetry runner, not a conformance claim. It intentionally uses
# --clients 1 and a single tcl-survey invocation so the suite cannot fabricate
# failures through dual-server port/test-dir contention.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

RUNNER_ID="tcl-integration-repl-current"
TIMEOUT_S=300
BASEPORT=47000
PORTCOUNT=4000
SKIP_BUILD=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --runner-id)
            RUNNER_ID="$2"
            shift 2
            ;;
        --timeout-s)
            TIMEOUT_S="$2"
            shift 2
            ;;
        --baseport)
            BASEPORT="$2"
            shift 2
            ;;
        --portcount)
            PORTCOUNT="$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD=1
            shift
            ;;
        *)
            echo "unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

FILES="integration/replication-2,integration/block-repl,integration/replication-3,integration/replication-4,integration/replication-buffer,integration/replication,integration/replication-psync,integration/replication-aof-sync,integration/replica-redirect"

cmd=(
    python3 "${ROOT}/harness/oracle/tcl-survey.py"
    --runner-id "${RUNNER_ID}"
    --profile integration-repl
    --timeout-s "${TIMEOUT_S}"
    --baseport "${BASEPORT}"
    --portcount "${PORTCOUNT}"
    --clients 1
    --files "${FILES}"
    --isolated-tests-copy
)

if [[ "${SKIP_BUILD}" -eq 1 ]]; then
    cmd+=(--skip-build)
fi

exec "${cmd[@]}"
