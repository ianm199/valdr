#!/usr/bin/env bash
# harness/oracle/setup_tcl_runner.sh — prepare the canonical Valkey TCL suite
# to run against our Rust server.
#
# Builds the debug binary if missing, then ensures `target/debug/valkey-server`
# exists as a symlink to our `target/debug/redis-server`. The TCL harness reads
# `$VALKEY_BIN_DIR/valkey-server`, so the symlink lets the unmodified upstream
# scripts launch our server without any patch to `reference/valkey/tests/`.
#
# Usage:
#   bash harness/oracle/setup_tcl_runner.sh [--skip-build]
#
# Then run the suite with, e.g.:
#   cd reference/valkey
#   VALKEY_BIN_DIR=$(pwd)/../../target/debug \
#     tclsh tests/test_helper.tcl \
#     --single unit/type/string --clients 1 --skip-leaks \
#     --denytags "needs:repl needs:debug"

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN_DIR="${ROOT}/target/debug"
RUST_BIN="${BIN_DIR}/redis-server"
VALKEY_LINK="${BIN_DIR}/valkey-server"

SKIP_BUILD=0
for arg in "$@"; do
    [[ "${arg}" == "--skip-build" ]] && SKIP_BUILD=1
done

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
    echo "==> cargo build --bin redis-server"
    (cd "${ROOT}" && cargo build --bin redis-server 2>&1 | tail -5)
fi

if [[ ! -x "${RUST_BIN}" ]]; then
    echo "ERROR: ${RUST_BIN} not found or not executable." >&2
    exit 2
fi

if [[ -L "${VALKEY_LINK}" || -e "${VALKEY_LINK}" ]]; then
    rm -f "${VALKEY_LINK}"
fi
ln -s "${RUST_BIN}" "${VALKEY_LINK}"
echo "==> linked ${VALKEY_LINK} -> ${RUST_BIN}"

echo ""
echo "Next:"
echo "  cd ${ROOT}/reference/valkey"
echo "  VALKEY_BIN_DIR=${BIN_DIR} \\"
echo "    tclsh tests/test_helper.tcl \\"
echo "    --single unit/type/string --clients 1 --skip-leaks \\"
echo "    --denytags \"needs:repl needs:debug\""
