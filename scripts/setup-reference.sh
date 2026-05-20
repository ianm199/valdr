#!/usr/bin/env bash
# scripts/setup-reference.sh — clone the pinned upstream Valkey source.
#
# valkey-rs verifies itself against upstream Valkey via three oracles
# (wire-diff, rdb-diff, and the upstream TCL test suite). All three need
# the real Valkey source built locally. This script clones it at the
# pinned commit recorded in harness/source.toml.
#
# Run once after cloning valkey-rs. Idempotent — safe to re-run.
#
# Usage:
#   bash scripts/setup-reference.sh
#
# Requirements:
#   - git, make, a C compiler (for building valkey-server)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REF_DIR="${ROOT}/reference/valkey"

# Pinned commit lives in harness/source.toml; we parse the [source] table
# manually rather than pulling in a TOML dep.
SOURCE_TOML="${ROOT}/harness/source.toml"
if [[ ! -f "${SOURCE_TOML}" ]]; then
    echo "ERROR: ${SOURCE_TOML} not found — wrong working tree?" >&2
    exit 1
fi

REPO_URL="$(awk -F' *= *' '/^repo/ {gsub(/"/, "", $2); print $2}' "${SOURCE_TOML}")"
PINNED_COMMIT="$(awk -F' *= *' '/^commit/ {gsub(/"/, "", $2); print $2}' "${SOURCE_TOML}" | awk '{print $1}')"

if [[ -z "${REPO_URL}" || -z "${PINNED_COMMIT}" ]]; then
    echo "ERROR: failed to parse repo / commit from ${SOURCE_TOML}" >&2
    exit 1
fi

echo "==> upstream:   ${REPO_URL}"
echo "==> pinned at:  ${PINNED_COMMIT}"
echo "==> target dir: ${REF_DIR}"
echo ""

if [[ -d "${REF_DIR}/.git" ]]; then
    echo "==> reference already cloned; updating to pinned commit"
    cd "${REF_DIR}"
    git fetch origin "${PINNED_COMMIT}" 2>/dev/null || git fetch origin
    git checkout --quiet "${PINNED_COMMIT}"
else
    echo "==> cloning"
    mkdir -p "$(dirname "${REF_DIR}")"
    git clone "${REPO_URL}" "${REF_DIR}"
    cd "${REF_DIR}"
    git checkout --quiet "${PINNED_COMMIT}"
fi

echo ""
echo "==> building valkey-server (this takes a minute)"
cd "${REF_DIR}"
make -j BUILD_TLS=no USE_SYSTEMD=no 2>&1 | tail -3

if [[ -x "${REF_DIR}/src/valkey-server" ]]; then
    VERSION="$("${REF_DIR}/src/valkey-server" --version 2>&1 | head -1)"
    echo ""
    echo "✓ done. reference binary: ${REF_DIR}/src/valkey-server"
    echo "✓ ${VERSION}"
    echo ""
    echo "Next steps:"
    echo "  cargo build --release"
    echo "  bash harness/oracle/smoke.sh --skip-build       # 21/21 wire-diff"
    echo "  bash harness/oracle/smoke.sh --skip-build --with-rdb  # + 378/378 RDB"
else
    echo "ERROR: build did not produce ${REF_DIR}/src/valkey-server" >&2
    exit 1
fi
