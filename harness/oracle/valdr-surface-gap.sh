#!/usr/bin/env bash
# valdr-engine command-surface gap report — the self-replenishing backlog engine.
#
# Diffs the FULL production command table (crates/redis-commands/src/dispatch.rs)
# against what the wasm-safe edge engine (crates/valdr-engine/src/lib.rs) actually
# dispatches, minus an explicit out-of-scope/subcommand exclusion list. Prints the
# in-scope commands still missing — i.e. the next waves of overnight work.
#
# Run this whenever the active wave queue empties. When it prints "0 in-scope
# commands remaining", the edge command surface is exhaustive and the campaign
# advances to Phase 2 (see CAMPAIGN_BACKLOG.md).
#
#   bash harness/oracle/valdr-surface-gap.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

DISPATCH=crates/redis-commands/src/dispatch.rs
ENGINE=crates/valdr-engine/src/lib.rs

# Universe: every top-level command literal in the production dispatcher.
universe=$(grep -rhoE 'b"[A-Z][A-Z0-9_]*"' "$DISPATCH" | tr -d 'b"' | sort -u)

# Implemented: every command the edge engine dispatches via ascii_eq(command, b"...").
implemented=$(grep -oE 'ascii_eq\(command, b"[A-Z][A-Z0-9_]*"' "$ENGINE" \
  | grep -oE 'b"[A-Z][A-Z0-9_]*"' | tr -d 'b"' | sort -u)

# Exclusion list: (a) server/cluster/replication/persistence/pubsub/admin surfaces
# that do not belong in a single-Durable-Object wasm engine, and (b) subcommand
# tokens that the grep catches but are not top-level commands. Edit deliberately;
# moving a command OUT of here is how you pull it into scope.
exclude=$(cat <<'EOF'
ACL
AUTH
BGREWRITEAOF
BGSAVE
CLIENT
CLUSTER
COMMAND
COMMANDLOG
CONFIG
CREATECONSUMER
DB
DBSIZE
DEBUG
FAILOVER
FCALL
FCALL_RO
FLUSHDB
FUNCTION
HELLO
HELP
INFO
KILL
LASTSAVE
LATENCY
MEMORY
MIGRATE
MODULE
MONITOR
MOVE
PFDEBUG
PFSELFTEST
PSUBSCRIBE
PSYNC
PUBLISH
PUBSUB
PUNSUBSCRIBE
PURGE
QUIT
READONLY
READWRITE
REPLACE
REPLCONF
REPLICAOF
REPLY
RESET
ROLE
SAVE
SELECT
SHUTDOWN
SLAVEOF
SLOWLOG
SPUBLISH
SSUBSCRIBE
STATS
STORE
SUBSCRIBE
SUNSUBSCRIBE
SWAPDB
SYNC
UNSUBSCRIBE
WAIT
WAITAOF
WHOAMI
EOF
)

in_scope=$(comm -23 <(printf '%s\n' "$universe") <(printf '%s\n' "$exclude" | sort -u))
missing=$(comm -23 <(printf '%s\n' "$in_scope") <(printf '%s\n' "$implemented"))

u_n=$(printf '%s\n' "$universe" | grep -c . || true)
i_n=$(printf '%s\n' "$implemented" | grep -c . || true)
s_n=$(printf '%s\n' "$in_scope" | grep -c . || true)
m_n=$(printf '%s\n' "$missing" | grep -c . || true)

echo "valdr-engine surface gap"
echo "  universe (dispatch.rs):     $u_n"
echo "  implemented (engine):       $i_n"
echo "  in-scope (universe-exclude): $s_n"
echo "  in-scope MISSING:           $m_n"
echo
if [ "$m_n" -eq 0 ]; then
  echo "0 in-scope commands remaining — edge surface is exhaustive. Advance to Phase 2."
else
  echo "in-scope commands still missing (next waves):"
  printf '%s\n' "$missing" | sed 's/^/  - /'
fi
