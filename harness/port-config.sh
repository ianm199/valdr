# redis-rs-port/harness/port-config.sh
# Sourced by port-harness/fanout.sh and port-harness/lib/install-agents.sh.
# Defines the project ↔ chassis interface for this port.
#
# See port-harness/docs/PORT_CONFIG.md for the schema.

# ────────────────────────────────────────────────────────────────────────
# Required variables
# ────────────────────────────────────────────────────────────────────────

PORT_NAME="redis-rs-port"
PORT_SOURCE_LANG="C"
PORT_TARGET_LANG="Rust"

PORT_PORTING_MD="$PORT_PROJECT_ROOT/PORTING.md"
PORT_ANALYSES_DIR="$PORT_PROJECT_ROOT/harness"  # macros.tsv / types.tsv / etc. live here for Redis
PORT_FILE_DEPS_TSV="$PORT_PROJECT_ROOT/harness/file-deps.tsv"
PORT_TYPE_VOCAB_TSV="$PORT_PROJECT_ROOT/harness/type-vocabulary.tsv"

PORT_SOURCE_DIR="$PORT_PROJECT_ROOT/reference/valkey/src"
PORT_TARGET_CRATES_DIR="crates"

PORT_TRAILER_FIELDS="source,target_crate,confidence,todos,port_notes,unsafe_blocks,notes"

PORT_COMPILE_CMD_PER_CRATE="cargo check -p <crate>"
PORT_IN_LOOP_VALIDATOR_CMD='rustc --edition 2021 --crate-type=lib --emit=metadata -o /tmp/syntax-check <file>'

PORT_EXPECTED_ERROR_PATTERNS='- `error[E0432]: unresolved import ...`
- `error[E0412]: cannot find type ... in this scope`
- `error[E0433]: failed to resolve: could not find ...`
- `error[E0425]: cannot find value/function ...`
- `error[E0282]: type annotations needed`
- `error: cannot find macro ... in this scope`
- `error: use of undeclared crate or module ...`
- `error: aborting due to N previous errors`'

PORT_BANNED_RULES='- **Banned for Redis data:** `String`, `&str`, `from_utf8`, `String::from_utf8`, `from_utf8_unchecked`. Use `&[u8]`, `Vec<u8>`, `Box<[u8]>`, or `RedisString`.
- **No `unsafe` in pilot crates** (redis-types, redis-protocol, redis-core, redis-commands, redis-server). If you need it, escalate via `TODO(architect)`.
- **No `panic!`, `unwrap()`, `expect()` outside test code.** Use `Result<T, RedisError>`.
- **No hand-edits to generated files** (`crates/redis-commands/src/generated.rs`).
- **Async/tokio IS allowed** for Redis — server is network code. (Differs from lua-rs-port.)'

PORT_TEST_DIFF_CMD="$PORT_PROJECT_ROOT/harness/oracle/wire-diff --diff-only"
PORT_IN_LOOP_ORACLE_CMD="$PORT_PROJECT_ROOT/harness/oracle/wire-diff"
PORT_RUN_PHASE_CMD="$PORT_PROJECT_ROOT/harness/oracle/run-phase.sh"
PORT_TEST_RESULTS_JSON="$PORT_PROJECT_ROOT/harness/oracle/test-results.json"
PORT_EVIDENCE_DIR="$PORT_PROJECT_ROOT/harness/oracle/results"

PORT_ANALYSES_INPUTS='1. `harness/type-vocabulary.tsv` — canonical owners for cross-crate types (look up, do not invent).
2. `harness/file-deps.tsv` — which crate this file maps to.
3. `harness/command-registry.json` — generated command metadata (arity, flags, key specs).
4. Reference Valkey source under `reference/valkey/src/` for the file you have been assigned (and any `.h` it directly includes).'

# ────────────────────────────────────────────────────────────────────────
# Optional variables
# ────────────────────────────────────────────────────────────────────────

PORT_AGENT_TRANSLATOR="translator"
PORT_AGENT_ALLOWED_TOOLS="Read,Write,Edit,Glob,Grep,Bash(cargo check*),Bash(rustc *),Bash(./harness/oracle/wire-diff*)"

# ────────────────────────────────────────────────────────────────────────
# Required functions
# ────────────────────────────────────────────────────────────────────────

# Print one C-file path (relative to reference/valkey/) per line.
port_files_for_phase() {
    local phase="$1"
    case "$phase" in
        pilot)
            # Pilot scope per REDIS_PORT_HARNESS_SPEC.md §First Pilot:
            # RESP parser + minimal TCP + a handful of commands.
            # File set will expand as we learn what's actually needed.
            printf '%s\n' \
                networking.c \
                resp_parser.c \
                t_string.c
            ;;
        protocol)
            printf '%s\n' resp_parser.c networking.c
            ;;
        strings)
            printf '%s\n' t_string.c
            ;;
        *)
            echo "unknown phase: $phase" >&2
            return 1
            ;;
    esac
}

# Print one line: "<crate>\t<rust-rel-path>" (tab-separated).
# When file-deps.tsv exists, look up there. Until it does, hardcoded.
port_target_for_file() {
    local cfile="$1"
    if [ -f "$PORT_FILE_DEPS_TSV" ]; then
        awk -F'\t' -v c="$cfile" '$1==c {print $2"\t"$3; exit}' "$PORT_FILE_DEPS_TSV"
        return 0
    fi
    # Fallback (until file-deps.tsv exists)
    case "$cfile" in
        resp_parser.c) printf 'redis-protocol\tsrc/parser.rs\n' ;;
        networking.c)  printf 'redis-core\tsrc/networking.rs\n' ;;
        t_string.c)    printf 'redis-commands\tsrc/string.rs\n' ;;
        t_list.c)      printf 'redis-commands\tsrc/list.rs\n' ;;
        t_hash.c)      printf 'redis-commands\tsrc/hash.rs\n' ;;
        t_set.c)       printf 'redis-commands\tsrc/set.rs\n' ;;
        t_zset.c)      printf 'redis-commands\tsrc/zset.rs\n' ;;
        db.c)          printf 'redis-core\tsrc/db.rs\n' ;;
        object.c)      printf 'redis-core\tsrc/object.rs\n' ;;
        server.c)      printf 'redis-server\tsrc/server.rs\n' ;;
        *)             return 1 ;;
    esac
}

# Exit 0 if rust file is already a real port; nonzero otherwise.
port_is_already_ported() {
    local rust="$1"
    [ -f "$rust" ] || return 1
    grep -qE '^//\s*source:.*\.[ch]\b' "$rust" 2>/dev/null \
        && ! grep -qE '^//\s*source:.*\(none' "$rust" 2>/dev/null
}

# In-loop validator. Returns 0 if file has no real syntax errors
# (cross-crate name-resolution errors are expected and ignored).
port_validate_target() {
    local rust="$1"
    local tmp out residual
    tmp=$(mktemp -t redis-port-syntax.XXXXXX)
    out=$(rustc --edition 2021 --crate-type=lib --emit=metadata -o "$tmp" "$rust" 2>&1)
    rm -f "$tmp"
    local filt='cannot find|could not find|failed to resolve|unresolved|aborting due to|type annotations needed|no `[A-Z]'
    residual=$(echo "$out" | grep '^error' | grep -vE "$filt" | wc -l | tr -d ' ')
    [ "$residual" -eq 0 ]
}

# Build the per-file translator prompt.
port_build_prompt() {
    local cfile="$1"
    local rust_full="$2"
    local crate="$3"
    cat <<EOF
Translate the C file at \`reference/valkey/src/$cfile\` to Rust at \`$rust_full\` per PORTING.md.

This is a Phase A task: faithful logic translation. The file does NOT need to compile.

Crate: $crate.

Key constraints from PORTING.md (review before writing):
- BYTES, not String/&str/from_utf8 for Redis data (keys, values, RESP payloads). Use &[u8], Vec<u8>, RedisString.
- No unsafe in pilot crates. TODO(architect) if you think you need it.
- No panic/unwrap outside tests; use Result<T, RedisError>.
- async fn / tokio ARE allowed (differs from lua-rs-port).
- Type-vocabulary hook blocks redefinitions of canonical types; pub use the owner instead, or TODO(architect).
- Commands take &mut CommandContext.
- Embed source references as // C: comments; full C-as-comments only for hairy code.
- End with PORT STATUS trailer.

Use the Translator subagent (.claude/agents/translator.md). When the
in-loop validator (rustc --emit=metadata) shows only expected name-
resolution errors, stop — don't try to make it compile cross-crate.
EOF
}
