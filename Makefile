# Valdr — one place to run things. `make` with no target prints this help.
#
# Everything here is a thin, documented wrapper over the scripts in
# harness/bench/ and harness/oracle/. The rule: if you find yourself typing a
# raw `python3 harness/bench/...` or a pile of VALKEY_* env vars, add or use a
# target here instead. One way to do each thing; flags are make variables you
# override on the command line, e.g.:
#
#     make bench-p1 TRIALS=40 COMMANDS=get,set
#     make bench-quick
#     make bench SKIP_BUILD=1

SHELL := /bin/bash
.DEFAULT_GOAL := help

# ── tunables (override on the command line: `make bench-p1 TRIALS=40`) ────────
SKIP_BUILD ?= 0          # 1 = reuse the existing release binary, skip cargo build
TRIALS     ?= 15         # bench-p1: paired trials per command
COMMANDS   ?= get,set,ping_mbulk   # bench-p1: any of get,set,incr,ping_mbulk
REQUESTS   ?= 50000      # bench-p1: requests per trial
CLIENTS    ?= 50         # concurrent clients
PAYLOAD    ?= 64         # value size in bytes
TIER       ?= fast       # oracle: fast (21 files ~14s) | all (full requested set)
CRATE      ?=            # test: narrow to one crate, e.g. redis-core
KIT        ?=            # test-kit: one kit, e.g. conn_transport_kit
FILES      ?=            # oracle: specific files, e.g. unit/type/string (overrides TIER)
FORMAT     ?= table      # bench/oracle output: table (human-readable) | json
REPL_KITS  ?= repl_correctness_kit repl_buffer_kit fullsync_lifecycle_kit psync_reconnect_kit failover_redirect_kit
SERVER_REPL_KITS ?= repl_wait_for_sync_kit

MATRIX_TSV = $$(ls -t harness/bench/results/*profile-matrix.tsv | head -1)

.PHONY: help build test test-kit repl-kits structure-audit bench bench-quick bench-p1 bench-show bench-release oracle oracle-full

help: ## Show this help
	@echo "Valdr targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) \
	  | sort \
	  | awk -F':.*## ' '{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Flags (override inline): SKIP_BUILD TRIALS COMMANDS REQUESTS CLIENTS PAYLOAD TIER CRATE KIT FILES REPL_KITS SERVER_REPL_KITS"
	@echo "  e.g.  make bench-p1 TRIALS=40 COMMANDS=get,set"
	@echo "        make test CRATE=redis-core         # one crate, not the workspace"
	@echo "        make oracle                        # fast tier, ~14s (TIER=all for full)"
	@echo "        make oracle FILES=unit/type/string # just these files"
	@echo "        make bench FORMAT=json             # raw JSON instead of the table"

build: ## Build the release server binary the benchmarks drive
	cargo build --release -p redis-server

test: ## Rust tests — the fast inner loop. CRATE=redis-core to narrow to one crate
	@if [ -n "$(CRATE)" ]; then cargo test -p $(CRATE); else cargo test --workspace; fi

test-kit: ## Custom subsystem testers only (fastest tier). KIT=conn_transport_kit for one
	@if [ -n "$(KIT)" ]; then \
	  cargo test -p redis-core --test $(KIT) 2>/dev/null \
	    || cargo test -p redis-commands --test $(KIT); \
	else \
	  cargo test -p redis-core --test conn_transport_kit \
	    && cargo test -p redis-commands --test aof_correctness_kit \
	    && cargo test -p redis-commands --test repl_correctness_kit; \
	fi

repl-kits: ## Replication/HA Rust kits only; fast debugger before long Tcl scoreboards
	@set -e; \
	for kit in $(REPL_KITS); do \
	  echo "==> redis-commands $$kit"; \
	  cargo test -p redis-commands --test $$kit; \
	done; \
	echo "==> redis-commands replica_dialer::tests"; \
	cargo test -p redis-commands --lib replica_dialer::tests; \
	for kit in $(SERVER_REPL_KITS); do \
	  echo "==> redis-server $$kit"; \
	  cargo test -p redis-server --test $$kit; \
	done

structure-audit: ## Maintainability hotspot report for Valdr compatibility crates/docs/tests
	python3 harness/structure_audit.py

bench: ## Full profile matrix (p1/p16/p100 + range-heavy) vs upstream. FORMAT=json for raw JSON
	@echo "running profile matrix (SKIP_BUILD=$(SKIP_BUILD), clients=$(CLIENTS), payload=$(PAYLOAD))…" >&2
	@VALKEY_BENCH_SKIP_BUILD=$(SKIP_BUILD) \
	VALKEY_MATRIX_CLIENTS=$(CLIENTS) VALKEY_MATRIX_PAYLOAD=$(PAYLOAD) \
	  bash harness/bench/run-profile-matrix.sh >/tmp/valdr-matrix.json
	@$(MAKE) -s bench-show FORMAT=$(FORMAT)

bench-quick: ## Fast narrow matrix (reduced request counts, reuses existing binary). FORMAT=json for raw JSON
	@echo "running quick narrow matrix (reusing existing binary)…" >&2
	@VALKEY_BENCH_SKIP_BUILD=1 \
	VALKEY_MATRIX_CORE_P1_REQUESTS=20000 \
	VALKEY_MATRIX_CORE_P16_REQUESTS=50000 \
	VALKEY_MATRIX_CORE_P100_REQUESTS=50000 \
	VALKEY_MATRIX_RANGE_REQUESTS=25000 \
	VALKEY_MATRIX_CLIENTS=$(CLIENTS) VALKEY_MATRIX_PAYLOAD=$(PAYLOAD) \
	  bash harness/bench/run-profile-matrix.sh >/tmp/valdr-matrix.json
	@$(MAKE) -s bench-show FORMAT=$(FORMAT)

bench-p1: ## Paired pipeline=1 parity probe (low-noise median+IQR; the per-request-overhead question)
	@if [ "$(SKIP_BUILD)" != "1" ]; then cargo build --release -p redis-server; fi
	python3 harness/bench/p1-parity-probe.py \
	  --commands $(COMMANDS) --trials $(TRIALS) \
	  --requests $(REQUESTS) --clients $(CLIENTS) --payload $(PAYLOAD)

bench-show: ## Reprint the most recent matrix. FORMAT=wide for all columns, json for raw
	@tsv="$(MATRIX_TSV)"; echo "── $$tsv ──"; \
	if [ "$(FORMAT)" = "json" ]; then cat /tmp/valdr-matrix.json; \
	elif [ "$(FORMAT)" = "wide" ]; then python3 harness/bench/format-matrix.py --wide "$$tsv"; \
	else python3 harness/bench/format-matrix.py "$$tsv"; fi

bench-release: ## The release-grade packet (warmup + all probes + Markdown bundle, ~90s)
	bash harness/bench/official-warm-run.sh

site-data: ## Regenerate docs/perf-data.json + README perf tables from the latest local benchmark artifacts (site fetches the JSON; commit both to update)
	python3 harness/bench/build-site-data.py

oracle: ## Tcl oracle vs our server. TIER=fast (~14s, default) | all; FILES=unit/type/string; FORMAT=json for raw
	@python3 harness/oracle/tcl-survey.py --runner-id make --profile single-node-external \
	  --timeout-s 180 --baseport 38000 --portcount 4000 --isolated-tests-copy --skip-build \
	  $(if $(FILES),--files $(FILES),--tier $(strip $(TIER))) >/tmp/valdr-oracle.raw; rc=$$?; \
	if [ "$(FORMAT)" = "json" ]; then python3 harness/oracle/summarize-survey.py --json /tmp/valdr-oracle.raw; \
	else python3 harness/oracle/summarize-survey.py /tmp/valdr-oracle.raw; fi; \
	exit $$rc

oracle-full: ## Full single-node publication suite (long; builds + runs everything)
	bash harness/oracle/run-single-node-tcl-suite.sh
