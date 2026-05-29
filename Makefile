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

MATRIX_TSV = $$(ls -t harness/bench/results/*profile-matrix.tsv | head -1)

.PHONY: help build bench bench-quick bench-p1 bench-show bench-release oracle

help: ## Show this help
	@echo "Valdr targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) \
	  | sort \
	  | awk -F':.*## ' '{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Flags (override inline): SKIP_BUILD TRIALS COMMANDS REQUESTS CLIENTS PAYLOAD"
	@echo "  e.g.  make bench-p1 TRIALS=40 COMMANDS=get,set"
	@echo "        make bench-quick      # fast narrow matrix, no rebuild"

build: ## Build the release server binary the benchmarks drive
	cargo build --release -p redis-server

bench: ## Full profile matrix (p1/p16/p100 + range-heavy) vs upstream, printed readably
	@echo "running profile matrix (SKIP_BUILD=$(SKIP_BUILD), clients=$(CLIENTS), payload=$(PAYLOAD))…"
	@VALKEY_BENCH_SKIP_BUILD=$(SKIP_BUILD) \
	VALKEY_MATRIX_CLIENTS=$(CLIENTS) VALKEY_MATRIX_PAYLOAD=$(PAYLOAD) \
	  bash harness/bench/run-profile-matrix.sh >/tmp/valdr-matrix.json
	@$(MAKE) -s bench-show

bench-quick: ## Fast narrow matrix (reduced request counts, reuses existing binary)
	@echo "running quick narrow matrix (reusing existing binary)…"
	@VALKEY_BENCH_SKIP_BUILD=1 \
	VALKEY_MATRIX_CORE_P1_REQUESTS=20000 \
	VALKEY_MATRIX_CORE_P16_REQUESTS=50000 \
	VALKEY_MATRIX_CORE_P100_REQUESTS=50000 \
	VALKEY_MATRIX_RANGE_REQUESTS=25000 \
	VALKEY_MATRIX_CLIENTS=$(CLIENTS) VALKEY_MATRIX_PAYLOAD=$(PAYLOAD) \
	  bash harness/bench/run-profile-matrix.sh >/tmp/valdr-matrix.json
	@$(MAKE) -s bench-show

bench-p1: ## Paired pipeline=1 parity probe (low-noise median+IQR; the per-request-overhead question)
	@if [ "$(SKIP_BUILD)" != "1" ]; then cargo build --release -p redis-server; fi
	python3 harness/bench/p1-parity-probe.py \
	  --commands $(COMMANDS) --trials $(TRIALS) \
	  --requests $(REQUESTS) --clients $(CLIENTS) --payload $(PAYLOAD)

bench-show: ## Reprint the most recent profile matrix as an aligned table
	@echo "── $(MATRIX_TSV) ──"
	@column -t -s $$'\t' "$(MATRIX_TSV)"

bench-release: ## The release-grade packet (warmup + all probes + Markdown bundle, ~90s)
	bash harness/bench/official-warm-run.sh

oracle: ## Run the upstream Tcl suite against our server (FILES=unit/type/string for one file)
	@if [ -n "$(FILES)" ]; then \
	  python3 harness/oracle/tcl-survey.py --runner-id make --profile single-node-external \
	    --timeout-s 180 --baseport 38000 --portcount 4000 \
	    --files $(FILES) --isolated-tests-copy --skip-build; \
	else \
	  bash harness/oracle/run-single-node-tcl-suite.sh; \
	fi
