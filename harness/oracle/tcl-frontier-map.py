#!/usr/bin/env python3
"""Build a ranked map of the hidden TCL conformance frontier.

This tool is intentionally diagnostic. It does not run the TCL suite and it
does not claim new conformance. It merges the full-suite inventory with the
latest focused `tcl-survey.py` logs, classifies timeout/no-summary files, and
writes the next packet candidates.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
INVENTORY = ROOT / "harness" / "oracle" / "results" / "tcl-suite-inventory" / "latest.json"
SURVEY_ROOT = ROOT / "harness" / "oracle" / "results" / "tcl-survey"
OUT_ROOT = ROOT / "harness" / "oracle" / "results" / "tcl-frontier"
DOC_PATH = ROOT / "docs" / "TCL_HIDDEN_FRONTIER_20260524.md"

FOCUS = [
    "unit/scripting",
    "unit/functions",
    "unit/multi",
    "unit/pubsub",
    "unit/type/stream",
    "unit/type/stream-cgroups",
    "unit/introspection",
    "unit/keyspace",
    "unit/geo",
    "integration/aof",
    "integration/rdb",
]

ADDITIONAL_RANK_TESTS = [
    "unit/type/list",
    "unit/type/hash",
    "unit/tracking",
    "unit/introspection-2",
    "unit/bitops",
    "unit/sort",
    "unit/dump",
    "unit/hyperloglog",
]

SUMMARY_RE = re.compile(r"Test Summary:\s+(\d+)\s+passed,\s+(\d+)\s+failed")
ERR_RE = re.compile(r"^\s*(?:\*+\s*)?\[err\]:\s*(.*)$")
EXCEPTION_RE = re.compile(r"^\[exception\]:\s+Executing test client:\s+(.*)$")
TEST_BRACE_RE = re.compile(r'^"test \{([^}]+)\}')
TEST_QUOTE_RE = re.compile(r'^"test "([^"]+)"')
ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


SUBSYSTEMS: dict[str, dict[str, Any]] = {
    "unit/scripting": {
        "root": "Lua scripting sandbox, redis.call reply conversion, ACL/category exposure",
        "local_files": [
            "crates/redis-commands/src/eval.rs",
            "crates/redis-commands/src/dispatch.rs",
            "crates/redis-core/src/acl.rs",
        ],
        "upstream_files": [
            "reference/valkey/src/eval.c",
            "reference/valkey/src/modules/lua/script_lua.c",
            "reference/valkey/tests/unit/scripting.tcl",
        ],
        "packet": "tcl-scripting-acl-globals-frontier-v1",
        "recommendation": (
            "Split into two passes: first make the no-summary ACL/FUNCTION abort "
            "diagnostic and correct, then address the revealed global-protection "
            "and Redis namespace failures. Keep all work inside the scripting/ACL lane."
        ),
        "product_value": "high",
        "risk": "high",
    },
    "unit/functions": {
        "root": "Function library engine, Lua function registry, async/blocking test interaction",
        "local_files": [
            "crates/redis-commands/src/eval.rs",
            "crates/redis-commands/src/connection.rs",
            "crates/redis-core/src/acl.rs",
        ],
        "upstream_files": [
            "reference/valkey/src/functions.c",
            "reference/valkey/src/modules/lua/function_lua.c",
            "reference/valkey/tests/unit/functions.tcl",
        ],
        "packet": "tcl-functions-timeout-scout-then-library-v1",
        "recommendation": (
            "Do not start with a broad function rewrite. First add a single-test "
            "bisect/scout runner for the timeout, then port only the first library "
            "lifecycle semantic that blocks summary output."
        ),
        "product_value": "high",
        "risk": "high",
    },
    "unit/multi": {
        "root": "Transaction state machine: WATCH-in-MULTI, dirty flag, queueing errors, DISCARD/UNWATCH",
        "local_files": [
            "crates/redis-commands/src/multi.rs",
            "crates/redis-core/src/client.rs",
            "crates/redis-core/src/db.rs",
        ],
        "upstream_files": [
            "reference/valkey/src/multi.c",
            "reference/valkey/tests/unit/multi.tcl",
        ],
        "packet": "tcl-multi-watch-dirty-queue-v1",
        "recommendation": (
            "Port the upstream transaction error-state lifecycle. This is smaller "
            "than scripting/functions and likely converts a timeout file into a "
            "counted fail/pass file quickly."
        ),
        "product_value": "high",
        "risk": "medium",
    },
    "unit/pubsub": {
        "root": "Pub/Sub keyspace notification ordering and client reply behavior",
        "local_files": [
            "crates/redis-commands/src/pubsub.rs",
            "crates/redis-core/src/notify.rs",
            "crates/redis-core/src/pubsub_registry.rs",
            "crates/redis-commands/src/connection.rs",
        ],
        "upstream_files": [
            "reference/valkey/src/pubsub.c",
            "reference/valkey/src/notify.c",
            "reference/valkey/tests/unit/pubsub.tcl",
        ],
        "packet": "tcl-pubsub-keyspace-notify-order-v1",
        "recommendation": (
            "Start from the stream event notification mismatch. Verify exact "
            "xgroup/xadd ordering and CLIENT REPLY behavior, then rerun the file "
            "with a short timeout to see if the hang collapses."
        ),
        "product_value": "medium",
        "risk": "medium",
    },
    "unit/type/stream": {
        "root": "Stream XREAD/XADD transaction wake behavior",
        "local_files": [
            "crates/redis-commands/src/stream.rs",
            "crates/redis-ds/src/stream.rs",
            "crates/redis-commands/src/multi.rs",
        ],
        "upstream_files": [
            "reference/valkey/src/t_stream.c",
            "reference/valkey/tests/unit/type/stream.tcl",
        ],
        "packet": "tcl-stream-transaction-xread-wake-v1",
        "recommendation": (
            "Port the upstream blocked stream client wake semantics around XADD "
            "inside MULTI before touching consumer-group metadata."
        ),
        "product_value": "high",
        "risk": "high",
    },
    "unit/type/stream-cgroups": {
        "root": "Consumer group PEL metadata and XREADGROUP blocking edge cases",
        "local_files": [
            "crates/redis-commands/src/stream.rs",
            "crates/redis-ds/src/stream.rs",
        ],
        "upstream_files": [
            "reference/valkey/src/t_stream.c",
            "reference/valkey/tests/unit/type/stream-cgroups.tcl",
        ],
        "packet": "tcl-stream-cgroups-pel-idle-seen-time-v1",
        "recommendation": (
            "Implement the missing `idle`/seen-time dictionary shape and keep the "
            "blocking XREADGROUP failures as separate follow-up packets."
        ),
        "product_value": "high",
        "risk": "high",
    },
    "unit/introspection": {
        "root": "Harness tmp-dir/server lifecycle first; then CLIENT/COMMAND/CONFIG/INFO introspection",
        "local_files": [
            "harness/oracle/tcl-survey.py",
            "crates/redis-commands/src/connection.rs",
            "crates/redis-commands/src/info.rs",
            "crates/redis-commands/src/generated.rs",
        ],
        "upstream_files": [
            "reference/valkey/src/networking.c",
            "reference/valkey/src/server.c",
            "reference/valkey/tests/unit/introspection.tcl",
        ],
        "packet": "tcl-introspection-runner-isolation-v1",
        "recommendation": (
            "Treat the current cat/stdout exception as runner isolation until "
            "reproduced otherwise. Give this file a dedicated tmp dir and only "
            "then cut CLIENT/COMMAND/INFO implementation packets."
        ),
        "product_value": "medium",
        "risk": "medium",
    },
    "unit/keyspace": {
        "root": "Harness tmp-dir/server lifecycle first; then keyspace/expire/SCAN semantics",
        "local_files": [
            "harness/oracle/tcl-survey.py",
            "crates/redis-core/src/db.rs",
            "crates/redis-core/src/expire.rs",
            "crates/redis-commands/src/dispatch.rs",
        ],
        "upstream_files": [
            "reference/valkey/src/db.c",
            "reference/valkey/src/expire.c",
            "reference/valkey/tests/unit/keyspace.tcl",
        ],
        "packet": "tcl-keyspace-runner-isolation-then-scan-expire-v1",
        "recommendation": (
            "First remove the tmp-dir/stdout artifact. If the file then emits a "
            "summary, rank concrete keyspace failures instead of guessing."
        ),
        "product_value": "medium",
        "risk": "medium",
    },
    "unit/geo": {
        "root": "Harness tmp-dir/server lifecycle first; then GEO command edge semantics",
        "local_files": [
            "harness/oracle/tcl-survey.py",
            "crates/redis-commands/src/geo.rs",
            "crates/redis-commands/src/geohash_geohash.rs",
        ],
        "upstream_files": [
            "reference/valkey/src/geo.c",
            "reference/valkey/tests/unit/geo.tcl",
        ],
        "packet": "tcl-geo-runner-isolation-then-edge-fails-v1",
        "recommendation": (
            "Do not rewrite GEO yet. The first observed failure is a runner "
            "artifact; isolate the test tmp dir, rerun, then use counted failures."
        ),
        "product_value": "medium",
        "risk": "low",
    },
    "integration/aof": {
        "root": "AOF durability, check utility compatibility, truncation/corruption repair semantics",
        "local_files": [
            "crates/redis-commands/src/aof.rs",
            "crates/redis-core/src/persistence.rs",
            "harness/oracle/persistence-frontier.py",
            "crates/redis-server/src/bin/valkey-check-aof.rs (new utility target)",
        ],
        "upstream_files": [
            "reference/valkey/src/aof.c",
            "reference/valkey/src/valkey-check-aof.c",
            "reference/valkey/tests/integration/aof.tcl",
        ],
        "packet": "tcl-aof-check-utility-and-corruption-frontier-v1",
        "recommendation": (
            "Add or alias the `valkey-check-aof` utility first; the current abort "
            "is not the server AOF path alone. Then target truncated/unfinished "
            "MULTI repair and logged-error parity."
        ),
        "product_value": "high",
        "risk": "medium",
    },
    "integration/rdb": {
        "root": "RDB integration utility/server launch behavior and bgsave cancel/future-version semantics",
        "local_files": [
            "crates/redis-core/src/rdb/load.rs",
            "crates/redis-core/src/rdb/save.rs",
            "crates/redis-commands/src/persist.rs",
            "harness/oracle/persistence-frontier.py",
        ],
        "upstream_files": [
            "reference/valkey/src/rdb.c",
            "reference/valkey/tests/integration/rdb.tcl",
        ],
        "packet": "tcl-rdb-integration-launch-bgsave-cancel-v1",
        "recommendation": (
            "RDB object oracles are strong; this frontier is process/integration "
            "behavior. Start by making the integration runner launch the right "
            "server binary, then implement bgsave-cancel/future-version edges."
        ),
        "product_value": "high",
        "risk": "medium",
    },
    "unit/type/list": {
        "root": "List quicklist/listpack encoding conversion and blocking-list edges",
        "local_files": [
            "crates/redis-commands/src/list.rs",
            "crates/redis-ds/src/quicklist.rs",
            "crates/redis-ds/src/listpack.rs",
        ],
        "upstream_files": [
            "reference/valkey/src/t_list.c",
            "reference/valkey/src/quicklist.c",
            "reference/valkey/tests/unit/type/list.tcl",
        ],
        "packet": "tcl-list-quicklist-encoding-frontier-v1",
        "recommendation": (
            "This is already counted fail, not hidden. It is a good breadth packet "
            "because the first visible failures name quicklist/listpack conversion "
            "rather than a vague timeout."
        ),
        "product_value": "high",
        "risk": "medium",
    },
    "unit/type/hash": {
        "root": "Hash listpack/ziplist compatibility and HINCRBYFLOAT edge wording",
        "local_files": [
            "crates/redis-commands/src/hash.rs",
            "crates/redis-core/src/rdb/hash.rs",
            "crates/redis-ds/src/listpack.rs",
        ],
        "upstream_files": [
            "reference/valkey/src/t_hash.c",
            "reference/valkey/tests/unit/type/hash.tcl",
        ],
        "packet": "tcl-hash-encoding-and-float-cleanup-v1",
        "recommendation": (
            "A bounded cleanup packet: the file already reaches summary and has "
            "three visible failures, so it is lower ambiguity than scripting."
        ),
        "product_value": "medium",
        "risk": "low",
    },
    "unit/tracking": {
        "root": "CLIENT TRACKING state, invalidation counters, script read tracking",
        "local_files": [
            "crates/redis-core/src/tracking.rs",
            "crates/redis-core/src/command_context.rs",
            "crates/redis-commands/src/connection.rs",
        ],
        "upstream_files": [
            "reference/valkey/src/tracking.c",
            "reference/valkey/tests/unit/tracking.tcl",
        ],
        "packet": "tcl-client-tracking-info-counters-v1",
        "recommendation": (
            "Fix the current no-summary variable gap around tracking info counters, "
            "then decide whether invalidation routing belongs in this wave."
        ),
        "product_value": "medium",
        "risk": "medium",
    },
    "unit/introspection-2": {
        "root": "COMMAND LIST/FILTERBY and command metadata introspection",
        "local_files": [
            "crates/redis-commands/src/connection.rs",
            "crates/redis-commands/src/generated.rs",
            "harness/command-registry.json",
        ],
        "upstream_files": [
            "reference/valkey/src/server.c",
            "reference/valkey/src/commands.c",
            "reference/valkey/tests/unit/introspection-2.tcl",
        ],
        "packet": "tcl-command-list-filterby-v1",
        "recommendation": (
            "Implement the missing COMMAND LIST/FILTERBY subcommand path from the "
            "generated registry before broader introspection polish."
        ),
        "product_value": "medium",
        "risk": "medium",
    },
    "unit/bitops": {
        "root": "Runner/server launch artifact first; then BITOP/BITFIELD edge semantics",
        "local_files": [
            "harness/oracle/tcl-survey.py",
            "crates/redis-commands/src/bitops.rs",
        ],
        "upstream_files": [
            "reference/valkey/src/bitops.c",
            "reference/valkey/tests/unit/bitops.tcl",
        ],
        "packet": "tcl-bitops-runner-isolation-then-edge-fails-v1",
        "recommendation": (
            "Current evidence is a tmp/stdout runner artifact. Isolate the run "
            "before spending implementation time."
        ),
        "product_value": "medium",
        "risk": "low",
    },
    "unit/sort": {
        "root": "SORT/SORT_RO semantics plus runner server-start artifact",
        "local_files": [
            "crates/redis-commands/src/sort.rs",
            "harness/oracle/tcl-survey.py",
        ],
        "upstream_files": [
            "reference/valkey/src/sort.c",
            "reference/valkey/tests/unit/sort.tcl",
        ],
        "packet": "tcl-sort-runner-launch-then-by-get-v1",
        "recommendation": (
            "The current timeout says the harness cannot start the server. Fix that "
            "visibility issue before changing SORT internals."
        ),
        "product_value": "medium",
        "risk": "medium",
    },
    "unit/dump": {
        "root": "DUMP/RESTORE integration runner launch and serialized object edge behavior",
        "local_files": [
            "crates/redis-commands/src/persist.rs",
            "crates/redis-core/src/rdb/mod.rs",
            "harness/oracle/tcl-survey.py",
        ],
        "upstream_files": [
            "reference/valkey/src/dump_payload.c",
            "reference/valkey/tests/unit/dump.tcl",
        ],
        "packet": "tcl-dump-runner-launch-then-restore-edges-v1",
        "recommendation": (
            "RDB object oracles are strong; first remove the test server launch "
            "failure, then focus on DUMP/RESTORE edge semantics."
        ),
        "product_value": "medium",
        "risk": "medium",
    },
    "unit/hyperloglog": {
        "root": "PF* command semantics plus runner server-start artifact",
        "local_files": [
            "crates/redis-commands/src/hyperloglog.rs",
            "harness/oracle/tcl-survey.py",
        ],
        "upstream_files": [
            "reference/valkey/src/hyperloglog.c",
            "reference/valkey/tests/unit/hyperloglog.tcl",
        ],
        "packet": "tcl-hyperloglog-runner-launch-then-pf-edges-v1",
        "recommendation": (
            "Current evidence points at server launch, not HLL math. Treat as a "
            "runner visibility packet first."
        ),
        "product_value": "medium",
        "risk": "medium",
    },
}


def utc_stamp() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def test_from_log_path(path: Path, payload: dict[str, Any]) -> str | None:
    cmd = payload.get("cmd") or []
    if "--single" in cmd:
        idx = cmd.index("--single")
        if idx + 1 < len(cmd):
            return str(cmd[idx + 1])
    stem = path.stem
    if "__" in stem:
        return stem.replace("__", "/")
    return stem


def parse_summary(output: str) -> tuple[int | None, int | None]:
    matches = SUMMARY_RE.findall(strip_ansi(output))
    if not matches:
        return None, None
    passed, failed = matches[-1]
    return int(passed), int(failed)


def parse_failures(output: str, limit: int = 10) -> list[str]:
    failures: list[str] = []
    for line in strip_ansi(output).splitlines():
        match = ERR_RE.match(line)
        if match:
            failures.append(match.group(1).strip())
        if len(failures) >= limit:
            break
    return failures


def parse_exception(output: str) -> str | None:
    for line in strip_ansi(output).splitlines():
        match = EXCEPTION_RE.match(line)
        if match:
            return match.group(1).strip()
    return None


def parse_abort_test(output: str) -> str | None:
    for line in strip_ansi(output).splitlines():
        stripped = line.strip()
        match = TEST_BRACE_RE.match(stripped) or TEST_QUOTE_RE.match(stripped)
        if match:
            return match.group(1).strip()
    return None


def first_failure_name(failures: list[str]) -> str | None:
    if not failures:
        return None
    first = failures[0]
    if " in tests/" in first:
        first = first.split(" in tests/", 1)[0]
    return first.strip() or None


def latest_survey_logs() -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    if not SURVEY_ROOT.exists():
        return latest
    for run_dir in sorted(SURVEY_ROOT.iterdir()):
        if not run_dir.is_dir():
            continue
        for path in sorted(run_dir.glob("*.json")):
            if path.name == "setup.json":
                continue
            try:
                payload = json.loads(path.read_text())
            except json.JSONDecodeError:
                continue
            test = test_from_log_path(path, payload)
            if not test:
                continue
            combined = f"{payload.get('stdout', '')}\n{payload.get('stderr', '')}"
            passed, failed = parse_summary(combined)
            latest[test] = {
                "run_id": run_dir.name,
                "path": str(path.relative_to(ROOT)),
                "passed": passed,
                "failed": failed,
                "timed_out": bool(payload.get("timed_out")),
                "returncode": payload.get("returncode"),
                "elapsed_s": payload.get("elapsed_s"),
                "abort_test": parse_abort_test(combined),
                "exception": parse_exception(combined),
                "failures": parse_failures(combined),
            }
    return latest


def file_status(log: dict[str, Any] | None) -> str:
    if not log:
        return "missing-log"
    if log["timed_out"]:
        return "timeout"
    if log["passed"] == 0 and log["failed"] == 0:
        return "zero-count"
    if log["passed"] is None or log["failed"] is None:
        return "no-summary"
    if log["failed"]:
        return "fail"
    return "pass"


def failure_mode(log: dict[str, Any] | None) -> str:
    if not log:
        return "missing evidence"
    if log["timed_out"]:
        if log["failures"]:
            return "timeout after visible failures"
        return "timeout with no visible failure"
    if log["passed"] == 0 and log["failed"] == 0:
        return "0/0 summary; runner selected no tests under current tag policy"
    if log["passed"] is None or log["failed"] is None:
        exc = log.get("exception") or ""
        if "stdout: No such file" in exc:
            return "no-summary from runner tmp/stdout artifact"
        if log.get("abort_test"):
            return "no-summary abort at named test"
        if exc:
            return "no-summary exception"
        return "no-summary with no parsed frontier"
    if log["failed"]:
        return "counted failures"
    return "passes"


def first_visible(log: dict[str, Any] | None) -> str | None:
    if not log:
        return None
    return log.get("abort_test") or first_failure_name(log.get("failures") or []) or log.get("exception")


def score_entry(test: str, source_tests: int, log: dict[str, Any] | None) -> int:
    meta = SUBSYSTEMS[test]
    value = {"high": 35, "medium": 22, "low": 12}[meta["product_value"]]
    risk_penalty = {"high": 18, "medium": 9, "low": 3}[meta["risk"]]
    status_bonus = 20 if file_status(log) in {"timeout", "no-summary"} else 8
    artifact_bonus = -18 if "runner tmp/stdout artifact" in failure_mode(log) else 0
    return source_tests + value + status_bonus + artifact_bonus - risk_penalty


def build() -> dict[str, Any]:
    inventory = json.loads(INVENTORY.read_text())
    by_test = {item["test"]: item for item in inventory["files"]}
    logs = latest_survey_logs()
    focus_entries: list[dict[str, Any]] = []
    rank_entries: list[dict[str, Any]] = []

    def make_entry(test: str) -> dict[str, Any] | None:
        item = by_test.get(test)
        if not item:
            return None
        log = logs.get(test) or (item.get("latest_log") or {})
        meta = SUBSYSTEMS[test]
        source_tests = int(item["source_tests"])
        status = file_status(log)
        packet = meta["packet"]
        recommendation = meta["recommendation"]
        if status == "pass":
            packet = "none-currently-passing-regression-guard"
            recommendation = (
                "Fresh focused scout reaches a clean summary for this file. "
                "Do not spend implementation time here now; keep it in the "
                "regression inventory."
            )
        return {
            "test": test,
            "file": f"{test}.tcl",
            "source_tests": source_tests,
            "latest_status": status,
            "failure_mode": failure_mode(log),
            "first_visible_failing_test": first_visible(log),
            "latest_log": log,
            "likely_root_subsystem": meta["root"],
            "local_source_files": meta["local_files"],
            "upstream_source_files": meta["upstream_files"],
            "recommended_packet": packet,
            "recommended_action": recommendation,
            "product_value": meta["product_value"],
            "implementation_risk": meta["risk"],
            "rank_score": score_entry(test, source_tests, log),
        }

    for test in FOCUS:
        entry = make_entry(test)
        if entry:
            focus_entries.append(entry)

    for test in dict.fromkeys(FOCUS + ADDITIONAL_RANK_TESTS):
        entry = make_entry(test)
        if not entry:
            continue
        if entry["latest_status"] in {"pass", "zero-count"}:
            continue
        rank_entries.append(entry)

    ranked = sorted(rank_entries, key=lambda item: item["rank_score"], reverse=True)
    full = inventory["full_suite"]
    status_tests = inventory["source_tests_by_status"]
    non_skipped = full["source_tests"] - status_tests.get("skipped-by-policy", 0)
    hidden = status_tests.get("no-summary", 0) + status_tests.get("timeout", 0)
    return {
        "schema_version": 1,
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "inventory": {
            "path": str(INVENTORY.relative_to(ROOT)),
            "full_suite": full,
            "counted_results": inventory["counted_results"],
            "files_by_status": inventory["files_by_status"],
            "source_tests_by_status": inventory["source_tests_by_status"],
            "non_skipped_source_tests": non_skipped,
            "hidden_timeout_or_no_summary_source_tests": hidden,
        },
        "focus": focus_entries,
        "ranked_packets": [
            {
                "rank": idx + 1,
                "packet": item["recommended_packet"],
                "test": item["test"],
                "source_tests": item["source_tests"],
                "rank_score": item["rank_score"],
                "first_visible_failing_test": item["first_visible_failing_test"],
                "likely_root_subsystem": item["likely_root_subsystem"],
                "local_source_files": item["local_source_files"],
                "recommended_action": item["recommended_action"],
                "product_value": item["product_value"],
                "implementation_risk": item["implementation_risk"],
                "latest_status": item["latest_status"],
                "failure_mode": item["failure_mode"],
            }
            for idx, item in enumerate(ranked[:10])
        ],
    }


def pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "n/a"
    return f"{(numerator / denominator) * 100:.1f}%"


def md_escape(value: object) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def write_doc(path: Path, data: dict[str, Any]) -> None:
    inv = data["inventory"]
    full = inv["full_suite"]["source_tests"]
    counted = inv["counted_results"]
    status = inv["source_tests_by_status"]
    non_skipped = inv["non_skipped_source_tests"]
    hidden = inv["hidden_timeout_or_no_summary_source_tests"]

    lines = [
        "# TCL Hidden Frontier - 2026-05-24",
        "",
        f"Generated: `{data['generated_at']}`",
        "",
        "This is an illumination artifact, not a conformance claim. It maps the",
        "timeout/no-summary bucket into concrete subsystem packets so broad",
        "implementation work can start from evidence instead of guessing.",
        "",
        "## Accounting Snapshot",
        "",
        f"- Full upstream TCL denominator: **{full}** source test blocks",
        f"- Counted runner result: **{counted['passed']} pass / {counted['failed']} fail / {counted['total']} counted**",
        f"- Conservative full-suite proof: **{pct(counted['passed'], full)}** counted-pass / full denominator",
        f"- Non-skipped denominator: **{non_skipped}** source test blocks",
        f"- Hidden timeout/no-summary bucket: **{hidden}** source tests (**{pct(hidden, non_skipped)}** of non-skipped)",
        "",
        "| Status | Source tests |",
        "|---|---:|",
    ]
    for key, value in sorted(status.items()):
        lines.append(f"| `{key}` | {value} |")

    lines.extend(
        [
            "",
            "## Focus Files",
            "",
            "| File | Tests | Status | Failure mode | First visible failing test / exception | Likely root | Next packet |",
            "|---|---:|---|---|---|---|---|",
        ]
    )
    for item in data["focus"]:
        lines.append(
            "| `{file}` | {tests} | `{status}` | {mode} | {first} | {root} | `{packet}` |".format(
                file=md_escape(item["file"]),
                tests=item["source_tests"],
                status=item["latest_status"],
                mode=md_escape(item["failure_mode"]),
                first=md_escape(item["first_visible_failing_test"]),
                root=md_escape(item["likely_root_subsystem"]),
                packet=md_escape(item["recommended_packet"]),
            )
        )

    lines.extend(
        [
            "",
            "## Ranked Packet Candidates",
            "",
            "| Rank | Packet | File | Tests | Value | Risk | Why next |",
            "|---:|---|---|---:|---|---|---|",
        ]
    )
    for packet in data["ranked_packets"]:
        lines.append(
            "| {rank} | `{packet}` | `{test}.tcl` | {tests} | {value} | {risk} | {why} |".format(
                rank=packet["rank"],
                packet=md_escape(packet["packet"]),
                test=md_escape(packet["test"]),
                tests=packet["source_tests"],
                value=md_escape(packet["product_value"]),
                risk=md_escape(packet["implementation_risk"]),
                why=md_escape(packet["recommended_action"]),
            )
        )

    lines.extend(
        [
            "",
            "## Per-File Notes",
            "",
        ]
    )
    for item in data["focus"]:
        lines.extend(
            [
                f"### `{item['file']}`",
                "",
                f"- Source tests hidden/covered by this file: **{item['source_tests']}**",
                f"- Latest status: `{item['latest_status']}` ({item['failure_mode']})",
                f"- First visible failing test: `{item['first_visible_failing_test']}`",
                f"- Recommended packet: `{item['recommended_packet']}`",
                f"- Recommended action: {item['recommended_action']}",
                f"- Likely root subsystem: {item['likely_root_subsystem']}",
                f"- Latest log: `{(item['latest_log'] or {}).get('path', '')}`",
                "- Local source files:",
            ]
        )
        lines.extend(f"  - `{path}`" for path in item["local_source_files"])
        lines.append("- Upstream source anchors:")
        lines.extend(f"  - `{path}`" for path in item["upstream_source_files"])
        failures = (item["latest_log"] or {}).get("failures") or []
        if failures:
            lines.append("- First parsed failures:")
            lines.extend(f"  - {failure}" for failure in failures[:5])
        exception = (item["latest_log"] or {}).get("exception")
        if exception:
            lines.append(f"- Parsed exception: `{exception}`")
        lines.append("")

    lines.extend(
        [
            "## Operating Guidance",
            "",
            "1. Do not spend the next long run on per-test wording fixes. The hidden bucket",
            "   is dominated by subsystem aborts and timeouts.",
            "2. Run one large subsystem packet at a time for scripting/functions/streams.",
            "   Those overlap enough that parallel edits will corrupt interpretation.",
            "3. Runner-artifact frontiers (`cat .../stdout`) should be fixed as harness",
            "   isolation first, not treated as command failures.",
            "4. Persistence integration frontiers are product-critical even though the",
            "   source-test count is smaller; they should run in parallel with scripting",
            "   only if the worktree is isolated.",
            "",
            "## Reproduction",
            "",
            "```bash",
            "python3 harness/oracle/tcl-suite-inventory.py",
            "python3 harness/oracle/tcl-survey.py --runner-id tcl-hidden-frontier-20260524 \\",
            "  --skip-build --timeout-s 75 \\",
            "  --files unit/scripting,unit/functions,unit/multi,unit/pubsub,unit/type/stream,unit/type/stream-cgroups,unit/introspection,unit/keyspace,unit/geo,integration/aof,integration/rdb",
            "python3 harness/oracle/tcl-frontier-map.py",
            "```",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=str(OUT_ROOT))
    parser.add_argument("--doc", default=str(DOC_PATH))
    parser.add_argument("--stamp", default=None)
    args = parser.parse_args()

    data = build()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = args.stamp or utc_stamp()
    stamped = out_dir / f"{stamp}.json"
    latest = out_dir / "latest.json"
    text = json.dumps(data, indent=2, sort_keys=True) + "\n"
    stamped.write_text(text, encoding="utf-8")
    latest.write_text(text, encoding="utf-8")
    write_doc(Path(args.doc), data)
    print(json.dumps({
        "schema_version": 1,
        "status": "pass",
        "json": str(stamped.relative_to(ROOT)),
        "latest_json": str(latest.relative_to(ROOT)),
        "doc": str(Path(args.doc).relative_to(ROOT)),
        "ranked_packets": [item["packet"] for item in data["ranked_packets"]],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
