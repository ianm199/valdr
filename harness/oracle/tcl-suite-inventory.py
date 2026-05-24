#!/usr/bin/env python3
"""Generate a full upstream TCL-suite accounting inventory.

This is an accounting tool, not a test runner. It scans every upstream
`.tcl` file, counts source `test` blocks, merges the latest per-file
`tcl-survey.py` result when one exists, and classifies every file as one of:

- pass
- fail
- timeout
- no-summary
- skipped-by-policy

The intent is to keep the full upstream denominator visible while focused
frontier runners continue to provide fast feedback.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
TESTS_ROOT = ROOT / "reference" / "valkey" / "tests"
SURVEY_ROOT = ROOT / "harness" / "oracle" / "results" / "tcl-survey"
OUT_ROOT = ROOT / "harness" / "oracle" / "results" / "tcl-suite-inventory"

TEST_RE = re.compile(r'^\s*test\s+(?:\{|")')
SUMMARY_RE = re.compile(r"Test Summary:\s+(\d+)\s+passed,\s+(\d+)\s+failed")
ERR_RE = re.compile(r"^\s*(?:\*+\s*)?\[err\]:\s*(.*)$")
EXCEPTION_RE = re.compile(r"^\[exception\]:\s+Executing test client:\s+(.*)$")
TEST_BRACE_RE = re.compile(r'^"test \{([^}]+)\}')
TEST_QUOTE_RE = re.compile(r'^"test "([^"]+)"')
ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def utc_stamp() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")


def test_name_from_log(path: Path, payload: dict[str, Any]) -> str | None:
    cmd = payload.get("cmd") or []
    if "--single" in cmd:
        idx = cmd.index("--single")
        if idx + 1 < len(cmd):
            return str(cmd[idx + 1])
    name = path.stem
    if name.startswith("unit__"):
        return name.replace("__", "/")
    return None


def parse_summary(output: str) -> tuple[int | None, int | None]:
    matches = SUMMARY_RE.findall(output)
    if not matches:
        return None, None
    passed, failed = matches[-1]
    return int(passed), int(failed)


def parse_failures(output: str, limit: int = 5) -> list[str]:
    failures: list[str] = []
    for line in ANSI_RE.sub("", output).splitlines():
        match = ERR_RE.match(line)
        if match:
            failures.append(match.group(1).strip())
        if len(failures) >= limit:
            break
    return failures


def parse_exception(output: str) -> str | None:
    for line in ANSI_RE.sub("", output).splitlines():
        match = EXCEPTION_RE.match(line)
        if match:
            return match.group(1).strip()
    return None


def parse_abort_test(output: str) -> str | None:
    for line in ANSI_RE.sub("", output).splitlines():
        stripped = line.strip()
        match = TEST_BRACE_RE.match(stripped) or TEST_QUOTE_RE.match(stripped)
        if match:
            return match.group(1).strip()
    return None


def count_tests(path: Path) -> int:
    total = 0
    for line in path.read_text(errors="replace").splitlines():
        if TEST_RE.match(line):
            total += 1
    return total


def bucket_for(rel: str) -> str:
    parts = rel.split("/")
    if len(parts) >= 2 and parts[0] == "unit" and parts[1] in {"type", "cluster", "moduleapi"}:
        return f"{parts[0]}/{parts[1]}"
    return parts[0]


def policy_reason(rel: str) -> str:
    if rel.startswith("helpers/") or rel.startswith("support/"):
        return "upstream harness helper, not a direct product conformance file"
    if rel in {"instances.tcl", "test_helper.tcl"}:
        return "upstream harness entry/helper file, not a direct product conformance file"
    if rel.startswith("sentinel/"):
        return "Sentinel is not implemented in the current product claim"
    if rel.startswith("integration/"):
        return "integration runner coverage is pending full-suite expansion"
    if rel.startswith("unit/cluster/"):
        return "cluster is not implemented in the current product claim"
    if rel.startswith("unit/moduleapi/"):
        return "loadable module C ABI is not exposed by this port"
    if rel in {"unit/tls.tcl", "unit/io-threads.tcl", "unit/mptcp.tcl"}:
        return "infrastructure feature deferred from the current product claim"
    return "not yet swept by a generated full-suite runner"


def latest_survey_logs() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not SURVEY_ROOT.exists():
        return out
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
            test_name = test_name_from_log(path, payload)
            if not test_name:
                continue
            combined = f"{payload.get('stdout', '')}\n{payload.get('stderr', '')}"
            passed, failed = parse_summary(combined)
            if payload.get("timed_out"):
                status = "timeout"
            elif passed is None or failed is None:
                status = "no-summary"
            elif failed:
                status = "fail"
            else:
                status = "pass"
            out[test_name] = {
                "status": status,
                "passed": passed,
                "failed": failed,
                "timed_out": bool(payload.get("timed_out")),
                "returncode": payload.get("returncode"),
                "elapsed_s": payload.get("elapsed_s"),
                "abort_test": parse_abort_test(combined),
                "exception": parse_exception(combined),
                "failures": parse_failures(combined),
                "log": str(path.relative_to(ROOT)),
                "run_id": run_dir.name,
            }
    return out


def build_inventory() -> dict[str, Any]:
    logs = latest_survey_logs()
    files = []
    for path in sorted(TESTS_ROOT.rglob("*.tcl")):
        rel_file = path.relative_to(TESTS_ROOT).as_posix()
        test_name = rel_file[:-4]
        source_tests = count_tests(path)
        log = logs.get(test_name)
        if log:
            status = log["status"]
            reason = None
            passed = log["passed"]
            failed = log["failed"]
        else:
            status = "skipped-by-policy"
            reason = policy_reason(rel_file)
            passed = None
            failed = None
        files.append(
            {
                "file": rel_file,
                "test": test_name,
                "bucket": bucket_for(rel_file),
                "source_tests": source_tests,
                "status": status,
                "passed": passed,
                "failed": failed,
                "reason": reason,
                "latest_log": log,
            }
        )

    by_status = Counter(item["status"] for item in files)
    tests_by_status = Counter()
    for item in files:
        tests_by_status[item["status"]] += item["source_tests"]
    buckets: dict[str, Counter[str]] = defaultdict(Counter)
    bucket_tests: dict[str, Counter[str]] = defaultdict(Counter)
    for item in files:
        buckets[item["bucket"]][item["status"]] += 1
        bucket_tests[item["bucket"]][item["status"]] += item["source_tests"]

    counted_pass = sum(item["passed"] or 0 for item in files)
    counted_fail = sum(item["failed"] or 0 for item in files)
    return {
        "schema_version": 1,
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "full_suite": {
            "files": len(files),
            "source_tests": sum(item["source_tests"] for item in files),
        },
        "counted_results": {
            "passed": counted_pass,
            "failed": counted_fail,
            "total": counted_pass + counted_fail,
        },
        "files_by_status": dict(sorted(by_status.items())),
        "source_tests_by_status": dict(sorted(tests_by_status.items())),
        "buckets": {
            name: {
                "files_by_status": dict(sorted(counter.items())),
                "source_tests_by_status": dict(sorted(bucket_tests[name].items())),
            }
            for name, counter in sorted(buckets.items())
        },
        "files": files,
    }


def bar(numerator: int, denominator: int, width: int = 30) -> str:
    if denominator <= 0:
        return "." * width
    filled = round((numerator / denominator) * width)
    return "#" * filled + "." * (width - filled)


def md_escape(value: object) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def write_markdown(path: Path, inventory: dict[str, Any]) -> None:
    full = inventory["full_suite"]
    counted = inventory["counted_results"]
    lines = [
        "# TCL Suite Inventory",
        "",
        f"Generated: `{inventory['generated_at']}`",
        "",
        "This is a full-denominator accounting snapshot. It merges latest",
        "`tcl-survey.py` results with every upstream `.tcl` file and marks",
        "unrun files as `skipped-by-policy` with an explicit reason.",
        "",
        "## Summary",
        "",
        f"- Full upstream inventory: {full['files']} files / {full['source_tests']} source test blocks",
        f"- Counted survey results: {counted['passed']} pass / {counted['failed']} fail / {counted['total']} counted",
        f"- Counted coverage bar: `{bar(counted['total'], full['source_tests'])}` {counted['total']}/{full['source_tests']}",
        "",
        "## Status By File",
        "",
        "| Status | Files | Source tests |",
        "|---|---:|---:|",
    ]
    for status, files in inventory["files_by_status"].items():
        tests = inventory["source_tests_by_status"].get(status, 0)
        lines.append(f"| `{status}` | {files} | {tests} |")

    lines.extend(
        [
            "",
            "## Buckets",
            "",
            "| Bucket | Files | Source tests | Status mix |",
            "|---|---:|---:|---|",
        ]
    )
    for bucket_name, bucket in inventory["buckets"].items():
        file_mix = bucket["files_by_status"]
        test_mix = bucket["source_tests_by_status"]
        files = sum(file_mix.values())
        tests = sum(test_mix.values())
        mix = ", ".join(f"{k}:{v}" for k, v in file_mix.items())
        lines.append(f"| `{bucket_name}` | {files} | {tests} | {md_escape(mix)} |")

    lines.extend(
        [
            "",
            "## Files",
            "",
            "| File | Tests | Status | Pass | Fail | Reason / frontier | Latest log |",
            "|---|---:|---|---:|---:|---|---|",
        ]
    )
    for item in inventory["files"]:
        log = item.get("latest_log") or {}
        reason = item.get("reason") or log.get("abort_test") or log.get("exception") or ""
        latest_log = log.get("log", "")
        lines.append(
            "| `{file}` | {tests} | `{status}` | {passed} | {failed} | {reason} | {log} |".format(
                file=md_escape(item["file"]),
                tests=item["source_tests"],
                status=item["status"],
                passed="" if item["passed"] is None else item["passed"],
                failed="" if item["failed"] is None else item["failed"],
                reason=md_escape(reason),
                log=md_escape(latest_log),
            )
        )

    path.write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=str(OUT_ROOT))
    parser.add_argument("--stamp", default=None)
    args = parser.parse_args()

    stamp = args.stamp or utc_stamp()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    inventory = build_inventory()
    json_path = out_dir / f"{stamp}.json"
    md_path = out_dir / f"{stamp}.md"
    latest_json = out_dir / "latest.json"
    latest_md = out_dir / "latest.md"

    json_text = json.dumps(inventory, indent=2, sort_keys=True) + "\n"
    json_path.write_text(json_text)
    latest_json.write_text(json_text)
    write_markdown(md_path, inventory)
    write_markdown(latest_md, inventory)

    print(json.dumps({
        "schema_version": 1,
        "status": "pass",
        "json": str(json_path.relative_to(ROOT)),
        "markdown": str(md_path.relative_to(ROOT)),
        "latest_json": str(latest_json.relative_to(ROOT)),
        "latest_markdown": str(latest_md.relative_to(ROOT)),
        "summary": inventory["full_suite"],
        "counted_results": inventory["counted_results"],
        "files_by_status": inventory["files_by_status"],
        "source_tests_by_status": inventory["source_tests_by_status"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
