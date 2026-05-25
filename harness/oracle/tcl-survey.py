#!/usr/bin/env python3
"""Survey Valkey TCL files with per-file timeouts and typed runner output.

This runner is intentionally telemetry-only. Its job is to safely widen
official-suite visibility, preserve raw logs, and produce packet candidates.
It does not mark a file as a public conformance claim just because it ran.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
REFERENCE = ROOT / "reference" / "valkey"
RESULTS_ROOT = ROOT / "harness" / "oracle" / "results" / "tcl-survey"
DEFAULT_FILES = [
    "unit/bitops",
    "unit/bitfield",
    "unit/geo",
    "unit/hyperloglog",
    "unit/scripting",
    "unit/scan",
    "unit/sort",
    "unit/dump",
    "unit/info",
    "unit/slowlog",
]
DEFAULT_DENY_TAGS = [
    "needs:repl",
    "needs:debug",
    "external:skip",
]


ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
SUMMARY_RE = re.compile(r"Test Summary:\s+(\d+)\s+passed,\s+(\d+)\s+failed")
ERR_RE = re.compile(r"^\s*(?:\*+\s*)?\[err\]:\s*(.*)$")
EXCEPTION_RE = re.compile(r"^\[exception\]:\s+Executing test client:\s+(.*)$")
TEST_BRACE_RE = re.compile(r'^"test \{([^}]+)\}')
TEST_QUOTE_RE = re.compile(r'^"test "([^"]+)"')


def utc_stamp() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")


def run_process(
    cmd: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    timeout_s: int,
) -> dict[str, Any]:
    started = time.monotonic()
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    timed_out = False
    try:
        stdout, stderr = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            stdout, stderr = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            stdout, stderr = proc.communicate()
    elapsed_s = time.monotonic() - started
    return {
        "cmd": cmd,
        "cwd": str(cwd),
        "returncode": proc.returncode,
        "timed_out": timed_out,
        "elapsed_s": elapsed_s,
        "stdout": stdout,
        "stderr": stderr,
    }


def parse_files(raw: str | None) -> list[str]:
    if not raw:
        return DEFAULT_FILES
    items = []
    for part in raw.split(","):
        item = part.strip()
        if item:
            items.append(item)
    return items


def parse_summary(output: str) -> tuple[int | None, int | None]:
    matches = SUMMARY_RE.findall(ANSI_RE.sub("", output))
    if not matches:
        return None, None
    passed, failed = matches[-1]
    return int(passed), int(failed)


def parse_failures(output: str, limit: int = 40) -> list[str]:
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


def write_log(path: Path, result: dict[str, Any]) -> None:
    payload = {
        "cmd": result["cmd"],
        "cwd": result["cwd"],
        "returncode": result["returncode"],
        "timed_out": result["timed_out"],
        "elapsed_s": result["elapsed_s"],
        "stdout": result["stdout"],
        "stderr": result["stderr"],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def tcl_command(test_file: str, args: argparse.Namespace) -> list[str]:
    cmd = [
        "tclsh",
        "tests/test_helper.tcl",
        "--single",
        test_file,
        "--clients",
        str(args.clients),
        "--skip-leaks",
    ]
    if args.baseport is not None:
        cmd.extend(["--baseport", str(args.baseport)])
    if args.portcount is not None:
        cmd.extend(["--portcount", str(args.portcount)])
    if args.deny_tags:
        cmd.extend(["--tags", " ".join(f"-{tag}" for tag in args.deny_tags)])
    if args.quiet:
        cmd.append("--quiet")
    return cmd


def setup_runner(args: argparse.Namespace) -> dict[str, Any]:
    cmd = ["bash", "harness/oracle/setup_tcl_runner.sh"]
    if args.skip_build:
        cmd.append("--skip-build")
    return run_process(cmd, cwd=ROOT, timeout_s=args.setup_timeout_s)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--runner-id",
        default="tcl-survey-unswept",
        help="Runner id to report in RunnerResult JSON. Defaults to the legacy unswept runner id.",
    )
    parser.add_argument("--files", help="Comma-separated TCL file list; default is the unswept survey set.")
    parser.add_argument("--timeout-s", type=int, default=90, help="Per-file timeout.")
    parser.add_argument("--setup-timeout-s", type=int, default=300, help="Build/symlink setup timeout.")
    parser.add_argument("--clients", type=int, default=1)
    parser.add_argument("--baseport", type=int, help="Initial port number for spawned Valkey servers.")
    parser.add_argument("--portcount", type=int, help="Port range for spawned Valkey servers.")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--quiet", action="store_true", default=True)
    parser.add_argument(
        "--deny-tag",
        action="append",
        dest="extra_deny_tags",
        default=[],
        help="Additional TCL tag to deny. Repeatable.",
    )
    parser.add_argument(
        "--deny-tags",
        help="Comma-separated extra TCL tags to deny.",
    )
    parser.add_argument(
        "--no-default-deny-tags",
        action="store_true",
        help="Do not apply the default needs:repl/needs:debug/external:skip deny policy.",
    )
    args = parser.parse_args()
    deny_tags = [] if args.no_default_deny_tags else list(DEFAULT_DENY_TAGS)
    deny_tags.extend(args.extra_deny_tags)
    if args.deny_tags:
        deny_tags.extend(tag.strip() for tag in args.deny_tags.split(",") if tag.strip())
    args.deny_tags = deny_tags

    files = parse_files(args.files)
    if not files:
        raise SystemExit("no TCL files selected")

    run_id = utc_stamp()
    run_dir = RESULTS_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    setup = setup_runner(args)
    write_log(run_dir / "setup.json", setup)
    if setup["returncode"] != 0 or setup["timed_out"]:
        result = {
            "schema_version": 1,
            "runner_id": args.runner_id,
            "status": "error",
            "surface": "correctness",
            "method": "official-suite",
            "summary": "TCL survey setup failed",
            "claim_level": "telemetry",
            "measurements": [
                {
                    "kind": "official",
                    "name": "tcl-survey-setup",
                    "metric": "tcl_survey_setup_pass",
                    "target": "rust-vs-reference",
                    "numerator": 0,
                    "denominator": 1,
                }
            ],
            "artifacts": [{"kind": "tcl-survey-log", "path": str((run_dir / "setup.json").relative_to(ROOT))}],
            "evidence": {
                "kind": "tcl_survey",
                "run_id": run_id,
                "setup": {key: setup[key] for key in ("cmd", "returncode", "timed_out", "elapsed_s")},
            },
        }
        print(json.dumps(result, sort_keys=True))
        return 0

    env = os.environ.copy()
    env["VALKEY_BIN_DIR"] = str(ROOT / "target" / "debug")

    file_results: list[dict[str, Any]] = []
    artifacts = [{"kind": "tcl-survey-log", "path": str((run_dir / "setup.json").relative_to(ROOT))}]
    measurements: list[dict[str, Any]] = []

    for test_file in files:
        safe_name = test_file.replace("/", "__")
        cmd = tcl_command(test_file, args)
        proc = run_process(cmd, cwd=REFERENCE, env=env, timeout_s=args.timeout_s)
        log_path = run_dir / f"{safe_name}.json"
        write_log(log_path, proc)
        artifacts.append({"kind": "tcl-survey-log", "path": str(log_path.relative_to(ROOT)), "test": test_file})

        combined = f"{proc['stdout']}\n{proc['stderr']}"
        passed, failed = parse_summary(combined)
        failures = parse_failures(combined)
        exception = parse_exception(combined)
        abort_test = parse_abort_test(combined)
        total = (passed or 0) + (failed or 0)
        completed = not proc["timed_out"] and proc["returncode"] is not None
        file_result = {
            "test": test_file,
            "passed": passed,
            "failed": failed,
            "total": total,
            "returncode": proc["returncode"],
            "timed_out": proc["timed_out"],
            "elapsed_s": proc["elapsed_s"],
            "failures": failures,
            "exception": exception,
            "abort_test": abort_test,
            "log": str(log_path.relative_to(ROOT)),
        }
        file_results.append(file_result)

        identity = {
            "kind": "official",
            "name": test_file,
            "target": "rust-vs-reference",
            "capability": "official-tcl-coverage",
            "test": test_file,
        }
        measurements.append(
            {
                **identity,
                "metric": "tcl_file_completed",
                "numerator": 1 if completed else 0,
                "denominator": 1,
            }
        )
        measurements.append(
            {
                **identity,
                "metric": "tcl_file_timeout",
                "numerator": 1 if proc["timed_out"] else 0,
                "denominator": 1,
            }
        )
        measurements.append(
            {
                **identity,
                "metric": "tcl_file_no_summary",
                "numerator": 1 if passed is None or failed is None else 0,
                "denominator": 1,
            }
        )
        if passed is not None and failed is not None:
            measurements.extend(
                [
                    {**identity, "metric": "tcl_pass_count", "value": passed, "unit": "tests"},
                    {**identity, "metric": "tcl_fail_count", "value": failed, "unit": "tests"},
                    {**identity, "metric": "tcl_total_count", "value": total, "unit": "tests"},
                ]
            )
            if total > 0:
                measurements.append(
                    {
                        **identity,
                        "metric": "tcl_file_pass_ratio",
                        "numerator": passed,
                        "denominator": total,
                        "value": passed / total,
                        "unit": "pass/total",
                    }
                )

    total_passed = sum(item["passed"] or 0 for item in file_results)
    total_failed = sum(item["failed"] or 0 for item in file_results)
    timed_out = sum(1 for item in file_results if item["timed_out"])
    no_summary = sum(1 for item in file_results if item["passed"] is None or item["failed"] is None)
    summary = (
        f"TCL survey: {len(file_results)} files, {total_passed} passed tests, "
        f"{total_failed} failed tests, {timed_out} timed out, {no_summary} without summary"
    )

    result = {
        "schema_version": 1,
        "runner_id": args.runner_id,
        "status": "pass",
        "surface": "correctness",
        "method": "official-suite",
        "summary": summary,
        "claim_level": "telemetry",
        "measurements": measurements,
        "artifacts": artifacts,
        "evidence": {
            "kind": "tcl_survey",
            "run_id": run_id,
            "files": file_results,
            "deny_tags": args.deny_tags,
            "clients": args.clients,
            "timeout_s": args.timeout_s,
            "setup": {key: setup[key] for key in ("cmd", "returncode", "timed_out", "elapsed_s")},
        },
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
