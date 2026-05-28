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
import shutil
import subprocess
import sys
import tempfile
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
DENY_TAG_PROFILES = {
    "default": DEFAULT_DENY_TAGS,
    # Many upstream unit files are tagged external:skip because they spawn or
    # reconfigure local servers. For single-node coverage expansion, keep
    # replication/debug/cluster out while allowing those local-server tests to
    # illuminate real behavior.
    "single-node-external": [
        "needs:repl",
        "repl",
        "needs:debug",
        "cluster",
        "needs:cluster",
    ],
    # Counts the single-server `attach_to_replication_stream` tests (a fake
    # replica attaches via inline SYNC; no real replica needed). These are
    # tagged `needs:repl` and so are hidden by the default profile, even though
    # they exercise command-propagation correctness on one node. The real
    # dual-server replica tests additionally carry `external:skip`, so denying
    # that (plus debug/cluster) keeps them out while letting the propagation
    # assertions illuminate. ~17 files / 77 `assert_replication_stream` sites.
    "single-node-repl": [
        "needs:debug",
        "external:skip",
        "cluster",
        "needs:cluster",
    ],
}


ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
SUMMARY_RE = re.compile(r"Test Summary:\s+(\d+)\s+passed,\s+(\d+)\s+failed")
ERR_RE = re.compile(r"^\s*(?:\*+\s*)?\[err\]:\s*(.*)$")
EXCEPTION_RE = re.compile(r"^\[exception\]:\s+Executing test client:\s+(.*)$")
TEST_BRACE_RE = re.compile(r'^"test \{([^}]+)\}')
TEST_QUOTE_RE = re.compile(r'^"test "([^"]+)"')


def utc_stamp() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%S%fZ")


def unique_run_dir(run_id: str) -> tuple[str, Path]:
    run_dir = RESULTS_ROOT / run_id
    if not run_dir.exists():
        return run_id, run_dir
    unique_id = f"{run_id}-{os.getpid()}"
    return unique_id, RESULTS_ROOT / unique_id


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
    if getattr(args, "tls", False):
        # test_helper --tls generates certs (tests/tls/gen-test-certs.sh) and
        # starts each server with tls-port/tls-cert-file/etc.; our build_tls_startup
        # honors those. Required for the `if {$::tls}`-gated unit/tls.tcl.
        cmd.append("--tls")
    if args.quiet:
        cmd.append("--quiet")
    return cmd


def prepare_isolated_reference(tmp_root: Path) -> Path:
    """Create a minimal Valkey tree with a private tests/tmp directory."""
    isolated = tmp_root / "valkey"
    isolated.mkdir(parents=True, exist_ok=False)
    shutil.copytree(REFERENCE / "tests", isolated / "tests", symlinks=True)
    # Some upstream support helpers reference [pwd]/src for valgrind/TLS/module
    # helper paths. VALKEY_BIN_DIR still points at the Rust binary directory.
    (isolated / "src").symlink_to(REFERENCE / "src")
    return isolated


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
    parser.add_argument(
        "--isolated-tests-copy",
        action="store_true",
        help=(
            "Run against a per-process copy of reference/valkey/tests so "
            "concurrent TCL probes do not race over tests/tmp."
        ),
    )
    parser.add_argument("--quiet", action="store_true", default=True)
    parser.add_argument(
        "--profile",
        choices=sorted(DENY_TAG_PROFILES),
        default="default",
        help="Named deny-tag profile. default preserves legacy behavior; single-node-external allows external:skip local-server tests while denying repl/debug/cluster.",
    )
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
    parser.add_argument(
        "--tls",
        action="store_true",
        help="Run test_helper in TLS mode: generate certs and start servers with "
        "tls-port/tls-cert-file/etc. Required for the if {$::tls}-gated unit/tls.tcl.",
    )
    parser.add_argument(
        "--tier",
        choices=("all", "fast"),
        default="all",
        help=(
            "all (default): every requested file. fast: exclude files listed in "
            "harness/oracle/SLOW_FILES.txt (files that take ≥2s in baseline "
            "because they exercise time-based behavior — pause windows, expiry, "
            "AOF rewrite, function loading). On the canonical 54-file single-node "
            "sweep: fast tier is 21 files in ~14s vs 11min full (~47× faster). "
            "Use fast for between-refactor gates; use all for end-of-wave + nightly. "
            "Ignored when --files is set."
        ),
    )
    args = parser.parse_args()
    deny_tags = [] if args.no_default_deny_tags else list(DENY_TAG_PROFILES[args.profile])
    deny_tags.extend(args.extra_deny_tags)
    if args.deny_tags:
        deny_tags.extend(tag.strip() for tag in args.deny_tags.split(",") if tag.strip())
    args.deny_tags = deny_tags

    files = parse_files(args.files)
    if not files:
        raise SystemExit("no TCL files selected")

    if args.tier == "fast" and not args.files:
        slow_path = Path(__file__).parent / "SLOW_FILES.txt"
        if slow_path.exists():
            slow = set()
            for line in slow_path.read_text().splitlines():
                stripped = line.split("#", 1)[0].strip()
                if stripped:
                    slow.add(stripped)
            before = len(files)
            files = [f for f in files if f not in slow]
            skipped = before - len(files)
            print(
                f"==> tier=fast: skipping {skipped} files listed in "
                f"harness/oracle/SLOW_FILES.txt ({before} -> {len(files)} files)",
                flush=True,
            )

    run_id, run_dir = unique_run_dir(utc_stamp())
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
                "profile": args.profile,
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
    isolated_root: str | None = None

    tmp_parent = Path(os.environ.get("VALKEY_RS_TCL_TMPDIR", "/tmp"))
    tmp_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f"tcl-survey-{run_id}-", dir=tmp_parent) as raw_tmp:
        reference_cwd = REFERENCE
        if args.isolated_tests_copy:
            reference_cwd = prepare_isolated_reference(Path(raw_tmp))
            isolated_root = str(reference_cwd)

        if getattr(args, "tls", False):
            # Generate the test cert suite (tests/tls/{ca,server,client}.{crt,key},
            # valkey.dh, …) that test_helper --tls and the spawned servers expect.
            # The upstream generator lives in utils/, writing to ./tests/tls.
            gen = REFERENCE / "utils" / "gen-test-certs.sh"
            cert_proc = run_process(
                ["bash", str(gen)],
                cwd=reference_cwd,
                timeout_s=args.setup_timeout_s,
            )
            if cert_proc.get("returncode") not in (0, None):
                print(
                    f"warning: gen-test-certs.sh exited {cert_proc.get('returncode')}",
                    file=sys.stderr,
                )

        for test_file in files:
            safe_name = test_file.replace("/", "__")
            cmd = tcl_command(test_file, args)
            proc = run_process(cmd, cwd=reference_cwd, env=env, timeout_s=args.timeout_s)
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
            "profile": args.profile,
            "isolated_tests_copy": args.isolated_tests_copy,
            "isolated_reference_root": isolated_root,
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
