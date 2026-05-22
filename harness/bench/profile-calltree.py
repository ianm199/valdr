#!/usr/bin/env python3
"""Capture raw call-tree/flamegraph-style profile artifacts for valkey-rs.

This runner keeps the same benchmark envelope as the existing performance
runners: upstream Valkey and valkey-rs are started on loopback, driven by the
pinned `valkey-benchmark`, and compared as telemetry only. The only added work
is attaching an OS profiler to the Rust server process while the workload runs.

Artifacts:
  harness/bench/results/<ts>-<sha>-calltree.tsv
  harness/bench/results/<ts>-<sha>-calltree.json
  harness/bench/profiles/<ts>-<sha>-calltree/<workload>/*
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
VALKEY_BIN = ROOT / "reference/valkey/src/valkey-server"
VALKEY_BENCH = ROOT / "reference/valkey/src/valkey-benchmark"
RUST_BIN = ROOT / "target/release/redis-server"
RESULTS_DIR = ROOT / "harness/bench/results"
PROFILES_DIR = ROOT / "harness/bench/profiles"
MACOS_SAMPLE = Path("/usr/bin/sample")


class Target(Enum):
    REFERENCE = "reference"
    RUST = "rust"


class Suite(Enum):
    SMOKE = "smoke"
    BIG = "big"


@dataclass(frozen=True)
class Workload:
    name: str
    command: str
    requests: int
    clients: int
    pipeline: int
    payload: int


@dataclass(frozen=True)
class BenchmarkRow:
    workload: str
    target: str
    command: str
    requests: int
    clients: int
    pipeline: int
    payload: int
    rps: float
    avg_ms: float
    min_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


@dataclass(frozen=True)
class ProfilerChoice:
    name: str
    path: str
    version: str
    note: str


def run_text(cmd: list[str], timeout: int = 10) -> str:
    try:
        return subprocess.check_output(
            cmd,
            cwd=ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return ""


def git_commit() -> str:
    return run_text(["git", "rev-parse", "--short", "HEAD"]) or "unknown"


def git_status_short() -> list[str]:
    out = run_text(["git", "status", "--short"])
    return [line for line in out.splitlines() if line]


def hardware_fingerprint() -> dict[str, str]:
    cpu = run_text(["sysctl", "-n", "machdep.cpu.brand_string"])
    if not cpu and Path("/proc/cpuinfo").exists():
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    return {
        "os": run_text(["uname", "-sr"]) or platform.platform(),
        "arch": run_text(["uname", "-m"]) or platform.machine() or "unknown",
        "cpu": cpu or platform.processor() or "unknown",
    }


def relative(path: Path) -> str:
    return str(path.relative_to(ROOT))


def require_binaries() -> None:
    if not VALKEY_BIN.exists() or not VALKEY_BENCH.exists():
        subprocess.run(["bash", "scripts/setup-reference.sh"], cwd=ROOT, check=True)
    if os.environ.get("VALKEY_BENCH_SKIP_BUILD") != "1" or not RUST_BIN.exists():
        subprocess.run(["cargo", "build", "--release", "-p", "redis-server"], cwd=ROOT, check=True)
    missing = [path for path in [VALKEY_BIN, VALKEY_BENCH, RUST_BIN] if not os.access(path, os.X_OK)]
    if missing:
        raise RuntimeError(f"missing executable benchmark dependency: {missing}")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_port(port: int, deadline_s: float = 8.0) -> None:
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"server did not listen on 127.0.0.1:{port}")


def start_server(target: Target, port: int, log_dir: Path) -> subprocess.Popen[str]:
    if target is Target.REFERENCE:
        cmd = [
            str(VALKEY_BIN),
            "--port",
            str(port),
            "--bind",
            "127.0.0.1",
            "--save",
            "",
            "--appendonly",
            "no",
            "--daemonize",
            "no",
            "--loglevel",
            "warning",
        ]
    else:
        cmd = [
            str(RUST_BIN),
            "--port",
            str(port),
            "--bind",
            "127.0.0.1",
            "--rdb-disabled",
            "--appendonly",
            "no",
        ]

    log_path = log_dir / f"{target.value}-{port}.server.log"
    log = log_path.open("w", encoding="utf-8")
    try:
        proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=log, text=True)
    finally:
        log.close()
    wait_for_port(port)
    return proc


def stop_server(proc: subprocess.Popen[str] | None) -> None:
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def parse_benchmark_csv(stdout: str, workload: Workload, target: Target) -> BenchmarkRow:
    reader = csv.reader(stdout.splitlines())
    header = next(reader, None)
    if not header:
        raise RuntimeError("valkey-benchmark emitted no CSV header")
    row = next(reader, None)
    if row is None or len(row) < 8:
        raise RuntimeError(f"valkey-benchmark emitted no parseable row: {stdout[-500:]}")
    return BenchmarkRow(
        workload=workload.name,
        target=target.value,
        command=row[0],
        requests=workload.requests,
        clients=workload.clients,
        pipeline=workload.pipeline,
        payload=workload.payload,
        rps=float(row[1]),
        avg_ms=float(row[2]),
        min_ms=float(row[3]),
        p50_ms=float(row[4]),
        p95_ms=float(row[5]),
        p99_ms=float(row[6]),
        max_ms=float(row[7]),
    )


def run_benchmark(target: Target, workload: Workload, log_dir: Path) -> BenchmarkRow:
    port = free_port()
    proc: subprocess.Popen[str] | None = None
    try:
        proc = start_server(target, port, log_dir)
        completed = subprocess.run(
            [
                str(VALKEY_BENCH),
                "-h",
                "127.0.0.1",
                "-p",
                str(port),
                "-n",
                str(workload.requests),
                "-c",
                str(workload.clients),
                "-P",
                str(workload.pipeline),
                "-d",
                str(workload.payload),
                "-t",
                workload.command,
                "--csv",
                "--precision",
                "2",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=900,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"valkey-benchmark failed for {target.value}/{workload.name}: "
                f"{completed.stderr[-2000:]}"
            )
        return parse_benchmark_csv(completed.stdout, workload, target)
    finally:
        stop_server(proc)


def choose_profiler(requested: str) -> ProfilerChoice | None:
    system = platform.system().lower()
    sample_available = MACOS_SAMPLE.exists() and os.access(MACOS_SAMPLE, os.X_OK)
    perf_path = shutil.which("perf")
    cargo_path = shutil.which("cargo")

    if requested in {"auto", "sample"} and system == "darwin" and sample_available:
        return ProfilerChoice(
            name="sample",
            path=str(MACOS_SAMPLE),
            version=run_text([str(MACOS_SAMPLE), "-h"])[:120],
            note="macOS wall-clock stack sampler attached to the Rust server PID",
        )
    if requested == "sample":
        return None

    if requested in {"auto", "perf"} and perf_path:
        return ProfilerChoice(
            name="perf",
            path=perf_path,
            version=run_text([perf_path, "--version"]),
            note="Linux perf record/script attached to the Rust server PID",
        )
    if requested == "perf":
        return None

    if requested in {"auto", "cargo-flamegraph"} and cargo_path:
        flamegraph_version = run_text([cargo_path, "flamegraph", "--version"])
        if flamegraph_version:
            return ProfilerChoice(
                name="cargo-flamegraph",
                path=cargo_path,
                version=flamegraph_version,
                note="cargo flamegraph attached to the Rust server PID",
            )
    return None


def available_tool_metadata() -> dict[str, Any]:
    perf_path = shutil.which("perf")
    cargo_path = shutil.which("cargo")
    return {
        "platform": platform.system(),
        "sample": {
            "path": str(MACOS_SAMPLE),
            "available": MACOS_SAMPLE.exists() and os.access(MACOS_SAMPLE, os.X_OK),
        },
        "perf": {
            "path": perf_path or "",
            "available": bool(perf_path),
            "version": run_text([perf_path, "--version"]) if perf_path else "",
        },
        "cargo_flamegraph": {
            "path": cargo_path or "",
            "available": bool(cargo_path and run_text([cargo_path, "flamegraph", "--version"])),
            "version": run_text([cargo_path, "flamegraph", "--version"]) if cargo_path else "",
        },
    }


def wait_for_profiler(proc: subprocess.Popen[str], seconds: int) -> dict[str, Any]:
    timed_out = False
    try:
        proc.wait(timeout=max(3, seconds + 8))
    except subprocess.TimeoutExpired:
        timed_out = True
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    return {"returncode": proc.returncode, "timed_out": timed_out}


def start_sample_profiler(
    pid: int,
    workload: Workload,
    seconds: int,
    out_dir: Path,
) -> tuple[dict[str, Any], subprocess.Popen[str]]:
    sample_path = out_dir / f"{workload.name}.sample.txt"
    log_path = out_dir / f"{workload.name}.sample.stderr.log"
    cmd = [
        str(MACOS_SAMPLE),
        str(pid),
        str(seconds),
        "1",
        "-mayDie",
        "-file",
        str(sample_path),
    ]
    log = log_path.open("w", encoding="utf-8")
    try:
        proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=log, text=True)
    finally:
        log.close()
    profile = {
        "tool": "sample",
        "command": cmd,
        "artifacts": [
            {"path": relative(sample_path), "kind": "raw-sample-calltree"},
            {"path": relative(log_path), "kind": "profiler-log"},
        ],
        "available": False,
    }
    return profile, proc


def start_perf_profiler(
    pid: int,
    workload: Workload,
    seconds: int,
    out_dir: Path,
    frequency: int,
) -> tuple[dict[str, Any], subprocess.Popen[str]]:
    data_path = out_dir / f"{workload.name}.perf.data"
    script_path = out_dir / f"{workload.name}.perf.script.txt"
    report_path = out_dir / f"{workload.name}.perf.report.txt"
    log_path = out_dir / f"{workload.name}.perf.stderr.log"
    perf = shutil.which("perf")
    if perf is None:
        raise RuntimeError("perf not found")
    cmd = [
        perf,
        "record",
        "-F",
        str(frequency),
        "-g",
        "-p",
        str(pid),
        "-o",
        str(data_path),
        "--",
        "sleep",
        str(seconds),
    ]
    log = log_path.open("w", encoding="utf-8")
    try:
        proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=log, text=True)
    finally:
        log.close()
    profile = {
        "tool": "perf",
        "command": cmd,
        "script_status": {"returncode": None},
        "report_status": {"returncode": None},
        "artifacts": [
            {"path": relative(data_path), "kind": "raw-perf-data"},
            {"path": relative(script_path), "kind": "perf-script"},
            {"path": relative(report_path), "kind": "perf-report"},
            {"path": relative(log_path), "kind": "profiler-log"},
        ],
        "available": False,
    }
    return profile, proc


def start_cargo_flamegraph_profiler(
    pid: int,
    workload: Workload,
    seconds: int,
    out_dir: Path,
) -> tuple[dict[str, Any], subprocess.Popen[str]]:
    svg_path = out_dir / f"{workload.name}.flamegraph.svg"
    log_path = out_dir / f"{workload.name}.cargo-flamegraph.stderr.log"
    cargo = shutil.which("cargo")
    if cargo is None:
        raise RuntimeError("cargo not found")
    cmd = [
        cargo,
        "flamegraph",
        "--pid",
        str(pid),
        "--duration",
        str(seconds),
        "--output",
        str(svg_path),
    ]
    log = log_path.open("w", encoding="utf-8")
    try:
        proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=log, text=True)
    finally:
        log.close()
    profile = {
        "tool": "cargo-flamegraph",
        "command": cmd,
        "artifacts": [
            {"path": relative(svg_path), "kind": "flamegraph-svg"},
            {"path": relative(log_path), "kind": "profiler-log"},
        ],
        "available": False,
    }
    return profile, proc


def start_profiler(
    choice: ProfilerChoice,
    pid: int,
    workload: Workload,
    seconds: int,
    out_dir: Path,
    frequency: int,
) -> tuple[dict[str, Any], subprocess.Popen[str]]:
    if choice.name == "sample":
        return start_sample_profiler(pid, workload, seconds, out_dir)
    if choice.name == "perf":
        return start_perf_profiler(pid, workload, seconds, out_dir, frequency)
    if choice.name == "cargo-flamegraph":
        return start_cargo_flamegraph_profiler(pid, workload, seconds, out_dir)
    raise RuntimeError(f"unsupported profiler: {choice.name}")


def artifact_path(profile: dict[str, Any], kind: str) -> Path | None:
    for artifact in profile.get("artifacts", []):
        if artifact.get("kind") == kind:
            return ROOT / artifact["path"]
    return None


def finish_profiler(profile: dict[str, Any], proc: subprocess.Popen[str], seconds: int) -> None:
    profile["status"] = wait_for_profiler(proc, seconds)
    tool = profile.get("tool")
    if tool == "perf":
        data_path = artifact_path(profile, "raw-perf-data")
        script_path = artifact_path(profile, "perf-script")
        report_path = artifact_path(profile, "perf-report")
        perf = shutil.which("perf")
        if perf and data_path and data_path.exists() and script_path and report_path:
            with script_path.open("w", encoding="utf-8") as out:
                completed = subprocess.run(
                    [perf, "script", "-i", str(data_path)],
                    cwd=ROOT,
                    stdout=out,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    timeout=120,
                )
                profile["script_status"] = {"returncode": completed.returncode}
            with report_path.open("w", encoding="utf-8") as out:
                completed = subprocess.run(
                    [perf, "report", "--stdio", "-i", str(data_path), "--sort", "comm,dso,symbol"],
                    cwd=ROOT,
                    stdout=out,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    timeout=120,
                )
                profile["report_status"] = {"returncode": completed.returncode}
        profile["available"] = bool(data_path and data_path.exists() and data_path.stat().st_size > 0)
        return

    if tool == "sample":
        sample_path = artifact_path(profile, "raw-sample-calltree")
        profile["available"] = bool(sample_path and sample_path.exists() and sample_path.stat().st_size > 0)
        return

    if tool == "cargo-flamegraph":
        svg_path = artifact_path(profile, "flamegraph-svg")
        profile["available"] = bool(svg_path and svg_path.exists() and svg_path.stat().st_size > 0)
        return

    profile["available"] = False


def run_rust_profiled(
    workload: Workload,
    log_dir: Path,
    profiler: ProfilerChoice,
    seconds: int,
    frequency: int,
) -> tuple[BenchmarkRow, dict[str, Any]]:
    port = free_port()
    proc: subprocess.Popen[str] | None = None
    profiler_result: dict[str, Any] | None = None
    profiler_proc: subprocess.Popen[str] | None = None
    workload_dir = log_dir / workload.name
    workload_dir.mkdir(parents=True, exist_ok=True)
    try:
        proc = start_server(Target.RUST, port, workload_dir)
        profiler_proc_ready_delay_s = 0.25
        profiler_result, profiler_proc = start_profiler(
            profiler,
            proc.pid,
            workload,
            seconds,
            workload_dir,
            frequency,
        )
        time.sleep(profiler_proc_ready_delay_s)
        completed = subprocess.run(
            [
                str(VALKEY_BENCH),
                "-h",
                "127.0.0.1",
                "-p",
                str(port),
                "-n",
                str(workload.requests),
                "-c",
                str(workload.clients),
                "-P",
                str(workload.pipeline),
                "-d",
                str(workload.payload),
                "-t",
                workload.command,
                "--csv",
                "--precision",
                "2",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=900,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"valkey-benchmark failed for rust/{workload.name}: "
                f"{completed.stderr[-2000:]}"
            )
        rust = parse_benchmark_csv(completed.stdout, workload, Target.RUST)
        return rust, profiler_result
    finally:
        if profiler_result is not None and profiler_proc is not None:
            finish_profiler(profiler_result, profiler_proc, seconds)
        stop_server(proc)


def workloads_for_suite(
    suite: Suite,
    selected: set[str],
    requests_override: int | None,
    clients: int,
    pipeline: int,
    payload: int,
) -> list[Workload]:
    if suite is Suite.SMOKE:
        base = [
            ("get-p100", "get", 2_000_000),
            ("set-p100", "set", 1_000_000),
            ("incr-p100", "incr", 1_000_000),
            ("ping-p100", "ping_mbulk", 2_000_000),
        ]
    else:
        base = [
            ("get-p100", "get", 20_000_000),
            ("set-p100", "set", 10_000_000),
            ("incr-p100", "incr", 10_000_000),
            ("ping-p100", "ping_mbulk", 20_000_000),
        ]
    rows = [
        Workload(
            name=name,
            command=command,
            requests=requests_override or requests,
            clients=clients,
            pipeline=pipeline,
            payload=payload,
        )
        for name, command, requests in base
    ]
    if not selected:
        return rows
    return [row for row in rows if row.name in selected or row.command in selected or row.name.split("-", 1)[0] in selected]


def parse_sample_top_frames(path: Path, limit: int = 12) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8", errors="replace")
    marker = "Sort by top of stack, same collapsed"
    if marker not in text:
        return []
    section = text.split(marker, 1)[1].split("Binary Images:", 1)[0]
    rows = []
    for line in section.splitlines():
        match = re.match(r"^\s*(?P<frame>.+?)\s+(?P<count>\d+)\s*$", line)
        if not match:
            continue
        rows.append({"frame": match.group("frame").strip(), "count": int(match.group("count"))})
        if len(rows) >= limit:
            break
    return rows


def summarize_profile(profile: dict[str, Any]) -> dict[str, Any]:
    artifact_paths = [ROOT / item["path"] for item in profile.get("artifacts", []) if "path" in item]
    existing = [path for path in artifact_paths if path.exists() and path.stat().st_size > 0]
    summary: dict[str, Any] = {
        "tool": profile.get("tool"),
        "available": bool(profile.get("available")),
        "artifact_count": len(existing),
        "bytes": sum(path.stat().st_size for path in existing),
    }
    for path in existing:
        if path.name.endswith(".sample.txt"):
            summary["sample_top_frames"] = parse_sample_top_frames(path)
            break
    return summary


def write_tsv(path: Path, commit: str, hardware: dict[str, str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as out:
        out.write("# valkey-rs call-tree profile benchmark\n")
        out.write(f"# commit\t{commit}\n")
        out.write(f"# os\t{hardware['os']}\n")
        out.write(f"# arch\t{hardware['arch']}\n")
        out.write(f"# cpu\t{hardware['cpu']}\n")
        out.write(
            "workload\tcommand\trequests\tclients\tpipeline\tpayload\treference_rps\trust_rps\t"
            "ratio\treference_p99_ms\trust_p99_ms\tprofiler\tprofile_artifacts\n"
        )
        for row in rows:
            artifact_paths = ",".join(item["path"] for item in row["profile"].get("artifacts", []) if "path" in item)
            out.write(
                f"{row['workload']}\t{row['command']}\t{row['requests']}\t{row['clients']}\t"
                f"{row['pipeline']}\t{row['payload']}\t{row['reference_rps']:.2f}\t"
                f"{row['rust_rps']:.2f}\t{row['ratio']:.6f}\t{row['reference_p99_ms']:.3f}\t"
                f"{row['rust_p99_ms']:.3f}\t{row['profile'].get('tool', '')}\t{artifact_paths}\n"
            )


def measurements_from_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    measurements = []
    for row in rows:
        workload = row["workload"]
        measurements.extend(
            [
                {
                    "metric": "throughput_req_s",
                    "target": "reference",
                    "workload": workload,
                    "value": row["reference_rps"],
                    "unit": "req/s",
                },
                {
                    "metric": "throughput_req_s",
                    "target": "rust",
                    "workload": workload,
                    "value": row["rust_rps"],
                    "unit": "req/s",
                },
                {
                    "metric": "throughput_ratio",
                    "target": "rust-vs-reference",
                    "workload": workload,
                    "value": row["ratio"],
                    "unit": "ratio",
                },
                {
                    "metric": "profile_artifact_bytes",
                    "target": "rust",
                    "workload": workload,
                    "value": row["profile_summary"]["bytes"],
                    "unit": "bytes",
                },
            ]
        )
    return measurements


def failure_result(summary: str, evidence: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "runner_id": "bench-profile-calltree",
        "status": "fail",
        "surface": "performance",
        "method": "bench-load",
        "summary": summary,
        "measurements": [],
        "artifacts": [],
        "evidence": evidence,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run valkey-rs benchmark workloads with raw call-tree profile artifacts")
    parser.add_argument("--suite", choices=[suite.value for suite in Suite], default=Suite.BIG.value)
    parser.add_argument("--workloads", default="", help="comma-separated workload names or command prefixes")
    parser.add_argument("--profile-seconds", type=int, default=8)
    parser.add_argument("--profiler", choices=["auto", "sample", "perf", "cargo-flamegraph"], default="auto")
    parser.add_argument("--perf-frequency", type=int, default=997)
    parser.add_argument("--requests", type=int, default=0, help="override request count for every workload")
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--pipeline", type=int, default=100)
    parser.add_argument("--payload", type=int, default=64)
    args = parser.parse_args()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PROFILES_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    commit = git_commit()
    hardware = hardware_fingerprint()
    git_status = git_status_short()
    tool_metadata = available_tool_metadata()
    profile_root = PROFILES_DIR / f"{stamp}-{commit}-calltree"
    profile_root.mkdir(parents=True, exist_ok=True)
    tsv_path = RESULTS_DIR / f"{stamp}-{commit}-calltree.tsv"
    json_path = RESULTS_DIR / f"{stamp}-{commit}-calltree.json"
    rows: list[dict[str, Any]] = []

    profiler = choose_profiler(args.profiler)
    if profiler is None:
        result = failure_result(
            f"profile calltree: no supported profiler available for request {args.profiler!r}",
            {
                "commit": commit,
                "git_status_short": git_status,
                "hardware": hardware,
                "tool_metadata": tool_metadata,
                "requested_profiler": args.profiler,
                "profile_root": relative(profile_root),
            },
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 1

    suite = Suite(args.suite)
    selected = {item.strip().lower() for item in args.workloads.split(",") if item.strip()}
    workloads: list[Workload] = []
    try:
        require_binaries()
        workloads = workloads_for_suite(
            suite=suite,
            selected=selected,
            requests_override=args.requests if args.requests > 0 else None,
            clients=args.clients,
            pipeline=args.pipeline,
            payload=args.payload,
        )
        if not workloads:
            raise RuntimeError(f"no workloads selected by {args.workloads!r}")

        for workload in workloads:
            print(f"==> {workload.name}: reference", file=sys.stderr)
            reference = run_benchmark(Target.REFERENCE, workload, profile_root)
            print(f"==> {workload.name}: rust + {profiler.name}", file=sys.stderr)
            rust, profile = run_rust_profiled(
                workload=workload,
                log_dir=profile_root,
                profiler=profiler,
                seconds=args.profile_seconds,
                frequency=args.perf_frequency,
            )
            ratio = rust.rps / reference.rps if reference.rps else 0.0
            profile_summary = summarize_profile(profile)
            rows.append(
                {
                    "workload": workload.name,
                    "command": rust.command,
                    "requests": workload.requests,
                    "clients": workload.clients,
                    "pipeline": workload.pipeline,
                    "payload": workload.payload,
                    "reference": asdict(reference),
                    "rust": asdict(rust),
                    "reference_rps": reference.rps,
                    "rust_rps": rust.rps,
                    "ratio": ratio,
                    "reference_p99_ms": reference.p99_ms,
                    "rust_p99_ms": rust.p99_ms,
                    "profile": profile,
                    "profile_summary": profile_summary,
                }
            )
    except Exception as exc:
        result = failure_result(
            f"profile calltree: {type(exc).__name__}: {exc}",
            {
                "commit": commit,
                "git_status_short": git_status,
                "hardware": hardware,
                "suite": suite.value,
                "workloads": [asdict(workload) for workload in workloads],
                "rows": rows,
                "profiler": asdict(profiler),
                "tool_metadata": tool_metadata,
                "profile_root": relative(profile_root),
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        )
        result["artifacts"] = [{"path": relative(json_path), "kind": "runner-result-json"}]
        json_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(result, indent=2, sort_keys=True))
        return 1

    write_tsv(tsv_path, commit, hardware, rows)
    ratios = [row["ratio"] for row in rows]
    profile_artifacts = []
    for row in rows:
        for artifact in row["profile"].get("artifacts", []):
            artifact_with_workload = dict(artifact)
            artifact_with_workload["workload"] = row["workload"]
            profile_artifacts.append(artifact_with_workload)
    for server_log in sorted(profile_root.glob("**/*.server.log")):
        profile_artifacts.append(
            {
                "path": relative(server_log),
                "kind": "server-log",
            }
        )

    passed = all(row["reference_rps"] > 0 and row["rust_rps"] > 0 for row in rows) and all(
        row["profile_summary"]["artifact_count"] > 0 for row in rows
    )
    median_ratio = sorted(ratios)[len(ratios) // 2]
    result = {
        "schema_version": 1,
        "runner_id": "bench-profile-calltree",
        "status": "pass" if passed else "fail",
        "surface": "performance",
        "method": "bench-load",
        "summary": (
            f"profile calltree: {profiler.name}, median {median_ratio:.2f}x, "
            f"min {min(ratios):.2f}x, max {max(ratios):.2f}x; "
            f"artifacts {len(profile_artifacts)}"
        ),
        "measurements": measurements_from_rows(rows),
        "artifacts": [
            {"path": relative(tsv_path), "kind": "summary-tsv"},
            {"path": relative(json_path), "kind": "runner-result-json"},
            *profile_artifacts,
        ],
        "evidence": {
            "commit": commit,
            "git_status_short": git_status,
            "hardware": hardware,
            "suite": suite.value,
            "workloads": [asdict(workload) for workload in workloads],
            "rows": rows,
            "profiler": asdict(profiler),
            "tool_metadata": tool_metadata,
            "profile_root": relative(profile_root),
            "method_detail": "bench-load-with-raw-calltree-profile-artifacts",
            "method_note": (
                "Profiles attach to the Rust server PID while valkey-benchmark drives the "
                "same loopback workload used for performance telemetry. The runner does not "
                "change server flags, command dispatch, persistence settings, or benchmark "
                "command selection outside its own artifact capture."
            ),
        },
    }
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
