#!/usr/bin/env python3
"""Run larger Valkey benchmarks while sampling valkey-rs stacks.

This runner is intentionally narrower than `run-profile-matrix.sh`: it focuses
on a few hot simple-command workloads for long enough that macOS stack sampling
captures the active serving path rather than only startup or idle time.

Artifacts:
  harness/bench/results/<ts>-<sha>-hotspots.tsv
  harness/bench/results/<ts>-<sha>-hotspots.json
  harness/bench/results/<ts>-<sha>-<workload>.sample.txt
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import socket
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
VALKEY_BIN = ROOT / "reference/valkey/src/valkey-server"
VALKEY_BENCH = ROOT / "reference/valkey/src/valkey-benchmark"
RUST_BIN = ROOT / "target/release/redis-server"
RESULTS_DIR = ROOT / "harness/bench/results"
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
    sample_seconds: int


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


def run_text(cmd: list[str]) -> str:
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return ""


def git_commit() -> str:
    return run_text(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"]) or "unknown"


def hardware_fingerprint() -> dict[str, str]:
    cpu = run_text(["sysctl", "-n", "machdep.cpu.brand_string"])
    if not cpu and Path("/proc/cpuinfo").exists():
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    return {
        "os": run_text(["uname", "-sr"]) or "unknown",
        "arch": run_text(["uname", "-m"]) or "unknown",
        "cpu": cpu or "unknown",
    }


def require_binaries() -> None:
    if not VALKEY_BIN.exists() or not VALKEY_BENCH.exists():
        subprocess.run(["bash", "scripts/setup-reference.sh"], cwd=ROOT, check=True)
    if not RUST_BIN.exists():
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


def start_server(target: Target, port: int, stamp: str) -> subprocess.Popen:
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

    log_path = RESULTS_DIR / f"{stamp}-{target.value}-{port}.log"
    log = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=log, text=True)
    wait_for_port(port)
    return proc


def stop_server(proc: subprocess.Popen | None) -> None:
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


def start_sampler(pid: int, workload: Workload, sample_path: Path) -> subprocess.Popen | None:
    if not MACOS_SAMPLE.exists():
        return None
    cmd = [
        str(MACOS_SAMPLE),
        str(pid),
        str(workload.sample_seconds),
        "1",
        "-mayDie",
        "-file",
        str(sample_path),
    ]
    return subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def run_benchmark(target: Target, workload: Workload, stamp: str, sample_path: Path | None) -> BenchmarkRow:
    port = free_port()
    proc: subprocess.Popen | None = None
    sampler: subprocess.Popen | None = None
    try:
        proc = start_server(target, port, stamp)
        if target is Target.RUST and sample_path is not None:
            sampler = start_sampler(proc.pid, workload, sample_path)
            time.sleep(0.25)
        cmd = [
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
        ]
        completed = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=600)
        if completed.returncode != 0:
            raise RuntimeError(
                f"valkey-benchmark failed for {target.value}/{workload.name}: "
                f"{completed.stderr[-2000:]}"
            )
        if sampler is not None:
            try:
                sampler.wait(timeout=max(2, workload.sample_seconds + 3))
            except subprocess.TimeoutExpired:
                sampler.terminate()
                sampler.wait(timeout=3)
        return parse_benchmark_csv(completed.stdout, workload, target)
    finally:
        stop_server(proc)


IDLE_PATTERNS = (
    "__semwait_signal",
    "__accept",
    "__ulock_wait",
    "__psynch_cvwait",
    "kevent",
    "mach_msg",
    "nanosleep",
    "pthread_cond",
    "semaphore_wait",
    "std::thread::functions::sleep",
)


def classify_frame(frame: str) -> str:
    lower = frame.lower()
    if any(pattern.lower() in lower for pattern in IDLE_PATTERNS):
        return "idle_or_wait"
    if (
        "pthread_mutex" in lower
        or "__psynch_mutex" in lower
        or "std::sys::pal::unix::sync::mutex" in lower
        or "std::sync::poison::mutex" in lower
        or "mutex::lock" in lower
    ):
        return "lock"
    if "redis-server" in lower or "redis_" in lower or "redis::" in lower:
        return "rust_user"
    if "__recvfrom" in lower or " recv" in lower:
        return "socket_read"
    if "__sendto" in lower or " send" in lower:
        return "socket_write"
    return "other"


def parse_sample_summary(sample_path: Path) -> dict:
    if not sample_path.exists():
        return {"available": False, "reason": "sample output missing"}

    text = sample_path.read_text(encoding="utf-8", errors="replace")
    sort_marker = "Sort by top of stack, same collapsed"
    if sort_marker not in text:
        return {"available": True, "raw_path": str(sample_path), "top_stacks": []}

    section = text.split(sort_marker, 1)[1].split("Binary Images:", 1)[0]
    top_stacks = []
    by_category: dict[str, int] = {}
    for line in section.splitlines():
        match = re.match(r"^\s*(?P<frame>.+?)\s+(?P<count>\d+)\s*$", line)
        if not match:
            continue
        frame = match.group("frame").strip()
        count = int(match.group("count"))
        category = classify_frame(frame)
        by_category[category] = by_category.get(category, 0) + count
        top_stacks.append({"frame": frame, "count": count, "category": category})

    non_idle = [row for row in top_stacks if row["category"] != "idle_or_wait"]
    return {
        "available": True,
        "raw_path": str(sample_path),
        "top_stacks": top_stacks[:20],
        "top_non_idle": non_idle[:15],
        "category_counts": by_category,
    }


def workloads_for_suite(suite: Suite, selected: set[str], sample_seconds: int) -> list[Workload]:
    if suite is Suite.SMOKE:
        rows = [
            Workload("get-p100", "get", 2_000_000, 50, 100, 64, sample_seconds),
            Workload("set-p100", "set", 1_000_000, 50, 100, 64, sample_seconds),
            Workload("incr-p100", "incr", 1_000_000, 50, 100, 64, sample_seconds),
        ]
    else:
        rows = [
            Workload("get-p100", "get", 20_000_000, 50, 100, 64, sample_seconds),
            Workload("set-p100", "set", 10_000_000, 50, 100, 64, sample_seconds),
            Workload("incr-p100", "incr", 10_000_000, 50, 100, 64, sample_seconds),
            Workload("ping-p100", "ping_mbulk", 20_000_000, 50, 100, 64, sample_seconds),
        ]
    if not selected:
        return rows
    return [row for row in rows if row.name.split("-", 1)[0] in selected or row.name in selected]


def write_tsv(path: Path, commit: str, hardware: dict, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as out:
        out.write("# valkey-rs profiled hotspot benchmark\n")
        out.write(f"# commit\t{commit}\n")
        out.write(f"# os\t{hardware['os']}\n")
        out.write(f"# arch\t{hardware['arch']}\n")
        out.write(f"# cpu\t{hardware['cpu']}\n")
        out.write(
            "workload\tcommand\trequests\tclients\tpipeline\tpayload\treference_rps\trust_rps\tratio\t"
            "reference_p99_ms\trust_p99_ms\tsample_path\n"
        )
        for row in rows:
            out.write(
                f"{row['workload']}\t{row['command']}\t{row['requests']}\t{row['clients']}\t"
                f"{row['pipeline']}\t{row['payload']}\t{row['reference_rps']:.2f}\t"
                f"{row['rust_rps']:.2f}\t{row['ratio']:.6f}\t{row['reference_p99_ms']:.3f}\t"
                f"{row['rust_p99_ms']:.3f}\t{row['sample_path']}\n"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run larger benchmarks with Rust server stack samples")
    parser.add_argument("--suite", choices=[suite.value for suite in Suite], default=Suite.BIG.value)
    parser.add_argument("--workloads", default="", help="comma-separated workload names or command prefixes")
    parser.add_argument("--sample-seconds", type=int, default=8)
    args = parser.parse_args()

    require_binaries()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    commit = git_commit()
    hardware = hardware_fingerprint()
    suite = Suite(args.suite)
    selected = {item.strip().lower() for item in args.workloads.split(",") if item.strip()}
    workloads = workloads_for_suite(suite, selected, args.sample_seconds)
    if not workloads:
        raise RuntimeError(f"no workloads selected by {args.workloads!r}")

    tsv_path = RESULTS_DIR / f"{stamp}-{commit}-hotspots.tsv"
    json_path = RESULTS_DIR / f"{stamp}-{commit}-hotspots.json"
    result_rows = []
    sample_summaries = {}

    for workload in workloads:
        print(f"==> {workload.name}: reference", file=sys.stderr)
        reference = run_benchmark(Target.REFERENCE, workload, stamp, None)
        sample_path = RESULTS_DIR / f"{stamp}-{commit}-{workload.name}.sample.txt"
        print(f"==> {workload.name}: rust + /usr/bin/sample", file=sys.stderr)
        rust = run_benchmark(Target.RUST, workload, stamp, sample_path)
        ratio = rust.rps / reference.rps if reference.rps else 0.0
        result_rows.append(
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
                "sample_path": str(sample_path.relative_to(ROOT)),
            }
        )
        sample_summaries[workload.name] = parse_sample_summary(sample_path)

    write_tsv(tsv_path, commit, hardware, result_rows)

    ratios = [row["ratio"] for row in result_rows]
    result = {
        "schema_version": 1,
        "runner_id": "bench-profile-hotspots",
        "status": "pass",
        "surface": "performance",
        "method": "bench-load-with-stack-sampling",
        "summary": (
            f"profile hotspots: median {sorted(ratios)[len(ratios)//2]:.2f}x, "
            f"min {min(ratios):.2f}x, max {max(ratios):.2f}x"
        ),
        "artifacts": [
            {"path": str(tsv_path.relative_to(ROOT))},
            {"path": str(json_path.relative_to(ROOT))},
        ],
        "evidence": {
            "commit": commit,
            "hardware": hardware,
            "suite": suite.value,
            "workloads": [asdict(workload) for workload in workloads],
            "rows": result_rows,
            "samples": sample_summaries,
            "sampler": str(MACOS_SAMPLE) if MACOS_SAMPLE.exists() else "unavailable",
            "sampler_note": (
                "/usr/bin/sample is wall-clock stack sampling. Treat wait/sleep/socket categories "
                "as scheduler/IO evidence, not pure CPU time. Use xctrace Time Profiler for a "
                "GUI-grade CPU trace when needed."
            ),
        },
    }
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
