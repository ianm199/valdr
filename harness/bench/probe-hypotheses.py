#!/usr/bin/env python3
"""Direct performance probes for validating valkey-rs bottleneck hypotheses.

This is intentionally not a work-packet runner. It is an operator tool for
answering narrow questions before more implementation work:

* Does throughput improve with pipeline depth or payload size?
* Does PING, which avoids DB work, still show a large fixed overhead?
* Do malloc stack logs point at parser/reply/key allocation hot paths?
* Can we capture an Instruments Time Profiler trace for a single workload?

Artifacts:
  harness/bench/results/<ts>-<sha>-protocol-shape.{tsv,json}
  harness/bench/results/<ts>-<sha>-alloc-stacks.{tsv,json}
  harness/bench/profiles/<ts>-<sha>-alloc-stacks/<workload>/*
  harness/bench/profiles/<ts>-<sha>-xctrace-time/<workload>/*
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
import xml.etree.ElementTree as ET
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


class Target(Enum):
    REFERENCE = "reference"
    RUST = "rust"


class ProbeSuite(Enum):
    SMOKE = "smoke"
    BIG = "big"


class CommandKind(Enum):
    PING_INLINE = "ping_inline"
    PING_MBULK = "ping_mbulk"
    GET = "get"
    SET = "set"
    INCR = "incr"


@dataclass(frozen=True)
class Workload:
    name: str
    command: CommandKind
    requests: int
    clients: int
    pipeline: int
    payload: int


@dataclass(frozen=True)
class BenchmarkRow:
    workload: str
    target: Target
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


def minimal_child_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    """Return a small process environment for benchmark children.

    Instruments/xctrace records the target process environment in the .trace
    bundle. Inheriting the operator's full shell environment can leak API keys
    into local profiler artifacts, so benchmark children get only the variables
    needed to execute normally.
    """
    keep = ("PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "LC_CTYPE")
    child = {key: value for key in keep if (value := os.environ.get(key))}
    if extra:
        child.update(extra)
    return child


def start_server(target: Target, port: int, log_path: Path, env: dict[str, str] | None = None) -> subprocess.Popen[str]:
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

    log_path.parent.mkdir(parents=True, exist_ok=True)
    log = log_path.open("w", encoding="utf-8")
    proc_env = minimal_child_env(env)
    try:
        proc = subprocess.Popen(cmd, cwd=ROOT, env=proc_env, stdout=log, stderr=log, text=True)
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
        target=target,
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


def run_benchmark(
    target: Target,
    workload: Workload,
    log_path: Path,
    env: dict[str, str] | None = None,
) -> tuple[BenchmarkRow, subprocess.Popen[str] | None]:
    port = free_port()
    proc: subprocess.Popen[str] | None = None
    keep_alive = env is not None and env.get("KEEP_SERVER_ALIVE") == "1"
    proc_env = {key: value for key, value in (env or {}).items() if key != "KEEP_SERVER_ALIVE"}
    try:
        proc = start_server(target, port, log_path, proc_env or None)
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
                workload.command.value,
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
        row = parse_benchmark_csv(completed.stdout, workload, target)
        if keep_alive:
            return row, proc
        return row, None
    finally:
        if not keep_alive:
            stop_server(proc)


def int_list(raw: str) -> list[int]:
    values = []
    for item in raw.split(","):
        item = item.strip()
        if item:
            values.append(int(item))
    if not values:
        raise ValueError(f"empty integer list: {raw!r}")
    return values


def command_list(raw: str) -> list[CommandKind]:
    commands = []
    for item in raw.split(","):
        item = item.strip().lower()
        if item:
            commands.append(CommandKind(item))
    if not commands:
        raise ValueError(f"empty command list: {raw!r}")
    return commands


def protocol_requests(suite: ProbeSuite, pipeline: int) -> int:
    if suite is ProbeSuite.SMOKE:
        return 50_000 if pipeline == 1 else 100_000
    return 200_000 if pipeline == 1 else 1_000_000


def protocol_workloads(
    suite: ProbeSuite,
    commands: list[CommandKind],
    pipelines: list[int],
    payloads: list[int],
    clients: int,
) -> list[Workload]:
    rows = []
    for command in commands:
        active_payloads = [64] if command in {CommandKind.PING_INLINE, CommandKind.PING_MBULK, CommandKind.INCR} else payloads
        for pipeline in pipelines:
            for payload in active_payloads:
                rows.append(
                    Workload(
                        name=f"{command.value}-p{pipeline}-d{payload}",
                        command=command,
                        requests=protocol_requests(suite, pipeline),
                        clients=clients,
                        pipeline=pipeline,
                        payload=payload,
                    )
                )
    return rows


def summarize_protocol(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_command: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_command.setdefault(row["command"], []).append(row)

    command_summary = {}
    for command, items in sorted(by_command.items()):
        ratios = [item["ratio"] for item in items]
        p1 = [item["ratio"] for item in items if item["pipeline"] == 1]
        p100 = [item["ratio"] for item in items if item["pipeline"] == 100]
        d8 = [item["ratio"] for item in items if item["payload"] == 8]
        d1024 = [item["ratio"] for item in items if item["payload"] == 1024]
        command_summary[command] = {
            "median_ratio": sorted(ratios)[len(ratios) // 2],
            "min_ratio": min(ratios),
            "max_ratio": max(ratios),
            "p100_over_p1": (sum(p100) / len(p100)) / (sum(p1) / len(p1)) if p1 and p100 else None,
            "d1024_over_d8": (sum(d1024) / len(d1024)) / (sum(d8) / len(d8)) if d8 and d1024 else None,
        }

    all_ratios = [row["ratio"] for row in rows]
    ping_ratios = [
        row["ratio"]
        for row in rows
        if row["command"] in {"PING_INLINE", "PING_MBULK"}
    ]
    return {
        "overall_median_ratio": sorted(all_ratios)[len(all_ratios) // 2],
        "overall_min_ratio": min(all_ratios),
        "overall_max_ratio": max(all_ratios),
        "ping_median_ratio": sorted(ping_ratios)[len(ping_ratios) // 2] if ping_ratios else None,
        "by_command": command_summary,
        "interpretation": {
            "fixed_overhead_signal": (
                "strong" if ping_ratios and sorted(ping_ratios)[len(ping_ratios) // 2] < 0.9 else "weak"
            ),
            "payload_amortization_signal": (
                "check by_command.*.d1024_over_d8; >1.2 means payload amortizes fixed overhead"
            ),
            "pipeline_amortization_signal": (
                "check by_command.*.p100_over_p1; >1.2 means batching amortizes fixed overhead"
            ),
        },
    }


def run_protocol_shape(args: argparse.Namespace) -> int:
    require_binaries()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    commit = git_commit()
    hardware = hardware_fingerprint()
    suite = ProbeSuite(args.suite)
    workloads = protocol_workloads(
        suite=suite,
        commands=command_list(args.commands),
        pipelines=int_list(args.pipelines),
        payloads=int_list(args.payloads),
        clients=args.clients,
    )
    tsv_path = RESULTS_DIR / f"{stamp}-{commit}-protocol-shape.tsv"
    json_path = RESULTS_DIR / f"{stamp}-{commit}-protocol-shape.json"
    rows = []
    for workload in workloads:
        print(f"==> {workload.name}: reference", file=sys.stderr)
        ref, _ = run_benchmark(Target.REFERENCE, workload, RESULTS_DIR / f"{stamp}-reference-{workload.name}.log")
        print(f"==> {workload.name}: rust", file=sys.stderr)
        rust, _ = run_benchmark(Target.RUST, workload, RESULTS_DIR / f"{stamp}-rust-{workload.name}.log")
        rows.append(
            {
                "workload": workload.name,
                "command": rust.command,
                "requests": workload.requests,
                "clients": workload.clients,
                "pipeline": workload.pipeline,
                "payload": workload.payload,
                "reference": asdict(ref) | {"target": ref.target.value},
                "rust": asdict(rust) | {"target": rust.target.value},
                "reference_rps": ref.rps,
                "rust_rps": rust.rps,
                "ratio": rust.rps / ref.rps if ref.rps else 0.0,
                "reference_p99_ms": ref.p99_ms,
                "rust_p99_ms": rust.p99_ms,
            }
        )

    with tsv_path.open("w", encoding="utf-8") as out:
        out.write("# valkey-rs protocol-shape hypothesis probe\n")
        out.write(f"# timestamp_utc\t{stamp}\n")
        out.write(f"# commit\t{commit}\n")
        out.write(f"# os\t{hardware['os']}\n")
        out.write(f"# arch\t{hardware['arch']}\n")
        out.write(f"# cpu\t{hardware['cpu']}\n")
        out.write(
            "workload\tcommand\trequests\tclients\tpipeline\tpayload\treference_rps\trust_rps\t"
            "ratio\treference_p99_ms\trust_p99_ms\n"
        )
        for row in rows:
            out.write(
                f"{row['workload']}\t{row['command']}\t{row['requests']}\t{row['clients']}\t"
                f"{row['pipeline']}\t{row['payload']}\t{row['reference_rps']:.2f}\t"
                f"{row['rust_rps']:.2f}\t{row['ratio']:.6f}\t"
                f"{row['reference_p99_ms']:.3f}\t{row['rust_p99_ms']:.3f}\n"
            )

    result = {
        "schema_version": 1,
        "probe_id": "protocol-shape",
        "status": "pass",
        "commit": commit,
        "hardware": hardware,
        "suite": suite.value,
        "rows": rows,
        "summary": summarize_protocol(rows),
        "artifacts": [{"path": relative(tsv_path)}, {"path": relative(json_path)}],
    }
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def run_limited(cmd: list[str], out_path: Path, timeout_s: int) -> dict[str, Any]:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with out_path.open("w", encoding="utf-8") as out:
            completed = subprocess.run(
                cmd,
                cwd=ROOT,
                stdout=out,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=timeout_s,
            )
        return {"returncode": completed.returncode, "timed_out": False, "path": relative(out_path)}
    except subprocess.TimeoutExpired:
        return {"returncode": None, "timed_out": True, "path": relative(out_path)}


def extract_symbol_counts(path: Path, patterns: tuple[str, ...]) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    counts: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not any(pattern in line for pattern in patterns):
            continue
        cleaned = re.sub(r"\s+", " ", line).strip()
        if not cleaned:
            continue
        counts[cleaned] = counts.get(cleaned, 0) + 1
    return [
        {"line": line, "hits": hits}
        for line, hits in sorted(counts.items(), key=lambda item: item[1], reverse=True)[:40]
    ]


def allocation_workloads(args: argparse.Namespace) -> list[Workload]:
    commands = command_list(args.commands)
    return [
        Workload(
            name=f"{command.value}-p{args.pipeline}-alloc",
            command=command,
            requests=args.requests,
            clients=args.clients,
            pipeline=args.pipeline,
            payload=args.payload,
        )
        for command in commands
    ]


def run_alloc_stacks(args: argparse.Namespace) -> int:
    require_binaries()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PROFILES_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    commit = git_commit()
    hardware = hardware_fingerprint()
    profile_root = PROFILES_DIR / f"{stamp}-{commit}-alloc-stacks"
    tsv_path = RESULTS_DIR / f"{stamp}-{commit}-alloc-stacks.tsv"
    json_path = RESULTS_DIR / f"{stamp}-{commit}-alloc-stacks.json"
    rows = []
    malloc_history = shutil.which("malloc_history")
    heap_tool = shutil.which("heap")
    if malloc_history is None:
        raise RuntimeError("malloc_history not found")

    for workload in allocation_workloads(args):
        workload_dir = profile_root / workload.name
        workload_dir.mkdir(parents=True, exist_ok=True)
        print(f"==> {workload.name}: rust with MallocStackLogging", file=sys.stderr)
        row, proc = run_benchmark(
            Target.RUST,
            workload,
            workload_dir / "rust-server.log",
            {
                "MallocStackLogging": "1",
                "MallocStackLoggingNoCompact": "1",
                "KEEP_SERVER_ALIVE": "1",
            },
        )
        assert proc is not None
        try:
            all_by_count = workload_dir / "malloc-history-all-by-count.txt"
            calltree = workload_dir / "malloc-history-calltree.txt"
            heap_summary = workload_dir / "heap-show-sizes.txt"
            count_status = run_limited([malloc_history, str(proc.pid), "-allByCount"], all_by_count, args.tool_timeout_s)
            calltree_status = run_limited(
                [
                    malloc_history,
                    str(proc.pid),
                    "-callTree",
                    "-collapseRecursion",
                    "-consolidateAllBySymbol",
                    "-ignoreThreads",
                    "-noContent",
                ],
                calltree,
                args.tool_timeout_s,
            )
            heap_status = (
                run_limited([heap_tool, "--showSizes", str(proc.pid)], heap_summary, args.tool_timeout_s)
                if heap_tool
                else {"returncode": None, "timed_out": False, "path": "", "skipped": "heap not found"}
            )
        finally:
            stop_server(proc)

        rows.append(
            {
                "workload": workload.name,
                "command": row.command,
                "requests": workload.requests,
                "clients": workload.clients,
                "pipeline": workload.pipeline,
                "payload": workload.payload,
                "rust_rps": row.rps,
                "rust_p99_ms": row.p99_ms,
                "artifacts": [
                    count_status,
                    calltree_status,
                    heap_status,
                ],
                "redis_symbol_lines": extract_symbol_counts(
                    calltree,
                    ("redis-server", "redis::", "parse_", "encode_", "RedisString", "Command"),
                ),
                "allocator_symbol_lines": extract_symbol_counts(
                    calltree,
                    ("malloc", "free", "realloc", "alloc::", "RawVec", "Vec<"),
                ),
            }
        )

    with tsv_path.open("w", encoding="utf-8") as out:
        out.write("# valkey-rs allocation-stack hypothesis probe\n")
        out.write(f"# timestamp_utc\t{stamp}\n")
        out.write(f"# commit\t{commit}\n")
        out.write(f"# os\t{hardware['os']}\n")
        out.write(f"# arch\t{hardware['arch']}\n")
        out.write(f"# cpu\t{hardware['cpu']}\n")
        out.write("workload\tcommand\trequests\tclients\tpipeline\tpayload\trust_rps\trust_p99_ms\tprofile_dir\n")
        for row in rows:
            out.write(
                f"{row['workload']}\t{row['command']}\t{row['requests']}\t{row['clients']}\t"
                f"{row['pipeline']}\t{row['payload']}\t{row['rust_rps']:.2f}\t"
                f"{row['rust_p99_ms']:.3f}\t{relative(profile_root / row['workload'])}\n"
            )

    result = {
        "schema_version": 1,
        "probe_id": "alloc-stacks",
        "status": "pass",
        "commit": commit,
        "hardware": hardware,
        "rows": rows,
        "artifacts": [{"path": relative(tsv_path)}, {"path": relative(json_path)}, {"path": relative(profile_root)}],
        "note": "MallocStackLogging adds overhead; use stack attribution, not throughput, as the evidence.",
    }
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def run_xctrace_time(args: argparse.Namespace) -> int:
    require_binaries()
    xctrace = shutil.which("xctrace")
    if xctrace is None:
        raise RuntimeError("xctrace not found")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    commit = git_commit()
    hardware = hardware_fingerprint()
    workload = Workload(
        name=f"{args.command}-p{args.pipeline}-time",
        command=CommandKind(args.command),
        requests=args.requests,
        clients=args.clients,
        pipeline=args.pipeline,
        payload=args.payload,
    )
    profile_root = PROFILES_DIR / f"{stamp}-{commit}-xctrace-time" / workload.name
    profile_root.mkdir(parents=True, exist_ok=True)
    json_path = RESULTS_DIR / f"{stamp}-{commit}-xctrace-time.json"
    port = free_port()
    proc: subprocess.Popen[str] | None = None
    xctrace_proc: subprocess.Popen[str] | None = None
    trace_path = profile_root / f"{workload.name}.trace"
    xctrace_log = profile_root / "xctrace.log"
    time_profile_xml = profile_root / "time-profile.xml"
    try:
        proc = start_server(Target.RUST, port, profile_root / "rust-server.log")
        with xctrace_log.open("w", encoding="utf-8") as log:
            xctrace_proc = subprocess.Popen(
                [
                    xctrace,
                    "record",
                    "--quiet",
                    "--no-prompt",
                    "--template",
                    "Time Profiler",
                    "--attach",
                    str(proc.pid),
                    "--time-limit",
                    f"{args.time_limit_s}s",
                    "--output",
                    str(trace_path),
                ],
                cwd=ROOT,
                stdout=log,
                stderr=log,
                text=True,
            )
        time.sleep(args.warmup_s)
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
                workload.command.value,
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
            raise RuntimeError(f"valkey-benchmark failed: {completed.stderr[-2000:]}")
        row = parse_benchmark_csv(completed.stdout, workload, Target.RUST)
        try:
            xctrace_proc.wait(timeout=args.time_limit_s + 15)
        except subprocess.TimeoutExpired:
            xctrace_proc.terminate()
            xctrace_proc.wait(timeout=5)
        export_status = export_xctrace_time_profile(xctrace, trace_path, time_profile_xml)
        cli_profile = parse_xctrace_time_profile(time_profile_xml)
        result = {
            "schema_version": 1,
            "probe_id": "xctrace-time",
            "status": "pass" if xctrace_proc.returncode == 0 else "warn",
            "commit": commit,
            "hardware": hardware,
            "workload": asdict(workload) | {"command": workload.command.value},
            "rust": asdict(row) | {"target": row.target.value},
            "artifacts": [
                {"path": relative(trace_path), "kind": "xctrace-time-profiler-trace"},
                {"path": relative(xctrace_log), "kind": "xctrace-log"},
                {"path": relative(time_profile_xml), "kind": "xctrace-time-profile-xml"},
                {"path": relative(json_path), "kind": "json-summary"},
            ],
            "xctrace_returncode": xctrace_proc.returncode,
            "xctrace_export_status": export_status,
            "cli_profile": cli_profile,
            "note": (
                "This probe is fully command-line: it records a Time Profiler trace, "
                "exports the time-profile table as XML, and aggregates top frames into "
                "cli_profile. The raw .trace can still be opened in Instruments for "
                "manual inspection. The benchmark child is launched with a minimal "
                "environment so the trace does not record operator shell secrets."
            ),
        }
    finally:
        stop_server(proc)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def export_xctrace_time_profile(xctrace: str, trace_path: Path, out_path: Path) -> dict[str, Any]:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        [
            xctrace,
            "export",
            "--input",
            str(trace_path),
            "--xpath",
            '/trace-toc/run[@number="1"]/data/table[@schema="time-profile"]',
            "--output",
            str(out_path),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return {
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-1000:],
        "stderr_tail": completed.stderr[-1000:],
        "path": relative(out_path),
    }


def parse_xctrace_time_profile(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size == 0:
        return {"available": False, "reason": "time-profile XML missing"}

    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        return {"available": False, "reason": f"XML parse error: {exc}"}

    frame_names: dict[str, str] = {}
    weight_values: dict[str, int] = {}
    backtrace_defs: dict[str, list[str]] = {}

    for frame in root.iter("frame"):
        frame_id = frame.attrib.get("id")
        name = frame.attrib.get("name")
        if frame_id and name:
            frame_names[frame_id] = name

    for weight in root.iter("weight"):
        weight_id = weight.attrib.get("id")
        if weight_id and weight.text:
            try:
                weight_values[weight_id] = int(weight.text)
            except ValueError:
                pass

    def frame_name(frame: ET.Element) -> str:
        if ref := frame.attrib.get("ref"):
            return frame_names.get(ref, f"<frame ref={ref}>")
        return frame.attrib.get("name") or "<unknown>"

    for backtrace in root.iter("backtrace"):
        backtrace_id = backtrace.attrib.get("id")
        if not backtrace_id:
            continue
        frames = [frame_name(frame) for frame in backtrace.findall("frame")]
        backtrace_defs[backtrace_id] = frames

    leaf_weight: dict[str, int] = {}
    inclusive_weight: dict[str, int] = {}
    sample_count = 0
    total_weight_ns = 0

    for row in root.iter("row"):
        weight_elem = row.find("weight")
        backtrace_elem = row.find("backtrace")
        if weight_elem is None or backtrace_elem is None:
            continue

        weight = 0
        if ref := weight_elem.attrib.get("ref"):
            weight = weight_values.get(ref, 0)
        elif weight_elem.text:
            try:
                weight = int(weight_elem.text)
            except ValueError:
                weight = 0
        if weight <= 0:
            continue

        if ref := backtrace_elem.attrib.get("ref"):
            frames = backtrace_defs.get(ref, [])
        else:
            frames = [frame_name(frame) for frame in backtrace_elem.findall("frame")]
        if not frames:
            continue

        sample_count += 1
        total_weight_ns += weight
        leaf_weight[frames[0]] = leaf_weight.get(frames[0], 0) + weight
        for frame in frames:
            inclusive_weight[frame] = inclusive_weight.get(frame, 0) + weight

    def top_rows(rows: dict[str, int], limit: int = 25) -> list[dict[str, Any]]:
        out = []
        for frame, ns in sorted(rows.items(), key=lambda item: item[1], reverse=True)[:limit]:
            out.append(
                {
                    "frame": frame,
                    "weight_ms": ns / 1_000_000.0,
                    "pct": (ns / total_weight_ns * 100.0) if total_weight_ns else 0.0,
                }
            )
        return out

    return {
        "available": True,
        "sample_count": sample_count,
        "total_weight_ms": total_weight_ns / 1_000_000.0,
        "top_self": top_rows(leaf_weight),
        "top_inclusive": top_rows(inclusive_weight),
        "note": "Weights are Time Profiler sample weights, not exact counters.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run direct valkey-rs performance hypothesis probes")
    sub = parser.add_subparsers(dest="probe", required=True)

    protocol = sub.add_parser("protocol-shape", help="compare ratios across pipeline depth and payload size")
    protocol.add_argument("--suite", choices=[suite.value for suite in ProbeSuite], default=ProbeSuite.SMOKE.value)
    protocol.add_argument("--commands", default="ping_inline,ping_mbulk,get,set,incr")
    protocol.add_argument("--pipelines", default="1,16,100")
    protocol.add_argument("--payloads", default="8,64,1024")
    protocol.add_argument("--clients", type=int, default=50)
    protocol.set_defaults(func=run_protocol_shape)

    allocs = sub.add_parser("alloc-stacks", help="capture malloc_history/heap output for selected Rust workloads")
    allocs.add_argument("--commands", default="get,set,incr,ping_mbulk")
    allocs.add_argument("--requests", type=int, default=500_000)
    allocs.add_argument("--clients", type=int, default=50)
    allocs.add_argument("--pipeline", type=int, default=100)
    allocs.add_argument("--payload", type=int, default=64)
    allocs.add_argument("--tool-timeout-s", type=int, default=45)
    allocs.set_defaults(func=run_alloc_stacks)

    time_prof = sub.add_parser("xctrace-time", help="record an Instruments Time Profiler trace for one Rust workload")
    time_prof.add_argument("--command", choices=[command.value for command in CommandKind], default=CommandKind.GET.value)
    time_prof.add_argument("--requests", type=int, default=2_000_000)
    time_prof.add_argument("--clients", type=int, default=50)
    time_prof.add_argument("--pipeline", type=int, default=100)
    time_prof.add_argument("--payload", type=int, default=64)
    time_prof.add_argument("--time-limit-s", type=int, default=8)
    time_prof.add_argument("--warmup-s", type=float, default=0.75)
    time_prof.set_defaults(func=run_xctrace_time)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
