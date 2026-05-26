#!/usr/bin/env python3
"""Fast pipeline-depth smoke probe for valkey-rs.

This is an operator probe, not a public benchmark claim. It runs a small
side-by-side matrix with tight per-cell timeouts so pipeline regressions become
cheap to classify and safe to bisect.

Artifacts:
  harness/bench/results/<ts>-<sha>-pipeline-smoke.tsv
  harness/bench/results/<ts>-<sha>-pipeline-smoke.json
  harness/bench/results/<ts>-{reference,rust}-pipeline-smoke-<workload>.log
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import socket
import subprocess
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


class Target(Enum):
    REFERENCE = "reference"
    RUST = "rust"


@dataclass(frozen=True)
class Workload:
    name: str
    command: str
    requests: int
    clients: int
    pipeline: int
    payload: int


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


def start_server(target: Target, port: int, log_path: Path) -> subprocess.Popen[str]:
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


def parse_csv(stdout: str) -> dict[str, Any]:
    reader = csv.reader(stdout.splitlines())
    header = next(reader, None)
    if not header:
        raise RuntimeError("valkey-benchmark emitted no CSV header")
    row = next(reader, None)
    if row is None or len(row) < 8:
        raise RuntimeError(f"valkey-benchmark emitted no parseable row: {stdout[-500:]}")
    return {
        "command": row[0],
        "rps": float(row[1]),
        "avg_ms": float(row[2]),
        "min_ms": float(row[3]),
        "p50_ms": float(row[4]),
        "p95_ms": float(row[5]),
        "p99_ms": float(row[6]),
        "max_ms": float(row[7]),
    }


def run_benchmark(target: Target, workload: Workload, stamp: str, timeout_s: int) -> dict[str, Any]:
    port = free_port()
    proc: subprocess.Popen[str] | None = None
    log_path = RESULTS_DIR / f"{stamp}-{target.value}-pipeline-smoke-{workload.name}.log"
    started = time.monotonic()
    try:
        proc = start_server(target, port, log_path)
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
            "3",
        ]
        try:
            completed = subprocess.run(
                cmd,
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=timeout_s,
            )
        except subprocess.TimeoutExpired:
            return {
                "target": target.value,
                "status": "timeout",
                "elapsed_s": time.monotonic() - started,
                "timeout_s": timeout_s,
                "log_path": relative(log_path),
                "command_line": cmd,
            }
        if completed.returncode != 0:
            return {
                "target": target.value,
                "status": "error",
                "elapsed_s": time.monotonic() - started,
                "returncode": completed.returncode,
                "stderr_tail": completed.stderr[-1000:],
                "log_path": relative(log_path),
                "command_line": cmd,
            }
        row = parse_csv(completed.stdout)
        row.update(
            {
                "target": target.value,
                "status": "ok",
                "elapsed_s": time.monotonic() - started,
                "log_path": relative(log_path),
                "command_line": cmd,
            }
        )
        return row
    finally:
        stop_server(proc)


def csv_list(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def int_list(raw: str) -> list[int]:
    values = []
    for item in csv_list(raw):
        values.append(int(item))
    if not values:
        raise ValueError(f"empty integer list: {raw!r}")
    return values


def request_count(args: argparse.Namespace, pipeline: int) -> int:
    return args.requests_p1 if pipeline == 1 else args.requests_pipelined


def build_workloads(args: argparse.Namespace) -> list[Workload]:
    workloads = []
    for command in csv_list(args.commands):
        for pipeline in int_list(args.pipelines):
            workloads.append(
                Workload(
                    name=f"{command}-p{pipeline}",
                    command=command,
                    requests=request_count(args, pipeline),
                    clients=args.clients,
                    pipeline=pipeline,
                    payload=args.payload,
                )
            )
    return workloads


def write_tsv(path: Path, stamp: str, commit: str, hardware: dict[str, str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as out:
        out.write("# valkey-rs pipeline smoke probe\n")
        out.write(f"# timestamp_utc\t{stamp}\n")
        out.write(f"# commit\t{commit}\n")
        out.write(f"# os\t{hardware['os']}\n")
        out.write(f"# arch\t{hardware['arch']}\n")
        out.write(f"# cpu\t{hardware['cpu']}\n")
        out.write(
            "workload\tcommand\trequests\tclients\tpipeline\tpayload\tstatus\treference_rps\trust_rps\t"
            "ratio\treference_elapsed_s\trust_elapsed_s\treference_p99_ms\trust_p99_ms\n"
        )
        for row in rows:
            out.write(
                f"{row['workload']}\t{row['command']}\t{row['requests']}\t{row['clients']}\t"
                f"{row['pipeline']}\t{row['payload']}\t{row['status']}\t"
                f"{row.get('reference_rps', 0.0):.2f}\t{row.get('rust_rps', 0.0):.2f}\t"
                f"{row.get('ratio', 0.0):.6f}\t"
                f"{row.get('reference_elapsed_s', 0.0):.3f}\t{row.get('rust_elapsed_s', 0.0):.3f}\t"
                f"{row.get('reference_p99_ms', 0.0):.3f}\t{row.get('rust_p99_ms', 0.0):.3f}\n"
            )


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ok = [row for row in rows if row["status"] == "ok"]
    ratios = [row["ratio"] for row in ok]
    by_pipeline: dict[str, list[float]] = {}
    for row in ok:
        by_pipeline.setdefault(str(row["pipeline"]), []).append(row["ratio"])
    return {
        "ok": len(ok),
        "total": len(rows),
        "timeouts": [row["workload"] for row in rows if row["status"] == "timeout"],
        "errors": [row["workload"] for row in rows if row["status"] == "error"],
        "min_ratio": min(ratios) if ratios else None,
        "median_ratio": sorted(ratios)[len(ratios) // 2] if ratios else None,
        "by_pipeline_median": {
            pipeline: sorted(values)[len(values) // 2] for pipeline, values in sorted(by_pipeline.items())
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a bounded pipeline smoke benchmark")
    parser.add_argument("--commands", default="get,ping_mbulk,set")
    parser.add_argument("--pipelines", default="1,16,100")
    parser.add_argument("--requests-p1", type=int, default=20_000)
    parser.add_argument("--requests-pipelined", type=int, default=200_000)
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--payload", type=int, default=64)
    parser.add_argument("--timeout-s", type=int, default=20)
    parser.add_argument(
        "--fail-below-p100",
        type=float,
        default=0.0,
        help="Exit non-zero when any successful P100 ratio is below this threshold.",
    )
    args = parser.parse_args()

    require_binaries()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    commit = git_commit()
    hardware = hardware_fingerprint()
    rows = []
    for workload in build_workloads(args):
        print(f"==> {workload.name}: reference", flush=True)
        reference = run_benchmark(Target.REFERENCE, workload, stamp, args.timeout_s)
        print(f"==> {workload.name}: rust", flush=True)
        rust = run_benchmark(Target.RUST, workload, stamp, args.timeout_s)
        status = "ok" if reference["status"] == "ok" and rust["status"] == "ok" else rust["status"]
        if reference["status"] != "ok":
            status = reference["status"]
        ratio = rust.get("rps", 0.0) / reference.get("rps", 0.0) if reference.get("rps") else 0.0
        rows.append(
            {
                "workload": workload.name,
                "command": reference.get("command", workload.command.upper()),
                "requests": workload.requests,
                "clients": workload.clients,
                "pipeline": workload.pipeline,
                "payload": workload.payload,
                "status": status,
                "ratio": ratio,
                "reference_rps": reference.get("rps", 0.0),
                "rust_rps": rust.get("rps", 0.0),
                "reference_elapsed_s": reference.get("elapsed_s", 0.0),
                "rust_elapsed_s": rust.get("elapsed_s", 0.0),
                "reference_p99_ms": reference.get("p99_ms", 0.0),
                "rust_p99_ms": rust.get("p99_ms", 0.0),
                "reference": reference,
                "rust": rust,
            }
        )

    tsv_path = RESULTS_DIR / f"{stamp}-{commit}-pipeline-smoke.tsv"
    json_path = RESULTS_DIR / f"{stamp}-{commit}-pipeline-smoke.json"
    write_tsv(tsv_path, stamp, commit, hardware, rows)
    summary = summarize(rows)
    result = {
        "schema_version": 1,
        "probe_id": "pipeline-smoke",
        "status": "pass",
        "commit": commit,
        "hardware": hardware,
        "parameters": vars(args),
        "summary": summary,
        "rows": rows,
        "artifacts": [{"path": relative(tsv_path)}, {"path": relative(json_path)}],
        "note": "Telemetry only. Use this for quick classification and bisect gates, not public claims.",
    }
    if summary["timeouts"] or summary["errors"]:
        result["status"] = "fail"
    if args.fail_below_p100 > 0:
        for row in rows:
            if row["pipeline"] == 100 and row["status"] == "ok" and row["ratio"] < args.fail_below_p100:
                result["status"] = "fail"
                break
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
