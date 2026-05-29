#!/usr/bin/env python3
"""Paired pipeline=1 parity probe for the per-request-overhead regime.

The release matrix (`run-profile-matrix.sh`) runs a single 50k trial per
command. At pipeline=1 a single trial has roughly +/-5% run-to-run noise, which
is the same size as the parity gap we are chasing -- so one trial cannot tell
0.95x from 1.02x. This probe answers the narrow question "are our p=1 GET/SET
below parity, and by how much, robustly?"

Method: start BOTH servers once and keep them warm, then alternate the
benchmark client between them trial-by-trial. Each trial is a (reference, rust)
pair measured back-to-back, so thermal/scheduler drift cancels within the pair.
We collect the distribution of paired ratios and report the median plus the
inter-quartile spread, which is the statistic that actually settles the
question.

This is a telemetry probe (same category as probe-hypotheses.py), not a
release-table runner. Artifacts land under harness/bench/results/.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import socket
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
VALKEY_BIN = ROOT / "reference/valkey/src/valkey-server"
VALKEY_BENCH = ROOT / "reference/valkey/src/valkey-benchmark"
RUST_BIN = ROOT / "target/release/redis-server"
RESULTS_DIR = ROOT / "harness/bench/results"


def run_text(cmd: list[str]) -> str:
    try:
        return subprocess.check_output(cmd, cwd=ROOT, text=True, stderr=subprocess.DEVNULL).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return ""


def git_commit() -> str:
    return run_text(["git", "rev-parse", "--short", "HEAD"]) or "unknown"


def hardware_fingerprint() -> dict[str, str]:
    return {
        "os": run_text(["uname", "-sr"]) or "unknown",
        "arch": run_text(["uname", "-m"]) or "unknown",
        "cpu": run_text(["sysctl", "-n", "machdep.cpu.brand_string"]) or "unknown",
    }


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


def start_server(target: str, port: int) -> subprocess.Popen:
    if target == "reference":
        cmd = [
            str(VALKEY_BIN), "--port", str(port), "--bind", "127.0.0.1",
            "--save", "", "--appendonly", "no", "--daemonize", "no",
            "--loglevel", "warning",
        ]
    else:
        cmd = [
            str(RUST_BIN), "--port", str(port), "--bind", "127.0.0.1",
            "--rdb-disabled", "--appendonly", "no",
        ]
    log = open(f"/tmp/p1-parity-{target}-{port}.log", "w", encoding="utf-8")
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


@dataclass
class Trial:
    rps: float
    p50_ms: float
    p99_ms: float


def bench_once(port: int, command: str, requests: int, clients: int, payload: int, pipeline: int = 1) -> Trial:
    completed = subprocess.run(
        [
            str(VALKEY_BENCH), "-h", "127.0.0.1", "-p", str(port),
            "-n", str(requests), "-c", str(clients), "-P", str(pipeline),
            "-d", str(payload), "-t", command, "--csv", "--precision", "2",
        ],
        cwd=ROOT, capture_output=True, text=True, timeout=300,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"valkey-benchmark failed: {completed.stderr[-1000:]}")
    reader = csv.reader(completed.stdout.splitlines())
    next(reader, None)
    row = next(reader, None)
    if row is None or len(row) < 7:
        raise RuntimeError(f"unparseable benchmark output: {completed.stdout[-300:]}")
    return Trial(rps=float(row[1]), p50_ms=float(row[4]), p99_ms=float(row[6]))


def quartiles(values: list[float]) -> tuple[float, float, float]:
    ordered = sorted(values)
    med = statistics.median(ordered)
    lo = statistics.median(ordered[: len(ordered) // 2]) if len(ordered) > 1 else ordered[0]
    hi = statistics.median(ordered[(len(ordered) + 1) // 2 :]) if len(ordered) > 1 else ordered[0]
    return lo, med, hi


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commands", default="get,set",
                        help="comma list among get,set,incr,ping_mbulk")
    parser.add_argument("--trials", type=int, default=15)
    parser.add_argument("--requests", type=int, default=50_000)
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--payload", type=int, default=64)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--pipeline", type=int, default=1)
    args = parser.parse_args()

    if not (os.access(VALKEY_BIN, os.X_OK) and os.access(VALKEY_BENCH, os.X_OK) and os.access(RUST_BIN, os.X_OK)):
        raise RuntimeError("missing reference/rust/benchmark binary; build them first")

    commands = [c.strip().lower() for c in args.commands.split(",") if c.strip()]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    commit = git_commit()

    ref_port = free_port()
    rust_port = free_port()
    ref_proc = rust_proc = None
    per_command: dict[str, dict] = {}
    try:
        print(f"==> starting servers (reference :{ref_port}, rust :{rust_port})", file=sys.stderr)
        ref_proc = start_server("reference", ref_port)
        rust_proc = start_server("rust", rust_port)

        for command in commands:
            print(f"==> warming {command} ({args.warmups} passes/server)", file=sys.stderr)
            for _ in range(args.warmups):
                bench_once(ref_port, command, args.requests, args.clients, args.payload, args.pipeline)
                bench_once(rust_port, command, args.requests, args.clients, args.payload, args.pipeline)

            ratios: list[float] = []
            ref_rps: list[float] = []
            rust_rps: list[float] = []
            ref_p99: list[float] = []
            rust_p99: list[float] = []
            for trial in range(args.trials):
                ref = bench_once(ref_port, command, args.requests, args.clients, args.payload, args.pipeline)
                rust = bench_once(rust_port, command, args.requests, args.clients, args.payload, args.pipeline)
                ratio = rust.rps / ref.rps if ref.rps else 0.0
                ratios.append(ratio)
                ref_rps.append(ref.rps)
                rust_rps.append(rust.rps)
                ref_p99.append(ref.p99_ms)
                rust_p99.append(rust.p99_ms)
                print(
                    f"    {command} trial {trial + 1}/{args.trials}: "
                    f"ref {ref.rps:>10.0f}  rust {rust.rps:>10.0f}  ratio {ratio:.4f}",
                    file=sys.stderr,
                )

            lo, med, hi = quartiles(ratios)
            per_command[command] = {
                "trials": args.trials,
                "ratio_median": med,
                "ratio_q1": lo,
                "ratio_q3": hi,
                "ratio_min": min(ratios),
                "ratio_max": max(ratios),
                "reference_rps_median": statistics.median(ref_rps),
                "rust_rps_median": statistics.median(rust_rps),
                "reference_p99_ms_median": statistics.median(ref_p99),
                "rust_p99_ms_median": statistics.median(rust_p99),
                "ratios": [round(r, 4) for r in ratios],
            }
    finally:
        stop_server(ref_proc)
        stop_server(rust_proc)

    result = {
        "probe_id": "p1-parity",
        "timestamp_utc": stamp,
        "commit": commit,
        "hardware": hardware_fingerprint(),
        "config": {
            "pipeline": args.pipeline,
            "requests": args.requests,
            "clients": args.clients,
            "payload": args.payload,
            "trials": args.trials,
            "warmups": args.warmups,
        },
        "commands": per_command,
    }

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RESULTS_DIR / f"{stamp}-{commit}-p1-parity.json"
    out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print(f"\n=== p={args.pipeline} parity (paired, {args.trials} trials, median ratio) ===", file=sys.stderr)
    for command, summary in per_command.items():
        verdict = "ABOVE parity" if summary["ratio_median"] >= 1.0 else "below parity"
        print(
            f"  {command:<10} median {summary['ratio_median']:.4f}x "
            f"[Q1 {summary['ratio_q1']:.4f}, Q3 {summary['ratio_q3']:.4f}]  "
            f"({verdict})",
            file=sys.stderr,
        )
    print(f"\nwrote {out_path.relative_to(ROOT)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
