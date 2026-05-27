#!/usr/bin/env python3
"""JSON document cache workload for redis-rs performance triage.

This is telemetry, not a public benchmark claim. It drives both upstream
Valkey and the Rust server with the same Python RESP client and real
JSON-shaped string payloads. The point is to add a more app-like workload than
the tiny default-suite payloads:

  * preload N keys with larger JSON documents;
  * run GET-heavy cache reads;
  * run SET update writes;
  * run an 80/15/5 GET/SET/MGET mixed workload.

Artifacts:
  harness/bench/results/<ts>-<sha>-json-doc-mix.tsv
  harness/bench/results/<ts>-<sha>-json-doc-mix.json
  harness/bench/results/<ts>-{reference,rust}-json-doc-mix.log
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import random
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
VALKEY_BIN = ROOT / "reference/valkey/src/valkey-server"
RUST_BIN = ROOT / "target/release/redis-server"
RESULTS_DIR = ROOT / "harness/bench/results"


class Target(Enum):
    REFERENCE = "reference"
    RUST = "rust"


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str


SCENARIOS: dict[str, Scenario] = {
    "get": Scenario("get", "100% GET of preloaded JSON documents"),
    "set": Scenario("set", "100% SET updates with JSON documents"),
    "mixed": Scenario("mixed", "80% GET, 15% SET, 5% MGET(4) JSON cache mix"),
}


class RespConnection:
    def __init__(self, port: int) -> None:
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5.0)
        self.sock.settimeout(30.0)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = bytearray()

    def close(self) -> None:
        self.sock.close()

    def send(self, payload: bytes) -> None:
        self.sock.sendall(payload)

    def _fill(self, n: int) -> None:
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("server closed connection")
            self.buf.extend(chunk)

    def read_exact(self, n: int) -> bytes:
        self._fill(n)
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out

    def read_line(self) -> bytes:
        while True:
            idx = self.buf.find(b"\r\n")
            if idx >= 0:
                out = bytes(self.buf[:idx])
                del self.buf[: idx + 2]
                return out
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("server closed connection")
            self.buf.extend(chunk)

    def read_reply(self) -> None:
        prefix = self.read_exact(1)
        if prefix in (b"+", b"-", b":"):
            self.read_line()
            return
        if prefix == b"$":
            length = int(self.read_line())
            if length >= 0:
                self.read_exact(length + 2)
            return
        if prefix == b"*":
            count = int(self.read_line())
            if count >= 0:
                for _ in range(count):
                    self.read_reply()
            return
        raise RuntimeError(f"unknown RESP prefix {prefix!r}")


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


def require_binaries(build: bool) -> None:
    if not VALKEY_BIN.exists():
        subprocess.run(["bash", "scripts/setup-reference.sh"], cwd=ROOT, check=True)
    if build and (os.environ.get("VALKEY_BENCH_SKIP_BUILD") != "1" or not RUST_BIN.exists()):
        subprocess.run(["cargo", "build", "--release", "-p", "redis-server"], cwd=ROOT, check=True)
    missing = [path for path in [VALKEY_BIN, RUST_BIN] if not os.access(path, os.X_OK)]
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


def encode_command(parts: list[bytes]) -> bytes:
    out = bytearray()
    out.extend(f"*{len(parts)}\r\n".encode("ascii"))
    for part in parts:
        out.extend(f"${len(part)}\r\n".encode("ascii"))
        out.extend(part)
        out.extend(b"\r\n")
    return bytes(out)


def make_json_doc(index: int, target_bytes: int) -> bytes:
    body_len = max(0, target_bytes - 260)
    doc: dict[str, Any] = {
        "id": index,
        "tenant": f"tenant-{index % 97}",
        "type": "checkout_session",
        "status": "active" if index % 5 else "archived",
        "version": index % 1000,
        "updated_at": "2026-05-27T00:00:00Z",
        "cart": {
            "currency": "USD",
            "subtotal": (index * 17) % 50000,
            "items": [
                {"sku": f"sku-{index % 1009}", "qty": 1 + (index % 3)},
                {"sku": f"sku-{(index + 19) % 1009}", "qty": 1},
            ],
        },
        "flags": ["mobile", "ab-test-b", "risk-ok"],
        "profile": {
            "country": "US",
            "loyalty": index % 11,
            "segments": ["repeat", "spring", "email"],
        },
        "body": "",
    }
    doc["body"] = ("json-cache-document-payload-" * ((body_len // 28) + 1))[:body_len]
    payload = json.dumps(doc, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(payload) > target_bytes:
        excess = len(payload) - target_bytes
        doc["body"] = doc["body"][:-excess] if excess < len(doc["body"]) else ""
        payload = json.dumps(doc, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return payload


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, math.ceil((pct / 100.0) * len(ordered)) - 1))
    return ordered[idx]


def preload(port: int, set_frames: list[bytes], batch_size: int) -> float:
    conn = RespConnection(port)
    started = time.perf_counter()
    try:
        for start in range(0, len(set_frames), batch_size):
            batch = set_frames[start : start + batch_size]
            conn.send(b"".join(batch))
            for _ in batch:
                conn.read_reply()
    finally:
        conn.close()
    return time.perf_counter() - started


def build_frames(keyspace: int, doc_bytes: int) -> tuple[list[bytes], list[bytes], int]:
    get_frames: list[bytes] = []
    set_frames: list[bytes] = []
    total_doc_bytes = 0
    for i in range(keyspace):
        key = f"doc:{i}".encode("ascii")
        payload = make_json_doc(i, doc_bytes)
        total_doc_bytes += len(payload)
        get_frames.append(encode_command([b"GET", key]))
        set_frames.append(encode_command([b"SET", key, payload]))
    avg_doc_bytes = int(total_doc_bytes / keyspace) if keyspace else 0
    return get_frames, set_frames, avg_doc_bytes


def choose_frame(
    scenario: str,
    rng: random.Random,
    get_frames: list[bytes],
    set_frames: list[bytes],
) -> bytes:
    keyspace = len(get_frames)
    if scenario == "get":
        return get_frames[rng.randrange(keyspace)]
    if scenario == "set":
        return set_frames[rng.randrange(keyspace)]
    roll = rng.random()
    if roll < 0.80:
        return get_frames[rng.randrange(keyspace)]
    if roll < 0.95:
        return set_frames[rng.randrange(keyspace)]
    keys = [f"doc:{rng.randrange(keyspace)}".encode("ascii") for _ in range(4)]
    return encode_command([b"MGET", *keys])


def worker(
    port: int,
    scenario: str,
    requests: int,
    pipeline: int,
    seed: int,
    get_frames: list[bytes],
    set_frames: list[bytes],
    barrier: threading.Barrier,
    out: dict[str, Any],
) -> None:
    rng = random.Random(seed)
    conn = RespConnection(port)
    latencies: list[float] = []
    completed = 0
    try:
        barrier.wait()
        while completed < requests:
            batch_count = min(pipeline, requests - completed)
            frames = [choose_frame(scenario, rng, get_frames, set_frames) for _ in range(batch_count)]
            started = time.perf_counter()
            conn.send(b"".join(frames))
            for _ in frames:
                conn.read_reply()
            elapsed = time.perf_counter() - started
            per_op = elapsed / batch_count
            latencies.extend([per_op] * batch_count)
            completed += batch_count
        out["completed"] = completed
        out["latencies"] = latencies
    except BaseException as exc:
        out["error"] = repr(exc)
        out["completed"] = completed
        out["latencies"] = latencies
    finally:
        conn.close()


def run_scenario(
    port: int,
    scenario: str,
    requests: int,
    clients: int,
    pipeline: int,
    seed: int,
    get_frames: list[bytes],
    set_frames: list[bytes],
) -> dict[str, Any]:
    per_client = requests // clients
    remainder = requests % clients
    barrier = threading.Barrier(clients + 1)
    results: list[dict[str, Any]] = [{} for _ in range(clients)]
    threads: list[threading.Thread] = []
    for idx in range(clients):
        client_requests = per_client + (1 if idx < remainder else 0)
        thread = threading.Thread(
            target=worker,
            args=(
                port,
                scenario,
                client_requests,
                pipeline,
                seed + idx * 997,
                get_frames,
                set_frames,
                barrier,
                results[idx],
            ),
            daemon=True,
        )
        threads.append(thread)
        thread.start()

    started = time.perf_counter()
    barrier.wait()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - started

    errors = [result["error"] for result in results if "error" in result]
    completed = sum(int(result.get("completed", 0)) for result in results)
    latencies = [value for result in results for value in result.get("latencies", [])]
    status = "error" if errors else "ok"
    return {
        "status": status,
        "errors": errors[:5],
        "requests": requests,
        "completed": completed,
        "elapsed_s": elapsed,
        "rps": completed / elapsed if elapsed > 0 else 0.0,
        "avg_ms": (sum(latencies) / len(latencies) * 1000.0) if latencies else 0.0,
        "p50_ms": percentile(latencies, 50) * 1000.0,
        "p90_ms": percentile(latencies, 90) * 1000.0,
        "p95_ms": percentile(latencies, 95) * 1000.0,
        "p99_ms": percentile(latencies, 99) * 1000.0,
        "max_ms": max(latencies) * 1000.0 if latencies else 0.0,
    }


def run_target(
    target: Target,
    scenarios: list[str],
    args: argparse.Namespace,
    stamp: str,
    get_frames: list[bytes],
    set_frames: list[bytes],
) -> dict[str, Any]:
    port = free_port()
    log_path = RESULTS_DIR / f"{stamp}-{target.value}-json-doc-mix.log"
    proc: subprocess.Popen[str] | None = None
    try:
        proc = start_server(target, port, log_path)
        preload_s = preload(port, set_frames, args.preload_pipeline)
        rows: dict[str, Any] = {}
        for scenario in scenarios:
            rows[scenario] = run_scenario(
                port,
                scenario,
                args.requests,
                args.clients,
                args.pipeline,
                args.seed,
                get_frames,
                set_frames,
            )
        return {
            "target": target.value,
            "port": port,
            "log_path": relative(log_path),
            "preload_s": preload_s,
            "rows": rows,
        }
    finally:
        stop_server(proc)


def write_tsv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as out:
        out.write(
            "scenario\tdescription\trequests\tclients\tpipeline\tdoc_bytes\tkeyspace\tstatus\t"
            "reference_rps\trust_rps\tratio\treference_p50_ms\trust_p50_ms\t"
            "reference_p90_ms\trust_p90_ms\treference_p95_ms\trust_p95_ms\t"
            "reference_p99_ms\trust_p99_ms\n"
        )
        for row in rows:
            out.write(
                f"{row['scenario']}\t{row['description']}\t{row['requests']}\t"
                f"{row['clients']}\t{row['pipeline']}\t{row['doc_bytes']}\t"
                f"{row['keyspace']}\t{row['status']}\t{row['reference_rps']:.2f}\t"
                f"{row['rust_rps']:.2f}\t{row['ratio']:.6f}\t"
                f"{row['reference_p50_ms']:.3f}\t{row['rust_p50_ms']:.3f}\t"
                f"{row['reference_p90_ms']:.3f}\t{row['rust_p90_ms']:.3f}\t"
                f"{row['reference_p95_ms']:.3f}\t{row['rust_p95_ms']:.3f}\t"
                f"{row['reference_p99_ms']:.3f}\t{row['rust_p99_ms']:.3f}\n"
            )


def csv_list(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a JSON document cache workload")
    parser.add_argument("--scenarios", default="get,set,mixed")
    parser.add_argument("--requests", type=int, default=30_000)
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--pipeline", type=int, default=1)
    parser.add_argument("--keyspace", type=int, default=5_000)
    parser.add_argument("--doc-bytes", type=int, default=4096)
    parser.add_argument("--preload-pipeline", type=int, default=256)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--no-build", action="store_true", help="Use existing target/release/redis-server")
    args = parser.parse_args()

    scenarios = csv_list(args.scenarios)
    unknown = [scenario for scenario in scenarios if scenario not in SCENARIOS]
    if unknown:
        raise SystemExit(f"unknown scenarios: {unknown}; choices: {sorted(SCENARIOS)}")
    if args.clients <= 0 or args.requests <= 0 or args.pipeline <= 0 or args.keyspace <= 0:
        raise SystemExit("clients, requests, pipeline, and keyspace must be positive")

    require_binaries(build=not args.no_build)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    commit = git_commit()
    get_frames, set_frames, avg_doc_bytes = build_frames(args.keyspace, args.doc_bytes)

    reference = run_target(Target.REFERENCE, scenarios, args, stamp, get_frames, set_frames)
    rust = run_target(Target.RUST, scenarios, args, stamp, get_frames, set_frames)

    rows: list[dict[str, Any]] = []
    for scenario in scenarios:
        ref_row = reference["rows"][scenario]
        rust_row = rust["rows"][scenario]
        status = "ok" if ref_row["status"] == "ok" and rust_row["status"] == "ok" else "error"
        ratio = rust_row["rps"] / ref_row["rps"] if ref_row["rps"] > 0 else 0.0
        rows.append(
            {
                "scenario": scenario,
                "description": SCENARIOS[scenario].description,
                "requests": args.requests,
                "clients": args.clients,
                "pipeline": args.pipeline,
                "doc_bytes": avg_doc_bytes,
                "keyspace": args.keyspace,
                "status": status,
                "ratio": ratio,
                "reference_rps": ref_row["rps"],
                "rust_rps": rust_row["rps"],
                "reference_p50_ms": ref_row["p50_ms"],
                "rust_p50_ms": rust_row["p50_ms"],
                "reference_p90_ms": ref_row["p90_ms"],
                "rust_p90_ms": rust_row["p90_ms"],
                "reference_p95_ms": ref_row["p95_ms"],
                "rust_p95_ms": rust_row["p95_ms"],
                "reference_p99_ms": ref_row["p99_ms"],
                "rust_p99_ms": rust_row["p99_ms"],
                "targets": {
                    "reference": ref_row,
                    "rust": rust_row,
                },
            }
        )

    tsv_path = RESULTS_DIR / f"{stamp}-{commit}-json-doc-mix.tsv"
    json_path = RESULTS_DIR / f"{stamp}-{commit}-json-doc-mix.json"
    write_tsv(tsv_path, rows)

    ok_ratios = [row["ratio"] for row in rows if row["status"] == "ok"]
    result = {
        "schema_version": 1,
        "probe_id": "json-doc-mix",
        "status": "pass" if all(row["status"] == "ok" for row in rows) else "fail",
        "commit": commit,
        "hardware": hardware_fingerprint(),
        "note": (
            "Telemetry only. This uses a Python RESP client and can become "
            "client-limited; use ratios and latency shape, not public claims."
        ),
        "parameters": {
            "scenarios": args.scenarios,
            "requests": args.requests,
            "clients": args.clients,
            "pipeline": args.pipeline,
            "keyspace": args.keyspace,
            "requested_doc_bytes": args.doc_bytes,
            "actual_avg_doc_bytes": avg_doc_bytes,
            "preload_pipeline": args.preload_pipeline,
            "seed": args.seed,
            "no_build": args.no_build,
        },
        "targets": {
            "reference": {
                "preload_s": reference["preload_s"],
                "log_path": reference["log_path"],
            },
            "rust": {
                "preload_s": rust["preload_s"],
                "log_path": rust["log_path"],
            },
        },
        "rows": rows,
        "summary": {
            "ok": sum(1 for row in rows if row["status"] == "ok"),
            "total": len(rows),
            "median_ratio": sorted(ok_ratios)[len(ok_ratios) // 2] if ok_ratios else 0.0,
            "min_ratio": min(ok_ratios) if ok_ratios else 0.0,
            "weakest_ratios": sorted(
                (
                    {
                        "scenario": row["scenario"],
                        "ratio": row["ratio"],
                        "reference_rps": row["reference_rps"],
                        "rust_rps": row["rust_rps"],
                    }
                    for row in rows
                    if row["status"] == "ok"
                ),
                key=lambda item: item["ratio"],
            ),
        },
        "artifacts": [
            {"path": relative(tsv_path)},
            {"path": relative(json_path)},
        ],
    }
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
