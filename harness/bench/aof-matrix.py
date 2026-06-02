#!/usr/bin/env python3
"""AOF append/fsync overhead benchmark.

This runner is telemetry, not a public performance claim. It runs the same
write workloads against reference Valkey and the Rust server across
appendonly-disabled and appendfsync modes, then records throughput, latency,
AOF bytes, and overhead relative to appendonly no.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
VALKEY_BIN = ROOT / "reference/valkey/src/valkey-server"
RUST_BIN = ROOT / "target/release/redis-server"
RESULTS_DIR = ROOT / "harness/bench/results"


class RespClient:
    def __init__(self, port: int):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.sock.settimeout(30)
        self.buf = b""

    def close(self) -> None:
        self.sock.close()

    def sendall(self, data: bytes) -> None:
        self.sock.sendall(data)

    def command(self, *parts: str | bytes) -> Any:
        self.sendall(encode_command(parts))
        return self.read()

    def _fill(self, n: int = 1) -> None:
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError(self.buf)
            self.buf += chunk

    def _line(self) -> bytes:
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError(self.buf)
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def read(self) -> Any:
        self._fill(1)
        typ = self.buf[:1]
        self.buf = self.buf[1:]
        if typ == b"+":
            return self._line().decode("utf-8", "replace")
        if typ == b"-":
            raise RuntimeError(self._line().decode("utf-8", "replace"))
        if typ == b":":
            return int(self._line())
        if typ == b"$":
            n = int(self._line())
            if n < 0:
                return None
            self._fill(n + 2)
            data, self.buf = self.buf[:n], self.buf[n + 2 :]
            return data
        if typ == b"*":
            n = int(self._line())
            if n < 0:
                return None
            return [self.read() for _ in range(n)]
        raise ValueError((typ, self.buf[:80]))


@dataclass(frozen=True)
class Cell:
    target: str
    aof_mode: str
    workload: str
    pipeline: int


def encode_command(parts: tuple[str | bytes, ...] | list[str | bytes]) -> bytes:
    out = f"*{len(parts)}\r\n".encode()
    for part in parts:
        if isinstance(part, str):
            part = part.encode()
        out += f"${len(part)}\r\n".encode() + part + b"\r\n"
    return out


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


def git_dirty() -> list[str]:
    out = run_text(["git", "status", "--short"], timeout=10)
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


def wait_for_port(port: int, proc: subprocess.Popen[str], deadline_s: float = 8.0) -> None:
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            if proc.poll() is not None:
                raise RuntimeError(f"server exited during startup: {proc.returncode}")
            time.sleep(0.05)
    raise RuntimeError(f"server did not listen on 127.0.0.1:{port}")


def server_command(target: str, port: int, data_dir: Path, aof_mode: str) -> list[str]:
    appendonly = "no" if aof_mode == "disabled" else "yes"
    fsync = "everysec" if aof_mode == "disabled" else aof_mode
    if target == "reference":
        return [
            str(VALKEY_BIN),
            "--port",
            str(port),
            "--bind",
            "127.0.0.1",
            "--dir",
            str(data_dir),
            "--save",
            "",
            "--appendonly",
            appendonly,
            "--appendfsync",
            fsync,
            "--daemonize",
            "no",
            "--loglevel",
            "warning",
        ]
    return [
        str(RUST_BIN),
        "--port",
        str(port),
        "--bind",
        "127.0.0.1",
        "--dir",
        str(data_dir),
        "--dbfilename",
        "dump.rdb",
        "--rdb-disabled",
        "--appendonly",
        appendonly,
        "--appendfsync",
        fsync,
    ]


def start_server(target: str, port: int, data_dir: Path, aof_mode: str, log_path: Path) -> subprocess.Popen[str]:
    cmd = server_command(target, port, data_dir, aof_mode)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log = log_path.open("w", encoding="utf-8")
    try:
        proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=log, text=True)
    finally:
        log.close()
    wait_for_port(port, proc)
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


def directory_size(path: Path) -> int:
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += item.stat().st_size
    return total


def csv_list(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def int_list(raw: str) -> list[int]:
    values = [int(item) for item in csv_list(raw)]
    if not values:
        raise ValueError(f"empty integer list: {raw!r}")
    return values


def payload_bytes(size: int) -> bytes:
    if size <= 0:
        return b""
    seed = b"0123456789abcdef"
    return (seed * ((size // len(seed)) + 1))[:size]


def workload_command(name: str, seq: int, keyspace: int, payload: bytes) -> tuple[str | bytes, ...]:
    slot = seq % max(keyspace, 1)
    if name == "set":
        return ("SET", f"aof:set:{slot}", payload)
    if name == "incr":
        return ("INCR", f"aof:incr:{slot}")
    if name == "hset":
        return ("HSET", f"aof:hash:{slot}", "f1", payload, "f2", payload)
    if name == "zadd":
        return ("ZADD", f"aof:zset:{slot}", str(seq), f"m:{seq}")
    if name == "rpush":
        return ("RPUSH", f"aof:list:{slot}", payload)
    raise ValueError(f"unknown workload {name!r}")


def worker(
    port: int,
    workload: str,
    start_seq: int,
    count: int,
    pipeline: int,
    keyspace: int,
    payload: bytes,
    barrier: threading.Barrier,
    out: dict[str, Any],
) -> None:
    samples: list[float] = []
    client = RespClient(port)
    try:
        barrier.wait()
        done = 0
        while done < count:
            batch = min(pipeline, count - done)
            frame = b"".join(
                encode_command(workload_command(workload, start_seq + done + idx, keyspace, payload))
                for idx in range(batch)
            )
            before = time.perf_counter()
            client.sendall(frame)
            for _ in range(batch):
                client.read()
            elapsed_ms = (time.perf_counter() - before) * 1000.0
            samples.extend([elapsed_ms / batch] * batch)
            done += batch
        out["samples"] = samples
        out["status"] = "ok"
    except Exception as exc:  # noqa: BLE001 - captured into benchmark result
        out["status"] = "error"
        out["error"] = repr(exc)
        out["samples"] = samples
    finally:
        client.close()


def percentile(sorted_values: list[float], pct: float) -> float | None:
    if not sorted_values:
        return None
    idx = min(len(sorted_values) - 1, max(0, int(round((len(sorted_values) - 1) * pct))))
    return sorted_values[idx]


def summarize_samples(samples: list[float]) -> dict[str, float | None]:
    ordered = sorted(samples)
    return {
        "latency_p50_ms": percentile(ordered, 0.50),
        "latency_p95_ms": percentile(ordered, 0.95),
        "latency_p99_ms": percentile(ordered, 0.99),
        "latency_p100_ms": ordered[-1] if ordered else None,
    }


def run_cell(cell: Cell, stamp: str, args: argparse.Namespace) -> dict[str, Any]:
    port = free_port()
    payload = payload_bytes(args.payload)
    log_path = RESULTS_DIR / f"{stamp}-{cell.target}-aof-matrix-{cell.aof_mode}-{cell.workload}-p{cell.pipeline}.log"
    requests_per_client = [args.requests // args.clients] * args.clients
    for idx in range(args.requests % args.clients):
        requests_per_client[idx] += 1

    proc: subprocess.Popen[str] | None = None
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="redis-rs-aof-matrix-") as tmp:
        data_dir = Path(tmp)
        try:
            proc = start_server(cell.target, port, data_dir, cell.aof_mode, log_path)
            warm = RespClient(port)
            try:
                for idx in range(args.warmup_requests):
                    warm.command(*workload_command(cell.workload, idx, args.keyspace, payload))
            finally:
                warm.close()

            barrier = threading.Barrier(args.clients)
            threads: list[threading.Thread] = []
            results: list[dict[str, Any]] = [{} for _ in range(args.clients)]
            seq = 0
            run_started = time.perf_counter()
            for idx, count in enumerate(requests_per_client):
                thread = threading.Thread(
                    target=worker,
                    args=(port, cell.workload, seq, count, cell.pipeline, args.keyspace, payload, barrier, results[idx]),
                )
                seq += count
                thread.start()
                threads.append(thread)
            for thread in threads:
                thread.join(args.timeout_s)
            elapsed_s = time.perf_counter() - run_started
            for thread in threads:
                if thread.is_alive():
                    raise TimeoutError(f"benchmark cell timed out after {args.timeout_s}s")

            samples: list[float] = []
            errors = []
            for result in results:
                samples.extend(result.get("samples", []))
                if result.get("status") != "ok":
                    errors.append(result.get("error", "unknown worker error"))
            time.sleep(args.post_write_sleep_s)
            aof_bytes = directory_size(data_dir)
            status = "ok" if not errors and len(samples) == args.requests else "error"
            row = {
                "target": cell.target,
                "aof_mode": cell.aof_mode,
                "workload": cell.workload,
                "pipeline": cell.pipeline,
                "requests": args.requests,
                "clients": args.clients,
                "payload": args.payload,
                "status": status,
                "errors": errors,
                "elapsed_s": elapsed_s,
                "throughput_rps": (len(samples) / elapsed_s) if elapsed_s > 0 else None,
                "aof_bytes": aof_bytes,
                "aof_bytes_per_command": (aof_bytes / len(samples)) if samples else None,
                "log_path": relative(log_path),
                "server_command": server_command(cell.target, port, data_dir, cell.aof_mode),
                "cell_elapsed_s": time.perf_counter() - started,
            }
            row.update(summarize_samples(samples))
            return row
        except Exception as exc:  # noqa: BLE001 - benchmark cell must report failures as data
            return {
                "target": cell.target,
                "aof_mode": cell.aof_mode,
                "workload": cell.workload,
                "pipeline": cell.pipeline,
                "requests": args.requests,
                "clients": args.clients,
                "payload": args.payload,
                "status": "error",
                "error": repr(exc),
                "elapsed_s": time.perf_counter() - started,
                "log_path": relative(log_path),
            }
        finally:
            stop_server(proc)


def add_overhead(rows: list[dict[str, Any]]) -> None:
    baseline: dict[tuple[str, str, int], dict[str, Any]] = {}
    for row in rows:
        if row["status"] == "ok" and row["aof_mode"] == "disabled":
            baseline[(row["target"], row["workload"], row["pipeline"])] = row
    for row in rows:
        base = baseline.get((row["target"], row["workload"], row["pipeline"]))
        if not base or row["status"] != "ok":
            continue
        base_rps = base.get("throughput_rps") or 0.0
        row_rps = row.get("throughput_rps") or 0.0
        row["throughput_vs_appendonly_no"] = row_rps / base_rps if base_rps else None
        row["throughput_overhead_pct"] = ((base_rps - row_rps) / base_rps * 100.0) if base_rps else None


def write_tsv(path: Path, stamp: str, commit: str, dirty: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as out:
        out.write("# valkey-rs AOF matrix benchmark\n")
        out.write(f"# timestamp_utc\t{stamp}\n")
        out.write(f"# commit\t{commit}\n")
        out.write(f"# dirty\t{json.dumps(dirty, sort_keys=True)}\n")
        out.write(
            "target\taof_mode\tworkload\tpipeline\trequests\tclients\tpayload\tstatus\t"
            "throughput_rps\tthroughput_vs_appendonly_no\tthroughput_overhead_pct\t"
            "p50_ms\tp95_ms\tp99_ms\tp100_ms\taof_bytes\taof_bytes_per_command\telapsed_s\n"
        )
        for row in rows:
            out.write(
                f"{row['target']}\t{row['aof_mode']}\t{row['workload']}\t{row['pipeline']}\t"
                f"{row['requests']}\t{row['clients']}\t{row['payload']}\t{row['status']}\t"
                f"{row.get('throughput_rps') or 0.0:.3f}\t"
                f"{row.get('throughput_vs_appendonly_no') or 0.0:.6f}\t"
                f"{row.get('throughput_overhead_pct') or 0.0:.3f}\t"
                f"{row.get('latency_p50_ms') or 0.0:.6f}\t"
                f"{row.get('latency_p95_ms') or 0.0:.6f}\t"
                f"{row.get('latency_p99_ms') or 0.0:.6f}\t"
                f"{row.get('latency_p100_ms') or 0.0:.6f}\t"
                f"{row.get('aof_bytes') or 0}\t"
                f"{row.get('aof_bytes_per_command') or 0.0:.3f}\t"
                f"{row.get('elapsed_s') or 0.0:.3f}\n"
            )


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ok = [row for row in rows if row["status"] == "ok"]
    failed = [row for row in rows if row["status"] != "ok"]
    overhead_rows = [
        row for row in ok if row["aof_mode"] != "disabled" and row.get("throughput_overhead_pct") is not None
    ]
    worst = None
    if overhead_rows:
        worst = max(overhead_rows, key=lambda row: row["throughput_overhead_pct"])
    return {
        "ok": len(ok),
        "total": len(rows),
        "failed": [
            f"{row['target']}/{row['aof_mode']}/{row['workload']}/p{row['pipeline']}" for row in failed
        ],
        "worst_overhead": {
            "target": worst["target"],
            "aof_mode": worst["aof_mode"],
            "workload": worst["workload"],
            "pipeline": worst["pipeline"],
            "throughput_overhead_pct": worst["throughput_overhead_pct"],
        }
        if worst
        else None,
    }


def measurements(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if row["status"] != "ok":
            continue
        tags = {
            "target": row["target"],
            "aof_mode": row["aof_mode"],
            "workload": row["workload"],
            "pipeline": row["pipeline"],
        }
        for metric, value in (
            ("aof_throughput_rps", row.get("throughput_rps")),
            ("aof_latency_p99_ms", row.get("latency_p99_ms")),
            ("aof_latency_p100_ms", row.get("latency_p100_ms")),
            ("aof_bytes_per_command", row.get("aof_bytes_per_command")),
            ("aof_throughput_vs_appendonly_no", row.get("throughput_vs_appendonly_no")),
            ("aof_throughput_overhead_pct", row.get("throughput_overhead_pct")),
        ):
            if value is None:
                continue
            out.append(
                {
                    "capability": "performance-aof",
                    "kind": "telemetry",
                    "metric": metric,
                    "name": f"{row['workload']}-p{row['pipeline']}-{row['aof_mode']}",
                    "unit": metric.rsplit("_", 1)[-1],
                    "value": value,
                    **tags,
                }
            )
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Run AOF append/fsync overhead matrix")
    parser.add_argument("--commands", default="set,incr,hset,zadd,rpush")
    parser.add_argument("--fsync-modes", default="no,everysec,always")
    parser.add_argument("--pipelines", default="1,16")
    parser.add_argument("--targets", default="reference,rust")
    parser.add_argument("--requests", type=int, default=50_000)
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--payload", type=int, default=64)
    parser.add_argument("--keyspace", type=int, default=10_000)
    parser.add_argument("--warmup-requests", type=int, default=250)
    parser.add_argument("--timeout-s", type=int, default=120)
    parser.add_argument("--post-write-sleep-s", type=float, default=0.05)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args()

    if args.quick:
        args.commands = "set,incr" if args.commands == parser.get_default("commands") else args.commands
        args.fsync_modes = "no,everysec,always" if args.fsync_modes == parser.get_default("fsync_modes") else args.fsync_modes
        args.pipelines = "1,16" if args.pipelines == parser.get_default("pipelines") else args.pipelines
        args.targets = "rust" if args.targets == parser.get_default("targets") else args.targets
        args.requests = 2_000 if args.requests == parser.get_default("requests") else args.requests
        args.clients = 4 if args.clients == parser.get_default("clients") else args.clients
        args.warmup_requests = 25 if args.warmup_requests == parser.get_default("warmup_requests") else args.warmup_requests
        args.timeout_s = 30 if args.timeout_s == parser.get_default("timeout_s") else args.timeout_s

    targets = csv_list(args.targets)
    aof_modes = ["disabled"] + csv_list(args.fsync_modes)
    workloads = csv_list(args.commands)
    pipelines = int_list(args.pipelines)

    require_binaries(build=not args.skip_build)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    commit = git_commit()
    dirty = git_dirty()

    rows = []
    for target in targets:
        for aof_mode in aof_modes:
            for workload in workloads:
                for pipeline in pipelines:
                    cell = Cell(target=target, aof_mode=aof_mode, workload=workload, pipeline=pipeline)
                    print(f"==> {target} {aof_mode} {workload} p{pipeline}", flush=True)
                    rows.append(run_cell(cell, stamp, args))
    add_overhead(rows)

    tsv_path = RESULTS_DIR / f"{stamp}-{commit}-aof-matrix.tsv"
    json_path = RESULTS_DIR / f"{stamp}-{commit}-aof-matrix.json"
    write_tsv(tsv_path, stamp, commit, dirty, rows)
    summary = summarize(rows)
    status = "pass" if summary["ok"] == summary["total"] else "fail"
    result = {
        "schema_version": 1,
        "runner_id": "bench-aof-matrix",
        "surface": "performance",
        "method": "bench-load",
        "claim_level": "telemetry",
        "status": status,
        "summary": summary,
        "evidence": {
            "kind": "aof_matrix",
            "commit": commit,
            "dirty": dirty,
            "timestamp": stamp,
            "hardware": hardware_fingerprint(),
            "parameters": vars(args),
            "rows": rows,
        },
        "measurements": measurements(rows),
        "artifacts": [
            {"kind": "bench-tsv", "path": relative(tsv_path)},
            {"kind": "bench-json", "path": relative(json_path)},
        ],
        "note": "Telemetry only. Repeat on an isolated benchmark host before making public claims.",
    }
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
