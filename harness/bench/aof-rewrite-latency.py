#!/usr/bin/env python3
"""AOF rewrite latency telemetry.

This runner measures command latency around BGREWRITEAOF while writes continue,
then restarts from the resulting AOF layout and verifies the acknowledged write
set. It is intentionally source-shaped and small enough for quick iteration.
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

COW_INFO_KEYS = [
    "keyspace_cow_active_snapshots",
    "keyspace_cow_snapshot_starts",
    "keyspace_cow_snapshot_drops",
    "keyspace_cow_segment_clones",
    "keyspace_cow_segment_clone_keys",
    "keyspace_cow_segment_clone_estimated_bytes",
    "keyspace_cow_segment_clone_max_keys",
    "keyspace_cow_segment_clone_max_estimated_bytes",
    "keyspace_cow_segment_clone_us",
    "keyspace_cow_segment_clone_max_us",
]


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


@dataclass
class WriteSample:
    seq: int
    started_s: float
    ended_s: float
    latency_ms: float


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


def server_command(target: str, port: int, data_dir: Path, fsync: str) -> list[str]:
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
            "yes",
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
        "yes",
        "--appendfsync",
        fsync,
    ]


def start_server(target: str, port: int, data_dir: Path, fsync: str, log_path: Path) -> subprocess.Popen[str]:
    cmd = server_command(target, port, data_dir, fsync)
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


def wait_until_loaded(client: RespClient, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    last_error = ""
    while time.monotonic() < deadline:
        try:
            if client.command("PING") == "PONG":
                return
        except RuntimeError as exc:
            last_error = str(exc)
            if "LOADING" not in last_error:
                raise
        time.sleep(0.05)
    raise TimeoutError(f"server did not finish loading within {timeout_s}s: {last_error}")


def normalize_bulk(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def info_fields(client: RespClient, section: str) -> dict[str, str]:
    raw = client.command("INFO", section)
    text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else str(raw)
    fields: dict[str, str] = {}
    for line in text.splitlines():
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields[key] = value.strip()
    return fields


def int_info(fields: dict[str, str], key: str, default: int = 0) -> int:
    try:
        return int(fields.get(key, default))
    except (TypeError, ValueError):
        return default


def cow_info(fields: dict[str, str]) -> dict[str, int]:
    return {key: int_info(fields, key) for key in COW_INFO_KEYS}


def cow_delta(before: dict[str, int], after: dict[str, int], key: str) -> int:
    return after.get(key, 0) - before.get(key, 0)


def cow_peak(samples: list[dict[str, int]]) -> dict[str, int]:
    if not samples:
        return {key: 0 for key in COW_INFO_KEYS}
    return {key: max(sample.get(key, 0) for sample in samples) for key in COW_INFO_KEYS}


def read_cow_info(port: int) -> dict[str, int]:
    client = RespClient(port)
    try:
        return cow_info(info_fields(client, "persistence"))
    finally:
        client.close()


def wait_aof_rewrite_done(client: RespClient, timeout_s: float) -> tuple[float, bool]:
    deadline = time.perf_counter() + timeout_s
    observed_in_progress = False
    while time.perf_counter() < deadline:
        fields = info_fields(client, "persistence")
        in_progress = fields.get("aof_rewrite_in_progress") == "1"
        observed_in_progress = observed_in_progress or in_progress
        if not in_progress:
            return time.perf_counter(), observed_in_progress
        time.sleep(0.01)
    raise TimeoutError("AOF rewrite did not finish")


def process_rss_kb(proc: subprocess.Popen[str]) -> int | None:
    if proc.poll() is not None:
        return None
    out = run_text(["ps", "-o", "rss=", "-p", str(proc.pid)], timeout=2)
    try:
        return int(out.strip())
    except ValueError:
        return None


def wait_aof_rewrite_done_with_rss(
    client: RespClient,
    timeout_s: float,
    proc: subprocess.Popen[str],
) -> tuple[float, bool, int | None, dict[str, int], list[dict[str, int]]]:
    deadline = time.perf_counter() + timeout_s
    observed_in_progress = False
    peak_rss_kb = process_rss_kb(proc)
    cow_samples: list[dict[str, int]] = []
    while time.perf_counter() < deadline:
        fields = info_fields(client, "persistence")
        cow_samples.append(cow_info(fields))
        in_progress = fields.get("aof_rewrite_in_progress") == "1"
        observed_in_progress = observed_in_progress or in_progress
        current_rss = process_rss_kb(proc)
        if current_rss is not None:
            peak_rss_kb = max(peak_rss_kb or current_rss, current_rss)
        if not in_progress:
            return (
                time.perf_counter(),
                observed_in_progress,
                peak_rss_kb,
                cow_peak(cow_samples),
                cow_samples,
            )
        time.sleep(0.01)
    raise TimeoutError("AOF rewrite did not finish")


def populate_dataset(client: RespClient, size: int) -> dict[str, Any]:
    for idx in range(size):
        assert client.command("SET", f"base:string:{idx}", f"v:{idx}") == "OK"
        client.command("HSET", f"base:hash:{idx % 128}", f"f:{idx}", f"v:{idx}")
        client.command("SADD", f"base:set:{idx % 128}", f"m:{idx}")
        client.command("ZADD", f"base:zset:{idx % 128}", str(idx), f"m:{idx}")
        client.command("RPUSH", f"base:list:{idx % 128}", f"v:{idx}")
    stream_status = "skipped"
    try:
        for idx in range(max(1, size // 20)):
            client.command("XADD", "base:stream", "*", "f", f"v:{idx}")
        stream_status = "ok"
    except RuntimeError as exc:
        stream_status = f"error:{exc}"
    return {"base_size": size, "stream_status": stream_status}


def write_worker(
    port: int,
    writes: int,
    write_pause_s: float,
    ready: threading.Event,
    out: dict[str, Any],
) -> None:
    client = RespClient(port)
    samples: list[WriteSample] = []
    try:
        ready.set()
        for seq in range(writes):
            started = time.perf_counter()
            reply = client.command("SET", f"rw:{seq}", str(seq))
            ended = time.perf_counter()
            if reply != "OK":
                raise RuntimeError(f"unexpected SET reply {reply!r}")
            samples.append(
                WriteSample(
                    seq=seq,
                    started_s=started,
                    ended_s=ended,
                    latency_ms=(ended - started) * 1000.0,
                )
            )
            if write_pause_s > 0:
                time.sleep(write_pause_s)
        out["status"] = "ok"
        out["samples"] = samples
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


def latency_stats(samples: list[WriteSample]) -> dict[str, float | int | None]:
    values = sorted(sample.latency_ms for sample in samples)
    return {
        "count": len(values),
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
        "p100_ms": values[-1] if values else None,
    }


def classify_samples(samples: list[WriteSample], rewrite_start: float, rewrite_end: float) -> dict[str, list[WriteSample]]:
    before = []
    during = []
    after = []
    for sample in samples:
        if sample.ended_s < rewrite_start:
            before.append(sample)
        elif sample.started_s <= rewrite_end and sample.ended_s >= rewrite_start:
            during.append(sample)
        else:
            after.append(sample)
    return {"before": before, "during": during, "after": after}


def parse_manifest(data_dir: Path) -> dict[str, Any]:
    manifests = list(data_dir.rglob("appendonly.aof.manifest"))
    if not manifests:
        return {"manifest_found": False, "base_bytes": 0, "incr_bytes": 0, "lines": []}
    manifest = manifests[0]
    lines = [line.strip() for line in manifest.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip()]
    base_bytes = 0
    incr_bytes = 0
    base_seq = None
    incr_seq = None
    for line in lines:
        parts = line.split()
        if len(parts) < 6 or parts[0] != "file":
            continue
        filename = parts[1]
        seq = int(parts[3]) if parts[2] == "seq" and parts[3].isdigit() else None
        typ = parts[5] if parts[4] == "type" else None
        size = sum(path.stat().st_size for path in data_dir.rglob(filename) if path.is_file())
        if typ == "b":
            base_bytes += size
            base_seq = seq
        elif typ == "i":
            incr_bytes += size
            incr_seq = seq
    return {
        "manifest_found": True,
        "manifest_path": str(manifest.relative_to(data_dir)),
        "lines": lines,
        "base_bytes": base_bytes,
        "incr_bytes": incr_bytes,
        "base_seq": base_seq,
        "incr_seq": incr_seq,
    }


def verify_after_restart(target: str, data_dir: Path, fsync: str, expected_writes: int, log_path: Path) -> dict[str, Any]:
    port = free_port()
    proc: subprocess.Popen[str] | None = None
    try:
        proc = start_server(target, port, data_dir, fsync, log_path)
        client = RespClient(port)
        try:
            wait_until_loaded(client)
            missing = []
            bad = []
            for seq in range(expected_writes):
                value = normalize_bulk(client.command("GET", f"rw:{seq}"))
                if value is None:
                    missing.append(seq)
                elif value != str(seq):
                    bad.append({"seq": seq, "value": value})
                if len(missing) + len(bad) > 20:
                    break
            base_checks = {
                "base:string:0": normalize_bulk(client.command("GET", "base:string:0")),
                "base:list:0": client.command("LLEN", "base:list:0"),
                "base:hash:0": client.command("HLEN", "base:hash:0"),
                "base:set:0": client.command("SCARD", "base:set:0"),
                "base:zset:0": client.command("ZCARD", "base:zset:0"),
            }
            try:
                base_checks["base:stream"] = client.command("XLEN", "base:stream")
            except RuntimeError as exc:
                base_checks["base:stream"] = f"error:{exc}"
            return {
                "passed": not missing and not bad and base_checks["base:string:0"] == "v:0",
                "missing": missing,
                "bad": bad,
                "base_checks": base_checks,
            }
        finally:
            client.close()
    finally:
        stop_server(proc)


def run_target(target: str, dataset_size: int, stamp: str, args: argparse.Namespace) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="redis-rs-aof-rewrite-latency-") as tmp:
        data_dir = Path(tmp)
        port = free_port()
        log_path = RESULTS_DIR / f"{stamp}-{target}-aof-rewrite-latency-ds{dataset_size}.log"
        restart_log_path = RESULTS_DIR / f"{stamp}-{target}-aof-rewrite-latency-ds{dataset_size}-restart.log"
        proc: subprocess.Popen[str] | None = None
        started = time.perf_counter()
        try:
            proc = start_server(target, port, data_dir, args.appendfsync, log_path)
            client = RespClient(port)
            try:
                dataset = populate_dataset(client, dataset_size)
            finally:
                client.close()
            rss_before_rewrite_kb = process_rss_kb(proc)
            cow_before_rewrite = read_cow_info(port)

            writer_ready = threading.Event()
            writer_result: dict[str, Any] = {}
            thread = threading.Thread(
                target=write_worker,
                args=(port, args.writes, args.write_pause_ms / 1000.0, writer_ready, writer_result),
            )
            thread.start()
            writer_ready.wait(timeout=5)
            time.sleep(args.rewrite_delay_s)

            rewrite_client = RespClient(port)
            try:
                rewrite_start = time.perf_counter()
                rewrite_reply = rewrite_client.command("BGREWRITEAOF")
                rewrite_reply_end = time.perf_counter()
                rss_after_reply_kb = process_rss_kb(proc)
                rewrite_info_after_reply = info_fields(rewrite_client, "persistence")
                cow_after_reply = cow_info(rewrite_info_after_reply)
                (
                    rewrite_end,
                    rewrite_observed_in_progress,
                    rss_peak_kb,
                    cow_peak_during_rewrite,
                    cow_during_samples,
                ) = wait_aof_rewrite_done_with_rss(rewrite_client, args.timeout_s, proc)
                rss_after_rewrite_kb = process_rss_kb(proc)
                cow_after_rewrite = cow_info(info_fields(rewrite_client, "persistence"))
            finally:
                rewrite_client.close()

            thread.join(args.timeout_s)
            if thread.is_alive():
                raise TimeoutError(f"writer timed out after {args.timeout_s}s")

            time.sleep(args.post_rewrite_sleep_s)
            manifest = parse_manifest(data_dir)
        finally:
            stop_server(proc)

        samples: list[WriteSample] = writer_result.get("samples", [])
        classified = classify_samples(samples, rewrite_start, rewrite_end)
        restart = verify_after_restart(target, data_dir, args.appendfsync, len(samples), restart_log_path)
        status = "ok" if writer_result.get("status") == "ok" and restart["passed"] else "error"
        row = {
            "target": target,
            "status": status,
            "error": writer_result.get("error"),
            "appendfsync": args.appendfsync,
            "dataset": dataset,
            "writes": args.writes,
            "acknowledged_writes": len(samples),
            "rewrite_reply": rewrite_reply,
            "rewrite_command_wall_ms": (rewrite_reply_end - rewrite_start) * 1000.0,
            "rewrite_start_block_ms": (rewrite_reply_end - rewrite_start) * 1000.0,
            "rewrite_post_reply_wall_ms": (rewrite_end - rewrite_reply_end) * 1000.0,
            "rewrite_wall_ms": (rewrite_end - rewrite_start) * 1000.0,
            "rewrite_observed_in_progress": rewrite_observed_in_progress,
            "snapshot": {
                "key_count": int_info(rewrite_info_after_reply, "aof_last_rewrite_snapshot_keys"),
                "capture_us": int_info(rewrite_info_after_reply, "aof_last_rewrite_snapshot_us"),
            },
            "cow": {
                "before_rewrite": cow_before_rewrite,
                "after_reply": cow_after_reply,
                "after_rewrite": cow_after_rewrite,
                "peak_during_rewrite": cow_peak_during_rewrite,
                "during_sample_count": len(cow_during_samples),
                "segment_clones_delta": cow_delta(
                    cow_before_rewrite,
                    cow_after_rewrite,
                    "keyspace_cow_segment_clones",
                ),
                "segment_clone_keys_delta": cow_delta(
                    cow_before_rewrite,
                    cow_after_rewrite,
                    "keyspace_cow_segment_clone_keys",
                ),
                "segment_clone_estimated_bytes_delta": cow_delta(
                    cow_before_rewrite,
                    cow_after_rewrite,
                    "keyspace_cow_segment_clone_estimated_bytes",
                ),
                "segment_clone_us_delta": cow_delta(
                    cow_before_rewrite,
                    cow_after_rewrite,
                    "keyspace_cow_segment_clone_us",
                ),
            },
            "rewrite_started_s": rewrite_start,
            "rewrite_ended_s": rewrite_end,
            "rss_kb": {
                "before_rewrite": rss_before_rewrite_kb,
                "after_reply": rss_after_reply_kb,
                "after_rewrite": rss_after_rewrite_kb,
                "peak_during_rewrite": rss_peak_kb,
            },
            "latency": {phase: latency_stats(items) for phase, items in classified.items()},
            "manifest": manifest,
            "restart": restart,
            "elapsed_s": time.perf_counter() - started,
            "log_path": relative(log_path),
            "restart_log_path": relative(restart_log_path),
            "server_command": server_command(target, port, data_dir, args.appendfsync),
        }
        return row


def csv_list(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def int_list(raw: str) -> list[int]:
    values = [int(item) for item in csv_list(raw)]
    if not values:
        raise ValueError(f"empty integer list: {raw!r}")
    return values


def write_tsv(path: Path, stamp: str, commit: str, dirty: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as out:
        out.write("# valkey-rs AOF rewrite latency benchmark\n")
        out.write(f"# timestamp_utc\t{stamp}\n")
        out.write(f"# commit\t{commit}\n")
        out.write(f"# dirty\t{json.dumps(dirty, sort_keys=True)}\n")
        out.write(
            "target\tstatus\tappendfsync\tdataset_size\twrites\tacknowledged_writes\t"
            "rewrite_snapshot_keys\trewrite_snapshot_us\t"
            "rewrite_command_wall_ms\trewrite_post_reply_wall_ms\trewrite_wall_ms\t"
            "before_p99_ms\tduring_p99_ms\tafter_p99_ms\tduring_p100_ms\t"
            "rss_before_kb\trss_peak_kb\trss_after_kb\tbase_bytes\tincr_bytes\t"
            "cow_active_after_reply\tcow_active_peak\tcow_active_after_rewrite\t"
            "cow_segment_clones_delta\tcow_segment_clone_keys_delta\t"
            "cow_segment_clone_estimated_bytes_delta\tcow_segment_clone_us_delta\t"
            "cow_segment_clone_max_us\tcow_samples\t"
            "restart_passed\telapsed_s\n"
        )
        for row in rows:
            rss = row.get("rss_kb") or {}
            cow = row.get("cow") or {}
            cow_after_reply = cow.get("after_reply") or {}
            cow_peak_during = cow.get("peak_during_rewrite") or {}
            cow_after_rewrite = cow.get("after_rewrite") or {}
            out.write(
                f"{row['target']}\t{row['status']}\t{row['appendfsync']}\t"
                f"{row['dataset']['base_size']}\t{row['writes']}\t{row['acknowledged_writes']}\t"
                f"{row.get('snapshot', {}).get('key_count') or 0}\t"
                f"{row.get('snapshot', {}).get('capture_us') or 0}\t"
                f"{row['rewrite_command_wall_ms']:.6f}\t"
                f"{row['rewrite_post_reply_wall_ms']:.6f}\t"
                f"{row['rewrite_wall_ms']:.6f}\t"
                f"{row['latency']['before'].get('p99_ms') or 0.0:.6f}\t"
                f"{row['latency']['during'].get('p99_ms') or 0.0:.6f}\t"
                f"{row['latency']['after'].get('p99_ms') or 0.0:.6f}\t"
                f"{row['latency']['during'].get('p100_ms') or 0.0:.6f}\t"
                f"{rss.get('before_rewrite') or 0}\t"
                f"{rss.get('peak_during_rewrite') or 0}\t"
                f"{rss.get('after_rewrite') or 0}\t"
                f"{row['manifest'].get('base_bytes') or 0}\t"
                f"{row['manifest'].get('incr_bytes') or 0}\t"
                f"{cow_after_reply.get('keyspace_cow_active_snapshots') or 0}\t"
                f"{cow_peak_during.get('keyspace_cow_active_snapshots') or 0}\t"
                f"{cow_after_rewrite.get('keyspace_cow_active_snapshots') or 0}\t"
                f"{cow.get('segment_clones_delta') or 0}\t"
                f"{cow.get('segment_clone_keys_delta') or 0}\t"
                f"{cow.get('segment_clone_estimated_bytes_delta') or 0}\t"
                f"{cow.get('segment_clone_us_delta') or 0}\t"
                f"{cow_after_rewrite.get('keyspace_cow_segment_clone_max_us') or 0}\t"
                f"{cow.get('during_sample_count') or 0}\t"
                f"{row['restart']['passed']}\t{row['elapsed_s']:.3f}\n"
            )


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ok = [row for row in rows if row["status"] == "ok"]
    failed = [row for row in rows if row["status"] != "ok"]
    worst = None
    if ok:
        worst = max(ok, key=lambda row: row["latency"]["during"].get("p100_ms") or 0.0)
    return {
        "ok": len(ok),
        "total": len(rows),
        "failed": [f"{row['target']}/dataset-{row['dataset']['base_size']}" for row in failed],
        "worst_during_p100": {
            "target": worst["target"],
            "dataset_size": worst["dataset"]["base_size"],
            "during_p100_ms": worst["latency"]["during"].get("p100_ms"),
            "rewrite_wall_ms": worst["rewrite_wall_ms"],
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
            "appendfsync": row["appendfsync"],
            "dataset_size": row["dataset"]["base_size"],
        }
        scalar_metrics = {
            "aof_rewrite_command_wall_ms": row["rewrite_command_wall_ms"],
            "aof_rewrite_start_block_ms": row["rewrite_start_block_ms"],
            "aof_rewrite_post_reply_wall_ms": row["rewrite_post_reply_wall_ms"],
            "aof_rewrite_wall_ms": row["rewrite_wall_ms"],
            "aof_rewrite_base_bytes": row["manifest"].get("base_bytes"),
            "aof_rewrite_incr_bytes": row["manifest"].get("incr_bytes"),
            "aof_rewrite_acknowledged_writes": row["acknowledged_writes"],
            "aof_rewrite_snapshot_keys": row.get("snapshot", {}).get("key_count"),
            "aof_rewrite_snapshot_us": row.get("snapshot", {}).get("capture_us"),
            "aof_rewrite_rss_before_kb": row.get("rss_kb", {}).get("before_rewrite"),
            "aof_rewrite_rss_peak_kb": row.get("rss_kb", {}).get("peak_during_rewrite"),
            "aof_rewrite_rss_after_kb": row.get("rss_kb", {}).get("after_rewrite"),
            "aof_rewrite_cow_active_after_reply": row.get("cow", {})
            .get("after_reply", {})
            .get("keyspace_cow_active_snapshots"),
            "aof_rewrite_cow_active_peak": row.get("cow", {})
            .get("peak_during_rewrite", {})
            .get("keyspace_cow_active_snapshots"),
            "aof_rewrite_cow_active_after_rewrite": row.get("cow", {})
            .get("after_rewrite", {})
            .get("keyspace_cow_active_snapshots"),
            "aof_rewrite_cow_segment_clones_delta": row.get("cow", {}).get(
                "segment_clones_delta"
            ),
            "aof_rewrite_cow_segment_clone_keys_delta": row.get("cow", {}).get(
                "segment_clone_keys_delta"
            ),
            "aof_rewrite_cow_segment_clone_estimated_bytes_delta": row.get("cow", {}).get(
                "segment_clone_estimated_bytes_delta"
            ),
            "aof_rewrite_cow_segment_clone_us_delta": row.get("cow", {}).get(
                "segment_clone_us_delta"
            ),
            "aof_rewrite_cow_segment_clone_max_us": row.get("cow", {})
            .get("after_rewrite", {})
            .get("keyspace_cow_segment_clone_max_us"),
        }
        for metric, value in scalar_metrics.items():
            if value is None:
                continue
            out.append(
                {
                    "capability": "performance-aof-rewrite",
                    "kind": "telemetry",
                    "metric": metric,
                    "name": f"{row['target']}-{row['appendfsync']}-dataset-{row['dataset']['base_size']}",
                    "value": value,
                    **tags,
                }
            )
        for phase, stats in row["latency"].items():
            for field in ("p99_ms", "p100_ms"):
                value = stats.get(field)
                if value is None:
                    continue
                out.append(
                    {
                        "capability": "performance-aof-rewrite",
                        "kind": "telemetry",
                        "metric": f"aof_rewrite_latency_{phase}_{field}",
                        "name": f"{row['target']}-{row['appendfsync']}-dataset-{row['dataset']['base_size']}-{phase}",
                        "value": value,
                        **tags,
                    }
                )
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure AOF rewrite latency under write load")
    parser.add_argument("--targets", default="reference,rust")
    parser.add_argument("--appendfsync", default="everysec", choices=["no", "everysec", "always"])
    parser.add_argument("--dataset-size", type=int, default=5_000)
    parser.add_argument(
        "--dataset-sizes",
        default="",
        help="Comma-separated dataset sizes. Overrides --dataset-size when set.",
    )
    parser.add_argument("--writes", type=int, default=5_000)
    parser.add_argument("--rewrite-delay-s", type=float, default=0.05)
    parser.add_argument("--write-pause-ms", type=float, default=0.0)
    parser.add_argument(
        "--post-rewrite-sleep-s",
        type=float,
        default=1.2,
        help="Wait after acknowledged writes before stop; everysec needs at least one fsync interval.",
    )
    parser.add_argument("--timeout-s", type=int, default=120)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args()

    if args.quick:
        args.targets = "rust" if args.targets == parser.get_default("targets") else args.targets
        args.dataset_size = 250 if args.dataset_size == parser.get_default("dataset_size") else args.dataset_size
        args.writes = 400 if args.writes == parser.get_default("writes") else args.writes
        args.rewrite_delay_s = 0.005 if args.rewrite_delay_s == parser.get_default("rewrite_delay_s") else args.rewrite_delay_s
        args.write_pause_ms = 0.05 if args.write_pause_ms == parser.get_default("write_pause_ms") else args.write_pause_ms
        args.timeout_s = 30 if args.timeout_s == parser.get_default("timeout_s") else args.timeout_s

    require_binaries(build=not args.skip_build)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    commit = git_commit()
    dirty = git_dirty()

    dataset_sizes = int_list(args.dataset_sizes) if args.dataset_sizes else [args.dataset_size]
    rows = []
    for target in csv_list(args.targets):
        for dataset_size in dataset_sizes:
            print(f"==> {target} rewrite latency dataset={dataset_size}", flush=True)
            rows.append(run_target(target, dataset_size, stamp, args))

    tsv_path = RESULTS_DIR / f"{stamp}-{commit}-aof-rewrite-latency.tsv"
    json_path = RESULTS_DIR / f"{stamp}-{commit}-aof-rewrite-latency.json"
    write_tsv(tsv_path, stamp, commit, dirty, rows)
    summary = summarize(rows)
    status = "pass" if summary["ok"] == summary["total"] else "fail"
    result = {
        "schema_version": 1,
        "runner_id": "bench-aof-rewrite-latency",
        "surface": "performance",
        "method": "bench-load",
        "claim_level": "telemetry",
        "status": status,
        "summary": summary,
        "evidence": {
            "kind": "aof_rewrite_latency",
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
