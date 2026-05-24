#!/usr/bin/env python3
"""Persistence restart-cycle oracle for the Rust server.

The RDB byte-level oracle proves codec compatibility. This runner proves the
operator path: start a server, write data, persist it, stop, restart from the
same directory, and verify the keyspace.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
RUST_BIN = ROOT / "target" / "debug" / "redis-server"


class RespError(RuntimeError):
    pass


class RespClient:
    def __init__(self, port: int):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=3)
        self.sock.settimeout(5)
        self.buf = b""

    def close(self) -> None:
        self.sock.close()

    def command(self, *parts: str | bytes) -> Any:
        self.sock.sendall(encode_command(*parts))
        return self._read()

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

    def _read(self) -> Any:
        self._fill(1)
        typ = self.buf[:1]
        self.buf = self.buf[1:]
        if typ == b"+":
            return self._line().decode("utf-8", "replace")
        if typ == b"-":
            raise RespError(self._line().decode("utf-8", "replace"))
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
            return [self._read() for _ in range(n)]
        raise ValueError((typ, self.buf[:80]))


@dataclass(frozen=True)
class ServerConfig:
    mode: str
    dir: Path
    port: int


def encode_command(*parts: str | bytes) -> bytes:
    out = f"*{len(parts)}\r\n".encode()
    for part in parts:
        if isinstance(part, str):
            part = part.encode()
        out += f"${len(part)}\r\n".encode() + part + b"\r\n"
    return out


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except subprocess.SubprocessError:
        return "unknown"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_server(port: int, proc: subprocess.Popen[str], timeout_s: float = 8.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            if proc.poll() is not None:
                out = proc.stdout.read() if proc.stdout else ""
                raise RuntimeError(f"server exited during startup:\n{out}")
            time.sleep(0.05)
    raise RuntimeError(f"server did not listen on 127.0.0.1:{port}")


def start_server(cfg: ServerConfig) -> subprocess.Popen[str]:
    cmd = [
        str(RUST_BIN),
        "--port",
        str(cfg.port),
        "--bind",
        "127.0.0.1",
        "--dir",
        str(cfg.dir),
        "--dbfilename",
        "dump.rdb",
    ]
    if cfg.mode in {"aof", "aof-rewrite"}:
        cmd += ["--appendonly", "yes", "--appendfsync", "always"]
    proc = subprocess.Popen(
        cmd,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    wait_for_server(cfg.port, proc)
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


def populate(client: RespClient) -> None:
    assert client.command("SET", "s", "value") == "OK"
    assert client.command("MSET", "m1", "one", "m2", "two") == "OK"
    assert client.command("RPUSH", "list", "a", "b", "c") == 3
    assert client.command("HSET", "hash", "f1", "v1", "f2", "v2") == 2
    assert client.command("SADD", "set", "x", "y", "z") == 3
    assert client.command("ZADD", "zset", "1", "a", "2", "b") == 2
    assert client.command("SET", "volatile", "soon") == "OK"
    assert client.command("PEXPIRE", "volatile", "600000") == 1


def normalize_bulk(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def digest(client: RespClient, *, writer_count: int | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "s": normalize_bulk(client.command("GET", "s")),
        "m1": normalize_bulk(client.command("GET", "m1")),
        "m2": normalize_bulk(client.command("GET", "m2")),
        "list": [normalize_bulk(x) for x in client.command("LRANGE", "list", "0", "-1")],
        "hash": sorted(
            normalize_bulk(x) for x in client.command("HGETALL", "hash")
        ),
        "set": sorted(normalize_bulk(x) for x in client.command("SMEMBERS", "set")),
        "zset": [
            normalize_bulk(x)
            for x in client.command("ZRANGE", "zset", "0", "-1", "WITHSCORES")
        ],
        "volatile_exists": client.command("EXISTS", "volatile"),
        "volatile_ttl_positive": client.command("PTTL", "volatile") > 0,
    }
    if writer_count is not None:
        out["dbsize"] = client.command("DBSIZE")
        out["writer_command_count"] = writer_count
        out["writer_keys"] = [
            normalize_bulk(client.command("GET", f"rw:{i}")) for i in range(writer_count)
        ]
    return out


def run_rdb_cycle(tmp: Path) -> dict[str, Any]:
    cfg = ServerConfig("rdb", tmp, free_port())
    proc = start_server(cfg)
    try:
        client = RespClient(cfg.port)
        try:
            populate(client)
            assert client.command("SAVE") == "OK"
            before = digest(client)
        finally:
            client.close()
    finally:
        stop_server(proc)

    proc = start_server(cfg)
    try:
        client = RespClient(cfg.port)
        try:
            after = digest(client)
        finally:
            client.close()
    finally:
        stop_server(proc)
    return {"before": before, "after": after, "passed": before == after}


def run_aof_cycle(tmp: Path) -> dict[str, Any]:
    cfg = ServerConfig("aof", tmp, free_port())
    proc = start_server(cfg)
    try:
        client = RespClient(cfg.port)
        try:
            populate(client)
            assert client.command("INCR", "counter") == 1
            assert client.command("INCRBY", "counter", "4") == 5
            before = digest(client)
            before["counter"] = normalize_bulk(client.command("GET", "counter"))
        finally:
            client.close()
    finally:
        stop_server(proc)

    proc = start_server(cfg)
    try:
        client = RespClient(cfg.port)
        try:
            after = digest(client)
            after["counter"] = normalize_bulk(client.command("GET", "counter"))
        finally:
            client.close()
    finally:
        stop_server(proc)
    return {"before": before, "after": after, "passed": before == after}


def wait_rewrite_done(client: RespClient, timeout_s: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        info = client.command("INFO", "persistence")
        text = info.decode("utf-8", "replace") if isinstance(info, bytes) else str(info)
        if "aof_rewrite_in_progress:0" in text:
            return
        time.sleep(0.1)
    raise TimeoutError("AOF rewrite did not finish")


def run_aof_rewrite_cycle(tmp: Path) -> dict[str, Any]:
    cfg = ServerConfig("aof-rewrite", tmp, free_port())
    proc = start_server(cfg)
    stop_writes = threading.Event()
    writer_error: list[str] = []
    writer_count = {"value": 0}

    def writer() -> None:
        try:
            client = RespClient(cfg.port)
            i = 0
            try:
                while not stop_writes.is_set():
                    client.command("SET", f"rw:{i}", str(i))
                    i += 1
                    writer_count["value"] = i
            finally:
                client.close()
        except Exception as exc:  # pragma: no cover - preserved in runner JSON.
            writer_error.append(str(exc))

    try:
        client = RespClient(cfg.port)
        try:
            populate(client)
            thread = threading.Thread(target=writer, name="aof-rewrite-writer")
            thread.start()
            assert client.command("BGREWRITEAOF") == "Background append only file rewriting started"
            wait_rewrite_done(client)
            stop_writes.set()
            thread.join(timeout=5)
            if writer_error:
                raise RuntimeError(f"writer failed: {writer_error[0]}")
            before = digest(client, writer_count=writer_count["value"])
        finally:
            stop_writes.set()
            client.close()
    finally:
        stop_server(proc)

    proc = start_server(cfg)
    try:
        client = RespClient(cfg.port)
        try:
            after = digest(client, writer_count=writer_count["value"])
        finally:
            client.close()
    finally:
        stop_server(proc)
    return {"before": before, "after": after, "passed": before == after}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["rdb", "aof", "aof-rewrite"], required=True)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument(
        "--strict-exit-code",
        action="store_true",
        help="Return non-zero when the cycle fails. Chassis json_command runners leave this off so they can parse RunnerResult JSON.",
    )
    args = parser.parse_args()

    if not args.skip_build:
        subprocess.run(["cargo", "build", "-p", "redis-server"], cwd=ROOT, check=True)
    if not RUST_BIN.exists():
        raise SystemExit(f"missing server binary: {RUST_BIN}")

    with tempfile.TemporaryDirectory(prefix=f"redis-rs-persistence-{args.mode}-") as raw:
        tmp = Path(raw)
        started = time.monotonic()
        try:
            if args.mode == "rdb":
                detail = run_rdb_cycle(tmp)
            elif args.mode == "aof":
                detail = run_aof_cycle(tmp)
            else:
                detail = run_aof_rewrite_cycle(tmp)
            status = "pass" if detail["passed"] else "fail"
            error = None
        except Exception as exc:
            detail = {}
            status = "fail"
            error = str(exc)

    measurements = [
        {
            "kind": "official",
            "name": args.mode,
            "metric": "persistence_cycle_pass",
            "target": "rust",
            "capability": f"persistence-{args.mode}",
            "numerator": 1 if status == "pass" else 0,
            "denominator": 1,
        }
    ]
    result = {
        "schema_version": 1,
        "runner_id": f"persistence-{args.mode}-cycle",
        "mode": args.mode,
        "status": status,
        "surface": "correctness",
        "method": "json-command",
        "summary": f"persistence {args.mode} cycle: {status}",
        "claim_level": "internal-regression-gate",
        "measurements": measurements,
        "evidence": {
            "kind": "persistence_cycle",
            "mode": args.mode,
            "commit": git_commit(),
            "timestamp": utc_stamp(),
            "elapsed_s": round(time.monotonic() - started, 3),
            "detail": detail,
            "error": error,
        },
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.strict_exit_code and status != "pass":
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
