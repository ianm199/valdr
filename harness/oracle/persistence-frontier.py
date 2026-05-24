#!/usr/bin/env python3
"""Focused persistence frontier oracle.

This runner exists because the upstream TCL persistence files are currently a
poor overnight signal: many useful cases sit behind `external:skip` or
`needs:debug`, and a generic file-level survey can report 0/0 while important
restart behavior is broken. The scenarios here are source-shaped from
`unit/other.tcl`, `unit/aofrw.tcl`, and `integration/aof*.tcl`, but they emit a
typed per-scenario RunnerResult the chassis can track.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[2]
RUST_BIN = ROOT / "target" / "debug" / "redis-server"
RESULTS_ROOT = ROOT / "harness" / "oracle" / "results" / "persistence-frontier"
AOF_DIRNAME = "appendonlydir"
AOF_BASENAME = "appendonly.aof"
AOF_MANIFEST = f"{AOF_BASENAME}.manifest"


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
class Server:
    proc: subprocess.Popen[str]
    port: int
    dir: Path


@dataclass(frozen=True)
class Scenario:
    name: str
    capability: str
    fn: Callable[[Path], dict[str, Any]]


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


def encode_command(*parts: str | bytes) -> bytes:
    out = f"*{len(parts)}\r\n".encode()
    for part in parts:
        if isinstance(part, str):
            part = part.encode()
        out += f"${len(part)}\r\n".encode() + part + b"\r\n"
    return out


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


def start_server(
    directory: Path,
    *,
    appendonly: bool = False,
    extra: list[str] | None = None,
) -> Server:
    port = free_port()
    cmd = [
        str(RUST_BIN),
        "--port",
        str(port),
        "--bind",
        "127.0.0.1",
        "--dir",
        str(directory),
        "--dbfilename",
        "dump.rdb",
    ]
    if appendonly:
        cmd += ["--appendonly", "yes", "--appendfsync", "always"]
    if extra:
        cmd += extra
    proc = subprocess.Popen(
        cmd,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    wait_for_server(port, proc)
    return Server(proc, port, directory)


def stop_server(server: Server | None) -> str:
    if server is None:
        return ""
    proc = server.proc
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
    if proc.stdout:
        return proc.stdout.read()[-8000:]
    return ""


def expect_startup_failure(
    directory: Path, *, extra: list[str] | None = None, appendonly: bool = True
) -> dict[str, Any]:
    port = free_port()
    cmd = [
        str(RUST_BIN),
        "--port",
        str(port),
        "--bind",
        "127.0.0.1",
        "--dir",
        str(directory),
    ]
    if appendonly:
        cmd += ["--appendonly", "yes"]
    if extra:
        cmd += extra
    proc = subprocess.Popen(
        cmd,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read() if proc.stdout else ""
            return {"passed": proc.returncode != 0, "returncode": proc.returncode, "output": out[-8000:]}
        time.sleep(0.05)
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=3)
    out = proc.stdout.read() if proc.stdout else ""
    return {"passed": False, "returncode": proc.returncode, "output": out[-8000:]}


def bulk(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def normalize_array(values: Any) -> list[str | None]:
    return [bulk(v) for v in values]


def populate_complex(client: RespClient) -> None:
    assert client.command("SET", "s", "value") == "OK"
    assert client.command("MSET", "m1", "one", "m2", "two") == "OK"
    assert client.command("INCR", "counter") == 1
    assert client.command("INCRBY", "counter", "41") == 42
    assert client.command("RPUSH", "list", "a", "b", "c") == 3
    assert client.command("HSET", "hash", "f1", "v1", "f2", "v2") == 2
    assert client.command("SADD", "set", "x", "y", "z") == 3
    assert client.command("ZADD", "zset", "1", "a", "2", "b") == 2


def digest(client: RespClient) -> dict[str, Any]:
    return {
        "s": bulk(client.command("GET", "s")),
        "m1": bulk(client.command("GET", "m1")),
        "m2": bulk(client.command("GET", "m2")),
        "counter": bulk(client.command("GET", "counter")),
        "list": normalize_array(client.command("LRANGE", "list", "0", "-1")),
        "hash": sorted(normalize_array(client.command("HGETALL", "hash"))),
        "set": sorted(normalize_array(client.command("SMEMBERS", "set"))),
        "zset": normalize_array(client.command("ZRANGE", "zset", "0", "-1", "WITHSCORES")),
    }


def wait_rewrite_done(client: RespClient, timeout_s: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        info = client.command("INFO", "persistence")
        text = info.decode("utf-8", "replace") if isinstance(info, bytes) else str(info)
        if "aof_rewrite_in_progress:0" in text:
            return
        time.sleep(0.1)
    raise TimeoutError("AOF rewrite did not finish")


def write_aof(path: Path, commands: list[tuple[str | bytes, ...]], *, trailer: bytes = b"") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"".join(encode_command(*cmd) for cmd in commands) + trailer)


def write_manifest(aof_dir: Path, lines: list[str]) -> Path:
    aof_dir.mkdir(parents=True, exist_ok=True)
    path = aof_dir / AOF_MANIFEST
    path.write_text("".join(f"{line}\n" for line in lines), encoding="utf-8")
    return path


def write_raw_manifest(aof_dir: Path, raw: str) -> Path:
    aof_dir.mkdir(parents=True, exist_ok=True)
    path = aof_dir / AOF_MANIFEST
    path.write_text(raw, encoding="utf-8")
    return path


def manifest_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8").splitlines()


def manifest_seq_by_type(path: Path) -> dict[str, int]:
    seqs: dict[str, int] = {}
    for line in manifest_lines(path):
        parts = line.split()
        if len(parts) < 6:
            continue
        if parts[0] != "file" or parts[2] != "seq" or parts[4] != "type":
            continue
        try:
            seqs[parts[5]] = int(parts[3])
        except ValueError:
            continue
    return seqs


def current_incr_from_manifest(aof_dir: Path) -> Path | None:
    manifest = aof_dir / AOF_MANIFEST
    best: tuple[int, str] | None = None
    for line in manifest_lines(manifest):
        parts = line.split()
        if len(parts) < 6:
            continue
        if parts[0] != "file" or parts[2] != "seq" or parts[4] != "type" or parts[5] != "i":
            continue
        try:
            seq = int(parts[3])
        except ValueError:
            continue
        if best is None or seq > best[0]:
            best = (seq, parts[1])
    if best is None:
        return None
    return aof_dir / best[1]


def scenario_rdb_debug_reload_complex(tmp: Path) -> dict[str, Any]:
    server = start_server(tmp)
    try:
        client = RespClient(server.port)
        try:
            populate_complex(client)
            assert client.command("SAVE") == "OK"
            before = digest(client)
            assert client.command("FLUSHALL") == "OK"
            assert client.command("DEBUG", "RELOAD", "NOSAVE") == "OK"
            after = digest(client)
            return {"passed": before == after, "before": before, "after": after}
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_aof_debug_loadaof_complex(tmp: Path) -> dict[str, Any]:
    server = start_server(tmp, appendonly=True)
    try:
        client = RespClient(server.port)
        try:
            populate_complex(client)
            before = digest(client)
            assert client.command("CONFIG", "SET", "appendonly", "no") == "OK"
            assert client.command("FLUSHALL") == "OK"
            assert client.command("DEBUG", "LOADAOF") == "OK"
            after = digest(client)
            return {"passed": before == after, "before": before, "after": after}
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_expires_after_rdb_reload(tmp: Path) -> dict[str, Any]:
    server = start_server(tmp)
    try:
        client = RespClient(server.port)
        try:
            assert client.command("SET", "x", "10") == "OK"
            assert client.command("EXPIRE", "x", "1000") == 1
            assert client.command("SET", "px", "10") == "OK"
            assert client.command("PEXPIRE", "px", "1000000") == 1
            assert client.command("SAVE") == "OK"
            assert client.command("DEBUG", "RELOAD", "NOSAVE") == "OK"
            ttl = client.command("TTL", "x")
            pttl = client.command("PTTL", "px")
            passed = 900 < ttl <= 1000 and 900_000 < pttl <= 1_000_000
            return {"passed": passed, "ttl": ttl, "pttl": pttl}
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_expires_after_aof_loadaof(tmp: Path) -> dict[str, Any]:
    server = start_server(tmp, appendonly=True)
    try:
        client = RespClient(server.port)
        try:
            assert client.command("SET", "x", "10") == "OK"
            assert client.command("EXPIRE", "x", "1000") == 1
            assert client.command("SET", "px", "10") == "OK"
            assert client.command("PEXPIRE", "px", "1000000") == 1
            assert client.command("CONFIG", "SET", "appendonly", "no") == "OK"
            assert client.command("FLUSHALL") == "OK"
            assert client.command("DEBUG", "LOADAOF") == "OK"
            ttl = client.command("TTL", "x")
            pttl = client.command("PTTL", "px")
            passed = 900 < ttl <= 1000 and 900_000 < pttl <= 1_000_000
            return {"passed": passed, "ttl": ttl, "pttl": pttl}
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_aof_load_truncated_yes(tmp: Path) -> dict[str, Any]:
    write_aof(
        tmp / "appendonly.aof",
        [("INCR", "foo")] * 5,
        trailer=encode_command("INCR", "foo")[:-1],
    )
    server = start_server(tmp, appendonly=True, extra=["--aof-load-truncated", "yes"])
    try:
        client = RespClient(server.port)
        try:
            value = bulk(client.command("GET", "foo"))
            return {"passed": value == "5", "foo": value}
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_aof_load_truncated_no_fails(tmp: Path) -> dict[str, Any]:
    write_aof(
        tmp / "appendonly.aof",
        [("SET", "foo", "hello")],
        trailer=encode_command("SET", "bar", "world")[:-2],
    )
    return expect_startup_failure(tmp, extra=["--aof-load-truncated", "no"])


def scenario_aof_unknown_command_fails(tmp: Path) -> dict[str, Any]:
    write_aof(
        tmp / "appendonly.aof",
        [("SET", "foo", "hello"), ("BLA", "foo", "hello"), ("SET", "foo", "again")],
    )
    return expect_startup_failure(tmp, extra=["--aof-load-truncated", "yes"])


def scenario_aof_getex_no_append(tmp: Path) -> dict[str, Any]:
    server = start_server(tmp, appendonly=True)
    try:
        client = RespClient(server.port)
        try:
            assert client.command("SET", "foo", "bar") == "OK"
            path = current_incr_from_manifest(tmp / AOF_DIRNAME)
            if path is None or not path.exists():
                legacy_path = tmp / "appendonly.aof"
                path = legacy_path if legacy_path.exists() else path
            if path is None or not path.exists():
                return {
                    "passed": False,
                    "reason": "no current AOF path found",
                    "manifest_lines": manifest_lines(tmp / AOF_DIRNAME / AOF_MANIFEST),
                }
            before = path.stat().st_size
            assert bulk(client.command("GETEX", "foo")) == "bar"
            time.sleep(0.05)
            after = path.stat().st_size
            return {
                "passed": before == after,
                "path": str(path.relative_to(tmp)),
                "size_before": before,
                "size_after": after,
            }
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_aof_spop_count_replay(tmp: Path) -> dict[str, Any]:
    write_aof(
        tmp / "appendonly.aof",
        [
            ("SADD", "set", "foo"),
            ("SADD", "set", "bar"),
            ("SADD", "set", "gah"),
            ("SPOP", "set", "2"),
        ],
    )
    server = start_server(tmp, appendonly=True)
    try:
        client = RespClient(server.port)
        try:
            card = client.command("SCARD", "set")
            return {"passed": card == 1, "scard": card}
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_aof_lmpop_zmpop_replay(tmp: Path) -> dict[str, Any]:
    write_aof(
        tmp / "appendonly.aof",
        [
            ("RPUSH", "mylist", "a", "b", "c"),
            ("LMPOP", "1", "mylist", "LEFT", "COUNT", "2"),
            ("ZADD", "myzset", "1", "a", "2", "b", "3", "c"),
            ("ZMPOP", "1", "myzset", "MIN", "COUNT", "2"),
        ],
    )
    server = start_server(tmp, appendonly=True)
    try:
        client = RespClient(server.port)
        try:
            llen = client.command("LLEN", "mylist")
            zcard = client.command("ZCARD", "myzset")
            return {"passed": llen == 1 and zcard == 1, "llen": llen, "zcard": zcard}
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_aof_rewrite_collections_digest(tmp: Path) -> dict[str, Any]:
    server = start_server(tmp, appendonly=True)
    try:
        client = RespClient(server.port)
        try:
            populate_complex(client)
            before = digest(client)
            assert client.command("BGREWRITEAOF") == "Background append only file rewriting started"
            wait_rewrite_done(client)
            assert client.command("CONFIG", "SET", "appendonly", "no") == "OK"
            assert client.command("FLUSHALL") == "OK"
            assert client.command("DEBUG", "LOADAOF") == "OK"
            after = digest(client)
            return {"passed": before == after, "before": before, "after": after}
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_multipart_manifest_basic_load(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    aof_dir.mkdir()
    write_aof(aof_dir / f"{AOF_BASENAME}.1.incr.aof", [("SET", "mp", "ok")])
    write_manifest(aof_dir, [f"file {AOF_BASENAME}.1.incr.aof seq 1 type i"])
    server = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(server.port)
        try:
            value = bulk(client.command("GET", "mp"))
            return {"passed": value == "ok", "mp": value}
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_multipart_manifest_missing_file_fails(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    write_aof(aof_dir / f"{AOF_BASENAME}.1.base.aof", [("SET", "k1", "v1")])
    write_aof(aof_dir / f"{AOF_BASENAME}.2.incr.aof", [("SET", "k2", "v2")])
    write_manifest(
        aof_dir,
        [
            f"file {AOF_BASENAME}.1.base.aof seq 1 type b",
            f"file {AOF_BASENAME}.1.incr.aof seq 1 type i",
            f"file {AOF_BASENAME}.2.incr.aof seq 2 type i",
        ],
    )
    return expect_startup_failure(tmp, extra=["--appenddirname", AOF_DIRNAME])


def scenario_multipart_manifest_non_monotonic_incr_fails(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    write_aof(aof_dir / f"{AOF_BASENAME}.1.incr.aof", [("SET", "k1", "v1")])
    write_aof(aof_dir / f"{AOF_BASENAME}.2.incr.aof", [("SET", "k2", "v2")])
    write_manifest(
        aof_dir,
        [
            f"file {AOF_BASENAME}.2.incr.aof seq 2 type i",
            f"file {AOF_BASENAME}.1.incr.aof seq 1 type i",
        ],
    )
    return expect_startup_failure(tmp, extra=["--appenddirname", AOF_DIRNAME])


def scenario_multipart_manifest_blank_line_fails(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    write_aof(aof_dir / f"{AOF_BASENAME}.1.incr.aof", [("SET", "k1", "v1")])
    write_aof(aof_dir / f"{AOF_BASENAME}.3.incr.aof", [("SET", "k3", "v3")])
    write_raw_manifest(
        aof_dir,
        f"file {AOF_BASENAME}.1.incr.aof seq 1 type i\n\n"
        f"file {AOF_BASENAME}.3.incr.aof seq 3 type i\n",
    )
    return expect_startup_failure(tmp, extra=["--appenddirname", AOF_DIRNAME])


def scenario_multipart_manifest_empty_file_fails(tmp: Path) -> dict[str, Any]:
    write_raw_manifest(tmp / AOF_DIRNAME, "")
    return expect_startup_failure(tmp, extra=["--appenddirname", AOF_DIRNAME])


def scenario_multipart_manifest_duplicate_base_fails(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    write_aof(aof_dir / f"{AOF_BASENAME}.1.base.aof", [("SET", "k1", "v1")])
    write_aof(aof_dir / f"{AOF_BASENAME}.2.base.aof", [("SET", "k2", "v2")])
    write_aof(aof_dir / f"{AOF_BASENAME}.1.incr.aof", [("SET", "k3", "v3")])
    write_manifest(
        aof_dir,
        [
            f"file {AOF_BASENAME}.1.base.aof seq 1 type b",
            f"file {AOF_BASENAME}.2.base.aof seq 2 type b",
            f"file {AOF_BASENAME}.1.incr.aof seq 1 type i",
        ],
    )
    return expect_startup_failure(tmp, extra=["--appenddirname", AOF_DIRNAME])


def scenario_multipart_manifest_unknown_type_fails(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    write_aof(aof_dir / f"{AOF_BASENAME}.1.base.aof", [("SET", "k1", "v1")])
    write_aof(aof_dir / f"{AOF_BASENAME}.1.incr.aof", [("SET", "k3", "v3")])
    write_manifest(
        aof_dir,
        [
            f"file {AOF_BASENAME}.1.base.aof seq 1 type x",
            f"file {AOF_BASENAME}.1.incr.aof seq 1 type i",
        ],
    )
    return expect_startup_failure(tmp, extra=["--appenddirname", AOF_DIRNAME])


def scenario_multipart_empty_dir_startup(tmp: Path) -> dict[str, Any]:
    (tmp / AOF_DIRNAME).mkdir()
    server = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(server.port)
        try:
            pong = client.command("PING")
            dbsize = client.command("DBSIZE")
            return {
                "passed": pong == "PONG" and dbsize == 0,
                "pong": pong,
                "dbsize": dbsize,
                "manifest_exists": (tmp / AOF_DIRNAME / AOF_MANIFEST).exists(),
            }
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_multipart_manifest_discontinuous_incr_load(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    write_aof(aof_dir / f"{AOF_BASENAME}.1.base.aof", [("SET", "k1", "v1")])
    write_aof(aof_dir / f"{AOF_BASENAME}.1.incr.aof", [("SET", "k2", "v2")])
    write_aof(aof_dir / f"{AOF_BASENAME}.3.incr.aof", [("SET", "k3", "v3")])
    write_manifest(
        aof_dir,
        [
            f"file {AOF_BASENAME}.1.base.aof seq 1 type b",
            f"file {AOF_BASENAME}.1.incr.aof seq 1 type i",
            f"file {AOF_BASENAME}.3.incr.aof seq 3 type i",
        ],
    )
    server = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(server.port)
        try:
            values = {key: bulk(client.command("GET", key)) for key in ("k1", "k2", "k3")}
            return {
                "passed": values == {"k1": "v1", "k2": "v2", "k3": "v3"},
                "values": values,
            }
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_multipart_manifest_empty_incr_load(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    write_aof(aof_dir / f"{AOF_BASENAME}.1.base.aof", [("SET", "k1", "v1")])
    write_aof(aof_dir / f"{AOF_BASENAME}.1.incr.aof", [])
    write_aof(aof_dir / f"{AOF_BASENAME}.3.incr.aof", [("SET", "k3", "v3")])
    write_manifest(
        aof_dir,
        [
            f"file {AOF_BASENAME}.1.base.aof seq 1 type b",
            f"file {AOF_BASENAME}.1.incr.aof seq 1 type i",
            f"file {AOF_BASENAME}.3.incr.aof seq 3 type i",
        ],
    )
    server = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(server.port)
        try:
            values = {key: bulk(client.command("GET", key)) for key in ("k1", "k2", "k3")}
            return {
                "passed": values == {"k1": "v1", "k2": None, "k3": "v3"},
                "values": values,
            }
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_multipart_appendonly_enable_layout(tmp: Path) -> dict[str, Any]:
    server = start_server(
        tmp,
        extra=[
            "--appenddirname",
            AOF_DIRNAME,
            "--auto-aof-rewrite-percentage",
            "0",
        ],
    )
    try:
        client = RespClient(server.port)
        try:
            assert client.command("SET", "k1", "v1") == "OK"
            assert client.command("CONFIG", "SET", "appendonly", "yes") == "OK"
            wait_rewrite_done(client)
            manifest = tmp / AOF_DIRNAME / AOF_MANIFEST
            lines = manifest_lines(manifest)
            referenced = [line.split()[1] for line in lines if len(line.split()) >= 2]
            existing = [name for name in referenced if (tmp / AOF_DIRNAME / name).exists()]
            has_base = any(" type b" in line for line in lines)
            has_incr = any(" type i" in line for line in lines)
            return {
                "passed": manifest.exists() and has_base and has_incr and len(existing) == len(referenced),
                "manifest_exists": manifest.exists(),
                "lines": lines,
                "referenced_existing": existing,
            }
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_multipart_rewrite_sequence_advance(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    write_aof(aof_dir / f"{AOF_BASENAME}.7.base.aof", [("SET", "k1", "v1")])
    write_aof(aof_dir / f"{AOF_BASENAME}.3.incr.aof", [("SET", "k2", "v2")])
    manifest = write_manifest(
        aof_dir,
        [
            f"file {AOF_BASENAME}.7.base.aof seq 7 type b",
            f"file {AOF_BASENAME}.3.incr.aof seq 3 type i",
        ],
    )
    server = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(server.port)
        try:
            before = manifest_seq_by_type(manifest)
            reply = client.command("BGREWRITEAOF")
            wait_rewrite_done(client)
            after = manifest_seq_by_type(manifest)
            return {
                "passed": after.get("b") == 8 and after.get("i") == 4,
                "reply": reply,
                "before": before,
                "after": after,
                "lines": manifest_lines(manifest),
            }
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_rdb_corrupt_file_fatal_startup(tmp: Path) -> dict[str, Any]:
    # integration/rdb.tcl: "Server should not start if RDB is corrupted".
    (tmp / "dump.rdb").write_bytes(b"REDIS-not-a-valid-rdb\xff\x00garbage-bytes")
    return expect_startup_failure(tmp, appendonly=False, extra=["--dbfilename", "dump.rdb"])


def scenario_rdb_missing_file_empty_startup(tmp: Path) -> dict[str, Any]:
    # integration/rdb.tcl: "Server started empty with non-existing RDB file".
    server = start_server(tmp)
    try:
        client = RespClient(server.port)
        try:
            pong = client.command("PING")
            dbsize = client.command("DBSIZE")
            return {"passed": pong == "PONG" and dbsize == 0, "pong": pong, "dbsize": dbsize}
        finally:
            client.close()
    finally:
        stop_server(server)


def scenario_rdb_bgsave_status_ok_file_written(tmp: Path) -> dict[str, Any]:
    # BGSAVE forks a COW child, writes dump.rdb, and reports ok status.
    server = start_server(tmp, extra=["--save", ""])
    try:
        client = RespClient(server.port)
        try:
            populate_complex(client)
            reply = bulk(client.command("BGSAVE"))
            text = ""
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                info = client.command("INFO", "persistence")
                text = info.decode("utf-8", "replace") if isinstance(info, bytes) else str(info)
                if "rdb_bgsave_in_progress:0" in text and "rdb_last_bgsave_status:ok" in text:
                    break
                time.sleep(0.1)
            rdb_exists = (tmp / "dump.rdb").exists()
            status_ok = "rdb_last_bgsave_status:ok" in text
            return {
                "passed": rdb_exists and status_ok and reply == "Background saving started",
                "reply": reply,
                "rdb_exists": rdb_exists,
                "status_ok": status_ok,
            }
        finally:
            client.close()
    finally:
        stop_server(server)


SCENARIOS: list[Scenario] = [
    Scenario("rdb-debug-reload-complex-dataset", "persistence-rdb", scenario_rdb_debug_reload_complex),
    Scenario("rdb-corrupt-file-fatal-startup", "persistence-rdb", scenario_rdb_corrupt_file_fatal_startup),
    Scenario("rdb-missing-file-empty-startup", "persistence-rdb", scenario_rdb_missing_file_empty_startup),
    Scenario("rdb-bgsave-status-ok-file-written", "persistence-rdb", scenario_rdb_bgsave_status_ok_file_written),
    Scenario("aof-debug-loadaof-complex-dataset", "persistence-aof", scenario_aof_debug_loadaof_complex),
    Scenario("expires-after-rdb-reload", "persistence-rdb", scenario_expires_after_rdb_reload),
    Scenario("expires-after-aof-loadaof", "persistence-aof", scenario_expires_after_aof_loadaof),
    Scenario("aof-load-truncated-yes-short-read", "persistence-aof", scenario_aof_load_truncated_yes),
    Scenario("aof-load-truncated-no-fails", "persistence-aof", scenario_aof_load_truncated_no_fails),
    Scenario("aof-unknown-command-fails-startup", "persistence-aof", scenario_aof_unknown_command_fails),
    Scenario("getex-does-not-append-to-aof", "persistence-aof-propagation", scenario_aof_getex_no_append),
    Scenario("aof-spop-count-replay", "persistence-aof-propagation", scenario_aof_spop_count_replay),
    Scenario("aof-lmpop-zmpop-replay", "persistence-aof-propagation", scenario_aof_lmpop_zmpop_replay),
    Scenario("aof-rewrite-collections-digest", "persistence-aof-rewrite", scenario_aof_rewrite_collections_digest),
    Scenario("multipart-aof-manifest-basic-load", "persistence-aof-manifest", scenario_multipart_manifest_basic_load),
    Scenario("multipart-aof-manifest-missing-file-fails", "persistence-aof-manifest", scenario_multipart_manifest_missing_file_fails),
    Scenario("multipart-aof-manifest-non-monotonic-incr-fails", "persistence-aof-manifest", scenario_multipart_manifest_non_monotonic_incr_fails),
    Scenario("multipart-aof-manifest-blank-line-fails", "persistence-aof-manifest", scenario_multipart_manifest_blank_line_fails),
    Scenario("multipart-aof-manifest-empty-file-fails", "persistence-aof-manifest", scenario_multipart_manifest_empty_file_fails),
    Scenario("multipart-aof-manifest-duplicate-base-fails", "persistence-aof-manifest", scenario_multipart_manifest_duplicate_base_fails),
    Scenario("multipart-aof-manifest-unknown-type-fails", "persistence-aof-manifest", scenario_multipart_manifest_unknown_type_fails),
    Scenario("multipart-aof-empty-dir-startup", "persistence-aof-manifest", scenario_multipart_empty_dir_startup),
    Scenario("multipart-aof-manifest-discontinuous-incr-load", "persistence-aof-manifest", scenario_multipart_manifest_discontinuous_incr_load),
    Scenario("multipart-aof-manifest-empty-incr-load", "persistence-aof-manifest", scenario_multipart_manifest_empty_incr_load),
    Scenario("multipart-aof-appendonly-enable-layout", "persistence-aof-manifest", scenario_multipart_appendonly_enable_layout),
    Scenario("multipart-aof-rewrite-sequence-advance", "persistence-aof-manifest", scenario_multipart_rewrite_sequence_advance),
]


def parse_scenarios(raw: str | None) -> list[Scenario]:
    if not raw:
        return SCENARIOS
    wanted = {part.strip() for part in raw.split(",") if part.strip()}
    known = {scenario.name: scenario for scenario in SCENARIOS}
    missing = sorted(wanted - set(known))
    if missing:
        raise SystemExit(f"unknown scenario(s): {', '.join(missing)}")
    return [scenario for scenario in SCENARIOS if scenario.name in wanted]


def run_scenario(scenario: Scenario, run_dir: Path) -> dict[str, Any]:
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix=f"redis-rs-{scenario.name}-") as raw:
        tmp = Path(raw)
        try:
            detail = scenario.fn(tmp)
            passed = bool(detail.get("passed"))
            error = None
        except Exception as exc:
            detail = {}
            passed = False
            error = str(exc)
    result = {
        "name": scenario.name,
        "capability": scenario.capability,
        "passed": passed,
        "elapsed_s": round(time.monotonic() - started, 3),
        "error": error,
        "detail": detail,
    }
    (run_dir / f"{scenario.name}.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--scenarios", help="Comma-separated scenario subset.")
    parser.add_argument("--fail-on-failure", action="store_true")
    args = parser.parse_args()

    if not args.skip_build:
        subprocess.run(["cargo", "build", "-p", "redis-server"], cwd=ROOT, check=True)
    if not RUST_BIN.exists():
        raise SystemExit(f"missing server binary: {RUST_BIN}")

    scenarios = parse_scenarios(args.scenarios)
    run_id = utc_stamp()
    run_dir = RESULTS_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    results = [run_scenario(scenario, run_dir) for scenario in scenarios]
    passed = sum(1 for item in results if item["passed"])
    failed = len(results) - passed
    measurements = [
        {
            "kind": "official",
            "name": item["name"],
            "metric": "persistence_frontier_pass",
            "target": "rust-vs-reference",
            "capability": item["capability"],
            "test": item["name"],
            "numerator": 1 if item["passed"] else 0,
            "denominator": 1,
        }
        for item in results
    ]
    measurements.extend(
        [
            {
                "kind": "official",
                "name": "persistence-frontier",
                "metric": "persistence_frontier_pass_count",
                "target": "rust-vs-reference",
                "capability": "persistence-frontier",
                "value": passed,
                "unit": "scenarios",
            },
            {
                "kind": "official",
                "name": "persistence-frontier",
                "metric": "persistence_frontier_fail_count",
                "target": "rust-vs-reference",
                "capability": "persistence-frontier",
                "value": failed,
                "unit": "scenarios",
            },
            {
                "kind": "official",
                "name": "persistence-frontier",
                "metric": "persistence_frontier_pass_ratio",
                "target": "rust-vs-reference",
                "capability": "persistence-frontier",
                "numerator": passed,
                "denominator": len(results),
                "value": passed / len(results) if results else 0,
                "unit": "pass/total",
            },
        ]
    )
    result = {
        "schema_version": 1,
        "runner_id": "persistence-frontier",
        "status": "fail" if failed and args.fail_on_failure else "pass",
        "surface": "correctness",
        "method": "official-suite",
        "summary": f"persistence frontier: {passed}/{len(results)} scenarios passing",
        "claim_level": "telemetry",
        "measurements": measurements,
        "artifacts": [
            {
                "kind": "persistence-frontier-scenario",
                "path": str((run_dir / f"{item['name']}.json").relative_to(ROOT)),
                "test": item["name"],
            }
            for item in results
        ],
        "evidence": {
            "kind": "persistence_frontier",
            "run_id": run_id,
            "commit": git_commit(),
            "elapsed_s": round(time.monotonic() - started, 3),
            "scenarios": results,
        },
    }
    (run_dir / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 1 if failed and args.fail_on_failure else 0


if __name__ == "__main__":
    sys.exit(main())
