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
import threading
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
AOF_FAULT_ENV = "VALDR_AOF_FAULT"


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
    env: dict[str, str] | None = None,
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
    proc_env = os.environ.copy()
    if env:
        proc_env.update(env)
    proc = subprocess.Popen(
        cmd,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=proc_env,
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


def info_section(client: RespClient, section: str) -> dict[str, str]:
    raw = client.command("INFO", section)
    text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else str(raw)
    fields: dict[str, str] = {}
    for line in text.splitlines():
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields[key] = value.strip()
    return fields


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


def manifest_names_by_type(lines: list[str], file_type: str | None = None) -> list[str]:
    names: list[str] = []
    for line in lines:
        parts = line.split()
        if len(parts) < 6:
            continue
        if parts[0] != "file" or parts[2] != "seq" or parts[4] != "type":
            continue
        if file_type is None or parts[5] == file_type:
            names.append(parts[1])
    return names


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


def current_base_from_manifest(aof_dir: Path) -> Path | None:
    manifest = aof_dir / AOF_MANIFEST
    best: tuple[int, str] | None = None
    for line in manifest_lines(manifest):
        parts = line.split()
        if len(parts) < 6:
            continue
        if parts[0] != "file" or parts[2] != "seq" or parts[4] != "type" or parts[5] != "b":
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


def run_check_aof(tmp: Path, target: Path, extra: list[str] | None = None) -> dict[str, Any]:
    checker = tmp / "valkey-check-aof"
    if not checker.exists():
        checker.symlink_to(RUST_BIN)
    cmd = [str(checker)]
    if extra:
        cmd.extend(extra)
    cmd.append(str(target))
    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=20,
    )
    return {
        "returncode": proc.returncode,
        "output": proc.stdout[-8000:],
        "passed": proc.returncode == 0,
    }


def setup_preliminary_rewrite_layout(aof_dir: Path) -> Path:
    write_aof(aof_dir / f"{AOF_BASENAME}.1.base.aof", [("SET", "crash:base", "1")])
    write_aof(aof_dir / f"{AOF_BASENAME}.1.incr.aof", [("SET", "crash:old-incr", "1")])
    write_aof(aof_dir / f"{AOF_BASENAME}.2.incr.aof", [("SET", "crash:new-incr", "1")])
    return write_manifest(
        aof_dir,
        [
            f"file {AOF_BASENAME}.1.base.aof seq 1 type b",
            f"file {AOF_BASENAME}.1.incr.aof seq 1 type i",
            f"file {AOF_BASENAME}.2.incr.aof seq 2 type i",
        ],
    )


def verify_crash_layout_loads(tmp: Path, absent: list[str] | None = None) -> dict[str, Any]:
    absent = absent or []
    server = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(server.port)
        try:
            values = {
                key: bulk(client.command("GET", key))
                for key in ("crash:base", "crash:old-incr", "crash:new-incr", *absent)
            }
            expected = {
                "crash:base": "1",
                "crash:old-incr": "1",
                "crash:new-incr": "1",
                **{key: None for key in absent},
            }
            return {"passed": values == expected, "values": values, "expected": expected}
        finally:
            client.close()
    finally:
        stop_server(server)


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
    legacy = tmp / "appendonly.aof"
    write_aof(
        legacy,
        [("INCR", "foo")] * 5,
        trailer=encode_command("INCR", "foo")[:-1],
    )
    server = start_server(tmp, appendonly=True, extra=["--aof-load-truncated", "yes"])
    try:
        client = RespClient(server.port)
        try:
            value = bulk(client.command("GET", "foo"))
            assert client.command("INCR", "foo") == 6
        finally:
            client.close()
    finally:
        stop_server(server)

    restart = start_server(tmp, appendonly=True, extra=["--aof-load-truncated", "no"])
    try:
        client = RespClient(restart.port)
        try:
            restarted_value = bulk(client.command("GET", "foo"))
        finally:
            client.close()
    finally:
        restart_log = stop_server(restart)

    legacy_size = legacy.stat().st_size if legacy.exists() else 0
    passed = value == "5" and restarted_value == "6" and legacy_size > 0
    return {
        "passed": passed,
        "foo_after_truncated_load": value,
        "foo_after_append_restart": restarted_value,
        "legacy_size_after_truncate": legacy_size,
        "restart_log_tail": restart_log[-2000:],
    }


def scenario_multipart_aof_load_truncated_append_survives_restart(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    incr = f"{AOF_BASENAME}.1.incr.aof"
    write_aof(aof_dir / incr, [("INCR", "foo")] * 5, trailer=encode_command("INCR", "foo")[:-1])
    write_manifest(aof_dir, [f"file {incr} seq 1 type i"])

    server = start_server(
        tmp,
        appendonly=True,
        extra=["--appenddirname", AOF_DIRNAME, "--aof-load-truncated", "yes"],
    )
    try:
        client = RespClient(server.port)
        try:
            value = bulk(client.command("GET", "foo"))
            incr_reply = client.command("INCR", "foo")
        finally:
            client.close()
    finally:
        stop_server(server)

    restart = start_server(
        tmp,
        appendonly=True,
        extra=["--appenddirname", AOF_DIRNAME, "--aof-load-truncated", "no"],
    )
    try:
        client = RespClient(restart.port)
        try:
            restarted_value = bulk(client.command("GET", "foo"))
        finally:
            client.close()
    finally:
        restart_log = stop_server(restart)

    incr_size = (aof_dir / incr).stat().st_size if (aof_dir / incr).exists() else 0
    passed = value == "5" and incr_reply == 6 and restarted_value == "6"
    return {
        "passed": passed,
        "foo_after_truncated_load": value,
        "incr_reply": incr_reply,
        "foo_after_append_restart": restarted_value,
        "incr_size_after_restart": incr_size,
        "manifest_lines": manifest_lines(aof_dir / AOF_MANIFEST),
        "restart_log_tail": restart_log[-2000:],
    }


def scenario_multipart_aof_expired_key_not_resurrected_by_later_push(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    incr = f"{AOF_BASENAME}.1.incr.aof"
    write_aof(
        aof_dir / incr,
        [
            ("RPUSH", "list", "foo"),
            ("PEXPIREAT", "list", "1000"),
            ("RPUSH", "list", "bar"),
        ],
    )
    write_manifest(aof_dir, [f"file {incr} seq 1 type i"])
    server = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(server.port)
        try:
            llen = client.command("LLEN", "list")
        finally:
            client.close()
    finally:
        log = stop_server(server)
    return {
        "passed": llen == 0,
        "llen": llen,
        "server_log_tail": log[-2000:],
    }


def scenario_aof_load_truncated_no_fails(tmp: Path) -> dict[str, Any]:
    write_aof(
        tmp / "appendonly.aof",
        [("SET", "foo", "hello")],
        trailer=encode_command("SET", "bar", "world")[:-2],
    )
    result = expect_startup_failure(tmp, extra=["--aof-load-truncated", "no"])
    result["passed"] = result["passed"] and "Unexpected end of file reading the append only file" in result["output"]
    return result


def scenario_aof_unfinished_multi_load_truncated_no_fails(tmp: Path) -> dict[str, Any]:
    write_aof(
        tmp / "appendonly.aof",
        [("SET", "foo", "hello"), ("MULTI",), ("SET", "bar", "world")],
    )
    result = expect_startup_failure(tmp, extra=["--aof-load-truncated", "no"])
    result["passed"] = result["passed"] and "Unexpected end of file reading the append only file" in result["output"]
    return result


def scenario_aof_bad_format_logs_upstream_error(tmp: Path) -> dict[str, Any]:
    path = tmp / "appendonly.aof"
    path.write_bytes(
        encode_command("SET", "foo", "hello")
        + b"!!!"
        + encode_command("SET", "foo", "again")
    )
    result = expect_startup_failure(tmp, extra=["--aof-load-truncated", "yes"])
    result["passed"] = result["passed"] and "Bad file format reading the append only file" in result["output"]
    return result


def scenario_aof_unknown_command_fails(tmp: Path) -> dict[str, Any]:
    write_aof(
        tmp / "appendonly.aof",
        [("SET", "foo", "hello"), ("bla", "foo", "hello"), ("SET", "foo", "again")],
    )
    result = expect_startup_failure(tmp, extra=["--aof-load-truncated", "yes"])
    result["passed"] = result["passed"] and "unknown command 'bla'" in result["output"]
    return result


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


def scenario_aof_blocked_lmpop_zmpop_wake_persists(tmp: Path) -> dict[str, Any]:
    server = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    list_reply: list[Any] = []
    zset_reply: list[Any] = []
    errors: list[str] = []

    def wait_list_pop() -> None:
        try:
            client = RespClient(server.port)
            try:
                list_reply.append(
                    client.command("BLMPOP", "5", "1", "wake:list", "LEFT", "COUNT", "2")
                )
            finally:
                client.close()
        except Exception as exc:  # noqa: BLE001 - captured into scenario detail
            errors.append(f"list:{exc!r}")

    def wait_zset_pop() -> None:
        try:
            client = RespClient(server.port)
            try:
                zset_reply.append(
                    client.command("BZMPOP", "5", "1", "wake:zset", "MIN", "COUNT", "2")
                )
            finally:
                client.close()
        except Exception as exc:  # noqa: BLE001 - captured into scenario detail
            errors.append(f"zset:{exc!r}")

    def normalize_blocked_reply(reply: Any) -> Any:
        if isinstance(reply, list) and len(reply) == 2:
            values = reply[1]
            if isinstance(values, list):
                if values and isinstance(values[0], list):
                    return [bulk(reply[0]), [normalize_array(pair) for pair in values]]
                return [bulk(reply[0]), normalize_array(values)]
        return str(reply)

    try:
        client = RespClient(server.port)
        try:
            list_thread = threading.Thread(target=wait_list_pop)
            list_thread.start()
            time.sleep(0.1)
            list_push = client.command("RPUSH", "wake:list", "a", "b", "c")
            list_thread.join(timeout=5)
            if list_thread.is_alive():
                errors.append("list thread timed out")

            zset_thread = threading.Thread(target=wait_zset_pop)
            zset_thread.start()
            time.sleep(0.1)
            zset_add = client.command("ZADD", "wake:zset", "1", "a", "2", "b", "3", "c")
            zset_thread.join(timeout=5)
            if zset_thread.is_alive():
                errors.append("zset thread timed out")

            live_list = normalize_array(client.command("LRANGE", "wake:list", "0", "-1"))
            live_zset = normalize_array(
                client.command("ZRANGE", "wake:zset", "0", "-1", "WITHSCORES")
            )
        finally:
            client.close()
    finally:
        stop_server(server)

    restart = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(restart.port)
        try:
            restart_list = normalize_array(client.command("LRANGE", "wake:list", "0", "-1"))
            restart_zset = normalize_array(
                client.command("ZRANGE", "wake:zset", "0", "-1", "WITHSCORES")
            )
        finally:
            client.close()
    finally:
        restart_log = stop_server(restart)

    list_view = normalize_blocked_reply(list_reply[0]) if list_reply else None
    zset_view = normalize_blocked_reply(zset_reply[0]) if zset_reply else None
    passed = (
        not errors
        and list_push == 3
        and zset_add == 3
        and list_view == ["wake:list", ["a", "b"]]
        and live_list == ["c"]
        and restart_list == live_list
        and live_zset == ["c", "3"]
        and restart_zset == live_zset
    )
    return {
        "passed": passed,
        "errors": errors,
        "list_reply": list_view,
        "zset_reply": zset_view,
        "live_list": live_list,
        "restart_list": restart_list,
        "live_zset": live_zset,
        "restart_zset": restart_zset,
        "restart_log_tail": restart_log[-2000:],
    }


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
            info = info_section(client, "persistence")
            base_size = int(info.get("aof_base_size", "0"))
            current_size = int(info.get("aof_current_size", "0"))
            info_ok = (
                info.get("aof_enabled") == "1"
                and info.get("aof_rewrite_in_progress") == "0"
                and info.get("aof_rewrite_scheduled") == "0"
                and base_size > 0
                and current_size >= base_size
            )
            return {
                "passed": manifest.exists()
                and has_base
                and has_incr
                and len(existing) == len(referenced)
                and info_ok,
                "manifest_exists": manifest.exists(),
                "lines": lines,
                "referenced_existing": existing,
                "info": {
                    "aof_enabled": info.get("aof_enabled"),
                    "aof_rewrite_in_progress": info.get("aof_rewrite_in_progress"),
                    "aof_rewrite_scheduled": info.get("aof_rewrite_scheduled"),
                    "aof_base_size": base_size,
                    "aof_current_size": current_size,
                },
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


def scenario_multipart_rewrite_preliminary_manifest_survives_restart(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    manifest = setup_preliminary_rewrite_layout(aof_dir)
    result = verify_crash_layout_loads(tmp)
    result["lines"] = manifest_lines(manifest)
    return result


def scenario_multipart_rewrite_temp_base_ignored_before_final_manifest(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    manifest = setup_preliminary_rewrite_layout(aof_dir)
    write_aof(aof_dir / "temp-rewriteaof-bg-999999.aof", [("SET", "crash:temp-base", "ignored")])
    result = verify_crash_layout_loads(tmp, absent=["crash:temp-base"])
    result["lines"] = manifest_lines(manifest)
    result["temp_base_exists"] = (aof_dir / "temp-rewriteaof-bg-999999.aof").exists()
    return result


def scenario_multipart_rewrite_final_base_ignored_before_manifest(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    manifest = setup_preliminary_rewrite_layout(aof_dir)
    write_aof(aof_dir / f"{AOF_BASENAME}.2.base.aof", [("SET", "crash:final-base", "ignored")])
    result = verify_crash_layout_loads(tmp, absent=["crash:final-base"])
    result["lines"] = manifest_lines(manifest)
    result["final_base_exists"] = (aof_dir / f"{AOF_BASENAME}.2.base.aof").exists()
    return result


def scenario_multipart_rewrite_failed_replayable_and_status_err(tmp: Path) -> dict[str, Any]:
    server = start_server(
        tmp,
        appendonly=True,
        extra=[
            "--appenddirname",
            AOF_DIRNAME,
            "--aof-use-rdb-preamble",
            "no",
        ],
    )
    try:
        client = RespClient(server.port)
        try:
            assert client.command("SET", "failed:before", "1") == "OK"
            conflict = tmp / AOF_DIRNAME / f"temp-rewriteaof-bg-{server.proc.pid}.aof"
            conflict.mkdir(parents=True, exist_ok=False)
            reply = client.command("BGREWRITEAOF")
            wait_rewrite_done(client)
            assert client.command("SET", "failed:after", "1") == "OK"
            info = info_section(client, "persistence")
            lines = manifest_lines(tmp / AOF_DIRNAME / AOF_MANIFEST)
        finally:
            client.close()
    finally:
        stop_server(server)

    restart = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(restart.port)
        try:
            values = {
                "failed:before": bulk(client.command("GET", "failed:before")),
                "failed:after": bulk(client.command("GET", "failed:after")),
            }
            passed = (
                values == {"failed:before": "1", "failed:after": "1"}
                and info.get("aof_rewrite_in_progress") == "0"
                and info.get("aof_last_bgrewrite_status") == "err"
                and len([line for line in lines if " type i" in line]) >= 2
            )
            return {
                "passed": passed,
                "reply": reply,
                "values": values,
                "info": {
                    "aof_rewrite_in_progress": info.get("aof_rewrite_in_progress"),
                    "aof_last_bgrewrite_status": info.get("aof_last_bgrewrite_status"),
                    "aof_current_size": info.get("aof_current_size"),
                },
                "lines": lines,
                "conflict_path": str(conflict.relative_to(tmp)),
            }
        finally:
            client.close()
    finally:
        stop_server(restart)


def scenario_multipart_rewrite_corrupt_final_base_fails_closed(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    write_aof(aof_dir / f"{AOF_BASENAME}.2.incr.aof", [("SET", "after", "1")])
    (aof_dir / f"{AOF_BASENAME}.2.base.aof").parent.mkdir(parents=True, exist_ok=True)
    (aof_dir / f"{AOF_BASENAME}.2.base.aof").write_bytes(b"not-a-valid-resp-base")
    write_manifest(
        aof_dir,
        [
            f"file {AOF_BASENAME}.2.base.aof seq 2 type b",
            f"file {AOF_BASENAME}.2.incr.aof seq 2 type i",
        ],
    )
    result = expect_startup_failure(tmp, extra=["--appenddirname", AOF_DIRNAME])
    result["base_path"] = f"{AOF_BASENAME}.2.base.aof"
    return result


def scenario_multipart_rewrite_success_deletes_history(tmp: Path) -> dict[str, Any]:
    server = start_server(
        tmp,
        appendonly=True,
        extra=[
            "--appenddirname",
            AOF_DIRNAME,
            "--aof-use-rdb-preamble",
            "no",
        ],
    )
    aof_dir = tmp / AOF_DIRNAME
    manifest = aof_dir / AOF_MANIFEST
    try:
        client = RespClient(server.port)
        try:
            assert client.command("SET", "history:before", "1") == "OK"
            before_lines = manifest_lines(manifest)
            before_names = manifest_names_by_type(before_lines)
            reply = client.command("BGREWRITEAOF")
            wait_rewrite_done(client)
            assert client.command("SET", "history:after", "1") == "OK"
            after_lines = manifest_lines(manifest)
            after_names = manifest_names_by_type(after_lines)
            old_exists = {
                name: (aof_dir / name).exists()
                for name in before_names
                if name not in after_names
            }
            new_exists = {name: (aof_dir / name).exists() for name in after_names}
            info = info_section(client, "persistence")
        finally:
            client.close()
    finally:
        stop_server(server)

    restart = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(restart.port)
        try:
            values = {
                "history:before": bulk(client.command("GET", "history:before")),
                "history:after": bulk(client.command("GET", "history:after")),
            }
            passed = (
                values == {"history:before": "1", "history:after": "1"}
                and bool(before_names)
                and len(after_names) == 2
                and not any(old_exists.values())
                and all(new_exists.values())
                and not manifest_names_by_type(after_lines, "h")
                and info.get("aof_last_bgrewrite_status") == "ok"
            )
            return {
                "passed": passed,
                "reply": reply,
                "values": values,
                "before_lines": before_lines,
                "after_lines": after_lines,
                "old_exists": old_exists,
                "new_exists": new_exists,
                "info": {
                    "aof_last_bgrewrite_status": info.get("aof_last_bgrewrite_status"),
                    "aof_rewrite_in_progress": info.get("aof_rewrite_in_progress"),
                },
            }
        finally:
            client.close()
    finally:
        stop_server(restart)


def scenario_multipart_rewrite_failure_preserves_history_files(tmp: Path) -> dict[str, Any]:
    server = start_server(
        tmp,
        appendonly=True,
        extra=[
            "--appenddirname",
            AOF_DIRNAME,
            "--aof-use-rdb-preamble",
            "no",
        ],
    )
    aof_dir = tmp / AOF_DIRNAME
    manifest = aof_dir / AOF_MANIFEST
    try:
        client = RespClient(server.port)
        try:
            assert client.command("SET", "preserve:before", "1") == "OK"
            before_lines = manifest_lines(manifest)
            before_names = manifest_names_by_type(before_lines)
            conflict = aof_dir / f"temp-rewriteaof-bg-{server.proc.pid}.aof"
            conflict.mkdir(parents=True, exist_ok=False)
            reply = client.command("BGREWRITEAOF")
            wait_rewrite_done(client)
            assert client.command("SET", "preserve:after", "1") == "OK"
            after_lines = manifest_lines(manifest)
            after_names = manifest_names_by_type(after_lines)
            referenced_exists = {name: (aof_dir / name).exists() for name in after_names}
            preserved_old = {
                name: (aof_dir / name).exists()
                for name in before_names
            }
            info = info_section(client, "persistence")
        finally:
            client.close()
    finally:
        stop_server(server)

    restart = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(restart.port)
        try:
            values = {
                "preserve:before": bulk(client.command("GET", "preserve:before")),
                "preserve:after": bulk(client.command("GET", "preserve:after")),
            }
            passed = (
                values == {"preserve:before": "1", "preserve:after": "1"}
                and bool(before_names)
                and all(preserved_old.values())
                and all(referenced_exists.values())
                and len(manifest_names_by_type(after_lines, "i")) >= 2
                and not manifest_names_by_type(after_lines, "h")
                and info.get("aof_last_bgrewrite_status") == "err"
            )
            return {
                "passed": passed,
                "reply": reply,
                "values": values,
                "before_lines": before_lines,
                "after_lines": after_lines,
                "preserved_old": preserved_old,
                "referenced_exists": referenced_exists,
                "info": {
                    "aof_last_bgrewrite_status": info.get("aof_last_bgrewrite_status"),
                    "aof_rewrite_in_progress": info.get("aof_rewrite_in_progress"),
                },
                "conflict_path": str(conflict.relative_to(tmp)),
            }
        finally:
            client.close()
    finally:
        stop_server(restart)


def scenario_multipart_startup_cleanup_removes_safe_orphans(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    base = f"{AOF_BASENAME}.1.base.aof"
    incr = f"{AOF_BASENAME}.1.incr.aof"
    write_aof(aof_dir / base, [("SET", "cleanup:base", "1")])
    write_aof(aof_dir / incr, [("SET", "cleanup:incr", "1")])
    write_manifest(
        aof_dir,
        [
            f"file {base} seq 1 type b",
            f"file {incr} seq 1 type i",
        ],
    )
    stale_temp = aof_dir / "temp-rewriteaof-bg-999999.aof"
    stale_rdb_temp = aof_dir / "temp-rewriteaof-bg-999999.rdb"
    stale_manifest = aof_dir / f"{AOF_MANIFEST}.tmp"
    orphan_base = aof_dir / f"{AOF_BASENAME}.9.base.aof"
    unknown = aof_dir / "manual-backup.aof"
    stale_temp.write_bytes(b"stale")
    stale_rdb_temp.write_bytes(b"stale")
    stale_manifest.write_text("stale", encoding="utf-8")
    write_aof(orphan_base, [("SET", "cleanup:orphan", "1")])
    unknown.write_bytes(b"manual")

    server = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(server.port)
        try:
            values = {
                "cleanup:base": bulk(client.command("GET", "cleanup:base")),
                "cleanup:incr": bulk(client.command("GET", "cleanup:incr")),
                "cleanup:orphan": bulk(client.command("GET", "cleanup:orphan")),
            }
        finally:
            client.close()
    finally:
        log = stop_server(server)

    exists = {
        "base": (aof_dir / base).exists(),
        "incr": (aof_dir / incr).exists(),
        "stale_temp": stale_temp.exists(),
        "stale_rdb_temp": stale_rdb_temp.exists(),
        "stale_manifest": stale_manifest.exists(),
        "orphan_base": orphan_base.exists(),
        "unknown": unknown.exists(),
    }
    passed = (
        values == {
            "cleanup:base": "1",
            "cleanup:incr": "1",
            "cleanup:orphan": None,
        }
        and exists["base"]
        and exists["incr"]
        and not exists["stale_temp"]
        and not exists["stale_rdb_temp"]
        and not exists["stale_manifest"]
        and not exists["orphan_base"]
        and exists["unknown"]
    )
    return {"passed": passed, "values": values, "exists": exists, "server_log_tail": log[-2000:]}


def scenario_multipart_startup_cleanup_preserves_manifest_references(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    base = f"{AOF_BASENAME}.1.base.aof"
    history = f"{AOF_BASENAME}.1.incr.aof"
    incr = f"{AOF_BASENAME}.2.incr.aof"
    write_aof(aof_dir / base, [("SET", "preserve-ref:base", "1")])
    write_aof(aof_dir / history, [("SET", "preserve-ref:history", "1")])
    write_aof(aof_dir / incr, [("SET", "preserve-ref:incr", "1")])
    write_manifest(
        aof_dir,
        [
            f"file {base} seq 1 type b",
            f"file {history} seq 1 type h",
            f"file {incr} seq 2 type i",
        ],
    )

    server = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(server.port)
        try:
            values = {
                "preserve-ref:base": bulk(client.command("GET", "preserve-ref:base")),
                "preserve-ref:history": bulk(client.command("GET", "preserve-ref:history")),
                "preserve-ref:incr": bulk(client.command("GET", "preserve-ref:incr")),
            }
        finally:
            client.close()
    finally:
        log = stop_server(server)

    exists = {name: (aof_dir / name).exists() for name in [base, history, incr]}
    passed = (
        values == {
            "preserve-ref:base": "1",
            "preserve-ref:history": None,
            "preserve-ref:incr": "1",
        }
        and all(exists.values())
    )
    return {"passed": passed, "values": values, "exists": exists, "server_log_tail": log[-2000:]}


def scenario_multipart_failed_rewrite_preliminary_chain_survives_cleanup(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    manifest = setup_preliminary_rewrite_layout(aof_dir)
    stale_temp = aof_dir / "temp-rewriteaof-bg-777777.aof"
    stale_temp.write_bytes(b"stale")

    server = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(server.port)
        try:
            values = {
                "crash:base": bulk(client.command("GET", "crash:base")),
                "crash:old-incr": bulk(client.command("GET", "crash:old-incr")),
                "crash:new-incr": bulk(client.command("GET", "crash:new-incr")),
            }
        finally:
            client.close()
    finally:
        log = stop_server(server)

    names = manifest_names_by_type(manifest_lines(manifest))
    exists = {name: (aof_dir / name).exists() for name in names}
    passed = (
        values == {"crash:base": "1", "crash:old-incr": "1", "crash:new-incr": "1"}
        and all(exists.values())
        and not stale_temp.exists()
    )
    return {
        "passed": passed,
        "values": values,
        "referenced_exists": exists,
        "stale_temp_exists": stale_temp.exists(),
        "server_log_tail": log[-2000:],
    }


def scenario_multipart_rewrite_success_cleanup_idempotent_restart(tmp: Path) -> dict[str, Any]:
    server = start_server(
        tmp,
        appendonly=True,
        extra=["--appenddirname", AOF_DIRNAME, "--aof-use-rdb-preamble", "no"],
    )
    aof_dir = tmp / AOF_DIRNAME
    manifest = aof_dir / AOF_MANIFEST
    try:
        client = RespClient(server.port)
        try:
            assert client.command("SET", "idempotent:before", "1") == "OK"
            before_names = manifest_names_by_type(manifest_lines(manifest))
            assert client.command("BGREWRITEAOF") == "Background append only file rewriting started"
            wait_rewrite_done(client)
            assert client.command("SET", "idempotent:after", "1") == "OK"
            first_lines = manifest_lines(manifest)
        finally:
            client.close()
    finally:
        stop_server(server)

    restart = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(restart.port)
        try:
            values = {
                "idempotent:before": bulk(client.command("GET", "idempotent:before")),
                "idempotent:after": bulk(client.command("GET", "idempotent:after")),
            }
            second_lines = manifest_lines(manifest)
        finally:
            client.close()
    finally:
        log = stop_server(restart)

    after_names = manifest_names_by_type(second_lines)
    old_exists = {name: (aof_dir / name).exists() for name in before_names if name not in after_names}
    live_exists = {name: (aof_dir / name).exists() for name in after_names}
    passed = (
        values == {"idempotent:before": "1", "idempotent:after": "1"}
        and first_lines == second_lines
        and len(after_names) == 2
        and not manifest_names_by_type(second_lines, "h")
        and not any(old_exists.values())
        and all(live_exists.values())
    )
    return {
        "passed": passed,
        "values": values,
        "first_lines": first_lines,
        "second_lines": second_lines,
        "old_exists": old_exists,
        "live_exists": live_exists,
        "server_log_tail": log[-2000:],
    }


def scenario_check_aof_valid_multipart_layout(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    base = f"{AOF_BASENAME}.1.base.aof"
    incr = f"{AOF_BASENAME}.1.incr.aof"
    write_aof(aof_dir / base, [("SET", "check:base", "1")])
    write_aof(aof_dir / incr, [("SET", "check:incr", "1")])
    manifest = write_manifest(
        aof_dir,
        [
            f"file {base} seq 1 type b",
            f"file {incr} seq 1 type i",
        ],
    )
    result = run_check_aof(tmp, manifest)
    result["passed"] = result["returncode"] == 0 and "All AOF files and manifest are valid" in result["output"]
    return result


def scenario_check_aof_missing_manifest_target_fails(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    base = f"{AOF_BASENAME}.1.base.aof"
    missing = f"{AOF_BASENAME}.1.incr.aof"
    write_aof(aof_dir / base, [("SET", "check:base", "1")])
    manifest = write_manifest(
        aof_dir,
        [
            f"file {base} seq 1 type b",
            f"file {missing} seq 1 type i",
        ],
    )
    result = run_check_aof(tmp, manifest)
    result["passed"] = result["returncode"] != 0 and "Cannot open file" in result["output"]
    return result


def scenario_check_aof_corrupt_incr_fails(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    base = f"{AOF_BASENAME}.1.base.aof"
    incr = f"{AOF_BASENAME}.1.incr.aof"
    write_aof(aof_dir / base, [("SET", "check:base", "1")])
    (aof_dir / incr).write_bytes(b"not-resp")
    manifest = write_manifest(
        aof_dir,
        [
            f"file {base} seq 1 type b",
            f"file {incr} seq 1 type i",
        ],
    )
    result = run_check_aof(tmp, manifest)
    result["passed"] = result["returncode"] != 0 and "not valid" in result["output"]
    return result


def scenario_check_aof_unfinished_multi_base_fails_not_last(tmp: Path) -> dict[str, Any]:
    aof_dir = tmp / AOF_DIRNAME
    base = f"{AOF_BASENAME}.1.base.aof"
    incr = f"{AOF_BASENAME}.1.incr.aof"
    write_aof(
        aof_dir / base,
        [
            ("SET", "check:base", "1"),
            ("MULTI",),
            ("SET", "check:inside-multi", "1"),
        ],
    )
    write_aof(aof_dir / incr, [("SET", "check:incr", "1")])
    manifest = write_manifest(
        aof_dir,
        [
            f"file {base} seq 1 type b",
            f"file {incr} seq 1 type i",
        ],
    )
    normal = run_check_aof(tmp, manifest)
    fix = run_check_aof(tmp, manifest, extra=["--fix"])
    passed = (
        normal["returncode"] != 0
        and "Reached EOF before reading EXEC for MULTI" in normal["output"]
        and "not valid" in normal["output"]
        and fix["returncode"] != 0
        and "Failed to truncate AOF" in fix["output"]
        and "because it is not the last file" in fix["output"]
    )
    return {
        "passed": passed,
        "normal": normal,
        "fix": fix,
    }


def scenario_check_aof_rdb_preamble_base_valid(tmp: Path) -> dict[str, Any]:
    server = start_server(
        tmp,
        appendonly=True,
        extra=["--appenddirname", AOF_DIRNAME, "--aof-use-rdb-preamble", "yes"],
    )
    aof_dir = tmp / AOF_DIRNAME
    manifest = aof_dir / AOF_MANIFEST
    try:
        client = RespClient(server.port)
        try:
            assert client.command("SET", "check:rdb", "1") == "OK"
            assert client.command("BGREWRITEAOF") == "Background append only file rewriting started"
            wait_rewrite_done(client)
        finally:
            client.close()
    finally:
        stop_server(server)

    result = run_check_aof(tmp, manifest)
    lines = manifest_lines(manifest)
    result["manifest_lines"] = lines
    result["passed"] = (
        result["returncode"] == 0
        and any(".base.rdb" in line for line in lines)
        and "Start to check BASE AOF (RDB format)." in result["output"]
        and "All AOF files and manifest are valid" in result["output"]
    )
    return result


def scenario_aof_debug_flush_sleep_delays_always_append(tmp: Path) -> dict[str, Any]:
    server = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    client = RespClient(server.port)
    try:
        debug_ok = client.command("DEBUG", "AOF-FLUSH-SLEEP", "75000")
        start = time.monotonic()
        set_ok = client.command("SET", "debug:aof-flush-sleep", "1")
        elapsed_ms = (time.monotonic() - start) * 1000.0
        reset_ok = client.command("DEBUG", "AOF-FLUSH-SLEEP", "0")
    finally:
        client.close()
        log = stop_server(server)

    aof = current_incr_from_manifest(tmp / AOF_DIRNAME)
    aof_size = aof.stat().st_size if aof and aof.exists() else 0
    passed = (
        debug_ok == "OK"
        and set_ok == "OK"
        and reset_ok == "OK"
        and elapsed_ms >= 50.0
        and aof_size > 0
    )
    return {
        "passed": passed,
        "debug_ok": debug_ok,
        "set_ok": set_ok,
        "reset_ok": reset_ok,
        "elapsed_ms": elapsed_ms,
        "aof_size": aof_size,
        "server_log_tail": log[-2000:],
    }


def scenario_aof_timestamp_annotations_generated(tmp: Path) -> dict[str, Any]:
    server = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(server.port)
        try:
            timestamp_ok = client.command("CONFIG", "SET", "aof-timestamp-enabled", "yes")
            preamble_ok = client.command("CONFIG", "SET", "aof-use-rdb-preamble", "no")
            set_ok = client.command("SET", "timestamp:frontier", "1")
            incr = current_incr_from_manifest(tmp / AOF_DIRNAME)
            incr_first = (
                incr.read_bytes().splitlines()[0].decode("utf-8", "replace")
                if incr and incr.exists() and incr.stat().st_size > 0
                else ""
            )
            rewrite_reply = client.command("BGREWRITEAOF")
            wait_rewrite_done(client)
            base = current_base_from_manifest(tmp / AOF_DIRNAME)
            base_first = (
                base.read_bytes().splitlines()[0].decode("utf-8", "replace")
                if base and base.exists() and base.stat().st_size > 0
                else ""
            )
        finally:
            client.close()
    finally:
        log = stop_server(server)

    passed = (
        timestamp_ok == "OK"
        and preamble_ok == "OK"
        and set_ok == "OK"
        and isinstance(rewrite_reply, str)
        and rewrite_reply.startswith("Background append only file rewriting")
        and incr_first.startswith("#TS:")
        and base_first.startswith("#TS:")
    )
    return {
        "passed": passed,
        "timestamp_ok": timestamp_ok,
        "preamble_ok": preamble_ok,
        "set_ok": set_ok,
        "rewrite_reply": rewrite_reply,
        "incr_first": incr_first,
        "base_first": base_first,
        "manifest_lines": manifest_lines(tmp / AOF_DIRNAME / AOF_MANIFEST),
        "server_log_tail": log[-2000:],
    }


def scenario_check_aof_truncate_to_timestamp(tmp: Path) -> dict[str, Any]:
    def timestamped_aof_bytes() -> bytes:
        return (
            b"#TS:1628217470\r\n"
            + encode_command("SET", "foo1", "bar1")
            + b"#TS:1628217471\r\n"
            + encode_command("SET", "foo2", "bar2")
            + b"#TS:1628217472\r\n"
            + b"#TS:1628217473\r\n"
            + encode_command("SET", "foo3", "bar3")
            + b"#TS:1628217474\r\n"
        )

    ok_dir = tmp / "timestamp-ok"
    ok_aof_dir = ok_dir / AOF_DIRNAME
    ok_base = f"{AOF_BASENAME}.1.base.aof"
    (ok_aof_dir / ok_base).parent.mkdir(parents=True, exist_ok=True)
    (ok_aof_dir / ok_base).write_bytes(timestamped_aof_bytes())
    ok_manifest = write_manifest(ok_aof_dir, [f"file {ok_base} seq 1 type b"])
    truncate = run_check_aof(ok_dir, ok_manifest, extra=["--truncate-to-timestamp", "1628217471"])

    server = start_server(ok_dir, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(server.port)
        try:
            values = {
                "foo1": bulk(client.command("GET", "foo1")),
                "foo2": bulk(client.command("GET", "foo2")),
                "foo3": bulk(client.command("GET", "foo3")),
            }
        finally:
            client.close()
    finally:
        restart_log = stop_server(server)

    abort_dir = tmp / "timestamp-abort"
    abort_aof_dir = abort_dir / AOF_DIRNAME
    (abort_aof_dir / ok_base).parent.mkdir(parents=True, exist_ok=True)
    (abort_aof_dir / ok_base).write_bytes(timestamped_aof_bytes())
    abort_manifest = write_manifest(abort_aof_dir, [f"file {ok_base} seq 1 type b"])
    abort = run_check_aof(abort_dir, abort_manifest, extra=["--truncate-to-timestamp", "1628217469"])

    nonlast_dir = tmp / "timestamp-nonlast"
    nonlast_aof_dir = nonlast_dir / AOF_DIRNAME
    incr = f"{AOF_BASENAME}.1.incr.aof"
    (nonlast_aof_dir / ok_base).parent.mkdir(parents=True, exist_ok=True)
    (nonlast_aof_dir / ok_base).write_bytes(timestamped_aof_bytes())
    write_aof(nonlast_aof_dir / incr, [("SET", "tail", "1")])
    nonlast_manifest = write_manifest(
        nonlast_aof_dir,
        [f"file {ok_base} seq 1 type b", f"file {incr} seq 1 type i"],
    )
    nonlast = run_check_aof(
        nonlast_dir,
        nonlast_manifest,
        extra=["--truncate-to-timestamp", "1628217473"],
    )

    passed = (
        truncate["returncode"] == 0
        and values == {"foo1": "bar1", "foo2": "bar2", "foo3": None}
        and abort["returncode"] != 0
        and "aborting" in abort["output"]
        and nonlast["returncode"] != 0
        and "Failed to truncate AOF" in nonlast["output"]
        and "to timestamp" in nonlast["output"]
        and "because it is not the last file" in nonlast["output"]
    )
    return {
        "passed": passed,
        "truncate": truncate,
        "values_after_truncate": values,
        "abort": abort,
        "nonlast": nonlast,
        "restart_log_tail": restart_log[-2000:],
    }


def run_faulted_manifest_rewrite(
    tmp: Path,
    *,
    fault: str,
    key_prefix: str,
    expected_manifest: str,
) -> dict[str, Any]:
    server = start_server(
        tmp,
        appendonly=True,
        extra=[
            "--appenddirname",
            AOF_DIRNAME,
            "--aof-use-rdb-preamble",
            "no",
        ],
        env={AOF_FAULT_ENV: fault},
    )
    aof_dir = tmp / AOF_DIRNAME
    manifest = aof_dir / AOF_MANIFEST
    server_log = ""
    try:
        client = RespClient(server.port)
        try:
            assert client.command("SET", f"{key_prefix}:before", "1") == "OK"
            before_lines = manifest_lines(manifest)
            reply = client.command("BGREWRITEAOF")
            wait_rewrite_done(client)
            assert client.command("SET", f"{key_prefix}:after", "1") == "OK"
            info = info_section(client, "persistence")
            after_lines = manifest_lines(manifest)
            after_names = manifest_names_by_type(after_lines)
            history_names = manifest_names_by_type(after_lines, "h")
            incr_names = manifest_names_by_type(after_lines, "i")
            base_names = manifest_names_by_type(after_lines, "b")
            referenced_exists = {name: (aof_dir / name).exists() for name in after_names}
            temp_base_exists = any(path.name.startswith("temp-rewriteaof-bg-") for path in aof_dir.iterdir())
            final_base_exists = (aof_dir / f"{AOF_BASENAME}.2.base.aof").exists()
        finally:
            client.close()
    finally:
        server_log = stop_server(server)

    restart = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(restart.port)
        try:
            values = {
                "before": bulk(client.command("GET", f"{key_prefix}:before")),
                "after": bulk(client.command("GET", f"{key_prefix}:after")),
            }
        finally:
            client.close()
    finally:
        stop_server(restart)

    manifest_ok = False
    if expected_manifest == "preliminary":
        manifest_ok = not history_names and len(incr_names) >= 2
    elif expected_manifest == "published-history":
        manifest_ok = bool(history_names) and len(incr_names) == 1 and len(base_names) == 1

    passed = (
        values == {"before": "1", "after": "1"}
        and info.get("aof_rewrite_in_progress") == "0"
        and info.get("aof_last_bgrewrite_status") == "err"
        and manifest_ok
        and all(referenced_exists.values())
    )
    return {
        "passed": passed,
        "fault": fault,
        "reply": reply,
        "values": values,
        "before_lines": before_lines,
        "after_lines": after_lines,
        "referenced_exists": referenced_exists,
        "temp_base_exists": temp_base_exists,
        "final_base_exists": final_base_exists,
        "server_log_tail": server_log[-2000:],
        "info": {
            "aof_rewrite_in_progress": info.get("aof_rewrite_in_progress"),
            "aof_last_bgrewrite_status": info.get("aof_last_bgrewrite_status"),
        },
    }


def scenario_multipart_rewrite_fault_preliminary_manifest_before_rename(tmp: Path) -> dict[str, Any]:
    server = start_server(
        tmp,
        appendonly=True,
        extra=[
            "--appenddirname",
            AOF_DIRNAME,
            "--aof-use-rdb-preamble",
            "no",
        ],
        env={AOF_FAULT_ENV: "manifest-preliminary-before-rename"},
    )
    aof_dir = tmp / AOF_DIRNAME
    manifest = aof_dir / AOF_MANIFEST
    server_log = ""
    try:
        client = RespClient(server.port)
        try:
            assert client.command("SET", "fault:prelim:before", "1") == "OK"
            before_lines = manifest_lines(manifest)
            try:
                client.command("BGREWRITEAOF")
                reply_error = None
            except RespError as err:
                reply_error = str(err)
            assert client.command("SET", "fault:prelim:after", "1") == "OK"
            info = info_section(client, "persistence")
            after_lines = manifest_lines(manifest)
            temp_manifest_exists = (aof_dir / f"{AOF_BASENAME}.manifest.tmp").exists()
            incr_names = manifest_names_by_type(after_lines, "i")
        finally:
            client.close()
    finally:
        server_log = stop_server(server)

    restart = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(restart.port)
        try:
            values = {
                "before": bulk(client.command("GET", "fault:prelim:before")),
                "after": bulk(client.command("GET", "fault:prelim:after")),
            }
        finally:
            client.close()
    finally:
        stop_server(restart)

    passed = (
        reply_error is not None
        and "BGREWRITEAOF failed" in reply_error
        and values == {"before": "1", "after": "1"}
        and info.get("aof_rewrite_in_progress") == "0"
        and info.get("aof_last_bgrewrite_status") == "err"
        and not temp_manifest_exists
        and len(incr_names) == len(manifest_names_by_type(before_lines, "i"))
    )
    return {
        "passed": passed,
        "fault": "manifest-preliminary-before-rename",
        "reply_error": reply_error,
        "values": values,
        "before_lines": before_lines,
        "after_lines": after_lines,
        "temp_manifest_exists": temp_manifest_exists,
        "server_log_tail": server_log[-2000:],
        "info": {
            "aof_rewrite_in_progress": info.get("aof_rewrite_in_progress"),
            "aof_last_bgrewrite_status": info.get("aof_last_bgrewrite_status"),
        },
    }


def scenario_multipart_rewrite_fault_base_before_rename(tmp: Path) -> dict[str, Any]:
    return run_faulted_manifest_rewrite(
        tmp,
        fault="base-before-rename",
        key_prefix="fault:base-before-rename",
        expected_manifest="preliminary",
    )


def scenario_multipart_rewrite_fault_base_after_rename_before_dir_sync(tmp: Path) -> dict[str, Any]:
    return run_faulted_manifest_rewrite(
        tmp,
        fault="base-after-rename-before-dir-sync",
        key_prefix="fault:base-after-rename",
        expected_manifest="preliminary",
    )


def scenario_multipart_rewrite_fault_manifest_final_before_sync(tmp: Path) -> dict[str, Any]:
    return run_faulted_manifest_rewrite(
        tmp,
        fault="manifest-final-before-sync",
        key_prefix="fault:manifest-before-sync",
        expected_manifest="preliminary",
    )


def scenario_multipart_rewrite_fault_manifest_final_before_rename(tmp: Path) -> dict[str, Any]:
    return run_faulted_manifest_rewrite(
        tmp,
        fault="manifest-final-before-rename",
        key_prefix="fault:manifest-before-rename",
        expected_manifest="preliminary",
    )


def scenario_multipart_rewrite_fault_manifest_final_after_rename_before_dir_sync(tmp: Path) -> dict[str, Any]:
    return run_faulted_manifest_rewrite(
        tmp,
        fault="manifest-final-after-rename-before-dir-sync",
        key_prefix="fault:manifest-after-rename",
        expected_manifest="published-history",
    )


def scenario_multipart_rewrite_window_survives_restart(tmp: Path) -> dict[str, Any]:
    server = start_server(
        tmp,
        appendonly=True,
        extra=["--appenddirname", AOF_DIRNAME],
    )
    written: list[str] = []
    writer_errors: list[str] = []

    def write_later() -> None:
        try:
            client = RespClient(server.port)
            try:
                for idx in range(24):
                    key = f"rewrite:during:{idx}"
                    assert client.command("SET", key, str(idx)) == "OK"
                    written.append(key)
                    time.sleep(0.005)
            finally:
                client.close()
        except Exception as exc:  # noqa: BLE001 - captured into scenario detail
            writer_errors.append(repr(exc))

    try:
        client = RespClient(server.port)
        try:
            for idx in range(200):
                assert client.command("SET", f"rewrite:base:{idx}", str(idx)) == "OK"
            assert client.command("SET", "rewrite:before", "1") == "OK"
            written.append("rewrite:before")

            thread = threading.Thread(target=write_later)
            thread.start()
            reply = client.command("BGREWRITEAOF")
            thread.join(timeout=5)
            if thread.is_alive():
                writer_errors.append("writer thread timed out")
            wait_rewrite_done(client)

            assert client.command("SET", "rewrite:after", "1") == "OK"
            written.append("rewrite:after")
            info = info_section(client, "persistence")
            lines = manifest_lines(tmp / AOF_DIRNAME / AOF_MANIFEST)
        finally:
            client.close()
    finally:
        stop_server(server)

    restart = start_server(tmp, appendonly=True, extra=["--appenddirname", AOF_DIRNAME])
    try:
        client = RespClient(restart.port)
        try:
            missing = [
                key
                for key in written
                if bulk(client.command("GET", key)) is None
            ]
            base_ok = bulk(client.command("GET", "rewrite:base:0")) == "0"
            passed = (
                not writer_errors
                and not missing
                and base_ok
                and info.get("aof_rewrite_in_progress") == "0"
                and info.get("aof_last_bgrewrite_status") == "ok"
                and any(" type b" in line for line in lines)
                and any(" type i" in line for line in lines)
            )
            return {
                "passed": passed,
                "reply": reply,
                "written_count": len(written),
                "missing": missing[:20],
                "writer_errors": writer_errors,
                "base_ok": base_ok,
                "info": {
                    "aof_rewrite_in_progress": info.get("aof_rewrite_in_progress"),
                    "aof_last_bgrewrite_status": info.get("aof_last_bgrewrite_status"),
                    "aof_current_size": info.get("aof_current_size"),
                    "aof_base_size": info.get("aof_base_size"),
                },
                "lines": lines,
            }
        finally:
            client.close()
    finally:
        stop_server(restart)


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
    Scenario("multipart-aof-load-truncated-append-survives-restart", "persistence-aof", scenario_multipart_aof_load_truncated_append_survives_restart),
    Scenario("multipart-aof-expired-key-not-resurrected-by-later-push", "persistence-aof", scenario_multipart_aof_expired_key_not_resurrected_by_later_push),
    Scenario("aof-load-truncated-no-fails", "persistence-aof", scenario_aof_load_truncated_no_fails),
    Scenario("aof-unfinished-multi-load-truncated-no-fails", "persistence-aof", scenario_aof_unfinished_multi_load_truncated_no_fails),
    Scenario("aof-bad-format-logs-upstream-error", "persistence-aof", scenario_aof_bad_format_logs_upstream_error),
    Scenario("aof-unknown-command-fails-startup", "persistence-aof", scenario_aof_unknown_command_fails),
    Scenario("getex-does-not-append-to-aof", "persistence-aof-propagation", scenario_aof_getex_no_append),
    Scenario("aof-spop-count-replay", "persistence-aof-propagation", scenario_aof_spop_count_replay),
    Scenario("aof-lmpop-zmpop-replay", "persistence-aof-propagation", scenario_aof_lmpop_zmpop_replay),
    Scenario("aof-blocked-lmpop-zmpop-wake-persists", "persistence-aof-propagation", scenario_aof_blocked_lmpop_zmpop_wake_persists),
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
    Scenario("multipart-aof-rewrite-preliminary-manifest-survives-restart", "persistence-aof-rewrite", scenario_multipart_rewrite_preliminary_manifest_survives_restart),
    Scenario("multipart-aof-rewrite-temp-base-ignored-before-final-manifest", "persistence-aof-rewrite", scenario_multipart_rewrite_temp_base_ignored_before_final_manifest),
    Scenario("multipart-aof-rewrite-final-base-ignored-before-manifest", "persistence-aof-rewrite", scenario_multipart_rewrite_final_base_ignored_before_manifest),
    Scenario("multipart-aof-rewrite-failed-replayable-and-status-err", "persistence-aof-rewrite", scenario_multipart_rewrite_failed_replayable_and_status_err),
    Scenario("multipart-aof-rewrite-corrupt-final-base-fails-closed", "persistence-aof-rewrite", scenario_multipart_rewrite_corrupt_final_base_fails_closed),
    Scenario("multipart-aof-rewrite-success-deletes-history", "persistence-aof-rewrite", scenario_multipart_rewrite_success_deletes_history),
    Scenario("multipart-aof-rewrite-failure-preserves-history-files", "persistence-aof-rewrite", scenario_multipart_rewrite_failure_preserves_history_files),
    Scenario("multipart-aof-startup-cleanup-removes-safe-orphans", "persistence-aof-cleanup", scenario_multipart_startup_cleanup_removes_safe_orphans),
    Scenario("multipart-aof-startup-cleanup-preserves-manifest-references", "persistence-aof-cleanup", scenario_multipart_startup_cleanup_preserves_manifest_references),
    Scenario("multipart-aof-failed-rewrite-preliminary-chain-survives-cleanup", "persistence-aof-cleanup", scenario_multipart_failed_rewrite_preliminary_chain_survives_cleanup),
    Scenario("multipart-aof-rewrite-success-cleanup-idempotent-restart", "persistence-aof-cleanup", scenario_multipart_rewrite_success_cleanup_idempotent_restart),
    Scenario("check-aof-valid-multipart-layout", "persistence-aof-check", scenario_check_aof_valid_multipart_layout),
    Scenario("check-aof-missing-manifest-target-fails", "persistence-aof-check", scenario_check_aof_missing_manifest_target_fails),
    Scenario("check-aof-corrupt-incr-fails", "persistence-aof-check", scenario_check_aof_corrupt_incr_fails),
    Scenario("check-aof-unfinished-multi-base-fails-not-last", "persistence-aof-check", scenario_check_aof_unfinished_multi_base_fails_not_last),
    Scenario("check-aof-rdb-preamble-base-valid", "persistence-aof-check", scenario_check_aof_rdb_preamble_base_valid),
    Scenario("aof-debug-flush-sleep-delays-always-append", "persistence-aof-debug", scenario_aof_debug_flush_sleep_delays_always_append),
    Scenario("aof-timestamp-annotations-generated", "persistence-aof-timestamp", scenario_aof_timestamp_annotations_generated),
    Scenario("check-aof-truncate-to-timestamp", "persistence-aof-check", scenario_check_aof_truncate_to_timestamp),
    Scenario("multipart-aof-rewrite-fault-preliminary-manifest-before-rename", "persistence-aof-rewrite-fault", scenario_multipart_rewrite_fault_preliminary_manifest_before_rename),
    Scenario("multipart-aof-rewrite-fault-base-before-rename", "persistence-aof-rewrite-fault", scenario_multipart_rewrite_fault_base_before_rename),
    Scenario("multipart-aof-rewrite-fault-base-after-rename-before-dir-sync", "persistence-aof-rewrite-fault", scenario_multipart_rewrite_fault_base_after_rename_before_dir_sync),
    Scenario("multipart-aof-rewrite-fault-manifest-final-before-sync", "persistence-aof-rewrite-fault", scenario_multipart_rewrite_fault_manifest_final_before_sync),
    Scenario("multipart-aof-rewrite-fault-manifest-final-before-rename", "persistence-aof-rewrite-fault", scenario_multipart_rewrite_fault_manifest_final_before_rename),
    Scenario("multipart-aof-rewrite-fault-manifest-final-after-rename-before-dir-sync", "persistence-aof-rewrite-fault", scenario_multipart_rewrite_fault_manifest_final_after_rename_before_dir_sync),
    Scenario("multipart-aof-rewrite-window-survives-restart", "persistence-aof-rewrite", scenario_multipart_rewrite_window_survives_restart),
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
