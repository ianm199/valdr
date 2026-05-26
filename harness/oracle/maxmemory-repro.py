#!/usr/bin/env python3
"""Small maxmemory reproducer for the volatile-policy TCL loop.

This intentionally avoids the full Valkey TCL harness. It starts one Rust
server, runs the problematic `unit/maxmemory.tcl` small-key loop for one policy,
and prints machine-readable timings/counters so hypotheses can be tested fast.
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
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SERVER = ROOT / "target" / "debug" / "redis-server"


class RespError(RuntimeError):
    pass


class RespClient:
    def __init__(self, port: int) -> None:
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.file = self.sock.makefile("rb")

    def close(self) -> None:
        try:
            self.file.close()
        finally:
            self.sock.close()

    def set_timeout(self, timeout_s: float) -> None:
        self.sock.settimeout(timeout_s)

    def command(self, *args: object) -> Any:
        buf = [f"*{len(args)}\r\n".encode()]
        for arg in args:
            data = str(arg).encode()
            buf.append(f"${len(data)}\r\n".encode())
            buf.append(data)
            buf.append(b"\r\n")
        self.sock.sendall(b"".join(buf))
        return self._read()

    def _readline(self) -> bytes:
        line = self.file.readline()
        if not line:
            raise EOFError("server closed connection")
        if not line.endswith(b"\r\n"):
            raise EOFError(f"short RESP line: {line!r}")
        return line[:-2]

    def _read(self) -> Any:
        prefix = self.file.read(1)
        if not prefix:
            raise EOFError("server closed connection")
        if prefix == b"+":
            return self._readline().decode(errors="replace")
        if prefix == b"-":
            msg = self._readline().decode(errors="replace")
            raise RespError(msg)
        if prefix == b":":
            return int(self._readline())
        if prefix == b"$":
            n = int(self._readline())
            if n < 0:
                return None
            data = self.file.read(n)
            trailer = self.file.read(2)
            if len(data) != n or trailer != b"\r\n":
                raise EOFError("short RESP bulk")
            return data.decode(errors="replace")
        if prefix == b"*":
            n = int(self._readline())
            if n < 0:
                return None
            return [self._read() for _ in range(n)]
        if prefix == b">":
            n = int(self._readline())
            return {"push": [self._read() for _ in range(n)]}
        if prefix == b"%":
            n = int(self._readline())
            out: dict[Any, Any] = {}
            for _ in range(n):
                key = self._read()
                out[key] = self._read()
            return out
        if prefix == b"_":
            line = self._readline()
            if line:
                raise ValueError(f"invalid null line {line!r}")
            return None
        if prefix == b"#":
            value = self._readline()
            return value == b"t"
        raise ValueError(f"unsupported RESP prefix {prefix!r}")


class ReplicationStream(RespClient):
    def __init__(self, port: int) -> None:
        super().__init__(port)
        self.sock.sendall(b"SYNC\r\n")
        while True:
            line = self._readline()
            if line:
                break
        if not line.startswith(b"$"):
            raise RuntimeError(f"unexpected SYNC header: {line!r}")
        rdb_len = int(line[1:])
        while rdb_len:
            chunk = self.file.read(rdb_len)
            if not chunk:
                raise EOFError("short SYNC payload")
            rdb_len -= len(chunk)

    def read_commands(self, max_commands: int, timeout_s: float) -> list[Any]:
        commands: list[Any] = []
        self.set_timeout(timeout_s)
        while len(commands) < max_commands:
            try:
                value = self._read()
            except (TimeoutError, socket.timeout, OSError):
                break
            if isinstance(value, list) and value and isinstance(value[0], str):
                value[0] = value[0].lower()
            if value == ["ping"]:
                continue
            commands.append(value)
        return commands


def find_free_port(preferred: int) -> int:
    for port in range(preferred, preferred + 200):
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise RuntimeError("no free port found")


def wait_for_server(port: int, proc: subprocess.Popen[Any], timeout_s: float = 8.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early with status {proc.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"server did not accept connections on {port}")


def info_map(client: RespClient, section: str = "memory") -> dict[str, str]:
    text = client.command("INFO", section)
    out: dict[str, str] = {}
    for raw in text.splitlines():
        if ":" not in raw or raw.startswith("#"):
            continue
        key, value = raw.split(":", 1)
        out[key] = value
    return out


def used_memory(client: RespClient) -> int:
    return int(info_map(client)["used_memory"])


def dbsize(client: RespClient) -> int:
    return int(client.command("DBSIZE"))


def run_volatile_loop(args: argparse.Namespace, port: int) -> dict[str, Any]:
    client = RespClient(port)
    try:
        client.command("FLUSHALL")
        client.command("CONFIG", "SET", "maxmemory", 0)
        client.command("CONFIG", "SET", "maxmemory-policy", "noeviction")

        base_used = used_memory(client)
        limit = base_used + args.memory_delta_kb * 1024
        client.command("CONFIG", "SET", "maxmemory", limit)
        client.command("CONFIG", "SET", "maxmemory-policy", args.policy)

        fill_start = time.monotonic()
        fill_samples: list[dict[str, Any]] = []
        numkeys = 0
        while numkeys < args.max_fill_keys:
            if numkeys % 2:
                client.command("SETEX", f"key:{numkeys}", 10000, "x")
            else:
                client.command("SET", f"key:{numkeys}", "x")
            numkeys += 1
            if numkeys % args.progress_every == 0:
                current_used = used_memory(client)
                fill_samples.append(
                    {
                        "keys": numkeys,
                        "used_memory": current_used,
                        "dbsize": dbsize(client),
                        "elapsed_s": time.monotonic() - fill_start,
                    }
                )
                if current_used + 4096 > limit:
                    break
        fill_elapsed = time.monotonic() - fill_start
        fill_used = used_memory(client)
        fill_dbsize = dbsize(client)

        add_start = time.monotonic()
        add_samples: list[dict[str, Any]] = []
        errors = 0
        for j in range(numkeys):
            try:
                client.command("SETEX", f"foo:{j}", 10000, "x")
            except RespError:
                errors += 1
            if (j + 1) % args.progress_every == 0:
                add_samples.append(
                    {
                        "added": j + 1,
                        "used_memory": used_memory(client),
                        "dbsize": dbsize(client),
                        "errors": errors,
                        "elapsed_s": time.monotonic() - add_start,
                    }
                )
        add_elapsed = time.monotonic() - add_start
        final_used = used_memory(client)
        final_dbsize = dbsize(client)

        missing_persistent: list[str] = []
        for j in range(0, numkeys, 2):
            exists = int(client.command("EXISTS", f"key:{j}"))
            if exists != 1:
                missing_persistent.append(f"key:{j}")
                if len(missing_persistent) >= 20:
                    break

        stats = info_map(client, "stats")
        memory = info_map(client, "memory")
        return {
            "scenario": "volatile-loop",
            "policy": args.policy,
            "limit": limit,
            "base_used": base_used,
            "numkeys": numkeys,
            "fill_elapsed_s": fill_elapsed,
            "fill_used_memory": fill_used,
            "fill_dbsize": fill_dbsize,
            "add_elapsed_s": add_elapsed,
            "final_used_memory": final_used,
            "final_dbsize": final_dbsize,
            "errors": errors,
            "evicted_keys": int(stats.get("evicted_keys", "0")),
            "under_limit_with_slack": final_used < limit + 4096,
            "missing_persistent_count": len(missing_persistent),
            "missing_persistent_sample": missing_persistent,
            "mem_clients_normal": int(memory.get("mem_clients_normal", "0")),
            "mem_clients_slaves": int(memory.get("mem_clients_slaves", "0")),
            "fill_samples": fill_samples,
            "add_samples": add_samples,
        }
    finally:
        client.close()


def run_tracking_feedback(args: argparse.Namespace, port: int) -> dict[str, Any]:
    main = RespClient(port)
    clients: list[RespClient] = []
    try:
        main.command("FLUSHALL")
        main.command("CONFIG", "SET", "latency-tracking", "no")
        main.command("CONFIG", "SET", "maxmemory", 0)
        main.command("CONFIG", "SET", "maxmemory-policy", "allkeys-lru")
        main.command("CONFIG", "SET", "maxmemory-eviction-tenacity", 100)

        for _ in range(args.tracking_clients):
            rd = RespClient(port)
            rd.command("HELLO", 3)
            rd.command("CLIENT", "TRACKING", "on")
            clients.append(rd)

        populate_start = time.monotonic()
        for j in range(args.tracking_keys):
            key = f"{j}{'x' * args.tracking_key_suffix_len}"
            main.command("SET", key, "x")
            for rd in clients:
                rd.command("GET", key)
        populate_elapsed = time.monotonic() - populate_start

        time.sleep(args.tracking_trim_sleep_s)
        before_memory = info_map(main, "memory")
        before_stats = info_map(main, "stats")
        before_used = int(before_memory["used_memory"])
        before_dbsize = dbsize(main)
        limit = before_used - args.tracking_limit_drop_bytes

        config_start = time.monotonic()
        main.command("CONFIG", "SET", "maxmemory", limit)
        config_elapsed = time.monotonic() - config_start

        after_memory = info_map(main, "memory")
        after_stats = info_map(main, "stats")
        after_dbsize = dbsize(main)
        invalidations = []
        for rd in clients:
            rd.set_timeout(args.tracking_read_timeout_s)
            started = time.monotonic()
            try:
                payload = rd._read()
                invalidations.append(
                    {
                        "received": True,
                        "elapsed_s": time.monotonic() - started,
                        "payload": repr(payload)[:300],
                    }
                )
            except Exception as exc:  # Timeout is useful telemetry here.
                invalidations.append(
                    {
                        "received": False,
                        "elapsed_s": time.monotonic() - started,
                        "error": repr(exc),
                    }
                )

        return {
            "scenario": "tracking-feedback",
            "tracking_clients": args.tracking_clients,
            "tracking_keys": args.tracking_keys,
            "populate_elapsed_s": populate_elapsed,
            "config_elapsed_s": config_elapsed,
            "before_used_memory": before_used,
            "before_dbsize": before_dbsize,
            "before_evicted_keys": int(before_stats.get("evicted_keys", "0")),
            "limit": limit,
            "after_used_memory": int(after_memory["used_memory"]),
            "after_dbsize": after_dbsize,
            "after_evicted_keys": int(after_stats.get("evicted_keys", "0")),
            "evicted_delta": int(after_stats.get("evicted_keys", "0"))
            - int(before_stats.get("evicted_keys", "0")),
            "tracking_total_keys": int(after_stats.get("tracking_total_keys", "0")),
            "tracking_total_items": int(after_stats.get("tracking_total_items", "0")),
            "mem_clients_normal": int(after_memory.get("mem_clients_normal", "0")),
            "under_expected_dbsize_range": 200 <= after_dbsize <= 300,
            "under_expected_eviction_range": 10
            <= (
                int(after_stats.get("evicted_keys", "0"))
                - int(before_stats.get("evicted_keys", "0"))
            )
            <= 50,
            "invalidations_received": sum(1 for item in invalidations if item["received"]),
            "invalidations": invalidations,
        }
    finally:
        main.close()
        for rd in clients:
            rd.close()


def run_lfu_init(args: argparse.Namespace, port: int) -> dict[str, Any]:
    client = RespClient(port)
    try:
        client.command("FLUSHALL")
        client.command("CONFIG", "SET", "maxmemory", 0)
        client.command("CONFIG", "SET", "maxmemory-policy", "allkeys-lfu")
        client.command("DEL", "foo")
        client.command("SET", "foo", "a")
        freq = int(client.command("OBJECT", "FREQ", "foo"))
        return {
            "scenario": "lfu-init",
            "object_freq": freq,
            "expected_freq": 5,
            "matches_expected": freq == 5,
        }
    finally:
        client.close()


def run_import_mode(args: argparse.Namespace, port: int) -> dict[str, Any]:
    client = RespClient(port)
    try:
        client.command("FLUSHALL")
        client.command("CONFIG", "SET", "maxmemory", 0)
        client.command("CONFIG", "SET", "maxmemory-policy", "noeviction")
        client.command("SET", "key", "val")
        client.command("CONFIG", "SET", "import-mode", "yes")
        client.command("CLIENT", "IMPORT-SOURCE", "on")
        client.command("CONFIG", "SET", "maxmemory-policy", "allkeys-lru")
        client.command("CONFIG", "SET", "maxmemory", 1)
        dbsize_while_import = dbsize(client)

        set_error = None
        try:
            client.command("SET", "key1", "val1")
        except RespError as exc:
            set_error = str(exc)

        client.command("CLIENT", "IMPORT-SOURCE", "off")
        client.command("CONFIG", "SET", "import-mode", "no")
        dbsize_after_import = dbsize(client)
        return {
            "scenario": "import-mode",
            "dbsize_while_import": dbsize_while_import,
            "set_error": set_error,
            "dbsize_after_import": dbsize_after_import,
            "kept_existing_key": dbsize_while_import == 1,
            "set_rejected_with_oom": bool(
                set_error and set_error.startswith("OOM command not allowed")
            ),
            "evicted_after_import": dbsize_after_import == 0,
        }
    finally:
        client.close()


def run_propagation_eviction(args: argparse.Namespace, port: int) -> dict[str, Any]:
    client = RespClient(port)
    repl: ReplicationStream | None = None
    try:
        client.command("FLUSHALL")
        client.command("CONFIG", "SET", "maxmemory", 0)
        client.command("CONFIG", "SET", "maxmemory-policy", "noeviction")
        client.command("CONFIG", "SET", "repl-ping-replica-period", 3600)
        repl = ReplicationStream(port)

        client.command("SET", "asdf1", 1)
        client.command("SET", "asdf2", 2)
        client.command("SET", "asdf3", 3)
        client.command("CONFIG", "SET", "maxmemory-policy", "allkeys-lru")
        client.command("CONFIG", "SET", "maxmemory", 1)

        deadline = time.monotonic() + 5.0
        while dbsize(client) != 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        after_eviction_dbsize = dbsize(client)

        client.command("CONFIG", "SET", "maxmemory", 0)
        client.command("CONFIG", "SET", "maxmemory-policy", "noeviction")
        client.command("SET", "asdf4", 4)
        commands = repl.read_commands(8, args.replication_read_timeout_s)
        unlink_count = sum(
            1
            for command in commands
            if isinstance(command, list) and command and command[0] == "unlink"
        )
        return {
            "scenario": "propagation-eviction",
            "after_eviction_dbsize": after_eviction_dbsize,
            "commands": commands,
            "unlink_count": unlink_count,
            "saw_three_unlinks": unlink_count == 3,
        }
    finally:
        if repl is not None:
            repl.close()
        client.close()


def run_propagation_eviction_multi(args: argparse.Namespace, port: int) -> dict[str, Any]:
    client = RespClient(port)
    repl: ReplicationStream | None = None
    try:
        client.command("FLUSHALL")
        client.command("CONFIG", "SET", "maxmemory", 0)
        client.command("CONFIG", "SET", "maxmemory-policy", "noeviction")
        client.command("CONFIG", "SET", "repl-ping-replica-period", 3600)
        repl = ReplicationStream(port)

        client.command("CONFIG", "SET", "maxmemory-policy", "allkeys-lru")
        client.command("MULTI")
        client.command("INCR", "x")
        client.command("CONFIG", "SET", "maxmemory", 1)
        client.command("INCR", "x")
        exec_reply = client.command("EXEC")

        deadline = time.monotonic() + 5.0
        while dbsize(client) != 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        after_eviction_dbsize = dbsize(client)

        commands = repl.read_commands(8, args.replication_read_timeout_s)
        command_names = [
            command[0]
            for command in commands
            if isinstance(command, list) and command and isinstance(command[0], str)
        ]
        return {
            "scenario": "propagation-eviction-multi",
            "exec_reply": exec_reply,
            "after_eviction_dbsize": after_eviction_dbsize,
            "commands": commands,
            "command_names": command_names,
            "matches_expected_shape": command_names
            == ["multi", "select", "incr", "incr", "exec", "unlink"],
        }
    finally:
        if repl is not None:
            repl.close()
        client.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario",
        choices=[
            "volatile-loop",
            "tracking-feedback",
            "lfu-init",
            "import-mode",
            "propagation-eviction",
            "propagation-eviction-multi",
        ],
        default="volatile-loop",
    )
    parser.add_argument("--server-bin", default=str(DEFAULT_SERVER))
    parser.add_argument("--baseport", type=int, default=46380)
    parser.add_argument("--policy", default="volatile-lfu")
    parser.add_argument("--memory-delta-kb", type=int, default=400)
    parser.add_argument("--max-fill-keys", type=int, default=20000)
    parser.add_argument("--progress-every", type=int, default=500)
    parser.add_argument("--tracking-clients", type=int, default=10)
    parser.add_argument("--tracking-keys", type=int, default=300)
    parser.add_argument("--tracking-key-suffix-len", type=int, default=1000)
    parser.add_argument("--tracking-limit-drop-bytes", type=int, default=40000)
    parser.add_argument("--tracking-trim-sleep-s", type=float, default=1.1)
    parser.add_argument("--tracking-read-timeout-s", type=float, default=2.0)
    parser.add_argument("--replication-read-timeout-s", type=float, default=0.5)
    parser.add_argument("--keep-logs", action="store_true")
    args = parser.parse_args()

    server_bin = Path(args.server_bin)
    if not server_bin.exists():
        print(f"missing server binary: {server_bin}", file=sys.stderr)
        return 2

    port = find_free_port(args.baseport)
    with tempfile.TemporaryDirectory(prefix="maxmemory-repro-") as tmp:
        tmpdir = Path(tmp)
        stdout_path = tmpdir / "stdout.log"
        stderr_path = tmpdir / "stderr.log"
        stdout = stdout_path.open("wb")
        stderr = stderr_path.open("wb")
        proc = subprocess.Popen(
            [
                str(server_bin),
                "--port",
                str(port),
                "--bind",
                "127.0.0.1",
                "--save",
                "",
                "--appendonly",
                "no",
                "--dir",
                str(tmpdir),
            ],
            cwd=str(ROOT),
            stdout=stdout,
            stderr=stderr,
        )
        try:
            wait_for_server(port, proc)
            if args.scenario == "tracking-feedback":
                result = run_tracking_feedback(args, port)
            elif args.scenario == "lfu-init":
                result = run_lfu_init(args, port)
            elif args.scenario == "import-mode":
                result = run_import_mode(args, port)
            elif args.scenario == "propagation-eviction":
                result = run_propagation_eviction(args, port)
            elif args.scenario == "propagation-eviction-multi":
                result = run_propagation_eviction_multi(args, port)
            else:
                result = run_volatile_loop(args, port)
            result["port"] = port
            result["server_bin"] = str(server_bin)
            print(json.dumps(result, indent=2, sort_keys=True))
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
            stdout.close()
            stderr.close()
            if args.keep_logs:
                print(f"logs: {tmpdir}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
