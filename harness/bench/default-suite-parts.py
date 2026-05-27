#!/usr/bin/env python3
"""Run Redis/Valkey benchmark default-suite parts as first-class probes.

This is a triage tool, not a public benchmark claim. It decomposes the
valkey-benchmark default suite into bounded cells so slow commands can be
found quickly and rerun directly.

Two modes matter:
  isolated: each selected test gets a fresh server.
  ordered: one server runs selected tests in default-suite order, preserving
           state between cells. This is useful for reproducing default-suite
           slowdowns such as LPOP after earlier LPUSH/RPUSH cells.

Artifacts:
  harness/bench/results/<ts>-<sha>-default-suite-parts.tsv
  harness/bench/results/<ts>-<sha>-default-suite-parts.json
  harness/bench/results/<ts>-{reference,rust}-default-suite-parts-*.log
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
class DefaultPart:
    selector: str
    title: str
    command: str
    note: str = ""


DEFAULT_PARTS: tuple[DefaultPart, ...] = (
    DefaultPart("ping_inline", "PING_INLINE", "PING inline"),
    DefaultPart("ping_mbulk", "PING_MBULK", "PING multibulk"),
    DefaultPart("set", "SET", "SET key:__rand_int__ __data__"),
    DefaultPart("get", "GET", "GET key:__rand_int__"),
    DefaultPart("incr", "INCR", "INCR counter:__rand_int__"),
    DefaultPart("lpush", "LPUSH", "LPUSH mylist __data__", "Mutates mylist."),
    DefaultPart("rpush", "RPUSH", "RPUSH mylist __data__", "Mutates mylist."),
    DefaultPart("lpop", "LPOP", "LPOP mylist", "State-sensitive: full default suite runs this after LPUSH/RPUSH."),
    DefaultPart("rpop", "RPOP", "RPOP mylist", "State-sensitive: full default suite runs this after LPOP."),
    DefaultPart("sadd", "SADD", "SADD myset element:__rand_int__", "Mutates myset."),
    DefaultPart("hset", "HSET", "HSET myhash element:__rand_int__ __data__"),
    DefaultPart("spop", "SPOP", "SPOP myset", "State-sensitive: full default suite runs this after SADD."),
    DefaultPart("zadd", "ZADD", "ZADD myzset score element", "Mutates myzset."),
    DefaultPart("zpopmin", "ZPOPMIN", "ZPOPMIN myzset", "State-sensitive: full default suite runs this after ZADD."),
    DefaultPart(
        "lrange_100",
        "LRANGE_100 (first 100 elements)",
        "LRANGE mylist 0 99",
        "valkey-benchmark emits an LPUSH prep row before this row.",
    ),
    DefaultPart(
        "lrange_300",
        "LRANGE_300 (first 300 elements)",
        "LRANGE mylist 0 299",
        "valkey-benchmark emits an LPUSH prep row before this row.",
    ),
    DefaultPart(
        "lrange_500",
        "LRANGE_500 (first 500 elements)",
        "LRANGE mylist 0 499",
        "valkey-benchmark emits an LPUSH prep row before this row.",
    ),
    DefaultPart(
        "lrange_600",
        "LRANGE_600 (first 600 elements)",
        "LRANGE mylist 0 599",
        "valkey-benchmark emits an LPUSH prep row before this row.",
    ),
    DefaultPart("mset", "MSET (10 keys)", "MSET 10 key/value pairs"),
    DefaultPart("mget", "MGET (10 keys)", "MGET 10 keys"),
    DefaultPart("xadd", "XADD", "XADD mystream * myfield __data__"),
    DefaultPart("function_load", "FUNCTION LOAD", "FUNCTION LOAD REPLACE <generated lib>"),
    DefaultPart("fcall", "FCALL", "FUNCTION LOAD REPLACE <generated lib>; FCALL foo1"),
)

PART_BY_SELECTOR = {part.selector: part for part in DEFAULT_PARTS}
PART_BY_TITLE = {part.title.lower(): part for part in DEFAULT_PARTS}


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
    if not VALKEY_BIN.exists() or not VALKEY_BENCH.exists():
        subprocess.run(["bash", "scripts/setup-reference.sh"], cwd=ROOT, check=True)
    if build and (os.environ.get("VALKEY_BENCH_SKIP_BUILD") != "1" or not RUST_BIN.exists()):
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


def normalize_title(title: str) -> str:
    return " ".join(title.lower().split())


def part_for_csv_title(title: str) -> DefaultPart | None:
    normalized = normalize_title(title)
    for part in DEFAULT_PARTS:
        if normalize_title(part.title) == normalized:
            return part
    if normalized == normalize_title("LPUSH (needed to benchmark LRANGE)"):
        return DefaultPart("lrange_prep", "LPUSH (needed to benchmark LRANGE)", "LPUSH mylist __data__")
    return None


def parse_csv_rows(stdout: str) -> list[dict[str, Any]]:
    reader = csv.reader(line for line in stdout.splitlines() if line.strip())
    header = next(reader, None)
    if not header:
        raise RuntimeError("valkey-benchmark emitted no CSV header")
    rows = []
    for row in reader:
        if len(row) < 8:
            continue
        part = part_for_csv_title(row[0])
        rows.append(
            {
                "selector": part.selector if part else row[0].lower().replace(" ", "_"),
                "title": row[0],
                "command": part.command if part else row[0],
                "rps": float(row[1]),
                "avg_ms": float(row[2]),
                "min_ms": float(row[3]),
                "p50_ms": float(row[4]),
                "p95_ms": float(row[5]),
                "p99_ms": float(row[6]),
                "max_ms": float(row[7]),
            }
        )
    if not rows:
        raise RuntimeError(f"valkey-benchmark emitted no parseable CSV rows: {stdout[-500:]}")
    return rows


def benchmark_command(
    port: int,
    part: DefaultPart,
    args: argparse.Namespace,
    extra: list[str] | None = None,
) -> list[str]:
    cmd = [
        str(VALKEY_BENCH),
        "-h",
        "127.0.0.1",
        "-p",
        str(port),
        "-n",
        str(args.requests),
        "-c",
        str(args.clients),
        "-P",
        str(args.pipeline),
        "-d",
        str(args.payload),
        "-t",
        part.selector,
        "--csv",
        "--precision",
        "3",
    ]
    if args.keyspace is not None:
        cmd.extend(["-r", str(args.keyspace)])
    if args.seed is not None:
        cmd.extend(["--seed", str(args.seed)])
    if extra:
        cmd.extend(extra)
    return cmd


def warmup_command(port: int, args: argparse.Namespace) -> list[str]:
    return [
        str(VALKEY_BENCH),
        "-h",
        "127.0.0.1",
        "-p",
        str(port),
        "-n",
        str(args.warmup_requests),
        "-c",
        str(args.warmup_clients),
        "-P",
        str(args.warmup_pipeline),
        "-d",
        str(args.warmup_payload),
        "-t",
        args.warmup_command,
        "--csv",
        "--precision",
        "3",
    ]


def run_warmup_on_port(port: int, args: argparse.Namespace) -> dict[str, Any]:
    if args.warmup_requests <= 0:
        return {"status": "skipped", "requests": 0}

    cmd = warmup_command(port, args)
    started = time.monotonic()
    try:
        completed = subprocess.run(
            cmd,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=args.timeout_s,
        )
    except subprocess.TimeoutExpired:
        return {
            "status": "timeout",
            "requests": args.warmup_requests,
            "elapsed_s": time.monotonic() - started,
            "command_line": cmd,
        }
    if completed.returncode != 0:
        return {
            "status": "error",
            "requests": args.warmup_requests,
            "elapsed_s": time.monotonic() - started,
            "returncode": completed.returncode,
            "stderr_tail": completed.stderr[-1000:],
            "command_line": cmd,
        }
    try:
        rows = parse_csv_rows(completed.stdout)
        rps = rows[-1].get("rps")
    except RuntimeError:
        rps = None
    return {
        "status": "ok",
        "requests": args.warmup_requests,
        "clients": args.warmup_clients,
        "pipeline": args.warmup_pipeline,
        "payload": args.warmup_payload,
        "command": args.warmup_command,
        "elapsed_s": time.monotonic() - started,
        "rps": rps,
        "command_line": cmd,
    }


def run_benchmark_on_port(
    target: Target,
    part: DefaultPart,
    port: int,
    log_path: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    started = time.monotonic()
    cmd = benchmark_command(port, part, args)
    try:
        completed = subprocess.run(
            cmd,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=args.timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode(errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode(errors="replace")
        return {
            "target": target.value,
            "selector": part.selector,
            "title": part.title,
            "status": "timeout",
            "elapsed_s": time.monotonic() - started,
            "timeout_s": args.timeout_s,
            "stdout_tail": stdout[-4000:],
            "stderr_tail": stderr[-2000:],
            "log_path": relative(log_path),
            "command_line": cmd,
            "emitted_rows": [],
        }
    if completed.returncode != 0:
        return {
            "target": target.value,
            "selector": part.selector,
            "title": part.title,
            "status": "error",
            "elapsed_s": time.monotonic() - started,
            "returncode": completed.returncode,
            "stdout_tail": completed.stdout[-4000:],
            "stderr_tail": completed.stderr[-2000:],
            "log_path": relative(log_path),
            "command_line": cmd,
            "emitted_rows": [],
        }
    try:
        emitted_rows = parse_csv_rows(completed.stdout)
    except RuntimeError as err:
        return {
            "target": target.value,
            "selector": part.selector,
            "title": part.title,
            "status": "parse_error",
            "elapsed_s": time.monotonic() - started,
            "error": str(err),
            "stdout_tail": completed.stdout[-4000:],
            "stderr_tail": completed.stderr[-2000:],
            "log_path": relative(log_path),
            "command_line": cmd,
            "emitted_rows": [],
        }

    primary = dict(
        next(
            (row for row in emitted_rows if normalize_title(row["title"]) == normalize_title(part.title)),
            emitted_rows[-1],
        )
    )
    primary.update(
        {
            "target": target.value,
            "status": "ok",
            "elapsed_s": time.monotonic() - started,
            "log_path": relative(log_path),
            "command_line": cmd,
            "emitted_rows": emitted_rows,
        }
    )
    return primary


def run_isolated(target: Target, part: DefaultPart, stamp: str, args: argparse.Namespace) -> dict[str, Any]:
    port = free_port()
    log_path = RESULTS_DIR / f"{stamp}-{target.value}-default-suite-parts-{args.mode}-{part.selector}.log"
    proc: subprocess.Popen[str] | None = None
    try:
        proc = start_server(target, port, log_path)
        warmup = run_warmup_on_port(port, args)
        if warmup["status"] not in ("ok", "skipped"):
            raise RuntimeError(f"warmup failed for {target.value}:{part.selector}: {warmup}")
        row = run_benchmark_on_port(target, part, port, log_path, args)
        row["warmup"] = warmup
        return row
    finally:
        stop_server(proc)


def run_ordered(target: Target, parts: list[DefaultPart], stamp: str, args: argparse.Namespace) -> list[dict[str, Any]]:
    port = free_port()
    log_path = RESULTS_DIR / f"{stamp}-{target.value}-default-suite-parts-{args.mode}.log"
    proc: subprocess.Popen[str] | None = None
    rows = []
    try:
        proc = start_server(target, port, log_path)
        warmup = run_warmup_on_port(port, args)
        if warmup["status"] not in ("ok", "skipped"):
            raise RuntimeError(f"warmup failed for {target.value}: {warmup}")
        for part in parts:
            row = run_benchmark_on_port(target, part, port, log_path, args)
            row["warmup"] = warmup
            rows.append(row)
            if args.stop_on_timeout and rows[-1]["status"] == "timeout":
                break
        return rows
    finally:
        stop_server(proc)


def csv_list(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def select_parts(raw: str) -> list[DefaultPart]:
    if raw == "all":
        return list(DEFAULT_PARTS)
    selected = []
    for item in csv_list(raw):
        normalized = item.lower()
        part = PART_BY_SELECTOR.get(normalized)
        if part is None:
            part = PART_BY_TITLE.get(normalized)
        if part is None:
            raise SystemExit(f"unknown default-suite part {item!r}; run `default-suite-parts.py list`")
        selected.append(part)
    return selected


def select_targets(raw: str) -> list[Target]:
    if raw == "both":
        return [Target.REFERENCE, Target.RUST]
    out = []
    for item in csv_list(raw):
        try:
            out.append(Target(item))
        except ValueError as exc:
            raise SystemExit(f"unknown target {item!r}; expected reference, rust, or both") from exc
    return out


def pair_rows(part: DefaultPart, target_rows: dict[str, dict[str, Any]]) -> dict[str, Any]:
    reference = target_rows.get(Target.REFERENCE.value)
    rust = target_rows.get(Target.RUST.value)
    status_values = [row["status"] for row in target_rows.values()]
    status = "ok" if status_values and all(status == "ok" for status in status_values) else ",".join(status_values)
    ratio = None
    if reference and rust and reference.get("rps"):
        ratio = rust.get("rps", 0.0) / reference["rps"]
    return {
        "selector": part.selector,
        "title": part.title,
        "command": part.command,
        "status": status,
        "ratio": ratio,
        "reference_rps": reference.get("rps") if reference else None,
        "rust_rps": rust.get("rps") if rust else None,
        "reference_elapsed_s": reference.get("elapsed_s") if reference else None,
        "rust_elapsed_s": rust.get("elapsed_s") if rust else None,
        "reference_p50_ms": reference.get("p50_ms") if reference else None,
        "rust_p50_ms": rust.get("p50_ms") if rust else None,
        "reference_p95_ms": reference.get("p95_ms") if reference else None,
        "rust_p95_ms": rust.get("p95_ms") if rust else None,
        "reference_p99_ms": reference.get("p99_ms") if reference else None,
        "rust_p99_ms": rust.get("p99_ms") if rust else None,
        "reference_max_ms": reference.get("max_ms") if reference else None,
        "rust_max_ms": rust.get("max_ms") if rust else None,
        "targets": target_rows,
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ok = [row for row in rows if row["status"] == "ok"]
    ratios = sorted(row["ratio"] for row in ok if row["ratio"] is not None)
    timeouts = []
    errors = []
    for row in rows:
        for target, target_row in row["targets"].items():
            if target_row["status"] == "timeout":
                timeouts.append(f"{row['selector']}:{target}")
            elif target_row["status"] != "ok":
                errors.append(f"{row['selector']}:{target}:{target_row['status']}")
    slowest_rust = sorted(
        [
            {
                "selector": row["selector"],
                "rps": row["rust_rps"],
                "elapsed_s": row["rust_elapsed_s"],
                "p99_ms": row["rust_p99_ms"],
            }
            for row in rows
            if row.get("rust_rps") is not None
        ],
        key=lambda row: row["rps"],
    )[:8]
    weakest_ratios = sorted(
        [
            {
                "selector": row["selector"],
                "ratio": row["ratio"],
                "rust_rps": row["rust_rps"],
                "reference_rps": row["reference_rps"],
            }
            for row in ok
            if row["ratio"] is not None
        ],
        key=lambda row: row["ratio"],
    )[:8]
    return {
        "ok": len(ok),
        "total": len(rows),
        "timeouts": timeouts,
        "errors": errors,
        "min_ratio": ratios[0] if ratios else None,
        "median_ratio": ratios[len(ratios) // 2] if ratios else None,
        "slowest_rust": slowest_rust,
        "weakest_ratios": weakest_ratios,
    }


def write_tsv(path: Path, stamp: str, commit: str, hardware: dict[str, str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as out:
        out.write("# valkey-rs default-suite parts probe\n")
        out.write(f"# timestamp_utc\t{stamp}\n")
        out.write(f"# commit\t{commit}\n")
        out.write(f"# os\t{hardware['os']}\n")
        out.write(f"# arch\t{hardware['arch']}\n")
        out.write(f"# cpu\t{hardware['cpu']}\n")
        out.write(f"# warmup_requests\t{rows[0]['targets'][next(iter(rows[0]['targets']))].get('warmup', {}).get('requests', '') if rows and rows[0].get('targets') else ''}\n")
        out.write(f"# warmup_command\t{rows[0]['targets'][next(iter(rows[0]['targets']))].get('warmup', {}).get('command', '') if rows and rows[0].get('targets') else ''}\n")
        out.write(
            "selector\ttitle\tstatus\treference_rps\trust_rps\tratio\t"
            "reference_elapsed_s\trust_elapsed_s\treference_p50_ms\trust_p50_ms\t"
            "reference_p95_ms\trust_p95_ms\treference_p99_ms\trust_p99_ms\t"
            "reference_max_ms\trust_max_ms\n"
        )
        for row in rows:
            out.write(
                f"{row['selector']}\t{row['title']}\t{row['status']}\t"
                f"{fmt(row['reference_rps'])}\t{fmt(row['rust_rps'])}\t{fmt(row['ratio'], 6)}\t"
                f"{fmt(row['reference_elapsed_s'], 3)}\t{fmt(row['rust_elapsed_s'], 3)}\t"
                f"{fmt(row['reference_p50_ms'], 3)}\t{fmt(row['rust_p50_ms'], 3)}\t"
                f"{fmt(row['reference_p95_ms'], 3)}\t{fmt(row['rust_p95_ms'], 3)}\t"
                f"{fmt(row['reference_p99_ms'], 3)}\t{fmt(row['rust_p99_ms'], 3)}\t"
                f"{fmt(row['reference_max_ms'], 3)}\t{fmt(row['rust_max_ms'], 3)}\n"
            )


def fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def print_list() -> None:
    print("selector\ttitle\tcommand\tnote")
    for part in DEFAULT_PARTS:
        print(f"{part.selector}\t{part.title}\t{part.command}\t{part.note}")


def run(args: argparse.Namespace) -> int:
    require_binaries(build=not args.no_build)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    commit = git_commit()
    hardware = hardware_fingerprint()
    parts = select_parts(args.tests)
    targets = select_targets(args.target)

    target_results: dict[str, list[dict[str, Any]]] = {}
    if args.mode == "ordered":
        for target in targets:
            print(f"==> {target.value}: ordered {'/'.join(part.selector for part in parts)}", flush=True)
            target_results[target.value] = run_ordered(target, parts, stamp, args)
    else:
        for target in targets:
            target_results[target.value] = []
            for part in parts:
                print(f"==> {target.value}: isolated {part.selector}", flush=True)
                target_results[target.value].append(run_isolated(target, part, stamp, args))

    rows = []
    for idx, part in enumerate(parts):
        per_target = {}
        for target in targets:
            results = target_results.get(target.value, [])
            if idx < len(results):
                per_target[target.value] = results[idx]
        if per_target:
            rows.append(pair_rows(part, per_target))

    tsv_path = RESULTS_DIR / f"{stamp}-{commit}-default-suite-parts.tsv"
    json_path = RESULTS_DIR / f"{stamp}-{commit}-default-suite-parts.json"
    write_tsv(tsv_path, stamp, commit, hardware, rows)
    summary = summarize(rows)
    result = {
        "schema_version": 1,
        "probe_id": "default-suite-parts",
        "status": "pass" if not summary["timeouts"] and not summary["errors"] else "fail",
        "commit": commit,
        "hardware": hardware,
        "parameters": vars(args),
        "parts": [asdict(part) for part in parts],
        "summary": summary,
        "rows": rows,
        "artifacts": [{"path": relative(tsv_path)}, {"path": relative(json_path)}],
        "note": (
            "Telemetry only. `ordered` mode preserves server state between selected parts; "
            "`isolated` mode starts a fresh server per part."
        ),
    }
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "pass" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Redis/Valkey default benchmark suite parts")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list", help="List known default-suite parts")
    run_parser = sub.add_parser("run", help="Run selected default-suite parts")
    run_parser.add_argument("--mode", choices=["isolated", "ordered"], default="isolated")
    run_parser.add_argument("--target", default="both", help="reference, rust, or both")
    run_parser.add_argument("--tests", default="all", help="all or comma-separated selectors")
    run_parser.add_argument("--requests", type=int, default=100_000)
    run_parser.add_argument("--clients", type=int, default=50)
    run_parser.add_argument("--pipeline", type=int, default=1)
    run_parser.add_argument("--payload", type=int, default=3)
    run_parser.add_argument("--timeout-s", type=int, default=20)
    run_parser.add_argument("--keyspace", type=int, default=None, help="Pass -r <keyspace> to valkey-benchmark")
    run_parser.add_argument("--seed", type=int, default=None, help="Pass --seed <seed> to valkey-benchmark")
    run_parser.add_argument("--warmup-requests", type=int, default=0)
    run_parser.add_argument("--warmup-clients", type=int, default=1)
    run_parser.add_argument("--warmup-pipeline", type=int, default=1)
    run_parser.add_argument("--warmup-payload", type=int, default=3)
    run_parser.add_argument("--warmup-command", default="ping_mbulk")
    run_parser.add_argument("--no-build", action="store_true", help="Use existing target/release/redis-server")
    run_parser.add_argument(
        "--stop-on-timeout",
        action="store_true",
        help="In ordered mode, stop the target sequence after the first timeout.",
    )
    args = parser.parse_args()
    if args.command == "list":
        print_list()
        return 0
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
