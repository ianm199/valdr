#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

VALKEY_BIN="${ROOT}/reference/valkey/src/valkey-server"
VALKEY_BENCH="${ROOT}/reference/valkey/src/valkey-benchmark"
RUST_BIN="${ROOT}/target/release/redis-server"

if [[ ! -x "$VALKEY_BIN" || ! -x "$VALKEY_BENCH" ]]; then
    bash scripts/setup-reference.sh >/dev/null
fi

if [[ "${VALKEY_BENCH_SKIP_BUILD:-0}" != "1" ]]; then
    cargo build --release -p redis-server >/dev/null
elif [[ ! -x "$RUST_BIN" ]]; then
    cargo build --release -p redis-server >/dev/null
fi

python3 - <<'PY'
import csv
import json
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path.cwd()
VALKEY_BIN = ROOT / "reference/valkey/src/valkey-server"
VALKEY_BENCH = ROOT / "reference/valkey/src/valkey-benchmark"
RUST_BIN = ROOT / "target/release/redis-server"


@dataclass(frozen=True)
class Profile:
    name: str
    requests: int
    clients: int
    pipeline: int
    payload: int
    tests: str


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


PROFILES = [
    Profile(
        name="core-p1",
        requests=env_int("VALKEY_MATRIX_CORE_P1_REQUESTS", 50_000),
        clients=env_int("VALKEY_MATRIX_CLIENTS", 50),
        pipeline=1,
        payload=env_int("VALKEY_MATRIX_PAYLOAD", 64),
        tests="set,get,incr,ping_mbulk",
    ),
    Profile(
        name="core-p16",
        requests=env_int("VALKEY_MATRIX_CORE_P16_REQUESTS", 200_000),
        clients=env_int("VALKEY_MATRIX_CLIENTS", 50),
        pipeline=16,
        payload=env_int("VALKEY_MATRIX_PAYLOAD", 64),
        tests="set,get,incr,ping_mbulk",
    ),
    Profile(
        name="core-p100",
        requests=env_int("VALKEY_MATRIX_CORE_P100_REQUESTS", 200_000),
        clients=env_int("VALKEY_MATRIX_CLIENTS", 50),
        pipeline=100,
        payload=env_int("VALKEY_MATRIX_PAYLOAD", 64),
        tests="set,get,incr,ping_mbulk",
    ),
    Profile(
        name="range-heavy-p16",
        requests=env_int("VALKEY_MATRIX_RANGE_REQUESTS", 100_000),
        clients=env_int("VALKEY_MATRIX_CLIENTS", 50),
        pipeline=16,
        payload=env_int("VALKEY_MATRIX_PAYLOAD", 64),
        tests="lrange_100,lrange_300",
    ),
]


def git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except subprocess.CalledProcessError:
        return "unknown"


def hardware_fingerprint() -> dict:
    def run(cmd: list[str]) -> str:
        try:
            return subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            return ""

    cpu = run(["sysctl", "-n", "machdep.cpu.brand_string"])
    if not cpu and Path("/proc/cpuinfo").exists():
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    return {
        "os": run(["uname", "-sr"]) or "unknown",
        "arch": run(["uname", "-m"]) or "unknown",
        "cpu": cpu or "unknown",
    }


def free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


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
    elif target == "rust":
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
    else:
        raise ValueError(target)

    log = open(f"/tmp/valkey-rs-bench-{target}-{port}.log", "w", encoding="utf-8")
    proc = subprocess.Popen(cmd, stdout=log, stderr=log, text=True)
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


def parse_csv(stdout: str) -> list[dict]:
    rows = []
    reader = csv.reader(stdout.splitlines())
    header = next(reader, None)
    if not header:
        raise RuntimeError("valkey-benchmark emitted no CSV header")
    for raw in reader:
        if len(raw) < 7:
            continue
        rows.append(
            {
                "command": raw[0],
                "rps": float(raw[1]),
                "avg_ms": float(raw[2]),
                "min_ms": float(raw[3]),
                "p50_ms": float(raw[4]),
                "p95_ms": float(raw[5]),
                "p99_ms": float(raw[6]),
                "max_ms": float(raw[7]) if len(raw) > 7 else None,
            }
        )
    if not rows:
        raise RuntimeError("valkey-benchmark emitted no parseable CSV rows")
    return rows


def run_profile(target: str, profile: Profile) -> list[dict]:
    port = free_port()
    proc = None
    try:
        proc = start_server(target, port)
        cmd = [
            str(VALKEY_BENCH),
            "-h",
            "127.0.0.1",
            "-p",
            str(port),
            "-n",
            str(profile.requests),
            "-c",
            str(profile.clients),
            "-P",
            str(profile.pipeline),
            "-d",
            str(profile.payload),
            "-t",
            profile.tests,
            "--csv",
            "--precision",
            "2",
        ]
        completed = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if completed.returncode != 0:
            raise RuntimeError(
                f"valkey-benchmark failed for {target}/{profile.name} rc={completed.returncode}: "
                f"{completed.stderr[-2000:]}"
            )
        rows = parse_csv(completed.stdout)
        for row in rows:
            row.update(
                {
                    "target": target,
                    "profile": profile.name,
                    "requests": profile.requests,
                    "clients": profile.clients,
                    "pipeline": profile.pipeline,
                    "payload": profile.payload,
                    "tests": profile.tests,
                    "command_line": cmd,
                }
            )
        return rows
    finally:
        stop_server(proc)


def main() -> int:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    commit = git_commit()
    hardware = hardware_fingerprint()
    results_dir = Path("harness/bench/results")
    results_dir.mkdir(parents=True, exist_ok=True)
    tsv_rel = f"harness/bench/results/{stamp}-{commit}-profile-matrix.tsv"
    tsv_path = Path(tsv_rel)

    all_reference = []
    all_rust = []
    for profile in PROFILES:
        print(f"==> {profile.name}: reference", file=sys.stderr)
        all_reference.extend(run_profile("reference", profile))
        print(f"==> {profile.name}: rust", file=sys.stderr)
        all_rust.extend(run_profile("rust", profile))

    reference_by_key = {(r["profile"], r["command"]): r for r in all_reference}
    rust_by_key = {(r["profile"], r["command"]): r for r in all_rust}
    ratio_rows = []
    for key, ref in reference_by_key.items():
        rust = rust_by_key.get(key)
        if rust is None:
            continue
        ratio_rows.append(
            {
                "profile": key[0],
                "command": key[1],
                "ratio": rust["rps"] / ref["rps"] if ref["rps"] else 0.0,
                "reference": ref,
                "rust": rust,
            }
        )

    if not ratio_rows:
        raise RuntimeError("no reference/rust benchmark rows matched")

    median_ratio = sorted(row["ratio"] for row in ratio_rows)[len(ratio_rows) // 2]
    min_ratio = min(row["ratio"] for row in ratio_rows)
    max_ratio = max(row["ratio"] for row in ratio_rows)
    get_p1 = next((row for row in ratio_rows if row["profile"] == "core-p1" and row["command"] == "GET"), None)
    get_p100 = next((row for row in ratio_rows if row["profile"] == "core-p100" and row["command"] == "GET"), None)

    with tsv_path.open("w", encoding="utf-8") as out:
        out.write("# valkey-rs profile matrix benchmark\n")
        out.write(f"# timestamp_utc\t{stamp}\n")
        out.write(f"# commit\t{commit}\n")
        out.write(f"# os\t{hardware['os']}\n")
        out.write(f"# arch\t{hardware['arch']}\n")
        out.write(f"# cpu\t{hardware['cpu']}\n")
        out.write("profile\tcommand\trequests\tclients\tpipeline\tpayload\treference_rps\trust_rps\tratio\treference_p50_ms\trust_p50_ms\treference_p95_ms\trust_p95_ms\treference_p99_ms\trust_p99_ms\n")
        for row in ratio_rows:
            ref = row["reference"]
            rust = row["rust"]
            out.write(
                f"{row['profile']}\t{row['command']}\t{ref['requests']}\t{ref['clients']}\t{ref['pipeline']}\t{ref['payload']}\t"
                f"{ref['rps']:.2f}\t{rust['rps']:.2f}\t{row['ratio']:.6f}\t"
                f"{ref['p50_ms']:.3f}\t{rust['p50_ms']:.3f}\t{ref['p95_ms']:.3f}\t{rust['p95_ms']:.3f}\t{ref['p99_ms']:.3f}\t{rust['p99_ms']:.3f}\n"
            )

    measurements = []
    for row in ratio_rows:
        workload = f"{row['profile']}/{row['command']}"
        measurements.extend(
            [
                {
                    "metric": "throughput_req_s",
                    "target": "reference",
                    "workload": workload,
                    "value": row["reference"]["rps"],
                    "unit": "req/s",
                },
                {
                    "metric": "throughput_req_s",
                    "target": "rust",
                    "workload": workload,
                    "value": row["rust"]["rps"],
                    "unit": "req/s",
                },
                {
                    "metric": "throughput_ratio",
                    "target": "rust-vs-reference",
                    "workload": workload,
                    "value": row["ratio"],
                    "unit": "ratio",
                },
                {
                    "metric": "p99_ms",
                    "target": "reference",
                    "workload": workload,
                    "value": row["reference"]["p99_ms"],
                    "unit": "ms",
                },
                {
                    "metric": "p99_ms",
                    "target": "rust",
                    "workload": workload,
                    "value": row["rust"]["p99_ms"],
                    "unit": "ms",
                },
            ]
        )

    passed = all(row["reference"]["rps"] > 0 and row["rust"]["rps"] > 0 for row in ratio_rows)
    result = {
        "schema_version": 1,
        "runner_id": "bench-profile-matrix",
        "status": "pass" if passed else "fail",
        "surface": "performance",
        "method": "bench-load",
        "summary": (
            f"profile matrix: median {median_ratio:.2f}x, min {min_ratio:.2f}x, max {max_ratio:.2f}x"
            + (
                f"; GET p1 {get_p1['ratio']:.2f}x"
                if get_p1
                else ""
            )
            + (
                f"; GET p100 {get_p100['ratio']:.2f}x"
                if get_p100
                else ""
            )
        ),
        "measurements": measurements,
        "artifacts": [{"path": tsv_rel}],
        "evidence": {
            "tool": str(VALKEY_BENCH),
            "commit": commit,
            "hardware": hardware,
            "profiles": [profile.__dict__ for profile in PROFILES],
            "ratios": ratio_rows,
            "tsv": tsv_rel,
        },
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
PY
