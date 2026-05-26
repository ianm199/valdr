#!/usr/bin/env python3
"""Reconstruct the TCL conformance convergence curve from git history.

The conformance dashboard wants a *time series* of per-section TCL pass counts,
but the project only ever recorded the wire-diff oracle over time. This driver
recovers the real curve by replaying the upstream Valkey TCL suite against the
Rust server built at historical commits.

It is deliberately isolated from the live tree so it can run alongside other
agents:

  * All builds and checkouts happen in a dedicated git worktree, never the main
    working tree.
  * The upstream test files come from a single frozen copy of
    ``reference/valkey/tests`` (the pinned upstream is constant, so the
    denominator stays fixed across every sampled commit).
  * A private baseport range keeps spawned servers off other runners' ports.

The TCL invocation mirrors ``harness/oracle/tcl-survey.py`` exactly (same
deny-tags, no ``--durable``) so the reconstructed numbers line up with the
project's own conformance reporting.

Output is written incrementally to ``convergence-data.json`` so the dashboard
can render a partial curve while the sweep is still running.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

MAIN = Path(__file__).resolve().parents[2]
PARENT = MAIN.parent
WORKTREE = PARENT / "redis-rs-port-convergence"
REF_FROZEN = PARENT / "redis-convergence-reference"
OUT = MAIN / "convergence-data.json"

SUMMARY_RE = re.compile(r"Test Summary:\s+(\d+)\s+passed,\s+(\d+)\s+failed")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

DENY_TAGS = ["needs:repl", "needs:debug", "external:skip"]

# The TCL harness binds its coordination socket on a free port in
# [baseport-32, baseport-1] and spawns servers from baseport up. Reusing one
# baseport across rapid sequential runs fills that 32-port window with
# TIME_WAIT sockets and the next run fails to bind. So each run gets its own
# well-separated baseport, kept below the macOS ephemeral range (49152) and
# clear of other agents' runners (45000, 52000).
BASEPORT_START = 30000
BASEPORT_STEP = 100
_next_baseport = [BASEPORT_START]


def alloc_baseport():
    bp = _next_baseport[0]
    _next_baseport[0] += BASEPORT_STEP
    if _next_baseport[0] >= 44000:
        _next_baseport[0] = BASEPORT_START
    return bp

SECTIONS = [
    ("unit/type/string", "Strings"),
    ("unit/type/incr", "Incr"),
    ("unit/type/list", "Lists"),
    ("unit/type/hash", "Hashes"),
    ("unit/type/set", "Sets"),
    ("unit/type/zset", "Sorted sets"),
    ("unit/type/stream", "Streams"),
    ("unit/type/stream-cgroups", "Stream groups"),
    ("unit/protocol", "Protocol"),
    ("unit/keyspace", "Keyspace"),
    ("unit/expire", "Expire"),
    ("unit/bitops", "Bitops"),
    ("unit/geo", "Geo"),
    ("unit/scan", "Scan"),
]


def run(cmd, *, cwd, timeout=None, env=None):
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


def git(args, *, cwd=MAIN):
    return run(["git", *args], cwd=cwd)


def sample_commits():
    """Last commit of each calendar day, oldest first."""
    res = git(["log", "--reverse", "--format=%H %cI", "main"])
    by_day = {}
    for line in res.stdout.splitlines():
        sha, iso = line.split(" ", 1)
        day = iso[:10]
        by_day[day] = (sha, iso)
    return [by_day[d] for d in sorted(by_day)]


def ensure_worktree(first_sha):
    if not WORKTREE.exists():
        git(["worktree", "add", "--detach", str(WORKTREE), first_sha])
    if REF_FROZEN.exists():
        return
    REF_FROZEN.mkdir(parents=True)
    shutil.copytree(MAIN / "reference/valkey/tests", REF_FROZEN / "tests", symlinks=True)
    (REF_FROZEN / "src").symlink_to(MAIN / "reference/valkey/src")


def build_at(sha):
    co = git(["checkout", "--detach", sha], cwd=WORKTREE)
    if co.returncode != 0:
        return False, f"checkout failed: {co.stderr.strip()[:200]}"
    build = run(
        ["cargo", "build", "--bin", "redis-server"],
        cwd=WORKTREE,
        timeout=900,
    )
    if build.returncode != 0:
        tail = build.stderr.strip().splitlines()[-3:]
        return False, "build failed: " + " | ".join(tail)
    binary = WORKTREE / "target/debug/redis-server"
    link = WORKTREE / "target/debug/valkey-server"
    if link.exists() or link.is_symlink():
        link.unlink()
    link.symlink_to(binary)
    return True, "ok"


def run_section(test_file, env):
    cmd = [
        "tclsh",
        "tests/test_helper.tcl",
        "--single",
        test_file,
        "--clients",
        "1",
        "--skip-leaks",
        "--baseport",
        str(alloc_baseport()),
        "--tags",
        " ".join(f"-{t}" for t in DENY_TAGS),
        "--quiet",
    ]
    try:
        proc = run(cmd, cwd=REF_FROZEN, timeout=90, env=env)
    except subprocess.TimeoutExpired:
        return {"passed": None, "failed": None, "total": 0, "timed_out": True}
    text = ANSI_RE.sub("", f"{proc.stdout}\n{proc.stderr}")
    m = SUMMARY_RE.findall(text)
    if not m:
        return {"passed": None, "failed": None, "total": 0, "timed_out": False}
    passed = sum(int(a) for a, _ in m)
    failed = sum(int(b) for _, b in m)
    return {
        "passed": passed,
        "failed": failed,
        "total": passed + failed,
        "timed_out": False,
    }


def write_out(points):
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": (
            "Upstream Valkey TCL suite replayed against the Rust server built at "
            "each sampled commit. Same deny-tags as harness/oracle/tcl-survey.py "
            "(needs:repl, needs:debug, external:skip), no --durable."
        ),
        "sections": [{"file": f, "label": lbl} for f, lbl in SECTIONS],
        "points": points,
    }
    OUT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="Only the most recent N sampled commits (for a quick smoke test).")
    args = ap.parse_args()

    commits = sample_commits()
    if args.limit:
        commits = commits[-args.limit :]
    if not commits:
        print("no commits", file=sys.stderr)
        return 1

    ensure_worktree(commits[0][0])

    env = None
    import os

    env = os.environ.copy()
    env["VALKEY_BIN_DIR"] = str(WORKTREE / "target/debug")

    points = []
    for i, (sha, iso) in enumerate(commits, 1):
        short = sha[:9]
        print(f"[{i}/{len(commits)}] {iso[:10]} {short} building...", flush=True)
        ok, msg = build_at(sha)
        point = {
            "date": iso[:10],
            "iso": iso,
            "sha": short,
            "build_ok": ok,
            "note": msg,
            "sections": {},
        }
        if ok:
            for test_file, _label in SECTIONS:
                r = run_section(test_file, env)
                point["sections"][test_file] = r
                p = r["passed"]
                print(
                    f"    {test_file:30s} "
                    + (f"{p}/{r['total']}" if p is not None else ("timeout" if r["timed_out"] else "no-summary")),
                    flush=True,
                )
        else:
            print(f"    SKIP ({msg})", flush=True)
        points.append(point)
        write_out(points)

    git(["checkout", "--detach", "main"], cwd=WORKTREE)
    print(f"done -> {OUT}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
