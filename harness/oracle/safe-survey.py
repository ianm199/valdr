#!/usr/bin/env python3
"""Measure TCL conformance for one or more commits under heavy containment, so
a runaway write (the cause of the 2026-05-26 disk crashes) cannot harm the
system disk. Optionally runs files in parallel across isolated worker sandboxes.

Modes:
  (default)            measure the already-built target/debug/redis-server.
  --last-days N        build + measure the last N daily commits (last-of-day).
  --commits a,b,c      build + measure these explicit commits.

Containment (the test phase, where runaways happen):
  * All test scratch lives inside an N-GB sparse disk image — total blast
    radius is physically capped well under free space.
  * ulimit -f caps any single file write (SIGXFSZ on overflow).
  * Each section runs in its own session; on timeout that process group is
    killed. A watchdog kills *every* worker group if free disk drops past a
    floor (covers the build phase too).
  * --workers K runs K files concurrently, each in its own test-dir copy and
    its own port window, so the upstream harness can't race on tests/tmp.

Sandbox is parameterized (--sandbox SUFFIX, --base-port) so a validation run
can use a separate mount/ports without disturbing another instance.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import queue
import re
import shutil
import signal
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

MAIN = Path(__file__).resolve().parents[2]
PARENT = MAIN.parent
WORKTREE = PARENT / "redis-rs-port-convergence"
DEFAULT_BIN = MAIN / "target/debug/redis-server"
CACHE = MAIN / "harness/oracle/convergence-cache.json"
OUT = MAIN / "convergence-data.json"

SUMMARY_RE = re.compile(r"Test Summary:\s+(\d+)\s+passed,\s+(\d+)\s+failed")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
DENY_TAGS = ["needs:repl", "needs:debug", "external:skip"]
TIMEOUT_S = 90

# set per-process from args in main(); functions/threads read these globals
IMAGE = Path("/tmp/convsafe.sparseimage")
MOUNT = Path("/Volumes/convsafe")
BINDIR = MOUNT / "bin"
UNIQUE = str(BINDIR / "valkey-server")
BASE_PORT = 31000

abort = threading.Event()
_active = set()
_active_lock = threading.Lock()

DENYLIST = {"unit/tls", "unit/mptcp", "unit/io-threads", "unit/oom-score-adj"}

LABEL_OVERRIDES = {
    "unit/type/string": "Strings", "unit/type/incr": "Incr", "unit/type/list": "Lists",
    "unit/type/list-2": "Lists (large)", "unit/type/list-3": "Lists (edge)",
    "unit/type/hash": "Hashes", "unit/type/set": "Sets", "unit/type/zset": "Sorted sets",
    "unit/type/stream": "Streams", "unit/type/stream-cgroups": "Stream groups",
    "unit/protocol": "Protocol", "unit/keyspace": "Keyspace", "unit/expire": "Expire",
    "unit/scan": "Scan", "unit/sort": "Sort", "unit/dump": "Dump/Restore", "unit/other": "Other ops",
    "unit/bitops": "Bitops", "unit/bitfield": "Bitfield", "unit/geo": "Geo", "unit/hyperloglog": "HyperLogLog",
    "unit/scripting": "Scripting", "unit/functions": "Functions",
    "unit/pubsub": "Pub/Sub", "unit/pubsubshard": "Sharded pub/sub", "unit/multi": "Transactions",
    "unit/acl": "ACL", "unit/acl-v2": "ACL v2", "unit/auth": "Auth",
    "unit/info": "INFO", "unit/info-command": "INFO command",
    "unit/introspection": "Introspection", "unit/introspection-2": "Introspection (2)",
    "unit/commandlog": "Command log", "unit/slowlog": "Slow log",
    "unit/latency-monitor": "Latency monitor", "unit/tracking": "Client tracking",
    "unit/maxmemory": "Max-memory", "unit/memefficiency": "Memory efficiency",
    "unit/lazyfree": "Lazy free", "unit/limits": "Limits", "unit/obuf-limits": "Output-buffer limits",
    "unit/client-eviction": "Client eviction", "unit/violations": "Violations",
    "unit/networking": "Networking", "unit/querybuf": "Query buffer", "unit/replybufsize": "Reply buffer",
    "unit/quit": "QUIT", "unit/pause": "CLIENT PAUSE", "unit/shutdown": "Shutdown",
    "unit/wait": "WAIT", "unit/aofrw": "AOF rewrite", "unit/fuzzer": "Fuzzer",
    "unit/hashexpire": "Hash field expiry",
}


def humanize(rel):
    return LABEL_OVERRIDES.get(rel) or rel.split("/")[-1].replace("-", " ").title()


def discover_sections():
    base = MAIN / "reference/valkey/tests"
    out = []
    for p in sorted(base.glob("unit/*.tcl")) + sorted(base.glob("unit/type/*.tcl")):
        rel = str(p.relative_to(base))[:-4]
        if rel in DENYLIST:
            continue
        out.append((rel, humanize(rel)))
    return out


SECTIONS = discover_sections()


def free_gb(path):
    return shutil.disk_usage(str(path)).free / 1e9


def git(args, cwd=MAIN):
    return subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True)


def daily_commits():
    res = git(["log", "--reverse", "--format=%H %cI", "main"])
    by_day = collections.OrderedDict()
    for line in res.stdout.splitlines():
        if not line.strip():
            continue
        sha, iso = line.split(" ", 1)
        by_day[iso[:10]] = (sha, iso)
    return [by_day[d] for d in sorted(by_day)]


# ---- containment ------------------------------------------------------------

def killpg_safe(pgid):
    try:
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def kill_all():
    with _active_lock:
        groups = list(_active)
    for pg in groups:
        killpg_safe(pg)
    subprocess.run(["pkill", "-9", "-f", UNIQUE], capture_output=True)


def watchdog(start_free, floor_drop_gb):
    floor = start_free - floor_drop_gb
    while not abort.is_set():
        if free_gb(MAIN) < floor:
            print(f"\n!! WATCHDOG: free < floor {floor:.1f} GB — KILLING ALL", flush=True)
            abort.set()
            kill_all()
            return
        time.sleep(2)


def setup_image(size, workers):
    detach_image()
    if IMAGE.exists():
        IMAGE.unlink()
    subprocess.run(
        ["hdiutil", "create", "-size", size, "-fs", "APFS", "-volname", MOUNT.name, "-type", "SPARSE", str(IMAGE)],
        check=True, capture_output=True,
    )
    subprocess.run(["hdiutil", "attach", str(IMAGE)], check=True, capture_output=True)
    BINDIR.mkdir()
    for i in range(workers):
        w = MOUNT / f"w{i}"
        w.mkdir()
        shutil.copytree(MAIN / "reference/valkey/tests", w / "tests", symlinks=True)
        (w / "src").symlink_to(MAIN / "reference/valkey/src")


def detach_image():
    if MOUNT.exists():
        subprocess.run(["hdiutil", "detach", "-force", str(MOUNT)], capture_output=True)


def point_binary(path, workers):
    link = BINDIR / "valkey-server"
    if link.exists() or link.is_symlink():
        link.unlink()
    link.symlink_to(path)
    for i in range(workers):
        shutil.rmtree(MOUNT / f"w{i}" / "tests/tmp", ignore_errors=True)


def run_section(test_file, slot, file_cap_blocks, timeout_s):
    deny = " ".join(f"-{t}" for t in DENY_TAGS)
    baseport = BASE_PORT + slot * 300
    inner = (
        f"ulimit -f {file_cap_blocks}; "
        f"exec tclsh tests/test_helper.tcl --single {test_file} "
        f"--clients 1 --skip-leaks --baseport {baseport} --tags '{deny}' --quiet"
    )
    env = os.environ.copy()
    env["VALKEY_BIN_DIR"] = str(BINDIR)
    proc = subprocess.Popen(
        ["bash", "-c", inner], cwd=str(MOUNT / f"w{slot}"), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        start_new_session=True,
    )
    pgid = os.getpgid(proc.pid)
    with _active_lock:
        _active.add(pgid)
    timed_out = False
    try:
        out, _ = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        timed_out = True
        killpg_safe(pgid)
        try:
            out, _ = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            out = ""
    finally:
        killpg_safe(pgid)
        with _active_lock:
            _active.discard(pgid)
    text = ANSI_RE.sub("", out or "")
    m = SUMMARY_RE.findall(text)
    if not m:
        return {"passed": None, "failed": None, "total": 0, "timed_out": timed_out}
    p = sum(int(a) for a, _ in m)
    f = sum(int(b) for _, b in m)
    return {"passed": p, "failed": f, "total": p + f, "timed_out": timed_out}


def survey(workers, file_cap_blocks, timeout_s):
    sections = {}
    lock = threading.Lock()
    slots = queue.Queue()
    for i in range(workers):
        slots.put(i)
    label_of = dict((f, l) for f, l in SECTIONS)

    def task(item):
        test_file, _label = item
        if abort.is_set():
            return
        slot = slots.get()
        try:
            r = run_section(test_file, slot, file_cap_blocks, timeout_s)
        finally:
            slots.put(slot)
        with lock:
            sections[test_file] = r
            shown = (f"{r['passed']}/{r['total']}" if r["passed"] is not None
                     else ("timeout" if r["timed_out"] else "no-summary"))
            print(f"    {label_of[test_file]:16s} {test_file:26s} {shown:>10s}  | sys free {free_gb(MAIN):.1f} GB", flush=True)
        return r

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(task, list(SECTIONS)))
    return sections


# ---- build (worktree) -------------------------------------------------------

def ensure_worktree(first_sha):
    if not WORKTREE.exists():
        git(["worktree", "add", "--detach", str(WORKTREE), first_sha])


def build_at(sha):
    co = git(["checkout", "--detach", sha], cwd=WORKTREE)
    if co.returncode != 0:
        return None, f"checkout failed: {co.stderr.strip()[:160]}"
    b = subprocess.run(["cargo", "build", "--bin", "redis-server"], cwd=str(WORKTREE), capture_output=True, text=True, timeout=1200)
    if b.returncode != 0:
        return None, "build failed: " + " | ".join(b.stderr.strip().splitlines()[-2:])
    return WORKTREE / "target/debug/redis-server", "ok"


# ---- cache + render ---------------------------------------------------------

def config_sig():
    raw = json.dumps({"deny": DENY_TAGS, "timeout": TIMEOUT_S, "sections": [f for f, _ in SECTIONS], "contained": True}, sort_keys=True)
    return hashlib.sha1(raw.encode()).hexdigest()[:12]


def is_transient(point):
    note = (point.get("note") or "").lower()
    return (not point.get("build_ok")) and ("space" in note or "enospc" in note)


def load_cache():
    return json.loads(CACHE.read_text()) if CACHE.exists() else {}


def save_cache(cache):
    CACHE.write_text(json.dumps(cache, indent=2, sort_keys=True) + "\n")


def render_view(selected, cache):
    sig = config_sig()
    points = [cache[sha[:9]] for sha, _ in selected
              if sha[:9] in cache and cache[sha[:9]].get("config") == sig]
    points.sort(key=lambda p: p["iso"])
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": (
            "Upstream Valkey TCL suite (54 single-node unit files) replayed against the "
            "Rust server built at each sampled commit, run inside a size-capped disk image "
            "with per-file write limits. Same deny-tags as tcl-survey.py "
            "(needs:repl, needs:debug, external:skip), no --durable."
        ),
        "sections": [{"file": f, "label": lbl} for f, lbl in SECTIONS],
        "points": points,
    }
    OUT.write_text(json.dumps(payload, indent=2) + "\n")


# ---- main -------------------------------------------------------------------

def main():
    global IMAGE, MOUNT, BINDIR, UNIQUE, BASE_PORT
    ap = argparse.ArgumentParser()
    ap.add_argument("--last-days", type=int)
    ap.add_argument("--commits")
    ap.add_argument("--workers", type=int, default=1, help="Files to run concurrently (isolated sandboxes).")
    ap.add_argument("--sandbox", default="", help="Suffix for the image/mount, to run beside another instance.")
    ap.add_argument("--base-port", type=int, default=31000)
    ap.add_argument("--image-size", default="8g")
    ap.add_argument("--file-cap-mb", type=int, default=1024)
    ap.add_argument("--floor-drop-gb", type=float, default=15.0)
    ap.add_argument("--timeout", type=int, default=TIMEOUT_S)
    args = ap.parse_args()

    IMAGE = Path(f"/tmp/convsafe{args.sandbox}.sparseimage")
    MOUNT = Path(f"/Volumes/convsafe{args.sandbox}")
    BINDIR = MOUNT / "bin"
    UNIQUE = str(BINDIR / "valkey-server")
    BASE_PORT = args.base_port
    workers = max(1, args.workers)

    file_cap_blocks = args.file_cap_mb * 1024 * 1024 // 512
    start_free = free_gb(MAIN)
    sig = config_sig()

    build_mode = bool(args.last_days or args.commits)
    if args.commits:
        shas = [s.strip() for s in args.commits.split(",") if s.strip()]
        commits = [(git(["rev-parse", s]).stdout.strip(), git(["log", "-1", "--format=%cI", s]).stdout.strip()) for s in shas]
    elif args.last_days:
        commits = daily_commits()[-args.last_days:]
    else:
        commits = [(git(["rev-parse", "HEAD"]).stdout.strip(), git(["log", "-1", "--format=%cI"]).stdout.strip())]

    print(f"mode={'build' if build_mode else 'current-binary'} | {len(commits)} commit(s) | workers={workers} | sandbox={MOUNT.name} port={BASE_PORT} | free {start_free:.1f} GB | config {sig}", flush=True)

    wd = threading.Thread(target=watchdog, args=(start_free, args.floor_drop_gb), daemon=True)
    wd.start()

    cache = load_cache()
    try:
        setup_image(args.image_size, workers)
        print(f"mounted {MOUNT} (image free {free_gb(MOUNT):.1f} GB)\n", flush=True)
        if build_mode:
            ensure_worktree(commits[0][0])

        for i, (sha, iso) in enumerate(commits, 1):
            short = sha[:9]
            if abort.is_set():
                print("aborted by watchdog; stopping.", flush=True)
                break
            cached = cache.get(short)
            if build_mode and cached and cached.get("config") == sig and not is_transient(cached):
                tot = sum((v.get("passed") or 0) for v in cached.get("sections", {}).values())
                print(f"[{i}/{len(commits)}] {iso[:10]} {short} cached (passing={tot})", flush=True)
                continue

            if build_mode:
                if free_gb(MAIN) < args.floor_drop_gb + 5:
                    print(f"[{i}/{len(commits)}] {iso[:10]} {short} ABORT: low disk before build", flush=True)
                    break
                print(f"[{i}/{len(commits)}] {iso[:10]} {short} building...", flush=True)
                binary, msg = build_at(sha)
            else:
                binary, msg = (DEFAULT_BIN, "current") if DEFAULT_BIN.exists() else (None, "no binary")
                print(f"[{i}/{len(commits)}] {iso[:10]} {short} measuring existing binary (x{workers})...", flush=True)

            point = {"date": iso[:10], "iso": iso, "sha": short, "full_sha": sha, "build_ok": bool(binary), "note": msg, "config": sig, "sections": {}}
            if binary:
                point_binary(binary, workers)
                point["sections"] = survey(workers, file_cap_blocks, args.timeout)
                tot = sum((v.get("passed") or 0) for v in point["sections"].values())
                print(f"    -> {tot} passing | sys free {free_gb(MAIN):.1f} GB", flush=True)
            else:
                print(f"    SKIP ({msg})", flush=True)

            cache[short] = point
            if build_mode:
                save_cache(cache)
                render_view(commits, cache)
    finally:
        abort.set()
        kill_all()
        detach_image()
        if IMAGE.exists():
            IMAGE.unlink()
        if build_mode and WORKTREE.exists():
            git(["checkout", "--detach", "main"], cwd=WORKTREE)

    end_free = free_gb(MAIN)
    print(f"\nDONE | free {start_free:.1f} -> {end_free:.1f} GB (max drop {start_free-end_free:.2f} GB)", flush=True)
    if not build_mode:
        (MAIN / f"harness/oracle/safe-survey-result{args.sandbox}.json").write_text(json.dumps(cache, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
