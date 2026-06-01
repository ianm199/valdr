#!/usr/bin/env python3
"""Generate docs/perf-data.json (and the README perf section) from the latest
local benchmark artifacts.

There is ONE source of truth for performance numbers: docs/perf-data.json. The
landing page (docs/index.html) fetches it at runtime; the README's Performance
section is rewritten from it by this script between the <!-- PERF:START --> and
<!-- PERF:END --> markers. Neither is ever hand-edited. This script is the "job"
that refreshes both:

    make bench-release      # produce fresh suite + matrix artifacts (local)
    make site-data          # regenerate docs/perf-data.json + README from them
    git commit docs/perf-data.json README.md   # Pages redeploys on push to docs/**

Source artifacts live under harness/bench/results/ (gitignored, local-only):
  - the newest *-profile-matrix.tsv      -> pipeline-depth table (p=1/16/100)
  - the newest full *-default-suite-parts.json (>= 15 rows) -> per-command table

`function_load` is excluded on purpose: its ratio is a benchmark artifact (our
idempotent-reload fast path vs Valkey's recompile of an identical library), not
a meaningful throughput number.
"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "harness/bench/results"
OUT = ROOT / "docs/perf-data.json"
README = ROOT / "README.md"
README_START = "<!-- PERF:START"
README_END = "<!-- PERF:END -->"

PIPELINE_PROFILES = {"core-p1": "p=1", "core-p16": "p=16", "core-p100": "p=100"}
PIPELINE_COMMANDS = ["GET", "SET", "PING_MBULK", "INCR"]
EXCLUDE_SELECTORS = {"function_load"}

# Both servers are launched from explicit CLI flags by the bench probes
# (pipeline-smoke.py / default-suite-parts.py start_server) — NO .conf file is
# involved. Persistence is off on both sides; everything else is stock defaults.
# Keep this in sync with those start_server() flag lists.
BENCH_CONFIG_NOTE = (
    "**Server config:** no `.conf` file — both servers are launched from explicit "
    "flags, persistence off, everything else stock defaults. Valkey: "
    "`--save \"\" --appendonly no --daemonize no --loglevel warning`. Valdr: "
    "`--rdb-disabled --appendonly no`. Both bound to `127.0.0.1`."
)
# Plain-text twin of BENCH_CONFIG_NOTE for the landing page (rendered via
# textContent, so no markdown). Same facts, no backticks.
BENCH_CONFIG_NOTE_PLAIN = (
    "No .conf file: both servers launched from explicit flags, persistence off, "
    "everything else stock defaults. Valkey: --save \"\" --appendonly no "
    "--daemonize no --loglevel warning. Valdr: --rdb-disabled --appendonly no. "
    "Both bound to 127.0.0.1."
)


def newest(glob: str) -> Path | None:
    files = sorted(RESULTS.glob(glob))
    return files[-1] if files else None


def newest_full_suite(min_rows: int = 15) -> Path | None:
    best = None
    for f in sorted(RESULTS.glob("*-default-suite-parts.json")):
        try:
            rows = json.loads(f.read_text())["rows"]
        except (json.JSONDecodeError, KeyError, OSError):
            continue
        if len(rows) >= min_rows:
            best = f
    return best


def reference_version() -> str:
    binary = ROOT / "reference/valkey/src/valkey-server"
    try:
        out = subprocess.check_output([str(binary), "--version"], text=True, stderr=subprocess.DEVNULL)
        for tok in out.split():
            if tok.startswith("v="):
                return f"Valkey {tok[2:]}"
    except (OSError, subprocess.CalledProcessError):
        pass
    return "Valkey (reference)"


def stamp_and_commit(path: Path) -> tuple[str, str]:
    name = path.name
    stamp = name.split("-")[0]
    commit = name.split("-")[1] if "-" in name else "unknown"
    try:
        iso = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        iso = stamp
    return iso, commit


def parse_matrix(path: Path) -> tuple[list[dict], dict]:
    meta = {}
    rows = []
    header = None
    for line in path.read_text().splitlines():
        if line.startswith("#"):
            parts = line[1:].strip().split("\t")
            if len(parts) == 2:
                meta[parts[0]] = parts[1]
            continue
        cells = line.split("\t")
        if header is None:
            header = cells
            continue
        r = dict(zip(header, cells))
        if r.get("profile") in PIPELINE_PROFILES and r.get("command") in PIPELINE_COMMANDS:
            label = "PING" if r["command"] == "PING_MBULK" else r["command"]
            rows.append(
                {
                    "workload": f"{label} {PIPELINE_PROFILES[r['profile']]}",
                    "valdr_rps": round(float(r["rust_rps"])),
                    "valkey_rps": round(float(r["reference_rps"])),
                    "ratio": round(float(r["ratio"]), 3),
                }
            )
    order = {p: i for i, p in enumerate(["p=1", "p=16", "p=100"])}
    cmd_order = {c: i for i, c in enumerate(["GET", "PING", "SET", "INCR"])}
    rows.sort(key=lambda x: (cmd_order.get(x["workload"].split()[0], 9), order.get(x["workload"].split()[1], 9)))
    return rows, meta


def parse_suite(path: Path) -> list[dict]:
    data = json.loads(path.read_text())
    out = []
    for r in data["rows"]:
        if r.get("selector") in EXCLUDE_SELECTORS:
            continue
        out.append(
            {
                "workload": r.get("selector") or r.get("command"),
                "valdr_rps": round(float(r["rust_rps"])),
                "valkey_rps": round(float(r["reference_rps"])),
                "ratio": round(float(r["ratio"]), 3),
            }
        )
    return out


def _rps(n: int) -> str:
    return f"{n:,}"


def _ratio(r: float) -> str:
    cell = f"{r:.3f}×"
    return f"**{cell}**" if r >= 1.3 else cell


def render_readme_block(payload: dict) -> str:
    """Render the marker-delimited README perf section from the same payload
    that becomes docs/perf-data.json — the single source of truth the
    valdr.dev landing page also fetches."""
    ref = payload["reference"]
    lines = [
        f"{README_START} — auto-generated from docs/perf-data.json by `make site-data`; do not hand-edit between these markers -->",
        "",
        f"Latest warmed local run: Valdr (`{payload['source_commit']}`) vs "
        f"**{ref}**, measured {payload['measured_utc']} on {payload['hardware']}. "
        "These tables and the [valdr.dev](https://valdr.dev) landing page both "
        "render `docs/perf-data.json` — one source of truth, no hand-typed numbers. "
        "Ratio = valdr_rps / valkey_rps; >1.00 = Valdr is faster. "
        "`function_load` is excluded (its ratio is a reload-fast-path artifact, not throughput).",
        "",
        BENCH_CONFIG_NOTE,
        "",
        "### Per-command (default `valkey-benchmark` suite)",
        "",
        f"| Command | Valdr rps | {ref} rps | Ratio |",
        "|---|---:|---:|---:|",
    ]
    for r in payload["default_suite"]:
        lines.append(
            f"| {r['workload'].upper()} | {_rps(r['valdr_rps'])} | {_rps(r['valkey_rps'])} | {_ratio(r['ratio'])} |"
        )

    wins = [r["workload"] for r in payload["default_suite"] if r["ratio"] >= 1.2]
    parity = [r["workload"] for r in payload["default_suite"] if 0.95 <= r["ratio"] < 1.2]
    behind = [r["workload"] for r in payload["default_suite"] if r["ratio"] < 0.95]

    def _names(xs: list[str]) -> str:
        return ", ".join(f"`{x}`" for x in xs) if xs else "—"

    lines += [
        "",
        f"- **Wins** (ratio ≥ 1.2×): {_names(wins)}.",
        f"- **Parity** (0.95×–1.2×): {_names(parity)}.",
        f"- **Behind** (< 0.95×): {_names(behind)} — where the port's "
        "oracle-gated behavioral fidelity currently extracts a perf cost not yet paid down.",
        "",
        "### Pipeline-depth curve (GET/SET/PING/INCR at p=1/16/100)",
        "",
        f"| Workload | Valdr rps | {ref} rps | Ratio |",
        "|---|---:|---:|---:|",
    ]
    for r in payload["pipeline_depth"]:
        lines.append(
            f"| {r['workload']} | {_rps(r['valdr_rps'])} | {_rps(r['valkey_rps'])} | {_ratio(r['ratio'])} |"
        )
    lines += ["", README_END]
    return "\n".join(lines)


def update_readme(payload: dict) -> bool:
    text = README.read_text()
    start = text.find(README_START)
    end = text.find(README_END)
    if start == -1 or end == -1:
        print(f"  README: markers not found ({README_START} … {README_END}) — skipped")
        return False
    new = text[:start] + render_readme_block(payload) + text[end + len(README_END):]
    if new == text:
        print("  README: already up to date")
        return False
    README.write_text(new)
    print(f"  README: refreshed perf section ({README.relative_to(ROOT)})")
    return True


def main() -> int:
    matrix = newest("*-profile-matrix.tsv")
    suite = newest_full_suite()
    if matrix is None and suite is None:
        raise SystemExit("no benchmark artifacts found under harness/bench/results/ — run `make bench-release` first")

    pipeline_rows, meta = parse_matrix(matrix) if matrix else ([], {})
    suite_rows = parse_suite(suite) if suite else []

    src = suite or matrix
    assert src is not None
    measured_iso, commit = stamp_and_commit(src)

    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "measured_utc": measured_iso,
        "source_commit": commit,
        "reference": reference_version(),
        "hardware": meta.get("cpu", "Apple M3 Max"),
        "note": "Single-node, single-threaded, same host, warmed. Ratio = valdr_rps / valkey_rps; >1.00 = Valdr is faster. function_load excluded (benchmark artifact).",
        "config_note": BENCH_CONFIG_NOTE_PLAIN,
        "pipeline_depth": pipeline_rows,
        "default_suite": suite_rows,
    }
    OUT.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"wrote {OUT.relative_to(ROOT)} — {len(pipeline_rows)} pipeline rows, {len(suite_rows)} suite rows")
    print(f"  source: matrix={matrix.name if matrix else '-'}  suite={suite.name if suite else '-'}  commit={commit}")
    update_readme(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
