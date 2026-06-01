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
import shutil
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

# The exact launch flags live in one place — the bench probes' start_server()
# (harness/bench/default-suite-parts.py). The note links there instead of
# repeating the flags, so there is no copy to drift out of sync.
BENCH_CONFIG_NOTE = (
    "**Server config:** no `.conf` file — both servers are launched from explicit "
    "flags by the [bench harness](harness/bench/default-suite-parts.py), persistence "
    "off, otherwise stock defaults."
)
# Plain-text twin of BENCH_CONFIG_NOTE for the landing page (rendered via
# textContent, so no markdown/links). Same facts.
BENCH_CONFIG_NOTE_PLAIN = (
    "No .conf file: both servers launched from explicit flags by the bench harness "
    "(harness/bench/default-suite-parts.py), persistence off, otherwise stock defaults."
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
        toks = {k: v for k, _, v in (t.partition("=") for t in out.split() if "=" in t)}
        ver = toks.get("v")
        if ver:
            malloc = toks.get("malloc", "")
            alloc = " (jemalloc)" if malloc.startswith("jemalloc") else (" (libc malloc)" if malloc == "libc" else "")
            return f"Valkey {ver}{alloc}"
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


PERF_SVG = ROOT / "docs/perf.svg"
PERF_PNG = ROOT / "docs/perf.png"
PERF_LINK = "https://valdr.dev"
SVG_FONT = "ui-sans-serif, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"


def write_perf_png() -> None:
    """Rasterize docs/perf.svg to a 2x PNG. The README embeds the PNG (not the
    SVG) so it renders inline on GitHub reliably and the image links to the
    site instead of opening the raw file. Requires librsvg; this is a declared
    requirement of `make site-data`, not an optional step."""
    rsvg = shutil.which("rsvg-convert")
    if rsvg is None:
        raise SystemExit("rsvg-convert not found — install librsvg (`brew install librsvg`) to render docs/perf.png")
    subprocess.run([rsvg, "-z", "2", str(PERF_SVG), "-o", str(PERF_PNG)], check=True)


def _bucket_color(r: float) -> str:
    if r >= 1.2:
        return "#2e9e5b"
    if r >= 0.95:
        return "#8a9099"
    return "#d65b4a"


def render_perf_svg(payload: dict) -> str:
    """Dependency-free diverging bar chart of per-command throughput ratios,
    rendered from the same payload that feeds docs/perf-data.json. Bars right of
    the parity line = valdr faster. Committed as docs/perf.svg and embedded in
    the README and the landing page; regenerated by `make site-data`."""
    ref = payload["reference"]
    ranked = sorted(payload["default_suite"], key=lambda r: r["ratio"], reverse=True)
    lo, hi = 0.6, 1.7
    row_h = 20
    top = 64
    left_gutter = 130
    plot_x = left_gutter + 8
    plot_w = 576
    width = plot_x + plot_w + 46
    height = top + len(ranked) * row_h + 18

    def x(r: float) -> float:
        return plot_x + (min(max(r, lo), hi) - lo) / (hi - lo) * plot_w

    px = x(1.0)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" font-family="{SVG_FONT}">',
        f'<rect x="0.5" y="0.5" width="{width - 1}" height="{height - 1}" rx="9" fill="#ffffff" stroke="#e4e4e7"/>',
        f'<text x="22" y="28" font-size="15" font-weight="700" fill="#18181b">valdr ÷ {ref} — throughput ratio by command</text>',
        f'<text x="22" y="46" font-size="11" fill="#71717a">bars right of the line = valdr faster &#183; warmed, single-node, {payload["hardware"]}</text>',
        f'<line x1="{px:.1f}" y1="{top - 6}" x2="{px:.1f}" y2="{height - 14}" stroke="#3f3f46" stroke-width="1" stroke-dasharray="3 3"/>',
        f'<text x="{px:.1f}" y="{top - 10}" font-size="10" fill="#3f3f46" text-anchor="middle">parity 1.0×</text>',
    ]
    for i, r in enumerate(ranked):
        ratio = r["ratio"]
        yc = top + i * row_h
        bar_y = yc + 3
        bar_h = row_h - 7
        color = _bucket_color(ratio)
        xr = x(ratio)
        if ratio >= 1.0:
            bx, bw = px, xr - px
            vx, anchor = xr + 5, "start"
        else:
            bx, bw = xr, px - xr
            vx, anchor = xr - 5, "end"
        parts.append(
            f'<rect x="{bx:.1f}" y="{bar_y}" width="{max(bw, 0.5):.1f}" height="{bar_h}" rx="2" fill="{color}"/>'
        )
        parts.append(
            f'<text x="{left_gutter}" y="{bar_y + bar_h - 2}" font-size="11" fill="#3f3f46" text-anchor="end">{r["workload"].upper()}</text>'
        )
        parts.append(
            f'<text x="{vx:.1f}" y="{bar_y + bar_h - 2}" font-size="10.5" fill="#52525b" text-anchor="{anchor}">{ratio:.2f}×</text>'
        )
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


def render_readme_block(payload: dict) -> str:
    """Render the marker-delimited README perf section from the same payload
    that becomes docs/perf-data.json — the single source of truth the
    valdr.dev landing page also fetches."""
    ref = payload["reference"]
    lines = [
        f"{README_START} — auto-generated from docs/perf-data.json by `make site-data`; do not hand-edit between these markers -->",
        "",
        f"[![valdr vs Valkey throughput ratio by command](docs/perf.png)]({PERF_LINK})",
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
        f"- **Behind** (< 0.95×): {_names(behind)} — where the port's ",
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
    PERF_SVG.write_text(render_perf_svg(payload))
    write_perf_png()
    print(f"wrote {OUT.relative_to(ROOT)} — {len(pipeline_rows)} pipeline rows, {len(suite_rows)} suite rows")
    print(f"wrote {PERF_SVG.relative_to(ROOT)} + {PERF_PNG.relative_to(ROOT)} — {len(suite_rows)}-bar ratio chart")
    print(f"  source: matrix={matrix.name if matrix else '-'}  suite={suite.name if suite else '-'}  commit={commit}")
    update_readme(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
