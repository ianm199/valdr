#!/usr/bin/env python3
"""Build a static performance-history dashboard from harness evidence.

The benchmark runners already write commit-keyed measurements into
`harness/evidence/ledger.jsonl` and richer runner blobs into
`harness/evidence/runs/`. This script joins those two sources into a compact
JSON timeline and a self-contained HTML dashboard.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import statistics
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
LEDGER = ROOT / "harness/evidence/ledger.jsonl"
DEFAULT_OUT = ROOT / "harness/bench/history"
REMOTE_COMMIT_PREFIX = "https://github.com/ianm199/valkey-rs/commit/"


RUNNER_KIND = {
    "bench-profile-matrix": "profile-matrix",
    "bench-profile-hotspots": "hotspots",
    "bench-profile-calltree": "calltree",
}

KIND_LABEL = {
    "profile-matrix": "Profile matrix",
    "hotspots": "Hotspots",
    "calltree": "Calltree",
}

SERIES_DEFS = [
    {
        "id": "matrix_median",
        "label": "Matrix median",
        "runner_kind": "profile-matrix",
        "field": "median",
        "color": "#2f6fed",
    },
    {
        "id": "hotspots_median",
        "label": "Hotspots median",
        "runner_kind": "hotspots",
        "field": "median",
        "color": "#c16a1a",
    },
    {
        "id": "calltree_median",
        "label": "Calltree median",
        "runner_kind": "calltree",
        "field": "median",
        "color": "#0f8f68",
    },
    {
        "id": "matrix_get_p1",
        "label": "Matrix GET p1",
        "runner_kind": "profile-matrix",
        "field": "get_p1",
        "color": "#7a4cc2",
    },
    {
        "id": "matrix_get_p100",
        "label": "Matrix GET p100",
        "runner_kind": "profile-matrix",
        "field": "get_p100",
        "color": "#d33f49",
    },
    {
        "id": "hotspots_get_p100",
        "label": "Hotspots GET p100",
        "runner_kind": "hotspots",
        "field": "get_p100",
        "color": "#8a6500",
    },
    {
        "id": "calltree_get_p100",
        "label": "Calltree GET p100",
        "runner_kind": "calltree",
        "field": "get_p100",
        "color": "#008c9e",
    },
]


@dataclass(frozen=True)
class Point:
    ts: str
    commit: str
    commit_subject: str
    packet: str
    runner: str
    runner_kind: str
    summary: str
    evidence: str
    ratios: dict[str, float]
    p99_ms: dict[str, float]
    artifacts: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        values = list(self.ratios.values())
        median = statistics.median(values) if values else None
        return {
            "ts": self.ts,
            "commit": self.commit,
            "commit_subject": self.commit_subject,
            "commit_url": REMOTE_COMMIT_PREFIX + self.commit if self.commit else "",
            "packet": self.packet,
            "runner": self.runner,
            "runner_kind": self.runner_kind,
            "runner_label": KIND_LABEL.get(self.runner_kind, self.runner_kind),
            "summary": self.summary,
            "evidence": self.evidence,
            "evidence_url": repo_link(self.evidence),
            "ratios": self.ratios,
            "p99_ms": self.p99_ms,
            "artifacts": [
                {**artifact, "url": repo_link(str(artifact.get("path", "")))}
                for artifact in self.artifacts
            ],
            "median": median,
            "min": min(values) if values else None,
            "max": max(values) if values else None,
            "get_p1": self.ratios.get("core-p1/GET"),
            "get_p100": self.ratios.get("core-p100/GET")
            or self.ratios.get("get-p100"),
            "set_p100": self.ratios.get("core-p100/SET")
            or self.ratios.get("set-p100"),
            "incr_p100": self.ratios.get("core-p100/INCR")
            or self.ratios.get("incr-p100"),
            "ping_p100": self.ratios.get("core-p100/PING_MBULK")
            or self.ratios.get("ping-p100"),
        }


def repo_link(path: str) -> str:
    if not path:
        return ""
    if path.startswith("harness/"):
        return "../../" + path[len("harness/") :]
    return "../../../" + path


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def load_ledger() -> list[dict[str, Any]]:
    rows = []
    for line in LEDGER.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def commit_subject(commit: str) -> str:
    if not commit:
        return ""
    try:
        return subprocess.check_output(
            ["git", "log", "-1", "--format=%s", commit],
            cwd=ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except subprocess.SubprocessError:
        return ""


def ratio_from_summary(summary: str, name: str) -> float | None:
    patterns = {
        "median": r"median\s+([0-9.]+)x",
        "min": r"min\s+([0-9.]+)x",
        "max": r"max\s+([0-9.]+)x",
        "get_p1": r"GET p1\s+([0-9.]+)x",
        "get_p100": r"GET p100\s+([0-9.]+)x",
    }
    match = re.search(patterns[name], summary)
    return float(match.group(1)) if match else None


def evidence_path(row: dict[str, Any]) -> Path | None:
    rel = row.get("evidence")
    if not isinstance(rel, str) or not rel:
        return None
    return ROOT / rel


def result_from_evidence(row: dict[str, Any]) -> dict[str, Any] | None:
    path = evidence_path(row)
    if path is None:
        return None
    blob = load_json(path)
    if not blob:
        return None
    result = blob.get("result")
    return result if isinstance(result, dict) else None


def collect_ratio_rows(result: dict[str, Any]) -> tuple[dict[str, float], dict[str, float]]:
    ratios: dict[str, float] = {}
    p99_ms: dict[str, float] = {}
    evidence = result.get("evidence", {})
    if not isinstance(evidence, dict):
        return ratios, p99_ms

    matrix_rows = evidence.get("ratios")
    if isinstance(matrix_rows, list):
        for item in matrix_rows:
            if not isinstance(item, dict):
                continue
            profile = str(item.get("profile", ""))
            command = str(item.get("command", ""))
            key = f"{profile}/{command}" if profile and command else ""
            if not key:
                continue
            ratio = item.get("ratio")
            if isinstance(ratio, (int, float)):
                ratios[key] = float(ratio)
            rust = item.get("rust")
            if isinstance(rust, dict) and isinstance(rust.get("p99_ms"), (int, float)):
                p99_ms[key] = float(rust["p99_ms"])

    profile_rows = evidence.get("rows")
    if isinstance(profile_rows, list):
        for item in profile_rows:
            if not isinstance(item, dict):
                continue
            key = str(item.get("workload") or item.get("command") or "")
            if not key:
                continue
            ratio = item.get("ratio")
            if isinstance(ratio, (int, float)):
                ratios[key] = float(ratio)
            p99 = item.get("rust_p99_ms")
            if isinstance(p99, (int, float)):
                p99_ms[key] = float(p99)

    return ratios, p99_ms


def point_from_completion(row: dict[str, Any]) -> Point | None:
    runner = row.get("runner")
    if runner not in RUNNER_KIND:
        return None
    result = result_from_evidence(row)
    if not result:
        return None
    ratios, p99_ms = collect_ratio_rows(result)
    summary = str(result.get("summary") or row.get("summary") or "")

    # If an older runner only gave a summary, keep the run visible as a point.
    if not ratios:
        median = ratio_from_summary(summary, "median")
        if median is not None:
            ratios["summary-median"] = median
        get_p1 = ratio_from_summary(summary, "get_p1")
        if get_p1 is not None:
            ratios["core-p1/GET"] = get_p1
        get_p100 = ratio_from_summary(summary, "get_p100")
        if get_p100 is not None:
            ratios["core-p100/GET"] = get_p100

    if not ratios:
        return None

    commit = str(row.get("commit") or result.get("evidence", {}).get("commit") or "")
    return Point(
        ts=str(row.get("ts") or ""),
        commit=commit,
        commit_subject=commit_subject(commit),
        packet=str(row.get("packet") or ""),
        runner=str(runner),
        runner_kind=RUNNER_KIND[str(runner)],
        summary=summary,
        evidence=str(row.get("evidence") or ""),
        ratios=ratios,
        p99_ms=p99_ms,
        artifacts=list(result.get("artifacts") or []),
    )


def build_history() -> dict[str, Any]:
    points_by_key: dict[tuple[str, str, str], Point] = {}
    for row in load_ledger():
        if row.get("kind") != "packet_completed":
            continue
        point = point_from_completion(row)
        if point is None:
            continue
        points_by_key[(point.packet, point.runner, point.evidence)] = point

    points = sorted(points_by_key.values(), key=lambda item: item.ts)
    point_dicts = [point.to_dict() for point in points]
    series = build_series(point_dicts)
    latest = {}
    for point in point_dicts:
        latest[point["runner_kind"]] = point

    annotations = [
        {
            "ts": row.get("ts"),
            "commit": row.get("commit"),
            "commit_subject": commit_subject(str(row.get("commit") or "")),
            "packet": row.get("packet"),
            "role": row.get("role"),
            "summary": row.get("summary") or row.get("metric"),
        }
        for row in load_ledger()
        if row.get("kind") == "packet_completed"
        and row.get("role") in {"architect", "perf-fixer", "translator"}
    ]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "project": "valkey-rs",
        "point_count": len(point_dicts),
        "series_defs": SERIES_DEFS,
        "series": series,
        "points": point_dicts,
        "latest": latest,
        "annotations": annotations,
    }


def build_series(points: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    series: dict[str, list[dict[str, Any]]] = {}
    for spec in SERIES_DEFS:
        rows = []
        for idx, point in enumerate(points):
            if point["runner_kind"] != spec["runner_kind"]:
                continue
            value = point.get(spec["field"])
            if value is None:
                continue
            rows.append(
                {
                    "idx": idx,
                    "ts": point["ts"],
                    "value": value,
                    "commit": point["commit"],
                    "packet": point["packet"],
                    "summary": point["summary"],
                    "evidence_url": point["evidence_url"],
                    "commit_url": point["commit_url"],
                }
            )
        series[spec["id"]] = rows
    return series


def render_html(history: dict[str, Any]) -> str:
    data = json.dumps(history, sort_keys=True)
    latest = history.get("latest", {})
    latest_cards = []
    for key in ("profile-matrix", "hotspots", "calltree"):
        point = latest.get(key)
        if not point:
            continue
        latest_cards.append(
            f"""
            <article class="metric-card">
              <div class="eyebrow">{html.escape(point['runner_label'])}</div>
              <div class="metric">{point['median']:.2f}x</div>
              <div class="subtle">{html.escape(point['commit'])} - {html.escape(point['packet'])}</div>
            </article>
            """
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>valkey-rs Performance History</title>
  <style>
    :root {{
      --bg: #f7f8fb;
      --panel: #ffffff;
      --text: #18202f;
      --muted: #5e6878;
      --line: #d8deea;
      --accent: #2f6fed;
      --good: #0f8f68;
      --warn: #c16a1a;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: var(--bg); color: var(--text); }}
    main {{ max-width: 1440px; margin: 0 auto; padding: 28px; }}
    header {{ display: flex; justify-content: space-between; gap: 24px; align-items: flex-start; margin-bottom: 24px; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; letter-spacing: 0; }}
    h2 {{ margin: 0 0 12px; font-size: 18px; letter-spacing: 0; }}
    p {{ margin: 0; color: var(--muted); line-height: 1.5; }}
    a {{ color: var(--accent); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .grid {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }}
    .metric-card, .panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 8px; }}
    .metric-card {{ padding: 16px; }}
    .eyebrow {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }}
    .metric {{ margin-top: 4px; font-size: 32px; font-weight: 700; }}
    .subtle {{ color: var(--muted); font-size: 12px; margin-top: 5px; overflow-wrap: anywhere; }}
    .panel {{ padding: 18px; margin-top: 14px; }}
    .chart-wrap {{ width: 100%; overflow-x: auto; }}
    svg {{ display: block; width: 100%; min-width: 880px; height: 420px; }}
    .axis {{ stroke: #9aa6bb; stroke-width: 1; }}
    .gridline {{ stroke: #e9edf5; stroke-width: 1; }}
    .series-line {{ fill: none; stroke-width: 2.5; }}
    .point {{ stroke: #fff; stroke-width: 1.5; }}
    .legend {{ display: flex; flex-wrap: wrap; gap: 10px 18px; margin: 12px 0 0; }}
    .legend-item {{ display: inline-flex; align-items: center; gap: 7px; color: var(--muted); font-size: 13px; }}
    .swatch {{ width: 11px; height: 11px; border-radius: 999px; display: inline-block; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ text-align: left; border-bottom: 1px solid var(--line); padding: 9px 8px; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 600; }}
    code {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }}
    .note {{ background: #eef4ff; border: 1px solid #cbdcff; color: #23314d; border-radius: 8px; padding: 12px 14px; }}
    @media (max-width: 900px) {{
      main {{ padding: 18px; }}
      header {{ display: block; }}
      .grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
<main>
  <header>
    <div>
      <h1>valkey-rs Performance History</h1>
      <p>Commit-keyed benchmark trajectory generated from <code>harness/evidence/ledger.jsonl</code> and runner evidence blobs.</p>
    </div>
    <p class="subtle">Generated {html.escape(history['generated_at'])}<br>{history['point_count']} benchmark points</p>
  </header>

  <section class="grid">
    {''.join(latest_cards)}
  </section>

  <section class="panel">
    <h2>Median Throughput Ratio</h2>
    <p class="note">1.00x means valkey-rs matches upstream Valkey for the same workload and hardware. Higher is better.</p>
    <div class="chart-wrap"><svg id="median-chart" role="img" aria-label="Median throughput ratio over time"></svg></div>
    <div class="legend" id="median-legend"></div>
  </section>

  <section class="panel">
    <h2>GET Signals</h2>
    <p>Tracks the GET-specific rows that kept showing up in our architecture discussions.</p>
    <div class="chart-wrap"><svg id="get-chart" role="img" aria-label="GET throughput ratio over time"></svg></div>
    <div class="legend" id="get-legend"></div>
  </section>

  <section class="panel">
    <h2>Benchmark Points</h2>
    <table id="points-table">
      <thead>
        <tr>
          <th>Time</th>
          <th>Runner</th>
          <th>Commit</th>
          <th>Packet</th>
          <th>Median</th>
          <th>GET p100</th>
          <th>Evidence</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </section>

  <section class="panel">
    <h2>Architecture Annotations</h2>
    <table id="annotations-table">
      <thead>
        <tr>
          <th>Time</th>
          <th>Role</th>
          <th>Commit</th>
          <th>Packet</th>
          <th>Summary</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </section>
</main>

<script>
const HISTORY = {data};
const SERIES = Object.fromEntries(HISTORY.series_defs.map(s => [s.id, s]));

function fmtRatio(value) {{
  return value == null ? "" : Number(value).toFixed(2) + "x";
}}

function shortTime(ts) {{
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts || "";
  return d.toLocaleTimeString([], {{hour: "2-digit", minute: "2-digit"}});
}}

function drawChart(svgId, legendId, seriesIds) {{
  const svg = document.getElementById(svgId);
  const legend = document.getElementById(legendId);
  const width = 1120, height = 420;
  const margin = {{left: 58, right: 24, top: 24, bottom: 58}};
  svg.setAttribute("viewBox", `0 0 ${{width}} ${{height}}`);
  svg.innerHTML = "";
  legend.innerHTML = "";

  const all = seriesIds.flatMap(id => (HISTORY.series[id] || []).map(p => ({{...p, seriesId: id}})));
  if (!all.length) return;
  const timestamps = [...new Set(HISTORY.points.map(p => p.ts))];
  const xByTs = new Map(timestamps.map((ts, idx) => [ts, idx]));
  const maxIndex = Math.max(1, timestamps.length - 1);
  const values = all.map(p => p.value);
  const yMax = Math.max(1.5, Math.ceil(Math.max(...values) * 10 + 1) / 10);
  const yMin = 0;
  const chartW = width - margin.left - margin.right;
  const chartH = height - margin.top - margin.bottom;
  const x = ts => margin.left + (xByTs.get(ts) || 0) / maxIndex * chartW;
  const y = value => margin.top + (yMax - value) / (yMax - yMin) * chartH;

  for (let tick = 0; tick <= yMax + 0.0001; tick += 0.25) {{
    const yy = y(tick);
    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("x1", margin.left);
    line.setAttribute("x2", width - margin.right);
    line.setAttribute("y1", yy);
    line.setAttribute("y2", yy);
    line.setAttribute("class", "gridline");
    svg.appendChild(line);
    const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
    text.setAttribute("x", margin.left - 10);
    text.setAttribute("y", yy + 4);
    text.setAttribute("text-anchor", "end");
    text.setAttribute("font-size", "11");
    text.setAttribute("fill", "#5e6878");
    text.textContent = tick.toFixed(2) + "x";
    svg.appendChild(text);
  }}

  const xAxis = document.createElementNS("http://www.w3.org/2000/svg", "line");
  xAxis.setAttribute("x1", margin.left);
  xAxis.setAttribute("x2", width - margin.right);
  xAxis.setAttribute("y1", height - margin.bottom);
  xAxis.setAttribute("y2", height - margin.bottom);
  xAxis.setAttribute("class", "axis");
  svg.appendChild(xAxis);

  HISTORY.points.forEach((point, idx) => {{
    if (idx % Math.ceil(HISTORY.points.length / 8) !== 0 && idx !== HISTORY.points.length - 1) return;
    const tx = x(point.ts);
    const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
    text.setAttribute("x", tx);
    text.setAttribute("y", height - 20);
    text.setAttribute("text-anchor", "end");
    text.setAttribute("font-size", "11");
    text.setAttribute("fill", "#5e6878");
    text.setAttribute("transform", `rotate(-35 ${{tx}} ${{height - 20}})`);
    text.textContent = point.commit;
    svg.appendChild(text);
  }});

  for (const id of seriesIds) {{
    const spec = SERIES[id];
    const points = HISTORY.series[id] || [];
    if (!points.length) continue;
    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("class", "series-line");
    path.setAttribute("stroke", spec.color);
    path.setAttribute("d", points.map((p, idx) => `${{idx ? "L" : "M"}} ${{x(p.ts).toFixed(1)}} ${{y(p.value).toFixed(1)}}`).join(" "));
    svg.appendChild(path);

    points.forEach(p => {{
      const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
      circle.setAttribute("class", "point");
      circle.setAttribute("cx", x(p.ts));
      circle.setAttribute("cy", y(p.value));
      circle.setAttribute("r", 4);
      circle.setAttribute("fill", spec.color);
      const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
      title.textContent = `${{spec.label}}\\n${{p.commit}}\\n${{p.packet}}\\n${{fmtRatio(p.value)}}`;
      circle.appendChild(title);
      svg.appendChild(circle);
    }});

    const item = document.createElement("span");
    item.className = "legend-item";
    item.innerHTML = `<span class="swatch" style="background:${{spec.color}}"></span>${{spec.label}}`;
    legend.appendChild(item);
  }}
}}

function renderTables() {{
  const tbody = document.querySelector("#points-table tbody");
  tbody.innerHTML = "";
  [...HISTORY.points].reverse().forEach(point => {{
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${{shortTime(point.ts)}}</td>
      <td>${{point.runner_label}}</td>
      <td><a href="${{point.commit_url}}"><code>${{point.commit}}</code></a><div class="subtle">${{point.commit_subject || ""}}</div></td>
      <td><code>${{point.packet}}</code></td>
      <td>${{fmtRatio(point.median)}}</td>
      <td>${{fmtRatio(point.get_p100)}}</td>
      <td><a href="${{point.evidence_url}}">evidence</a></td>
    `;
    tbody.appendChild(tr);
  }});

  const atbody = document.querySelector("#annotations-table tbody");
  atbody.innerHTML = "";
  [...HISTORY.annotations].reverse().slice(0, 24).forEach(row => {{
    const commitUrl = row.commit ? `${{REMOTE_COMMIT_PREFIX}}${{row.commit}}` : "";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${{shortTime(row.ts)}}</td>
      <td>${{row.role || ""}}</td>
      <td>${{row.commit ? `<a href="${{commitUrl}}"><code>${{row.commit}}</code></a>` : ""}}<div class="subtle">${{row.commit_subject || ""}}</div></td>
      <td><code>${{row.packet || ""}}</code></td>
      <td>${{row.summary || ""}}</td>
    `;
    atbody.appendChild(tr);
  }});
}}

const REMOTE_COMMIT_PREFIX = "{REMOTE_COMMIT_PREFIX}";
drawChart("median-chart", "median-legend", ["matrix_median", "hotspots_median", "calltree_median"]);
drawChart("get-chart", "get-legend", ["matrix_get_p1", "matrix_get_p100", "hotspots_get_p100", "calltree_get_p100"]);
renderTables();
</script>
</body>
</html>
"""


def build(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    history = build_history()
    (out_dir / "history.json").write_text(
        json.dumps(history, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (out_dir / "index.html").write_text(render_html(history), encoding="utf-8")
    print(f"wrote {out_dir / 'index.html'}")
    print(f"points: {history['point_count']}")


def serve(out_dir: Path, port: int) -> None:
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, directory=str(out_dir), **kwargs)

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"serving http://127.0.0.1:{port}/")
    server.serve_forever()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--port", type=int, default=8022)
    args = parser.parse_args()
    build(args.out_dir)
    if args.serve:
        serve(args.out_dir, args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
