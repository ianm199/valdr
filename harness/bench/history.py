#!/usr/bin/env python3
"""Build a static performance-history dashboard from harness evidence.

The benchmark runners already write commit-keyed measurements into
`harness/evidence/ledger.jsonl` and richer runner blobs into
`harness/evidence/runs/`. This script joins those two sources into a compact
JSON timeline and a self-contained HTML dashboard.
"""

from __future__ import annotations

import argparse
import csv
import html
import io
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
RESULTS_DIR = ROOT / "harness/bench/results"
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
        "label": "Curated matrix median",
        "runner_kind": "profile-matrix",
        "field": "median",
        "color": "#2f6fed",
    },
    {
        "id": "hotspots_median",
        "label": "Curated hotspots median",
        "runner_kind": "hotspots",
        "field": "median",
        "color": "#c16a1a",
    },
    {
        "id": "calltree_median",
        "label": "Curated calltree median",
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

RAW_SERIES_DEFS = [
    {
        "id": "raw_legacy_median",
        "label": "Legacy median",
        "runner_kind": "raw-legacy",
        "field": "median",
        "color": "#a13d63",
    },
    {
        "id": "raw_matrix_median",
        "label": "Raw matrix median (all TSVs)",
        "runner_kind": "raw-profile-matrix",
        "field": "median",
        "color": "#2f6fed",
    },
    {
        "id": "raw_hotspots_median",
        "label": "Raw hotspots median",
        "runner_kind": "raw-hotspots",
        "field": "median",
        "color": "#c16a1a",
    },
    {
        "id": "raw_calltree_median",
        "label": "Raw calltree median",
        "runner_kind": "raw-calltree",
        "field": "median",
        "color": "#0f8f68",
    },
]

RAW_KIND_LABEL = {
    "raw-legacy": "Legacy benchmark",
    "raw-profile-matrix": "Raw profile matrix",
    "raw-hotspots": "Raw hotspots",
    "raw-calltree": "Raw calltree",
}


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


def parse_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(str(value).replace("_", ""))
    except (TypeError, ValueError):
        return None


def compact_ts(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")
    except ValueError:
        return value


def repo_link(path: str) -> str:
    if not path:
        return ""
    if path.startswith("harness/"):
        return "../../" + path[len("harness/") :]
    return "../../../" + path


def parse_tsv_metadata(path: Path, lines: list[str]) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for line in lines:
        if not line.startswith("#"):
            continue
        body = line[1:].strip()
        if "\t" in body:
            key, value = body.split("\t", 1)
        elif ":" in body:
            key, value = body.split(":", 1)
        else:
            continue
        metadata[key.strip()] = value.strip()

    name_match = re.match(r"(?P<ts>\d{8}T\d{6}Z)-(?P<commit>[0-9a-f]+)", path.name)
    if name_match:
        metadata.setdefault("timestamp_utc", name_match.group("ts"))
        metadata.setdefault("commit", name_match.group("commit"))
    return metadata


def raw_kind_from_path(path: Path, header: str) -> str | None:
    if path.name.endswith("-profile-matrix.tsv") or header.startswith("profile\tcommand"):
        return "raw-profile-matrix"
    if path.name.endswith("-hotspots.tsv") or "sample_path" in header:
        return "raw-hotspots"
    if path.name.endswith("-calltree.tsv") or "profile_artifacts" in header:
        return "raw-calltree"
    if header.startswith("test\tupstream_rps"):
        return "raw-legacy"
    return None


def raw_key(row: dict[str, str], kind: str) -> str:
    if kind == "raw-profile-matrix":
        profile = row.get("profile", "")
        command = row.get("command", "")
        return f"{profile}/{command}" if profile and command else command
    if kind in {"raw-hotspots", "raw-calltree"}:
        return row.get("workload") or row.get("command") or ""
    return row.get("test") or row.get("command") or ""


def raw_p99(row: dict[str, str], kind: str) -> float | None:
    if kind == "raw-legacy":
        return parse_float(row.get("valkey_rs_p99_ms"))
    return parse_float(row.get("rust_p99_ms"))


def infer_raw_shape(rows: list[dict[str, str]], metadata: dict[str, str]) -> dict[str, int | None]:
    first = rows[0] if rows else {}
    return {
        "requests": parse_int(metadata.get("requests") or first.get("requests")),
        "clients": parse_int(metadata.get("clients") or first.get("clients")),
        "pipeline": parse_int(metadata.get("pipeline") or first.get("pipeline")),
        "payload": parse_int(metadata.get("payload_bytes") or first.get("payload")),
    }


def raw_point_from_tsv(path: Path) -> dict[str, Any] | None:
    lines = path.read_text(encoding="utf-8").splitlines()
    data_lines = [line for line in lines if line.strip() and not line.startswith("#")]
    if not data_lines:
        return None

    header = data_lines[0]
    kind = raw_kind_from_path(path, header)
    if kind is None:
        return None

    metadata = parse_tsv_metadata(path, lines)
    reader = csv.DictReader(io.StringIO("\n".join(data_lines)), delimiter="\t")
    ratios: dict[str, float] = {}
    p99_ms: dict[str, float] = {}
    clean_rows: list[dict[str, str]] = []
    for row in reader:
        key = raw_key(row, kind)
        if not key or key == "test":
            continue
        ratio = parse_float(row.get("ratio"))
        if ratio is None:
            continue
        ratios[key] = ratio
        p99 = raw_p99(row, kind)
        if p99 is not None:
            p99_ms[key] = p99
        clean_rows.append(row)

    if not ratios:
        return None

    values = list(ratios.values())
    rel = str(path.relative_to(ROOT))
    commit = metadata.get("commit", "")
    shape = infer_raw_shape(clean_rows, metadata)
    return {
        "ts": compact_ts(metadata.get("timestamp_utc", "")),
        "commit": commit,
        "commit_subject": commit_subject(commit),
        "commit_url": REMOTE_COMMIT_PREFIX + commit if commit else "",
        "runner_kind": kind,
        "runner_label": RAW_KIND_LABEL[kind],
        "summary": (
            f"{RAW_KIND_LABEL[kind]} median {statistics.median(values):.2f}x "
            f"min {min(values):.2f}x max {max(values):.2f}x"
        ),
        "source": rel,
        "source_url": repo_link(rel),
        "ratios": ratios,
        "p99_ms": p99_ms,
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
        "get_p1": ratios.get("core-p1/GET"),
        "get_p100": ratios.get("core-p100/GET") or ratios.get("get-p100") or ratios.get("GET"),
        "set_p100": ratios.get("core-p100/SET") or ratios.get("set-p100") or ratios.get("SET"),
        "incr_p100": ratios.get("core-p100/INCR") or ratios.get("incr-p100") or ratios.get("INCR"),
        "ping_p100": (
            ratios.get("core-p100/PING_MBULK")
            or ratios.get("ping-p100")
            or ratios.get("PING_MBULK")
        ),
        **shape,
    }


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
    raw_points = collect_raw_points()
    raw_series = build_series(raw_points, RAW_SERIES_DEFS)
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
    latest_result_mtime = max(
        (path.stat().st_mtime_ns for path in RESULTS_DIR.glob("*.tsv")),
        default=0,
    ) if RESULTS_DIR.exists() else 0
    signature = {
        "point_count": len(point_dicts),
        "raw_point_count": len(raw_points),
        "latest_point": point_dicts[-1]["evidence"] if point_dicts else "",
        "latest_raw": raw_points[-1]["source"] if raw_points else "",
        "ledger_mtime_ns": LEDGER.stat().st_mtime_ns if LEDGER.exists() else 0,
        "results_mtime_ns": latest_result_mtime,
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "project": "valkey-rs",
        "signature": signature,
        "point_count": len(point_dicts),
        "raw_point_count": len(raw_points),
        "series_defs": SERIES_DEFS,
        "raw_series_defs": RAW_SERIES_DEFS,
        "series": series,
        "raw_series": raw_series,
        "points": point_dicts,
        "raw_points": raw_points,
        "latest": latest,
        "annotations": annotations,
    }


def collect_raw_points() -> list[dict[str, Any]]:
    points = []
    if not RESULTS_DIR.exists():
        return points
    for path in sorted(RESULTS_DIR.glob("*.tsv")):
        point = raw_point_from_tsv(path)
        if point is not None:
            points.append(point)
    return sorted(points, key=lambda item: item["ts"])


def build_series(
    points: list[dict[str, Any]],
    series_defs: list[dict[str, str]] = SERIES_DEFS,
) -> dict[str, list[dict[str, Any]]]:
    series: dict[str, list[dict[str, Any]]] = {}
    for spec in series_defs:
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
                    "packet": point.get("packet") or point.get("source") or "",
                    "summary": point["summary"],
                    "evidence_url": point.get("evidence_url") or point.get("source_url") or "",
                    "commit_url": point["commit_url"],
                }
            )
        series[spec["id"]] = rows
    return series


def render_html(history: dict[str, Any]) -> str:
    data = json.dumps(history, sort_keys=True)
    latest = history.get("latest", {})
    raw_points = history.get("raw_points", [])
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
    if raw_points:
        first_matrix = next(
            (point for point in raw_points if point["runner_kind"] == "raw-profile-matrix"),
            None,
        )
        if first_matrix:
            latest_cards.append(
                f"""
                <article class="metric-card">
                  <div class="eyebrow">First matrix TSV</div>
                  <div class="metric">{first_matrix['median']:.2f}x</div>
                  <div class="subtle">{html.escape(first_matrix['commit'])} · {html.escape(first_matrix['source'])}</div>
                </article>
                """
            )
        worst_raw = min(raw_points, key=lambda point: point["median"])
        latest_raw = raw_points[-1]
        latest_cards.append(
            f"""
            <article class="metric-card">
              <div class="eyebrow">Raw TSV climb</div>
              <div class="metric">{worst_raw['median']:.2f}x &rarr; {latest_raw['median']:.2f}x</div>
              <div class="subtle">worst raw point {html.escape(worst_raw['commit'])} to latest {html.escape(latest_raw['commit'])}</div>
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
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }}
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
    .warn-note {{ background: #fff7eb; border-color: #f0c994; color: #3c2a12; }}
    .tooltip {{
      position: fixed;
      z-index: 20;
      max-width: 360px;
      pointer-events: none;
      background: rgba(24, 32, 47, .96);
      color: #fff;
      border-radius: 8px;
      padding: 10px 12px;
      box-shadow: 0 10px 24px rgba(20, 30, 45, .18);
      font-size: 12px;
      line-height: 1.35;
      opacity: 0;
      transform: translate(-50%, calc(-100% - 12px));
      transition: opacity .08s ease-out;
      overflow-wrap: anywhere;
    }}
    .tooltip strong {{ display: block; font-size: 13px; margin-bottom: 4px; }}
    .tooltip .muted {{ color: #c9d3e4; }}
    .tooltip.show {{ opacity: 1; }}
    .point-hit {{ fill: transparent; cursor: crosshair; }}
    .refresh-status {{ text-align: right; }}
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
    <p class="subtle refresh-status">Generated {html.escape(history['generated_at'])}<br>{history['point_count']} curated points · {history['raw_point_count']} raw TSV points<br><span id="refresh-status">Auto-refresh enabled</span></p>
  </header>

  <section class="grid">
    {''.join(latest_cards)}
  </section>

  <section class="panel">
    <h2>Granular Raw TSV History</h2>
    <p class="note warn-note">This view includes early one-off TSVs and intermediate profiling runs from <code>harness/bench/results/</code>. It is telemetry, not a controlled release claim, and it is the right place to see the original ~0.05x to ~0.10x baseline.</p>
    <div class="chart-wrap"><svg id="raw-chart" role="img" aria-label="Raw benchmark TSV throughput ratio over time"></svg></div>
    <div class="legend" id="raw-legend"></div>
  </section>

  <section class="panel">
    <h2>Curated Packet Evidence</h2>
    <p class="note">These points come from packet-completion evidence blobs, so this series starts later than the raw TSV history. 1.00x means valkey-rs matches upstream Valkey for the same workload and hardware.</p>
    <div class="chart-wrap"><svg id="median-chart" role="img" aria-label="Curated median throughput ratio over time"></svg></div>
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
    <h2>Raw TSV Points</h2>
    <table id="raw-table">
      <thead>
        <tr>
          <th>Time</th>
          <th>Kind</th>
          <th>Commit</th>
          <th>Shape</th>
          <th>Median</th>
          <th>Min</th>
          <th>GET</th>
          <th>PING</th>
          <th>File</th>
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
<div class="tooltip" id="chart-tooltip" role="tooltip"></div>

<script>
const HISTORY = {data};
const SERIES = Object.fromEntries(HISTORY.series_defs.map(s => [s.id, s]));
const RAW_SERIES = Object.fromEntries(HISTORY.raw_series_defs.map(s => [s.id, s]));
const INITIAL_SIGNATURE = JSON.stringify(HISTORY.signature || {{}});

function fmtRatio(value) {{
  return value == null ? "" : Number(value).toFixed(2) + "x";
}}

function shortTime(ts) {{
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts || "";
  return d.toLocaleTimeString([], {{hour: "2-digit", minute: "2-digit"}});
}}

function longTime(ts) {{
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts || "";
  return d.toLocaleString();
}}

function tooltipHtml(spec, point) {{
  const summary = point.summary ? `<div class="muted">${{point.summary}}</div>` : "";
  const target = point.packet || point.source || "";
  return `
    <strong>${{spec.label}} · ${{fmtRatio(point.value)}}</strong>
    <div><span class="muted">commit</span> ${{point.commit || ""}}</div>
    <div><span class="muted">time</span> ${{longTime(point.ts)}}</div>
    <div><span class="muted">item</span> ${{target}}</div>
    ${{summary}}
  `;
}}

function showTooltip(event, html) {{
  const tip = document.getElementById("chart-tooltip");
  tip.innerHTML = html;
  tip.style.left = `${{event.clientX}}px`;
  tip.style.top = `${{event.clientY}}px`;
  tip.classList.add("show");
}}

function moveTooltip(event) {{
  const tip = document.getElementById("chart-tooltip");
  tip.style.left = `${{event.clientX}}px`;
  tip.style.top = `${{event.clientY}}px`;
}}

function hideTooltip() {{
  document.getElementById("chart-tooltip").classList.remove("show");
}}

function drawChart(svgId, legendId, seriesIds, seriesData = HISTORY.series, seriesDefs = SERIES, pointSource = HISTORY.points) {{
  const svg = document.getElementById(svgId);
  const legend = document.getElementById(legendId);
  const width = 1120, height = 420;
  const margin = {{left: 58, right: 24, top: 24, bottom: 58}};
  svg.setAttribute("viewBox", `0 0 ${{width}} ${{height}}`);
  svg.innerHTML = "";
  legend.innerHTML = "";

  const all = seriesIds.flatMap(id => (seriesData[id] || []).map(p => ({{...p, seriesId: id}})));
  if (!all.length) return;
  const timestamps = [...new Set(pointSource.map(p => p.ts))];
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

  pointSource.forEach((point, idx) => {{
    if (idx % Math.ceil(pointSource.length / 8) !== 0 && idx !== pointSource.length - 1) return;
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
    const spec = seriesDefs[id];
    const points = seriesData[id] || [];
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

      const hit = document.createElementNS("http://www.w3.org/2000/svg", "circle");
      hit.setAttribute("class", "point-hit");
      hit.setAttribute("cx", x(p.ts));
      hit.setAttribute("cy", y(p.value));
      hit.setAttribute("r", 11);
      hit.addEventListener("mouseenter", event => showTooltip(event, tooltipHtml(spec, p)));
      hit.addEventListener("mousemove", moveTooltip);
      hit.addEventListener("mouseleave", hideTooltip);
      svg.appendChild(hit);
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

  const rawBody = document.querySelector("#raw-table tbody");
  rawBody.innerHTML = "";
  [...HISTORY.raw_points].reverse().forEach(point => {{
    const tr = document.createElement("tr");
    const shape = [
      point.requests ? `${{point.requests}} req` : "",
      point.clients ? `c=${{point.clients}}` : "",
      point.pipeline ? `p=${{point.pipeline}}` : "",
      point.payload ? `d=${{point.payload}}` : "",
    ].filter(Boolean).join(" · ");
    tr.innerHTML = `
      <td>${{shortTime(point.ts)}}</td>
      <td>${{point.runner_label}}</td>
      <td><a href="${{point.commit_url}}"><code>${{point.commit}}</code></a><div class="subtle">${{point.commit_subject || ""}}</div></td>
      <td>${{shape}}</td>
      <td>${{fmtRatio(point.median)}}</td>
      <td>${{fmtRatio(point.min)}}</td>
      <td>${{fmtRatio(point.get_p100)}}</td>
      <td>${{fmtRatio(point.ping_p100)}}</td>
      <td><a href="${{point.source_url}}">tsv</a></td>
    `;
    rawBody.appendChild(tr);
  }});
}}

const REMOTE_COMMIT_PREFIX = "{REMOTE_COMMIT_PREFIX}";
drawChart("median-chart", "median-legend", ["matrix_median", "hotspots_median", "calltree_median"]);
drawChart("get-chart", "get-legend", ["matrix_get_p1", "matrix_get_p100", "hotspots_get_p100", "calltree_get_p100"]);
drawChart("raw-chart", "raw-legend", ["raw_legacy_median", "raw_matrix_median", "raw_hotspots_median", "raw_calltree_median"], HISTORY.raw_series, RAW_SERIES, HISTORY.raw_points);
renderTables();

async function checkForRefresh() {{
  const status = document.getElementById("refresh-status");
  try {{
    const response = await fetch(`history.json?check=${{Date.now()}}`, {{cache: "no-store"}});
    const next = await response.json();
    const nextSignature = JSON.stringify(next.signature || {{}});
    if (nextSignature !== INITIAL_SIGNATURE) {{
      status.textContent = "New data found; reloading...";
      window.location.reload();
      return;
    }}
    status.textContent = "Auto-refresh checked " + new Date().toLocaleTimeString();
  }} catch (err) {{
    status.textContent = "Auto-refresh check failed";
  }}
}}
setInterval(checkForRefresh, 30000);
</script>
</body>
</html>
"""


def build(out_dir: Path, *, quiet: bool = False) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    history = build_history()
    (out_dir / "history.json").write_text(
        json.dumps(history, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (out_dir / "index.html").write_text(render_html(history), encoding="utf-8")
    if not quiet:
        print(f"wrote {out_dir / 'index.html'}")
        print(f"points: {history['point_count']}")


def needs_rebuild(out_dir: Path) -> bool:
    index = out_dir / "index.html"
    history_json = out_dir / "history.json"
    if not index.exists() or not history_json.exists():
        return True
    existing = load_json(history_json) or {}
    existing_signature = existing.get("signature")
    current_signature = build_history().get("signature")
    return existing_signature != current_signature


def serve(out_dir: Path, port: int) -> None:
    class Handler(SimpleHTTPRequestHandler):
        def do_GET(self) -> None:
            route = self.path.split("?", 1)[0]
            if route in {"/", "/index.html", "/history.json"} and needs_rebuild(out_dir):
                try:
                    build(out_dir, quiet=True)
                except Exception as err:  # noqa: BLE001 - keep serving old dashboard on rebuild failure.
                    print(f"dashboard rebuild failed: {err}")
            super().do_GET()

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
    if args.serve:
        if needs_rebuild(args.out_dir):
            build(args.out_dir)
        serve(args.out_dir, args.port)
    else:
        build(args.out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
