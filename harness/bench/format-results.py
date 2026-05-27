#!/usr/bin/env python3
"""Format benchmark JSON artifacts as readable Markdown tables.

This is a reporting helper for local performance triage. With no arguments it
prints the latest default-suite-parts, pipeline-smoke, and json-doc-mix result
artifacts found under harness/bench/results.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = ROOT / "harness/bench/results"
LATEST_PROBES = ("default-suite-parts", "pipeline-smoke", "json-doc-mix")


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def latest_artifact(probe_id: str) -> Path | None:
    matches = sorted(RESULTS_DIR.glob(f"*-{probe_id}.json"))
    return matches[-1] if matches else None


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("|", "\\|").replace("\n", " ")


def fmt_ratio(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return ""
    return f"{value:.3f}x"


def fmt_rps(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return ""
    return f"{value:,.2f}"


def fmt_ms(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return ""
    return f"{value:.3f}"


def fmt_int(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return ""
    return f"{int(value):,}"


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    out = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        out.append("| " + " | ".join(row) + " |")
    return "\n".join(out)


def artifact_header(path: Path, data: dict[str, Any]) -> str:
    summary = data.get("summary") or {}
    params = data.get("parameters") or {}
    bits = [
        f"artifact `{rel(path)}`",
        f"status `{data.get('status', 'unknown')}`",
        f"commit `{data.get('commit', 'unknown')}`",
    ]
    if isinstance(summary.get("ok"), int) and isinstance(summary.get("total"), int):
        bits.append(f"ok `{summary['ok']}/{summary['total']}`")
    if isinstance(summary.get("median_ratio"), (int, float)):
        bits.append(f"median `{fmt_ratio(summary['median_ratio'])}`")
    if isinstance(summary.get("min_ratio"), (int, float)):
        bits.append(f"min `{fmt_ratio(summary['min_ratio'])}`")
    if params:
        selected = []
        for key in ("mode", "requests", "clients", "pipeline", "payload"):
            if key in params and params[key] is not None:
                selected.append(f"{key}={params[key]}")
        if selected:
            bits.append("params `" + ", ".join(selected) + "`")
    return "_Source: " + "; ".join(bits) + "_"


def render_default_suite(path: Path, data: dict[str, Any]) -> str:
    rows = []
    for row in data.get("rows", []):
        rows.append(
            [
                text(row.get("selector")),
                text(row.get("title") or row.get("command")),
                fmt_ratio(row.get("ratio")),
                fmt_rps(row.get("reference_rps")),
                fmt_rps(row.get("rust_rps")),
                fmt_ms(row.get("reference_p50_ms")),
                fmt_ms(row.get("rust_p50_ms")),
                fmt_ms(row.get("reference_p99_ms")),
                fmt_ms(row.get("rust_p99_ms")),
            ]
        )
    return "\n\n".join(
        [
            "### Default Suite Parts",
            artifact_header(path, data),
            markdown_table(
                [
                    "Selector",
                    "Command",
                    "Ratio",
                    "Valkey rps",
                    "Rust rps",
                    "Valkey p50 ms",
                    "Rust p50 ms",
                    "Valkey p99 ms",
                    "Rust p99 ms",
                ],
                rows,
            ),
        ]
    )


def render_pipeline_smoke(path: Path, data: dict[str, Any]) -> str:
    rows = []
    for row in data.get("rows", []):
        rows.append(
            [
                text(row.get("workload")),
                text(row.get("command")),
                fmt_int(row.get("pipeline")),
                fmt_int(row.get("requests")),
                fmt_int(row.get("clients")),
                fmt_int(row.get("payload")),
                fmt_ratio(row.get("ratio")),
                fmt_rps(row.get("reference_rps")),
                fmt_rps(row.get("rust_rps")),
                fmt_ms(row.get("reference_p99_ms")),
                fmt_ms(row.get("rust_p99_ms")),
            ]
        )
    return "\n\n".join(
        [
            "### Pipeline Smoke",
            artifact_header(path, data),
            markdown_table(
                [
                    "Workload",
                    "Command",
                    "P",
                    "Requests",
                    "Clients",
                    "Payload",
                    "Ratio",
                    "Valkey rps",
                    "Rust rps",
                    "Valkey p99 ms",
                    "Rust p99 ms",
                ],
                rows,
            ),
        ]
    )


def render_json_doc_mix(path: Path, data: dict[str, Any]) -> str:
    rows = []
    for row in data.get("rows", []):
        rows.append(
            [
                text(row.get("scenario")),
                text(row.get("description")),
                fmt_int(row.get("doc_bytes")),
                fmt_int(row.get("requests")),
                fmt_int(row.get("clients")),
                fmt_int(row.get("pipeline")),
                fmt_ratio(row.get("ratio")),
                fmt_rps(row.get("reference_rps")),
                fmt_rps(row.get("rust_rps")),
                fmt_ms(row.get("reference_p50_ms")),
                fmt_ms(row.get("rust_p50_ms")),
                fmt_ms(row.get("reference_p90_ms")),
                fmt_ms(row.get("rust_p90_ms")),
                fmt_ms(row.get("reference_p99_ms")),
                fmt_ms(row.get("rust_p99_ms")),
            ]
        )
    return "\n\n".join(
        [
            "### JSON Document Mix",
            artifact_header(path, data),
            markdown_table(
                [
                    "Scenario",
                    "Description",
                    "Doc bytes",
                    "Requests",
                    "Clients",
                    "P",
                    "Ratio",
                    "Valkey rps",
                    "Rust rps",
                    "Valkey p50 ms",
                    "Rust p50 ms",
                    "Valkey p90 ms",
                    "Rust p90 ms",
                    "Valkey p99 ms",
                    "Rust p99 ms",
                ],
                rows,
            ),
        ]
    )


def render_artifact(path: Path) -> str:
    data = load_json(path)
    probe_id = data.get("probe_id")
    if probe_id == "default-suite-parts":
        return render_default_suite(path, data)
    if probe_id == "pipeline-smoke":
        return render_pipeline_smoke(path, data)
    if probe_id == "json-doc-mix":
        return render_json_doc_mix(path, data)
    raise SystemExit(f"unsupported artifact type in {path}: probe_id={probe_id!r}")


def resolve_paths(args: argparse.Namespace) -> list[Path]:
    if args.paths:
        return [Path(path).resolve() for path in args.paths]

    probes = LATEST_PROBES if args.latest == "all" else tuple(args.latest.split(","))
    paths = []
    for probe_id in probes:
        path = latest_artifact(probe_id.strip())
        if path is None:
            raise SystemExit(f"no JSON artifact found for probe {probe_id!r}")
        paths.append(path)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="*",
        help="Explicit benchmark JSON artifacts to render. Defaults to latest artifacts.",
    )
    parser.add_argument(
        "--latest",
        default="all",
        help="Comma-separated probe IDs to render when no paths are given, or 'all'.",
    )
    args = parser.parse_args()

    rendered = [render_artifact(path) for path in resolve_paths(args)]
    print("\n\n".join(rendered))


if __name__ == "__main__":
    main()
