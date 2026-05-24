#!/usr/bin/env python3
"""Render the single_node_core_v1 conformance dashboard.

This is a dashboard generator, not a test runner. It reads the latest full
TCL-suite inventory produced by tcl-suite-inventory.py and projects that
full upstream denominator onto the product envelope we currently care about:
single-node Redis/Valkey behavior without persistence restart/rewrite,
multi-node replication, cluster, Sentinel, TLS/io-threads, or RedisModule C ABI.

Outputs:
  harness/oracle/results/single-node-core-v1/latest.json
  harness/oracle/results/single-node-core-v1/latest.txt
  harness/oracle/results/single-node-core-v1/latest.html
"""

from __future__ import annotations

import datetime as dt
import html
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
INVENTORY = ROOT / "harness" / "oracle" / "results" / "tcl-suite-inventory" / "latest.json"
OUT_DIR = ROOT / "harness" / "oracle" / "results" / "single-node-core-v1"


CORE_FILES: dict[str, str] = {
    # Access control, auth, introspection, and operator-visible server state.
    "unit/acl-v2.tcl": "auth/config/introspection",
    "unit/acl.tcl": "auth/config/introspection",
    "unit/auth.tcl": "auth/config/introspection",
    "unit/commandlog.tcl": "auth/config/introspection",
    "unit/info-command.tcl": "auth/config/introspection",
    "unit/info.tcl": "auth/config/introspection",
    "unit/introspection-2.tcl": "auth/config/introspection",
    "unit/introspection.tcl": "auth/config/introspection",
    "unit/latency-monitor.tcl": "auth/config/introspection",
    "unit/limits.tcl": "auth/config/introspection",
    "unit/slowlog.tcl": "auth/config/introspection",

    # Protocol and client-facing connection semantics.
    "unit/networking.tcl": "protocol/client",
    "unit/obuf-limits.tcl": "protocol/client",
    "unit/protocol.tcl": "protocol/client",
    "unit/querybuf.tcl": "protocol/client",
    "unit/quit.tcl": "protocol/client",
    "unit/replybufsize.tcl": "protocol/client",
    "unit/tracking.tcl": "protocol/client",
    "unit/violations.tcl": "protocol/client",

    # Keyspace and lifetime behavior inside one server.
    "unit/client-eviction.tcl": "keyspace/memory",
    "unit/dump.tcl": "keyspace/memory",
    "unit/expire.tcl": "keyspace/memory",
    "unit/hashexpire.tcl": "keyspace/memory",
    "unit/keyspace.tcl": "keyspace/memory",
    "unit/lazyfree.tcl": "keyspace/memory",
    "unit/maxmemory.tcl": "keyspace/memory",
    "unit/memefficiency.tcl": "keyspace/memory",
    "unit/other.tcl": "keyspace/memory",
    "unit/pause.tcl": "keyspace/memory",
    "unit/scan.tcl": "keyspace/memory",
    "unit/shutdown.tcl": "keyspace/memory",

    # Transactions, blocking commands, pubsub, scripting, and functions.
    "unit/functions.tcl": "execution",
    "unit/multi.tcl": "execution",
    "unit/pubsub.tcl": "execution",
    "unit/pubsubshard.tcl": "execution",
    "unit/scripting.tcl": "execution",
    "unit/wait.tcl": "execution",

    # Data structures and command families.
    "unit/bitfield.tcl": "data-types",
    "unit/bitops.tcl": "data-types",
    "unit/geo.tcl": "data-types",
    "unit/hyperloglog.tcl": "data-types",
    "unit/sort.tcl": "data-types",
    "unit/type/hash.tcl": "data-types",
    "unit/type/incr.tcl": "data-types",
    "unit/type/list-2.tcl": "data-types",
    "unit/type/list-3.tcl": "data-types",
    "unit/type/list.tcl": "data-types",
    "unit/type/set.tcl": "data-types",
    "unit/type/stream-cgroups.tcl": "data-types",
    "unit/type/stream.tcl": "data-types",
    "unit/type/string.tcl": "data-types",
    "unit/type/zset.tcl": "data-types",
}


STATUS_LABEL = {
    "pass": "proved",
    "fail": "known-fail",
    "no-summary": "abort/no-summary",
    "timeout": "timeout",
    "not-swept": "not-swept",
    "missing": "missing",
}

STATUS_ORDER = {
    "fail": 0,
    "no-summary": 1,
    "timeout": 2,
    "not-swept": 3,
    "missing": 4,
    "pass": 5,
}


def bar(value: int, total: int, width: int = 34) -> str:
    if total <= 0:
        return "." * width
    filled = round((value / total) * width)
    return "#" * filled + "." * (width - filled)


def pct(value: int, total: int) -> str:
    if total <= 0:
        return "0.0%"
    return f"{value * 100.0 / total:.1f}%"


def load_inventory() -> dict[str, Any]:
    if not INVENTORY.exists():
        raise SystemExit(
            f"missing {INVENTORY.relative_to(ROOT)}; run "
            "python3 harness/oracle/tcl-suite-inventory.py first"
        )
    return json.loads(INVENTORY.read_text())


def normalize_status(item: dict[str, Any] | None) -> str:
    if item is None:
        return "missing"
    status = item.get("status")
    if status == "skipped-by-policy":
        return "not-swept"
    return str(status)


def frontier(item: dict[str, Any] | None) -> str:
    if item is None:
        return "not present in latest inventory"
    if item.get("status") == "pass":
        passed = item.get("passed")
        failed = item.get("failed")
        if passed is None:
            return "file completed without counted Tcl tests"
        return f"{passed} pass / {failed or 0} fail in latest survey"
    log = item.get("latest_log") or {}
    parts: list[str] = []
    if log.get("abort_test"):
        parts.append(str(log["abort_test"]))
    if log.get("exception"):
        parts.append(str(log["exception"]))
    for failure in log.get("failures") or []:
        parts.append(str(failure))
    if item.get("reason"):
        parts.append(str(item["reason"]))
    if log.get("timed_out"):
        parts.append("runner timed out")
    if not parts:
        parts.append("needs first generated survey")
    return "; ".join(parts[:3])


def classify_full_suite(file_name: str) -> str:
    if file_name in CORE_FILES:
        return "single_node_core_v1"
    if file_name in {"unit/aofrw.tcl"}:
        return "persistence_next"
    if file_name.startswith("integration/"):
        return "integration_next"
    if file_name.startswith("sentinel/"):
        return "sentinel_later"
    if file_name.startswith("unit/cluster/"):
        return "cluster_later"
    if file_name.startswith("unit/moduleapi/"):
        return "module_strategy_later"
    if file_name in {"unit/tls.tcl", "unit/io-threads.tcl", "unit/mptcp.tcl", "unit/oom-score-adj.tcl"}:
        return "platform_later"
    if file_name.startswith("helpers/") or file_name.startswith("support/") or file_name in {
        "instances.tcl",
        "test_helper.tcl",
    }:
        return "harness_files"
    if file_name == "unit/fuzzer.tcl":
        return "robustness_later"
    return "unclassified"


def build_model(inventory: dict[str, Any]) -> dict[str, Any]:
    files_by_name = {item["file"]: item for item in inventory["files"]}
    core_rows: list[dict[str, Any]] = []
    status_tests: Counter[str] = Counter()
    status_files: Counter[str] = Counter()
    group_tests: dict[str, Counter[str]] = defaultdict(Counter)
    group_files: dict[str, Counter[str]] = defaultdict(Counter)
    counted_pass = 0
    counted_fail = 0

    for file_name, group in sorted(CORE_FILES.items()):
        item = files_by_name.get(file_name)
        status = normalize_status(item)
        source_tests = int((item or {}).get("source_tests") or 0)
        passed = (item or {}).get("passed") or 0
        failed = (item or {}).get("failed") or 0
        counted_pass += int(passed)
        counted_fail += int(failed)
        status_tests[status] += source_tests
        status_files[status] += 1
        group_tests[group][status] += source_tests
        group_files[group][status] += 1
        core_rows.append(
            {
                "file": file_name,
                "group": group,
                "source_tests": source_tests,
                "status": status,
                "status_label": STATUS_LABEL.get(status, status),
                "passed": passed,
                "failed": failed,
                "frontier": frontier(item),
                "latest_log": ((item or {}).get("latest_log") or {}).get("log"),
            }
        )

    full_categories: dict[str, Counter[str]] = defaultdict(Counter)
    for item in inventory["files"]:
        category = classify_full_suite(item["file"])
        full_categories[category]["files"] += 1
        full_categories[category]["source_tests"] += int(item.get("source_tests") or 0)

    total_core_tests = sum(row["source_tests"] for row in core_rows)
    known_done = status_tests["pass"] + status_tests["fail"]
    unknown = status_tests["not-swept"] + status_tests["no-summary"] + status_tests["timeout"] + status_tests["missing"]
    model = {
        "schema_version": 1,
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "inventory_generated_at": inventory.get("generated_at"),
        "full_suite": inventory.get("full_suite"),
        "core": {
            "files": len(core_rows),
            "source_tests": total_core_tests,
            "counted_pass": counted_pass,
            "counted_fail": counted_fail,
            "status_tests": dict(status_tests),
            "status_files": dict(status_files),
            "known_done_tests": known_done,
            "unknown_tests": unknown,
            "pass_ratio_source_tests": (status_tests["pass"] / total_core_tests) if total_core_tests else 0.0,
        },
        "groups": {
            group: {
                "files": dict(group_files[group]),
                "source_tests": dict(group_tests[group]),
            }
            for group in sorted(group_tests)
        },
        "full_suite_categories": {
            name: dict(counter)
            for name, counter in sorted(full_categories.items())
        },
        "rows": sorted(
            core_rows,
            key=lambda row: (STATUS_ORDER.get(row["status"], 99), -row["source_tests"], row["file"]),
        ),
    }
    return model


def render_text(model: dict[str, Any]) -> str:
    core = model["core"]
    total = int(core["source_tests"])
    statuses = core["status_tests"]
    lines = [
        "single_node_core_v1 conformance dashboard",
        f"generated: {model['generated_at']}",
        f"inventory: {model.get('inventory_generated_at')}",
        "",
        f"full upstream TCL suite: {model['full_suite']['files']} files / {model['full_suite']['source_tests']} source test blocks",
        f"single_node_core_v1:     {core['files']} files / {total} source test blocks",
        f"raw counted core results: {core['counted_pass']} pass / {core['counted_fail']} fail",
        "",
        "Core source-test accounting:",
    ]
    for status in ("pass", "fail", "no-summary", "timeout", "not-swept", "missing"):
        value = int(statuses.get(status, 0))
        if value == 0 and status == "missing":
            continue
        label = STATUS_LABEL.get(status, status)
        lines.append(f"  {label:16} {bar(value, total)} {value:4d} / {total} ({pct(value, total)})")
    lines.extend(["", "By subsystem:"])
    for group, data in model["groups"].items():
        group_total = sum(int(v) for v in data["source_tests"].values())
        proved = int(data["source_tests"].get("pass", 0))
        fail = int(data["source_tests"].get("fail", 0))
        pending = group_total - proved - fail
        lines.append(
            f"  {group:24} {bar(proved, group_total, 24)} "
            f"proved={proved:4d} fail={fail:3d} pending={pending:4d} total={group_total:4d}"
        )
    lines.extend(["", "Next frontier files:"])
    frontier_rows = [row for row in model["rows"] if row["status"] != "pass"]
    for row in frontier_rows:
        lines.append(
            f"  [{row['status_label']:<16}] {row['file']:<30} "
            f"{row['source_tests']:4d} tests  {row['frontier']}"
        )
    if not frontier_rows:
        lines.append("  none")
    lines.extend(["", "Full suite categories:"])
    for name, values in model["full_suite_categories"].items():
        lines.append(f"  {name:24} {values['files']:3d} files / {values['source_tests']:4d} tests")
    lines.append("")
    return "\n".join(lines)


STYLE = """
:root {
  --bg:#0f1115; --panel:#171b22; --panel2:#11151b; --ink:#edf0f5;
  --muted:#9aa3b2; --line:#2b3340; --pass:#5ed17a; --fail:#ff796c;
  --warn:#f0c35b; --blue:#72a7ff; --cyan:#5bd7df;
}
* { box-sizing: border-box; }
body { margin:0; padding:28px; background:var(--bg); color:var(--ink);
  font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
h1 { margin:0 0 6px; font-size:28px; }
h2 { margin:26px 0 10px; font-size:18px; }
.muted { color:var(--muted); }
.grid { display:grid; gap:14px; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); margin:20px 0; }
.card { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:15px; }
.label { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.04em; }
.big { font-size:28px; font-weight:700; margin-top:4px; }
.bar { height:12px; background:#2a303a; border-radius:99px; overflow:hidden; display:flex; margin:8px 0 4px; }
.seg-pass { background:var(--pass); }
.seg-fail { background:var(--fail); }
.seg-nosummary { background:var(--warn); }
.seg-timeout { background:#c887ff; }
.seg-unswept { background:#596476; }
.legend { display:flex; flex-wrap:wrap; gap:12px; color:var(--muted); font-size:12px; }
.swatch { display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:4px; vertical-align:-1px; }
table { width:100%; border-collapse:collapse; background:var(--panel2); border:1px solid var(--line); }
th,td { padding:7px 9px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }
th { color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.04em; }
td.file { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; white-space:nowrap; }
.status { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-weight:700; white-space:nowrap; }
.status.pass { color:var(--pass); }
.status.fail { color:var(--fail); }
.status.no-summary { color:var(--warn); }
.status.timeout { color:#c887ff; }
.status.not-swept { color:#b8c1cf; }
.pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:2px 7px; color:var(--muted); font-size:12px; }
.subgrid { display:grid; gap:14px; grid-template-columns:1.1fr .9fr; align-items:start; }
@media (max-width: 900px) { .subgrid { grid-template-columns:1fr; } body { padding:16px; } }
"""


def status_widths(statuses: dict[str, int], total: int) -> str:
    if total <= 0:
        return ""
    pieces = []
    for status, css in (
        ("pass", "seg-pass"),
        ("fail", "seg-fail"),
        ("no-summary", "seg-nosummary"),
        ("timeout", "seg-timeout"),
        ("not-swept", "seg-unswept"),
    ):
        value = int(statuses.get(status, 0))
        if value:
            pieces.append(f'<div class="{css}" title="{html.escape(status)}: {value}" style="width:{value * 100 / total:.4f}%"></div>')
    return "".join(pieces)


def render_html(model: dict[str, Any]) -> str:
    core = model["core"]
    total = int(core["source_tests"])
    statuses = core["status_tests"]
    rows = []
    for row in model["rows"]:
        rows.append(
            "<tr>"
            f"<td class=\"file\">{html.escape(row['file'])}</td>"
            f"<td>{html.escape(row['group'])}</td>"
            f"<td>{row['source_tests']}</td>"
            f"<td class=\"status {html.escape(row['status'])}\">{html.escape(row['status_label'])}</td>"
            f"<td>{html.escape(row['frontier'])}</td>"
            "</tr>"
        )
    group_rows = []
    for group, data in model["groups"].items():
        group_total = sum(int(v) for v in data["source_tests"].values())
        proved = int(data["source_tests"].get("pass", 0))
        failed = int(data["source_tests"].get("fail", 0))
        pending = group_total - proved - failed
        group_rows.append(
            "<tr>"
            f"<td>{html.escape(group)}</td>"
            f"<td>{group_total}</td>"
            f"<td>{proved}</td>"
            f"<td>{failed}</td>"
            f"<td>{pending}</td>"
            f"<td><div class=\"bar\">{status_widths(data['source_tests'], group_total)}</div></td>"
            "</tr>"
        )
    category_rows = []
    for name, values in model["full_suite_categories"].items():
        category_rows.append(
            "<tr>"
            f"<td>{html.escape(name)}</td>"
            f"<td>{values['files']}</td>"
            f"<td>{values['source_tests']}</td>"
            "</tr>"
        )
    pass_src = int(statuses.get("pass", 0))
    fail_src = int(statuses.get("fail", 0))
    unknown_src = int(core["unknown_tests"])
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>single_node_core_v1 conformance</title>
<style>{STYLE}</style>
</head>
<body>
<h1>single_node_core_v1 conformance</h1>
<div class="muted">Generated {html.escape(model['generated_at'])}; inventory {html.escape(str(model.get('inventory_generated_at')))}.</div>

<div class="grid">
  <div class="card"><div class="label">Core denominator</div><div class="big">{core['files']} files / {total} tests</div></div>
  <div class="card"><div class="label">Proved source tests</div><div class="big">{pass_src} ({pct(pass_src, total)})</div></div>
  <div class="card"><div class="label">Known red source tests</div><div class="big">{fail_src} ({pct(fail_src, total)})</div></div>
  <div class="card"><div class="label">Unknown / unswept source tests</div><div class="big">{unknown_src} ({pct(unknown_src, total)})</div></div>
</div>

<div class="card">
  <div class="label">Core source-test accounting</div>
  <div class="bar">{status_widths(statuses, total)}</div>
  <div class="legend">
    <span><span class="swatch seg-pass"></span>proved {statuses.get('pass', 0)}</span>
    <span><span class="swatch seg-fail"></span>known-fail {statuses.get('fail', 0)}</span>
    <span><span class="swatch seg-nosummary"></span>abort/no-summary {statuses.get('no-summary', 0)}</span>
    <span><span class="swatch seg-timeout"></span>timeout {statuses.get('timeout', 0)}</span>
    <span><span class="swatch seg-unswept"></span>not-swept {statuses.get('not-swept', 0)}</span>
  </div>
  <p class="muted">This is source-test accounting: a passing file contributes all of its upstream <code>test</code> blocks to proved. Abort/no-summary and timeout files are intentionally not hidden.</p>
</div>

<div class="subgrid">
  <section>
    <h2>Subsystems</h2>
    <table><thead><tr><th>group</th><th>tests</th><th>proved</th><th>fail</th><th>pending</th><th>mix</th></tr></thead><tbody>
    {''.join(group_rows)}
    </tbody></table>
  </section>
  <section>
    <h2>Full-suite split</h2>
    <table><thead><tr><th>category</th><th>files</th><th>tests</th></tr></thead><tbody>
    {''.join(category_rows)}
    </tbody></table>
  </section>
</div>

<h2>Core file frontier</h2>
<table><thead><tr><th>file</th><th>group</th><th>tests</th><th>status</th><th>frontier / latest evidence</th></tr></thead><tbody>
{''.join(rows)}
</tbody></table>
</body>
</html>
"""


def main() -> int:
    inventory = load_inventory()
    model = build_model(inventory)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "latest.json").write_text(json.dumps(model, indent=2, sort_keys=True) + "\n")
    text = render_text(model)
    (OUT_DIR / "latest.txt").write_text(text)
    (OUT_DIR / "latest.html").write_text(render_html(model))
    print(text)
    print(f"wrote {OUT_DIR.relative_to(ROOT)}/latest.html")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
