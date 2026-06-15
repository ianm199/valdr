#!/usr/bin/env python3
"""Report structural maintainability hotspots for Valdr compatibility crates.

This is intentionally lightweight: it uses text scans rather than a Rust parser
so it can run anywhere `python3` is available and serve as a fast first pass
before choosing a refactor packet.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


DEFAULT_ROOTS = [
    "crates/redis-commands/src",
    "crates/redis-commands/tests",
    "crates/redis-core/src",
    "crates/redis-server/src",
    "crates/redis-server/tests",
    "docs",
    "harness/oracle",
]
SOURCE_SUFFIXES = {".rs", ".py", ".sh", ".md"}
HOTSPOT_PATTERNS = [
    "TODO(port)",
    "TODO(architect)",
    "OnceLock",
    "thread_local!",
    "unsafe",
    "unwrap(",
    "expect(",
]


@dataclass(frozen=True)
class FileStat:
    path: str
    lines: int
    bytes: int


@dataclass(frozen=True)
class FunctionStat:
    path: str
    line: int
    name: str
    lines: int


def iter_files(root: Path, roots: Iterable[str]) -> Iterable[Path]:
    for rel in roots:
        base = root / rel
        if not base.exists():
            continue
        if base.is_file():
            if base.suffix in SOURCE_SUFFIXES:
                yield base
            continue
        for path in base.rglob("*"):
            if path.is_file() and path.suffix in SOURCE_SUFFIXES:
                yield path


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def file_stats(root: Path, files: Iterable[Path]) -> list[FileStat]:
    out = []
    for path in files:
        text = read_text(path)
        out.append(
            FileStat(
                path=str(path.relative_to(root)),
                lines=text.count("\n") + (0 if text.endswith("\n") or not text else 1),
                bytes=len(text.encode("utf-8")),
            )
        )
    return sorted(out, key=lambda stat: (stat.lines, stat.bytes), reverse=True)


FN_RE = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:unsafe\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\b"
)


def function_stats(root: Path, files: Iterable[Path]) -> list[FunctionStat]:
    out: list[FunctionStat] = []
    for path in files:
        if path.suffix != ".rs":
            continue
        lines = read_text(path).splitlines()
        i = 0
        while i < len(lines):
            match = FN_RE.match(lines[i])
            if not match:
                i += 1
                continue
            name = match.group(1)
            start = i
            depth = 0
            seen_open = False
            end = i
            j = i
            while j < len(lines):
                # Approximate function bodies. This is ranking telemetry, not a
                # compiler; false positives are acceptable for the audit lane.
                for ch in lines[j]:
                    if ch == "{":
                        depth += 1
                        seen_open = True
                    elif ch == "}":
                        depth -= 1
                if seen_open and depth <= 0:
                    end = j
                    break
                j += 1
            out.append(
                FunctionStat(
                    path=str(path.relative_to(root)),
                    line=start + 1,
                    name=name,
                    lines=end - start + 1,
                )
            )
            i = max(j + 1, i + 1)
    return sorted(out, key=lambda stat: stat.lines, reverse=True)


def hotspot_counts(root: Path, files: Iterable[Path]) -> dict[str, list[dict[str, int | str]]]:
    by_pattern: dict[str, list[dict[str, int | str]]] = {}
    for pattern in HOTSPOT_PATTERNS:
        rows = []
        for path in files:
            text = read_text(path)
            count = text.count(pattern)
            if count:
                rows.append({"path": str(path.relative_to(root)), "count": count})
        rows.sort(key=lambda row: int(row["count"]), reverse=True)
        by_pattern[pattern] = rows
    return by_pattern


def harness_result_counts(root: Path) -> dict[str, int]:
    base = root / "harness" / "oracle" / "results"
    total = sum(1 for path in base.rglob("*") if path.is_file()) if base.exists() else 0
    tracked_raw = subprocess.run(
        ["git", "ls-files", "harness/oracle/results"],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    ).stdout.splitlines()
    tracked = len([line for line in tracked_raw if line.strip()])
    return {
        "total_files": total,
        "tracked_files": tracked,
        "untracked_or_ignored_files": max(total - tracked, 0),
    }


def render_table(title: str, rows: list[str]) -> None:
    print(f"\n## {title}")
    if rows:
        print("\n".join(rows))
    else:
        print("(none)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="repository root")
    parser.add_argument("--limit", type=int, default=25, help="rows per section")
    parser.add_argument(
        "--function-threshold",
        type=int,
        default=200,
        help="minimum function length for the long-function section",
    )
    parser.add_argument("--json", action="store_true", help="emit JSON")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    files = sorted(set(iter_files(root, DEFAULT_ROOTS)))
    files_for_stats = file_stats(root, files)
    functions = function_stats(root, files)
    hotspots = hotspot_counts(root, files)
    result_counts = harness_result_counts(root)

    payload = {
        "roots": DEFAULT_ROOTS,
        "file_count": len(files),
        "largest_files": [stat.__dict__ for stat in files_for_stats[: args.limit]],
        "long_functions": [
            stat.__dict__
            for stat in functions
            if stat.lines >= args.function_threshold
        ][: args.limit],
        "hotspots": {
            pattern: rows[: args.limit] for pattern, rows in hotspots.items()
        },
        "harness_results": result_counts,
    }

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    print("Valdr structure audit")
    print(f"scanned_files: {payload['file_count']}")
    print(
        "harness_oracle_results: "
        f"{result_counts['total_files']} total, "
        f"{result_counts['tracked_files']} tracked, "
        f"{result_counts['untracked_or_ignored_files']} untracked_or_ignored"
    )

    render_table(
        "Largest files",
        [
            f"{stat['lines']:>6} lines  {stat['path']}"
            for stat in payload["largest_files"]
        ],
    )
    render_table(
        f"Functions >= {args.function_threshold} lines",
        [
            f"{stat['lines']:>6} lines  {stat['path']}:{stat['line']}  {stat['name']}"
            for stat in payload["long_functions"]
        ],
    )
    for pattern in HOTSPOT_PATTERNS:
        render_table(
            f"Hotspots: {pattern}",
            [
                f"{row['count']:>6}  {row['path']}"
                for row in payload["hotspots"][pattern]
            ],
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
