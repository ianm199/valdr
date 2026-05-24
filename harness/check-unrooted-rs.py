#!/usr/bin/env python3
"""Fail if a top-level Rust file under crates/*/src is not rooted.

Rust only compiles files that are reachable from a crate root. Bulk translation
passes can accidentally leave source-shaped drafts under `src/` that Cargo
ignores. This check keeps live implementation and parked reference material
separate.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


MOD_RE = re.compile(r"(?m)^\s*(?:pub\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;")


def rooted_files(src: Path) -> set[str]:
    roots = [p for p in (src / "lib.rs", src / "main.rs") if p.exists()]
    text = "\n".join(p.read_text(errors="ignore") for p in roots)
    names = {p.name for p in roots}
    names.update(f"{m}.rs" for m in MOD_RE.findall(text))
    return names


def main() -> int:
    repo = Path(__file__).resolve().parents[1]
    failures: list[Path] = []
    for src in sorted((repo / "crates").glob("*/src")):
        rooted = rooted_files(src)
        for path in sorted(src.glob("*.rs")):
            if path.name not in rooted:
                failures.append(path.relative_to(repo))

    if failures:
        print("unrooted Rust files under crate src/:", file=sys.stderr)
        for path in failures:
            print(f"  {path}", file=sys.stderr)
        print(
            "\nMove parked translations to source-drafts/ or root them with mod declarations.",
            file=sys.stderr,
        )
        return 1

    print("ok: no unrooted top-level Rust files under crates/*/src")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
