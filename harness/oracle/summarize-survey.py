#!/usr/bin/env python3
"""Human-readable one-screen summary of a tcl-survey JSON blob.

Reads from a path argument or stdin. tcl-survey.py interleaves `==>` progress
lines on stdout before the final JSON, so we locate the JSON by the first `{`
and decode from there rather than assuming the stream is pure JSON.

Default output: overall status, the survey summary line, and one aligned row
per file (passed/total + FAIL/timeout marker). With `--json`: the re-serialized
JSON (used by `make oracle FORMAT=json`).
"""

import json
import sys


def load(argv: list[str]) -> tuple[dict, bool]:
    as_json = "--json" in argv
    args = [a for a in argv[1:] if a != "--json"]
    raw = open(args[0], encoding="utf-8").read() if args else sys.stdin.read()
    start = raw.find("{")
    if start < 0:
        raise SystemExit("no JSON object found in survey output")
    data, _ = json.JSONDecoder().raw_decode(raw[start:])
    return data, as_json


def main() -> int:
    data, as_json = load(sys.argv)
    if as_json:
        print(json.dumps(data, indent=2))
        return 0
    status = data.get("status", "unknown").upper()
    print(f"{status}: {data.get('summary', '')}")
    for f in data.get("evidence", {}).get("files", []):
        mark = "  ✗ FAIL" if f.get("failed") else ""
        if f.get("timed_out"):
            mark += "  (timeout)"
        if f.get("exception"):
            mark += "  (exception)"
        print(f"  {f.get('passed', 0):>4}/{f.get('total', 0):<4}  {f.get('test', '?')}{mark}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
