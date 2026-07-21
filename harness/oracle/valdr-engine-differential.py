#!/usr/bin/env python3
"""Differential oracle for valdr-engine vs the pinned reference Valkey binary.

Runs identical command fixtures through (a) valdr-engine in-process via the
valdr-fixture-runner JSONL driver and (b) the reference valkey-server over
RESP2 TCP, and diffs the raw reply frames.

Fixture files are JSONL under harness/oracle/valdr-fixtures/. Each line:

    {"id": "<string>",
     "cmd": ["SET", "k", "v"],
     "now_millis": <optional u64, engine host clock; wall clock if absent>,
     "mode": "exact" | "ttl_band" | "error_prefix" | "type_only" | "set_equal" | "scan_reply"
             | "float_g10",
     "band": <int, required for ttl_band>,
     "sleep_ms": <optional int, harness sleeps before dispatching this line>,
     "known_unsupported": <optional bool, record-only, never a verdict>}

One persistent engine process per fixture file (fresh engine state per file);
FLUSHALL + SCRIPT FLUSH are issued to valkey between files. Divergences are
recorded in the report, never fixed here. Exit code is 0 when the harness ran
clean; --strict exits 1 if any fixture diverged.

Ports are confined to 38000-38999 (other oracle runners own 36000-37999).
"""

import argparse
import json
import math
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

VALID_MODES = ("exact", "ttl_band", "error_prefix", "type_only", "set_equal", "scan_reply", "float_g10")
PORT_RANGE = (38000, 38999)


class HarnessError(Exception):
    """The harness itself failed; distinct from an engine/valkey divergence."""


class EngineCrashed(Exception):
    """The engine process died mid-fixture; carries the fixture id + stderr."""

    def __init__(self, fixture_id, stderr):
        super().__init__(f"engine crashed on fixture {fixture_id!r}")
        self.fixture_id = fixture_id
        self.stderr = stderr


class Incomplete(Exception):
    """A RESP frame needs more bytes than are currently buffered."""


def parse_frame(buf, pos=0):
    """Parse one RESP2 frame at pos; return ((tag, value), end_offset)."""
    if pos >= len(buf):
        raise Incomplete()
    tag = buf[pos:pos + 1]
    line_end = buf.find(b"\r\n", pos)
    if line_end == -1:
        raise Incomplete()
    line = buf[pos + 1:line_end]
    after = line_end + 2
    if tag == b"+":
        return ("+", line), after
    if tag == b"-":
        return ("-", line), after
    if tag == b":":
        return (":", int(line)), after
    if tag == b"$":
        length = int(line)
        if length == -1:
            return ("$", None), after
        if len(buf) < after + length + 2:
            raise Incomplete()
        return ("$", buf[after:after + length]), after + length + 2
    if tag == b"*":
        count = int(line)
        if count == -1:
            return ("*", None), after
        items = []
        cursor = after
        for _ in range(count):
            node, cursor = parse_frame(buf, cursor)
            items.append(node)
        return ("*", items), cursor
    raise HarnessError(f"unsupported RESP type byte {tag!r} at offset {pos}")


def encode_command(argv):
    """Encode argv (list of bytes) as a RESP2 array of bulk strings."""
    parts = [b"*%d\r\n" % len(argv)]
    for arg in argv:
        parts.append(b"$%d\r\n%s\r\n" % (len(arg), arg))
    return b"".join(parts)


def printable(data):
    """Render bytes as a quoted ASCII-safe string for the report."""
    out = []
    for byte in data:
        char = chr(byte)
        if char == '"':
            out.append('\\"')
        elif char == "\\":
            out.append("\\\\")
        elif 32 <= byte < 127:
            out.append(char)
        elif char == "\r":
            out.append("\\r")
        elif char == "\n":
            out.append("\\n")
        else:
            out.append(f"\\x{byte:02x}")
    return "".join(out)


def render(node):
    """Render a parsed frame as a one-line human-readable string."""
    tag, value = node
    if tag == "+":
        return "+" + printable(value)
    if tag == "-":
        return "-" + printable(value)
    if tag == ":":
        return f":{value}"
    if tag == "$":
        if value is None:
            return "(nil)"
        return f'"{printable(value)}"'
    if value is None:
        return "(nil array)"
    return "[" + ", ".join(render(item) for item in value) + "]"


def first_error_token(node):
    tag, value = node
    if tag != "-":
        return None
    return value.split(b" ", 1)[0]


def canonicalize_float_g10(node):
    """Render a finite numeric bulk-string frame via `%.10g`, upstream's own
    tolerance for float replies (`roundFloat`, reference/valkey
    tests/support/util.tcl:498). Returns None for anything that is not a
    finite numeric bulk string (wrong RESP type, a non-numeric payload, or
    inf/nan) so the caller falls back to exact byte comparison instead of
    ever masking a genuine type or error divergence.
    """
    tag, value = node
    if tag != "$" or value is None:
        return None
    try:
        text = value.decode("ascii")
    except UnicodeDecodeError:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    if not math.isfinite(parsed):
        return None
    return "%.10g" % parsed


def compare(mode, band, engine_raw, valkey_raw):
    """Return True when the two raw frames agree under the fixture's mode."""
    if engine_raw == valkey_raw:
        return True
    if mode == "exact":
        return False
    engine_node, _ = parse_frame(engine_raw)
    valkey_node, _ = parse_frame(valkey_raw)
    if mode == "ttl_band":
        if engine_node[0] != ":" or valkey_node[0] != ":":
            return False
        return abs(engine_node[1] - valkey_node[1]) <= band
    if mode == "error_prefix":
        engine_token = first_error_token(engine_node)
        valkey_token = first_error_token(valkey_node)
        return engine_token is not None and engine_token == valkey_token
    if mode == "type_only":
        return engine_node[0] == valkey_node[0]
    if mode == "set_equal":
        if engine_node[0] != "*" or valkey_node[0] != "*":
            return False
        if engine_node[1] is None or valkey_node[1] is None:
            return False
        engine_items = sorted(render(item) for item in engine_node[1])
        valkey_items = sorted(render(item) for item in valkey_node[1])
        return engine_items == valkey_items
    if mode == "scan_reply":
        # SCAN-family reply: a 2-element array [cursor, [elements]]. The cursor is
        # an opaque, implementation-specific token, so this mode is only valid for
        # fixtures where the scan COMPLETES IN ONE PASS (both sides return cursor
        # "0"); the element batch is compared order-independently because the
        # engine's HashMap iteration order differs from valkey's dict order.
        if engine_node[0] != "*" or valkey_node[0] != "*":
            return False
        if engine_node[1] is None or valkey_node[1] is None:
            return False
        if len(engine_node[1]) != 2 or len(valkey_node[1]) != 2:
            return False
        engine_cursor, engine_elems = engine_node[1]
        valkey_cursor, valkey_elems = valkey_node[1]
        if render(engine_cursor) != render(valkey_cursor):
            return False
        if engine_elems[0] != "*" or valkey_elems[0] != "*":
            return False
        engine_items = sorted(render(item) for item in (engine_elems[1] or []))
        valkey_items = sorted(render(item) for item in (valkey_elems[1] or []))
        return engine_items == valkey_items
    if mode == "float_g10":
        engine_canon = canonicalize_float_g10(engine_node)
        valkey_canon = canonicalize_float_g10(valkey_node)
        if engine_canon is None or valkey_canon is None:
            return False
        return engine_canon == valkey_canon
    raise HarnessError(f"unknown compare mode {mode!r}")


class ValkeyClient:
    """Minimal RESP2 client over a raw socket, capturing exact frame bytes."""

    def __init__(self, port, timeout_s=30.0):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout_s)
        self.sock.settimeout(timeout_s)
        self.buf = b""

    def close(self):
        self.sock.close()

    def roundtrip(self, argv):
        """Send one command, return the raw bytes of exactly one reply frame."""
        self.sock.sendall(encode_command(argv))
        while True:
            try:
                _, end = parse_frame(self.buf)
                raw = self.buf[:end]
                self.buf = self.buf[end:]
                return raw
            except Incomplete:
                chunk = self.sock.recv(65536)
                if not chunk:
                    raise HarnessError("valkey closed the connection mid-reply")
                self.buf += chunk


class EngineRunner:
    """One valdr-fixture-runner process; one persistent Engine per process."""

    def __init__(self, runner_bin, cwd):
        self.proc = subprocess.Popen(
            [str(runner_bin)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(cwd),
        )

    def roundtrip(self, fixture_line):
        request = json.dumps(fixture_line, separators=(",", ":")) + "\n"
        self.proc.stdin.write(request.encode("utf-8"))
        self.proc.stdin.flush()
        reply = self.proc.stdout.readline()
        if not reply:
            stderr = self.proc.stderr.read().decode("utf-8", "replace")
            raise EngineCrashed(fixture_line["id"], stderr.strip())
        decoded = json.loads(reply)
        if decoded["id"] != fixture_line["id"]:
            raise HarnessError(
                f"runner reply id {decoded['id']!r} != sent id {fixture_line['id']!r}"
            )
        return bytes.fromhex(decoded["resp_hex"])

    def close(self):
        if self.proc.poll() is None:
            self.proc.stdin.close()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


def find_free_port():
    for port in range(PORT_RANGE[0], PORT_RANGE[1] + 1):
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            probe.bind(("127.0.0.1", port))
            return port
        except OSError:
            continue
        finally:
            probe.close()
    raise HarnessError(f"no free port in {PORT_RANGE[0]}-{PORT_RANGE[1]}")


def wait_for_pong(port, deadline_s=15.0):
    started = time.monotonic()
    while time.monotonic() - started < deadline_s:
        try:
            client = ValkeyClient(port, timeout_s=2.0)
            try:
                raw = client.roundtrip([b"PING"])
                if raw == b"+PONG\r\n":
                    return
            finally:
                client.close()
        except (OSError, HarnessError):
            time.sleep(0.1)
    raise HarnessError(f"valkey on port {port} never answered PING within {deadline_s}s")


def boot_valkey(server_bin, port, workdir):
    logfile = open(os.path.join(workdir, "valkey.log"), "wb")
    proc = subprocess.Popen(
        [
            str(server_bin),
            "--port", str(port),
            "--save", "",
            "--appendonly", "no",
            "--daemonize", "no",
        ],
        stdout=logfile,
        stderr=subprocess.STDOUT,
        cwd=workdir,
    )
    return proc, logfile


def stop_valkey(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def load_fixture_lines(path):
    lines = []
    with open(path, "r", encoding="utf-8") as handle:
        for line_no, raw in enumerate(handle, start=1):
            text = raw.strip()
            if not text:
                continue
            fixture = json.loads(text)
            if "id" not in fixture or "cmd" not in fixture:
                raise HarnessError(f"{path}:{line_no}: fixture needs 'id' and 'cmd'")
            mode = fixture.get("mode", "exact")
            if mode not in VALID_MODES:
                raise HarnessError(f"{path}:{line_no}: unknown mode {mode!r}")
            if mode == "ttl_band" and "band" not in fixture:
                raise HarnessError(f"{path}:{line_no}: ttl_band requires 'band'")
            lines.append(fixture)
    return lines


def run_fixture_file(path, runner_bin, repo_root, valkey, crashed_ids):
    """Run one fixture file; return a list of per-fixture result dicts.

    Fixture ids in crashed_ids (from earlier attempts of this file) are skipped
    on BOTH sides to keep engine and valkey state in lockstep, and recorded as
    ENGINE-CRASH findings. Raises EngineCrashed when the engine process dies on
    a fixture not yet in crashed_ids; the caller re-flushes valkey and retries
    the whole file so state stays symmetric.
    """
    results = []
    runner = EngineRunner(runner_bin, repo_root)
    try:
        for fixture in load_fixture_lines(path):
            if fixture["id"] in crashed_ids:
                results.append({
                    "file": path.name,
                    "id": fixture["id"],
                    "cmd": fixture["cmd"],
                    "mode": fixture.get("mode", "exact"),
                    "band": fixture.get("band", 0),
                    "verdict": "ENGINE-CRASH",
                    "engine": crashed_ids[fixture["id"]],
                    "valkey": "(line skipped on valkey too, to preserve state parity)",
                })
                continue
            sleep_ms = fixture.get("sleep_ms")
            if sleep_ms:
                time.sleep(sleep_ms / 1000.0)
            engine_request = {"id": fixture["id"], "cmd": fixture["cmd"]}
            if "now_millis" in fixture:
                engine_request["now_millis"] = fixture["now_millis"]
            engine_raw = runner.roundtrip(engine_request)
            valkey_raw = valkey.roundtrip([arg.encode("utf-8") for arg in fixture["cmd"]])
            mode = fixture.get("mode", "exact")
            band = fixture.get("band", 0)
            if fixture.get("known_unsupported"):
                verdict = "KNOWN-UNSUPPORTED"
            elif compare(mode, band, engine_raw, valkey_raw):
                verdict = "PASS"
            else:
                verdict = "DIVERGE"
            results.append({
                "file": path.name,
                "id": fixture["id"],
                "cmd": fixture["cmd"],
                "mode": mode,
                "band": band,
                "verdict": verdict,
                "engine": render(parse_frame(engine_raw)[0]),
                "valkey": render(parse_frame(valkey_raw)[0]),
            })
    finally:
        runner.close()
    return results


def command_group(cmd):
    head = cmd[0].upper()
    if head == "SCRIPT" and len(cmd) > 1:
        return f"SCRIPT {cmd[1].upper()}"
    return head


def write_report(out_path, results, meta):
    passes = [r for r in results if r["verdict"] == "PASS"]
    diverges = [r for r in results if r["verdict"] == "DIVERGE"]
    crashes = [r for r in results if r["verdict"] == "ENGINE-CRASH"]
    known = [r for r in results if r["verdict"] == "KNOWN-UNSUPPORTED"]

    lines = []
    lines.append("valdr-engine differential oracle")
    lines.append(f"run at:      {meta['timestamp']}")
    lines.append(f"engine:      valdr-fixture-runner ({meta['runner_bin']})")
    lines.append(f"reference:   {meta['server_bin']} on port {meta['port']}")
    lines.append(f"fixtures:    {meta['fixtures_dir']}")
    lines.append("")
    lines.append("SUMMARY")
    lines.append(f"  fixtures run:        {len(results)}")
    lines.append(f"  pass:                {len(passes)}")
    lines.append(f"  diverge:             {len(diverges)}")
    lines.append(f"  engine-crash:        {len(crashes)}")
    lines.append(f"  known-unsupported:   {len(known)}")
    lines.append("")
    lines.append("PER-FIXTURE RESULTS")
    current_file = None
    for result in results:
        if result["file"] != current_file:
            current_file = result["file"]
            lines.append("")
            lines.append(f"== {current_file} ==")
        lines.append(
            f"  {result['verdict']:<18} {result['id']:<42} mode={result['mode']}"
        )
        lines.append(f"      engine: {result['engine']}")
        lines.append(f"      valkey: {result['valkey']}")
    lines.append("")
    lines.append("DIVERGENCES (grouped by command; ENGINE-CRASH entries included)")
    if not diverges and not crashes:
        lines.append("  none")
    groups = {}
    for result in diverges + crashes:
        groups.setdefault(command_group(result["cmd"]), []).append(result)
    for group in sorted(groups):
        lines.append("")
        lines.append(f"[{group}]")
        for result in groups[group]:
            lines.append(f"  {result['file']} :: {result['id']}  (mode={result['mode']})")
            lines.append(f"      cmd:    {result['cmd']}")
            lines.append(f"      engine: {result['engine']}")
            lines.append(f"      valkey: {result['valkey']}")
    lines.append("")
    lines.append("KNOWN-UNSUPPORTED (engine lacks these by design; record-only)")
    if not known:
        lines.append("  none")
    for result in known:
        lines.append(f"  {result['file']} :: {result['id']}")
        lines.append(f"      cmd:    {result['cmd']}")
        lines.append(f"      engine: {result['engine']}")
        lines.append(f"      valkey: {result['valkey']}")
    lines.append("")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")
    return len(passes), len(diverges) + len(crashes), len(known)


def run_selftest():
    """Assert the float_g10 canonicalizer's documented semantics through
    `compare()` directly — no valkey binary, no fixture runner. Returns True
    when every assertion holds; prints each failure to stderr and returns
    False otherwise.
    """

    def bulk(text):
        data = text.encode("ascii")
        return b"$%d\r\n%s\r\n" % (len(data), data)

    def error(text):
        return b"-%s\r\n" % text.encode("ascii")

    cases = [
        ("10.6 canonicalizes equal to 10.59999999999999964", bulk("10.6"), bulk("10.59999999999999964"), True),
        ("0 does not canonicalize equal to 0.00000000000000000001", bulk("0"), bulk("0.00000000000000000001"), False),
        ("abc equals abc", bulk("abc"), bulk("abc"), True),
        ("abc does not equal abd", bulk("abc"), bulk("abd"), False),
        ("an error reply is never canonicalized against a numeric bulk reply", error("ERR value is not a valid float"), bulk("10.6"), False),
    ]
    failures = []
    for label, engine_raw, valkey_raw, expected in cases:
        actual = compare("float_g10", 0, engine_raw, valkey_raw)
        if actual != expected:
            failures.append(f"{label}: expected {expected}, got {actual}")
    if failures:
        for failure in failures:
            print(f"SELFTEST FAIL: {failure}", file=sys.stderr)
        return False
    print(f"valdr float_g10 selftest: {len(cases)}/{len(cases)} assertions passed")
    return True


def main():
    repo_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--selftest",
        action="store_true",
        help="run the float_g10 canonicalizer self-check (no valkey/runner needed) and exit",
    )
    parser.add_argument(
        "--server-bin",
        default=str(repo_root / "reference" / "valkey" / "src" / "valkey-server"),
        help="path to the pinned reference valkey-server binary",
    )
    parser.add_argument(
        "--runner-bin",
        default="",
        help="prebuilt valdr-fixture-runner binary; built via cargo when empty",
    )
    parser.add_argument(
        "--fixtures-dir",
        default=str(repo_root / "harness" / "oracle" / "valdr-fixtures"),
        help="directory of *.jsonl fixture files",
    )
    parser.add_argument(
        "--out",
        default=str(
            repo_root
            / "harness"
            / "oracle"
            / "results"
            / f"valdr-differential-{time.strftime('%Y%m%d')}.txt"
        ),
        help="report output path",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit 1 if any fixture diverged",
    )
    parser.add_argument(
        "--files",
        default="",
        help="comma-separated fixture basenames to restrict the run (e.g. 'set.jsonl,hash') "
        "— default: all *.jsonl. Per-file fresh-engine + FLUSHALL semantics are unchanged, "
        "so a single-file run's verdict matches that group's verdict in the full run.",
    )
    args = parser.parse_args()

    if args.selftest:
        sys.exit(0 if run_selftest() else 1)

    server_bin = Path(args.server_bin)
    if not server_bin.is_file():
        raise HarnessError(f"valkey-server binary not found: {server_bin}")

    if args.runner_bin:
        runner_bin = Path(args.runner_bin)
    else:
        subprocess.run(
            ["cargo", "build", "-q", "-p", "valdr-fixture-runner"],
            cwd=str(repo_root),
            check=True,
        )
        target_root = Path(os.environ["CARGO_TARGET_DIR"]) if os.environ.get("CARGO_TARGET_DIR") else repo_root / "target"
        runner_bin = target_root / "debug" / "valdr-fixture-runner"
    if not runner_bin.is_file():
        raise HarnessError(f"valdr-fixture-runner binary not found: {runner_bin}")

    fixtures_dir = Path(args.fixtures_dir)
    fixture_files = sorted(fixtures_dir.glob("*.jsonl"))
    if args.files:
        wanted = {
            (f if f.endswith(".jsonl") else f + ".jsonl")
            for f in (s.strip() for s in args.files.split(","))
            if f
        }
        present = {p.name for p in fixture_files}
        missing = wanted - present
        if missing:
            raise HarnessError(
                f"--files: no such fixture(s) in {fixtures_dir}: {sorted(missing)}"
            )
        fixture_files = [p for p in fixture_files if p.name in wanted]
    if not fixture_files:
        raise HarnessError(f"no *.jsonl fixture files in {fixtures_dir}")

    port = find_free_port()
    results = []
    with tempfile.TemporaryDirectory(prefix="valdr-diff-") as workdir:
        proc, logfile = boot_valkey(server_bin, port, workdir)
        client = None
        try:
            wait_for_pong(port)
            client = ValkeyClient(port)
            for path in fixture_files:
                crashed_ids = {}
                for _attempt in range(6):
                    flush_db = client.roundtrip([b"FLUSHALL"])
                    flush_scripts = client.roundtrip([b"SCRIPT", b"FLUSH"])
                    if flush_db != b"+OK\r\n" or flush_scripts != b"+OK\r\n":
                        raise HarnessError(
                            f"valkey reset failed before {path.name}: "
                            f"{flush_db!r} / {flush_scripts!r}"
                        )
                    try:
                        results.extend(
                            run_fixture_file(
                                path, runner_bin, repo_root, client, crashed_ids
                            )
                        )
                        break
                    except EngineCrashed as crash:
                        crashed_ids[crash.fixture_id] = (
                            f"(engine process died: {crash.stderr})"
                        )
                else:
                    raise HarnessError(
                        f"{path.name}: engine crashed on every retry; "
                        f"crashed ids: {sorted(crashed_ids)}"
                    )
        finally:
            if client is not None:
                client.close()
            stop_valkey(proc)
            logfile.close()

    meta = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "runner_bin": runner_bin,
        "server_bin": server_bin,
        "port": port,
        "fixtures_dir": fixtures_dir,
    }
    out_path = Path(args.out)
    passed, diverged, known = write_report(out_path, results, meta)
    print(
        f"valdr differential: {len(results)} fixtures, {passed} pass, "
        f"{diverged} diverge, {known} known-unsupported -> {out_path}"
    )
    if args.strict and diverged:
        sys.exit(1)


if __name__ == "__main__":
    main()
