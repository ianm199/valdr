#!/usr/bin/env python3
"""fit-bench: a product-fit comparison harness for EdgeStash vs platform alternatives.

Where the differential oracle (harness/oracle/valdr-engine-differential.py) settles
*correctness* — does valdr-engine behave like valkey-server — this harness settles
*product fit*: for a concrete use case, how does EdgeStash compare to the
Cloudflare-native alternatives on the axes a buyer actually weighs.

Phase 1 scope is everything reachable on one Cloudflare account with no third-party
service:

  edgestash   valdr-engine (wasm) inside a Durable Object — the system under test.
  raw-do      a Durable Object whose reserve logic is hand-written TypeScript.
  kv          a Worker backing the same logic on Workers KV (eventual, non-atomic).

The lead use case is flash-sale inventory, because its invariant — never reserve
more units than exist — is binary and unarguable: a backend either oversells under
concurrency or it does not. EdgeStash and raw-do are expected to tie on correctness
(both inherit Durable Object request serialization); the point of including raw-do
is the honest "why a Redis engine at all" axis — portability, Lua-as-config, and an
oracle — measured on everything *except* correctness, where they are level.

Two run modes:

  --mode local   In-process simulation of each backend's documented consistency
                 model: serialized request delivery (Durable Objects, EdgeStash)
                 versus a non-atomic read-modify-write (naive Workers KV get/set).
                 The oversell emerges from the model, not a hardcoded verdict. This
                 is the deterministic inner loop used to develop the harness and to
                 show the *direction* of the contrast. It is explicitly NOT a
                 publishable magnitude — it reports the worst-case lost update and
                 says so.

  --mode http    Fire real concurrent HTTP load at deployed backend URLs and measure
                 what actually happens — reserved/oversold counts plus an
                 interleaved latency probe. This is the oracle: the number you cite.
                 Requires the backend Workers to be deployed (see README.md).

Usage:
  python3 harness/fit-bench/fit_bench.py --mode local --buyers 50 --stock 10
  python3 harness/fit-bench/fit_bench.py --mode http \\
      --edgestash-base https://edgestash-valdr.<acct>.workers.dev \\
      --kv-base https://fitbench-kv.<acct>.workers.dev \\
      --raw-do-base https://fitbench-raw-do.<acct>.workers.dev \\
      --buyers 50 --stock 10 --latency-samples 40
"""

import argparse
import concurrent.futures
import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from hashlib import sha1


RESERVE_SCRIPT = (
    "local stock_key = KEYS[1]\n"
    "local hold_key = KEYS[2]\n"
    "local qty = tonumber(ARGV[1])\n"
    "local existing = redis.call('GET', hold_key)\n"
    "if existing then\n"
    "  local sep = string.find(existing, ':', 1, true)\n"
    "  local remaining = tonumber(string.sub(existing, sep + 1))\n"
    "  return {'reserved', remaining}\n"
    "end\n"
    "local stock = tonumber(redis.call('GET', stock_key) or '0')\n"
    "if stock < qty then\n"
    "  return {err='SOLDOUT no stock'}\n"
    "end\n"
    "local remaining = tonumber(redis.call('INCRBY', stock_key, -qty))\n"
    "redis.call('SET', hold_key, tostring(qty) .. ':' .. tostring(remaining), 'PX', tonumber(ARGV[2]))\n"
    "return {'reserved', remaining}\n"
)
"""The EdgeStash reserve script, verbatim from crates/edgestash-demo/tests/demo_inventory.rs.

It is the atomic read-decrement-write the whole comparison turns on: the SOLDOUT
guard runs before the INCRBY, so stock can never be decremented past zero, and the
Durable Object runs the whole script as one indivisible step.
"""


@dataclass
class UseCaseResult:
    """The outcome of one concurrency run of the inventory use case against one backend.

    `reserved` is how many buyers were told they won a unit; `oversold` is how many
    more than the seeded stock won (the invariant violation — zero is the only
    correct value); `final_stock` is the backend's own view of remaining stock after
    the storm, which a correct backend lands at exactly zero and never below.
    """

    backend: str
    buyers: int
    stock: int
    reserved: int
    oversold: int
    final_stock: int
    modeled: bool
    note: str = ""

    @property
    def correct(self) -> bool:
        """A run is correct iff nobody oversold and stock never went negative."""
        return self.oversold == 0 and self.final_stock >= 0


@dataclass
class LatencySummary:
    """Warm-decision latency over an interleaved probe, in milliseconds."""

    backend: str
    samples: int
    p50: float
    p90: float
    p99: float


@dataclass
class CostLine:
    """One backend's modeled cost per million decisions, with provenance.

    Costs are modeled from public pricing on the date in PRICING_AS_OF. A line whose
    `verified` is False was not confirmed against the provider's live pricing page in
    this session and is printed with a warning marker; treat it as a placeholder to
    confirm, not a measured fact.
    """

    backend: str
    usd_per_million: float
    breakdown: str
    verified: bool


PRICING_AS_OF = "2026-06-21"

DO_REQUEST_USD_PER_M = 0.15
"""Cloudflare Durable Objects request price, $/million (1M included free). Verified
2026-06-21 against Cloudflare's Durable Objects pricing docs."""

DO_DURATION_USD_PER_GB_S = 12.50
"""Durable Objects active-duration price, $/million GB-seconds. NOT re-verified this
session; confirm against the live pricing page before citing."""

WORKER_REQUEST_USD_PER_M = 0.30
"""Workers request price, $/million (10M included on the paid plan). NOT re-verified
this session; confirm before citing."""

KV_READ_USD_PER_M = 0.50
KV_WRITE_USD_PER_M = 5.00
"""Workers KV read / write price, $/million. NOT re-verified this session; KV writes
are the expensive operation and dominate this use case, so confirm before citing."""

DO_OBJECT_MEMORY_GB = 128 / 1024
"""A Durable Object is billed duration at the 128 MiB tier while it is active."""


def cost_inventory(backend: str, active_ms: float) -> CostLine:
    """Model $/million reserve decisions for a backend.

    `active_ms` is the wall-clock the request keeps its Durable Object active, which
    drives the duration component; pass the measured warm p50 in http mode and an
    estimate otherwise. KV has no Durable Object, so it pays Workers requests plus a
    KV read and a KV write per decision instead of duration.
    """
    if backend == "kv":
        usd = WORKER_REQUEST_USD_PER_M + KV_READ_USD_PER_M + KV_WRITE_USD_PER_M
        breakdown = (
            f"worker ${WORKER_REQUEST_USD_PER_M:.2f} + kv-read ${KV_READ_USD_PER_M:.2f}"
            f" + kv-write ${KV_WRITE_USD_PER_M:.2f}"
        )
        return CostLine("kv", usd, breakdown, verified=False)

    duration_gb_s_per_m = (active_ms / 1000.0) * DO_OBJECT_MEMORY_GB * 1_000_000
    duration_usd = (duration_gb_s_per_m / 1_000_000) * DO_DURATION_USD_PER_GB_S
    usd = WORKER_REQUEST_USD_PER_M + DO_REQUEST_USD_PER_M + duration_usd
    breakdown = (
        f"worker ${WORKER_REQUEST_USD_PER_M:.2f} + do-req ${DO_REQUEST_USD_PER_M:.2f}"
        f" + do-dur ${duration_usd:.4f} (@{active_ms:.1f}ms active)"
    )
    return CostLine(backend, usd, breakdown, verified=False)


def simulate_serialized(buyers: int, stock: int) -> UseCaseResult:
    """Model a backend whose host serializes requests to the owning shard.

    Durable Objects deliver one request at a time to a given object, and EdgeStash's
    Lua reserve runs atomically inside that delivery, so the read-decrement-write
    cannot interleave. The first `stock` buyers win; the rest see SOLDOUT; stock
    lands at exactly zero.
    """
    reserved = min(stock, buyers)
    return UseCaseResult(
        backend="serialized-model",
        buyers=buyers,
        stock=stock,
        reserved=reserved,
        oversold=max(0, reserved - stock),
        final_stock=stock - reserved,
        modeled=True,
        note="serialized atomic RMW (Durable Object / EdgeStash)",
    )


def simulate_rmw_race(buyers: int, stock: int) -> UseCaseResult:
    """Model a naive Workers KV reserve under the worst-case lost update.

    Workers KV is eventually consistent with no atomic read-modify-write: a Worker
    GETs stock, decides, then PUTs the new value, and concurrent Workers can all read
    the same pre-decrement value before any write lands. The worst case every
    concurrent buyer reads the seeded stock (> 0) and believes it won, so reserved
    equals the whole storm and oversold is the overshoot. The committed stock is
    whatever the last writer put, which bears no relation to how many "won" — the
    signature of a lost update.

    This is the modeled worst case to show the *direction* of the failure. The
    measured magnitude comes from --mode http against deployed KV.
    """
    reserved = buyers if stock > 0 else 0
    return UseCaseResult(
        backend="kv-model",
        buyers=buyers,
        stock=stock,
        reserved=reserved,
        oversold=max(0, reserved - stock),
        final_stock=stock - 1 if stock > 0 else stock,
        modeled=True,
        note="worst-case lost update (non-atomic KV get/set) — direction only, not a magnitude",
    )


class HttpClient:
    """A minimal stdlib HTTP client with a fixed per-request timeout."""

    def __init__(self, timeout_s: float):
        self.timeout_s = timeout_s

    def request(self, method: str, url: str, body: bytes | None = None,
                content_type: str = "application/json") -> tuple[int, bytes]:
        """Issue one request and return (status, body_bytes), treating 4xx/5xx as data.

        A 4xx/5xx is a normal outcome here (SOLDOUT is a 409), so this resolves the
        HTTPError into a status/body pair rather than raising, and lets genuine
        transport failures propagate.
        """
        request = urllib.request.Request(url=url, method=method, data=body)
        if body is not None:
            request.add_header("content-type", content_type)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()


class SimpleWorkerAdapter:
    """Adapter for a backend Worker exposing the fit-bench inventory contract.

    The KV and raw-do Workers both implement the same three routes so the harness
    drives them identically:

      PUT  /seed?sku=<sku>&stock=<n>     reset a SKU to n units
      POST /reserve?sku=<sku>&buyer=<id> reserve one unit; 200 {"reserved":k} on a
                                         win, 409 {"soldout":true} when drained
      GET  /stock?sku=<sku>             current stock as {"stock":n}
    """

    def __init__(self, name: str, base: str, client: HttpClient):
        self.name = name
        self.base = base.rstrip("/")
        self.client = client

    def setup(self, sku: str, stock: int) -> None:
        status, raw = self.client.request("PUT", f"{self.base}/seed?sku={sku}&stock={stock}")
        if status != 200:
            raise RuntimeError(f"{self.name} seed failed: {status} {raw!r}")

    def reserve_one(self, sku: str, buyer: str) -> str:
        status, raw = self.client.request("POST", f"{self.base}/reserve?sku={sku}&buyer={buyer}")
        if status == 200 and b"reserved" in raw:
            return "reserved"
        if status == 409 or b"soldout" in raw.lower():
            return "soldout"
        return "other"

    def final_stock(self, sku: str) -> int:
        status, raw = self.client.request("GET", f"{self.base}/stock?sku={sku}")
        if status != 200:
            raise RuntimeError(f"{self.name} stock read failed: {status} {raw!r}")
        return int(json.loads(raw)["stock"])


class EdgeStashAdapter:
    """Adapter for the deployed EdgeStash Worker, driving the same reserve flow the
    live inventory fixture uses: SCRIPT LOAD the reserve script, SET the stock key,
    then EVALSHA per buyer through the tenant-scoped raw command route."""

    def __init__(self, base: str, client: HttpClient):
        self.name = "edgestash"
        self.base = base.rstrip("/")
        self.client = client
        self.reserve_sha = sha1(RESERVE_SCRIPT.encode()).hexdigest()

    def setup(self, sku: str, stock: int) -> None:
        status, raw = self.client.request(
            "POST", f"{self.base}/v1/valdr/{sku}/SCRIPT/LOAD",
            body=RESERVE_SCRIPT.encode(), content_type="text/plain",
        )
        if status != 200 or self.reserve_sha.encode() not in raw:
            raise RuntimeError(f"edgestash SCRIPT LOAD failed: {status} {raw!r}")
        status, raw = self.client.request("GET", f"{self.base}/v1/valdr/{sku}/SET/stock/{stock}")
        if status != 200:
            raise RuntimeError(f"edgestash seed stock failed: {status} {raw!r}")

    def reserve_one(self, sku: str, buyer: str) -> str:
        hold = f"hold%3Aflash%3A{buyer}"
        url = f"{self.base}/v1/valdr/{sku}/EVALSHA/{self.reserve_sha}/2/stock/{hold}/1/600000"
        status, raw = self.client.request("GET", url)
        if status == 200 and b"reserved" in raw:
            return "reserved"
        if b"SOLDOUT" in raw:
            return "soldout"
        return "other"

    def final_stock(self, sku: str) -> int:
        status, raw = self.client.request("GET", f"{self.base}/v1/valdr/{sku}/GET/stock")
        if status != 200:
            raise RuntimeError(f"edgestash stock read failed: {status} {raw!r}")
        return int(json.loads(raw)["result"])


def run_inventory_http(adapter, buyers: int, stock: int) -> UseCaseResult:
    """Seed a fresh SKU, fire `buyers` concurrent reserves, and tally the outcome.

    Each buyer reserves under a distinct id so retries cannot mask a race, and every
    request runs on its own thread so the storm hits the backend genuinely
    concurrently, the way the shell fixture's backgrounded curls do.
    """
    sku = f"fitbench-{int(time.time())}-{buyers}x{stock}"
    adapter.setup(sku, stock)

    outcomes: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=buyers) as pool:
        futures = [pool.submit(adapter.reserve_one, sku, f"buyer-{i}") for i in range(buyers)]
        for future in concurrent.futures.as_completed(futures):
            outcomes.append(future.result())

    reserved = outcomes.count("reserved")
    other = outcomes.count("other")
    final = adapter.final_stock(sku)
    note = "measured" if other == 0 else f"measured ({other} malformed responses)"
    return UseCaseResult(
        backend=adapter.name,
        buyers=buyers,
        stock=stock,
        reserved=reserved,
        oversold=max(0, reserved - stock),
        final_stock=final,
        modeled=False,
        note=note,
    )


def probe_latency(adapter, sku: str, samples: int) -> LatencySummary:
    """Measure warm single-reserve latency with fresh stock per sample.

    Runs sequentially so each timing is an isolated warm round trip rather than a
    contended one; the concurrency run above is what stresses correctness.
    """
    timings: list[float] = []
    for i in range(samples):
        adapter.setup(f"{sku}-lat-{i}", 1_000_000)
        start = time.perf_counter()
        adapter.reserve_one(f"{sku}-lat-{i}", f"probe-{i}")
        timings.append((time.perf_counter() - start) * 1000.0)
    ordered = sorted(timings)
    return LatencySummary(
        backend=adapter.name,
        samples=samples,
        p50=statistics.median(ordered),
        p90=ordered[min(len(ordered) - 1, int(0.90 * len(ordered)))],
        p99=ordered[min(len(ordered) - 1, int(0.99 * len(ordered)))],
    )


@dataclass
class Matrix:
    """The assembled comparison: one correctness row per backend, plus optional
    latency and cost columns when those were measured/modeled."""

    results: list[UseCaseResult] = field(default_factory=list)
    latencies: dict[str, LatencySummary] = field(default_factory=dict)
    costs: dict[str, CostLine] = field(default_factory=dict)

    def render(self) -> str:
        lines = []
        lines.append("flash-sale inventory — never reserve more units than exist")
        lines.append("")
        header = f"{'backend':<16}{'reserved':>10}{'oversold':>10}{'final_stock':>13}{'correct':>9}  source"
        lines.append(header)
        lines.append("-" * len(header))
        for r in self.results:
            verdict = "PASS" if r.correct else "FAIL"
            source = "MODELED" if r.modeled else "measured"
            lines.append(
                f"{r.backend:<16}{r.reserved:>10}{r.oversold:>10}{r.final_stock:>13}{verdict:>9}  {source}"
            )
        if self.latencies:
            lines.append("")
            lines.append("warm reserve latency (ms)")
            lines.append(f"{'backend':<16}{'p50':>8}{'p90':>8}{'p99':>8}{'samples':>10}")
            for name, lat in self.latencies.items():
                lines.append(f"{name:<16}{lat.p50:>8.1f}{lat.p90:>8.1f}{lat.p99:>8.1f}{lat.samples:>10}")
        if self.costs:
            lines.append("")
            lines.append(f"modeled cost per million decisions (pricing as of {PRICING_AS_OF})")
            for name, cost in self.costs.items():
                mark = "" if cost.verified else "  ⚠ unverified pricing"
                lines.append(f"{name:<16}${cost.usd_per_million:>7.2f}/M   {cost.breakdown}{mark}")
        return "\n".join(lines)


def run_local(buyers: int, stock: int) -> Matrix:
    """The deterministic inner loop: model the documented consistency contrast."""
    matrix = Matrix()
    edgestash = simulate_serialized(buyers, stock)
    edgestash.backend = "edgestash"
    raw_do = simulate_serialized(buyers, stock)
    raw_do.backend = "raw-do"
    raw_do.note = "serialized atomic RMW (Durable Object, TypeScript)"
    kv = simulate_rmw_race(buyers, stock)
    kv.backend = "kv"
    matrix.results = [edgestash, raw_do, kv]
    matrix.costs = {
        "edgestash": cost_inventory("edgestash", active_ms=3.0),
        "raw-do": cost_inventory("raw-do", active_ms=1.0),
        "kv": cost_inventory("kv", active_ms=0.0),
    }
    return matrix


def run_http(args) -> Matrix:
    """The oracle: drive real deployed backends and measure correctness + latency."""
    client = HttpClient(timeout_s=args.timeout_s)
    adapters = []
    if args.edgestash_base:
        adapters.append(EdgeStashAdapter(args.edgestash_base, client))
    if args.raw_do_base:
        adapters.append(SimpleWorkerAdapter("raw-do", args.raw_do_base, client))
    if args.kv_base:
        adapters.append(SimpleWorkerAdapter("kv", args.kv_base, client))
    if not adapters:
        raise SystemExit("http mode needs at least one of --edgestash-base/--raw-do-base/--kv-base")

    matrix = Matrix()
    for adapter in adapters:
        matrix.results.append(run_inventory_http(adapter, args.buyers, args.stock))
        if args.latency_samples > 0:
            lat = probe_latency(adapter, f"fitbench-{int(time.time())}", args.latency_samples)
            matrix.latencies[adapter.name] = lat
            active_ms = lat.p50 if adapter.name != "kv" else 0.0
            matrix.costs[adapter.name] = cost_inventory(adapter.name, active_ms)
    return matrix


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=["local", "http"], default="local")
    parser.add_argument("--buyers", type=int, default=50)
    parser.add_argument("--stock", type=int, default=10)
    parser.add_argument("--edgestash-base", default="")
    parser.add_argument("--kv-base", default="")
    parser.add_argument("--raw-do-base", default="")
    parser.add_argument("--latency-samples", type=int, default=40)
    parser.add_argument("--timeout-s", type=float, default=15.0)
    parser.add_argument("--json", action="store_true", help="emit the matrix as JSON")
    return parser.parse_args(argv)


def matrix_to_json(matrix: Matrix) -> str:
    payload = {
        "results": [vars(r) | {"correct": r.correct} for r in matrix.results],
        "latencies": {k: vars(v) for k, v in matrix.latencies.items()},
        "costs": {k: vars(v) for k, v in matrix.costs.items()},
        "pricing_as_of": PRICING_AS_OF,
    }
    return json.dumps(payload, indent=2)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    matrix = run_local(args.buyers, args.stock) if args.mode == "local" else run_http(args)
    print(matrix_to_json(matrix) if args.json else matrix.render())
    if args.mode == "local":
        print("\nlocal mode is the modeled inner loop; run --mode http for measured numbers.",
              file=sys.stderr)
    any_fail = any(not r.correct for r in matrix.results if not r.modeled)
    return 1 if any_fail else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
