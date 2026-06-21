/**
 * fit-bench KV backend — flash-sale inventory on Workers KV.
 *
 * This Worker deliberately implements the reserve as a non-atomic
 * read-modify-write on Workers KV: GET the stock, decide, PUT stock-1. Workers KV
 * has no compare-and-set and is eventually consistent, so under concurrent reserves
 * multiple requests read the same pre-decrement value and all believe they won. The
 * resulting oversell is the point of this backend — it is the measured
 * counter-example to EdgeStash's atomic Durable Object reserve. Do NOT "fix" the
 * race; it is the experiment.
 *
 * Contract (shared with the raw-do backend so the harness drives both identically):
 *   PUT  /seed?sku=<sku>&stock=<n>
 *   POST /reserve?sku=<sku>&buyer=<id>   -> 200 {"reserved":k} | 409 {"soldout":true}
 *   GET  /stock?sku=<sku>                -> 200 {"stock":n}
 */

export interface Env {
  STOCK_KV: KVNamespace;
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const sku = url.searchParams.get("sku");
    if (sku === null) return json({ error: "missing sku" }, 400);
    const key = `stock:${sku}`;

    if (req.method === "PUT" && url.pathname === "/seed") {
      const stock = url.searchParams.get("stock");
      if (stock === null) return json({ error: "missing stock" }, 400);
      await env.STOCK_KV.put(key, stock);
      return json({ ok: true, stock: Number(stock) });
    }

    if (req.method === "POST" && url.pathname === "/reserve") {
      const raw = await env.STOCK_KV.get(key);
      if (raw === null) return json({ error: "sku not seeded" }, 404);
      const current = Number(raw);
      if (current <= 0) return json({ soldout: true }, 409);
      const remaining = current - 1;
      await env.STOCK_KV.put(key, String(remaining));
      return json({ reserved: remaining });
    }

    if (req.method === "GET" && url.pathname === "/stock") {
      const raw = await env.STOCK_KV.get(key);
      if (raw === null) return json({ error: "sku not seeded" }, 404);
      return json({ stock: Number(raw) });
    }

    return json({ error: "not found" }, 404);
  },
};
