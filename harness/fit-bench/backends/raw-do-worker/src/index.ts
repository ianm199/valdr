/**
 * fit-bench raw-do backend — flash-sale inventory on a hand-written Durable Object.
 *
 * This is the "why a Redis engine at all" baseline. A Durable Object owns each SKU
 * and the reserve is plain TypeScript: read the counter, decide, write it back.
 * Because Cloudflare delivers one request at a time to a given Durable Object (and
 * input gates hold other events while a storage await is in flight), this
 * read-modify-write is atomic with no engine, no Lua, and no wasm — so this backend
 * is *correct* under concurrency, exactly like EdgeStash.
 *
 * Including it keeps the comparison honest: EdgeStash does not beat a raw Durable
 * Object on inventory correctness; they tie. EdgeStash's edge is elsewhere — a Redis
 * API and portability, Lua policy you change without redeploying, and a differential
 * oracle proving the semantics. fit-bench therefore measures this backend on every
 * axis *except* correctness, where it is level.
 *
 * Same contract as the KV backend:
 *   PUT  /seed?sku=<sku>&stock=<n>
 *   POST /reserve?sku=<sku>&buyer=<id>   -> 200 {"reserved":k} | 409 {"soldout":true}
 *   GET  /stock?sku=<sku>                -> 200 {"stock":n}
 */

export interface Env {
  SKU: DurableObjectNamespace;
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const sku = url.searchParams.get("sku");
    if (sku === null) {
      return new Response(JSON.stringify({ error: "missing sku" }), {
        status: 400,
        headers: { "content-type": "application/json" },
      });
    }
    const id = env.SKU.idFromName(sku);
    return env.SKU.get(id).fetch(req);
  },
};

export class SkuObject {
  private state: DurableObjectState;

  constructor(state: DurableObjectState) {
    this.state = state;
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const json = (body: unknown, status = 200): Response =>
      new Response(JSON.stringify(body), {
        status,
        headers: { "content-type": "application/json" },
      });

    if (req.method === "PUT" && url.pathname === "/seed") {
      const stock = url.searchParams.get("stock");
      if (stock === null) return json({ error: "missing stock" }, 400);
      await this.state.storage.put("stock", Number(stock));
      return json({ ok: true, stock: Number(stock) });
    }

    if (req.method === "POST" && url.pathname === "/reserve") {
      const current = await this.state.storage.get<number>("stock");
      if (current === undefined) return json({ error: "sku not seeded" }, 404);
      if (current <= 0) return json({ soldout: true }, 409);
      const remaining = current - 1;
      await this.state.storage.put("stock", remaining);
      return json({ reserved: remaining });
    }

    if (req.method === "GET" && url.pathname === "/stock") {
      const current = await this.state.storage.get<number>("stock");
      if (current === undefined) return json({ error: "sku not seeded" }, 404);
      return json({ stock: current });
    }

    return json({ error: "not found" }, 404);
  }
}
