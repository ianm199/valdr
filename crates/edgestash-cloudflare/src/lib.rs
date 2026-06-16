//! Cloudflare Worker adapter for the EdgeStash Valdr demo.
//!
//! The command engine remains in `valdr-engine` and the provider-neutral Worker
//! shape remains in `edgestash-demo`. This crate is only the Cloudflare host
//! boundary: route to a tenant Durable Object, keep one hot engine instance per
//! object, translate Worker requests/responses, and flush only the keys a
//! request changed to Durable Object storage.
//!
//! Storage layout: the Durable Object `Storage` is itself a key/value store, so
//! each Redis key is held under its own storage-key (`k:<hex>`) rather than in
//! one whole-DB blob. Values are the raw UTF-8 JSON that `export_key` produces,
//! stored as `String` so they round-trip cleanly through `Storage::put`/`get`
//! and the `Storage::list` cold-start scan. A mutating request writes only the
//! keys it changed — O(dirty) async writes, not one O(state) blob rewrite.
//!
//! Time authority: every request carries `Date::now()` from the Workers
//! runtime. Client-supplied `now_millis` in request bodies is rejected unless
//! the `EDGESTASH_ALLOW_CLIENT_TIME` var is set to `"true"`, which exists for
//! deterministic local fixtures and must not be set on a real deployment.
//!
//! Concurrency: the execute-then-flush sequence relies on Durable Object input
//! gates — while a storage operation is awaited the runtime delivers no other
//! events to this object, so the hot engine cannot observe interleaved
//! requests between execution and per-key flush. If a flush fails, the hot
//! instance is discarded so the next request restores from the keys that
//! storage actually accepted.

use std::cell::RefCell;

use edgestash_demo::{
    EdgeHttpMethod, EdgeHttpRequest, EdgeHttpResponse, EdgeObject, MemoryObjectStorage,
};
use worker::durable::{DurableObject, State};
use worker::js_sys::Map as JsMap;
use worker::*;

const DURABLE_OBJECT_BINDING: &str = "EDGESTASH";
const CLIENT_TIME_VAR: &str = "EDGESTASH_ALLOW_CLIENT_TIME";

/// The interactive demo dashboard, served at `GET /`. A single self-contained
/// HTML file (no build step, no external assets) that drives the live tenant
/// routes and visualizes the Lua token bucket draining and refilling.
const DASHBOARD_HTML: &str = include_str!("../assets/dashboard.html");

#[event(fetch)]
pub async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    if let Some(response) = static_asset(edge_method(req.method()), req.url()?.path()) {
        return worker_response(response);
    }
    let Some(tenant) = tenant_from_request(&req)? else {
        return worker_response(EdgeHttpResponse {
            status: 404,
            content_type: "application/json",
            body: br#"{"error":"ERR route not found"}"#.to_vec(),
        });
    };
    let namespace = env.durable_object(DURABLE_OBJECT_BINDING)?;
    let stub = namespace.get_by_name(&tenant)?;
    stub.fetch_with_request(req).await
}

#[durable_object]
pub struct EdgeStashObject {
    state: State,
    env: Env,
    hot: RefCell<Option<EdgeObject<MemoryObjectStorage>>>,
}

impl DurableObject for EdgeStashObject {
    fn new(state: State, env: Env) -> Self {
        Self {
            state,
            env,
            hot: RefCell::new(None),
        }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let method = edge_method(req.method());
        let path = path_and_query(&req)?;
        let body = req.bytes().await?;
        let now_millis = Date::now().as_millis();

        if self.hot.borrow().is_none() {
            let entries = self.load_entries().await?;
            let object = EdgeObject::open(MemoryObjectStorage::from_entries(entries))
                .map_err(edge_error)?
                .with_client_time_allowed(client_time_allowed(&self.env));
            *self.hot.borrow_mut() = Some(object);
        }

        let (response, flush) = {
            let mut hot = self.hot.borrow_mut();
            let object = hot
                .as_mut()
                .ok_or_else(|| Error::RustError("hot engine instance missing".to_owned()))?;
            let response = object.handle_http(EdgeHttpRequest {
                method,
                path: &path,
                body: &body,
                now_millis,
            });
            let flush = drain_flush(object)?;
            (response, flush)
        };

        for (skey, value) in flush {
            let result = match value {
                Some(value) => self.state.storage().put(&skey, value).await,
                None => self.state.storage().delete(&skey).await.map(|_| ()),
            };
            if let Err(error) = result {
                *self.hot.borrow_mut() = None;
                return Err(error);
            }
        }

        worker_response(response)
    }
}

impl EdgeStashObject {
    /// Cold-start scan: read every Durable Object storage entry and turn each
    /// into a `(storage_key, value_bytes)` pair. Values were written as `String`
    /// (raw UTF-8 JSON from `export_key`), so each list value is a JS string we
    /// decode back to bytes. `EdgeObject::open` keeps only the `k:`-prefixed
    /// entries, so any unrelated bookkeeping key is harmless here.
    async fn load_entries(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let map: JsMap = self.state.storage().list().await?;
        let mut entries: Vec<(String, Vec<u8>)> = Vec::with_capacity(map.size() as usize);
        let mut decode_error: Option<Error> = None;
        map.for_each(&mut |value, key| {
            let (Some(key), Some(value)) = (key.as_string(), value.as_string()) else {
                decode_error = Some(Error::RustError(
                    "Durable Object entry was not a string".to_owned(),
                ));
                return;
            };
            entries.push((key, value.into_bytes()));
        });
        match decode_error {
            Some(error) => Err(error),
            None => Ok(entries),
        }
    }
}

/// Resolve the keys the request changed into a flush plan: for each storage-key
/// the hot store marked dirty, `Some(value)` when the key still holds a value
/// (a `put`) or `None` when it was deleted (a `delete`). The dirty set is
/// drained, so a later read-only request flushes nothing.
fn drain_flush(
    object: &mut EdgeObject<MemoryObjectStorage>,
) -> Result<Vec<(String, Option<String>)>> {
    let dirty = object.storage_mut().drain_dirty();
    let mut flush: Vec<(String, Option<String>)> = Vec::with_capacity(dirty.len());
    for skey in dirty {
        let value = match object.storage().value(&skey) {
            Some(bytes) => Some(
                String::from_utf8(bytes.to_vec())
                    .map_err(|_| Error::RustError("Valdr value was not UTF-8".to_owned()))?,
            ),
            None => None,
        };
        flush.push((skey, value));
    }
    Ok(flush)
}

fn client_time_allowed(env: &Env) -> bool {
    match env.var(CLIENT_TIME_VAR) {
        Ok(value) => value.to_string() == "true",
        Err(_) => false,
    }
}

fn tenant_from_request(req: &Request) -> Result<Option<String>> {
    let path = req.url()?.path().to_owned();
    Ok(tenant_from_path(&path))
}

fn tenant_from_path(path: &str) -> Option<String> {
    let mut segments = path.trim_start_matches('/').split('/');
    match (segments.next(), segments.next(), segments.next()) {
        (Some("v1"), Some("policy" | "limit" | "ai" | "valdr"), Some(tenant))
            if !tenant.is_empty() =>
        {
            Some(tenant.to_owned())
        }
        _ => None,
    }
}

/// Serve the demo's static assets directly from the Worker, without routing to
/// a Durable Object: `GET /` (and `/dashboard`) returns the dashboard page,
/// `GET /script` returns the exact Lua limiter source the engine runs, and
/// `GET /favicon.ico` is answered with an empty 204. Any other path returns
/// `None`, so the caller falls through to tenant routing. `HEAD` is treated
/// like `GET` so health checks and link unfurlers see the same status.
fn static_asset(method: EdgeHttpMethod, path: &str) -> Option<EdgeHttpResponse> {
    if !matches!(method, EdgeHttpMethod::Get | EdgeHttpMethod::Head) {
        return None;
    }
    match path {
        "/" | "/dashboard" => Some(EdgeHttpResponse {
            status: 200,
            content_type: "text/html; charset=utf-8",
            body: DASHBOARD_HTML.as_bytes().to_vec(),
        }),
        "/script" => Some(EdgeHttpResponse {
            status: 200,
            content_type: "text/plain; charset=utf-8",
            body: edgestash_demo::LIMITER_SCRIPT.as_bytes().to_vec(),
        }),
        "/favicon.ico" => Some(EdgeHttpResponse {
            status: 204,
            content_type: "image/x-icon",
            body: Vec::new(),
        }),
        _ => None,
    }
}

fn path_and_query(req: &Request) -> Result<String> {
    let url = req.url()?;
    let mut path = url.path().to_owned();
    if let Some(query) = url.query() {
        path.push('?');
        path.push_str(query);
    }
    Ok(path)
}

fn edge_method(method: Method) -> EdgeHttpMethod {
    match method {
        Method::Get => EdgeHttpMethod::Get,
        Method::Post => EdgeHttpMethod::Post,
        Method::Put => EdgeHttpMethod::Put,
        Method::Head => EdgeHttpMethod::Head,
        _ => EdgeHttpMethod::Other,
    }
}

fn worker_response(response: EdgeHttpResponse) -> Result<Response> {
    let headers = Headers::new();
    headers.set("content-type", response.content_type)?;
    Ok(Response::from_bytes(response.body)?
        .with_headers(headers)
        .with_status(response.status))
}

fn edge_error(error: edgestash_demo::EdgeError) -> Error {
    Error::RustError(format!("EdgeStash error: {error:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_routes_cover_public_edge_paths() {
        assert_eq!(
            tenant_from_path("/v1/policy/tenant-42"),
            Some("tenant-42".to_owned())
        );
        assert_eq!(
            tenant_from_path("/v1/limit/tenant-42"),
            Some("tenant-42".to_owned())
        );
        assert_eq!(
            tenant_from_path("/v1/ai/tenant-42"),
            Some("tenant-42".to_owned())
        );
        assert_eq!(
            tenant_from_path("/v1/valdr/tenant-42/GET/foo"),
            Some("tenant-42".to_owned())
        );
        assert_eq!(tenant_from_path("/v1/cache/tenant-42/GET/foo"), None);
    }

    #[test]
    fn static_assets_serve_dashboard_and_script_without_a_tenant() {
        let dash = static_asset(EdgeHttpMethod::Get, "/").expect("dashboard at /");
        assert_eq!(dash.status, 200);
        assert_eq!(dash.content_type, "text/html; charset=utf-8");
        assert!(dash.body.windows(9).any(|w| w == b"EdgeStash"));

        let alias = static_asset(EdgeHttpMethod::Get, "/dashboard").expect("dashboard alias");
        assert_eq!(alias.body, dash.body);

        let script = static_asset(EdgeHttpMethod::Get, "/script").expect("script at /script");
        assert_eq!(script.status, 200);
        assert_eq!(script.content_type, "text/plain; charset=utf-8");
        assert_eq!(script.body, edgestash_demo::LIMITER_SCRIPT.as_bytes());
    }

    #[test]
    fn static_assets_fall_through_for_tenant_routes_and_non_get() {
        assert!(static_asset(EdgeHttpMethod::Get, "/v1/ai/tenant-42").is_none());
        assert!(static_asset(EdgeHttpMethod::Get, "/v1/policy/tenant-42").is_none());
        assert!(static_asset(EdgeHttpMethod::Post, "/").is_none());
        assert!(static_asset(EdgeHttpMethod::Put, "/script").is_none());
        assert_eq!(
            static_asset(EdgeHttpMethod::Get, "/favicon.ico").map(|r| r.status),
            Some(204)
        );
    }
}
