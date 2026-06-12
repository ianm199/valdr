//! Cloudflare Worker adapter for the EdgeStash Valdr demo.
//!
//! The command engine remains in `valdr-engine` and the provider-neutral Worker
//! shape remains in `edgestash-demo`. This crate is only the Cloudflare host
//! boundary: route to a tenant Durable Object, restore/persist its snapshot, and
//! translate Worker requests/responses.

use edgestash_demo::{
    EdgeHttpMethod, EdgeHttpRequest, EdgeHttpResponse, EdgeObject, MemoryObjectStorage,
};
use worker::durable::{DurableObject, State, Storage};
use worker::*;

const DURABLE_OBJECT_BINDING: &str = "EDGESTASH";
const SNAPSHOT_KEY: &str = "valdr-engine-snapshot-v1";

#[event(fetch)]
pub async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
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
}

impl DurableObject for EdgeStashObject {
    fn new(state: State, env: Env) -> Self {
        Self { state, env }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let _ = &self.env;
        handle_durable_request(self.state.storage(), req).await
    }
}

async fn handle_durable_request(storage: Storage, mut req: Request) -> Result<Response> {
    let edge_method = edge_method(req.method());
    let path = path_and_query(&req)?;
    let body = req.bytes().await?;
    let snapshot = storage.get::<String>(SNAPSHOT_KEY).await?;
    let memory = match snapshot {
        Some(snapshot) => MemoryObjectStorage::with_snapshot_bytes(snapshot.into_bytes()),
        None => MemoryObjectStorage::default(),
    };

    let mut object = EdgeObject::open(memory).map_err(edge_error)?;
    let edge_response = object.handle_http(EdgeHttpRequest {
        method: edge_method,
        path: &path,
        body: &body,
    });

    let memory = object.into_storage();
    if let Some(snapshot) = memory.snapshot_bytes() {
        let snapshot = String::from_utf8(snapshot.to_vec())
            .map_err(|_| Error::RustError("Valdr snapshot was not UTF-8".to_owned()))?;
        storage.put(SNAPSHOT_KEY, snapshot).await?;
    }

    worker_response(edge_response)
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
}
