use crate::{
    authority::{Authority, VerifiedSession},
    canonical::fingerprint,
    database::{Database, safe_error},
    http_auth::HttpAuthentication,
    serving::{Admission, NativeServing},
};
use anyhow::{Result, ensure};
use axum::{
    body::{Body, to_bytes},
    http::{Request, Response, StatusCode, header},
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;
const VERSION: &str = "2026-07-28";
const MAX_RESPONSE: usize = 8 * 1024 * 1024 + 65536;
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Profile {
    environment: String,
    application: String,
    objects: Vec<Uuid>,
    actions: Vec<Uuid>,
}
#[derive(Clone)]
pub(crate) struct NativeMcp {
    db: Database,
    profiles: Arc<BTreeMap<(String, String), Profile>>,
}
#[derive(Clone)]
struct Item {
    resource: Uuid,
    action: bool,
}
impl Item {
    fn key(&self) -> String {
        format!(
            "{}_{}",
            if self.action { "action" } else { "query" },
            self.resource
        )
    }
    fn uri(&self) -> String {
        format!(
            "moria://resource/{}/{}",
            if self.action { "Action" } else { "ObjectType" },
            self.resource
        )
    }
}
fn reply(status: StatusCode, value: Value) -> Response<Body> {
    let bytes = serde_json::to_vec(&value).unwrap();
    if bytes.len() > MAX_RESPONSE {
        return error(
            None,
            StatusCode::PAYLOAD_TOO_LARGE,
            1001,
            "RESPONSE_TOO_LARGE",
            None,
        );
    }
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .header("mcp-protocol-version", VERSION)
        .header("x-content-type-options", "nosniff")
        .body(Body::from(bytes))
        .unwrap()
}
fn error(
    id: Option<&Value>,
    status: StatusCode,
    code: i64,
    message: &str,
    data: Option<Value>,
) -> Response<Body> {
    let mut value = json!({"jsonrpc":"2.0","error":{"code":code,"message":message}});
    if let Some(id) = id {
        value["id"] = id.clone();
    }
    if let Some(data) = data {
        value["error"]["data"] = data;
    }
    reply(status, value)
}
fn header_name(value: &str) -> Result<String> {
    if let Some(encoded) = value
        .strip_prefix("=?base64?")
        .and_then(|v| v.strip_suffix("?="))
    {
        ensure!(encoded.len() <= 16384, "INPUT_INVALID");
        return Ok(String::from_utf8(STANDARD.decode(encoded)?)?);
    }
    ensure!(
        !value.starts_with(' ')
            && !value.ends_with(' ')
            && value.bytes().all(|b| (32..=126).contains(&b) || b == b'\t'),
        "INPUT_INVALID"
    );
    Ok(value.into())
}
fn keys(value: &Value, allowed: &[&str]) -> Result<()> {
    ensure!(
        value
            .as_object()
            .is_some_and(|v| v.keys().all(|k| allowed.contains(&k.as_str()))),
        "INPUT_INVALID"
    );
    Ok(())
}
impl NativeMcp {
    pub(crate) fn configured(db: Database) -> Result<Self> {
        let encoded = std::env::var("MORIA_NATIVE_MCP_PROFILES").unwrap_or_else(|_| "[]".into());
        ensure!(encoded.len() <= 16 * 1024 * 1024, "Invalid MCP profiles");
        let configured: Vec<Profile> = serde_json::from_str(&encoded)?;
        ensure!(configured.len() <= 64, "Invalid MCP profiles");
        let mut profiles = BTreeMap::new();
        for profile in configured {
            ensure!(
                !profile.environment.is_empty()
                    && !profile.application.is_empty()
                    && profile.objects.len() <= 2000
                    && profile.actions.len() <= 2000
                    && profile.objects.iter().collect::<BTreeSet<_>>().len()
                        == profile.objects.len()
                    && profile.actions.iter().collect::<BTreeSet<_>>().len()
                        == profile.actions.len(),
                "Invalid MCP profiles"
            );
            ensure!(
                profiles
                    .insert(
                        (profile.environment.clone(), profile.application.clone()),
                        profile
                    )
                    .is_none(),
                "Invalid MCP profiles"
            );
        }
        Ok(Self {
            db,
            profiles: Arc::new(profiles),
        })
    }
    pub(crate) async fn handle(
        &self,
        authentication: &HttpAuthentication,
        remote: SocketAddr,
        request: Request<Body>,
    ) -> Response<Body> {
        if request.headers().contains_key("x-moria-key") {
            return error(
                None,
                StatusCode::FORBIDDEN,
                1001,
                "Dedicated bearer or browser session required",
                None,
            );
        }
        let session = match authentication
            .authenticate(remote, request.headers(), request.method())
            .await
        {
            Ok(session) => session,
            Err(e) => {
                let code = safe_error(&e);
                return error(
                    None,
                    if code == "UNAUTHENTICATED" {
                        StatusCode::UNAUTHORIZED
                    } else {
                        StatusCode::FORBIDDEN
                    },
                    1001,
                    code,
                    None,
                );
            }
        };
        if request.method() != "POST" {
            let mut response = error(
                None,
                StatusCode::METHOD_NOT_ALLOWED,
                -32600,
                "MCP requires POST",
                None,
            );
            response
                .headers_mut()
                .insert(header::ALLOW, header::HeaderValue::from_static("POST"));
            return response;
        }
        if request.uri().query().is_some()
            || !request
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| {
                    v.split(';')
                        .next()
                        .is_some_and(|v| v.eq_ignore_ascii_case("application/json"))
                })
            || request
                .headers()
                .get(header::CONTENT_ENCODING)
                .is_some_and(|v| v != "identity")
        {
            return error(
                None,
                StatusCode::BAD_REQUEST,
                -32600,
                "JSON request required",
                None,
            );
        }
        let accept = request
            .headers()
            .get(header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !accept
            .split(',')
            .any(|v| v.trim().split(';').next() == Some("application/json"))
            || !accept
                .split(',')
                .any(|v| v.trim().split(';').next() == Some("text/event-stream"))
        {
            return error(
                None,
                StatusCode::NOT_ACCEPTABLE,
                -32600,
                "MCP requires JSON and event-stream acceptance",
                None,
            );
        }
        let headers = request.headers().clone();
        let body = match tokio::time::timeout(
            Duration::from_secs(30),
            to_bytes(request.into_body(), 65536),
        )
        .await
        {
            Ok(Ok(bytes)) => bytes,
            _ => {
                return error(
                    None,
                    StatusCode::PAYLOAD_TOO_LARGE,
                    -32600,
                    "Request limit exceeded",
                    None,
                );
            }
        };
        let value: Value = match serde_json::from_slice(&body) {
            Ok(value) => value,
            Err(_) => return error(None, StatusCode::BAD_REQUEST, -32700, "Parse error", None),
        };
        let id = value
            .get("id")
            .filter(|id| id.is_string() || id.as_i64().is_some() || id.as_u64().is_some());
        let method = value["method"].as_str().unwrap_or("");
        let params = &value["params"];
        if value["jsonrpc"] != "2.0"
            || id.is_none()
            || keys(&value, &["jsonrpc", "id", "method", "params"]).is_err()
            || !params.is_object()
        {
            return error(id, StatusCode::BAD_REQUEST, -32600, "Invalid request", None);
        }
        let version = params["_meta"]["io.modelcontextprotocol/protocolVersion"].as_str();
        if version.is_none()
            || !params["_meta"]["io.modelcontextprotocol/clientCapabilities"].is_object()
        {
            return error(
                id,
                StatusCode::BAD_REQUEST,
                -32602,
                "Required request metadata missing",
                None,
            );
        }
        let version = version.unwrap();
        if headers
            .get("mcp-protocol-version")
            .and_then(|v| v.to_str().ok())
            != Some(version)
            || headers.get("mcp-method").and_then(|v| v.to_str().ok()) != Some(method)
        {
            return error(id, StatusCode::BAD_REQUEST, -32020, "Header mismatch", None);
        }
        if version != VERSION {
            return error(
                id,
                StatusCode::BAD_REQUEST,
                -32022,
                "Unsupported protocol version",
                Some(json!({"supported":[VERSION],"requested":version})),
            );
        }
        if ["tools/call", "resources/read"].contains(&method) {
            let name = params[if method == "tools/call" {
                "name"
            } else {
                "uri"
            }]
            .as_str();
            if name.is_none()
                || headers
                    .get("mcp-name")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| header_name(v).ok())
                    .as_deref()
                    != name
            {
                return error(id, StatusCode::BAD_REQUEST, -32020, "Header mismatch", None);
            }
        }
        if ![
            "server/discover",
            "tools/list",
            "resources/list",
            "resources/read",
            "tools/call",
        ]
        .contains(&method)
        {
            return error(
                id,
                StatusCode::NOT_FOUND,
                -32601,
                "Method not found; supported protocol 2026-07-28",
                None,
            );
        }
        let profile = match self
            .profiles
            .get(&(session.environment().into(), session.application().into()))
        {
            Some(profile) => profile,
            None => return error(id, StatusCode::FORBIDDEN, 1001, "FORBIDDEN", None),
        };
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            self.dispatch(&session, profile, method, params),
        )
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")));
        match result {
            Ok(mut result) => {
                result["resultType"] = json!("complete");
                result["_meta"]["io.modelcontextprotocol/serverInfo"] =
                    json!({"name":"moria","version":env!("CARGO_PKG_VERSION")});
                reply(
                    StatusCode::OK,
                    json!({"jsonrpc":"2.0","id":id.unwrap(),"result":result}),
                )
            }
            Err(e) => error(id, StatusCode::BAD_REQUEST, -32602, safe_error(&e), None),
        }
    }
    fn items(profile: &Profile) -> Vec<Item> {
        let mut items = profile
            .objects
            .iter()
            .map(|id| Item {
                resource: *id,
                action: false,
            })
            .chain(profile.actions.iter().map(|id| Item {
                resource: *id,
                action: true,
            }))
            .collect::<Vec<_>>();
        items.sort_by_key(Item::key);
        items
    }
    async fn metadata(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        item: &Item,
    ) -> Result<Value> {
        let resource = item.resource.to_string();
        let (kind, revision, shape) = if item.action {
            NativeServing::action_markings(client, session, &resource).await?;
            let row=client.query_one("SELECT revision,definition->'inputs' FROM moria.native_serving_actions WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&resource]).await?;
            (
                "Action",
                row.get::<_, String>(0),
                json!({"inputs":row.get::<_,Value>(1)}),
            )
        } else {
            NativeServing::protected_candidates(client, session, &resource, &[], None).await?;
            let row=client.query_one("SELECT revision,definition->'schema' FROM moria.native_serving_bindings WHERE environment=$1 AND object_type_id=$2 AND enabled",&[&session.environment(),&item.resource]).await?;
            (
                "ObjectType",
                row.get::<_, String>(0),
                json!({"schema":row.get::<_,Value>(1)}),
            )
        };
        let title = client
            .query_opt(
                "SELECT reference FROM moria.fs_resource_identities WHERE resource_id=$1",
                &[&item.resource],
            )
            .await?
            .map(|row| row.get::<_, String>(0))
            .unwrap_or_else(|| resource.clone());
        Ok(json!({"resource":resource,"kind":kind,"revision":revision,"title":title,"shape":shape}))
    }
    async fn dispatch(
        &self,
        session: &VerifiedSession,
        profile: &Profile,
        method: &str,
        params: &Value,
    ) -> Result<Value> {
        if method == "server/discover" {
            keys(params, &["_meta"])?;
            let admission = Admission::begin(&self.db, session, false).await?;
            let result=Authority::current(admission.client(),session).await.map(|_|json!({"supportedVersions":[VERSION],"capabilities":{"tools":{},"resources":{}},"ttlMs":0,"cacheScope":"private"}));
            return admission.complete(result).await;
        }
        let items = Self::items(profile);
        if method == "tools/call" {
            keys(params, &["_meta", "name", "arguments"])?;
            let name = params["name"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
            let item = items
                .iter()
                .find(|item| item.key() == name)
                .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?;
            let arguments = &params["arguments"];
            let serving = NativeServing {
                db: self.db.clone(),
            };
            let result = if item.action {
                keys(arguments, &["idempotencyKey", "input"])?;
                let key = arguments["idempotencyKey"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("IDEMPOTENCY_KEY_INVALID"))?;
                serving
                    .invoke(
                        session,
                        &item.resource.to_string(),
                        key,
                        &arguments["input"],
                    )
                    .await
            } else {
                keys(arguments, &["query"])?;
                serving
                    .query(session, &item.resource.to_string(), &arguments["query"])
                    .await
            };
            return Ok(match result {
                Ok(value) => {
                    json!({"content":[{"type":"text","text":"Result returned in structuredContent."}],"structuredContent":value,"isError":false})
                }
                Err(error) => {
                    json!({"content":[{"type":"text","text":safe_error(&error)}],"isError":true})
                }
            });
        }
        let admission = Admission::begin(&self.db, session, false).await?;
        let result = self
            .discover_in(admission.client(), session, profile, method, params, &items)
            .await;
        admission.complete(result).await
    }
    async fn discover_in(
        &self,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        profile: &Profile,
        method: &str,
        params: &Value,
        items: &[Item],
    ) -> Result<Value> {
        let state = Authority::current(client, session).await?;
        if method == "resources/read" {
            keys(params, &["_meta", "uri"])?;
            let uri = params["uri"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
            let item = items
                .iter()
                .find(|item| item.uri() == uri)
                .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?;
            let value = Self::metadata(client, session, item).await?;
            Authority::finish(client, session, &state).await?;
            return Ok(
                json!({"contents":[{"uri":uri,"mimeType":"application/json","text":serde_json::to_string(&value)?}],"ttlMs":0,"cacheScope":"private"}),
            );
        }
        keys(params, &["_meta", "cursor"])?;
        let actor = fingerprint(&json!([
            session.environment(),
            session.principal(),
            session.application()
        ]))?;
        let profile_hash = fingerprint(&json!([
            profile.environment,
            profile.application,
            profile.objects,
            profile.actions
        ]))?;
        let mut after = String::new();
        let mut expiry: Option<String> = None;
        if let Some(cursor) = params.get("cursor") {
            let token = cursor
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("CURSOR_INVALIDATED"))?;
            ensure!(token.len() == 43, "CURSOR_INVALIDATED");
            let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
            let row=client.query_opt("SELECT after_key,expires_at::text FROM moria.runtime_mcp_cursors WHERE token_hash=$1 AND environment=$2 AND actor_hash=$3 AND profile_hash=$4 AND method=$5 AND security_epoch=$6 AND instance_epoch=$7 AND expires_at>clock_timestamp()",&[&hash,&session.environment(),&actor,&profile_hash,&method,&state.security_epoch.parse::<i64>()?,&state.instance_epoch.parse::<i64>()?]).await?.ok_or_else(||anyhow::anyhow!("CURSOR_INVALIDATED"))?;
            after = row.get(0);
            expiry = Some(row.get(1));
        }
        let mut values = Vec::new();
        let mut last = after.clone();
        let mut more = false;
        let mut page_bytes = 0usize;
        for (scanned, item) in items.iter().filter(|item| item.key() > after).enumerate() {
            if scanned == 100 || values.len() == 50 {
                more = true;
                break;
            }
            let previous = last.clone();
            last = item.key();
            let operation = format!(
                "{}:{}",
                item.resource,
                if item.action { "invoke" } else { "read" }
            );
            if !state.identity.granted_operations.contains(&operation)
                || !state.identity.application_scopes.contains(&operation)
            {
                continue;
            }
            client.batch_execute("SAVEPOINT mcp_resource").await?;
            let metadata = match Self::metadata(client, session, item).await {
                Ok(value) => {
                    client
                        .batch_execute("RELEASE SAVEPOINT mcp_resource")
                        .await?;
                    value
                }
                Err(error) => {
                    client
                        .batch_execute(
                            "ROLLBACK TO SAVEPOINT mcp_resource; RELEASE SAVEPOINT mcp_resource",
                        )
                        .await?;
                    let code = safe_error(&error);
                    if ["FORBIDDEN", "NOT_FOUND", "SCHEMA_CHANGED"].contains(&code) {
                        continue;
                    }
                    return Err(error);
                }
            };
            let entry = if method == "resources/list" {
                json!({"uri":item.uri(),"name":metadata["title"],"mimeType":"application/json"})
            } else {
                json!({"name":item.key(),"title":metadata["title"],"description":if item.action{"Invoke the scoped action with a stable idempotency key."}else{"Query the scoped object resource under current row and property policies."},
                    "inputSchema":if item.action{json!({"type":"object","properties":{"idempotencyKey":{"type":"string","minLength":1,"maxLength":256},"input":crate::values::wire_schema(&json!({"type":"object","properties":metadata["shape"]["inputs"]}))?},"required":["idempotencyKey","input"],"additionalProperties":false})}else{json!({"type":"object","properties":{"query":{"type":"object"}},"required":["query"],"additionalProperties":false})},
                    "annotations":{"readOnlyHint":!item.action,"idempotentHint":true,"openWorldHint":item.action}})
            };
            let size = serde_json::to_vec(&entry)?.len();
            ensure!(size <= 8 * 1024 * 1024, "RESOURCE_LIMIT");
            if !values.is_empty() && page_bytes + size > 4 * 1024 * 1024 {
                last = previous;
                more = true;
                break;
            }
            page_bytes += size;
            values.push(entry);
        }
        let mut result = if method == "resources/list" {
            json!({"resources":values,"ttlMs":0,"cacheScope":"private"})
        } else {
            json!({"tools":values,"ttlMs":0,"cacheScope":"private"})
        };
        if more {
            let mut random = [0u8; 32];
            getrandom::fill(&mut random)?;
            let token = URL_SAFE_NO_PAD.encode(random);
            let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
            client.execute("INSERT INTO moria.runtime_mcp_cursors(token_hash,environment,actor_hash,profile_hash,method,security_epoch,instance_epoch,after_key,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,COALESCE($9::text::timestamptz,clock_timestamp()+interval '15 minutes'))",&[&hash,&session.environment(),&actor,&profile_hash,&method,&state.security_epoch.parse::<i64>()?,&state.instance_epoch.parse::<i64>()?,&last,&expiry]).await?;
            result["nextCursor"] = json!(token);
        }
        Authority::finish(client, session, &state).await?;
        Ok(result)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mirrored_names_are_exact_and_not_authority() {
        assert_eq!(header_name("query_test").unwrap(), "query_test");
        assert_eq!(header_name("=?base64?5LiW55WM?=").unwrap(), "世界");
        assert!(header_name(" padded ").is_err());
        assert!(header_name("=?base64?invalid?=").is_err());
    }
}
