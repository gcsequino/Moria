use crate::{database::Database, models::NativeModels};
use anyhow::{Result, ensure};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{ConnectInfo, Path, State},
    http::{Request, StatusCode, header},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::post,
};
use futures::StreamExt;
use serde_json::{Value, json};
use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(Clone)]
struct HttpState {
    gateway: NativeModels,
    authentication: crate::http_auth::HttpAuthentication,
    admission: Arc<Semaphore>,
    global_admission: Arc<Semaphore>,
}

pub(crate) async fn serve(db: Database, address: SocketAddr) -> Result<()> {
    ensure!(
        address.ip().is_loopback(),
        "Loopback model listener required"
    );
    let listener = tokio::net::TcpListener::bind(address).await?;
    let address = listener.local_addr()?;
    let router = router(db, address.to_string(), Arc::new(Semaphore::new(64)))?;
    println!(
        "{}",
        json!({"event":"modelsServing","address":address.to_string()})
    );
    axum::serve(
        crate::http_admission::BoundedListener::new(listener),
        router.into_make_service_with_connect_info::<crate::http_admission::Peer>(),
    )
    .with_graceful_shutdown(async {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}}
    })
    .await?;
    Ok(())
}

pub(crate) fn router(
    db: Database,
    host: String,
    global_admission: Arc<Semaphore>,
) -> Result<Router> {
    let state = HttpState {
        gateway: NativeModels::new(db.clone())?,
        authentication: crate::http_auth::HttpAuthentication::configured(db, &host)?,
        admission: Arc::new(Semaphore::new(32)),
        global_admission,
    };
    let router = Router::new()
        .route("/api/v1/models/{resource}/{operation}", post(handle))
        .with_state(state);
    Ok(router)
}

async fn handle(
    State(state): State<HttpState>,
    ConnectInfo(remote): ConnectInfo<crate::http_admission::Peer>,
    Path((resource, operation)): Path<(String, String)>,
    request: Request<Body>,
) -> Response {
    let mut permits = Vec::with_capacity(2);
    for budget in [&state.global_admission, &state.admission] {
        match budget.clone().try_acquire_owned() {
            Ok(permit) => permits.push(permit),
            Err(_) => {
                let mut response = (
                    StatusCode::TOO_MANY_REQUESTS,
                    axum::Json(json!({"error":"RESOURCE_LIMIT"})),
                )
                    .into_response();
                response.headers_mut().insert(
                    header::CACHE_CONTROL,
                    header::HeaderValue::from_static("no-store"),
                );
                return response;
            }
        }
    }
    let processing = remote.processing();
    let result = tokio::time::timeout(
        Duration::from_secs(125),
        dispatch(state, remote.0, resource, operation, request),
    )
    .await
    .unwrap_or_else(|_| Err(anyhow::anyhow!("MODEL_INCOMPLETE")));
    drop(processing);
    let mut response = match result {
        Ok(response) => response,
        Err(error) => {
            let code = error_code(&error);
            let status = match code {
                "UNAUTHENTICATED" => StatusCode::UNAUTHORIZED,
                "FORBIDDEN"
                | "DESTINATION_FORBIDDEN"
                | "AUTHORIZATION_CHANGED"
                | "SECRET_CONSUMER_DENIED"
                | "SECRET_APPROVAL_REQUIRED"
                | "SECRET_APPROVAL_REVOKED" => StatusCode::FORBIDDEN,
                "NOT_FOUND" => StatusCode::NOT_FOUND,
                "RESULT_EXPIRED" => StatusCode::GONE,
                "RESOURCE_LIMIT" => StatusCode::TOO_MANY_REQUESTS,
                "SECRET_CONFIGURATION_UNAVAILABLE" => StatusCode::SERVICE_UNAVAILABLE,
                "SECRET_SOURCE_CHANGED"
                | "SECRET_APPROVAL_IMMUTABLE"
                | "SECRET_APPROVAL_CONFLICT"
                | "CELL_OWNERSHIP_LOST"
                | "IDEMPOTENCY_CONFLICT"
                | "MODEL_CALL_IN_PROGRESS"
                | "MODEL_CALL_UNAVAILABLE" => StatusCode::CONFLICT,
                "MODEL_UNAVAILABLE" | "MODEL_INCOMPLETE" | "MODEL_PROTOCOL"
                | "MODEL_ZERO_VECTOR" => StatusCode::BAD_GATEWAY,
                "INTERNAL" => StatusCode::INTERNAL_SERVER_ERROR,
                _ => StatusCode::BAD_REQUEST,
            };
            (status, axum::Json(json!({"error":code}))).into_response()
        }
    };
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    crate::http_admission::hold_response(response, permits)
}

async fn dispatch(
    state: HttpState,
    remote: SocketAddr,
    resource: String,
    operation: String,
    request: Request<Body>,
) -> Result<Response> {
    ensure!(
        request.uri().query().is_none() && resource.len() <= 1024,
        "MODEL_REQUEST_INVALID"
    );
    ensure!(
        request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v
                .split(';')
                .next()
                .is_some_and(|v| v.eq_ignore_ascii_case("application/json"))),
        "MODEL_REQUEST_INVALID"
    );
    ensure!(
        request
            .headers()
            .get(header::CONTENT_ENCODING)
            .is_none_or(|value| value.to_str().is_ok_and(|value| value == "identity")),
        "MODEL_REQUEST_INVALID"
    );
    let session = state
        .authentication
        .authenticate(remote, request.headers(), request.method())
        .await?;
    let key = request
        .headers()
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let body = tokio::time::timeout(
        Duration::from_secs(10),
        to_bytes(request.into_body(), 256 * 1024),
    )
    .await
    .map_err(|_| anyhow::anyhow!("MODEL_REQUEST_INVALID"))?
    .map_err(|_| anyhow::anyhow!("MODEL_REQUEST_INVALID"))?;
    let input: Value =
        serde_json::from_slice(&body).map_err(|_| anyhow::anyhow!("MODEL_REQUEST_INVALID"))?;
    if operation == "capabilities" {
        ensure!(
            input.as_object().is_some_and(|o| o.is_empty()),
            "MODEL_REQUEST_INVALID"
        );
        return Ok(
            axum::Json(state.gateway.capabilities(&session, &resource).await?).into_response(),
        );
    }
    if operation == "status" || operation == "cancel" {
        ensure!(
            input.as_object().is_some_and(|o| o.len() == 1),
            "MODEL_REQUEST_INVALID"
        );
        let key = input["key"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("MODEL_REQUEST_INVALID"))?;
        return Ok(axum::Json(
            state
                .gateway
                .inspect(&session, &resource, key, operation == "cancel")
                .await?,
        )
        .into_response());
    }
    if operation == "probe" {
        ensure!(
            input.as_object().is_some_and(|o| o.is_empty()),
            "MODEL_REQUEST_INVALID"
        );
        return Ok(axum::Json(state.gateway.probe(&session, &resource).await?).into_response());
    }
    let key = key.ok_or_else(|| anyhow::anyhow!("MODEL_CALL_KEY_INVALID"))?;
    if operation == "stream" {
        let receiver = state
            .gateway
            .stream(&session, &resource, &key, &input)
            .await?;
        let stream = tokio_stream::wrappers::ReceiverStream::new(receiver)
            .enumerate()
            .map(|(index, item)| {
                let (kind, value) = match item {
                    Ok(value) => (value["kind"].as_str().unwrap_or("event").to_owned(), value),
                    Err(error) => ("error".into(), json!({"error":error_code(&error)})),
                };
                Ok::<Event, Infallible>(
                    Event::default()
                        .event(kind)
                        .id((index + 1).to_string())
                        .data(value.to_string()),
                )
            });
        return Ok(Sse::new(stream)
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(10)))
            .into_response());
    }
    let result = match operation.as_str() {
        "chat" => {
            state
                .gateway
                .chat(&session, &resource, &key, &input)
                .await?
        }
        "embed" => {
            state
                .gateway
                .embed(&session, &resource, &key, &input)
                .await?
        }
        _ => anyhow::bail!("NOT_FOUND"),
    };
    Ok(axum::Json(result).into_response())
}

pub(crate) fn error_code(error: &anyhow::Error) -> &'static str {
    domain_error_code(error).unwrap_or_else(|| crate::serving::error_code(error))
}

pub(crate) fn domain_error_code(error: &anyhow::Error) -> Option<&'static str> {
    const MODEL_CODES: &[&str] = &[
        "MODEL_REQUEST_INVALID",
        "MODEL_CALL_KEY_INVALID",
        "MODEL_BINDING_INVALID",
        "MODEL_SCHEMA_INVALID",
        "MODEL_SCHEMA_LIMIT",
        "MODEL_PROTOCOL",
        "MODEL_INCOMPLETE",
        "MODEL_UNAVAILABLE",
        "MODEL_ZERO_VECTOR",
        "MODEL_CONTEXT_LIMIT",
        "MODEL_CALL_UNAVAILABLE",
        "MODEL_CALL_IN_PROGRESS",
        "MODEL_BINDING_CHANGED",
        "MODEL_CANCELLED",
        "MODEL_CONFIGURATION_UNAVAILABLE",
        "UNSUPPORTED_CAPABILITY",
        "CELL_OWNERSHIP_LOST",
        "IDEMPOTENCY_CONFLICT",
        "RESOURCE_LIMIT",
        "RESULT_EXPIRED",
    ];
    for cause in error.chain() {
        let value = cause.to_string();
        if let Some(code) = MODEL_CODES.iter().find(|code| **code == value) {
            return Some(code);
        }
    }
    None
}
