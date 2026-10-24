// Benchmark kernel only. Not a production authorization or HTTP implementation.
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode, Uri},
    Json, Router,
};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::{
    types::{ToSql, Type},
    NoTls,
};

struct App {
    pool: Pool,
    sql: HashMap<String, String>,
}
type Failure = Box<dyn std::error::Error + Send + Sync>;

async fn one(
    c: &tokio_postgres::Client,
    sql: &str,
    params: &[(&(dyn ToSql + Sync), Type)],
) -> Result<Option<Value>, Failure> {
    let rows = c.query_typed(sql, params).await?;
    rows.first()
        .map(|r| serde_json::from_str(r.get::<_, &str>(0)).map_err(Into::into))
        .transpose()
}

async fn handle(
    State(app): State<Arc<App>>,
    uri: Uri,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> (StatusCode, Json<Value>) {
    let c = match app.pool.get().await {
        Ok(c) => c,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({}))),
    };
    let mut locked = false;
    let mut began = false;
    let outcome: Result<(StatusCode, Value), Failure> = async {
        if uri.path() == "/action" {
            c.batch_execute("SELECT pg_advisory_lock(91011)").await?;
            locked = true;
            c.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
                .await?;
            began = true;
        }
        let token = headers
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");
        let auth = one(&c, &app.sql["auth"], &[(&token, Type::TEXT)]).await?;
        let Some(auth) = auth else {
            return Ok((StatusCode::UNAUTHORIZED, json!({})));
        };
        let region = auth["region"].as_str().ok_or("missing region")?;
        let id: i64 = q.get("id").map(|v| v.parse()).transpose()?.unwrap_or(0);
        let result = match uri.path() {
            "/queue" => {
                let priority: i32 = q
                    .get("priority")
                    .map(|v| v.parse())
                    .transpose()?
                    .unwrap_or(0);
                let after: i64 = q.get("after").map(|v| v.parse()).transpose()?.unwrap_or(0);
                let name = if q.get("layout").is_some_and(|v| v == "json") {
                    "queue_json"
                } else {
                    "queue_typed"
                };
                one(
                    &c,
                    &app.sql[name],
                    &[
                        (&region, Type::TEXT),
                        (&priority, Type::INT4),
                        (&after, Type::INT8),
                    ],
                )
                .await?
            }
            "/detail" => {
                one(
                    &c,
                    &app.sql["detail"],
                    &[(&id, Type::INT8), (&region, Type::TEXT)],
                )
                .await?
            }
            "/action" => {
                let key = q.get("key").ok_or("missing key")?;
                let receipt = one(&c, &app.sql["receipt"], &[(key, Type::TEXT)]).await?;
                let result = if receipt.is_some() {
                    receipt
                } else {
                    one(
                        &c,
                        &app.sql["action"],
                        &[(&id, Type::INT8), (&region, Type::TEXT), (key, Type::TEXT)],
                    )
                    .await?
                };
                c.batch_execute("COMMIT").await?;
                began = false;
                result
            }
            _ => None,
        };
        Ok(match result {
            Some(v) => (StatusCode::OK, v),
            None => (StatusCode::NOT_FOUND, json!({})),
        })
    }
    .await;
    let mut cleanup_failed = false;
    if began {
        cleanup_failed |= c.batch_execute("ROLLBACK").await.is_err();
    }
    if locked {
        cleanup_failed |= c
            .batch_execute("SELECT pg_advisory_unlock(91011)")
            .await
            .is_err();
    }
    if cleanup_failed {
        drop(deadpool_postgres::Object::take(c));
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({})));
    }
    let (status, value) = outcome.unwrap_or((StatusCode::INTERNAL_SERVER_ERROR, json!({})));
    (status, Json(value))
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if std::env::args().any(|s| s == "--version") {
        println!("{}", include_str!("../../rust-version.txt").trim());
        return;
    }
    let config = std::env::var("BENCH_DSN").unwrap().parse().unwrap();
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(manager).max_size(20).build().unwrap();
    let sql =
        serde_json::from_str(&std::fs::read_to_string("benchmarks/ontology/queries.json").unwrap())
            .unwrap();
    let router = Router::new()
        .fallback(handle)
        .with_state(Arc::new(App { pool, sql }));
    let listener =
        tokio::net::TcpListener::bind(format!("127.0.0.1:{}", std::env::var("PORT").unwrap()))
            .await
            .unwrap();
    println!("READY");
    axum::serve(listener, router).await.unwrap();
}
