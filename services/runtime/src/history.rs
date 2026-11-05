use crate::database::Database;
use anyhow::{Result, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, stdin};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Approval {
    environment: String,
    request_key: String,
    retain_seconds: i64,
    keep_generations: i32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    id: Uuid,
}

pub async fn cli(db: Database, arguments: Vec<String>) -> Result<()> {
    ensure!(arguments.len() == 1, "INPUT_INVALID");
    let mut bytes = Vec::new();
    stdin().take(16385).read_to_end(&mut bytes).await?;
    ensure!(bytes.len() <= 16384, "INPUT_INVALID");
    if arguments[0] == "once" {
        let request: Reference = serde_json::from_slice(&bytes)?;
        let staging = bounded_step(
            &db,
            request.id,
            "SELECT moria.runtime_indexing_staging_once($1)",
        )
        .await?;
        let mut history =
            bounded_step(&db, request.id, "SELECT moria.runtime_history_step($1)").await?;
        if matches!(history["state"].as_str(), Some("complete" | "planRequired"))
            && staging["pending"] == true
        {
            history["state"] = if matches!(staging["state"].as_str(), Some("busy" | "backupBusy")) {
                json!("busy")
            } else {
                json!("indexingStaging")
            };
        }
        history["indexingStaging"] = staging;
        println!("{history}");
        return Ok(());
    }
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    tx.batch_execute("SET LOCAL statement_timeout='5s'; SET LOCAL idle_in_transaction_session_timeout='5s'; SET LOCAL lock_timeout='250ms'").await?;
    tx.query_one("SELECT moria.runtime_history_operator()", &[])
        .await?;
    let result = if arguments[0] == "approve" {
        let request: Approval = serde_json::from_slice(&bytes)?;
        ensure!(
            !request.environment.is_empty()
                && request.environment.len() <= 1024
                && (1..=256).contains(&request.request_key.len())
                && (86400..=315360000).contains(&request.retain_seconds)
                && (2..=100).contains(&request.keep_generations),
            "INPUT_INVALID"
        );
        let id: Uuid = tx
            .query_one(
                "SELECT moria.runtime_history_approve($1,$2,$3,$4)",
                &[
                    &request.environment,
                    &request.request_key,
                    &request.retain_seconds,
                    &request.keep_generations,
                ],
            )
            .await?
            .get(0);
        json!({"policy":id})
    } else {
        let request: Reference = serde_json::from_slice(&bytes)?;
        match arguments[0].as_str() {
            "disable" => {
                ensure!(tx.execute("UPDATE moria.runtime_history_policies SET enabled=false WHERE id=$1", &[&request.id]).await? == 1, "NOT_FOUND");
                json!({"policy":request.id,"enabled":false})
            }
            "plan" => {
                let id: Uuid = tx.query_one("SELECT moria.runtime_history_plan($1)", &[&request.id]).await?.get(0);
                json!({"collection":id})
            }
            "once" => tx.query_one("SELECT moria.runtime_history_step($1)", &[&request.id]).await?.get::<_,Value>(0),
            "inspect" => tx.query_opt("SELECT to_jsonb(p)||jsonb_build_object('retainSeconds',p.retain_seconds::text,'collections',COALESCE((SELECT jsonb_agg(to_jsonb(c)||jsonb_build_object('floor',c.floor::text,'removedRows',c.removed_rows::text,'droppedTables',c.dropped_tables::text)) FROM (SELECT * FROM moria.runtime_history_collections WHERE policy=p.id ORDER BY observed_at DESC,id DESC LIMIT 20)c),'[]'::jsonb)) FROM moria.runtime_history_policies p WHERE p.id=$1", &[&request.id]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?.get(0),
            _ => anyhow::bail!("INPUT_INVALID"),
        }
    };
    tx.commit().await?;
    println!("{result}");
    Ok(())
}

async fn bounded_step(db: &Database, policy: Uuid, statement: &str) -> Result<Value> {
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    tx.batch_execute("SET LOCAL statement_timeout='5s'; SET LOCAL idle_in_transaction_session_timeout='5s'; SET LOCAL lock_timeout='250ms'").await?;
    tx.query_one("SELECT moria.runtime_history_operator()", &[])
        .await?;
    let result = tx.query_one(statement, &[&policy]).await?.get(0);
    tx.commit().await?;
    Ok(result)
}
