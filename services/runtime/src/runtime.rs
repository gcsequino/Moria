use crate::{
    database::{Database, safe_error},
    worker::Worker,
};
use anyhow::{Result, bail};
use serde_json::{Value, json};
use std::{env, time::Duration};
use uuid::Uuid;

#[tokio::main(worker_threads = 2)]
pub async fn entrypoint() {
    if let Err(error) = run().await {
        let database_code = if ["recovery", "restore-from"]
            .contains(&env::args().nth(1).as_deref().unwrap_or(""))
        {
            error.chain().find_map(|cause| {
                cause
                    .downcast_ref::<tokio_postgres::Error>()
                    .and_then(|error| error.as_db_error())
                    .map(|error| error.code().code().to_owned())
            })
        } else {
            None
        };
        if ["recovery", "restore-from"].contains(&env::args().nth(1).as_deref().unwrap_or("")) {
            eprintln!(
                "{}",
                json!({"error":safe_error(&error),"databaseCode":database_code})
            );
        } else {
            eprintln!("{}", json!({"error":safe_error(&error)}));
        }
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    crate::initialize_reader_limits()?;
    let command = env::args().nth(1).unwrap_or_else(|| "worker".into());
    if command == "schema" {
        println!(
            "{}",
            json!({"schema":serde_json::from_str::<Value>(include_str!("../schema.json"))?,"sourceDigest":env!("MORIA_INDEXING_SOURCE_SHA256"),"protocolVersion":1})
        );
        return Ok(());
    }
    let operator_command = [
        "recovery",
        "secret",
        "restore-from",
        "retention",
        "dataset-retention",
        "history-retention",
        "filesystem-retention",
    ]
    .contains(&command.as_str())
        || (command == "delivery" && env::args().nth(2).as_deref() == Some("reconcile"));
    let db = if operator_command {
        Database::configured_recovery()?
    } else {
        Database::configured()?
    };
    if command == "restore-from" {
        return crate::recovery_control::restore_from(db).await;
    }
    if operator_command {
        db.recovery_schema_lease().await?;
    } else {
        db.schema_lease().await?;
    }
    let worker = Worker {
        db: db.clone(),
        owner: Uuid::new_v4(),
    };
    match command.as_str() {
        "retention" => crate::retention::cli(db.clone(), env::args().skip(2).collect()).await?,
        "filesystem-retention" => {
            crate::filesystem_gc::cli(db.clone(), env::args().skip(2).collect()).await?
        }
        "history-retention" => {
            crate::history::cli(db.clone(), env::args().skip(2).collect()).await?
        }
        "dataset-retention" => {
            crate::dataset_retention::cli(db.clone(), env::args().skip(2).collect()).await?
        }
        "filesystem" => {
            anyhow::ensure!(
                env::args().nth(2).as_deref() == Some("configure-storage")
                    && env::args().nth(3).is_none(),
                "INPUT_INVALID"
            );
            crate::filesystem::configure_storage(db.clone()).await?;
            println!("{}", json!({"configured":true}));
        }

        "indexing-service" => {
            crate::indexing_service::cli(db.clone(), env::args().skip(2).collect()).await?
        }
        "pipelines" => crate::pipelines::cli(db.clone(), env::args().skip(2).collect()).await?,
        "directory" => crate::directory::cli(db.clone(), env::args().skip(2).collect()).await?,
        "recovery" => crate::recovery::cli(db.clone(), env::args().skip(2).collect()).await?,
        "scratch" => crate::scratch::cli(db.clone(), env::args().skip(2).collect()).await?,
        "evaluations" => crate::evaluations::cli(db.clone(), env::args().skip(2).collect()).await?,
        "agents" => crate::agents::cli(db.clone(), env::args().skip(2).collect()).await?,
        "functions" => crate::functions::cli(db.clone(), env::args().skip(2).collect()).await?,
        "search" => crate::search::cli(db.clone(), env::args().skip(2).collect()).await?,
        "models" => crate::models::cli(db.clone(), env::args().skip(2).collect()).await?,
        "changes" => crate::changes::cli(db.clone(), env::args().skip(2).collect()).await?,
        "delivery" => crate::delivery::cli(db.clone(), env::args().skip(2).collect()).await?,
        "serve" => {
            crate::serving::serve(
                db.clone(),
                env::var("MORIA_NATIVE_LISTEN")
                    .unwrap_or_else(|_| "127.0.0.1:0".into())
                    .parse()?,
            )
            .await?
        }
        "rpc" => crate::transport::serve(db.clone()).await?,
        "execution" => crate::execution::cli(db.clone(), env::args().skip(2).collect()).await?,
        "automations" => crate::automations::cli(db.clone(), env::args().skip(2).collect()).await?,
        "identity" => crate::identity::cli(db.clone(), env::args().skip(2).collect()).await?,
        "secret" => crate::secrets::cli(db.clone(), env::args().skip(2).collect()).await?,
        "plan" => {
            worker.plan().await?;
        }
        "task" => {
            worker.task().await?;
        }
        "advance" => {
            worker.advance().await?;
        }
        "once" => {
            worker.tick().await?;
        }
        "status" => {
            let client = db.get().await?;
            for row in client.query("SELECT id,status,source_revision::text,receipt,error FROM moria.indexing_builds ORDER BY created_at DESC LIMIT 100",&[]).await? {
                println!("{}",json!({"id":row.get::<_,Uuid>(0),"status":row.get::<_,String>(1),"sourceRevision":row.get::<_,String>(2),"receipt":row.get::<_,Option<Value>>(3),"error":row.get::<_,Option<String>>(4)}));
            }
        }
        "worker" | "drain" => {
            let shutdown = async {
                let mut terminate =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
                tokio::select! { result=tokio::signal::ctrl_c()=>result,_=terminate.recv()=>Ok(()) }
            };
            tokio::pin!(shutdown);
            loop {
                let worked =
                    tokio::select! { _=&mut shutdown=>break,result=worker.tick()=>result? };
                if command == "drain" && !worked {
                    let active:i64=db.get().await?.query_one("SELECT count(*) FROM moria.indexing_builds WHERE status IN ('pending','planning','running','preparing','ready')",&[]).await?.get(0);
                    if active == 0 {
                        break;
                    }
                }
                if !worked {
                    tokio::select! {_=&mut shutdown=>break,_=tokio::time::sleep(Duration::from_millis(200))=>{}}
                }
            }
        }
        "cancel" => {
            let id = Uuid::parse_str(
                &env::args()
                    .nth(2)
                    .ok_or_else(|| anyhow::anyhow!("Build ID required"))?,
            )?;
            db.get().await?.execute("WITH cancelled AS (UPDATE moria.indexing_builds SET status='cancelled' WHERE id=$1 AND status IN ('pending','planning','running','preparing','ready') RETURNING id) UPDATE moria.indexing_attempts SET outcome='cancelled',finished_at=clock_timestamp() WHERE build IN (SELECT id FROM cancelled) AND outcome='running'",&[&id]).await?;
        }
        "retry" => {
            let id = Uuid::parse_str(
                &env::args()
                    .nth(2)
                    .ok_or_else(|| anyhow::anyhow!("Build ID required"))?,
            )?;
            let mut client = db.get().await?;
            let tx = client.transaction().await?;
            tx.query_one("SELECT moria.runtime_indexing_retry_authority($1)", &[&id])
                .await?;
            tx.query_one("SELECT moria.runtime_source_store_retry($1)", &[&id])
                .await?;
            let row=tx.query_one("UPDATE moria.indexing_builds b SET status=CASE WHEN planned THEN 'running' ELSE 'pending' END,error=NULL,owner=NULL,fence=fence+1,lease_until=clock_timestamp() WHERE id=$1 AND status='failed' AND EXISTS(SELECT 1 FROM moria.dataset_heads h WHERE h.environment=b.environment AND h.dataset=b.source AND h.revision=b.source_revision) RETURNING planned",&[&id]).await?;
            tx.query_one("SELECT moria.indexing_check($1)", &[&id])
                .await?;
            tx.execute("DELETE FROM moria.indexing_rows r USING moria.indexing_tasks t WHERE r.task=t.id AND t.build=$1",&[&id]).await?;
            tx.execute(
                "DELETE FROM moria.staged_source_rows WHERE stage=$1",
                &[&id],
            )
            .await?;
            if row.get::<_, bool>(0) {
                tx.execute("UPDATE moria.indexing_tasks SET status='pending',owner=NULL,fence=fence+1,lease_until=NULL,row_count=NULL,digest=NULL WHERE build=$1",&[&id]).await?;
            } else {
                tx.execute("DELETE FROM moria.indexing_tasks WHERE build=$1", &[&id])
                    .await?;
            }
            tx.execute("UPDATE moria.source_stages SET status='loading' WHERE id=$1 AND status<>'published'",&[&id]).await?;
            tx.commit().await?;
        }
        _ => bail!("Unknown indexing command"),
    }
    Ok(())
}
