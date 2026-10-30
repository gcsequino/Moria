use anyhow::{Result, bail};
use moria_indexing::{
    database::{Database, safe_error},
    worker::Worker,
};
use serde_json::{Value, json};
use std::{env, time::Duration};
use uuid::Uuid;

#[tokio::main(worker_threads = 2)]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{}", json!({"error":safe_error(&error)}));
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let command = env::args().nth(1).unwrap_or_else(|| "worker".into());
    if command == "schema" {
        println!(
            "{}",
            json!({"schema":serde_json::from_str::<Value>(include_str!("../schema.json"))?,"sourceDigest":env!("MORIA_INDEXING_SOURCE_SHA256"),"protocolVersion":1})
        );
        return Ok(());
    }
    let db = Database::configured()?;
    db.schema_lease().await?;
    let worker = Worker {
        db: db.clone(),
        owner: Uuid::new_v4(),
    };
    match command.as_str() {
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
            let row=tx.query_one("UPDATE moria.indexing_builds b SET status=CASE WHEN planned THEN 'running' ELSE 'pending' END,error=NULL,owner=NULL,fence=fence+1,lease_until=clock_timestamp(),deadline=clock_timestamp()+(definition->>'timeoutSeconds')::integer*interval '1 second' WHERE id=$1 AND status='failed' AND EXISTS(SELECT 1 FROM moria.dataset_heads h WHERE h.environment=b.environment AND h.dataset=b.source AND h.revision=b.source_revision) RETURNING planned",&[&id]).await?;
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
