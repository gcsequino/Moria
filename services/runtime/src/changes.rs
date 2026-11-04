use crate::database::Database;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Position {
    pub incarnation: Uuid,
    pub environment: String,
    pub commit_revision: String,
}

/// Trusted internal replay API; never registered on the public object HTTP router.
pub struct ChangeLog {
    pub db: Database,
}

impl ChangeLog {
    pub async fn position(&self, environment: &str) -> Result<Position> {
        let client = self.db.get().await?;
        let row=client.query_one("SELECT i.incarnation,COALESCE(max(t.commit_revision),0)::text FROM moria.indexing_instance i CROSS JOIN moria.environments e LEFT JOIN moria.runtime_change_transactions t ON t.environment=e.id AND t.incarnation=i.incarnation AND t.complete WHERE e.id=$1 GROUP BY i.incarnation",&[&environment]).await?;
        Ok(Position {
            incarnation: row.get(0),
            environment: environment.to_owned(),
            commit_revision: row.get(1),
        })
    }

    pub async fn read(
        &self,
        after: &Position,
        transactions: i64,
        item_budget: i64,
    ) -> Result<Value> {
        ensure!(
            (1..=100).contains(&transactions) && (1..=10000).contains(&item_budget),
            "Invalid change budget"
        );
        let revision: i64 = after.commit_revision.parse()?;
        ensure!(revision >= 0, "Invalid change position");
        let mut client = self.db.get().await?;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .start()
            .await?;
        Self::check(&*tx, after).await?;
        let rows=tx.query("SELECT t.id,t.commit_revision::text,t.item_count FROM moria.runtime_change_transactions t WHERE t.environment=$1 AND t.incarnation=$2 AND t.complete AND t.commit_revision>$3 ORDER BY t.commit_revision LIMIT $4",&[&after.environment,&after.incarnation,&revision,&transactions]).await?;
        let mut frames = Vec::new();
        let mut count = 0i64;
        let mut next = after.clone();
        for row in rows {
            let n = i64::from(row.get::<_, i32>(2));
            if count + n > item_budget {
                ensure!(!frames.is_empty(), "RESOURCE_LIMIT");
                break;
            }
            let id: Uuid = row.get(0);
            let commit: String = row.get(1);
            let items=tx.query("SELECT ordinal,revision::text,object_type,kind,uid,generation FROM moria.runtime_change_items WHERE transaction_id=$1 ORDER BY ordinal",&[&id]).await?;
            ensure!(
                items.len() as i64 == n
                    && items
                        .iter()
                        .enumerate()
                        .all(|(i, r)| r.get::<_, i32>(0) as usize == i),
                "Incomplete change transaction"
            );
            let events:Vec<Value>=items.into_iter().map(|item|json!({"ordinal":item.get::<_,i32>(0),"revision":item.get::<_,String>(1),"objectType":item.get::<_,String>(2),"kind":item.get::<_,String>(3),"uid":item.get::<_,Option<Uuid>>(4),"generation":item.get::<_,Option<Uuid>>(5)})).collect();
            frames.push(json!({"position":{"incarnation":after.incarnation,"environment":after.environment,"commitRevision":commit,"ordinal":n},"events":events,"transactionEnd":true}));
            next.commit_revision = commit;
            count += n;
        }
        tx.commit().await?;
        let result = json!({"transactions":frames,"next":next});
        ensure!(
            serde_json::to_vec(&result)?.len() <= 8 * 1024 * 1024,
            "RESOURCE_LIMIT"
        );
        Ok(result)
    }

    async fn check<C: tokio_postgres::GenericClient + Sync>(
        client: &C,
        position: &Position,
    ) -> Result<()> {
        ensure!(client.query_one("SELECT EXISTS(SELECT 1 FROM moria.indexing_instance i,moria.environments e WHERE i.incarnation=$1 AND e.id=$2)",&[&position.incarnation,&position.environment]).await?.get::<_,bool>(0),"INCARNATION_CHANGED");
        let revision: i64 = position.commit_revision.parse()?;
        client
            .query_one(
                "SELECT moria.runtime_history_assert($1,$2)",
                &[&position.environment, &revision],
            )
            .await?;
        ensure!(revision==0 || client.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_change_transactions WHERE environment=$1 AND incarnation=$2 AND commit_revision=$3 AND complete)",&[&position.environment,&position.incarnation,&revision]).await?.get::<_,bool>(0),"CHANGE_POSITION_UNKNOWN");
        Ok(())
    }

    pub async fn pin(
        &self,
        id: Uuid,
        owner: Uuid,
        position: &Position,
        seconds: i32,
    ) -> Result<()> {
        ensure!((1..=3600).contains(&seconds), "Invalid pin lifetime");
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT id FROM moria.environments WHERE id=$1 FOR SHARE",
            &[&position.environment],
        )
        .await?;
        Self::check(&*tx, position).await?;
        let revision: i64 = position.commit_revision.parse()?;
        let changed=tx.execute("INSERT INTO moria.runtime_change_pins(id,environment,incarnation,owner,after_revision,expires_at) VALUES($1,$2,$3,$4,$5,clock_timestamp()+$6::integer*interval '1 second') ON CONFLICT(id) DO UPDATE SET after_revision=EXCLUDED.after_revision,expires_at=EXCLUDED.expires_at WHERE runtime_change_pins.owner=EXCLUDED.owner AND runtime_change_pins.environment=EXCLUDED.environment AND runtime_change_pins.incarnation=EXCLUDED.incarnation AND runtime_change_pins.after_revision<=EXCLUDED.after_revision AND runtime_change_pins.expires_at>clock_timestamp()",&[&id,&position.environment,&position.incarnation,&owner,&revision,&seconds]).await?;
        ensure!(changed == 1, "PIN_CONFLICT");
        tx.commit().await?;
        Ok(())
    }

    pub async fn release(&self, id: Uuid, owner: Uuid) -> Result<()> {
        self.db
            .get()
            .await?
            .execute(
                "DELETE FROM moria.runtime_change_pins WHERE id=$1 AND owner=$2",
                &[&id, &owner],
            )
            .await?;
        Ok(())
    }
}

pub async fn cli(db: Database, arguments: Vec<String>) -> Result<()> {
    use tokio::io::{AsyncReadExt, stdin};
    let mut bytes = Vec::new();
    stdin().take(65537).read_to_end(&mut bytes).await?;
    ensure!(bytes.len() <= 65536, "Change request byte limit");
    let input: Value = serde_json::from_slice(&bytes)?;
    let log = ChangeLog { db };
    let text = |key: &str| {
        input[key]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing change field"))
    };
    let result = match arguments.first().map(String::as_str) {
        Some("position") => serde_json::to_value(log.position(text("environment")?).await?)?,
        Some("read") => {
            log.read(
                &serde_json::from_value(input["after"].clone())?,
                input["transactions"].as_i64().unwrap_or(10),
                input["itemBudget"].as_i64().unwrap_or(1000),
            )
            .await?
        }
        Some("pin") => {
            log.pin(
                Uuid::parse_str(text("id")?)?,
                Uuid::parse_str(text("owner")?)?,
                &serde_json::from_value(input["after"].clone())?,
                input["seconds"]
                    .as_i64()
                    .and_then(|v| i32::try_from(v).ok())
                    .unwrap_or(60),
            )
            .await?;
            json!({"pinned":true})
        }
        Some("release") => {
            log.release(
                Uuid::parse_str(text("id")?)?,
                Uuid::parse_str(text("owner")?)?,
            )
            .await?;
            json!({"released":true})
        }
        _ => anyhow::bail!("Unknown change command"),
    };
    println!("{result}");
    Ok(())
}
