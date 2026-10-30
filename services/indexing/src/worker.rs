use crate::{
    database::{Database, safe_error},
    io::{Scope, ScopedFiles},
    values,
};
use anyhow::{Result, bail, ensure};
use futures::{TryStreamExt, stream};
use iceberg::{TableIdent, scan::FileScanTask, spec::TableMetadata, table::StaticTable};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio_postgres::Row;
use uuid::Uuid;

#[derive(Clone)]
pub struct Worker {
    pub db: Database,
    pub owner: Uuid,
}

struct Heartbeat(tokio::task::JoinHandle<()>);
impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Worker {
    async fn audit_claim(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        build: Uuid,
        unit: Uuid,
        kind: &str,
        fence: i64,
    ) -> Result<()> {
        tx.execute("UPDATE moria.indexing_attempts SET outcome='interrupted',finished_at=clock_timestamp() WHERE build=$1 AND kind=$2 AND unit=$3 AND outcome='running'",&[&build,&kind,&unit]).await?;
        tx.execute("INSERT INTO moria.indexing_attempts(build,kind,unit,fence,owner) VALUES($1,$2,$3,$4,$5)",&[&build,&kind,&unit,&fence,&self.owner]).await?;
        Ok(())
    }
    fn heartbeat(&self, build: Uuid, task: Option<Uuid>, fence: i64) -> Heartbeat {
        let db = self.db.clone();
        let owner = self.owner;
        Heartbeat(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let Ok(client) = db.get().await else { return };
                let result = if let Some(task) = task {
                    client.execute("UPDATE moria.indexing_tasks SET lease_until=clock_timestamp()+interval '30 seconds' WHERE id=$1 AND build=$2 AND owner=$3 AND fence=$4 AND status='running' AND lease_until>clock_timestamp()",&[&task,&build,&owner,&fence]).await
                } else {
                    client.execute("UPDATE moria.indexing_builds SET lease_until=clock_timestamp()+interval '30 seconds' WHERE id=$1 AND owner=$2 AND fence=$3 AND lease_until>clock_timestamp() AND status IN ('planning','running','ready')",&[&build,&owner,&fence]).await
                };
                if !matches!(result, Ok(1)) {
                    return;
                }
            }
        }))
    }

    pub async fn plan(&self) -> Result<bool> {
        let mut client = self.db.get().await?;
        let transaction = client.transaction().await?;
        let row=transaction.query_opt("SELECT id FROM moria.indexing_builds WHERE status='pending' OR (status='planning' AND lease_until<=clock_timestamp()) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1",&[]).await?;
        let Some(row) = row else { return Ok(false) };
        let build: Uuid = row.get(0);
        let row=transaction.query_one("UPDATE moria.indexing_builds SET status='planning',owner=$2,fence=fence+1,lease_until=clock_timestamp()+interval '30 seconds' WHERE id=$1 RETURNING *",&[&build,&self.owner]).await?;
        self.audit_claim(&transaction, build, build, "coordinator", row.get("fence"))
            .await?;
        transaction.commit().await?;
        drop(client);
        let fence: i64 = row.get("fence");
        let _heartbeat = self.heartbeat(build, None, fence);
        println!(
            "{}",
            json!({"event":"planning","build":build,"worker":self.owner,"fence":fence.to_string()})
        );
        if let Err(error) = self.plan_build(&row, fence).await {
            let code = safe_error(&error);
            self.db
                .fail_owned(build, None, self.owner, fence, code)
                .await?;
            eprintln!("{}", json!({"build":build,"error":code}));
        }
        Ok(true)
    }

    async fn table(&self, row: &Row, scope: Scope) -> Result<StaticTable> {
        let definition: Value = row.get("definition");
        ensure!(
            definition["version"] == 1 && definition["valueVersion"] == 1,
            "Unsupported indexing version"
        );
        let warehouse = definition["warehouse"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing warehouse"))?;
        let files = ScopedFiles::new(warehouse, scope.clone())?;
        let metadata_location: String = row.get("metadata_location");
        let metadata = TableMetadata::read_from(&files.file_io(), &metadata_location).await?;
        let root = metadata
            .location()
            .strip_prefix("file://")
            .unwrap_or(metadata.location());
        let metadata_path = metadata_location
            .strip_prefix("file://")
            .unwrap_or(&metadata_location);
        ensure!(
            std::path::Path::new(root).starts_with(warehouse)
                && std::path::Path::new(metadata_path)
                    .starts_with(std::path::Path::new(root).join("metadata")),
            "Table metadata escaped its warehouse"
        );
        let narrowed = ScopedFiles::new(root, scope)?;
        StaticTable::from_metadata(
            metadata,
            TableIdent::from_strs(["moria", "pinned"])?,
            narrowed.file_io(),
        )
        .await
        .map_err(Into::into)
    }

    async fn plan_build(&self, row: &Row, fence: i64) -> Result<()> {
        let build: Uuid = row.get("id");
        let definition: Value = row.get("definition");
        let scope = Scope {
            db: self.db.clone(),
            build,
            task: None,
            owner: self.owner,
            fence,
        };
        let table = self.table(row, scope.clone()).await?;
        let snapshot: i64 = row.get::<_, String>("snapshot_id").parse()?;
        let columns = definition["schema"]["properties"]
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("Missing schema"))?;
        let scan = table
            .scan()
            .snapshot_id(snapshot)
            .select(columns.keys().map(String::as_str))
            .build()?;
        let mut tasks = scan.plan_files().await?;
        let mut ordinal = 0_i32;
        {
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            tx.query_one("SELECT 1 FROM moria.indexing_builds WHERE id=$1 AND owner=$2 AND fence=$3 AND status='planning' AND lease_until>clock_timestamp() FOR UPDATE",&[&build,&self.owner,&fence]).await?;
            tx.query_one("SELECT moria.indexing_check($1)", &[&build])
                .await?;
            tx.execute("DELETE FROM moria.indexing_tasks WHERE build=$1", &[&build])
                .await?;
            tx.commit().await?;
        }
        while let Some(mut task) = tasks.try_next().await? {
            ensure!(ordinal < 1_000_000, "Task planning budget exceeded");
            ensure!(
                task.deletes.is_empty(),
                "Iceberg delete files are unsupported by this reader release"
            );
            // Persisting these optional native values is not supported by the pinned reader ABI.
            ensure!(
                task.partition.as_ref().is_none_or(|p| p.iter().len() == 0)
                    && task
                        .partition_spec
                        .as_ref()
                        .is_none_or(|p| p.is_unpartitioned())
                    && task.name_mapping.is_none(),
                "Unsupported Iceberg partition/name mapping"
            );
            task.partition = None;
            task.partition_spec = None;
            let plan = json!({"kind":"scan"});
            let wire = crate::wire::scan_plan(task)?;
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            tx.query_one("SELECT 1 FROM moria.indexing_builds WHERE id=$1 AND owner=$2 AND fence=$3 AND lease_until>clock_timestamp() FOR UPDATE",&[&build,&self.owner,&fence]).await?;
            tx.query_one("SELECT moria.indexing_check($1)", &[&build])
                .await?;
            tx.execute(
                "INSERT INTO moria.indexing_tasks(build,ordinal,plan,wire) VALUES($1,$2,$3,$4)",
                &[&build, &ordinal, &plan, &wire],
            )
            .await?;
            tx.commit().await?;
            ordinal += 1;
        }
        scope.check().await?;
        self.db.get().await?.execute("UPDATE moria.indexing_builds SET status='running',planned=true,lease_until=clock_timestamp() WHERE id=$1 AND owner=$2 AND fence=$3 AND status='planning' AND lease_until>clock_timestamp()",&[&build,&self.owner,&fence]).await?;
        self.db.get().await?.execute("UPDATE moria.indexing_attempts SET outcome='planned',finished_at=clock_timestamp() WHERE build=$1 AND kind='coordinator' AND fence=$2 AND outcome='running'",&[&build,&fence]).await?;
        Ok(())
    }

    pub async fn task(&self) -> Result<bool> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let row=tx.query_opt("SELECT t.id FROM moria.indexing_tasks t JOIN moria.indexing_builds b ON b.id=t.build WHERE b.status='running' AND (t.plan->>'kind'='scan' OR NOT EXISTS(SELECT 1 FROM moria.indexing_tasks s WHERE s.build=t.build AND s.plan->>'kind'='scan' AND s.status<>'sealed')) AND (t.status='pending' OR (t.status='running' AND t.lease_until<=clock_timestamp())) ORDER BY b.created_at,t.ordinal FOR UPDATE OF t SKIP LOCKED LIMIT 1",&[]).await?;
        let Some(row) = row else { return Ok(false) };
        let task: Uuid = row.get(0);
        let row=tx.query_one("UPDATE moria.indexing_tasks SET status='running',owner=$2,fence=fence+1,lease_until=clock_timestamp()+interval '30 seconds' WHERE id=$1 RETURNING *",&[&task,&self.owner]).await?;
        self.audit_claim(&tx, row.get("build"), task, "task", row.get("fence"))
            .await?;
        tx.commit().await?;
        drop(client);
        let build: Uuid = row.get("build");
        let fence: i64 = row.get("fence");
        let plan: Value = row.get("plan");
        let _heartbeat = self.heartbeat(build, Some(task), fence);
        println!(
            "{}",
            json!({"event":"claimed","build":build,"task":task,"worker":self.owner,"fence":fence.to_string(),"kind":plan["kind"]})
        );
        let scope = Scope {
            db: self.db.clone(),
            build,
            task: Some(task),
            owner: self.owner,
            fence,
        };
        let result = match plan["kind"].as_str() {
            Some("scan") => match row.get::<_, Option<Vec<u8>>>("wire") {
                Some(wire) => self.scan(scope, &wire).await,
                None => Err(anyhow::anyhow!("Missing scan protocol")),
            },
            Some("reduce") => match plan["partition"]
                .as_i64()
                .and_then(|n| i32::try_from(n).ok())
                .filter(|n| *n >= 0 && *n < 4096)
            {
                Some(partition) => self.reduce(scope, partition).await,
                None => Err(anyhow::anyhow!("Invalid partition plan")),
            },
            _ => Err(anyhow::anyhow!("Unsupported task kind")),
        };
        if let Err(error) = result {
            // An expired owner cannot fail a replacement's build.
            self.db
                .fail_owned(build, Some(task), self.owner, fence, safe_error(&error))
                .await?;
            eprintln!(
                "{}",
                json!({"build":build,"task":task,"error":safe_error(&error)})
            );
        }
        Ok(true)
    }

    async fn scan(&self, scope: Scope, wire: &[u8]) -> Result<()> {
        let scan: FileScanTask = crate::wire::read_scan(wire)?;
        let task = scope.task.unwrap();
        let row = self
            .db
            .get()
            .await?
            .query_one(
                "SELECT * FROM moria.indexing_builds WHERE id=$1",
                &[&scope.build],
            )
            .await?;
        let definition: Value = row.get("definition");
        let table = self.table(&row, scope.clone()).await?;
        ensure!(
            scan.deletes.is_empty() && scan.partition.is_none() && scan.name_mapping.is_none(),
            "Unsupported persisted task"
        );
        let expected = scan.record_count;
        let mut batches = table
            .reader_builder()
            .with_batch_size(1000)
            .with_data_file_concurrency_limit(1)
            .with_range_fetch_concurrency(1)
            .build()
            .read(Box::pin(stream::iter([Ok(scan)])))?
            .stream();
        let schema = &definition["schema"];
        let columns = schema["properties"]
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("Missing schema"))?;
        let key = schema["primaryKey"].as_str().unwrap();
        let environment = Uuid::parse_str(definition["environmentId"].as_str().unwrap())?;
        let target = Uuid::parse_str(definition["targetId"].as_str().unwrap())?;
        let partitions = definition["partitions"].as_u64().unwrap() as u32;
        let mut count = 0_i64;
        let mut digest = Sha256::new();
        {
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            self.lock_task(&tx, &scope).await?;
            tx.execute("DELETE FROM moria.indexing_rows WHERE task=$1", &[&task])
                .await?;
            tx.commit().await?;
        }
        while let Some(batch) = batches.try_next().await? {
            scope.check().await?;
            ensure!(
                batch.get_array_memory_size() <= 64 * 1024 * 1024,
                "Arrow batch memory budget exceeded"
            );
            let mut ids = Vec::with_capacity(batch.num_rows());
            let mut values = Vec::with_capacity(batch.num_rows());
            let mut shards = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let mut value = serde_json::Map::new();
                for (name, column) in columns {
                    let array = batch
                        .column_by_name(name)
                        .ok_or_else(|| anyhow::anyhow!("Missing column"))?;
                    value.insert(
                        name.clone(),
                        values::arrow_value(array.as_ref(), row, column)?,
                    );
                }
                let id = Uuid::parse_str(
                    value[key]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("Invalid UID"))?,
                )?;
                let value = Value::Object(value);
                let bytes = serde_json::to_vec(&value)?;
                ensure!(
                    bytes.len() <= 4 * 1024 * 1024,
                    "Object byte budget exceeded"
                );
                digest.update((bytes.len() as u64).to_be_bytes());
                digest.update(bytes);
                shards.push(values::partition(environment, target, id, partitions) as i32);
                ids.push(id);
                values.push(value);
            }
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            self.lock_task(&tx, &scope).await?;
            tx.execute("INSERT INTO moria.indexing_rows(task,fence,uid,partition,value) SELECT $1,$2,* FROM unnest($3::uuid[],$4::integer[],$5::jsonb[])",&[&task,&scope.fence,&ids,&shards,&values]).await?;
            tx.commit().await?;
            count += batch.num_rows() as i64;
        }
        if let Some(expected) = expected {
            ensure!(count as u64 == expected, "Scan row count mismatch");
        }
        self.seal(&scope, count, format!("{:x}", digest.finalize()))
            .await
    }

    async fn lock_task(&self, tx: &tokio_postgres::Transaction<'_>, scope: &Scope) -> Result<()> {
        tx.query_one("SELECT 1 FROM moria.indexing_tasks WHERE id=$1 AND owner=$2 AND fence=$3 AND status='running' AND lease_until>clock_timestamp() FOR UPDATE",&[&scope.task.unwrap(),&self.owner,&scope.fence]).await?;
        tx.query_one("SELECT moria.indexing_check($1)", &[&scope.build])
            .await?;
        Ok(())
    }

    async fn seal(&self, scope: &Scope, count: i64, digest: String) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        self.lock_task(&tx, scope).await?;
        tx.execute("UPDATE moria.indexing_tasks SET status='sealed',row_count=$2,digest=$3,lease_until=NULL WHERE id=$1",&[&scope.task.unwrap(),&count,&digest]).await?;
        tx.execute("UPDATE moria.indexing_attempts SET outcome='sealed',finished_at=clock_timestamp() WHERE build=$1 AND kind='task' AND unit=$2 AND fence=$3 AND owner=$4 AND outcome='running'",&[&scope.build,&scope.task.unwrap(),&scope.fence,&self.owner]).await?;
        tx.commit().await?;
        println!(
            "{}",
            json!({"event":"sealed","build":scope.build,"task":scope.task,"rows":count.to_string()})
        );
        Ok(())
    }

    async fn reduce(&self, scope: Scope, partition: i32) -> Result<()> {
        let build = scope.build;
        let client = self.db.get().await?;
        let target: String = client
            .query_one(
                "SELECT target FROM moria.indexing_builds WHERE id=$1",
                &[&build],
            )
            .await?
            .get(0);
        let duplicate=client.query_opt("SELECT r.uid FROM moria.indexing_rows r JOIN moria.indexing_tasks t ON t.id=r.task AND t.fence=r.fence WHERE t.build=$1 AND t.status='sealed' AND t.plan->>'kind'='scan' AND r.partition=$2 GROUP BY r.uid HAVING count(*)>1 LIMIT 1",&[&build,&partition]).await?;
        ensure!(duplicate.is_none(), "Duplicate canonical UID");
        drop(client);
        let mut after: Option<Uuid> = None;
        let mut count = 0_i64;
        let mut digest = Sha256::new();
        loop {
            scope.check().await?;
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            self.lock_task(&tx, &scope).await?;
            let rows=tx.query("SELECT r.uid,r.value FROM moria.indexing_rows r JOIN moria.indexing_tasks t ON t.id=r.task AND t.fence=r.fence WHERE t.build=$1 AND t.status='sealed' AND t.plan->>'kind'='scan' AND r.partition=$2 AND ($3::uuid IS NULL OR r.uid>$3) ORDER BY r.uid LIMIT 1000",&[&build,&partition,&after]).await?;
            if rows.is_empty() {
                tx.commit().await?;
                break;
            }
            let ids: Vec<Uuid> = rows.iter().map(|r| r.get(0)).collect();
            let values: Vec<Value> = rows.iter().map(|r| r.get(1)).collect();
            let registered: i64 = tx
                .query_one(
                    "SELECT count(*) FROM moria.identities WHERE uid=ANY($1)",
                    &[&ids],
                )
                .await?
                .get(0);
            ensure!(registered == ids.len() as i64, "Unregistered UID");
            tx.execute("INSERT INTO moria.object_homes(uid,object_type) SELECT unnest($1::uuid[]),$2 ON CONFLICT(uid) DO NOTHING",&[&ids,&target]).await?;
            ensure!(tx.query_opt("SELECT 1 FROM moria.object_homes WHERE uid=ANY($1) AND object_type<>$2 LIMIT 1",&[&ids,&target]).await?.is_none(),"UID home conflict");
            tx.execute("INSERT INTO moria.staged_source_rows(stage,uid,value) SELECT $1,* FROM unnest($2::uuid[],$3::jsonb[]) ON CONFLICT(stage,uid) DO UPDATE SET value=EXCLUDED.value",&[&build,&ids,&values]).await?;
            tx.commit().await?;
            for (id, value) in ids.iter().zip(&values) {
                digest.update(id.as_bytes());
                digest.update(serde_json::to_vec(value)?);
            }
            after = Some(*ids.last().unwrap());
            count += ids.len() as i64;
        }
        self.seal(&scope, count, format!("{:x}", digest.finalize()))
            .await
    }

    pub async fn advance(&self) -> Result<bool> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let row=tx.query_opt("SELECT b.* FROM moria.indexing_builds b WHERE b.status IN ('running','ready') AND (b.owner=$1 OR b.lease_until<=clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM moria.indexing_tasks t WHERE t.build=b.id AND t.status<>'sealed') ORDER BY b.created_at FOR UPDATE OF b SKIP LOCKED LIMIT 1",&[&self.owner]).await?;
        let Some(row) = row else { return Ok(false) };
        let build: Uuid = row.get("id");
        let fence:i64=tx.query_one("UPDATE moria.indexing_builds SET owner=$2,fence=fence+1,lease_until=clock_timestamp()+interval '30 seconds' WHERE id=$1 RETURNING fence",&[&build,&self.owner]).await?.get(0);
        self.audit_claim(&tx, build, build, "coordinator", fence)
            .await?;
        tx.commit().await?;
        drop(client);
        let _heartbeat = self.heartbeat(build, None, fence);
        let result = self.advance_build(&row, fence).await;
        match result {
            Ok(worked) => Ok(worked),
            Err(error) => {
                self.db
                    .fail_owned(build, None, self.owner, fence, safe_error(&error))
                    .await?;
                eprintln!("{}", json!({"build":build,"error":safe_error(&error)}));
                Ok(true)
            }
        }
    }

    async fn advance_build(&self, row: &Row, fence: i64) -> Result<bool> {
        let build: Uuid = row.get("id");
        self.db.guard_claim(build, None, self.owner, fence).await?;
        let mut client = self.db.get().await?;
        let pending: i64 = client
            .query_one(
                "SELECT count(*) FROM moria.indexing_tasks WHERE build=$1 AND status<>'sealed'",
                &[&build],
            )
            .await?
            .get(0);
        if pending > 0 {
            return Ok(false);
        }
        let reductions:i64=client.query_one("SELECT count(*) FROM moria.indexing_tasks WHERE build=$1 AND plan->>'kind'='reduce'",&[&build]).await?.get(0);
        let definition: Value = row.get("definition");
        if reductions == 0 {
            let tx = client.transaction().await?;
            tx.query_one("SELECT 1 FROM moria.indexing_builds WHERE id=$1 AND owner=$2 AND fence=$3 AND lease_until>clock_timestamp() FOR UPDATE",&[&build,&self.owner,&fence]).await?;
            tx.query_one("SELECT moria.indexing_check($1)", &[&build])
                .await?;
            for partition in 0..definition["partitions"].as_i64().unwrap() {
                let ordinal = 1_000_000 + partition as i32;
                tx.execute(
                    "INSERT INTO moria.indexing_tasks(build,ordinal,plan) VALUES($1,$2,$3)",
                    &[
                        &build,
                        &ordinal,
                        &json!({"kind":"reduce","partition":partition}),
                    ],
                )
                .await?;
            }
            tx.execute(
                "UPDATE moria.indexing_builds SET lease_until=clock_timestamp() WHERE id=$1",
                &[&build],
            )
            .await?;
            tx.execute("UPDATE moria.indexing_attempts SET outcome='scheduled',finished_at=clock_timestamp() WHERE build=$1 AND kind='coordinator' AND fence=$2 AND outcome='running'",&[&build,&fence]).await?;
            tx.commit().await?;
            return Ok(true);
        }
        drop(client);
        // Statistics preparation and retained-overlay validation occur outside the publication lock.
        let schema = &definition["schema"];
        let environment: String = row.get("environment");
        let target: String = row.get("target");
        let client = self.db.get().await?;
        client
            .batch_execute("ANALYZE moria.staged_source_rows")
            .await?;
        let mut after: Option<Uuid> = None;
        loop {
            self.db.guard_claim(build, None, self.owner, fence).await?;
            let overlays=client.query("SELECT DISTINCT ON(uid) uid,overrides FROM moria.overlay_versions WHERE environment=$1 AND object_type=$2 AND ($3::uuid IS NULL OR uid>$3) ORDER BY uid,revision DESC LIMIT 1000",&[&environment,&target,&after]).await?;
            for overlay in &overlays {
                let overrides: Value = overlay.get(1);
                for (name, value) in overrides.as_object().unwrap() {
                    let descriptor = schema["properties"]
                        .get(name)
                        .ok_or_else(|| anyhow::anyhow!("Retained overlay schema changed"))?;
                    values::normalize(value, descriptor)?;
                }
            }
            if overlays.is_empty() {
                break;
            }
            after = Some(overlays.last().unwrap().get(0));
        }
        drop(client);
        let count: i64 = self
            .db
            .get()
            .await?
            .query_one(
                "SELECT count(*) FROM moria.staged_source_rows WHERE stage=$1",
                &[&build],
            )
            .await?
            .get(0);
        let expected:i64=self.db.get().await?.query_one("SELECT COALESCE(sum(row_count),0)::bigint FROM moria.indexing_tasks WHERE build=$1 AND plan->>'kind'='scan' AND status='sealed'",&[&build]).await?.get(0);
        ensure!(count == expected, "Incomplete generation");
        let mut client = self.db.get().await?;
        let result=async {
        client
            .query_one(
                "SELECT pg_advisory_lock(hashtextextended($1,0))",
                &[&format!("moria/environment/{environment}")],
            )
            .await?;
        client
            .query_one(
                "SELECT pg_advisory_lock_shared(hashtextextended('moria/filesystem',0))",
                &[],
            )
            .await?;
            let tx=client.transaction().await?;
            tx.query_one("SELECT moria.indexing_check($1)",&[&build]).await?;
            tx.query_one("SELECT 1 FROM moria.indexing_builds WHERE id=$1 AND owner=$2 AND fence=$3 AND lease_until>clock_timestamp() FOR UPDATE",&[&build,&self.owner,&fence]).await?;
            tx.execute("UPDATE moria.source_stages SET status='ready' WHERE id=$1 AND status='loading'",&[&build]).await?;
            tx.execute("UPDATE moria.indexing_builds SET status='ready' WHERE id=$1",&[&build]).await?;
            let receipt:Value=tx.query_one("SELECT moria.indexing_activate($1,$2,$3)",&[&build,&self.owner,&fence]).await?.get(0);
            tx.execute("UPDATE moria.indexing_attempts SET outcome='active',finished_at=clock_timestamp() WHERE build=$1 AND kind='coordinator' AND fence=$2 AND outcome='running'",&[&build,&fence]).await?;
            tx.commit().await?;
            println!("{}",json!({"event":"active","build":build,"receipt":receipt}));
            Ok::<_,anyhow::Error>(())
        }.await;
        if client
            .batch_execute("SELECT pg_advisory_unlock_all()")
            .await
            .is_err()
        {
            drop(deadpool_postgres::Object::take(client));
            bail!("Publication connection cleanup failed");
        }
        result?;
        Ok(true)
    }

    pub async fn tick(&self) -> Result<bool> {
        self.db.get().await?.execute("WITH expired AS (UPDATE moria.indexing_builds SET status='failed',error='DEADLINE_EXCEEDED' WHERE deadline<=clock_timestamp() AND status IN ('pending','planning','running','preparing','ready') RETURNING id) UPDATE moria.indexing_attempts SET outcome='DEADLINE_EXCEEDED',finished_at=clock_timestamp() WHERE build IN (SELECT id FROM expired) AND outcome='running'",&[]).await?;
        if self.plan().await? {
            return Ok(true);
        }
        if self.task().await? {
            return Ok(true);
        }
        self.advance().await
    }
}
