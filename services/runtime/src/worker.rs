use crate::{
    database::{Database, safe_error},
    io::{Scope, ScopedFiles},
    values,
};
use anyhow::{Context, Result, bail, ensure};
use futures::{TryStreamExt, stream};
use iceberg::{TableIdent, scan::FileScanTask, spec::TableMetadata, table::StaticTable};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio_postgres::Row;
use uuid::Uuid;
mod source;

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

const SCAN_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
const SINGLE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
struct SourceLayout {
    columns: serde_json::Map<String, Value>,
    mapping: std::collections::BTreeMap<String, String>,
    identity: bool,
}
impl SourceLayout {
    fn new(definition: &Value, actual: &std::collections::BTreeMap<String, Value>) -> Result<Self> {
        let target = definition["schema"]["properties"]
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("SCHEMA_CHANGED"))?;
        let source = definition.get("sourceColumns");
        let mapping = definition.get("sourceMapping");
        ensure!(source.is_some() == mapping.is_some(), "SCHEMA_CHANGED");
        let (columns, mapping) = if let (Some(source), Some(mapping)) = (source, mapping) {
            let columns = source
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("SCHEMA_CHANGED"))?
                .clone();
            let mapping: std::collections::BTreeMap<String, String> =
                serde_json::from_value(mapping.clone())?;
            ensure!(
                columns.len() <= 256
                    && serde_json::to_value(actual)? == source.clone()
                    && mapping.len() == target.len()
                    && target.iter().all(|(name, descriptor)| mapping
                        .get(name)
                        .is_some_and(|source| columns.get(source) == Some(descriptor))),
                "SCHEMA_CHANGED"
            );
            (columns, mapping)
        } else {
            (
                target.clone(),
                target
                    .keys()
                    .map(|name| (name.clone(), name.clone()))
                    .collect(),
            )
        };
        let identity =
            columns == *target && mapping.iter().all(|(target, source)| target == source);
        Ok(Self {
            columns,
            mapping,
            identity,
        })
    }
    fn row(&self, batch: &arrow_array::RecordBatch, row: usize) -> Result<(Value, Option<Value>)> {
        let mut canonical = serde_json::Map::new();
        let mut bytes = 0usize;
        for (name, column) in &self.columns {
            let array = batch
                .column_by_name(name)
                .ok_or_else(|| anyhow::anyhow!("SCHEMA_CHANGED"))?;
            let value = values::arrow_value(array.as_ref(), row, column)?;
            bytes += serde_json::to_vec(&value)?.len();
            ensure!(bytes <= 4 * 1024 * 1024, "DECODED_ROW_LIMIT");
            canonical.insert(name.clone(), value);
        }
        if self.identity {
            return Ok((Value::Object(canonical), None));
        }
        let mapped = self
            .mapping
            .iter()
            .map(|(target, source)| {
                Ok((
                    target.clone(),
                    canonical
                        .get(source)
                        .ok_or_else(|| anyhow::anyhow!("SCHEMA_CHANGED"))?
                        .clone(),
                ))
            })
            .collect::<Result<serde_json::Map<_, _>>>()?;
        Ok((Value::Object(mapped), Some(Value::Object(canonical))))
    }
}
fn payload_allowed(bytes: usize, rows: usize) -> bool {
    bytes > 0 && (bytes <= SCAN_PAYLOAD_BYTES || (rows == 1 && bytes <= SINGLE_PAYLOAD_BYTES))
}
fn planning_phase(error: &anyhow::Error) -> Option<&'static str> {
    const PHASES: &[&str] = &[
        "INDEXING_PLAN_TABLE",
        "INDEXING_PLAN_SCHEMA",
        "INDEXING_PLAN_SCAN",
        "INDEXING_PLAN_FILES",
        "INDEXING_PLAN_NEXT_FILE",
        "INDEXING_PLAN_WIRE",
    ];
    error.chain().find_map(|cause| {
        PHASES
            .iter()
            .copied()
            .find(|phase| *phase == cause.to_string())
    })
}

impl Worker {
    async fn admit_candidate(
        tx: &tokio_postgres::Transaction<'_>,
        build: Uuid,
        unit: Uuid,
        owner: Option<Uuid>,
        fence: i64,
    ) -> Result<bool> {
        // The selected candidate remains locked across a failed authority check.
        tx.batch_execute("SAVEPOINT indexing_candidate_admission")
            .await?;
        match tx
            .query_one("SELECT moria.indexing_check($1)", &[&build])
            .await
        {
            Ok(_) => {
                tx.batch_execute("RELEASE SAVEPOINT indexing_candidate_admission")
                    .await?;
                Ok(true)
            }
            Err(error) => {
                let rejection = error
                    .as_db_error()
                    .filter(|error| error.code().code() == "P0001")
                    .and_then(|error| {
                        [
                            "AUTHORIZATION_CHANGED",
                            "UNAUTHENTICATED",
                            "FORBIDDEN",
                            "BINDING_CHANGED",
                            "INCARNATION_CHANGED",
                            "BUILD_INACTIVE",
                            "MARKING_DOWNGRADE",
                            "INDEXING_ACCEPTANCE_REQUIRED",
                            "INDEXING_DELEGATION_INACTIVE",
                            "INDEXING_AUTHORITY_CHANGED",
                            "DATASET_UNKNOWN",
                            "DATASET_BINDING_CHANGED",
                            "WHOLE_DATASET_REQUIRED",
                            "JOB_DELEGATION_INACTIVE",
                            "SERVICE_PURPOSE_REVOKED",
                        ]
                        .into_iter()
                        .find(|code| *code == error.message())
                    });
                let Some(code) = rejection else {
                    return Err(error.into());
                };
                tx.batch_execute("ROLLBACK TO SAVEPOINT indexing_candidate_admission; RELEASE SAVEPOINT indexing_candidate_admission").await?;
                let failed = tx.execute("UPDATE moria.indexing_builds SET status='failed',error=$2 WHERE id=$1 AND status NOT IN ('active','failed','cancelled','superseded','conflicted')", &[&build, &code]).await?;
                if failed == 1 {
                    if let Some(owner) = owner {
                        tx.execute("UPDATE moria.indexing_attempts SET outcome=$5,finished_at=clock_timestamp() WHERE build=$1 AND unit=$2 AND owner=$3 AND fence=$4 AND outcome='running'", &[&build, &unit, &owner, &fence, &code]).await?;
                    }
                    eprintln!(
                        "{}",
                        json!({"build":build,"error":code,"phase":"admission"})
                    );
                }
                Ok(false)
            }
        }
    }
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
        let row=transaction.query_opt("SELECT id,owner,fence FROM moria.indexing_builds WHERE (status='pending' OR (status='planning' AND lease_until<=clock_timestamp())) AND moria.runtime_environment_cell_available(environment) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1",&[]).await?;
        let Some(row) = row else { return Ok(false) };
        let build: Uuid = row.get(0);
        if !Self::admit_candidate(
            &transaction,
            build,
            build,
            row.get("owner"),
            row.get("fence"),
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(true);
        }
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
            let sql_state = error.chain().find_map(|cause| {
                cause
                    .downcast_ref::<tokio_postgres::Error>()
                    .and_then(|error| error.as_db_error())
                    .map(|error| error.code().code())
            });
            eprintln!(
                "{}",
                json!({"build":build,"error":code,"phase":planning_phase(&error),"sqlState":sql_state})
            );
        }
        Ok(true)
    }

    async fn table(&self, row: &Row, scope: Scope) -> Result<StaticTable> {
        let definition: Value = row.get("definition");
        ensure!(
            definition["version"] == 1 && definition["valueVersion"] == 1,
            "Unsupported indexing version"
        );
        let metadata_location: String = row.get("metadata_location");
        if metadata_location.starts_with("moria-snapshot-v2:") {
            use crate::{
                catalog::NativeSnapshotRef,
                storage::{
                    ClosureLimits, IndexNamespace, IndexNamespaceReadAuthority, PosixStore,
                    ReadAuthority, StorageLimits, configured_reader_with_fingerprint,
                },
            };
            let reference = NativeSnapshotRef::decode(&metadata_location)?;
            let binding=self.db.get().await?.query_one("SELECT n.revision,n.iceberg_uuid FROM moria.native_storage_namespaces n JOIN moria.indexing_builds b ON b.id=$1 WHERE n.environment=b.environment AND n.resource=b.source AND n.purpose='dataset-source' AND n.enabled AND n.warehouse_id=$2 AND n.table_id=$3",
                &[&scope.build,&reference.root.object.warehouse_id,&reference.root.object.table_id]).await?;
            let uuid: Option<Uuid> = binding.get("iceberg_uuid");
            let uuid = uuid.ok_or_else(|| anyhow::anyhow!("INVALID_DATASET_BINDING"))?;
            let namespace = IndexNamespace {
                warehouse_id: reference.root.object.warehouse_id.clone(),
                table_id: reference.root.object.table_id.clone(),
                revision: binding.get("revision"),
            };
            let authority = std::sync::Arc::new(IndexNamespaceReadAuthority {
                scope,
                namespace: namespace.clone(),
            });
            authority.check_read(&reference.root.object).await?;
            let (reader, binding_fingerprint) = configured_reader_with_fingerprint(&namespace)?;
            let managed:Option<Value>=self.db.get().await?.query_one("SELECT moria.runtime_dataset_binding($1,moria.runtime_data_resource($1,$2),$3,$4,$5)",&[&row.get::<_,String>("environment"),&row.get::<_,String>("source"),&namespace.warehouse_id,&namespace.table_id,&namespace.revision]).await?.get(0);
            let managed = managed.unwrap_or(Value::Null);
            if !managed.is_null() {
                ensure!(
                    managed["physicalBindingFingerprint"].as_str()
                        == Some(reader.physical_fingerprint()?.as_str()),
                    "DATASET_BINDING_INVALID"
                );
            }
            let scratch_root = std::env::var("MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT")
                .map_err(|_| anyhow::anyhow!("SNAPSHOT_SCRATCH_UNAVAILABLE"))?;
            let scratch = PosixStore::provision(
                std::path::Path::new(&scratch_root),
                &namespace.warehouse_id,
                &namespace.table_id,
                StorageLimits {
                    max_object_bytes: 0,
                    chunk_bytes: 1024 * 1024,
                },
            )?;
            let limits = std::env::var("MORIA_NATIVE_CLOSURE_LIMITS")
                .map_err(|_| anyhow::anyhow!("CLOSURE_BUDGET_UNAVAILABLE"))?;
            ensure!(limits.len() <= 4096, "INVALID_BINDING");
            let limits: ClosureLimits = serde_json::from_str(&limits)?;
            let table = crate::attestation::table(
                &scratch,
                reader,
                authority,
                reference,
                uuid,
                row.get::<_, String>("snapshot_id").parse()?,
                binding_fingerprint,
                limits,
            )
            .await?;
            if !managed.is_null() {
                let expected: iceberg::spec::Schema =
                    serde_json::from_value(managed["icebergSchema"].clone())?;
                ensure!(
                    &expected == table.metadata().current_schema().as_ref(),
                    "DATASET_BINDING_INVALID"
                );
            }
            return Ok(table);
        }
        ensure!(!self.db.get().await?.query_one("SELECT EXISTS(SELECT 1 FROM moria.fs_resource_identities WHERE resource_id::text=moria.runtime_data_resource($1,$2))",&[&row.get::<_,String>("environment"),&row.get::<_,String>("source")]).await?.get::<_,bool>(0),"DATASET_BINDING_INVALID");
        let warehouse = definition["warehouse"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing warehouse"))?;
        let files = ScopedFiles::new(warehouse, scope.clone())?;
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
        let source = self
            .source(row, scope.clone())
            .await
            .context("INDEXING_PLAN_TABLE")?;
        let snapshot: i64 = row.get::<_, String>("snapshot_id").parse()?;
        let actual_columns =
            crate::snapshot::columns(&serde_json::to_value(source.schema().as_ref())?)?;
        let layout =
            SourceLayout::new(&definition, &actual_columns).context("INDEXING_PLAN_SCHEMA")?;
        let mut tasks = source.plans(snapshot, &layout).await?;
        let mut ordinal = 0_i32;
        {
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            tx.query_one("SELECT 1 FROM moria.indexing_builds WHERE id=$1 AND owner=$2 AND fence=$3 AND status='planning' AND lease_until>clock_timestamp() FOR UPDATE",&[&build,&self.owner,&fence]).await?;
            tx.query_one("SELECT moria.indexing_check($1)", &[&build])
                .await?;
            tx.query_one(
                "SELECT moria.runtime_source_store_open($1,$2,$3)",
                &[&build, &self.owner, &fence],
            )
            .await?;
            tx.execute("DELETE FROM moria.indexing_tasks WHERE build=$1", &[&build])
                .await?;
            tx.commit().await?;
        }
        while let Some((plan, wire)) = tasks.try_next().await.context("INDEXING_PLAN_NEXT_FILE")? {
            ensure!(ordinal < 1_000_000, "Task planning budget exceeded");
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
        let row=tx.query_opt("SELECT t.id,t.build,t.owner,t.fence FROM moria.indexing_tasks t JOIN moria.indexing_builds b ON b.id=t.build WHERE b.status='running' AND moria.runtime_environment_cell_available(b.environment) AND (t.plan->>'kind'='scan' OR NOT EXISTS(SELECT 1 FROM moria.indexing_tasks s WHERE s.build=t.build AND s.plan->>'kind'='scan' AND s.status<>'sealed')) AND (t.status='pending' OR (t.status='running' AND t.lease_until<=clock_timestamp())) ORDER BY b.created_at,t.ordinal FOR UPDATE OF t SKIP LOCKED LIMIT 1",&[]).await?;
        let Some(row) = row else { return Ok(false) };
        let task: Uuid = row.get(0);
        if !Self::admit_candidate(
            &tx,
            row.get("build"),
            task,
            row.get("owner"),
            row.get("fence"),
        )
        .await?
        {
            tx.commit().await?;
            return Ok(true);
        }
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
            Some("scan") => {
                self.scan(
                    scope,
                    &plan,
                    row.get::<_, Option<Vec<u8>>>("wire").as_deref(),
                )
                .await
            }
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

    async fn scan(&self, scope: Scope, plan: &Value, wire: Option<&[u8]>) -> Result<()> {
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
        let source = self.source(&row, scope.clone()).await?;
        let schema = &definition["schema"];
        let actual_columns =
            crate::snapshot::columns(&serde_json::to_value(source.schema().as_ref())?)?;
        let layout = SourceLayout::new(&definition, &actual_columns)?;
        let (expected, mut batches) = source.batches(plan, wire).await?;
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
            let mut ids = Vec::new();
            let mut values = Vec::new();
            let mut source_values = Vec::new();
            let mut shards = Vec::new();
            let mut payload_bytes = 0usize;
            for row in 0..batch.num_rows() {
                let (value, canonical) = layout.row(&batch, row)?;
                let id = Uuid::parse_str(
                    value[key]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("Invalid UID"))?,
                )?;
                let bytes = serde_json::to_vec(&value)?;
                ensure!(
                    bytes.len() <= 4 * 1024 * 1024,
                    "Object byte budget exceeded"
                );
                let source_bytes = canonical.as_ref().map(serde_json::to_vec).transpose()?;
                let row_bytes = bytes.len() + source_bytes.as_ref().map_or(0, Vec::len);
                ensure!(
                    payload_allowed(row_bytes, 1),
                    "Reducer wire byte budget exceeded"
                );
                if !ids.is_empty()
                    && (payload_bytes + row_bytes > SCAN_PAYLOAD_BYTES || ids.len() == 1000)
                {
                    self.insert_scanned(&scope, &ids, &shards, &values, &source_values)
                        .await?;
                    ids.clear();
                    shards.clear();
                    values.clear();
                    source_values.clear();
                    payload_bytes = 0;
                }
                digest.update((bytes.len() as u64).to_be_bytes());
                digest.update(&bytes);
                if let Some(source_bytes) = source_bytes {
                    digest.update(b"canonical-source/v1\0");
                    digest.update((source_bytes.len() as u64).to_be_bytes());
                    digest.update(source_bytes);
                }
                shards.push(values::partition(environment, target, id, partitions) as i32);
                ids.push(id);
                values.push(value);
                source_values.push(canonical);
                payload_bytes += row_bytes;
            }
            if !ids.is_empty() {
                self.insert_scanned(&scope, &ids, &shards, &values, &source_values)
                    .await?;
            }
            count += batch.num_rows() as i64;
        }
        if let Some(expected) = expected {
            ensure!(count as u64 == expected, "Scan row count mismatch");
        }
        self.seal(&scope, count, format!("{:x}", digest.finalize()))
            .await
    }

    async fn insert_scanned(
        &self,
        scope: &Scope,
        ids: &[Uuid],
        shards: &[i32],
        values: &[Value],
        sources: &[Option<Value>],
    ) -> Result<()> {
        ensure!(
            !ids.is_empty()
                && ids.len() <= 1000
                && ids.len() == shards.len()
                && ids.len() == values.len()
                && ids.len() == sources.len(),
            "RESOURCE_LIMIT"
        );
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        self.lock_task(&tx, scope).await?;
        tx.execute("INSERT INTO moria.indexing_rows(build,task,fence,uid,partition,value,source_value) SELECT $6,$1,$2,* FROM unnest($3::uuid[],$4::integer[],$5::jsonb[],$7::jsonb[])",&[&scope.task.unwrap(),&scope.fence,&ids,&shards,&values,&scope.build,&sources]).await?;
        tx.commit().await?;
        Ok(())
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
        let origin = client
            .query_one(
                "SELECT target,environment,source FROM moria.indexing_builds WHERE id=$1",
                &[&build],
            )
            .await?;
        let target: String = origin.get(0);
        let environment: String = origin.get(1);
        let source: String = origin.get(2);
        let store = crate::source_store::SourceStore::task(&**client, &scope).await?;
        let duplicate=client.query_opt("SELECT r.uid FROM moria.indexing_rows r JOIN moria.indexing_tasks t ON t.id=r.task AND t.fence=r.fence WHERE r.build=$1 AND t.build=$1 AND t.status='sealed' AND t.plan->>'kind'='scan' AND r.partition=$2 GROUP BY r.uid HAVING count(*)>1 LIMIT 1",&[&build,&partition]).await?;
        ensure!(duplicate.is_none(), "Duplicate canonical UID");
        drop(client);
        let mut after: Option<Uuid> = None;
        let mut count = 0_i64;
        let mut digest = Sha256::new();
        let mut max_wire_bytes = 0_i64;
        let mut batches = 0_u64;
        loop {
            scope.check().await?;
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            self.lock_task(&tx, &scope).await?;
            // Limit candidate keys before computing JSON lengths or returning wide payloads.
            let rows = tx
                .query(
                    "SELECT * FROM moria.indexing_reduce_page($1,$2,$3)",
                    &[&build, &partition, &after],
                )
                .await?;
            if rows.is_empty() {
                tx.commit().await?;
                break;
            }
            let wire_bytes: i64 = rows.iter().map(|r| r.get::<_, i64>(2)).sum();
            ensure!(
                usize::try_from(wire_bytes).is_ok_and(|bytes| payload_allowed(bytes, rows.len())),
                "Reducer wire byte budget exceeded"
            );
            max_wire_bytes = max_wire_bytes.max(wire_bytes);
            batches += 1;
            let ids: Vec<Uuid> = rows.iter().map(|r| r.get(0)).collect();
            let values: Vec<Value> = rows.iter().map(|r| r.get(1)).collect();
            let sources: Vec<Option<Value>> = rows.iter().map(|r| r.get(3)).collect();
            let registered: i64 = tx
                .query_one(
                    "SELECT count(*) FROM moria.identities WHERE uid=ANY($1)",
                    &[&ids],
                )
                .await?
                .get(0);
            ensure!(registered == ids.len() as i64, "Unregistered UID");
            tx.query_one(
                "SELECT moria.runtime_object_import_check($1,$2,$3,$4)",
                &[&environment, &target, &ids, &source],
            )
            .await?;
            tx.execute("INSERT INTO moria.object_homes(uid,object_type) SELECT unnest($1::uuid[]),$2 ON CONFLICT(uid) DO NOTHING",&[&ids,&target]).await?;
            ensure!(tx.query_opt("SELECT 1 FROM moria.object_homes WHERE uid=ANY($1) AND object_type<>$2 LIMIT 1",&[&ids,&target]).await?.is_none(),"UID home conflict");
            tx.execute("INSERT INTO moria.staged_source_rows(stage,uid,value,source_value) SELECT $1,* FROM unnest($2::uuid[],$3::jsonb[],$4::jsonb[]) ON CONFLICT(stage,uid) DO UPDATE SET value=EXCLUDED.value,source_value=EXCLUDED.source_value",&[&build,&ids,&values,&sources]).await?;
            if let Some(store) = &store {
                store.keys(&*tx, &scope, &ids, &values, &sources).await?;
            }
            tx.commit().await?;
            for ((id, value), source) in ids.iter().zip(&values).zip(&sources) {
                digest.update(id.as_bytes());
                digest.update(serde_json::to_vec(value)?);
                if let Some(source) = source {
                    let bytes = serde_json::to_vec(source)?;
                    digest.update(b"canonical-source/v1\0");
                    digest.update((bytes.len() as u64).to_be_bytes());
                    digest.update(bytes);
                }
            }
            after = Some(*ids.last().unwrap());
            count += ids.len() as i64;
        }
        println!(
            "{}",
            json!({"event":"reducer_batches","build":build,"task":scope.task,
            "batches":batches.to_string(),"maxWireBytes":max_wire_bytes.to_string(),"normalBatchLimitBytes":SCAN_PAYLOAD_BYTES.to_string(),"singleRowLimitBytes":SINGLE_PAYLOAD_BYTES.to_string()})
        );
        self.seal(&scope, count, format!("{:x}", digest.finalize()))
            .await
    }

    pub async fn advance(&self) -> Result<bool> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let row=tx.query_opt("SELECT b.* FROM moria.indexing_builds b WHERE b.status IN ('running','ready') AND moria.runtime_environment_cell_available(b.environment) AND (b.owner=$1 OR b.lease_until<=clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM moria.indexing_tasks t WHERE t.build=b.id AND t.status<>'sealed') ORDER BY b.created_at FOR UPDATE OF b SKIP LOCKED LIMIT 1",&[&self.owner]).await?;
        let Some(row) = row else { return Ok(false) };
        let build: Uuid = row.get("id");
        if !Self::admit_candidate(&tx, build, build, row.get("owner"), row.get("fence")).await? {
            tx.commit().await?;
            return Ok(true);
        }
        let fence:i64=tx.query_one("UPDATE moria.indexing_builds SET owner=$2,fence=fence+1,lease_until=clock_timestamp()+interval '30 seconds' WHERE id=$1 RETURNING fence",&[&build,&self.owner]).await?.get(0);
        self.audit_claim(&tx, build, build, "coordinator", fence)
            .await?;
        tx.commit().await?;
        drop(client);
        let _heartbeat = self.heartbeat(build, None, fence);
        let result = self.advance_build(&row, fence).await;
        match result {
            Ok(worked) => Ok(worked),
            Err(error)
                if error.chain().any(|cause| {
                    cause
                        .downcast_ref::<tokio_postgres::Error>()
                        .and_then(|e| e.as_db_error())
                        .is_some_and(|e| {
                            matches!(
                                e.message(),
                                "PROJECTION_CATCHUP_REQUIRED" | "PROJECTION_DDL_BUSY"
                            )
                        })
                }) =>
            {
                // The next fenced coordinator resumes durable projection work after dropping publication locks.
                self.db.get().await?.execute("UPDATE moria.indexing_builds SET lease_until=clock_timestamp() WHERE id=$1 AND owner=$2 AND fence=$3",&[&build,&self.owner,&fence]).await?;
                self.db.get().await?.execute("UPDATE moria.indexing_attempts SET outcome='scheduled',finished_at=clock_timestamp() WHERE build=$1 AND kind='coordinator' AND owner=$2 AND fence=$3 AND outcome='running'",&[&build,&self.owner,&fence]).await?;
                println!(
                    "{}",
                    json!({"event":"projection_retry","build":build,"fence":fence.to_string()})
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(true)
            }
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
        self.db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_source_store_seal($1,$2,$3)",
                &[&build, &self.owner, &fence],
            )
            .await?;
        crate::projection::prepare(
            &self.db,
            build,
            self.owner,
            fence,
            crate::storage::Cancellation::default(),
        )
        .await?;
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

#[cfg(test)]
mod mapping_tests {
    use super::*;
    use std::sync::Arc;
    #[test]
    fn canonical_hidden_fields_survive_mapping_with_exact_types() {
        let actual=serde_json::from_value(json!({"id":{"type":"uid","nullable":false},"original":{"type":"string","nullable":false},"region":{"type":"string","nullable":false}})).unwrap();
        let definition = json!({"schema":{"primaryKey":"uid","properties":{"uid":{"type":"uid","nullable":false},"name":{"type":"string","nullable":false}}},"sourceColumns":actual,"sourceMapping":{"uid":"id","name":"original"}});
        let layout = SourceLayout::new(&definition, &actual).unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::FixedSizeBinary(16), false),
            arrow_schema::Field::new("original", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("region", arrow_schema::DataType::Utf8, false),
        ]));
        let uid = Uuid::new_v4();
        let ids = arrow_array::FixedSizeBinaryArray::try_from_iter(
            [uid.as_bytes().as_slice()].into_iter(),
        )
        .unwrap();
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ids),
                Arc::new(arrow_array::StringArray::from(vec!["mapped"])),
                Arc::new(arrow_array::StringArray::from(vec!["West"])),
            ],
        )
        .unwrap();
        let (mapped, source) = layout.row(&batch, 0).unwrap();
        assert_eq!(mapped, json!({"uid":uid,"name":"mapped"}));
        assert_eq!(
            source.unwrap(),
            json!({"id":uid,"original":"mapped","region":"West"})
        );
        let mut invalid = definition.clone();
        invalid["sourceColumns"]["region"]["nullable"] = json!(true);
        assert!(SourceLayout::new(&invalid, &actual).is_err());
        invalid = definition.clone();
        invalid.as_object_mut().unwrap().remove("sourceMapping");
        assert!(SourceLayout::new(&invalid, &actual).is_err());
        assert!(payload_allowed(8 * 1024 * 1024, 1000));
        assert!(!payload_allowed(8 * 1024 * 1024 + 1, 2));
        assert!(payload_allowed(16 * 1024 * 1024, 1));
        assert!(!payload_allowed(16 * 1024 * 1024 + 1, 1));
    }
}
