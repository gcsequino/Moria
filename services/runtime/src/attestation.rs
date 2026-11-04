use crate::{
    catalog::{NativeSnapshotRef, SnapshotRoot, load_snapshot, snapshot_root},
    io::Scope,
    storage::{
        Cancellation, ClosureLimits, ClosureSpool, IndexNamespaceReadAuthority, LogicalObject,
        ObjectReader, ObjectVersion, PosixStore, ReadAuthority, RecordingReader, SealedObject,
        versioned_stream,
    },
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use iceberg::{
    TableIdent,
    io::{
        FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
        StorageConfig, StorageFactory,
    },
    spec::TableMetadata,
    table::StaticTable,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fmt::Debug, ops::Range, sync::Arc, time::Duration};
use uuid::Uuid;

#[derive(Clone, Debug)]
struct VerifierAuthority {
    source: Arc<IndexNamespaceReadAuthority>,
    key: String,
    owner: Uuid,
    fence: i64,
}
#[async_trait]
impl ReadAuthority for VerifierAuthority {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        self.source.check_read(object).await?;
        self.source
            .scope
            .db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_snapshot_verifier_check($1,$2,$3)",
                &[&self.key, &self.owner, &self.fence],
            )
            .await?;
        Ok(())
    }
}
struct Heartbeat(tokio::task::JoinHandle<()>);
impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone, Debug)]
struct View {
    source: Arc<IndexNamespaceReadAuthority>,
    reader: Arc<dyn ObjectReader>,
    key: String,
    fence: i64,
    reference: NativeSnapshotRef,
    root: SnapshotRoot,
    root_version: ObjectVersion,
}
impl View {
    async fn current(&self) -> Result<()> {
        self.source.check_read(&self.reference.root.object).await
    }
    async fn entry(&self, name: &str) -> Result<(SealedObject, ObjectVersion)> {
        self.current().await?;
        let row=self.source.scope.db.get().await?.query_one("SELECT e.content,e.physical_version FROM moria.runtime_snapshot_verified_entries e JOIN moria.runtime_snapshot_verifications v ON v.key=e.verification AND v.fence=e.fence AND v.status='ready' WHERE e.verification=$1 AND e.fence=$2 AND e.name=$3",&[&self.key,&self.fence,&name]).await?;
        let content: SealedObject = serde_json::from_value(row.get(0))?;
        let version: ObjectVersion = serde_json::from_value(row.get(1))?;
        ensure!(
            content.object.warehouse_id == self.reference.root.object.warehouse_id
                && content.object.table_id == self.reference.root.object.table_id
                && content.size == version.size,
            "BINDING_MISMATCH"
        );
        self.source.check_read(&content.object).await?;
        Ok((content, version))
    }
    async fn bytes(
        &self,
        content: &SealedObject,
        version: ObjectVersion,
        max: u64,
    ) -> Result<Bytes> {
        ensure!(content.size <= max, "LIMIT_EXCEEDED");
        let mut stream = versioned_stream(
            self.reader.clone(),
            content.object.clone(),
            version,
            self.source.clone(),
            Cancellation::default(),
        )
        .await?;
        let mut bytes = Vec::with_capacity(content.size as usize);
        let mut hash = Sha256::new();
        while let Some(frame) = stream.next().await {
            let frame = frame?;
            ensure!(
                bytes
                    .len()
                    .checked_add(frame.len())
                    .is_some_and(|n| n as u64 <= content.size),
                "CONTENT_MISMATCH"
            );
            hash.update(&frame);
            bytes.extend_from_slice(&frame);
        }
        ensure!(
            bytes.len() as u64 == content.size
                && format!("sha256:{:x}", hash.finalize()) == content.sha256,
            "CONTENT_MISMATCH"
        );
        Ok(bytes.into())
    }
    async fn table(&self) -> Result<StaticTable> {
        self.current().await?;
        ensure!(
            self.reader
                .version(
                    &self.reference.root.object,
                    self.source.as_ref(),
                    &Cancellation::default()
                )
                .await?
                == self.root_version,
            "STORAGE_VERSION_CHANGED"
        );
        let (content, version) = self.entry("@metadata").await?;
        ensure!(content == self.root.proof.metadata, "SNAPSHOT_PIN_MISMATCH");
        let metadata: TableMetadata =
            serde_json::from_slice(&self.bytes(&content, version, 8 * 1024 * 1024).await?)?;
        ensure!(
            metadata.uuid() == self.root.proof.table_uuid
                && metadata
                    .snapshot_by_id(self.root.proof.snapshot_id)
                    .is_some(),
            "SNAPSHOT_PIN_MISMATCH"
        );
        let file_io = FileIOBuilder::new(Arc::new(AttestedStorage(self.clone()))).build();
        let table = StaticTable::from_metadata(
            metadata,
            TableIdent::from_strs(["moria", "attested"])?,
            file_io,
        )
        .await?;
        self.current().await?;
        Ok(table)
    }
}

fn denied() -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::DataInvalid,
        "Attested snapshot I/O denied",
    )
}
#[derive(Clone, Debug)]
struct AttestedStorage(View);
impl Serialize for AttestedStorage {
    fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "Live snapshot grants cannot be serialized",
        ))
    }
}
impl<'de> Deserialize<'de> for AttestedStorage {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "Live snapshot grants must be injected",
        ))
    }
}
#[typetag::serde(name = "moria-attested-v1")]
impl StorageFactory for AttestedStorage {
    fn build(&self, _: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.clone()))
    }
}
#[derive(Debug)]
struct AttestedRead {
    view: View,
    content: SealedObject,
    version: ObjectVersion,
}
#[async_trait]
impl FileRead for AttestedRead {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        self.view.current().await.map_err(|_| denied())?;
        self.view
            .reader
            .range_at_version(
                &self.content.object,
                range,
                &self.version,
                self.view.source.as_ref(),
                &Cancellation::default(),
            )
            .await
            .map_err(|_| denied())
    }
}
#[typetag::serde(name = "moria-attested-v1")]
#[async_trait]
impl Storage for AttestedStorage {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        self.metadata(path).await.map(|_| true)
    }
    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        let (content, _) = self.0.entry(path).await.map_err(|_| denied())?;
        Ok(FileMetadata { size: content.size })
    }
    async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
        let (content, version) = self.0.entry(path).await.map_err(|_| denied())?;
        self.0
            .bytes(&content, version, 8 * 1024 * 1024)
            .await
            .map_err(|_| denied())
    }
    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        let (content, version) = self.0.entry(path).await.map_err(|_| denied())?;
        Ok(Box::new(AttestedRead {
            view: self.0.clone(),
            content,
            version,
        }))
    }
    fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
        let prefix = format!(
            "moria://{}/{}/",
            self.0.reference.root.object.warehouse_id, self.0.reference.root.object.table_id
        );
        if path.len() > 512
            || !path.starts_with(&prefix)
            || path[prefix.len()..]
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
        {
            return Err(denied());
        }
        Ok(InputFile::new(Arc::new(self.clone()), path.into()))
    }
    async fn write(&self, _: &str, _: Bytes) -> iceberg::Result<()> {
        Err(denied())
    }
    async fn writer(&self, _: &str) -> iceberg::Result<Box<dyn FileWrite>> {
        Err(denied())
    }
    async fn delete(&self, _: &str) -> iceberg::Result<()> {
        Err(denied())
    }
    async fn delete_prefix(&self, _: &str) -> iceberg::Result<()> {
        Err(denied())
    }
    async fn delete_stream(&self, _: BoxStream<'static, String>) -> iceberg::Result<()> {
        Err(denied())
    }
    fn new_output(&self, _: &str) -> iceberg::Result<OutputFile> {
        Err(denied())
    }
}

/// A once-verified immutable snapshot inventory; permissions are always read from the current source grant.
#[allow(clippy::too_many_arguments)]
pub async fn table(
    scratch: &PosixStore,
    reader: Arc<dyn ObjectReader>,
    source: Arc<IndexNamespaceReadAuthority>,
    reference: NativeSnapshotRef,
    expected_table: Uuid,
    expected_snapshot: i64,
    binding_fingerprint: String,
    limits: ClosureLimits,
) -> Result<StaticTable> {
    let scope: &Scope = &source.scope;
    source.check_read(&reference.root.object).await?;
    let identity=scope.db.get().await?.query_one("SELECT environment,source,(SELECT incarnation::text FROM moria.indexing_instance) FROM moria.indexing_builds WHERE id=$1",&[&scope.build]).await?;
    let reader_fingerprint = format!(
        "iceberg-v2-cow-flat-bounded-v2/{}",
        env!("MORIA_INDEXING_SOURCE_SHA256")
    );
    let key = crate::canonical::fingerprint(
        &json!({"version":1,"environment":identity.get::<_,String>(0),"resource":identity.get::<_,String>(1),"incarnation":identity.get::<_,String>(2),"reference":reference,"tableUuid":expected_table,"snapshotId":expected_snapshot.to_string(),"binding":binding_fingerprint,"reader":reader_fingerprint}),
    )?;
    let owner = Uuid::new_v4();
    let mut claimed = None;
    for _ in 0..200 {
        source.check_read(&reference.root.object).await?;
        let mut client = scope.db.get().await?;
        let tx = client.transaction().await?;
        tx.execute("INSERT INTO moria.runtime_snapshot_verifications(key,environment,resource,reference,binding_fingerprint,reader_fingerprint,status,owner,fence,lease_until) VALUES($1,$2,$3,$4,$5,$6,'verifying',$7,1,clock_timestamp()+interval '30 seconds') ON CONFLICT DO NOTHING",
            &[&key,&identity.get::<_,String>(0),&identity.get::<_,String>(1),&serde_json::to_value(&reference)?,&binding_fingerprint,&reader_fingerprint,&owner]).await?;
        let row=tx.query_one("SELECT *,lease_until<=clock_timestamp() AS expired FROM moria.runtime_snapshot_verifications WHERE key=$1 FOR UPDATE",&[&key]).await?;
        if row.get::<_, String>("status") == "ready" {
            let root: SnapshotRoot = serde_json::from_value(row.get("root"))?;
            let root_version: ObjectVersion = serde_json::from_value(row.get("root_version"))?;
            let fence = row.get("fence");
            tx.commit().await?;
            let view = View {
                source: source.clone(),
                reader,
                key,
                fence,
                reference,
                root,
                root_version,
            };
            return view.table().await;
        }
        if row.get::<_, Uuid>("owner") == owner {
            claimed = Some(row.get::<_, i64>("fence"));
            tx.commit().await?;
            break;
        }
        if row.get::<_, bool>("expired") {
            let fence:i64=tx.query_one("UPDATE moria.runtime_snapshot_verifications SET owner=$2,fence=fence+1,lease_until=clock_timestamp()+interval '30 seconds' WHERE key=$1 RETURNING fence",&[&key,&owner]).await?.get(0);
            claimed = Some(fence);
            tx.commit().await?;
            break;
        }
        tx.commit().await?;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    let fence = claimed.ok_or_else(|| anyhow::anyhow!("VERIFICATION_BUSY"))?;
    println!(
        "{}",
        json!({"event":"snapshot_verification_claimed","key":key,"owner":owner,"fence":fence.to_string()})
    );
    let verifier = Arc::new(VerifierAuthority {
        source: source.clone(),
        key: key.clone(),
        owner,
        fence,
    });
    let cancel = Cancellation::default();
    let heartbeat = {
        let verifier = verifier.clone();
        let object = reference.root.object.clone();
        let cancel = cancel.clone();
        Heartbeat(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let result=async{verifier.check_read(&object).await?;let n=verifier.source.scope.db.get().await?.execute("UPDATE moria.runtime_snapshot_verifications SET lease_until=clock_timestamp()+interval '30 seconds' WHERE key=$1 AND owner=$2 AND fence=$3 AND status='verifying' AND lease_until>clock_timestamp()",&[&verifier.key,&verifier.owner,&verifier.fence]).await?;ensure!(n==1,"VERIFIER_FENCE_LOST");Ok::<_,anyhow::Error>(())}.await;
                if result.is_err() {
                    cancel.cancel();
                    return;
                }
            }
        }))
    };
    ensure!(limits.max_spool_bytes >= 128 * 1024, "LIMIT_EXCEEDED");
    let budget = ClosureLimits {
        max_spool_bytes: limits.max_spool_bytes / 2,
        ..limits
    };
    let cache = ClosureSpool::create_read_cache(
        scratch,
        LogicalObject::new(
            &reference.root.object.warehouse_id,
            &reference.root.object.table_id,
            &Uuid::new_v4().to_string(),
        )?,
        budget,
        verifier.clone(),
        cancel.clone(),
    )
    .await?;
    cache.database()?.execute_batch("CREATE TABLE verified_versions(object_id TEXT PRIMARY KEY,version TEXT NOT NULL,actual_hash TEXT) WITHOUT ROWID;")?;
    let recording = Arc::new(RecordingReader {
        inner: reader.clone(),
        spool: cache,
    });
    let loaded = load_snapshot(
        scratch,
        recording.clone(),
        verifier.clone(),
        &reference,
        expected_table,
        expected_snapshot,
        budget,
        cancel.clone(),
    )
    .await?;
    let root = snapshot_root(recording.as_ref(), verifier.clone(), &reference, cancel).await?;
    let root_version = recording.verified(&reference.root)?;
    let mut after = None;
    loop {
        let entries = loaded
            .spool()
            .read_page(after.as_deref(), verifier.as_ref())
            .await?;
        if entries.is_empty() {
            break;
        }
        after = Some(entries.last().unwrap().name.clone());
        let rows=entries.iter().map(|entry|Ok(json!({"name":entry.name,"content":entry.content,"physicalVersion":recording.verified(&entry.content)?}))).collect::<Result<Vec<_>>>()?;
        export(&verifier, Value::Array(rows)).await?;
    }
    export(&verifier,json!([{"name":"@metadata","content":root.proof.metadata,"physicalVersion":loaded.verified_version(&root.proof.metadata)?}])).await?;
    verifier.check_read(&reference.root.object).await?;
    let mut client = scope.db.get().await?;
    let tx = client.transaction().await?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))",
        &[&format!(
            "moria/environment/{}",
            identity.get::<_, String>(0)
        )],
    )
    .await?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock_shared(hashtextextended('moria/filesystem',0))",
        &[],
    )
    .await?;
    tx.query_one(
        "SELECT 1 FROM moria.environments WHERE id=$1 FOR SHARE",
        &[&identity.get::<_, String>(0)],
    )
    .await?;
    tx.query_one(
        "SELECT 1 FROM moria.indexing_builds WHERE id=$1 FOR SHARE",
        &[&scope.build],
    )
    .await?;
    if let Some(task) = scope.task {
        tx.query_one(
            "SELECT 1 FROM moria.indexing_tasks WHERE id=$1 FOR SHARE",
            &[&task],
        )
        .await?;
    }
    tx.query_one(
        "SELECT moria.native_storage_check($1,$2,$3,$4,$5,$6,$7,false)",
        &[
            &scope.build,
            &scope.task,
            &scope.owner,
            &scope.fence,
            &source.namespace.warehouse_id,
            &source.namespace.table_id,
            &source.namespace.revision,
        ],
    )
    .await?;
    tx.query_one(
        "SELECT moria.runtime_snapshot_verifier_check($1,$2,$3)",
        &[&key, &owner, &fence],
    )
    .await?;
    let count:i64=tx.query_one("SELECT count(*) FROM moria.runtime_snapshot_verified_entries WHERE verification=$1 AND fence=$2",&[&key,&fence]).await?.get(0);
    ensure!(
        count as u64 == root.proof.closure.entries + 1,
        "INCOMPLETE_VERIFICATION"
    );
    let n=tx.execute("UPDATE moria.runtime_snapshot_verifications SET status='ready',root=$4,root_version=$5,verification_count=verification_count+1,verified_at=clock_timestamp() WHERE key=$1 AND owner=$2 AND fence=$3 AND status='verifying' AND lease_until>clock_timestamp()",&[&key,&owner,&fence,&serde_json::to_value(&root)?,&serde_json::to_value(&root_version)?]).await?;
    ensure!(n == 1, "VERIFIER_FENCE_LOST");
    tx.commit().await?;
    drop(heartbeat);
    View {
        source,
        reader,
        key,
        fence,
        reference,
        root,
        root_version,
    }
    .table()
    .await
}

async fn export(verifier: &VerifierAuthority, rows: Value) -> Result<()> {
    ensure!(
        serde_json::to_vec(&rows)?.len() <= 1024 * 1024,
        "LIMIT_EXCEEDED"
    );
    let mut client = verifier.source.scope.db.get().await?;
    let tx = client.transaction().await?;
    tx.query_one(
        "SELECT moria.runtime_snapshot_verifier_check($1,$2,$3)",
        &[&verifier.key, &verifier.owner, &verifier.fence],
    )
    .await?;
    tx.execute("INSERT INTO moria.runtime_snapshot_verified_entries(verification,fence,name,content,physical_version) SELECT $1,$2,item->>'name',item->'content',item->'physicalVersion' FROM jsonb_array_elements($3::jsonb) item",&[&verifier.key,&verifier.fence,&rows]).await?;
    tx.commit().await?;
    Ok(())
}
