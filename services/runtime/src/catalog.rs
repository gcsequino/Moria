use crate::{
    authority::{Authority, VerifiedSession},
    database::Database,
    execution::Lease,
    snapshot::{SnapshotPin, SnapshotProof, verify_retained_subset, verify_snapshot},
    storage::{
        Cancellation, ClosureEntry, ClosureLimits, ClosureSpool, IndexNamespace, LogicalObject,
        ObjectReader, ObjectStore, ObjectVersion, ObjectWriter, PosixStore, ReadAuthority,
        RecordingReader, SealedObject, VerifiedVersionAuthority, WriteAuthority,
    },
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

const REF_PREFIX: &str = "moria-snapshot-v2:";
const PAGE_MAX: usize = 256 * 1024 + 4096;

/// A current Dataset capability constructed from a verified session and a real producer lease.
/// The captured namespace state includes every input's inherited marking and binding revision.
#[derive(Clone, Debug)]
pub struct DatasetIoGrant {
    db: Database,
    session: VerifiedSession,
    lease: Lease,
    namespace: IndexNamespace,
    state: Value,
}

impl DatasetIoGrant {
    pub(crate) fn is_managed(&self) -> bool {
        !self.state["datasetBinding"].is_null()
    }
    pub(crate) fn namespace(&self) -> &IndexNamespace {
        &self.namespace
    }
    pub(crate) fn session(&self) -> &VerifiedSession {
        &self.session
    }
    pub(crate) fn lease(&self) -> &Lease {
        &self.lease
    }
    pub(crate) fn database(&self) -> &Database {
        &self.db
    }
    pub(crate) fn expected_revision(&self) -> Result<i64> {
        Ok(self.state["expectedRevision"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("UNDECLARED_DATASET"))?
            .parse()?)
    }
    pub(crate) fn validate_store(&self, store: &dyn ObjectReader) -> Result<()> {
        if !self.state["datasetBinding"].is_null() {
            ensure!(
                self.state["datasetBinding"]["physicalBindingFingerprint"].as_str()
                    == Some(store.physical_fingerprint()?.as_str()),
                "DATASET_BINDING_INVALID"
            );
        }
        Ok(())
    }
    pub(crate) fn primary_key(&self) -> Result<Vec<String>> {
        if self.state["datasetBinding"].is_null() {
            return Ok(Vec::new());
        }
        Ok(serde_json::from_value(
            self.state["datasetBinding"]["primaryKey"].clone(),
        )?)
    }
    pub(crate) fn key_schema_hash(&self) -> Result<&str> {
        self.state["datasetBinding"]["keySchemaHash"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("DATASET_PRIMARY_KEY_INVALID"))
    }
    pub(crate) fn require_write_mode(&self, mode: &str) -> Result<()> {
        ensure!(
            ["replace", "append", "upsert", "delete"].contains(&mode),
            "DATASET_WRITE_MODE_INVALID"
        );
        if self.state["datasetBinding"].is_null() {
            ensure!(mode == "replace", "DATASET_WRITE_CAPABILITY_REQUIRED");
        } else {
            ensure!(
                self.state["datasetBinding"]["writeModes"]
                    .as_array()
                    .is_some_and(|modes| modes.contains(&serde_json::json!(mode))),
                "DATASET_WRITE_MODE_DENIED"
            );
        }
        Ok(())
    }
    pub(crate) fn validate_schema(&self, schema: &iceberg::spec::Schema) -> Result<()> {
        if !self.state["datasetBinding"].is_null() {
            let accepted: iceberg::spec::Schema =
                serde_json::from_value(self.state["datasetBinding"]["icebergSchema"].clone())?;
            ensure!(&accepted == schema, "SCHEMA_CHANGED");
        }
        Ok(())
    }
    pub async fn acquire(
        db: Database,
        session: VerifiedSession,
        lease: Lease,
        namespace: IndexNamespace,
    ) -> Result<Self> {
        let client = db.get().await?;
        let _ = Authority::current(&**client, &session).await?;
        drop(client);
        let mut grant = Self {
            db: db.clone(),
            session,
            lease,
            namespace,
            state: Value::Null,
        };
        grant.state = grant.current(false).await?;
        Ok(grant)
    }
    async fn current(&self, write: bool) -> Result<Value> {
        let client = self.db.get().await?;
        self.current_in(&client, write).await
    }
    async fn current_in(&self, client: &tokio_postgres::Client, write: bool) -> Result<Value> {
        let fence: i64 = self.lease.fence.parse()?;
        ensure!(
            fence > 0 && fence.to_string() == self.lease.fence,
            "LEASE_LOST"
        );
        let value: Value = client
            .query_one(
                "SELECT moria.runtime_dataset_io_check($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                &[
                    &self.session.session_hash(),
                    &self.lease.execution,
                    &self.lease.task,
                    &self.lease.owner,
                    &fence,
                    &self.lease.incarnation,
                    &self.namespace.warehouse_id,
                    &self.namespace.table_id,
                    &self.namespace.revision,
                    &write,
                ],
            )
            .await?
            .get(0);
        Ok(value)
    }
    fn bound(&self, object: &LogicalObject) -> Result<()> {
        // LogicalObject::new validates all opaque IDs, including object IDs received through serde.
        ensure!(
            object.version == 1
                && LogicalObject::new(&object.warehouse_id, &object.table_id, &object.object_id)?
                    == *object
                && object.warehouse_id == self.namespace.warehouse_id
                && object.table_id == self.namespace.table_id,
            "BINDING_MISMATCH"
        );
        Ok(())
    }
    pub fn resource(&self) -> &str {
        self.state["resource"].as_str().unwrap()
    }
    pub fn table_uuid(&self) -> Result<Uuid> {
        Ok(self.state["icebergUuid"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("BINDING_MISMATCH"))?
            .parse()?)
    }
    pub fn consumed_markings(&self) -> Result<Vec<String>> {
        let mut labels = std::collections::BTreeSet::new();
        for field in ["markings", "inheritedMarkings"] {
            let values: Vec<String> = serde_json::from_value(self.state[field].clone())?;
            labels.extend(values);
        }
        Ok(labels.into_iter().collect())
    }
    pub async fn check_current(&self) -> Result<()> {
        ensure!(
            self.current(false).await? == self.state,
            "STORAGE_NAMESPACE_REVOKED"
        );
        Ok(())
    }
    pub(crate) async fn check_current_in(&self, client: &tokio_postgres::Client) -> Result<()> {
        ensure!(
            self.current_in(client, false).await? == self.state,
            "STORAGE_NAMESPACE_REVOKED"
        );
        Ok(())
    }
}

#[async_trait]
impl ReadAuthority for DatasetIoGrant {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        self.bound(object)?;
        self.check_current().await?;
        self.db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_object_read($1,$2,$3)",
                &[&object.warehouse_id, &object.table_id, &object.object_id],
            )
            .await?;
        Ok(())
    }
}
#[async_trait]
impl WriteAuthority for DatasetIoGrant {
    fn scope_key(&self) -> String {
        format!(
            "dataset/{}/{}/{}/{}/{}/{}/{}/{}",
            self.lease.execution,
            self.lease.task.map(|v| v.to_string()).unwrap_or_default(),
            self.lease.owner,
            self.lease.fence,
            self.lease.incarnation,
            self.namespace.warehouse_id,
            self.namespace.table_id,
            self.namespace.revision
        )
    }
    async fn check_write(&self, object: &LogicalObject) -> Result<()> {
        self.bound(object)?;
        ensure!(
            self.current(true).await? == self.state,
            "STORAGE_NAMESPACE_REVOKED"
        );
        Ok(())
    }
    async fn record_multipart(
        &self,
        object: &LogicalObject,
        upload_id: &str,
        physical: &str,
    ) -> Result<()> {
        self.bound(object)?;
        self.db.get().await?.query_one("SELECT moria.runtime_dataset_record_multipart($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",&[&self.session.session_hash(),&self.lease.execution,&self.lease.task,&self.lease.owner,&self.lease.fence.parse::<i64>()?,&self.lease.incarnation,&self.namespace.warehouse_id,&self.namespace.table_id,&self.namespace.revision,&physical,&object.object_id,&upload_id]).await?;
        Ok(())
    }
    async fn admit_write(
        &self,
        object: &LogicalObject,
        size: u64,
        sha256: &str,
        physical: &str,
    ) -> Result<()> {
        self.bound(object)?;
        self.db.get().await?.query_one(
            "SELECT moria.runtime_dataset_admit_write($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
            &[&self.session.session_hash(),&self.lease.execution,&self.lease.task,&self.lease.owner,&self.lease.fence.parse::<i64>()?,&self.lease.incarnation,
              &self.namespace.warehouse_id,&self.namespace.table_id,&self.namespace.revision,&physical,&object.object_id,&i64::try_from(size)?,&sha256]
        ).await?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NativeSnapshotRef {
    pub version: u32,
    pub root: SealedObject,
}
impl NativeSnapshotRef {
    pub fn encode(&self) -> Result<String> {
        self.validate()?;
        Ok(format!(
            "{REF_PREFIX}{}",
            crate::canonical::canonical_json(&serde_json::to_value(self)?)?
        ))
    }
    pub fn decode(value: &str) -> Result<Self> {
        ensure!(value.len() <= 4096, "INVALID_NATIVE_SNAPSHOT_REF");
        let result: Self = serde_json::from_str(
            value
                .strip_prefix(REF_PREFIX)
                .ok_or_else(|| anyhow::anyhow!("INVALID_NATIVE_SNAPSHOT_REF"))?,
        )?;
        result.validate()?;
        ensure!(
            result.encode()? == value,
            "NONCANONICAL_NATIVE_SNAPSHOT_REF"
        );
        Ok(result)
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 2 && self.root.size > 0 && self.root.size <= 16384,
            "INVALID_NATIVE_SNAPSHOT_REF"
        );
        validate_object(&self.root)?;
        Ok(())
    }
}

fn validate_object(object: &SealedObject) -> Result<()> {
    ensure!(
        object.object.version == 1
            && LogicalObject::new(
                &object.object.warehouse_id,
                &object.object.table_id,
                &object.object.object_id
            )? == object.object
            && object.size <= 9_007_199_254_740_991
            && object.sha256.len() == 71
            && object.sha256.starts_with("sha256:")
            && object.sha256[7..]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "INVALID_NATIVE_SNAPSHOT_REF"
    );
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ClosurePage {
    pub version: u32,
    pub ordinal: u64,
    pub previous: Option<SealedObject>,
    pub entries: Vec<ClosureEntry>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SnapshotRoot {
    pub version: u32,
    pub proof: SnapshotProof,
    pub pages: u64,
    pub last_page: Option<SealedObject>,
}

async fn put_json<T: Serialize>(
    store: &dyn ObjectWriter,
    authority: Arc<dyn WriteAuthority>,
    namespace: &IndexNamespace,
    value: &T,
    max: usize,
    cancel: Cancellation,
) -> Result<SealedObject> {
    let bytes = crate::canonical::canonical_json(&serde_json::to_value(value)?)?.into_bytes();
    ensure!(bytes.len() <= max, "LIMIT_EXCEEDED");
    let object = LogicalObject::new(
        &namespace.warehouse_id,
        &namespace.table_id,
        &Uuid::new_v4().to_string(),
    )?;
    let mut write = store
        .begin_object(
            object,
            bytes.len() as u64,
            format!("sha256:{:x}", Sha256::digest(&bytes)),
            authority,
            cancel,
        )
        .await?;
    write.append(&bytes).await?;
    write.seal().await
}

pub(crate) async fn write_snapshot_root(
    store: Arc<dyn ObjectStore>,
    read: Arc<dyn ReadAuthority>,
    write: Arc<dyn WriteAuthority>,
    namespace: &IndexNamespace,
    spool: &ClosureSpool,
    proof: SnapshotProof,
    cancel: Cancellation,
) -> Result<(NativeSnapshotRef, SnapshotRoot)> {
    let recording = Arc::new(RecordingReader::new(store.clone(), spool.clone())?);
    for counter in [
        proof.closure.entries,
        proof.closure.referenced_bytes,
        proof.data_files,
        proof.physical_rows,
        proof.scanned_snapshots,
    ] {
        ensure!(counter <= 9_007_199_254_740_991, "LIMIT_EXCEEDED");
    }
    let mut after = None;
    let mut last_page = None;
    let mut pages = 0u64;
    loop {
        let entries = spool.read_page(after.as_deref(), read.as_ref()).await?;
        if entries.is_empty() {
            break;
        }
        after = Some(entries.last().unwrap().name.clone());
        let page = ClosurePage {
            version: 2,
            ordinal: pages,
            previous: last_page,
            entries,
        };
        let page_object = put_json(
            store.as_ref(),
            write.clone(),
            namespace,
            &page,
            PAGE_MAX,
            cancel.clone(),
        )
        .await?;
        let _: ClosurePage = read_descriptor(
            recording.as_ref(),
            read.clone(),
            &page_object,
            PAGE_MAX,
            cancel.clone(),
        )
        .await?;
        last_page = Some(page_object);
        pages = pages
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("LIMIT_EXCEEDED"))?;
    }
    let root = SnapshotRoot {
        version: 2,
        proof,
        pages,
        last_page,
    };
    let root_object = put_json(
        store.as_ref(),
        write.clone(),
        namespace,
        &root,
        16384,
        cancel.clone(),
    )
    .await?;
    let _: SnapshotRoot = read_descriptor(
        recording.as_ref(),
        read.clone(),
        &root_object,
        16384,
        cancel.clone(),
    )
    .await?;
    recording.check_versions(read.as_ref(), &cancel).await?;
    Ok((
        NativeSnapshotRef {
            version: 2,
            root: root_object,
        },
        root,
    ))
}

/// Prepare performs all engine I/O before taking the environment publication lock.
/// The returned ID references a server-created row; a caller-supplied SnapshotProof is never accepted.
pub async fn prepare_snapshot(
    store: Arc<dyn ObjectStore>,
    grant: Arc<DatasetIoGrant>,
    spool: &ClosureSpool,
    pin: SnapshotPin,
    cancel: Cancellation,
) -> Result<Uuid> {
    ensure!(
        grant.lease.task.is_none() && pin.table_uuid == grant.table_uuid()?,
        "COORDINATOR_REQUIRED"
    );
    grant.require_write_mode("replace")?;
    grant.check_write(&pin.metadata.object).await?;
    grant.validate_store(store.as_ref())?;
    let proof = verify_snapshot(spool, store.clone(), grant.clone(), pin).await?;
    let table =
        crate::snapshot::verified_table(spool, store.clone(), grant.clone(), &proof).await?;
    grant.validate_schema(table.metadata().current_schema().as_ref())?;
    let key_rows =
        crate::dataset_writer::validate_primary_keys(spool, &table, &grant, &proof).await?;
    let (native_reference, root) = write_snapshot_root(
        store.clone(),
        grant.clone(),
        grant.clone(),
        grant.namespace(),
        spool,
        proof,
        cancel.clone(),
    )
    .await?;
    crate::dataset_retention::register_native_root(
        store.clone(),
        grant.clone(),
        &native_reference,
        &root,
        spool,
        cancel.clone(),
    )
    .await?;
    let reference = native_reference.encode()?;
    let mut client = grant.db.get().await?;
    let tx = client.transaction().await?;
    let current: Value = tx
        .query_one(
            "SELECT moria.runtime_dataset_io_check($1,$2,NULL,$3,$4,$5,$6,$7,$8,true)",
            &[
                &grant.session.session_hash(),
                &grant.lease.execution,
                &grant.lease.owner,
                &grant.lease.fence.parse::<i64>()?,
                &grant.lease.incarnation,
                &grant.namespace.warehouse_id,
                &grant.namespace.table_id,
                &grant.namespace.revision,
            ],
        )
        .await?
        .get(0);
    ensure!(current == grant.state, "STORAGE_NAMESPACE_REVOKED");
    if let Some(rows) = key_rows {
        tx.execute("INSERT INTO moria.runtime_dataset_key_certificates(root,key_schema_hash,snapshot_id,key_rows) VALUES(moria.runtime_dataset_root_key($1),$2,$3,$4) ON CONFLICT DO NOTHING",&[&reference,&grant.key_schema_hash()?,&root.proof.snapshot_id.to_string(),&i64::try_from(rows)?]).await?;
        ensure!(tx.query_one("SELECT key_rows=$3 AND snapshot_id=$4 FROM moria.runtime_dataset_key_certificates WHERE root=moria.runtime_dataset_root_key($1) AND key_schema_hash=$2",&[&reference,&grant.key_schema_hash()?,&i64::try_from(rows)?,&root.proof.snapshot_id.to_string()]).await?.get::<_,bool>(0),"DATASET_PRIMARY_KEY_INVALID");
    }
    let delta_profile = if grant.is_managed() {
        Some(crate::dataset_commit_delta::profile(
            table.metadata().current_schema(),
            &grant.state["datasetBinding"]["icebergSchema"],
            grant.state["datasetBinding"]["revision"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("DATASET_BINDING_INVALID"))?,
            &grant.primary_key()?,
            grant.key_schema_hash()?,
        )?)
    } else {
        None
    };
    let id = Uuid::new_v4();
    let expected = grant.state["expectedRevision"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("UNDECLARED_DATASET"))?
        .parse::<i64>()?;
    tx.execute("INSERT INTO moria.runtime_snapshot_candidates(id,environment,execution,resource,expected_revision,namespace_state,session_hash,owner,fence,incarnation,metadata_location,snapshot_id,proof,delta_profile) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
        &[&id,&grant.session.environment(),&grant.lease.execution,&grant.resource(),&expected,&grant.state,&grant.session.session_hash(),
        &grant.lease.owner,&grant.lease.fence.parse::<i64>()?,&grant.lease.incarnation,&reference,&root.proof.snapshot_id.to_string(),&serde_json::to_value(&root.proof)?,&delta_profile]).await?;
    tx.commit().await?;
    Ok(id)
}

pub async fn publish_snapshots(
    db: &Database,
    session: &VerifiedSession,
    lease: &Lease,
    candidates: &[Uuid],
    checkpoint: Option<Value>,
) -> Result<Value> {
    ensure!(lease.task.is_none(), "COORDINATOR_REQUIRED");
    Ok(db
        .get()
        .await?
        .query_one(
            "SELECT moria.runtime_snapshot_publish($1,$2,$3,$4,$5,$6,$7)",
            &[
                &session.session_hash(),
                &lease.execution,
                &lease.owner,
                &lease.fence.parse::<i64>()?,
                &lease.incarnation,
                &candidates,
                &checkpoint,
            ],
        )
        .await?
        .get(0))
}

/// A bounded, digest-checked protected object fetch used by native consumers of immutable closure pages.
pub async fn read_descriptor<T: serde::de::DeserializeOwned + Serialize>(
    reader: &dyn ObjectReader,
    authority: Arc<dyn ReadAuthority>,
    object: &SealedObject,
    maximum: usize,
    cancel: Cancellation,
) -> Result<T> {
    validate_object(object)?;
    ensure!(object.size <= maximum as u64, "LIMIT_EXCEEDED");
    let stream = reader
        .read_object(object.object.clone(), authority, cancel)
        .await?;
    decode_descriptor(stream, object).await
}

async fn decode_descriptor<T: serde::de::DeserializeOwned + Serialize>(
    mut stream: futures::stream::BoxStream<'static, Result<bytes::Bytes>>,
    object: &SealedObject,
) -> Result<T> {
    let mut bytes = Vec::with_capacity(object.size as usize);
    let mut hash = Sha256::new();
    while let Some(frame) = stream.next().await {
        let frame = frame?;
        ensure!(
            bytes
                .len()
                .checked_add(frame.len())
                .is_some_and(|n| n <= object.size as usize),
            "CONTENT_MISMATCH"
        );
        hash.update(&frame);
        bytes.extend_from_slice(&frame);
    }
    ensure!(
        bytes.len() == object.size as usize
            && format!("sha256:{:x}", hash.finalize()) == object.sha256,
        "CONTENT_MISMATCH"
    );
    let result: T = serde_json::from_slice(&bytes)?;
    ensure!(
        crate::canonical::canonical_json(&serde_json::to_value(&result)?)?.as_bytes() == bytes,
        "NONCANONICAL_NATIVE_SNAPSHOT_OBJECT"
    );
    Ok(result)
}

fn unsupported_immutable_version(error: &anyhow::Error) -> bool {
    matches!(
        error.root_cause().to_string().as_str(),
        "INVALID_STORAGE_VERSION" | "CONDITIONAL_DELETE_UNSUPPORTED"
    )
}

/// Wrapper metadata is read at its actual qualified version, never a substituted current copy.
async fn read_qualified_descriptor<T: serde::de::DeserializeOwned + Serialize>(
    reader: Arc<dyn ObjectReader>,
    authority: Arc<dyn ReadAuthority>,
    object: &SealedObject,
    maximum: usize,
    known: &dyn VerifiedVersionAuthority,
    cancel: Cancellation,
) -> Result<Option<T>> {
    validate_object(object)?;
    ensure!(object.size <= maximum as u64, "LIMIT_EXCEEDED");
    let version = known.verified_version(object).await?;
    match crate::storage::qualified_s3_version(&version) {
        Ok(()) => {}
        Err(error) if unsupported_immutable_version(&error) => return Ok(None),
        Err(error) => return Err(error),
    }
    ensure!(version.size == object.size, "CONTENT_MISMATCH");
    let stream =
        crate::storage::versioned_stream(reader, object.object.clone(), version, authority, cancel)
            .await?;
    Ok(Some(decode_descriptor(stream, object).await?))
}

/// Iterate a sealed closure with constant working memory. Consumers must also enforce configured total entry/byte budgets.
pub async fn snapshot_root(
    reader: &dyn ObjectReader,
    authority: Arc<dyn ReadAuthority>,
    reference: &NativeSnapshotRef,
    cancel: Cancellation,
) -> Result<SnapshotRoot> {
    reference.validate()?;
    let root: SnapshotRoot =
        read_descriptor(reader, authority, &reference.root, 16384, cancel).await?;
    validate_snapshot_root(reference, &root)?;
    Ok(root)
}
fn validate_snapshot_root(reference: &NativeSnapshotRef, root: &SnapshotRoot) -> Result<()> {
    for count in [
        root.pages,
        root.proof.closure.entries,
        root.proof.closure.referenced_bytes,
        root.proof.data_files,
        root.proof.physical_rows,
        root.proof.scanned_snapshots,
    ] {
        ensure!(count <= 9_007_199_254_740_991, "LIMIT_EXCEEDED");
    }
    validate_object(&root.proof.metadata)?;
    if let Some(page) = &root.last_page {
        validate_object(page)?;
        ensure!(
            page.object.warehouse_id == reference.root.object.warehouse_id
                && page.object.table_id == reference.root.object.table_id,
            "BINDING_MISMATCH"
        );
    }
    ensure!(
        root.version == 2
            && root.proof.version == 1
            && root.proof.reader_profile == "iceberg-v2-cow-flat-bounded-v2"
            && root.pages == root.proof.closure.entries.div_ceil(256)
            && (root.pages > 0) == root.last_page.is_some()
            && root.proof.metadata.object.warehouse_id == reference.root.object.warehouse_id
            && root.proof.metadata.object.table_id == reference.root.object.table_id,
        "INVALID_NATIVE_SNAPSHOT_ROOT"
    );
    Ok(())
}
pub async fn snapshot_page(
    reader: &dyn ObjectReader,
    authority: Arc<dyn ReadAuthority>,
    object: &SealedObject,
    ordinal: u64,
    cancel: Cancellation,
) -> Result<ClosurePage> {
    let page: ClosurePage = read_descriptor(reader, authority, object, PAGE_MAX, cancel).await?;
    validate_snapshot_page(object, ordinal, &page)?;
    Ok(page)
}
fn validate_snapshot_page(object: &SealedObject, ordinal: u64, page: &ClosurePage) -> Result<()> {
    ensure!(
        page.version == 2
            && page.ordinal == ordinal
            && !page.entries.is_empty()
            && page.entries.len() <= 256
            && (ordinal > 0) == page.previous.is_some(),
        "INVALID_NATIVE_SNAPSHOT_PAGE"
    );
    for entry in &page.entries {
        validate_object(&entry.content)?;
        ensure!(
            entry.content.object.warehouse_id == object.object.warehouse_id
                && entry.content.object.table_id == object.object.table_id,
            "BINDING_MISMATCH"
        );
    }
    if let Some(previous) = &page.previous {
        validate_object(previous)?;
        ensure!(
            previous.object.warehouse_id == object.object.warehouse_id
                && previous.object.table_id == object.object.table_id,
            "BINDING_MISMATCH"
        );
    }
    Ok(())
}

pub struct LoadedSnapshot {
    spool: ClosureSpool,
    proof: SnapshotProof,
    reader: Arc<dyn ObjectReader>,
    authority: Arc<dyn ReadAuthority>,
}

/// Metadata resolved from authority state; this record alone never grants storage access.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatasetPin {
    pub resource: Uuid,
    pub revision: String,
    pub snapshot: String,
    pub metadata_location: String,
    pub namespace: IndexNamespace,
    pub table_uuid: Uuid,
    pub markings: Vec<String>,
    pub security_epoch: String,
    pub instance_epoch: String,
}
impl DatasetPin {
    pub fn execution_input(&self) -> crate::execution::PinnedInput {
        crate::execution::PinnedInput {
            resource: self.resource,
            revision: self.revision.clone(),
            snapshot: self.snapshot.clone(),
        }
    }
}

async fn dataset_resource(
    client: &tokio_postgres::Transaction<'_>,
    session: &VerifiedSession,
    resource: &str,
) -> Result<Uuid> {
    ensure!(
        !resource.is_empty() && resource.len() <= 1024,
        "DATASET_UNKNOWN"
    );
    let rows=client.query("SELECT e.resource_id FROM moria.fs_entries e JOIN moria.fs_resource_bindings b ON b.entry_id=e.id WHERE e.environment_id=$1 AND NOT e.deleted AND e.kind='resource' AND (e.resource_id::text=$2 OR b.reference=$2) LIMIT 2",&[&session.environment(),&resource]).await?;
    ensure!(rows.len() <= 1, "DATASET_BINDING_AMBIGUOUS");
    Ok(match rows.first() {
        Some(row) => row
            .get::<_, Option<Uuid>>(0)
            .ok_or_else(|| anyhow::anyhow!("DATASET_UNKNOWN"))?,
        None => resource
            .parse()
            .map_err(|_| anyhow::anyhow!("DATASET_UNKNOWN"))?,
    })
}
async fn dataset_pin(
    client: &tokio_postgres::Transaction<'_>,
    session: &VerifiedSession,
    resource: Uuid,
    row: &tokio_postgres::Row,
) -> Result<DatasetPin> {
    let metadata: String = row.get("metadata_location");
    let reference = NativeSnapshotRef::decode(&metadata)?;
    let revision: String = row.get("revision");
    let snapshot: String = row.get("snapshot_id");
    ensure!(
        snapshot.parse::<i64>()?.to_string() == snapshot,
        "INPUT_PIN_CHANGED"
    );
    let labels: Vec<String> = client
        .query_one(
            "SELECT moria.runtime_dataset_labels($1,$2)",
            &[&session.session_hash(), &resource.to_string()],
        )
        .await?
        .get(0);
    let namespace=client.query_one("SELECT revision,iceberg_uuid FROM moria.native_storage_namespaces WHERE environment=$1 AND resource=$2 AND purpose='dataset-source' AND enabled AND warehouse_id=$3 AND table_id=$4",&[&session.environment(),&resource.to_string(),&reference.root.object.warehouse_id,&reference.root.object.table_id]).await?;
    let current = Authority::current(client, session).await?;
    client
        .query_one(
            "SELECT moria.runtime_dataset_binding($1,$2,$3,$4,$5)",
            &[
                &session.environment(),
                &resource.to_string(),
                &reference.root.object.warehouse_id,
                &reference.root.object.table_id,
                &namespace.get::<_, String>("revision"),
            ],
        )
        .await?;
    Ok(DatasetPin {
        resource,
        revision,
        snapshot,
        metadata_location: metadata,
        namespace: IndexNamespace {
            warehouse_id: reference.root.object.warehouse_id,
            table_id: reference.root.object.table_id,
            revision: namespace.get("revision"),
        },
        table_uuid: namespace
            .get::<_, Option<Uuid>>("iceberg_uuid")
            .ok_or_else(|| anyhow::anyhow!("INVALID_DATASET_BINDING"))?,
        markings: labels,
        security_epoch: current.security_epoch,
        instance_epoch: current.instance_epoch,
    })
}
async fn dataset_admission(
    client: &tokio_postgres::Transaction<'_>,
    session: &VerifiedSession,
) -> Result<()> {
    client
        .batch_execute("SET LOCAL statement_timeout='5s'")
        .await?;
    client
        .query_one(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))",
            &[&format!("moria/environment/{}", session.environment())],
        )
        .await?;
    client
        .query_one(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended('moria/filesystem',0))",
            &[],
        )
        .await?;
    client
        .query_one(
            "SELECT security_epoch FROM moria.environments WHERE id=$1 FOR SHARE",
            &[&session.environment()],
        )
        .await?;
    Authority::current(client, session).await?;
    Ok(())
}

/// Resolve a current whole-Dataset input before constructing an immutable execution specification.
pub async fn resolve_dataset_pin(
    db: &Database,
    session: &VerifiedSession,
    resource: &str,
) -> Result<DatasetPin> {
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    let pin = resolve_dataset_pin_in(&tx, session, resource).await?;
    tx.commit().await?;
    Ok(pin)
}
/// Resolve a retained Iceberg snapshot before accepting the execution that will pin its actual root.
/// The optional revision cap is a catalog-history bound, not permission to synthesize a later input pin.
pub async fn resolve_dataset_pin_at(
    db: &Database,
    session: &VerifiedSession,
    resource: &str,
    snapshot: &str,
    as_of_revision: Option<&str>,
) -> Result<DatasetPin> {
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    let pin = resolve_dataset_pin_at_in(&tx, session, resource, snapshot, as_of_revision).await?;
    tx.commit().await?;
    Ok(pin)
}
pub(crate) async fn resolve_dataset_pin_at_in(
    tx: &tokio_postgres::Transaction<'_>,
    session: &VerifiedSession,
    resource: &str,
    snapshot: &str,
    as_of_revision: Option<&str>,
) -> Result<DatasetPin> {
    ensure!(
        snapshot
            .parse::<i64>()
            .is_ok_and(|value| value > 0 && value.to_string() == snapshot),
        "INPUT_PIN_CHANGED"
    );
    dataset_admission(tx, session).await?;
    let resource = dataset_resource(tx, session, resource).await?;
    // Establish current read/label authority before revealing whether a historical snapshot exists.
    tx.query_one(
        "SELECT moria.runtime_dataset_labels($1,$2)",
        &[&session.session_hash(), &resource.to_string()],
    )
    .await?;
    let current:i64=tx.query_one("SELECT revision FROM moria.dataset_heads WHERE environment=$1 AND dataset=$2 FOR SHARE",&[&session.environment(),&resource.to_string()]).await?.get(0);
    let cap = match as_of_revision {
        Some(value) => {
            let parsed = value.parse::<i64>()?;
            ensure!(
                parsed >= 0 && parsed <= current && parsed.to_string() == value,
                "INPUT_PIN_CHANGED"
            );
            parsed
        }
        None => current,
    };
    let row=tx.query_opt("SELECT h.revision::text AS revision,h.snapshot_id,h.metadata_location FROM moria.snapshot_history h JOIN moria.native_storage_namespaces n ON n.environment=h.environment AND n.resource=h.dataset AND n.purpose='dataset-source' AND n.enabled LEFT JOIN moria.native_dataset_bindings b ON b.environment=h.environment AND b.resource::text=h.dataset LEFT JOIN moria.runtime_dataset_roots r ON r.key=moria.runtime_dataset_root_key(h.metadata_location) LEFT JOIN moria.runtime_dataset_expired_roots e ON e.root=r.key WHERE h.environment=$1 AND h.dataset=$2 AND h.snapshot_id=$3 AND h.revision<=$4 AND e.root IS NULL AND ((b.resource IS NOT NULL AND b.enabled AND r.sealed AND r.environment=h.environment AND r.dataset=h.dataset AND EXISTS(SELECT 1 FROM moria.runtime_dataset_root_locations l WHERE l.root=r.key AND l.sealed AND l.incarnation=(SELECT incarnation FROM moria.indexing_instance) AND l.physical_fingerprint=b.physical_binding_fingerprint)) OR (b.resource IS NULL AND EXISTS(SELECT 1 FROM moria.runtime_snapshot_candidates c WHERE c.environment=h.environment AND c.resource=h.dataset AND c.metadata_location=h.metadata_location AND c.snapshot_id=h.snapshot_id AND c.proof->>'readerProfile'='iceberg-v2-cow-flat-bounded-v2'))) ORDER BY h.revision DESC LIMIT 1 FOR SHARE OF h",&[&session.environment(),&resource.to_string(),&snapshot,&cap]).await?;
    let Some(row) = row else {
        let expired:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM moria.snapshot_history h JOIN moria.runtime_dataset_expired_roots e ON e.root=moria.runtime_dataset_root_key(h.metadata_location) WHERE h.environment=$1 AND h.dataset=$2 AND h.snapshot_id=$3 AND h.revision<=$4)",&[&session.environment(),&resource.to_string(),&snapshot,&cap]).await?.get(0);
        if expired {
            anyhow::bail!("DATASET_SNAPSHOT_EXPIRED");
        }
        anyhow::bail!("DATASET_SNAPSHOT_NOT_FOUND");
    };
    let pin = dataset_pin(tx, session, resource, &row).await?;
    tx.query_one(
        "SELECT moria.runtime_dataset_root_assert($1)",
        &[&pin.metadata_location],
    )
    .await?;
    Ok(pin)
}
pub(crate) async fn resolve_dataset_pin_in(
    tx: &tokio_postgres::Transaction<'_>,
    session: &VerifiedSession,
    resource: &str,
) -> Result<DatasetPin> {
    dataset_admission(tx, session).await?;
    let resource = dataset_resource(tx, session, resource).await?;
    let row=tx.query_one("SELECT h.revision::text AS revision,h.snapshot_id,h.metadata_location FROM moria.dataset_heads h JOIN moria.snapshot_history s ON s.environment=h.environment AND s.dataset=h.dataset AND s.revision=h.revision AND s.metadata_location=h.metadata_location AND s.snapshot_id=h.snapshot_id WHERE h.environment=$1 AND h.dataset=$2 AND h.metadata_location IS NOT NULL FOR SHARE OF h,s",&[&session.environment(),&resource.to_string()]).await?;
    let pin = dataset_pin(tx, session, resource, &row).await?;
    Ok(pin)
}

/// The caller retains environment/filesystem shared admission through binding acceptance.
/// Resolves metadata under a registered service without creating a data session.
pub(crate) async fn resolve_dataset_pin_registered_in(
    client: &tokio_postgres::Client,
    service: &crate::jobs::RegisteredService,
    resource: &str,
) -> Result<DatasetPin> {
    let resource: String = client
        .query_one(
            "SELECT moria.runtime_data_resource($1,$2)",
            &[&service.environment(), &resource],
        )
        .await?
        .get(0);
    let resource: Uuid = resource
        .parse()
        .map_err(|_| anyhow::anyhow!("UNSUPPORTED_DATASET_BINDING"))?;
    let labels = service
        .resource_labels_in(client, &resource.to_string(), "read")
        .await?;
    client
        .query_one(
            "SELECT moria.runtime_dataset_kind($1,$2)",
            &[&service.environment(), &resource.to_string()],
        )
        .await?;
    let row=client.query_one("SELECT h.revision::text AS revision,h.snapshot_id,h.metadata_location FROM moria.dataset_heads h JOIN moria.snapshot_history s ON s.environment=h.environment AND s.dataset=h.dataset AND s.revision=h.revision AND s.metadata_location=h.metadata_location AND s.snapshot_id=h.snapshot_id WHERE h.environment=$1 AND h.dataset=$2 AND h.metadata_location IS NOT NULL FOR SHARE OF h,s",&[&service.environment(),&resource.to_string()]).await?;
    dataset_pin_registered_row(client, service, resource, &row, labels).await
}
pub(crate) async fn dataset_pin_registered_row(
    client: &tokio_postgres::Client,
    service: &crate::jobs::RegisteredService,
    resource: Uuid,
    row: &tokio_postgres::Row,
    markings: Vec<String>,
) -> Result<DatasetPin> {
    let metadata_location: String = row.get("metadata_location");
    let reference = NativeSnapshotRef::decode(&metadata_location)?;
    let snapshot: String = row.get("snapshot_id");
    ensure!(
        snapshot.parse::<i64>()?.to_string() == snapshot,
        "INPUT_PIN_CHANGED"
    );
    let namespace=client.query_one("SELECT revision,iceberg_uuid FROM moria.native_storage_namespaces WHERE environment=$1 AND resource=$2 AND purpose='dataset-source' AND enabled AND warehouse_id=$3 AND table_id=$4",&[&service.environment(),&resource.to_string(),&reference.root.object.warehouse_id,&reference.root.object.table_id]).await?;
    let state = service.state_in(client).await?;
    client
        .query_one(
            "SELECT moria.runtime_dataset_binding($1,$2,$3,$4,$5)",
            &[
                &service.environment(),
                &resource.to_string(),
                &reference.root.object.warehouse_id,
                &reference.root.object.table_id,
                &namespace.get::<_, String>("revision"),
            ],
        )
        .await?;
    ensure!(
        markings
            .iter()
            .all(|label| state.identity.markings.contains(label)),
        "FORBIDDEN"
    );
    Ok(DatasetPin {
        resource,
        revision: row.get("revision"),
        snapshot,
        metadata_location,
        namespace: IndexNamespace {
            warehouse_id: reference.root.object.warehouse_id,
            table_id: reference.root.object.table_id,
            revision: namespace.get("revision"),
        },
        table_uuid: namespace
            .get::<_, Option<Uuid>>("iceberg_uuid")
            .ok_or_else(|| anyhow::anyhow!("INVALID_DATASET_BINDING"))?,
        markings,
        security_epoch: state.security_epoch,
        instance_epoch: state.instance_epoch,
    })
}

pub struct PinnedDatasetRead {
    pub pin: DatasetPin,
    pub snapshot: LoadedSnapshot,
    pub consumed_markings: Vec<String>,
}
/// Read only the exact Dataset declared by a live native task/coordinator, through trusted provider configuration.
pub async fn read_pinned_dataset(
    db: Database,
    session: VerifiedSession,
    lease: Lease,
    resource: &str,
    cancel: Cancellation,
) -> Result<PinnedDatasetRead> {
    cancel.check()?;
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    dataset_admission(&tx, &session).await?;
    let resource = dataset_resource(&tx, &session, resource).await?;
    let rows=tx.query("SELECT h.revision::text AS revision,h.snapshot_id,h.metadata_location FROM moria.runtime_executions e CROSS JOIN LATERAL jsonb_array_elements(e.spec->'pinnedInputs') p JOIN moria.snapshot_history h ON h.environment=e.environment AND h.dataset=p->>'resource' AND h.revision::text=p->>'revision' AND h.snapshot_id=p->>'snapshot' WHERE e.id=$1 AND e.environment=$2 AND p->>'resource'=$3 LIMIT 2",&[&lease.execution,&session.environment(),&resource.to_string()]).await?;
    ensure!(rows.len() == 1, "UNDECLARED_DATASET");
    let pin = dataset_pin(&tx, &session, resource, &rows[0]).await?;
    tx.commit().await?;
    drop(client);
    let grant = Arc::new(DatasetIoGrant::acquire(db, session, lease, pin.namespace.clone()).await?);
    let consumed_markings = grant.consumed_markings()?;
    let reader = crate::storage::configured_reader(&pin.namespace)?;
    grant.validate_store(reader.as_ref())?;
    let scratch_root = std::env::var("MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT")
        .map_err(|_| anyhow::anyhow!("SNAPSHOT_SCRATCH_UNAVAILABLE"))?;
    let scratch = PosixStore::provision(
        std::path::Path::new(&scratch_root),
        &pin.namespace.warehouse_id,
        &pin.namespace.table_id,
        crate::storage::StorageLimits {
            max_object_bytes: 128 * 1024 * 1024,
            chunk_bytes: 1024 * 1024,
        },
    )?;
    let limits: ClosureLimits = serde_json::from_str(
        &std::env::var("MORIA_NATIVE_CLOSURE_LIMITS")
            .map_err(|_| anyhow::anyhow!("SNAPSHOT_BUDGET_UNAVAILABLE"))?,
    )?;
    let reference = NativeSnapshotRef::decode(&pin.metadata_location)?;
    let snapshot = load_snapshot(
        &scratch,
        reader,
        grant.clone(),
        &reference,
        pin.table_uuid,
        pin.snapshot.parse()?,
        limits,
        cancel,
    )
    .await?;
    let table = snapshot.table().await?;
    grant.validate_schema(table.metadata().current_schema().as_ref())?;
    Ok(PinnedDatasetRead {
        pin,
        snapshot,
        consumed_markings,
    })
}
impl LoadedSnapshot {
    pub(crate) fn verified_version(&self, content: &SealedObject) -> Result<ObjectVersion> {
        RecordingReader::new(self.reader.clone(), self.spool.clone())?.verified(content)
    }
    pub(crate) fn spool(&self) -> &ClosureSpool {
        &self.spool
    }
    pub fn proof(&self) -> &SnapshotProof {
        &self.proof
    }
    /// Retains the private cache and rechecks the source authority throughout native scans.
    pub async fn table(&self) -> Result<iceberg::table::StaticTable> {
        crate::snapshot::verified_table(
            &self.spool,
            self.reader.clone(),
            self.authority.clone(),
            &self.proof,
        )
        .await
    }
}

/// Materialize only private bounded scratch under a read grant, then revalidate the entire immutable snapshot.
/// `scratch` must use the source logical namespace under a separately provisioned service-private root.
#[allow(clippy::too_many_arguments)]
pub async fn load_snapshot(
    scratch: &PosixStore,
    reader: Arc<dyn ObjectReader>,
    authority: Arc<dyn ReadAuthority>,
    reference: &NativeSnapshotRef,
    expected_table_uuid: Uuid,
    expected_snapshot_id: i64,
    limits: ClosureLimits,
    cancel: Cancellation,
) -> Result<LoadedSnapshot> {
    load_snapshot_inner(
        scratch,
        reader,
        authority,
        reference,
        expected_table_uuid,
        expected_snapshot_id,
        limits,
        cancel,
        None,
    )
    .await
}

/// Metadata-only reuse is available only through an exact retained-root version authority.
pub(crate) async fn load_qualified_snapshot(
    scratch: &PosixStore,
    reader: Arc<dyn ObjectReader>,
    authority: Arc<dyn ReadAuthority>,
    reference: &NativeSnapshotRef,
    expected_table_uuid: Uuid,
    expected_snapshot_id: i64,
    limits: ClosureLimits,
    cancel: Cancellation,
    known: &dyn VerifiedVersionAuthority,
) -> Result<LoadedSnapshot> {
    load_snapshot_inner(
        scratch,
        reader,
        authority,
        reference,
        expected_table_uuid,
        expected_snapshot_id,
        limits,
        cancel,
        Some(known),
    )
    .await
}

async fn load_snapshot_inner(
    scratch: &PosixStore,
    reader: Arc<dyn ObjectReader>,
    authority: Arc<dyn ReadAuthority>,
    reference: &NativeSnapshotRef,
    expected_table_uuid: Uuid,
    expected_snapshot_id: i64,
    limits: ClosureLimits,
    cancel: Cancellation,
    known: Option<&dyn VerifiedVersionAuthority>,
) -> Result<LoadedSnapshot> {
    reference.validate()?;
    let mut known = known;
    let root = if let Some(known_ref) = known {
        match read_qualified_descriptor::<SnapshotRoot>(
            reader.clone(),
            authority.clone(),
            &reference.root,
            16384,
            known_ref,
            cancel.clone(),
        )
        .await?
        {
            Some(root) => {
                validate_snapshot_root(reference, &root)?;
                root
            }
            None => {
                known = None;
                snapshot_root(
                    reader.as_ref(),
                    authority.clone(),
                    reference,
                    cancel.clone(),
                )
                .await?
            }
        }
    } else {
        snapshot_root(
            reader.as_ref(),
            authority.clone(),
            reference,
            cancel.clone(),
        )
        .await?
    };
    ensure!(
        root.proof.table_uuid == expected_table_uuid
            && root.proof.snapshot_id == expected_snapshot_id
            && root.proof.closure.entries <= limits.max_entries
            && root.proof.closure.referenced_bytes <= limits.max_referenced_bytes,
        "SNAPSHOT_PIN_MISMATCH"
    );
    let object = LogicalObject::new(
        &reference.root.object.warehouse_id,
        &reference.root.object.table_id,
        &Uuid::new_v4().to_string(),
    )?;
    let mut spool =
        ClosureSpool::create_read_cache(scratch, object, limits, authority.clone(), cancel.clone())
            .await?;
    let mut current = root.last_page;
    let mut first_of_later_page: Option<String> = None;
    for ordinal in (0..root.pages).rev() {
        let object = current
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("MISSING_SNAPSHOT_PAGE"))?
            .clone();
        let page = if let Some(known_ref) = known {
            match read_qualified_descriptor::<ClosurePage>(
                reader.clone(),
                authority.clone(),
                &object,
                PAGE_MAX,
                known_ref,
                cancel.clone(),
            )
            .await?
            {
                Some(page) => {
                    validate_snapshot_page(&object, ordinal, &page)?;
                    page
                }
                None => {
                    known = None;
                    snapshot_page(
                        reader.as_ref(),
                        authority.clone(),
                        &object,
                        ordinal,
                        cancel.clone(),
                    )
                    .await?
                }
            }
        } else {
            snapshot_page(
                reader.as_ref(),
                authority.clone(),
                &object,
                ordinal,
                cancel.clone(),
            )
            .await?
        };
        ensure!(
            page.entries
                .windows(2)
                .all(|items| items[0].name < items[1].name)
                && first_of_later_page.as_ref().is_none_or(|first| page
                    .entries
                    .last()
                    .unwrap()
                    .name
                    < *first)
                && (ordinal == root.pages - 1 || page.entries.len() == 256),
            "INVALID_SNAPSHOT_ORDER"
        );
        first_of_later_page = Some(page.entries[0].name.clone());
        current = page.previous;
        spool.append_page(&page.entries).await?;
    }
    ensure!(current.is_none(), "EXTRA_SNAPSHOT_PAGE");
    let mut qualified = false;
    let closure = if let Some(known) = known {
        match spool
            .seal_verified(reader.as_ref(), authority.clone(), known)
            .await
        {
            Ok(closure) => {
                qualified = true;
                closure
            }
            Err(error) if unsupported_immutable_version(&error) => {
                spool.seal(reader.as_ref(), authority.clone()).await?
            }
            Err(error) => return Err(error),
        }
    } else {
        spool.seal(reader.as_ref(), authority.clone()).await?
    };
    ensure!(
        closure.entries == root.proof.closure.entries
            && closure.referenced_bytes == root.proof.closure.referenced_bytes
            && closure.sha256 == root.proof.closure.sha256,
        "CLOSURE_MISMATCH"
    );
    let pin = SnapshotPin {
        metadata: root.proof.metadata,
        table_uuid: expected_table_uuid,
        snapshot_id: expected_snapshot_id,
    };
    let proof = if qualified {
        verify_retained_subset(&spool, reader.clone(), authority.clone(), pin).await?
    } else {
        verify_snapshot(&spool, reader.clone(), authority.clone(), pin).await?
    };
    ensure!(
        proof.data_files == root.proof.data_files
            && proof.physical_rows == root.proof.physical_rows
            && proof.scanned_snapshots == root.proof.scanned_snapshots,
        "SNAPSHOT_PROOF_MISMATCH"
    );
    Ok(LoadedSnapshot {
        spool,
        proof,
        reader,
        authority,
    })
}
