//! Private operator recovery custody. Retained catalog references define exact byte membership.
use crate::{
    canonical::canonical_json,
    catalog::{NativeSnapshotRef, snapshot_page, snapshot_root},
    database::Database,
    recovery::{RETENTION_ADMISSION, RecoveryLease},
    storage::{
        self, Cancellation, ClosureEntry, IndexNamespace, LogicalObject, ObjectReader, ObjectStore,
        ObjectVersion, ReadAuthority, SealedObject, WriteAuthority,
    },
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{fmt, sync::Arc};
use tokio_postgres::Client;
use uuid::Uuid;

const PAGE_BYTES: usize = 256 * 1024;
const PAGE_ENTRIES: usize = 128;
const SAFE_INTEGER: u64 = 9_007_199_254_740_991;
fn capture_phase<T>(result: Result<T>, phase: &'static str) -> Result<T> {
    result.map_err(|error|{
        let code=crate::database::safe_error(&error);
        if !matches!(code,"INDEXING_FAILED"|"INDEXING_DATABASE_ERROR"){return error;}
        let sqlstate=error.chain().find_map(|cause|cause.downcast_ref::<tokio_postgres::Error>().and_then(|e|e.as_db_error()).map(|e|e.code().code().to_owned()));
        let category=if sqlstate.is_some(){"postgres"}else if error.chain().any(|e|e.is::<serde_json::Error>()){"json"}else if error.chain().any(|e|e.is::<std::io::Error>()){"filesystem"}else{"other"};
        eprintln!("{}",serde_json::json!({"event":"recovery_phase_failure","phase":phase,"category":category,"sqlstate":sqlstate}));
        anyhow::anyhow!(phase)
    })
}

mod graph;
mod offline;
pub(crate) use offline::{OfflineRecovery, RecoveredContentProof};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RecoveryContentInventory {
    pub version: u32,
    pub root: SealedObject,
    pub entries: u64,
    pub referenced_bytes: u64,
    pub closure_sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InventoryMember {
    source: SealedObject,
    source_namespace: IndexNamespace,
    source_kind: String,
    physical_fingerprint: String,
    source_version: ObjectVersion,
    backup: SealedObject,
    backup_version: ObjectVersion,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InventoryPage {
    version: u32,
    ordinal: u64,
    previous: Option<SealedObject>,
    members: Vec<InventoryMember>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CapturedNamespace {
    namespace: IndexNamespace,
    kind: String,
    physical_fingerprint: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct NamespacePage {
    version: u32,
    ordinal: u32,
    previous: Option<SealedObject>,
    namespaces: Vec<CapturedNamespace>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InventoryRoot {
    version: u32,
    recovery_set: Uuid,
    entries: u64,
    referenced_bytes: u64,
    pages: u64,
    last_page: Option<SealedObject>,
    closure_sha256: String,
    rebuild_required: Vec<String>,
    namespaces: u32,
    namespace_pages: u32,
    last_namespace_page: Option<SealedObject>,
    namespace_sha256: String,
}
fn rebuild_dispositions() -> Vec<String> {
    vec![
        "external-index-artifacts".into(),
        "external-execution-images".into(),
        "external-model-engines".into(),
        "external-app-host-artifacts".into(),
        "fenced-private-partsets".into(),
    ]
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RecoveryContentLimits {
    pub roots: u64,
    pub members: u64,
    pub bytes: u64,
    pub spill_bytes: u64,
    pub dump_bytes: u64,
    pub time_seconds: u64,
}
impl RecoveryContentLimits {
    pub fn configured() -> Result<Self> {
        let text = std::env::var("MORIA_NATIVE_RECOVERY_LIMITS")
            .map_err(|_| anyhow::anyhow!("RECOVERY_LIMITS_REQUIRED"))?;
        ensure!(text.len() <= 4096, "RECOVERY_INVALID");
        let value: Self = serde_json::from_str(&text)?;
        ensure!(
            [
                value.roots,
                value.members,
                value.bytes,
                value.spill_bytes,
                value.dump_bytes
            ]
            .iter()
            .all(|n| *n > 0 && *n <= SAFE_INTEGER)
                && value.spill_bytes >= 65536
                && (1..=86400).contains(&value.time_seconds),
            "RECOVERY_INVALID"
        );
        Ok(value)
    }
}
fn validate_content(content: &SealedObject) -> Result<()> {
    ensure!(
        content.object.version == 1
            && LogicalObject::new(
                &content.object.warehouse_id,
                &content.object.table_id,
                &content.object.object_id
            )? == content.object
            && content.size <= SAFE_INTEGER
            && content.sha256.len() == 71
            && content.sha256.starts_with("sha256:")
            && content.sha256[7..]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "RECOVERY_CONTENT_INVALID"
    );
    Ok(())
}
fn inventory_hash() -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(b"moria-recovery-inventory-v1\0");
    hash
}
fn namespace_hash() -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(b"moria-recovery-namespaces-v1\0");
    hash
}
fn hash_namespace(hash: &mut Sha256, namespace: &CapturedNamespace) -> Result<()> {
    let bytes = canonical_json(&serde_json::to_value(namespace)?)?;
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes.as_bytes());
    Ok(())
}
fn hash_member(hash: &mut Sha256, member: &InventoryMember) -> Result<()> {
    let bytes = canonical_json(&serde_json::to_value(member)?)?;
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes.as_bytes());
    Ok(())
}
async fn charge_state(client: &Client, set: Uuid, bytes: usize) -> Result<()> {
    let bytes = i64::try_from(
        bytes
            .checked_add(1024)
            .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_LIMIT"))?,
    )?;
    ensure!(client.execute("UPDATE moria.runtime_recovery_content_counters SET state_bytes=state_bytes+$2 WHERE recovery_set=$1 AND state_bytes+$2<=max_state_bytes",&[&set,&bytes]).await?==1,"RECOVERY_CONTENT_LIMIT");
    Ok(())
}

#[derive(Clone)]
struct SourceAuthority {
    db: Database,
    lease: RecoveryLease,
}
impl fmt::Debug for SourceAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoverySourceAuthority")
            .field("set", &self.lease.set_id())
            .finish()
    }
}
#[async_trait]
impl ReadAuthority for SourceAuthority {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        let token = self.lease.token();
        let fence: i64 = token.fence.parse()?;
        let client = self.db.get().await?;
        let content: Value = client
            .query_one(
                "SELECT moria.runtime_recovery_member_check($1,$2,$3,$4,$5,$6,$7)",
                &[
                    &token.set,
                    &token.owner,
                    &fence,
                    &token.incarnation,
                    &object.warehouse_id,
                    &object.table_id,
                    &object.object_id,
                ],
            )
            .await?
            .get(0);
        let content: SealedObject = serde_json::from_value(content)?;
        ensure!(&content.object == object, "RECOVERY_OBJECT_DENIED");
        Ok(())
    }
}
#[derive(Clone)]
pub struct BackupAuthority {
    db: Database,
    lease: RecoveryLease,
    namespace: IndexNamespace,
    physical: String,
}
impl fmt::Debug for BackupAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveryBackupAuthority")
            .field("set", &self.lease.set_id())
            .finish()
    }
}
fn backup_object(namespace: &IndexNamespace, scope: &str, suffix: &str) -> Result<LogicalObject> {
    ensure!(
        !suffix.is_empty()
            && suffix.len() <= 64
            && suffix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
        "RECOVERY_INVALID"
    );
    LogicalObject::new(
        &namespace.warehouse_id,
        &namespace.table_id,
        &format!("{scope}_{suffix}"),
    )
}
impl BackupAuthority {
    pub async fn acquire(
        db: Database,
        lease: RecoveryLease,
        namespace: IndexNamespace,
        store: &dyn ObjectReader,
    ) -> Result<Self> {
        lease.check().await?;
        ensure!(
            !(namespace.warehouse_id == "filesystem" && namespace.table_id == "content"),
            "RECOVERY_NAMESPACE_INVALID"
        );
        let physical = store.physical_fingerprint()?;
        let token = lease.token();
        db.get()
            .await?
            .query_one(
                "SELECT moria.runtime_recovery_backup_bind($1,$2,$3,$4,$5,$6)",
                &[
                    &token.set,
                    &token.owner,
                    &token.fence.parse::<i64>()?,
                    &token.incarnation,
                    &serde_json::to_value(&namespace)?,
                    &physical,
                ],
            )
            .await?;
        Ok(Self {
            db,
            lease,
            namespace,
            physical,
        })
    }
    pub fn namespace(&self) -> &IndexNamespace {
        &self.namespace
    }
    pub fn object(&self, suffix: &str) -> Result<LogicalObject> {
        backup_object(&self.namespace, &self.lease.scope_key(), suffix)
    }
    async fn check(&self, object: &LogicalObject, write: bool) -> Result<()> {
        ensure!(
            object.warehouse_id == self.namespace.warehouse_id
                && object.table_id == self.namespace.table_id,
            "RECOVERY_OBJECT_DENIED"
        );
        LogicalObject::new(&object.warehouse_id, &object.table_id, &object.object_id)?;
        let token = self.lease.token();
        self.db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_recovery_backup_object($1,$2,$3,$4,$5,$6,$7,$8)",
                &[
                    &token.set,
                    &token.owner,
                    &token.fence.parse::<i64>()?,
                    &token.incarnation,
                    &serde_json::to_value(&self.namespace)?,
                    &self.physical,
                    &serde_json::to_value(object)?,
                    &write,
                ],
            )
            .await?;

        Ok(())
    }
}
#[async_trait]
impl ReadAuthority for BackupAuthority {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        self.check(object, false).await
    }
}
#[async_trait]
impl WriteAuthority for BackupAuthority {
    fn scope_key(&self) -> String {
        self.lease.scope_key()
    }
    async fn check_write(&self, object: &LogicalObject) -> Result<()> {
        self.check(object, true).await
    }
}

struct Capture {
    db: Database,
    lease: RecoveryLease,
    source: Arc<SourceAuthority>,
    backup: Arc<dyn ObjectStore>,
    backup_authority: Arc<BackupAuthority>,
    limits: RecoveryContentLimits,
    cancel: Cancellation,
}
impl Capture {
    async fn namespace(
        &self,
        snapshot: &Client,
        object: &LogicalObject,
        kind: &str,
    ) -> Result<IndexNamespace> {
        self.lease.check().await?;
        let namespace = storage::configured_namespace(&object.warehouse_id, &object.table_id)?;
        let state: Value = snapshot
            .query_one(
                "SELECT moria.runtime_recovery_namespace_state($1,$2,$3)",
                &[&object.warehouse_id, &object.table_id, &kind],
            )
            .await?
            .get(0);
        if kind == "dataset" {
            ensure!(
                state["revision"].as_str() == Some(namespace.revision.as_str()),
                "RECOVERY_NAMESPACE_CHANGED"
            );
        }
        let reader = storage::configured_reader(&namespace)?;
        let physical = reader.physical_fingerprint()?;
        let expected = if kind == "dataset" {
            state["managed"]["physicalBindingFingerprint"].as_str()
        } else {
            state["physicalBindingFingerprint"].as_str()
        };
        if let Some(expected) = expected {
            ensure!(expected == physical, "RECOVERY_NAMESPACE_CHANGED");
        }
        let mut writer = self.db.get().await?;
        let tx = writer.transaction().await?;
        self.lease.check_in(tx.client()).await?;
        let current: Value = tx
            .query_one(
                "SELECT moria.runtime_recovery_namespace_state($1,$2,$3)",
                &[&object.warehouse_id, &object.table_id, &kind],
            )
            .await?
            .get(0);
        ensure!(state == current, "RECOVERY_NAMESPACE_CHANGED");
        if tx.execute("INSERT INTO moria.runtime_recovery_namespaces VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING",&[&self.lease.set_id(),&object.warehouse_id,&object.table_id,&serde_json::to_value(&namespace)?,&kind,&state,&physical]).await?==1{charge_state(tx.client(),self.lease.set_id(),serde_json::to_vec(&state)?.len()+serde_json::to_vec(&namespace)?.len()).await?;}
        let row=tx.query_one("SELECT namespace,kind,captured_state,physical_fingerprint FROM moria.runtime_recovery_namespaces WHERE recovery_set=$1 AND warehouse_id=$2 AND table_id=$3",&[&self.lease.set_id(),&object.warehouse_id,&object.table_id]).await?;
        ensure!(
            row.get::<_, Value>(0) == serde_json::to_value(&namespace)?
                && row.get::<_, String>(1) == kind
                && row.get::<_, Value>(2) == state
                && row.get::<_, String>(3) == physical,
            "RECOVERY_NAMESPACE_CHANGED"
        );
        tx.commit().await?;
        Ok(namespace)
    }
    async fn member(&self, content: &SealedObject) -> Result<()> {
        validate_content(content)?;
        self.cancel.check()?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        self.lease.check_in(tx.client()).await?;
        let inserted=tx.execute("INSERT INTO moria.runtime_recovery_members(recovery_set,warehouse_id,table_id,object_id,content) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",&[&self.lease.set_id(),&content.object.warehouse_id,&content.object.table_id,&content.object.object_id,&serde_json::to_value(content)?]).await?;
        if inserted == 1 {
            ensure!(tx.execute("UPDATE moria.runtime_recovery_content_counters SET members=members+1,bytes=bytes+$2 WHERE recovery_set=$1 AND members<max_members AND bytes+$2<=max_bytes",&[&self.lease.set_id(),&(content.size as i64)]).await?==1,"RECOVERY_CONTENT_LIMIT");
            charge_state(
                tx.client(),
                self.lease.set_id(),
                serde_json::to_vec(content)?.len(),
            )
            .await?;
        }
        let old:Value=tx.query_one("SELECT content FROM moria.runtime_recovery_members WHERE recovery_set=$1 AND warehouse_id=$2 AND table_id=$3 AND object_id=$4",&[&self.lease.set_id(),&content.object.warehouse_id,&content.object.table_id,&content.object.object_id]).await?.get(0);
        ensure!(
            old == serde_json::to_value(content)?,
            "RECOVERY_CONTENT_CONFLICT"
        );
        tx.commit().await?;
        Ok(())
    }
    async fn root(&self, snapshot: &Client, kind: &str, descriptor: Value) -> Result<()> {
        self.cancel.check()?;
        self.lease.check().await?;
        let encoded = canonical_json(&descriptor)?;
        ensure!(encoded.len() <= 16384, "RECOVERY_CONTENT_INVALID");
        let hash = format!("{:x}", Sha256::digest(encoded.as_bytes()));
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        self.lease.check_in(tx.client()).await?;
        let inserted=tx.execute("INSERT INTO moria.runtime_recovery_roots(recovery_set,kind,root_hash,descriptor) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",&[&self.lease.set_id(),&kind,&hash,&descriptor]).await?;
        if inserted == 1 {
            ensure!(tx.execute("UPDATE moria.runtime_recovery_content_counters SET roots=roots+1 WHERE recovery_set=$1 AND roots<max_roots",&[&self.lease.set_id()]).await?==1,"RECOVERY_CONTENT_LIMIT");
            charge_state(tx.client(), self.lease.set_id(), encoded.len()).await?;
        }
        let row=tx.query_one("SELECT id,descriptor,complete FROM moria.runtime_recovery_roots WHERE recovery_set=$1 AND root_hash=$2",&[&self.lease.set_id(),&hash]).await?;
        ensure!(
            row.get::<_, Value>(1) == descriptor,
            "RECOVERY_CONTENT_CONFLICT"
        );
        let root_id: Uuid = row.get(0);
        let complete: bool = row.get(2);
        tx.commit().await?;
        drop(client);
        if complete {
            return Ok(());
        }
        if kind == "filesystem" {
            let content: SealedObject = serde_json::from_value(descriptor)?;
            self.namespace(snapshot, &content.object, kind).await?;
            self.member(&content).await?;
        } else {
            let reference = NativeSnapshotRef::decode(
                descriptor
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_INVALID"))?,
            )?;
            let namespace = self
                .namespace(snapshot, &reference.root.object, kind)
                .await?;
            self.member(&reference.root).await?;
            let reader = storage::configured_reader(&namespace)?;
            if reference.version == 3 {
                self.graph(snapshot, descriptor.as_str().unwrap(), reader.as_ref())
                    .await?;
            } else {
                let root = snapshot_root(
                    reader.as_ref(),
                    self.source.clone(),
                    &reference,
                    self.cancel.clone(),
                )
                .await?;
                self.member(&root.proof.metadata).await?;
                self.graph(snapshot, descriptor.as_str().unwrap(), reader.as_ref())
                    .await?;
                ensure!(
                    root.proof.closure.entries <= self.limits.members
                        && root.proof.closure.referenced_bytes <= self.limits.bytes,
                    "RECOVERY_CONTENT_LIMIT"
                );
                let mut page = root.last_page;
                let mut count = 0u64;
                for ordinal in (0..root.pages).rev() {
                    let object = page.ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_INVALID"))?;
                    self.member(&object).await?;
                    let value = snapshot_page(
                        reader.as_ref(),
                        self.source.clone(),
                        &object,
                        ordinal,
                        self.cancel.clone(),
                    )
                    .await?;
                    for entry in &value.entries {
                        let prefix =
                            format!("moria://{}/{}/", namespace.warehouse_id, namespace.table_id);
                        ensure!(
                            entry.name.starts_with(&prefix) && entry.name.len() <= 512,
                            "RECOVERY_CONTENT_INVALID"
                        );
                        self.member(&entry.content).await?;
                        let mut client = self.db.get().await?;
                        let tx = client.transaction().await?;
                        self.lease.check_in(tx.client()).await?;
                        if tx.execute("INSERT INTO moria.runtime_recovery_closure_members VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",&[&self.lease.set_id(),&root_id,&entry.name,&entry.content.object.warehouse_id,&entry.content.object.table_id,&entry.content.object.object_id]).await?==1{charge_state(tx.client(),self.lease.set_id(),serde_json::to_vec(entry)?.len()).await?;}
                        ensure!(tx.query_one("SELECT warehouse_id=$4 AND table_id=$5 AND object_id=$6 FROM moria.runtime_recovery_closure_members WHERE recovery_set=$1 AND root_id=$2 AND name=$3",&[&self.lease.set_id(),&root_id,&entry.name,&entry.content.object.warehouse_id,&entry.content.object.table_id,&entry.content.object.object_id]).await?.get::<_,bool>(0),"RECOVERY_CONTENT_CONFLICT");
                        tx.commit().await?;
                        count = count
                            .checked_add(1)
                            .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_LIMIT"))?;
                    }
                    page = value.previous;
                }
                ensure!(
                    page.is_none() && count == root.proof.closure.entries,
                    "RECOVERY_CONTENT_INVALID"
                );
                let mut digest = storage::closure_digest_start();
                let mut after: Option<String> = None;
                let mut bytes = 0u64;
                let mut observed = 0u64;
                loop {
                    self.lease.check().await?;
                    let rows=self.db.get().await?.query("SELECT c.name,m.content FROM moria.runtime_recovery_closure_members c JOIN moria.runtime_recovery_members m ON m.recovery_set=c.recovery_set AND m.warehouse_id=c.warehouse_id AND m.table_id=c.table_id AND m.object_id=c.object_id WHERE c.recovery_set=$1 AND c.root_id=$2 AND ($3::text IS NULL OR c.name>$3 COLLATE \"C\") ORDER BY c.name LIMIT 128",&[&self.lease.set_id(),&root_id,&after]).await?;
                    if rows.is_empty() {
                        break;
                    }
                    for row in &rows {
                        let entry = ClosureEntry {
                            name: row.get(0),
                            content: serde_json::from_value(row.get(1))?,
                        };
                        storage::closure_digest_entry(&mut digest, &entry)?;
                        bytes = bytes
                            .checked_add(entry.content.size)
                            .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_LIMIT"))?;
                        observed += 1;
                    }
                    after = Some(rows.last().unwrap().get(0));
                }
                ensure!(
                    observed == root.proof.closure.entries
                        && bytes == root.proof.closure.referenced_bytes
                        && format!("sha256:{:x}", digest.finalize()) == root.proof.closure.sha256,
                    "RECOVERY_CONTENT_INVALID"
                );
            }
        }
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        self.lease.check_in(tx.client()).await?;
        tx.execute(
            "UPDATE moria.runtime_recovery_roots SET complete=true WHERE recovery_set=$1 AND id=$2",
            &[&self.lease.set_id(), &root_id],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn enumerate(&self, snapshot: &Client) -> Result<()> {
        let namespaces=snapshot.query("SELECT warehouse_id,table_id,'dataset' AS kind FROM moria.native_storage_namespaces WHERE purpose='dataset-source' UNION ALL SELECT warehouse_id,table_id,'filesystem' AS kind FROM moria.fs_storage_bindings ORDER BY warehouse_id,table_id LIMIT 257",&[]).await?;
        ensure!(namespaces.len() <= 256, "RECOVERY_NAMESPACE_LIMIT");
        for namespace in namespaces {
            self.namespace(
                snapshot,
                &LogicalObject::new(
                    namespace.get("warehouse_id"),
                    namespace.get("table_id"),
                    "namespace_probe",
                )?,
                namespace.get("kind"),
            )
            .await?;
        }
        let mut after: Option<(String, String, i64)> = None;
        let mut roots = 0u64;
        loop {
            self.lease.check().await?;
            let rows=snapshot.query("SELECT h.environment,h.dataset,h.revision,h.metadata_location FROM moria.snapshot_history h WHERE ($1::text IS NULL OR (h.environment,h.dataset,h.revision)>($1,$2,$3)) AND h.metadata_location IS NOT NULL AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_expired_roots e WHERE e.root=moria.runtime_dataset_root_key(h.metadata_location)) ORDER BY h.environment,h.dataset,h.revision LIMIT 64",&[&after.as_ref().map(|v|&v.0),&after.as_ref().map(|v|&v.1),&after.as_ref().map(|v|v.2)]).await?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                self.root(snapshot, "dataset", Value::String(row.get(3)))
                    .await?;
                roots += 1;
                ensure!(roots <= self.limits.roots, "RECOVERY_CONTENT_LIMIT");
            }
            let row = rows.last().unwrap();
            after = Some((row.get(0), row.get(1), row.get(2)));
        }
        let mut after: Option<Uuid> = None;
        loop {
            self.lease.check().await?;
            let rows=snapshot.query("SELECT c.id,c.metadata_location FROM moria.runtime_snapshot_candidates c WHERE ($1::uuid IS NULL OR c.id>$1) AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_expired_roots e WHERE e.root=moria.runtime_dataset_root_key(c.metadata_location)) ORDER BY c.id LIMIT 64",&[&after]).await?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                self.root(snapshot, "dataset", Value::String(row.get(1)))
                    .await?;
                roots += 1;
                ensure!(roots <= self.limits.roots, "RECOVERY_CONTENT_LIMIT");
            }
            after = Some(rows.last().unwrap().get(0));
        }
        let mut after: Option<(Uuid, i64)> = None;
        loop {
            self.lease.check().await?;
            let rows=snapshot.query("SELECT entry_id,revision,snapshot->'content' AS content FROM moria.fs_entry_versions WHERE ($1::uuid IS NULL OR (entry_id,revision)>($1,$2)) AND snapshot->'content' IS NOT NULL AND snapshot->'content'<>'null'::jsonb ORDER BY entry_id,revision LIMIT 64",&[&after.as_ref().map(|v|v.0),&after.as_ref().map(|v|v.1)]).await?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let content: crate::filesystem::FileContent = serde_json::from_value(row.get(2))?;
                self.root(
                    snapshot,
                    "filesystem",
                    serde_json::to_value(SealedObject {
                        object: content.location,
                        size: content.size,
                        sha256: content.digest,
                    })?,
                )
                .await?;
                roots += 1;
                ensure!(roots <= self.limits.roots, "RECOVERY_CONTENT_LIMIT");
            }
            let row = rows.last().unwrap();
            after = Some((row.get(0), row.get(1)));
        }
        let row=self.db.get().await?.query_one("SELECT count(*)::bigint,COALESCE(sum((content->>'size')::bigint),0)::bigint FROM moria.runtime_recovery_members WHERE recovery_set=$1",&[&self.lease.set_id()]).await?;
        ensure!(
            row.get::<_, i64>(0) as u64 <= self.limits.members
                && row.get::<_, i64>(1) as u64 <= self.limits.bytes,
            "RECOVERY_CONTENT_LIMIT"
        );
        Ok(())
    }
    async fn copy_members(&self) -> Result<()> {
        let mut after: Option<(String, String, String)> = None;
        loop {
            self.cancel.check()?;
            self.lease.check().await?;
            let rows=self.db.get().await?.query("SELECT m.*,n.namespace,n.kind,n.physical_fingerprint FROM moria.runtime_recovery_members m JOIN moria.runtime_recovery_namespaces n USING(recovery_set,warehouse_id,table_id) WHERE m.recovery_set=$1 AND ($2::text IS NULL OR (m.warehouse_id,m.table_id,m.object_id)>($2,$3,$4)) ORDER BY m.warehouse_id,m.table_id,m.object_id LIMIT 64",&[&self.lease.set_id(),&after.as_ref().map(|v|&v.0),&after.as_ref().map(|v|&v.1),&after.as_ref().map(|v|&v.2)]).await?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let source: SealedObject = serde_json::from_value(row.get("content"))?;
                let namespace: IndexNamespace = serde_json::from_value(row.get("namespace"))?;
                let reader = storage::configured_reader(&namespace)?;
                ensure!(
                    reader.physical_fingerprint()? == row.get::<_, String>("physical_fingerprint"),
                    "RECOVERY_NAMESPACE_CHANGED"
                );
                let admitted: Option<Value> = row.get("source_version");
                let version = if let Some(admitted) = admitted {
                    serde_json::from_value::<storage::ObjectVersion>(admitted)?
                } else {
                    reader
                        .version(&source.object, self.source.as_ref(), &self.cancel)
                        .await?
                };
                ensure!(version.size == source.size, "CONTENT_MISMATCH");
                {
                    let mut client = self.db.get().await?;
                    let tx = client.transaction().await?;
                    self.lease.check_in(tx.client()).await?;
                    let version_value = serde_json::to_value(&version)?;
                    if tx.execute("UPDATE moria.runtime_recovery_members SET source_version=$5 WHERE recovery_set=$1 AND warehouse_id=$2 AND table_id=$3 AND object_id=$4 AND source_version IS NULL",&[&self.lease.set_id(),&source.object.warehouse_id,&source.object.table_id,&source.object.object_id,&version_value]).await?==1{charge_state(tx.client(),self.lease.set_id(),serde_json::to_vec(&version_value)?.len()).await?;}
                    ensure!(tx.query_one("SELECT source_version=$5 FROM moria.runtime_recovery_members WHERE recovery_set=$1 AND warehouse_id=$2 AND table_id=$3 AND object_id=$4",&[&self.lease.set_id(),&source.object.warehouse_id,&source.object.table_id,&source.object.object_id,&version_value]).await?.get::<_,bool>(0),"STORAGE_VERSION_CHANGED");
                    tx.commit().await?;
                }
                let output = self
                    .backup_authority
                    .object(&format!("data_{}", row.get::<_, Uuid>("backup_object")))?;
                self.backup_authority.check_write(&output).await?;
                let existing = self
                    .backup
                    .recover_object(
                        output.clone(),
                        source.size,
                        source.sha256.clone(),
                        self.backup_authority.clone(),
                        self.backup_authority.clone(),
                        self.cancel.clone(),
                    )
                    .await?;
                let mut upload = if existing.is_none() {
                    Some(
                        self.backup
                            .begin_object(
                                output,
                                source.size,
                                source.sha256.clone(),
                                self.backup_authority.clone(),
                                self.cancel.clone(),
                            )
                            .await?,
                    )
                } else {
                    None
                };
                let mut stream = storage::versioned_stream(
                    reader.clone(),
                    source.object.clone(),
                    version.clone(),
                    self.source.clone(),
                    self.cancel.clone(),
                )
                .await?;
                let mut hash = Sha256::new();
                let mut bytes = 0u64;
                while let Some(frame) = stream.next().await {
                    let frame = frame?;
                    bytes = bytes
                        .checked_add(frame.len() as u64)
                        .filter(|n| *n <= source.size)
                        .ok_or_else(|| anyhow::anyhow!("CONTENT_MISMATCH"))?;
                    hash.update(&frame);
                    if let Some(upload) = &mut upload {
                        upload.append(&frame).await?;
                    }
                }
                ensure!(
                    bytes == source.size
                        && format!("sha256:{:x}", hash.finalize()) == source.sha256,
                    "CONTENT_MISMATCH"
                );
                reader
                    .range_at_version(
                        &source.object,
                        0..source.size.min(1),
                        &version,
                        self.source.as_ref(),
                        &self.cancel,
                    )
                    .await?;
                let copied = match upload {
                    Some(upload) => upload.seal().await?,
                    None => existing.unwrap(),
                };
                let backup_version = self
                    .backup
                    .version(&copied.object, self.backup_authority.as_ref(), &self.cancel)
                    .await?;
                ensure!(backup_version.size == source.size, "CONTENT_MISMATCH");
                let mut client = self.db.get().await?;
                let tx = client.transaction().await?;
                self.lease.check_in(tx.client()).await?;
                let copied = serde_json::to_value(copied)?;
                let backup_version = serde_json::to_value(backup_version)?;
                if tx.execute("UPDATE moria.runtime_recovery_members SET backup=$5,backup_version=$6 WHERE recovery_set=$1 AND warehouse_id=$2 AND table_id=$3 AND object_id=$4 AND backup IS NULL",&[&self.lease.set_id(),&source.object.warehouse_id,&source.object.table_id,&source.object.object_id,&copied,&backup_version]).await?==1{charge_state(tx.client(),self.lease.set_id(),serde_json::to_vec(&copied)?.len()+serde_json::to_vec(&backup_version)?.len()).await?;}
                ensure!(tx.query_one("SELECT backup=$5 AND backup_version=$6 FROM moria.runtime_recovery_members WHERE recovery_set=$1 AND warehouse_id=$2 AND table_id=$3 AND object_id=$4",&[&self.lease.set_id(),&source.object.warehouse_id,&source.object.table_id,&source.object.object_id,&copied,&backup_version]).await?.get::<_,bool>(0),"RECOVERY_CONTENT_CONFLICT");
                tx.commit().await?;
            }
            let row = rows.last().unwrap();
            after = Some((
                row.get("warehouse_id"),
                row.get("table_id"),
                row.get("object_id"),
            ));
        }
        Ok(())
    }
    async fn put<T: Serialize>(
        &self,
        suffix: &str,
        value: &T,
        maximum: usize,
    ) -> Result<SealedObject> {
        let bytes = canonical_json(&serde_json::to_value(value)?)?.into_bytes();
        ensure!(bytes.len() <= maximum, "RECOVERY_CONTENT_LIMIT");
        let object = self.backup_authority.object(suffix)?;
        self.backup_authority.check_write(&object).await?;
        let digest = format!("sha256:{:x}", Sha256::digest(&bytes));
        if let Some(existing) = self
            .backup
            .recover_object(
                object.clone(),
                bytes.len() as u64,
                digest.clone(),
                self.backup_authority.clone(),
                self.backup_authority.clone(),
                self.cancel.clone(),
            )
            .await?
        {
            return Ok(existing);
        }
        let mut write = self
            .backup
            .begin_object(
                object,
                bytes.len() as u64,
                digest,
                self.backup_authority.clone(),
                self.cancel.clone(),
            )
            .await?;
        write.append(&bytes).await?;
        write.seal().await
    }
    async fn inventory(&self) -> Result<RecoveryContentInventory> {
        let namespace_rows=self.db.get().await?.query("SELECT namespace,kind,physical_fingerprint FROM moria.runtime_recovery_namespaces WHERE recovery_set=$1 ORDER BY warehouse_id,table_id LIMIT 257",&[&self.lease.set_id()]).await?;
        ensure!(namespace_rows.len() <= 256, "RECOVERY_NAMESPACE_LIMIT");
        let mut namespace_hash = namespace_hash();
        let mut namespace_page = Vec::new();
        let mut last_namespace_page = None;
        let mut namespace_pages = 0u32;
        let namespaces = namespace_rows.len() as u32;
        for row in namespace_rows {
            let namespace = CapturedNamespace {
                namespace: serde_json::from_value(row.get("namespace"))?,
                kind: row.get("kind"),
                physical_fingerprint: row.get("physical_fingerprint"),
            };
            hash_namespace(&mut namespace_hash, &namespace)?;
            namespace_page.push(namespace);
            if namespace_page.len() == PAGE_ENTRIES {
                last_namespace_page = Some(
                    self.put(
                        &format!("namespace_page_{namespace_pages}"),
                        &NamespacePage {
                            version: 1,
                            ordinal: namespace_pages,
                            previous: last_namespace_page,
                            namespaces: std::mem::take(&mut namespace_page),
                        },
                        PAGE_BYTES,
                    )
                    .await?,
                );
                namespace_pages += 1;
            }
        }
        if !namespace_page.is_empty() {
            last_namespace_page = Some(
                self.put(
                    &format!("namespace_page_{namespace_pages}"),
                    &NamespacePage {
                        version: 1,
                        ordinal: namespace_pages,
                        previous: last_namespace_page,
                        namespaces: namespace_page,
                    },
                    PAGE_BYTES,
                )
                .await?,
            );
            namespace_pages += 1;
        }
        let mut after: Option<(String, String, String)> = None;
        let mut hash = inventory_hash();
        let mut count = 0u64;
        let mut bytes = 0u64;
        let mut page = Vec::new();
        let mut last_page = None;
        let mut ordinal = 0u64;
        loop {
            self.lease.check().await?;
            let rows=self.db.get().await?.query("SELECT m.*,n.namespace,n.kind,n.physical_fingerprint FROM moria.runtime_recovery_members m JOIN moria.runtime_recovery_namespaces n USING(recovery_set,warehouse_id,table_id) WHERE m.recovery_set=$1 AND ($2::text IS NULL OR (m.warehouse_id,m.table_id,m.object_id)>($2,$3,$4)) ORDER BY m.warehouse_id,m.table_id,m.object_id LIMIT 64",&[&self.lease.set_id(),&after.as_ref().map(|v|&v.0),&after.as_ref().map(|v|&v.1),&after.as_ref().map(|v|&v.2)]).await?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let member = InventoryMember {
                    source: serde_json::from_value(row.get("content"))?,
                    source_namespace: serde_json::from_value(row.get("namespace"))?,
                    source_kind: row.get("kind"),
                    physical_fingerprint: row.get("physical_fingerprint"),
                    source_version: serde_json::from_value(
                        row.get::<_, Option<Value>>("source_version")
                            .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_INCOMPLETE"))?,
                    )?,
                    backup: serde_json::from_value(
                        row.get::<_, Option<Value>>("backup")
                            .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_INCOMPLETE"))?,
                    )?,
                    backup_version: serde_json::from_value(
                        row.get::<_, Option<Value>>("backup_version")
                            .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_INCOMPLETE"))?,
                    )?,
                };
                validate_content(&member.source)?;
                validate_content(&member.backup)?;
                ensure!(
                    member.source.size == member.backup.size
                        && member.source.sha256 == member.backup.sha256,
                    "RECOVERY_CONTENT_CONFLICT"
                );
                let mut prospective = page.clone();
                prospective.push(member.clone());
                if !page.is_empty()
                    && (prospective.len() > PAGE_ENTRIES
                        || canonical_json(&serde_json::to_value(InventoryPage {
                            version: 1,
                            ordinal,
                            previous: last_page.clone(),
                            members: prospective,
                        })?)?
                        .len()
                            > PAGE_BYTES)
                {
                    last_page = Some(
                        self.put(
                            &format!("page_{ordinal}"),
                            &InventoryPage {
                                version: 1,
                                ordinal,
                                previous: last_page,
                                members: std::mem::take(&mut page),
                            },
                            PAGE_BYTES,
                        )
                        .await?,
                    );
                    ordinal += 1;
                }
                hash_member(&mut hash, &member)?;
                count += 1;
                bytes = bytes
                    .checked_add(member.source.size)
                    .filter(|n| *n <= self.limits.bytes)
                    .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_LIMIT"))?;
                page.push(member);
            }
            let row = rows.last().unwrap();
            after = Some((
                row.get("warehouse_id"),
                row.get("table_id"),
                row.get("object_id"),
            ));
        }
        if !page.is_empty() {
            last_page = Some(
                self.put(
                    &format!("page_{ordinal}"),
                    &InventoryPage {
                        version: 1,
                        ordinal,
                        previous: last_page,
                        members: page,
                    },
                    PAGE_BYTES,
                )
                .await?,
            );
            ordinal += 1;
        }
        let digest = format!("sha256:{:x}", hash.finalize());
        let root = InventoryRoot {
            version: 1,
            recovery_set: self.lease.set_id(),
            entries: count,
            referenced_bytes: bytes,
            pages: ordinal,
            last_page,
            closure_sha256: digest.clone(),
            rebuild_required: rebuild_dispositions(),
            namespaces,
            namespace_pages,
            last_namespace_page,
            namespace_sha256: format!("sha256:{:x}", namespace_hash.finalize()),
        };
        let sealed = self.put("inventory", &root, 16384).await?;
        let result = RecoveryContentInventory {
            version: 1,
            root: sealed,
            entries: count,
            referenced_bytes: bytes,
            closure_sha256: digest,
        };
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT 1 FROM moria.runtime_recovery_sets WHERE id=$1 FOR UPDATE",
            &[&self.lease.set_id()],
        )
        .await?;
        self.lease.check_in(tx.client()).await?;
        ensure!(!tx.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_recovery_roots WHERE recovery_set=$1 AND NOT complete)",&[&self.lease.set_id()]).await?.get::<_,bool>(0),"RECOVERY_CONTENT_INCOMPLETE");
        let counters=tx.query_one("SELECT members,bytes FROM moria.runtime_recovery_content_counters WHERE recovery_set=$1",&[&self.lease.set_id()]).await?;
        ensure!(
            counters.get::<_, i64>(0) as u64 == count && counters.get::<_, i64>(1) as u64 == bytes,
            "RECOVERY_CONTENT_INCOMPLETE"
        );
        ensure!(tx.execute("UPDATE moria.runtime_recovery_sets SET content_inventory=$2 WHERE id=$1 AND (content_inventory IS NULL OR content_inventory=$2)",&[&self.lease.set_id(),&serde_json::to_value(&result)?]).await?==1,"RECOVERY_MANIFEST_CONFLICT");
        tx.commit().await?;
        Ok(result)
    }
}

/// The returned references are server-sealed; this is not a caller declaration of backup health.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapturedRecovery {
    pub content_inventory: RecoveryContentInventory,
    pub metadata_dump: SealedObject,
    pub keys: crate::recovery_keys::RecoveryKeys,
    pub exported_snapshot: String,
}
struct Renewal(tokio::task::JoinHandle<()>);
impl Drop for Renewal {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct SnapshotSession(Option<deadpool_postgres::Object>);
impl std::ops::Deref for SnapshotSession {
    type Target = Client;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}
impl Drop for SnapshotSession {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            drop(deadpool_postgres::Object::take(client));
        }
    }
}
pub async fn check_capture_namespaces_in(client: &Client, lease: &RecoveryLease) -> Result<()> {
    lease.check_in(client).await?;
    let rows=client.query("SELECT warehouse_id,table_id,kind,captured_state FROM moria.runtime_recovery_namespaces WHERE recovery_set=$1 ORDER BY warehouse_id,table_id",&[&lease.set_id()]).await?;
    ensure!(rows.len() <= 256, "RECOVERY_CONTENT_LIMIT");
    for row in rows {
        let warehouse: String = row.get(0);
        let table: String = row.get(1);
        let kind: String = row.get(2);
        if kind == "dataset" {
            client.query_one("SELECT resource FROM moria.native_storage_namespaces WHERE warehouse_id=$1 AND table_id=$2 FOR SHARE",&[&warehouse,&table]).await?;
            client.query("SELECT b.resource FROM moria.native_dataset_bindings b JOIN moria.native_storage_namespaces n ON n.environment=b.environment AND n.resource=b.resource::text WHERE n.warehouse_id=$1 AND n.table_id=$2 FOR SHARE OF b",&[&warehouse,&table]).await?;
        } else {
            client.query_one("SELECT binding_hash FROM moria.fs_storage_bindings WHERE warehouse_id=$1 AND table_id=$2 FOR SHARE",&[&warehouse,&table]).await?;
        }
        let current: Value = client
            .query_one(
                "SELECT moria.runtime_recovery_namespace_state($1,$2,$3)",
                &[&warehouse, &table, &kind],
            )
            .await?
            .get(0);
        ensure!(
            current == row.get::<_, Value>(3),
            "RECOVERY_NAMESPACE_CHANGED"
        );
    }
    Ok(())
}

/// Holds session retention admission before opening the exported snapshot; all authority writes use fresh transactions.
pub async fn capture(
    db: Database,
    lease: RecoveryLease,
    keys: crate::recovery_keys::KeyRecoveryPlan,
    cancel: Cancellation,
) -> Result<CapturedRecovery> {
    let limits = capture_phase(
        RecoveryContentLimits::configured(),
        "RECOVERY_CONFIGURATION_FAILED",
    )?;
    let timeout = std::time::Duration::from_secs(limits.time_seconds);
    let namespace_text = std::env::var("MORIA_NATIVE_RECOVERY_NAMESPACE")
        .map_err(|_| anyhow::anyhow!("RECOVERY_NAMESPACE_INVALID"))?;
    ensure!(namespace_text.len() <= 4096, "RECOVERY_NAMESPACE_INVALID");
    let namespace: IndexNamespace = capture_phase(
        serde_json::from_str(&namespace_text).map_err(Into::into),
        "RECOVERY_CONFIGURATION_FAILED",
    )?;
    let (store, _) = capture_phase(
        storage::configured_store_with_fingerprint(&namespace),
        "RECOVERY_CONFIGURATION_FAILED",
    )?;
    let authority = Arc::new(capture_phase(
        BackupAuthority::acquire(db.clone(), lease.clone(), namespace, store.as_ref()).await,
        "RECOVERY_CONFIGURATION_FAILED",
    )?);
    {
        let mut client = db.get().await?;
        let tx = client.transaction().await?;
        lease.check_in(tx.client()).await?;
        let values = [
            limits.roots as i64,
            limits.members as i64,
            limits.bytes as i64,
            limits.dump_bytes as i64,
        ];
        tx.execute("INSERT INTO moria.runtime_recovery_content_counters(recovery_set,max_roots,max_members,max_bytes,max_dump_bytes,max_state_bytes) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",&[&lease.set_id(),&values[0],&values[1],&values[2],&values[3],&(limits.spill_bytes as i64)]).await?;
        let row=tx.query_one("SELECT max_roots,max_members,max_bytes,max_dump_bytes,max_state_bytes FROM moria.runtime_recovery_content_counters WHERE recovery_set=$1",&[&lease.set_id()]).await?;
        ensure!(
            (0..4).all(|i| row.get::<_, i64>(i) == values[i])
                && row.get::<_, i64>(4) == limits.spill_bytes as i64,
            "RECOVERY_MANIFEST_CONFLICT"
        );
        tx.commit().await?;
    }
    let renew_lease = lease.clone();
    let renew_cancel = cancel.clone();
    let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let renew_failed = failed.clone();
    let renewal = Renewal(tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            if renew_lease.renew().await.is_err() {
                renew_failed.store(true, std::sync::atomic::Ordering::SeqCst);
                renew_cancel.cancel();
                break;
            }
        }
    }));
    let mut snapshot = SnapshotSession(Some(db.get().await?));
    let operation = async {
        lease.check_key_manifest(&keys.fingerprint()?).await?;
        keys.check_unchanged()?;
        snapshot
            .query_one(
                "SELECT pg_advisory_lock_shared(hashtextextended($1,0))",
                &[&RETENTION_ADMISSION],
            )
            .await?;
        snapshot
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?;
        let exported: String = snapshot
            .query_one("SELECT pg_export_snapshot()", &[])
            .await?
            .get(0);
        {
            let mut writer = db.get().await?;
            let tx = writer.transaction().await?;
            tx.query_one(
                "SELECT 1 FROM moria.runtime_recovery_sets WHERE id=$1 FOR UPDATE",
                &[&lease.set_id()],
            )
            .await?;
            lease.check_in(tx.client()).await?;
            ensure!(tx.execute("UPDATE moria.runtime_recovery_sets SET exported_snapshot=$2 WHERE id=$1 AND (exported_snapshot IS NULL OR exported_snapshot=$2)",&[&lease.set_id(),&exported]).await?==1,"RECOVERY_MANIFEST_CONFLICT");
            tx.commit().await?;
        }
        let capture = Capture {
            db: db.clone(),
            lease: lease.clone(),
            source: Arc::new(SourceAuthority {
                db: db.clone(),
                lease: lease.clone(),
            }),
            backup: store.clone(),
            backup_authority: authority.clone(),
            limits,
            cancel: cancel.clone(),
        };
        capture_phase(
            capture.enumerate(&snapshot).await,
            "RECOVERY_ENUMERATION_FAILED",
        )?;
        let dump = capture_phase(
            lease
                .dump_metadata(
                    &snapshot,
                    &exported,
                    store.as_ref(),
                    authority.object("metadata")?,
                    authority.clone(),
                    cancel.clone(),
                    capture.limits.dump_bytes,
                    timeout,
                )
                .await,
            "RECOVERY_METADATA_FAILED",
        )?;
        capture_phase(capture.copy_members().await, "RECOVERY_COPY_FAILED")?;
        let content_inventory =
            capture_phase(capture.inventory().await, "RECOVERY_INVENTORY_FAILED")?;
        let recovered_keys = capture_phase(
            keys.copy(
                &lease,
                &authority.namespace.warehouse_id,
                &authority.namespace.table_id,
                store.as_ref(),
                authority.clone(),
                cancel.clone(),
            )
            .await,
            "RECOVERY_KEYS_FAILED",
        )?;
        keys.check_unchanged()?;
        {
            let mut client = db.get().await?;
            let tx = client.transaction().await?;
            tx.query_one(
                "SELECT 1 FROM moria.runtime_recovery_sets WHERE id=$1 FOR UPDATE",
                &[&lease.set_id()],
            )
            .await?;
            check_capture_namespaces_in(tx.client(), &lease).await?;
            tx.commit().await?;
        }
        Ok::<_, anyhow::Error>(CapturedRecovery {
            content_inventory,
            metadata_dump: dump,
            keys: recovered_keys,
            exported_snapshot: exported,
        })
    };
    let result = {
        let mut operation = Box::pin(operation);
        let deadline = tokio::time::sleep(timeout);
        tokio::pin!(deadline);
        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(5));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;
        loop {
            tokio::select! {
                result=&mut operation=>break result,
                _=&mut deadline=>break Err(anyhow::anyhow!("RECOVERY_DEADLINE")),
                _=heartbeat.tick()=>{
                    let alive=tokio::time::timeout(std::time::Duration::from_secs(10),snapshot.query_one("SELECT 1",&[])).await;
                    if !matches!(alive,Ok(Ok(_))) {break Err(anyhow::anyhow!("RECOVERY_SNAPSHOT_LOST"));}
                }
            }
        }
    };
    renewal.0.abort();
    let cleanup = snapshot
        .batch_execute("ROLLBACK; SELECT pg_advisory_unlock_all()")
        .await;
    if cleanup.is_err() {
        anyhow::bail!("RECOVERY_SNAPSHOT_CLEANUP_FAILED");
    }
    drop(snapshot.0.take());
    if failed.load(std::sync::atomic::Ordering::SeqCst) {
        anyhow::bail!("RECOVERY_LEASE_LOST");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn backup_data_objects_accept_the_durable_uuid_identity_without_path_syntax() -> Result<()> {
        let namespace = IndexNamespace {
            warehouse_id: "backup".into(),
            table_id: "retained".into(),
            revision: "v1".into(),
        };
        let set = Uuid::new_v4();
        let member = Uuid::new_v4();
        let scope = format!("recovery_{}", set.simple());
        let object = backup_object(&namespace, &scope, &format!("data_{member}"))?;
        assert_eq!(object.object_id, format!("{scope}_data_{member}"));
        for invalid in ["../data", "data/name", "data?name", "data%2fname", ""] {
            assert!(backup_object(&namespace, &scope, invalid).is_err());
        }
        Ok(())
    }
}
