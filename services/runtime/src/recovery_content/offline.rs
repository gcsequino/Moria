//! Clean-host custody is rooted in an operator-verified manifest, never the restored database.
mod graph;
use super::*;
use crate::canonical::fingerprint;
use crate::recovery::{METADATA_PART_BYTES, MetadataArchive, MetadataPage};
use crate::recovery_keys::RecoveryKeys;
use crate::storage::{ClosureLimits, ClosureSpool, PosixStore, StorageLimits};
use rusqlite::OptionalExtension;
use serde_json::json;
use std::collections::BTreeMap;

#[derive(Debug)]
struct SeedAuthority {
    object: LogicalObject,
    cancel: Cancellation,
}
#[async_trait]
impl ReadAuthority for SeedAuthority {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        self.cancel.check()?;
        ensure!(*object == self.object, "RECOVERY_MEMBERSHIP_DENIED");
        Ok(())
    }
}

struct Membership {
    spool: ClosureSpool,
    namespace: IndexNamespace,
    physical: String,
    reader: Arc<dyn ObjectStore>,
    cancel: Cancellation,
    deadline: tokio::time::Instant,
}
impl fmt::Debug for Membership {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveryMembership")
            .field("namespace", &self.namespace)
            .finish()
    }
}
impl Membership {
    fn add(&self, content: &SealedObject) -> Result<()> {
        validate_content(content)?;
        ensure!(
            content.object.warehouse_id == self.namespace.warehouse_id
                && content.object.table_id == self.namespace.table_id,
            "RECOVERY_MEMBERSHIP_DENIED"
        );
        self.cancel.check()?;
        let text = serde_json::to_string(content)?;
        let db = self.spool.database()?;
        db.execute("INSERT INTO recovery_allowed(object,content) VALUES(?1,?2) ON CONFLICT(object) DO NOTHING",rusqlite::params![content.object.object_id,text])?;
        let old: String = db.query_row(
            "SELECT content FROM recovery_allowed WHERE object=?1",
            [&content.object.object_id],
            |r| r.get(0),
        )?;
        ensure!(old == text, "RECOVERY_MANIFEST_CONFLICT");
        Ok(())
    }
    fn content(&self, object: &LogicalObject) -> Result<SealedObject> {
        self.cancel.check()?;
        ensure!(
            tokio::time::Instant::now() < self.deadline,
            "RECOVERY_CONTENT_DEADLINE"
        );
        ensure!(
            object.warehouse_id == self.namespace.warehouse_id
                && object.table_id == self.namespace.table_id
                && self.reader.physical_fingerprint()? == self.physical,
            "RECOVERY_MEMBERSHIP_DENIED"
        );
        let text: Option<String> = self
            .spool
            .database()?
            .query_row(
                "SELECT content FROM recovery_allowed WHERE object=?1",
                [&object.object_id],
                |r| r.get(0),
            )
            .optional()?;
        let content: SealedObject = serde_json::from_str(
            &text.ok_or_else(|| anyhow::anyhow!("RECOVERY_MEMBERSHIP_DENIED"))?,
        )?;
        ensure!(content.object == *object, "RECOVERY_MEMBERSHIP_DENIED");
        Ok(content)
    }
    fn version(&self, object: &LogicalObject, version: &ObjectVersion) -> Result<()> {
        let content = self.content(object)?;
        ensure!(content.size == version.size, "RECOVERY_CONTENT_MISMATCH");
        let text = serde_json::to_string(version)?;
        let db = self.spool.database()?;
        db.execute("UPDATE recovery_allowed SET provider_version=?2 WHERE object=?1 AND provider_version IS NULL",rusqlite::params![object.object_id,text])?;
        let old: String = db.query_row(
            "SELECT provider_version FROM recovery_allowed WHERE object=?1",
            [&object.object_id],
            |r| r.get(0),
        )?;
        ensure!(old == text, "RECOVERY_PROVIDER_CHANGED");
        Ok(())
    }
}
#[async_trait]
impl ReadAuthority for Membership {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        self.content(object)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct CustodyReader {
    membership: Arc<Membership>,
}
#[async_trait]
impl ObjectReader for CustodyReader {
    fn physical_fingerprint(&self) -> Result<String> {
        self.membership.reader.physical_fingerprint()
    }
    fn chunk_bytes(&self) -> usize {
        self.membership.reader.chunk_bytes()
    }
    async fn version(
        &self,
        object: &LogicalObject,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<ObjectVersion> {
        cancel.check()?;
        authority.check_read(object).await?;
        let version = self
            .membership
            .reader
            .version(object, self.membership.as_ref(), cancel)
            .await?;
        self.membership.version(object, &version)?;
        authority.check_read(object).await?;
        cancel.check()?;
        Ok(version)
    }
    async fn range_at_version(
        &self,
        object: &LogicalObject,
        range: std::ops::Range<u64>,
        version: &ObjectVersion,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<bytes::Bytes> {
        cancel.check()?;
        authority.check_read(object).await?;
        self.membership.version(object, version)?;
        let bytes = self
            .membership
            .reader
            .range_at_version(object, range, version, self.membership.as_ref(), cancel)
            .await?;
        authority.check_read(object).await?;
        cancel.check()?;
        Ok(bytes)
    }
    async fn range(
        &self,
        object: &LogicalObject,
        range: std::ops::Range<u64>,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<bytes::Bytes> {
        let version = self.version(object, authority, cancel).await?;
        self.range_at_version(object, range, &version, authority, cancel)
            .await
    }
    async fn read_object(
        &self,
        object: LogicalObject,
        authority: Arc<dyn ReadAuthority>,
        cancel: Cancellation,
    ) -> Result<futures::stream::BoxStream<'static, Result<bytes::Bytes>>> {
        let version = self.version(&object, authority.as_ref(), &cancel).await?;
        storage::versioned_stream(Arc::new(self.clone()), object, version, authority, cancel).await
    }
}

/// This handle can only be constructed after bounded, hashed manifest-chain validation.
pub(crate) struct OfflineRecovery {
    membership: Arc<Membership>,
    inventory: RecoveryContentInventory,
    set: Uuid,
    limits: RecoveryContentLimits,
}
struct OfflineConfig {
    namespace: IndexNamespace,
    reader: Arc<dyn ObjectStore>,
    scratch: PosixStore,
    limits: RecoveryContentLimits,
    cancel: Cancellation,
}
impl OfflineRecovery {
    /// All roots must come from the external manifest whose digest the operator has verified.
    pub(crate) async fn open(
        set: Uuid,
        inventory: RecoveryContentInventory,
        metadata: &SealedObject,
        keys: &RecoveryKeys,
        cancel: Cancellation,
    ) -> Result<Self> {
        let limits = RecoveryContentLimits::configured()?;
        let namespace: IndexNamespace = serde_json::from_str(
            &std::env::var("MORIA_NATIVE_RECOVERY_NAMESPACE")
                .map_err(|_| anyhow::anyhow!("RECOVERY_CONFIG_INVALID"))?,
        )?;
        let reader = storage::configured_store_with_fingerprint(&namespace)?.0;
        let scratch = PosixStore::provision(
            std::path::Path::new(
                &std::env::var("MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT")
                    .map_err(|_| anyhow::anyhow!("RECOVERY_CONFIG_INVALID"))?,
            ),
            &namespace.warehouse_id,
            &namespace.table_id,
            StorageLimits {
                max_object_bytes: 16 * 1024 * 1024,
                chunk_bytes: 1024 * 1024,
            },
        )?;
        Self::open_on(
            set,
            inventory,
            metadata,
            keys,
            OfflineConfig {
                namespace,
                reader,
                scratch,
                limits,
                cancel,
            },
        )
        .await
    }
    async fn open_on(
        set: Uuid,
        inventory: RecoveryContentInventory,
        metadata: &SealedObject,
        keys: &RecoveryKeys,
        config: OfflineConfig,
    ) -> Result<Self> {
        let OfflineConfig {
            namespace,
            reader,
            scratch,
            limits,
            cancel,
        } = config;
        ensure!(
            inventory.version == 1
                && inventory.entries <= limits.members
                && inventory.referenced_bytes <= limits.bytes,
            "RECOVERY_CONTENT_LIMIT"
        );
        let seed = Arc::new(SeedAuthority {
            object: inventory.root.object.clone(),
            cancel: cancel.clone(),
        });
        let spool = ClosureSpool::create_read_cache(
            &scratch,
            inventory.root.object.clone(),
            ClosureLimits {
                max_entries: limits.members,
                max_referenced_bytes: limits.bytes,
                max_spool_bytes: limits.spill_bytes,
            },
            seed,
            cancel.clone(),
        )
        .await?;
        {
            let db = spool.database()?;
            db.execute_batch("CREATE TABLE recovery_allowed(object TEXT PRIMARY KEY,content TEXT NOT NULL,provider_version TEXT);CREATE TABLE recovery_restore_members(warehouse TEXT NOT NULL,table_id TEXT NOT NULL,object TEXT NOT NULL,backup_object TEXT NOT NULL UNIQUE,member TEXT NOT NULL,target_version TEXT,PRIMARY KEY(warehouse,table_id,object));CREATE TABLE recovery_seen_pages(object TEXT PRIMARY KEY);CREATE TABLE recovery_restore_namespaces(warehouse TEXT NOT NULL,table_id TEXT NOT NULL,namespace TEXT NOT NULL,PRIMARY KEY(warehouse,table_id));")?;
        }
        let membership = Arc::new(Membership {
            physical: reader.physical_fingerprint()?,
            reader,
            namespace,
            spool,
            cancel,
            deadline: tokio::time::Instant::now()
                + std::time::Duration::from_secs(limits.time_seconds),
        });
        membership.add(&inventory.root)?;
        membership.add(metadata)?;
        keys.validate()?;
        for key in &keys.entries {
            membership.add(&key.copy)?;
        }
        let value = Self {
            membership,
            inventory,
            set,
            limits,
        };
        for (index, key) in keys.entries.iter().enumerate() {
            value.expected(&key.copy.object, &format!("_key_{index}"))?;
        }
        value.inventory_chain().await?;
        value.metadata_chain(metadata).await?;
        Ok(value)
    }
    pub(crate) fn reader(&self) -> Arc<dyn ObjectReader> {
        Arc::new(CustodyReader {
            membership: self.membership.clone(),
        })
    }
    pub(crate) fn authority(&self) -> Arc<dyn ReadAuthority> {
        self.membership.clone()
    }
    async fn json<T: serde::de::DeserializeOwned>(&self, sealed: &SealedObject) -> Result<T> {
        ensure!(
            sealed.size > 0 && sealed.size <= PAGE_BYTES as u64,
            "RECOVERY_MANIFEST_CONFLICT"
        );
        let version = self
            .membership
            .reader
            .version(
                &sealed.object,
                self.membership.as_ref(),
                &self.membership.cancel,
            )
            .await?;
        self.membership.version(&sealed.object, &version)?;
        let bytes = self
            .membership
            .reader
            .range_at_version(
                &sealed.object,
                0..sealed.size,
                &version,
                self.membership.as_ref(),
                &self.membership.cancel,
            )
            .await?;
        ensure!(
            format!("sha256:{:x}", Sha256::digest(&bytes)) == sealed.sha256,
            "RECOVERY_CONTENT_MISMATCH"
        );
        ensure!(
            self.membership
                .reader
                .version(
                    &sealed.object,
                    self.membership.as_ref(),
                    &self.membership.cancel
                )
                .await?
                == version,
            "RECOVERY_PROVIDER_CHANGED"
        );
        Ok(serde_json::from_slice(&bytes)?)
    }
    fn expected(&self, object: &LogicalObject, suffix: &str) -> Result<()> {
        ensure!(
            *object
                == LogicalObject::new(
                    &self.membership.namespace.warehouse_id,
                    &self.membership.namespace.table_id,
                    &format!("recovery_{}{}", self.set.simple(), suffix)
                )?,
            "RECOVERY_MANIFEST_CONFLICT"
        );
        Ok(())
    }
    fn seen(&self, object: &LogicalObject) -> Result<()> {
        ensure!(
            self.membership.spool.database()?.execute(
                "INSERT OR IGNORE INTO recovery_seen_pages(object) VALUES(?1)",
                [&object.object_id]
            )? == 1,
            "RECOVERY_MANIFEST_CONFLICT"
        );
        Ok(())
    }
    async fn inventory_chain(&self) -> Result<()> {
        self.expected(&self.inventory.root.object, "_inventory")?;
        let root: InventoryRoot = self.json(&self.inventory.root).await?;
        ensure!(
            root.version == 1
                && root.recovery_set == self.set
                && root.entries == self.inventory.entries
                && root.referenced_bytes == self.inventory.referenced_bytes
                && root.closure_sha256 == self.inventory.closure_sha256
                && root.rebuild_required == rebuild_dispositions()
                && root.pages <= self.limits.members,
            "RECOVERY_MANIFEST_CONFLICT"
        );
        self.namespace_chain(&root).await?;
        let mut next = root.last_page;
        let mut ordinal = root.pages;
        let mut entries = 0u64;
        let mut bytes = 0u64;
        while let Some(page) = next {
            ensure!(ordinal > 0, "RECOVERY_MANIFEST_CONFLICT");
            ordinal -= 1;
            self.expected(&page.object, &format!("_page_{ordinal}"))?;
            self.membership.add(&page)?;
            self.seen(&page.object)?;
            let page: InventoryPage = self.json(&page).await?;
            ensure!(
                page.version == 1
                    && page.ordinal == ordinal
                    && !page.members.is_empty()
                    && page.members.len() <= PAGE_ENTRIES,
                "RECOVERY_MANIFEST_CONFLICT"
            );
            for member in page.members {
                validate_content(&member.source)?;
                validate_content(&member.backup)?;
                let prefix = format!("recovery_{}_data_", self.set.simple());
                let backup_id = Uuid::parse_str(
                    member
                        .backup
                        .object
                        .object_id
                        .strip_prefix(&prefix)
                        .ok_or_else(|| anyhow::anyhow!("RECOVERY_MANIFEST_CONFLICT"))?,
                )?;
                self.expected(&member.backup.object, &format!("_data_{backup_id}"))?;
                ensure!(
                    member.source.size == member.backup.size
                        && member.source.sha256 == member.backup.sha256
                        && member.source_namespace.warehouse_id
                            == member.source.object.warehouse_id
                        && member.source_namespace.table_id == member.source.object.table_id
                        && !member.source_namespace.revision.is_empty()
                        && member.source_namespace.revision.len() <= 128
                        && member.source_version.size == member.source.size
                        && member.backup_version.size == member.backup.size
                        && !member.source_version.token.is_empty()
                        && member.source_version.token.len() <= 4096
                        && !member.backup_version.token.is_empty()
                        && member.backup_version.token.len() <= 4096
                        && matches!(member.source_kind.as_str(), "dataset" | "filesystem"),
                    "RECOVERY_MANIFEST_CONFLICT"
                );
                entries = entries
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_LIMIT"))?;
                bytes = bytes
                    .checked_add(member.source.size)
                    .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_LIMIT"))?;
                ensure!(
                    entries <= self.inventory.entries && bytes <= self.inventory.referenced_bytes,
                    "RECOVERY_CONTENT_LIMIT"
                );
                self.membership.add(&member.backup)?;
                let db = self.membership.spool.database()?;
                let declared:Option<String>=db.query_row("SELECT namespace FROM recovery_restore_namespaces WHERE warehouse=?1 AND table_id=?2",rusqlite::params![member.source.object.warehouse_id,member.source.object.table_id],|r|r.get(0)).optional()?;
                let declared: CapturedNamespace = serde_json::from_str(
                    &declared.ok_or_else(|| anyhow::anyhow!("RECOVERY_MANIFEST_CONFLICT"))?,
                )?;
                ensure!(
                    declared.namespace == member.source_namespace
                        && declared.kind == member.source_kind
                        && declared.physical_fingerprint == member.physical_fingerprint,
                    "RECOVERY_MANIFEST_CONFLICT"
                );
                ensure!(db.execute("INSERT OR IGNORE INTO recovery_restore_members(warehouse,table_id,object,backup_object,member) VALUES(?1,?2,?3,?4,?5)",rusqlite::params![member.source.object.warehouse_id,member.source.object.table_id,member.source.object.object_id,member.backup.object.object_id,serde_json::to_string(&member)?])?==1,"RECOVERY_MANIFEST_CONFLICT");
            }
            next = page.previous;
        }
        ensure!(
            ordinal == 0
                && entries == self.inventory.entries
                && bytes == self.inventory.referenced_bytes,
            "RECOVERY_MANIFEST_CONFLICT"
        );
        let mut hash = inventory_hash();
        let mut after = None::<(String, String, String)>;
        loop {
            let members = self.page_members(after.as_ref())?;
            if members.is_empty() {
                break;
            }
            for member in members {
                hash_member(&mut hash, &member)?;
                after = Some((
                    member.source.object.warehouse_id,
                    member.source.object.table_id,
                    member.source.object.object_id,
                ));
            }
        }
        ensure!(
            format!("sha256:{:x}", hash.finalize()) == self.inventory.closure_sha256,
            "RECOVERY_CONTENT_MISMATCH"
        );
        Ok(())
    }
    fn page_members(
        &self,
        after: Option<&(String, String, String)>,
    ) -> Result<Vec<InventoryMember>> {
        let db = self.membership.spool.database()?;
        let mut stmt=db.prepare("SELECT member FROM recovery_restore_members WHERE ?1 IS NULL OR (warehouse,table_id,object)>(?1,?2,?3) ORDER BY warehouse,table_id,object LIMIT 64")?;
        let rows = stmt.query_map(
            rusqlite::params![
                after.map(|a| &a.0),
                after.map(|a| &a.1),
                after.map(|a| &a.2)
            ],
            |r| r.get::<_, String>(0),
        )?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    fn namespaces(&self) -> Result<Vec<CapturedNamespace>> {
        let db = self.membership.spool.database()?;
        let mut statement=db.prepare("SELECT namespace FROM recovery_restore_namespaces ORDER BY warehouse,table_id LIMIT 257")?;
        let rows = statement.query_map([], |r| r.get::<_, String>(0))?;
        let values = rows
            .map(|r| Ok(serde_json::from_str(&r?)?))
            .collect::<Result<Vec<_>>>()?;
        ensure!(values.len() <= 256, "RECOVERY_NAMESPACE_LIMIT");
        Ok(values)
    }
    async fn namespace_chain(&self, root: &InventoryRoot) -> Result<()> {
        ensure!(
            root.namespaces <= 256 && root.namespace_pages == root.namespaces.div_ceil(128),
            "RECOVERY_MANIFEST_CONFLICT"
        );
        let mut ordinal = root.namespace_pages;
        let mut next = root.last_namespace_page.clone();
        let mut count = 0u32;
        while let Some(page) = next {
            ensure!(ordinal > 0, "RECOVERY_MANIFEST_CONFLICT");
            ordinal -= 1;
            self.expected(&page.object, &format!("_namespace_page_{ordinal}"))?;
            self.membership.add(&page)?;
            self.seen(&page.object)?;
            let page: NamespacePage = self.json(&page).await?;
            ensure!(
                page.version == 1
                    && page.ordinal == ordinal
                    && !page.namespaces.is_empty()
                    && page.namespaces.len() <= 128,
                "RECOVERY_MANIFEST_CONFLICT"
            );
            for namespace in page.namespaces {
                LogicalObject::new(
                    &namespace.namespace.warehouse_id,
                    &namespace.namespace.table_id,
                    "namespace_probe",
                )?;
                ensure!(
                    !namespace.namespace.revision.is_empty()
                        && namespace.namespace.revision.len() <= 128
                        && matches!(namespace.kind.as_str(), "dataset" | "filesystem")
                        && !namespace.physical_fingerprint.is_empty()
                        && namespace.physical_fingerprint.len() <= 128,
                    "RECOVERY_MANIFEST_CONFLICT"
                );
                count += 1;
                ensure!(count <= root.namespaces, "RECOVERY_NAMESPACE_LIMIT");
                ensure!(self.membership.spool.database()?.execute("INSERT OR IGNORE INTO recovery_restore_namespaces(warehouse,table_id,namespace) VALUES(?1,?2,?3)",rusqlite::params![namespace.namespace.warehouse_id,namespace.namespace.table_id,serde_json::to_string(&namespace)?])?==1,"RECOVERY_MANIFEST_CONFLICT");
            }
            next = page.previous;
        }
        ensure!(
            ordinal == 0 && count == root.namespaces,
            "RECOVERY_MANIFEST_CONFLICT"
        );
        let mut hash = namespace_hash();
        for namespace in self.namespaces()? {
            hash_namespace(&mut hash, &namespace)?;
        }
        ensure!(
            format!("sha256:{:x}", hash.finalize()) == root.namespace_sha256,
            "RECOVERY_CONTENT_MISMATCH"
        );
        Ok(())
    }
    async fn metadata_chain(&self, sealed: &SealedObject) -> Result<()> {
        self.expected(&sealed.object, "_metadata")?;
        let root: MetadataArchive = self.json(sealed).await?;
        ensure!(
            root.version == 1
                && root.format == "postgresql-custom-parts-v1"
                && root.part_bytes as usize == METADATA_PART_BYTES
                && root.bytes > 0
                && root.bytes <= self.limits.dump_bytes
                && root.parts as u64 == root.bytes.div_ceil(METADATA_PART_BYTES as u64),
            "RECOVERY_MANIFEST_CONFLICT"
        );
        let mut next = Some(root.last_page);
        let mut end = root.parts;
        let mut offset = root.bytes;
        while let Some(page) = next {
            ensure!(end > 0, "RECOVERY_MANIFEST_CONFLICT");
            self.expected(&page.object, &format!("_metadata_page_{}", (end - 1) / 128))?;
            self.membership.add(&page)?;
            self.seen(&page.object)?;
            let page: MetadataPage = self.json(&page).await?;
            ensure!(
                page.version == 1
                    && !page.parts.is_empty()
                    && page.parts.len() <= 128
                    && page.start_index.checked_add(page.parts.len() as u32) == Some(end)
                    && page.start_offset
                        == u64::from(page.start_index) * METADATA_PART_BYTES as u64,
                "RECOVERY_MANIFEST_CONFLICT"
            );
            let mut part_offset = page.start_offset;
            for (i, part) in page.parts.iter().enumerate() {
                let index = page.start_index + i as u32;
                self.expected(&part.object, &format!("_metadata_part_{index}"))?;
                ensure!(
                    part.size
                        == if index + 1 == root.parts {
                            root.bytes - u64::from(index) * METADATA_PART_BYTES as u64
                        } else {
                            METADATA_PART_BYTES as u64
                        },
                    "RECOVERY_MANIFEST_CONFLICT"
                );
                part_offset = part_offset
                    .checked_add(part.size)
                    .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_LIMIT"))?;
                self.membership.add(part)?;
            }
            ensure!(part_offset == offset, "RECOVERY_MANIFEST_CONFLICT");
            end = page.start_index;
            offset = page.start_offset;
            next = page.previous;
        }
        ensure!(end == 0 && offset == 0, "RECOVERY_MANIFEST_CONFLICT");
        Ok(())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RestoredNamespace {
    source: IndexNamespace,
    target: IndexNamespace,
    kind: String,
    source_physical_fingerprint: String,
    physical_fingerprint: String,
}
impl RestoredNamespace {
    pub(crate) fn source(&self) -> &IndexNamespace {
        &self.source
    }
    pub(crate) fn target(&self) -> &IndexNamespace {
        &self.target
    }
    pub(crate) fn kind(&self) -> &str {
        &self.kind
    }
    pub(crate) fn physical_fingerprint(&self) -> &str {
        &self.physical_fingerprint
    }
    pub(crate) fn source_physical_fingerprint(&self) -> &str {
        &self.source_physical_fingerprint
    }
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecoveredContentProof {
    #[serde(skip)]
    recovery_set: Uuid,
    restore: Uuid,
    target_custody: Vec<TargetCustody>,
    inventory: RecoveryContentInventory,
    entries: u64,
    bytes: u64,
    target_mapping_hash: String,
    namespaces: Vec<RestoredNamespace>,
    rebuild_required: Vec<String>,
    #[serde(skip)]
    custody: Arc<Membership>,
    #[serde(skip)]
    targets: BTreeMap<(String, String), Arc<TargetAuthority>>,
}
impl RecoveredContentProof {
    pub(crate) fn inventory(&self) -> &RecoveryContentInventory {
        &self.inventory
    }
    pub(crate) fn entries(&self) -> u64 {
        self.entries
    }
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
    pub(crate) fn target_mapping_hash(&self) -> &str {
        &self.target_mapping_hash
    }
    pub(crate) fn namespaces(&self) -> &[RestoredNamespace] {
        &self.namespaces
    }
    /// Requalify only exact copied members of previously qualified native roots in this actual disabled restore.
    pub(crate) async fn qualify_datasets_in(
        &self,
        client: &Client,
        restore: Uuid,
        incarnation: Uuid,
    ) -> Result<()> {
        self.custody.cancel.check()?;
        ensure!(restore == self.restore, "RECOVERY_MANIFEST_CONFLICT");
        client
            .query_one("SELECT moria.runtime_recovery_operator()", &[])
            .await?;
        let previous:Uuid=client.query_one("SELECT r.previous_incarnation FROM moria.runtime_restores r JOIN moria.runtime_recovery_control c ON c.restore_id=r.id JOIN moria.indexing_instance i ON i.incarnation=r.new_incarnation WHERE r.id=$1 AND r.recovery_set=$2 AND r.new_incarnation=$3 AND r.status='disabled' AND r.leases_fenced AND NOT c.dispatch_enabled",&[&restore,&self.recovery_set,&incarnation]).await?.get(0);
        let inventory = serde_json::to_value(&self.inventory)?;
        for namespace in self
            .namespaces
            .iter()
            .filter(|value| value.kind == "dataset")
        {
            let source = serde_json::to_value(&namespace.source)?;
            let target = serde_json::to_value(&namespace.target)?;
            ensure!(!client.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_issued_objects o JOIN moria.runtime_dataset_producers p ON p.scope=o.producer WHERE o.physical_fingerprint=$1 AND p.incarnation<>$2) OR EXISTS(SELECT 1 FROM moria.runtime_dataset_root_qualifications WHERE physical_fingerprint=$1 AND incarnation<>$2) OR EXISTS(SELECT 1 FROM moria.runtime_dataset_gc_candidates WHERE physical_fingerprint=$1)",&[&namespace.physical_fingerprint,&incarnation]).await?.get::<_,bool>(0),"RECOVERY_TARGET_NOT_FRESH");

            let scope = format!(
                "dataset-restore/{}/{}/{}",
                restore,
                incarnation,
                fingerprint(&target)?
            );
            client.execute("INSERT INTO moria.runtime_dataset_producers(scope,restore,incarnation,namespace,physical_fingerprint) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",&[&scope,&restore,&incarnation,&target,&namespace.physical_fingerprint]).await?;
            client.execute("INSERT INTO moria.runtime_dataset_root_qualifications(root,physical_fingerprint,incarnation,restore,producer,namespace,inventory,target_mapping_hash) SELECT r.key,$1,$2,$3,$4,$5,$6,$7 FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_root_locations l ON l.root=r.key AND l.physical_fingerprint=$8 AND l.incarnation=$9 AND l.sealed WHERE r.namespace=$10 AND r.reader_profile='iceberg-v2-cow-flat-bounded-v2' AND r.sealed AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_expired_roots e WHERE e.root=r.key) ON CONFLICT DO NOTHING",&[&namespace.physical_fingerprint,&incarnation,&restore,&scope,&target,&inventory,&self.target_mapping_hash,&namespace.source_physical_fingerprint,&previous,&source]).await?;
        }
        let mut after = None::<(String, String, String)>;
        loop {
            self.custody.cancel.check()?;
            ensure!(
                tokio::time::Instant::now() < self.custody.deadline,
                "RECOVERY_CONTENT_DEADLINE"
            );
            let members = {
                let db = self.custody.spool.database()?;
                let mut statement=db.prepare("SELECT member,target_version FROM recovery_restore_members WHERE ?1 IS NULL OR (warehouse,table_id,object)>(?1,?2,?3) ORDER BY warehouse,table_id,object LIMIT 64")?;
                statement
                    .query_map(
                        rusqlite::params![
                            after.as_ref().map(|a| &a.0),
                            after.as_ref().map(|a| &a.1),
                            after.as_ref().map(|a| &a.2)
                        ],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                    )?
                    .map(|row| {
                        let (m, v) = row?;
                        Ok((
                            serde_json::from_str::<InventoryMember>(&m)?,
                            serde_json::from_str::<ObjectVersion>(&v)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            if members.is_empty() {
                break;
            }
            let mut values = Vec::new();
            for (member, version) in &members {
                let object = &member.source.object;
                if member.source_kind == "dataset" {
                    let target = self
                        .targets
                        .get(&(object.warehouse_id.clone(), object.table_id.clone()))
                        .ok_or_else(|| anyhow::anyhow!("RECOVERY_TARGET_REQUIRED"))?;
                    ensure!(
                        version.size == member.source.size,
                        "RECOVERY_CONTENT_MISMATCH"
                    );
                    let source = self
                        .namespaces
                        .iter()
                        .find(|namespace| {
                            namespace.source.warehouse_id == object.warehouse_id
                                && namespace.source.table_id == object.table_id
                                && namespace.kind == "dataset"
                        })
                        .ok_or_else(|| anyhow::anyhow!("RECOVERY_TARGET_REQUIRED"))?;
                    values.push(json!({"content":member.source,"physicalVersion":version,"physicalFingerprint":target.physical,"sourcePhysicalFingerprint":source.source_physical_fingerprint}));
                }
            }
            if !values.is_empty() {
                let values = Value::Array(values);
                self.qualify_graph_nodes_in(client, restore, incarnation, previous, &values)
                    .await?;
                client.execute("INSERT INTO moria.runtime_dataset_qualified_members(root,physical_fingerprint,incarnation,warehouse_id,table_id,object_id,content,physical_version) SELECT m.root,q.physical_fingerprint,q.incarnation,m.warehouse_id,m.table_id,m.object_id,m.content,v->'physicalVersion' FROM jsonb_array_elements($1) v JOIN moria.runtime_dataset_root_members m ON m.warehouse_id=v->'content'->'object'->>'warehouseId' AND m.table_id=v->'content'->'object'->>'tableId' AND m.object_id=v->'content'->'object'->>'objectId' AND m.content=v->'content' JOIN moria.runtime_dataset_root_qualifications q ON q.root=m.root AND q.restore=$2 AND q.incarnation=$3 AND q.physical_fingerprint=v->>'physicalFingerprint' ON CONFLICT DO NOTHING",&[&values,&restore,&incarnation]).await?;
            }
            let object = &members.last().unwrap().0.source.object;
            after = Some((
                object.warehouse_id.clone(),
                object.table_id.clone(),
                object.object_id.clone(),
            ));
        }
        client.execute("UPDATE moria.runtime_dataset_root_qualifications q SET sealed=true WHERE q.restore=$1 AND q.incarnation=$2 AND NOT q.sealed AND EXISTS(SELECT 1 FROM moria.runtime_dataset_root_members m WHERE m.root=q.root) AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_root_members m WHERE m.root=q.root AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_qualified_members v WHERE v.root=m.root AND v.physical_fingerprint=q.physical_fingerprint AND v.incarnation=q.incarnation AND v.warehouse_id=m.warehouse_id AND v.table_id=m.table_id AND v.object_id=m.object_id AND v.content=m.content))",&[&restore,&incarnation]).await?;
        ensure!(!client.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_root_qualifications q JOIN moria.runtime_dataset_roots r ON r.key=q.root JOIN moria.snapshot_history h ON h.metadata_location=r.metadata_location WHERE q.restore=$1 AND q.incarnation=$2 AND NOT q.sealed)",&[&restore,&incarnation]).await?.get::<_,bool>(0),"RECOVERY_CONTENT_MISMATCH");
        ensure!(!client.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_qualified_members m JOIN moria.runtime_dataset_root_qualifications q USING(root,physical_fingerprint,incarnation) JOIN moria.runtime_dataset_issued_objects o ON o.physical_fingerprint=m.physical_fingerprint AND o.warehouse_id=m.warehouse_id AND o.table_id=m.table_id AND o.object_id=m.object_id WHERE q.restore=$1 AND q.incarnation=$2 AND q.sealed AND o.producer<>q.producer) OR EXISTS(SELECT 1 FROM moria.runtime_dataset_qualified_members m JOIN moria.runtime_dataset_root_qualifications q USING(root,physical_fingerprint,incarnation) JOIN moria.runtime_dataset_gc_candidates c ON c.physical_fingerprint=m.physical_fingerprint AND c.warehouse_id=m.warehouse_id AND c.table_id=m.table_id AND c.object_id=m.object_id WHERE q.restore=$1 AND q.incarnation=$2 AND q.sealed)",&[&restore,&incarnation]).await?.get::<_,bool>(0),"RECOVERY_TARGET_NOT_FRESH");
        client.execute("INSERT INTO moria.runtime_dataset_issued_objects(physical_fingerprint,warehouse_id,table_id,object_id,producer,expected_size,expected_sha256) SELECT DISTINCT m.physical_fingerprint,m.warehouse_id,m.table_id,m.object_id,q.producer,(m.content->>'size')::bigint,m.content->>'sha256' FROM moria.runtime_dataset_qualified_members m JOIN moria.runtime_dataset_root_qualifications q USING(root,physical_fingerprint,incarnation) WHERE q.restore=$1 AND q.incarnation=$2 AND q.sealed ON CONFLICT DO NOTHING",&[&restore,&incarnation]).await?;
        self.qualify_graph_roots_in(client, restore, incarnation, previous)
            .await?;
        self.custody.cancel.check()?;
        Ok(())
    }
    pub(crate) async fn check_current(&self) -> Result<()> {
        let mut targets = BTreeMap::new();
        for (key, old) in &self.targets {
            let store = storage::configured_store_with_fingerprint(&old.namespace)?.0;
            ensure!(
                store.physical_fingerprint()? == old.physical,
                "RECOVERY_TARGET_CHANGED"
            );
            targets.insert(
                key.clone(),
                Arc::new(TargetAuthority {
                    membership: self.custody.clone(),
                    namespace: old.namespace.clone(),
                    store,
                    physical: old.physical.clone(),
                }),
            );
        }
        self.check_targets(&targets).await
    }
    async fn check_targets(
        &self,
        targets: &BTreeMap<(String, String), Arc<TargetAuthority>>,
    ) -> Result<()> {
        for custody in &self.target_custody {
            let target = targets
                .get(&(
                    custody.namespace.warehouse_id.clone(),
                    custody.namespace.table_id.clone(),
                ))
                .ok_or_else(|| anyhow::anyhow!("RECOVERY_TARGET_REQUIRED"))?;
            let authority = CustodyAuthority::new(target.clone())?;
            ensure!(
                target.physical == custody.physical_fingerprint
                    && target
                        .store
                        .version(&custody.object.object, &authority, &self.custody.cancel)
                        .await?
                        == custody.physical_version,
                "RECOVERY_TARGET_CHANGED"
            );
            let bytes = target
                .store
                .range_at_version(
                    &custody.object.object,
                    0..custody.object.size,
                    &custody.physical_version,
                    &authority,
                    &self.custody.cancel,
                )
                .await?;
            ensure!(
                bytes.len() as u64 == custody.object.size
                    && format!("sha256:{:x}", Sha256::digest(&bytes)) == custody.object.sha256,
                "RECOVERY_TARGET_CHANGED"
            );
            ensure!(
                target
                    .store
                    .version(&custody.object.object, &authority, &self.custody.cancel)
                    .await?
                    == custody.physical_version,
                "RECOVERY_TARGET_CHANGED"
            );
        }
        for target in targets.values() {
            self.custody.cancel.check()?;
            ensure!(
                target.store.physical_fingerprint()? == target.physical
                    && tokio::time::Instant::now() < self.custody.deadline,
                "RECOVERY_TARGET_CHANGED"
            );
        }
        let mut after = None::<(String, String, String)>;
        loop {
            let members = {
                let db = self.custody.spool.database()?;
                let mut statement=db.prepare("SELECT member,target_version FROM recovery_restore_members WHERE ?1 IS NULL OR (warehouse,table_id,object)>(?1,?2,?3) ORDER BY warehouse,table_id,object LIMIT 64")?;
                let rows = statement.query_map(
                    rusqlite::params![
                        after.as_ref().map(|a| &a.0),
                        after.as_ref().map(|a| &a.1),
                        after.as_ref().map(|a| &a.2)
                    ],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )?;
                rows.map(|r| {
                    let (member, version) = r?;
                    Ok((
                        serde_json::from_str::<InventoryMember>(&member)?,
                        serde_json::from_str::<ObjectVersion>(&version)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?
            };
            if members.is_empty() {
                break;
            }
            for (member, version) in members {
                let object = &member.source.object;
                let target = targets
                    .get(&(object.warehouse_id.clone(), object.table_id.clone()))
                    .ok_or_else(|| anyhow::anyhow!("RECOVERY_TARGET_REQUIRED"))?;
                ensure!(
                    target
                        .store
                        .version(object, target.as_ref(), &self.custody.cancel)
                        .await?
                        == version,
                    "RECOVERY_PROVIDER_CHANGED"
                );
                let mut stream = storage::versioned_stream(
                    target.store.clone(),
                    object.clone(),
                    version.clone(),
                    target.clone(),
                    self.custody.cancel.clone(),
                )
                .await?;
                let mut hash = Sha256::new();
                let mut seen = 0u64;
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    seen = seen
                        .checked_add(chunk.len() as u64)
                        .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_LIMIT"))?;
                    ensure!(seen <= member.source.size, "RECOVERY_CONTENT_MISMATCH");
                    hash.update(&chunk);
                }
                ensure!(
                    seen == member.source.size
                        && format!("sha256:{:x}", hash.finalize()) == member.source.sha256
                        && target
                            .store
                            .version(object, target.as_ref(), &self.custody.cancel)
                            .await?
                            == version,
                    "RECOVERY_CONTENT_MISMATCH"
                );
                after = Some((
                    object.warehouse_id.clone(),
                    object.table_id.clone(),
                    object.object_id.clone(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug)]
struct TargetAuthority {
    membership: Arc<Membership>,
    namespace: IndexNamespace,
    store: Arc<dyn ObjectStore>,
    physical: String,
}
impl TargetAuthority {
    fn check(&self, object: &LogicalObject) -> Result<()> {
        self.membership.cancel.check()?;
        ensure!(
            tokio::time::Instant::now() < self.membership.deadline,
            "RECOVERY_CONTENT_DEADLINE"
        );
        ensure!(
            object.warehouse_id == self.namespace.warehouse_id
                && object.table_id == self.namespace.table_id
                && self.store.physical_fingerprint()? == self.physical,
            "RECOVERY_MEMBERSHIP_DENIED"
        );
        let present:bool=self.membership.spool.database()?.query_row("SELECT EXISTS(SELECT 1 FROM recovery_restore_members WHERE warehouse=?1 AND table_id=?2 AND object=?3)",rusqlite::params![object.warehouse_id,object.table_id,object.object_id],|r|r.get(0))?;
        ensure!(present, "RECOVERY_MEMBERSHIP_DENIED");
        Ok(())
    }
}
#[async_trait]
impl ReadAuthority for TargetAuthority {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        self.check(object)
    }
}
#[async_trait]
impl WriteAuthority for TargetAuthority {
    fn scope_key(&self) -> String {
        format!("recovery-offline/{}", self.namespace.revision)
    }
    async fn check_write(&self, object: &LogicalObject) -> Result<()> {
        self.check(object)
    }
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TargetCustody {
    namespace: IndexNamespace,
    physical_fingerprint: String,
    object: SealedObject,
    physical_version: ObjectVersion,
}
#[derive(Debug)]
struct CustodyAuthority {
    target: Arc<TargetAuthority>,
    object: LogicalObject,
}
impl CustodyAuthority {
    fn new(target: Arc<TargetAuthority>) -> Result<Self> {
        let object = LogicalObject::new(
            &target.namespace.warehouse_id,
            &target.namespace.table_id,
            "moria_restore_target_custody_v1",
        )?;
        Ok(Self { target, object })
    }
    fn check(&self, object: &LogicalObject) -> Result<()> {
        self.target.membership.cancel.check()?;
        ensure!(
            tokio::time::Instant::now() < self.target.membership.deadline,
            "RECOVERY_CONTENT_DEADLINE"
        );
        ensure!(
            object == &self.object
                && self.target.store.physical_fingerprint()? == self.target.physical,
            "RECOVERY_TARGET_CHANGED"
        );
        Ok(())
    }
}
#[async_trait]
impl ReadAuthority for CustodyAuthority {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        self.check(object)
    }
}
#[async_trait]
impl WriteAuthority for CustodyAuthority {
    fn scope_key(&self) -> String {
        format!("restore-target-custody/{}", self.target.namespace.revision)
    }
    async fn check_write(&self, object: &LogicalObject) -> Result<()> {
        self.check(object)
    }
}
impl OfflineRecovery {
    async fn target_custody(
        &self,
        restore: Uuid,
        mapping: &str,
        targets: &BTreeMap<(String, String), Arc<TargetAuthority>>,
    ) -> Result<(
        Vec<TargetCustody>,
        std::collections::BTreeSet<(String, String)>,
    )> {
        ensure!(!restore.is_nil(), "RECOVERY_INVALID");
        let mut existing = Vec::new();
        let mut fresh = std::collections::BTreeSet::new();
        let mut pending = Vec::new();
        for (key, target) in targets {
            let authority = Arc::new(CustodyAuthority::new(target.clone())?);
            let body=canonical_json(&json!({"version":1,"restore":restore,"recoverySet":self.set,"inventory":self.inventory,"namespace":target.namespace,"physicalFingerprint":target.physical,"targetMappingHash":mapping}))?.into_bytes();
            ensure!(body.len() <= 4096, "RECOVERY_CONTENT_LIMIT");
            let object = SealedObject {
                object: authority.object.clone(),
                size: body.len() as u64,
                sha256: format!("sha256:{:x}", Sha256::digest(&body)),
            };
            if target
                .store
                .inspect(&object.object, authority.as_ref(), &self.membership.cancel)
                .await?
                .is_some()
            {
                let recovered = target
                    .store
                    .recover_object(
                        object.object.clone(),
                        object.size,
                        object.sha256.clone(),
                        authority.clone(),
                        authority.clone(),
                        self.membership.cancel.clone(),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("RECOVERY_TARGET_NOT_FRESH"))?;
                ensure!(
                    recovered.as_ref() == Some(&object),
                    "RECOVERY_TARGET_NOT_FRESH"
                );
                let version = target
                    .store
                    .version(&object.object, authority.as_ref(), &self.membership.cancel)
                    .await?;
                existing.push(TargetCustody {
                    namespace: target.namespace.clone(),
                    physical_fingerprint: target.physical.clone(),
                    object,
                    physical_version: version,
                });
            } else {
                fresh.insert(key.clone());
                pending.push((target.clone(), authority, object, body));
            }
        }
        // Absence is checked for exact inventory keys before creating any ownership marker. No prefix is enumerated.
        let mut after = None;
        loop {
            let members = self.page_members(after.as_ref())?;
            if members.is_empty() {
                break;
            }
            for member in members {
                let object = &member.source.object;
                let key = (object.warehouse_id.clone(), object.table_id.clone());
                if fresh.contains(&key) {
                    let target = targets
                        .get(&key)
                        .ok_or_else(|| anyhow::anyhow!("RECOVERY_TARGET_REQUIRED"))?;
                    ensure!(
                        target
                            .store
                            .inspect(object, target.as_ref(), &self.membership.cancel)
                            .await?
                            .is_none(),
                        "RECOVERY_TARGET_NOT_FRESH"
                    );
                }
                after = Some((key.0, key.1, object.object_id.clone()));
            }
        }
        for (target, authority, object, body) in pending {
            let mut write = target
                .store
                .begin_object(
                    object.object.clone(),
                    object.size,
                    object.sha256.clone(),
                    authority.clone(),
                    self.membership.cancel.clone(),
                )
                .await?;
            for bytes in body.chunks(target.store.chunk_bytes()) {
                write.append(bytes).await?;
            }
            let sealed = write
                .seal()
                .await
                .map_err(|_| anyhow::anyhow!("RECOVERY_TARGET_NOT_FRESH"))?;
            ensure!(sealed == object, "RECOVERY_TARGET_NOT_FRESH");
            let version = target
                .store
                .version(&object.object, authority.as_ref(), &self.membership.cancel)
                .await?;
            existing.push(TargetCustody {
                namespace: target.namespace.clone(),
                physical_fingerprint: target.physical.clone(),
                object,
                physical_version: version,
            });
        }
        existing.sort_by(|a, b| {
            (&a.namespace.warehouse_id, &a.namespace.table_id)
                .cmp(&(&b.namespace.warehouse_id, &b.namespace.table_id))
        });
        Ok((existing, fresh))
    }
}
impl OfflineRecovery {
    /// Target namespaces are operator-installed configuration; request JSON never supplies physical paths.
    pub(crate) async fn copy_inventory(
        &self,
        restore: Uuid,
        targets: Vec<IndexNamespace>,
    ) -> Result<RecoveredContentProof> {
        ensure!(targets.len() <= 256, "RECOVERY_CONTENT_LIMIT");
        let targets = targets
            .into_iter()
            .map(|namespace| {
                Ok((
                    namespace.clone(),
                    storage::configured_store_with_fingerprint(&namespace)?.0,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        self.copy_stores(restore, targets).await
    }
    async fn copy_stores(
        &self,
        restore: Uuid,
        targets: Vec<(IndexNamespace, Arc<dyn ObjectStore>)>,
    ) -> Result<RecoveredContentProof> {
        ensure!(targets.len() <= 256, "RECOVERY_CONTENT_LIMIT");
        let mut guards = BTreeMap::new();
        for (namespace, store) in targets {
            let key = (namespace.warehouse_id.clone(), namespace.table_id.clone());
            let physical = store.physical_fingerprint()?;
            ensure!(
                physical != self.membership.physical,
                "RECOVERY_TARGET_CONFLICT"
            );
            ensure!(
                guards
                    .insert(
                        key,
                        Arc::new(TargetAuthority {
                            membership: self.membership.clone(),
                            namespace,
                            store,
                            physical
                        })
                    )
                    .is_none(),
                "RECOVERY_TARGET_CONFLICT"
            );
        }
        let mut after = None;
        let mut namespaces = BTreeMap::new();
        for namespace in self.namespaces()? {
            let key = (
                namespace.namespace.warehouse_id.clone(),
                namespace.namespace.table_id.clone(),
            );
            let target = guards
                .get(&key)
                .ok_or_else(|| anyhow::anyhow!("RECOVERY_TARGET_REQUIRED"))?;
            ensure!(
                target.namespace.revision == namespace.namespace.revision,
                "RECOVERY_TARGET_REVISION_CHANGED"
            );
            ensure!(
                target.physical != namespace.physical_fingerprint,
                "RECOVERY_TARGET_NOT_ISOLATED"
            );
            namespaces.insert(
                (key.0, key.1, namespace.namespace.revision.clone()),
                RestoredNamespace {
                    source: namespace.namespace,
                    target: target.namespace.clone(),
                    kind: namespace.kind,
                    source_physical_fingerprint: namespace.physical_fingerprint,
                    physical_fingerprint: target.physical.clone(),
                },
            );
        }
        ensure!(guards.len() == namespaces.len(), "RECOVERY_TARGET_CONFLICT");
        let mapping = crate::canonical::fingerprint(&serde_json::to_value(
            namespaces.values().collect::<Vec<_>>(),
        )?)?;
        let (target_custody, fresh_targets) =
            self.target_custody(restore, &mapping, &guards).await?;
        let mut entries = 0u64;
        let mut bytes = 0u64;
        loop {
            let members = self.page_members(after.as_ref())?;
            if members.is_empty() {
                break;
            }
            for member in members {
                let key = (
                    member.source.object.warehouse_id.clone(),
                    member.source.object.table_id.clone(),
                );
                let target = guards
                    .get(&key)
                    .ok_or_else(|| anyhow::anyhow!("RECOVERY_TARGET_REQUIRED"))?;
                ensure!(
                    target.namespace.revision == member.source_namespace.revision,
                    "RECOVERY_TARGET_REVISION_CHANGED"
                );
                ensure!(
                    target.physical != member.physical_fingerprint,
                    "RECOVERY_TARGET_NOT_ISOLATED"
                );
                let version = self
                    .membership
                    .reader
                    .version(
                        &member.backup.object,
                        self.membership.as_ref(),
                        &self.membership.cancel,
                    )
                    .await?;
                self.membership.version(&member.backup.object, &version)?;
                if fresh_targets.contains(&key) {
                    ensure!(
                        target
                            .store
                            .inspect(
                                &member.source.object,
                                target.as_ref(),
                                &self.membership.cancel
                            )
                            .await?
                            .is_none(),
                        "RECOVERY_TARGET_NOT_FRESH"
                    );
                }
                let recovered = if fresh_targets.contains(&key) {
                    None
                } else {
                    target
                        .store
                        .recover_object(
                            member.source.object.clone(),
                            member.source.size,
                            member.source.sha256.clone(),
                            target.clone(),
                            target.clone(),
                            self.membership.cancel.clone(),
                        )
                        .await?
                };
                let mut write = if recovered.is_none() {
                    Some(
                        target
                            .store
                            .begin_object(
                                member.source.object.clone(),
                                member.source.size,
                                member.source.sha256.clone(),
                                target.clone(),
                                self.membership.cancel.clone(),
                            )
                            .await?,
                    )
                } else {
                    None
                };
                let mut stream = storage::versioned_stream(
                    self.membership.reader.clone(),
                    member.backup.object.clone(),
                    version.clone(),
                    self.membership.clone(),
                    self.membership.cancel.clone(),
                )
                .await?;
                let mut hash = Sha256::new();
                let mut seen = 0u64;
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    seen = seen
                        .checked_add(chunk.len() as u64)
                        .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_LIMIT"))?;
                    ensure!(seen <= member.source.size, "RECOVERY_CONTENT_MISMATCH");
                    hash.update(&chunk);
                    if let Some(write) = &mut write {
                        write.append(&chunk).await?;
                    }
                }
                ensure!(
                    seen == member.source.size
                        && format!("sha256:{:x}", hash.finalize()) == member.source.sha256
                        && self
                            .membership
                            .reader
                            .version(
                                &member.backup.object,
                                self.membership.as_ref(),
                                &self.membership.cancel
                            )
                            .await?
                            == version,
                    "RECOVERY_CONTENT_MISMATCH"
                );
                if let Some(write) = write {
                    ensure!(
                        write.seal().await? == member.source,
                        "RECOVERY_CONTENT_MISMATCH"
                    );
                }
                let target_version = target
                    .store
                    .version(
                        &member.source.object,
                        target.as_ref(),
                        &self.membership.cancel,
                    )
                    .await?;
                target.check(&member.source.object)?;
                self.membership.spool.database()?.execute("UPDATE recovery_restore_members SET target_version=?4 WHERE warehouse=?1 AND table_id=?2 AND object=?3",rusqlite::params![member.source.object.warehouse_id,member.source.object.table_id,member.source.object.object_id,serde_json::to_string(&target_version)?])?;
                let ns_key = (
                    key.0.clone(),
                    key.1.clone(),
                    member.source_namespace.revision.clone(),
                );
                if let Some(previous) = namespaces.get(&ns_key) {
                    let previous: &RestoredNamespace = previous;
                    ensure!(
                        previous.kind == member.source_kind
                            && previous.source_physical_fingerprint == member.physical_fingerprint,
                        "RECOVERY_MANIFEST_CONFLICT"
                    );
                } else {
                    namespaces.insert(
                        ns_key,
                        RestoredNamespace {
                            source: member.source_namespace.clone(),
                            target: target.namespace.clone(),
                            kind: member.source_kind.clone(),
                            source_physical_fingerprint: member.physical_fingerprint.clone(),
                            physical_fingerprint: target.physical.clone(),
                        },
                    );
                }
                entries += 1;
                bytes = bytes
                    .checked_add(seen)
                    .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONTENT_LIMIT"))?;
                after = Some((key.0, key.1, member.source.object.object_id));
            }
        }
        ensure!(
            entries == self.inventory.entries && bytes == self.inventory.referenced_bytes,
            "RECOVERY_CONTENT_MISMATCH"
        );
        let mut after = None;
        loop {
            let members = self.page_members(after.as_ref())?;
            if members.is_empty() {
                break;
            }
            for member in members {
                let object = &member.source.object;
                let target = guards
                    .get(&(object.warehouse_id.clone(), object.table_id.clone()))
                    .ok_or_else(|| anyhow::anyhow!("RECOVERY_TARGET_REQUIRED"))?;
                let previous:String=self.membership.spool.database()?.query_row("SELECT target_version FROM recovery_restore_members WHERE warehouse=?1 AND table_id=?2 AND object=?3",rusqlite::params![object.warehouse_id,object.table_id,object.object_id],|r|r.get(0))?;
                ensure!(
                    target
                        .store
                        .version(object, target.as_ref(), &self.membership.cancel)
                        .await?
                        == serde_json::from_str::<ObjectVersion>(&previous)?,
                    "RECOVERY_PROVIDER_CHANGED"
                );
                after = Some((
                    object.warehouse_id.clone(),
                    object.table_id.clone(),
                    object.object_id.clone(),
                ));
            }
        }
        let namespaces = namespaces.into_values().collect::<Vec<_>>();
        let mapping = crate::canonical::fingerprint(&serde_json::to_value(&namespaces)?)?;
        Ok(RecoveredContentProof {
            recovery_set: self.set,
            restore,
            target_custody,
            inventory: self.inventory.clone(),
            entries,
            bytes,
            target_mapping_hash: mapping,
            namespaces,
            rebuild_required: rebuild_dispositions(),
            custody: self.membership.clone(),
            targets: guards,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    struct Directory(std::path::PathBuf);
    impl Directory {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("moria-recovery-offline-{}", Uuid::new_v4()));
            std::fs::create_dir(&path).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
            Self(path)
        }
        fn store(&self, warehouse: &str, table: &str) -> Arc<dyn ObjectStore> {
            Arc::new(
                PosixStore::provision(
                    &self.0,
                    warehouse,
                    table,
                    StorageLimits {
                        max_object_bytes: 16 * 1024 * 1024,
                        chunk_bytes: 4096,
                    },
                )
                .unwrap(),
            )
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[derive(Debug)]
    struct FixtureAuthority {
        warehouse: String,
        table: String,
    }
    impl FixtureAuthority {
        fn check(&self, object: &LogicalObject) -> Result<()> {
            ensure!(
                object.warehouse_id == self.warehouse && object.table_id == self.table,
                "FORBIDDEN"
            );
            Ok(())
        }
    }
    #[async_trait]
    impl ReadAuthority for FixtureAuthority {
        async fn check_read(&self, object: &LogicalObject) -> Result<()> {
            self.check(object)
        }
    }
    #[async_trait]
    impl WriteAuthority for FixtureAuthority {
        fn scope_key(&self) -> String {
            "fixture".into()
        }
        async fn check_write(&self, object: &LogicalObject) -> Result<()> {
            self.check(object)
        }
    }
    async fn put(
        store: &dyn ObjectStore,
        authority: Arc<FixtureAuthority>,
        object: LogicalObject,
        bytes: &[u8],
    ) -> SealedObject {
        let mut write = store
            .begin_object(
                object,
                bytes.len() as u64,
                format!("sha256:{:x}", Sha256::digest(bytes)),
                authority,
                Cancellation::default(),
            )
            .await
            .unwrap();
        write.append(bytes).await.unwrap();
        write.seal().await.unwrap()
    }
    struct Fixture {
        backup: Directory,
        scratch: Directory,
        target: Directory,
        store: Arc<dyn ObjectStore>,
        namespace: IndexNamespace,
        inventory: RecoveryContentInventory,
        metadata: SealedObject,
        keys: RecoveryKeys,
        set: Uuid,
        restore: Uuid,
        member: InventoryMember,
    }
    impl Fixture {
        async fn new() -> Self {
            let backup = Directory::new();
            let scratch = Directory::new();
            let target = Directory::new();
            let namespace = IndexNamespace {
                warehouse_id: "backup".into(),
                table_id: "retained".into(),
                revision: "v1".into(),
            };
            let store = backup.store("backup", "retained");
            let authority = Arc::new(FixtureAuthority {
                warehouse: "backup".into(),
                table: "retained".into(),
            });
            let set = Uuid::new_v4();
            let scope = format!("recovery_{}", set.simple());
            let object = |suffix: &str| {
                LogicalObject::new("backup", "retained", &format!("{scope}_{suffix}")).unwrap()
            };
            let source = SealedObject {
                object: LogicalObject::new("data", "table", "immutable_source").unwrap(),
                size: 12,
                sha256: format!("sha256:{:x}", Sha256::digest(b"source bytes")),
            };
            let copied = put(
                store.as_ref(),
                authority.clone(),
                object(&format!("data_{}", Uuid::new_v4())),
                b"source bytes",
            )
            .await;
            let backup_version = store
                .version(&copied.object, authority.as_ref(), &Cancellation::default())
                .await
                .unwrap();
            let member = InventoryMember {
                source,
                source_namespace: IndexNamespace {
                    warehouse_id: "data".into(),
                    table_id: "table".into(),
                    revision: "source-v1".into(),
                },
                source_kind: "dataset".into(),
                physical_fingerprint: "original-provider".into(),
                source_version: ObjectVersion {
                    size: 12,
                    token: "retained-original-version".into(),
                },
                backup: copied,
                backup_version,
            };
            let page = InventoryPage {
                version: 1,
                ordinal: 0,
                previous: None,
                members: vec![member.clone()],
            };
            let page = put(
                store.as_ref(),
                authority.clone(),
                object("page_0"),
                canonical_json(&serde_json::to_value(page).unwrap())
                    .unwrap()
                    .as_bytes(),
            )
            .await;
            let mut hash = inventory_hash();
            hash_member(&mut hash, &member).unwrap();
            let digest = format!("sha256:{:x}", hash.finalize());
            let namespace_values = vec![
                CapturedNamespace {
                    namespace: member.source_namespace.clone(),
                    kind: "dataset".into(),
                    physical_fingerprint: member.physical_fingerprint.clone(),
                },
                CapturedNamespace {
                    namespace: IndexNamespace {
                        warehouse_id: "filesystem".into(),
                        table_id: "content".into(),
                        revision: "fs-v1".into(),
                    },
                    kind: "filesystem".into(),
                    physical_fingerprint: "empty-original-provider".into(),
                },
            ];
            let mut namespace_hash = namespace_hash();
            for namespace in &namespace_values {
                hash_namespace(&mut namespace_hash, namespace).unwrap();
            }
            let namespace_page = put(
                store.as_ref(),
                authority.clone(),
                object("namespace_page_0"),
                canonical_json(
                    &serde_json::to_value(NamespacePage {
                        version: 1,
                        ordinal: 0,
                        previous: None,
                        namespaces: namespace_values,
                    })
                    .unwrap(),
                )
                .unwrap()
                .as_bytes(),
            )
            .await;
            let root = InventoryRoot {
                version: 1,
                recovery_set: set,
                entries: 1,
                referenced_bytes: 12,
                pages: 1,
                last_page: Some(page),
                closure_sha256: digest.clone(),
                rebuild_required: rebuild_dispositions(),
                namespaces: 2,
                namespace_pages: 1,
                last_namespace_page: Some(namespace_page),
                namespace_sha256: format!("sha256:{:x}", namespace_hash.finalize()),
            };
            let root = put(
                store.as_ref(),
                authority.clone(),
                object("inventory"),
                canonical_json(&serde_json::to_value(root).unwrap())
                    .unwrap()
                    .as_bytes(),
            )
            .await;
            let inventory = RecoveryContentInventory {
                version: 1,
                root,
                entries: 1,
                referenced_bytes: 12,
                closure_sha256: digest,
            };
            let part = put(
                store.as_ref(),
                authority.clone(),
                object("metadata_part_0"),
                b"archive",
            )
            .await;
            let meta_page = MetadataPage {
                version: 1,
                start_index: 0,
                start_offset: 0,
                parts: vec![part.clone()],
                previous: None,
            };
            let meta_page = put(
                store.as_ref(),
                authority.clone(),
                object("metadata_page_0"),
                &serde_json::to_vec(&meta_page).unwrap(),
            )
            .await;
            let archive = MetadataArchive {
                version: 1,
                format: "postgresql-custom-parts-v1".into(),
                part_bytes: METADATA_PART_BYTES as u32,
                parts: 1,
                bytes: 7,
                sha256: part.sha256,
                last_page: meta_page,
                database_roles: vec![],
                database_memberships: vec![],
            };
            let metadata = put(
                store.as_ref(),
                authority.clone(),
                object("metadata"),
                &serde_json::to_vec(&archive).unwrap(),
            )
            .await;
            let key = put(
                store.as_ref(),
                authority.clone(),
                object("key_0"),
                &[17u8; 32],
            )
            .await;
            let keys=RecoveryKeys{version:1,fingerprint:crate::canonical::fingerprint(&serde_json::json!([{"keyId":"session-encryption","size":"32","sha256":key.sha256}])).unwrap().trim_start_matches("sha256:").into(),entries:vec![crate::recovery_keys::RecoveryKey{key_id:"session-encryption".into(),copy:key}]};
            Self {
                backup,
                scratch,
                target,
                store,
                namespace,
                inventory,
                metadata,
                keys,
                set,
                restore: Uuid::new_v4(),
                member,
            }
        }
        async fn open(&self) -> Result<OfflineRecovery> {
            let scratch = PosixStore::provision(
                &self.scratch.0,
                "backup",
                "retained",
                StorageLimits {
                    max_object_bytes: 16 * 1024 * 1024,
                    chunk_bytes: 4096,
                },
            )?;
            OfflineRecovery::open_on(
                self.set,
                self.inventory.clone(),
                &self.metadata,
                &self.keys,
                OfflineConfig {
                    namespace: self.namespace.clone(),
                    reader: self.store.clone(),
                    scratch,
                    limits: RecoveryContentLimits {
                        roots: 10,
                        members: 100,
                        bytes: 1024 * 1024,
                        spill_bytes: 4 * 1024 * 1024,
                        dump_bytes: 1024 * 1024,
                        time_seconds: 30,
                    },
                    cancel: Cancellation::default(),
                },
            )
            .await
        }
        fn targets(&self) -> Vec<(IndexNamespace, Arc<dyn ObjectStore>)> {
            vec![
                (
                    IndexNamespace {
                        warehouse_id: "data".into(),
                        table_id: "table".into(),
                        revision: "source-v1".into(),
                    },
                    self.target.store("data", "table"),
                ),
                (
                    IndexNamespace {
                        warehouse_id: "filesystem".into(),
                        table_id: "content".into(),
                        revision: "fs-v1".into(),
                    },
                    self.target.store("filesystem", "content"),
                ),
            ]
        }
    }
    #[tokio::test]
    async fn offline_inventory_copies_exact_members_without_database_and_replays() -> Result<()> {
        let fixture = Fixture::new().await;
        let recovery = fixture.open().await?;
        let proof = recovery
            .copy_stores(fixture.restore, fixture.targets())
            .await?;
        assert_eq!(proof.entries(), 1);
        assert_eq!(proof.bytes(), 12);
        assert_eq!(proof.namespaces().len(), 2);
        assert_eq!(proof.namespaces()[1].kind(), "filesystem");
        proof.check_targets(&proof.targets).await?;
        assert_eq!(proof.namespaces()[0].source().revision, "source-v1");
        assert_eq!(proof.namespaces()[0].target().revision, "source-v1");
        assert_ne!(
            proof.namespaces()[0].physical_fingerprint(),
            "original-provider"
        );
        assert_eq!(
            recovery
                .copy_stores(fixture.restore, fixture.targets())
                .await?
                .target_mapping_hash(),
            proof.target_mapping_hash()
        );
        let different_restore = recovery
            .copy_stores(Uuid::new_v4(), fixture.targets())
            .await
            .err()
            .expect("a later restore cannot adopt an earlier custody marker");
        assert!(
            different_restore
                .to_string()
                .contains("RECOVERY_TARGET_NOT_FRESH")
        );
        let unknown = LogicalObject::new("backup", "retained", "unlisted")?;
        assert!(
            recovery
                .reader()
                .version(
                    &unknown,
                    recovery.authority().as_ref(),
                    &Cancellation::default()
                )
                .await
                .is_err()
        );
        fixture
            .keys
            .verify(
                recovery.reader().as_ref(),
                recovery.authority().as_ref(),
                &Cancellation::default(),
            )
            .await?;
        Ok(())
    }
    #[tokio::test]
    async fn offline_inventory_rejects_identical_unowned_target_and_custody_tamper() -> Result<()> {
        let fixture = Fixture::new().await;
        let recovery = fixture.open().await?;
        let target = fixture.target.store("data", "table");
        let authority = Arc::new(FixtureAuthority {
            warehouse: "data".into(),
            table: "table".into(),
        });
        put(
            target.as_ref(),
            authority.clone(),
            fixture.member.source.object.clone(),
            b"source bytes",
        )
        .await;
        for _ in 0..2 {
            let error = recovery
                .copy_stores(fixture.restore, fixture.targets())
                .await
                .err()
                .expect("matching foreign bytes are not custody");
            assert!(error.to_string().contains("RECOVERY_TARGET_NOT_FRESH"));
        }
        let marker = LogicalObject::new("data", "table", "moria_restore_target_custody_v1")?;
        assert!(
            target
                .inspect(&marker, authority.as_ref(), &Cancellation::default())
                .await?
                .is_none()
        );
        let fixture = Fixture::new().await;
        let recovery = fixture.open().await?;
        let proof = recovery
            .copy_stores(fixture.restore, fixture.targets())
            .await?;
        proof.check_targets(&proof.targets).await?;
        let marker = &proof.target_custody[0];
        let path = fixture
            .target
            .0
            .join(&marker.namespace.warehouse_id)
            .join(&marker.namespace.table_id)
            .join("objects")
            .join(&marker.object.object.object_id);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        std::fs::write(&path, b"different custody")?;
        assert!(proof.check_targets(&proof.targets).await.is_err());
        Ok(())
    }
    #[tokio::test]
    async fn offline_inventory_rejects_changed_provider_version_and_existing_wrong_target()
    -> Result<()> {
        let fixture = Fixture::new().await;
        let recovery = fixture.open().await?;
        let target = fixture.target.store("data", "table");
        let authority = Arc::new(FixtureAuthority {
            warehouse: "data".into(),
            table: "table".into(),
        });
        put(
            target.as_ref(),
            authority,
            fixture.member.source.object.clone(),
            b"wrong bytes!",
        )
        .await;
        assert!(
            recovery
                .copy_stores(fixture.restore, fixture.targets())
                .await
                .is_err()
        );
        let object = fixture.metadata.object.clone();
        let path = fixture
            .backup
            .0
            .join("backup/retained/objects")
            .join(&object.object_id);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        assert!(
            recovery
                .reader()
                .version(
                    &object,
                    recovery.authority().as_ref(),
                    &Cancellation::default()
                )
                .await
                .is_err()
        );
        Ok(())
    }
    #[tokio::test]
    async fn offline_inventory_rejects_same_size_backup_corruption_and_logical_revision_changes()
    -> Result<()> {
        let fixture = Fixture::new().await;
        let recovery = fixture.open().await?;
        let mut targets = fixture.targets();
        targets[0].0.revision = "different-policy-revision".into();
        assert!(
            recovery
                .copy_stores(fixture.restore, targets)
                .await
                .err()
                .expect("revision change must fail")
                .to_string()
                .contains("RECOVERY_TARGET_REVISION_CHANGED")
        );
        let path = fixture
            .backup
            .0
            .join("backup/retained/objects")
            .join(&fixture.member.backup.object.object_id);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        std::fs::write(&path, b"source BYTE!")?;
        assert!(
            recovery
                .copy_stores(fixture.restore, fixture.targets())
                .await
                .is_err()
        );
        Ok(())
    }
}
