use crate::{
    storage::{
        ClosureEntry, ClosureSpool, ObjectReadWindow, ObjectReader, ReadAuthority, RecordingReader,
        SealedClosure, SealedObject,
    },
    values,
};
use anyhow::{Result, bail, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture};
use iceberg::{
    TableIdent,
    io::{
        FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
        StorageConfig, StorageFactory,
    },
    spec::{
        DataContentType, DataFileFormat, FormatVersion, Manifest, ManifestContentType,
        ManifestList, Schema, TableMetadata,
    },
    table::StaticTable,
};
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    },
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt::Debug, ops::Range, sync::Arc};
use uuid::Uuid;

#[path = "snapshot_parquet_bounds.rs"]
pub(crate) mod parquet_bounds;

const METADATA_MAX: usize = 8 * 1024 * 1024;
const DECODED_MAX: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct SnapshotPin {
    pub metadata: SealedObject,
    pub table_uuid: Uuid,
    pub snapshot_id: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotProof {
    pub version: u32,
    pub reader_profile: String,
    pub metadata: SealedObject,
    pub closure: SealedClosure,
    pub table_uuid: Uuid,
    #[serde(with = "snapshot_id_text")]
    pub snapshot_id: i64,
    pub data_files: u64,
    pub physical_rows: u64,
    pub scanned_snapshots: u64,
}

mod snapshot_id_text {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
        let text = String::deserialize(deserializer)?;
        let number = text.parse::<i64>().map_err(serde::de::Error::custom)?;
        if number.to_string() != text {
            return Err(serde::de::Error::custom("Invalid snapshot ID"));
        }
        Ok(number)
    }
}

#[derive(Clone)]
struct Context {
    spool: ClosureSpool,
    reader: Arc<dyn ObjectReader>,
    authority: Arc<dyn ReadAuthority>,
    owner: Uuid,
    discovery: bool,
    retained_subset: bool,
}

impl Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotContext")
            .field("spool", &self.spool.id())
            .finish_non_exhaustive()
    }
}

impl Context {
    async fn current(&self) -> Result<()> {
        self.spool.check_current(self.authority.as_ref()).await?;
        self.owner()?;
        Ok(())
    }
    fn owner(&self) -> Result<()> {
        let db = self.spool.database()?;
        let owner: String = db.query_row(
            "SELECT value FROM info WHERE key='snapshot_owner'",
            [],
            |row| row.get(0),
        )?;
        ensure!(
            owner == self.owner.to_string(),
            "SNAPSHOT_VERIFIER_REPLACED"
        );
        Ok(())
    }
    fn reference(
        &self,
        name: &str,
        role: &str,
        expected_size: Option<u64>,
    ) -> Result<ClosureEntry> {
        self.owner()?;
        let entry = self.spool.lookup(name)?;
        if let Some(size) = expected_size {
            ensure!(entry.content.size == size, "REFERENCE_SIZE_MISMATCH");
        }
        let db = self.spool.database()?;
        let previous: Option<String> = db
            .query_row(
                "SELECT role FROM snapshot_used WHERE name=?1",
                [name],
                |row| row.get(0),
            )
            .optional()?;
        ensure!(
            previous.is_none_or(|previous| previous == role),
            "CONFLICTING_REFERENCE_ROLE"
        );
        db.execute("INSERT INTO snapshot_used(name,role,processed) VALUES(?1,?2,0) ON CONFLICT(name) DO NOTHING", rusqlite::params![name,role])?;
        Ok(entry)
    }
    fn immutable(&self, kind: &str, id: &str, value: &Value) -> Result<()> {
        self.owner()?;
        let signature = format!("{:x}", Sha256::digest(serde_json::to_vec(value)?));
        let db = self.spool.database()?;
        let old: Option<String> = db
            .query_row(
                "SELECT signature FROM snapshot_definitions WHERE kind=?1 AND id=?2",
                rusqlite::params![kind, id],
                |row| row.get(0),
            )
            .optional()?;
        ensure!(
            old.is_none_or(|old| old == signature),
            "CONFLICTING_IMMUTABLE_DEFINITION"
        );
        db.execute(
            "INSERT INTO snapshot_definitions VALUES(?1,?2,?3) ON CONFLICT(kind,id) DO NOTHING",
            rusqlite::params![kind, id, signature],
        )?;
        Ok(())
    }
    async fn bytes(&self, content: &SealedObject, maximum: usize) -> Result<Bytes> {
        ensure!(content.size <= maximum as u64, "METADATA_BYTE_LIMIT");
        let mut stream = self
            .reader
            .read_object(
                content.object.clone(),
                self.authority.clone(),
                self.spool.cancellation(),
            )
            .await?;
        let mut bytes = Vec::with_capacity(content.size as usize);
        while let Some(frame) = stream.next().await {
            let frame = frame?;
            ensure!(frame.len() <= maximum - bytes.len(), "METADATA_BYTE_LIMIT");
            bytes.extend_from_slice(&frame);
        }
        ensure!(
            bytes.len() as u64 == content.size
                && format!("sha256:{:x}", Sha256::digest(&bytes)) == content.sha256,
            "CONTENT_MISMATCH"
        );
        Ok(bytes.into())
    }
    async fn referenced_bytes(
        &self,
        name: &str,
        role: &str,
        expected_size: Option<u64>,
    ) -> Result<Bytes> {
        let entry = self.reference(name, role, expected_size)?;
        self.bytes(&entry.content, METADATA_MAX).await
    }
    fn processed(&self, name: &str) -> Result<bool> {
        let db = self.spool.database()?;
        Ok(db.query_row(
            "SELECT processed FROM snapshot_used WHERE name=?1",
            [name],
            |row| row.get(0),
        )?)
    }
    fn finish_reference(&self, name: &str) -> Result<()> {
        self.owner()?;
        self.spool
            .database()?
            .execute("UPDATE snapshot_used SET processed=1 WHERE name=?1", [name])?;
        Ok(())
    }
    fn file_io(&self) -> FileIO {
        FileIOBuilder::new(Arc::new(ClosureStorage(self.clone()))).build()
    }
}

pub async fn verify_snapshot(
    spool: &ClosureSpool,
    reader: Arc<dyn ObjectReader>,
    authority: Arc<dyn ReadAuthority>,
    pin: SnapshotPin,
) -> Result<SnapshotProof> {
    inspect_snapshot(spool, reader, authority, pin, false, false)
        .await?
        .ok_or_else(|| anyhow::anyhow!("SNAPSHOT_PROOF_MISMATCH"))
}

/// Discover only names reachable from server-pruned metadata; this never issues an attestation.
/// The destination must subsequently be sealed and verified by the ordinary strict verifier.
pub(crate) async fn prune_snapshot_closure(
    source: &ClosureSpool,
    destination: &mut ClosureSpool,
    reader: Arc<dyn ObjectReader>,
    authority: Arc<dyn ReadAuthority>,
    pin: SnapshotPin,
) -> Result<()> {
    inspect_snapshot(source, reader, authority.clone(), pin, true, false).await?;
    let mut after = String::new();
    loop {
        source.check_current(authority.as_ref()).await?;
        let names = {
            let db = source.database()?;
            let mut statement =
                db.prepare("SELECT name FROM snapshot_used WHERE name>?1 ORDER BY name LIMIT 256")?;
            statement
                .query_map([&after], |row| row.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        if names.is_empty() {
            break;
        }
        let values = names
            .iter()
            .map(|name| source.lookup(name))
            .collect::<Result<Vec<_>>>()?;
        destination.append_page(&values).await?;
        after = names.last().unwrap().clone();
    }
    Ok(())
}

/// Revalidate metadata for an immutable subset whose exact data versions were qualified at spool sealing.
/// The maintenance caller constructs metadata only by removing old snapshot/log references from its verified root.
pub(crate) async fn verify_retained_subset(
    spool: &ClosureSpool,
    reader: Arc<dyn ObjectReader>,
    authority: Arc<dyn ReadAuthority>,
    pin: SnapshotPin,
) -> Result<SnapshotProof> {
    let qualified: String = spool.database()?.query_row(
        "SELECT value FROM info WHERE key='qualified_versions'",
        [],
        |row| row.get(0),
    )?;
    ensure!(
        qualified == "versioned-qualified-root-v1",
        "SNAPSHOT_PROOF_MISMATCH"
    );
    inspect_snapshot(spool, reader, authority, pin, false, true)
        .await?
        .ok_or_else(|| anyhow::anyhow!("SNAPSHOT_PROOF_MISMATCH"))
}

async fn inspect_snapshot(
    spool: &ClosureSpool,
    reader: Arc<dyn ObjectReader>,
    authority: Arc<dyn ReadAuthority>,
    pin: SnapshotPin,
    discovery: bool,
    retained_subset: bool,
) -> Result<Option<SnapshotProof>> {
    crate::initialize_reader_limits()?;
    spool.check_current(authority.as_ref()).await?;
    let closure: SealedClosure = {
        let db = spool.database()?;
        let receipt: String =
            db.query_row("SELECT value FROM info WHERE key='receipt'", [], |row| {
                row.get(0)
            })?;
        serde_json::from_str(&receipt)?
    };
    ensure!(
        pin.metadata.object.warehouse_id == closure.object.warehouse_id
            && pin.metadata.object.table_id == closure.object.table_id,
        "BINDING_MISMATCH"
    );
    let owner = Uuid::new_v4();
    {
        let mut db = spool.database()?;
        let tx = db.transaction()?;
        tx.execute_batch("DROP TABLE IF EXISTS snapshot_used; DROP TABLE IF EXISTS snapshot_definitions; DROP TABLE IF EXISTS snapshot_edges; DROP TABLE IF EXISTS snapshot_scans; DROP TABLE IF EXISTS snapshot_files;
            CREATE TABLE snapshot_used(name TEXT PRIMARY KEY,role TEXT NOT NULL,processed INTEGER NOT NULL) WITHOUT ROWID;
            CREATE TABLE snapshot_definitions(kind TEXT NOT NULL,id TEXT NOT NULL,signature TEXT NOT NULL,PRIMARY KEY(kind,id)) WITHOUT ROWID;
            CREATE TABLE snapshot_edges(parent TEXT NOT NULL,child TEXT NOT NULL,PRIMARY KEY(parent,child)) WITHOUT ROWID;
            CREATE INDEX snapshot_edges_child ON snapshot_edges(child,parent);
            CREATE TABLE snapshot_scans(signature TEXT PRIMARY KEY) WITHOUT ROWID;
            CREATE TABLE snapshot_files(name TEXT PRIMARY KEY,bytes INTEGER NOT NULL,rows INTEGER NOT NULL) WITHOUT ROWID;")?;
        tx.execute("INSERT INTO info VALUES('snapshot_owner',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [owner.to_string()])?;
        tx.commit()?;
    }
    let recording = Arc::new(RecordingReader::new(reader, spool.clone())?);
    let context = Context {
        spool: spool.clone(),
        reader: recording.clone(),
        authority,
        owner,
        discovery,
        retained_subset,
    };
    let location = format!(
        "moria://{}/{}",
        closure.object.warehouse_id, closure.object.table_id
    );
    let root = context.bytes(&pin.metadata, METADATA_MAX).await?;
    validate_metadata(&context, &root, "root", &location, &pin, true).await?;
    loop {
        context.current().await?;
        let pending: Option<String> = context.spool.database()?.query_row("SELECT name FROM snapshot_used WHERE role='metadata' AND processed=0 ORDER BY name LIMIT 1", [], |row| row.get(0)).optional()?;
        let Some(name) = pending else {
            break;
        };
        let bytes = context.referenced_bytes(&name, "metadata", None).await?;
        validate_metadata(&context, &bytes, &name, &location, &pin, false).await?;
        context.finish_reference(&name)?;
    }
    {
        let mut db = context.spool.database()?;
        let tx = db.transaction()?;
        loop {
            let changed = tx.execute(
                "DELETE FROM snapshot_edges WHERE child NOT IN (SELECT parent FROM snapshot_edges)",
                [],
            )?;
            if changed == 0 {
                break;
            }
        }
        let cyclic: bool =
            tx.query_row("SELECT EXISTS(SELECT 1 FROM snapshot_edges)", [], |row| {
                row.get(0)
            })?;
        ensure!(!cyclic, "CYCLIC_METADATA_HISTORY");
        tx.commit()?;
    }
    context.current().await?;
    if discovery {
        return Ok(None);
    }
    let db = context.spool.database()?;
    let unknown: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM entries e LEFT JOIN snapshot_used u ON u.name=e.name WHERE u.name IS NULL)", [], |row| row.get(0))?;
    ensure!(!unknown, "EXTRA_SNAPSHOT_OBJECT");
    let (files, rows): (u64, u64) = db.query_row(
        "SELECT count(*),COALESCE(sum(rows),0) FROM snapshot_files",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let snapshots: u64 =
        db.query_row("SELECT count(*) FROM snapshot_scans", [], |row| row.get(0))?;
    drop(db);
    context.current().await?;
    let mut after = None;
    loop {
        let entries = spool
            .read_page(after.as_deref(), context.authority.as_ref())
            .await?;
        if entries.is_empty() {
            break;
        }
        for entry in &entries {
            recording.verified(&entry.content)?;
        }
        after = Some(entries.last().unwrap().name.clone());
    }
    recording.verified(&pin.metadata)?;
    recording
        .check_versions(context.authority.as_ref(), &spool.cancellation())
        .await?;
    Ok(Some(SnapshotProof {
        version: 1,
        reader_profile: "iceberg-v2-cow-flat-bounded-v2".into(),
        metadata: pin.metadata,
        closure,
        table_uuid: pin.table_uuid,
        snapshot_id: pin.snapshot_id,
        data_files: files,
        physical_rows: rows,
        scanned_snapshots: snapshots,
    }))
}

pub(crate) async fn verified_table(
    spool: &ClosureSpool,
    reader: Arc<dyn ObjectReader>,
    authority: Arc<dyn ReadAuthority>,
    proof: &SnapshotProof,
) -> Result<StaticTable> {
    spool.check_current(authority.as_ref()).await?;
    let owner: String = spool.database()?.query_row(
        "SELECT value FROM info WHERE key='snapshot_owner'",
        [],
        |row| row.get(0),
    )?;
    let context = Context {
        spool: spool.clone(),
        reader: Arc::new(RecordingReader::new(reader, spool.clone())?),
        authority,
        owner: owner.parse()?,
        discovery: false,
        retained_subset: false,
    };
    let bytes = context.bytes(&proof.metadata, METADATA_MAX).await?;
    let metadata: TableMetadata = serde_json::from_slice(&bytes)?;
    ensure!(
        metadata.uuid() == proof.table_uuid && metadata.snapshot_by_id(proof.snapshot_id).is_some(),
        "SNAPSHOT_PIN_MISMATCH"
    );
    let table = StaticTable::from_metadata(
        metadata,
        TableIdent::from_strs(["moria", "verified"])?,
        context.file_io(),
    )
    .await?;
    context.current().await?;
    Ok(table)
}

async fn validate_metadata(
    context: &Context,
    bytes: &[u8],
    name: &str,
    location: &str,
    pin: &SnapshotPin,
    root: bool,
) -> Result<()> {
    let raw: Value = serde_json::from_slice(bytes)?;
    ensure!(
        raw["format-version"] == 2
            && raw["table-uuid"].as_str() == Some(pin.table_uuid.to_string().as_str())
            && raw["location"] == location,
        "TABLE_MISMATCH"
    );
    let schemas = raw["schemas"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("MISSING_SCHEMA"))?;
    ensure!(!schemas.is_empty(), "MISSING_SCHEMA");
    let db = context.spool.database()?;
    db.execute(
        "DELETE FROM snapshot_definitions WHERE kind='local-schema'",
        [],
    )?;
    for schema in schemas {
        let id = schema["schema-id"]
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("MISSING_SCHEMA_ID"))?;
        let native: Schema = serde_json::from_value(schema.clone())?;
        columns(schema)?;
        ensure!(
            db.execute(
                "INSERT OR IGNORE INTO snapshot_definitions VALUES('local-schema',?1,'')",
                [id.to_string()]
            )? == 1,
            "DUPLICATE_SCHEMA_ID"
        );
        context.immutable("schema", &id.to_string(), &serde_json::to_value(native)?)?;
    }
    let current_schema = raw["current-schema-id"]
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("MISSING_CURRENT_SCHEMA"))?;
    ensure!(
        schemas.iter().any(|s| s["schema-id"] == current_schema),
        "UNRESOLVED_CURRENT_SCHEMA"
    );
    ensure!(
        raw["partition-specs"]
            .as_array()
            .is_some_and(|specs| specs.iter().all(|spec| spec["fields"]
                .as_array()
                .is_some_and(|fields| fields.is_empty())))
            && raw["sort-orders"]
                .as_array()
                .is_some_and(|orders| !orders.is_empty()
                    && orders.iter().all(|order| order["fields"]
                        .as_array()
                        .is_some_and(|fields| fields.is_empty())))
            && raw
                .get("statistics")
                .is_none_or(|v| v.as_array().is_some_and(|a| a.is_empty()))
            && raw
                .get("partition-statistics")
                .is_none_or(|v| v.as_array().is_some_and(|a| a.is_empty()))
            && raw["properties"]
                .get("schema.name-mapping.default")
                .is_none(),
        "UNSUPPORTED_METADATA_FEATURE"
    );
    let metadata: TableMetadata = serde_json::from_value(raw.clone())?;
    let normalized = serde_json::to_value(&metadata)?;
    let normalized_snapshots: BTreeMap<i64, &Value> = normalized["snapshots"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[])
        .iter()
        .map(|snapshot| {
            Ok((
                snapshot["snapshot-id"]
                    .as_i64()
                    .ok_or_else(|| anyhow::anyhow!("MISSING_SNAPSHOT_ID"))?,
                snapshot,
            ))
        })
        .collect::<Result<_>>()?;
    if root {
        ensure!(
            raw["current-snapshot-id"].as_i64() == Some(pin.snapshot_id),
            "SNAPSHOT_MISMATCH"
        );
    }
    let snapshots = raw["snapshots"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("MISSING_SNAPSHOTS"))?;
    if let Some(current) = raw["current-snapshot-id"].as_i64() {
        ensure!(
            snapshots.iter().any(|s| s["snapshot-id"] == current),
            "UNRESOLVED_CURRENT_SNAPSHOT"
        );
    }
    db.execute(
        "DELETE FROM snapshot_definitions WHERE kind='local-snapshot'",
        [],
    )?;
    for snapshot in snapshots {
        let id = snapshot["snapshot-id"]
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("MISSING_SNAPSHOT_ID"))?;
        ensure!(
            db.execute(
                "INSERT OR IGNORE INTO snapshot_definitions VALUES('local-snapshot',?1,'')",
                [id.to_string()]
            )? == 1,
            "DUPLICATE_SNAPSHOT_ID"
        );
        if let Some(id) = snapshot["schema-id"].as_i64() {
            ensure!(
                schemas.iter().any(|s| s["schema-id"] == id),
                "UNRESOLVED_SNAPSHOT_SCHEMA"
            );
        }
        context.immutable("snapshot", &id.to_string(), normalized_snapshots[&id])?;
    }
    if let Some(refs) = raw["refs"].as_object() {
        for reference in refs.values() {
            ensure!(
                snapshots
                    .iter()
                    .any(|snapshot| snapshot["snapshot-id"] == reference["snapshot-id"]),
                "UNRESOLVED_SNAPSHOT_REFERENCE"
            );
        }
    }
    if let Some(history) = raw["metadata-log"].as_array() {
        for entry in history {
            let child = entry["metadata-file"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("INVALID_METADATA_HISTORY"))?;
            context.reference(child, "metadata", None)?;
            db.execute(
                "INSERT INTO snapshot_edges VALUES(?1,?2)",
                rusqlite::params![name, child],
            )?;
        }
    }
    drop(db);
    let table = StaticTable::from_metadata(
        metadata,
        TableIdent::from_strs(["moria", "verified"])?,
        context.file_io(),
    )
    .await?;
    for snapshot in snapshots {
        context.current().await?;
        let id = snapshot["snapshot-id"].as_i64().unwrap();
        let manifest_list = snapshot["manifest-list"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("MISSING_MANIFEST_LIST"))?;
        let list_bytes = context
            .referenced_bytes(manifest_list, "manifest-list", None)
            .await?;
        preflight_avro(&list_bytes)?;
        let list = ManifestList::parse_with_version(&list_bytes, FormatVersion::V2)?;
        {
            let db = context.spool.database()?;
            db.execute(
                "DELETE FROM snapshot_definitions WHERE kind='local-manifest'",
                [],
            )?;
            for file in list.entries() {
                ensure!(
                    db.execute(
                        "INSERT OR IGNORE INTO snapshot_definitions VALUES('local-manifest',?1,'')",
                        [&file.manifest_path]
                    )? == 1,
                    "DUPLICATE_MANIFEST_REFERENCE"
                );
            }
        }
        let files = list.consume_entries().into_iter().collect::<Vec<_>>();
        for file in files {
            ensure!(
                file.content == ManifestContentType::Data
                    && file.manifest_length >= 0
                    && file.key_metadata.is_none()
                    && file.first_row_id.is_none(),
                "UNSUPPORTED_MANIFEST"
            );
            context.reference(
                &file.manifest_path,
                "manifest",
                Some(file.manifest_length as u64),
            )?;
            if context.processed(&file.manifest_path)? {
                continue;
            }
            let manifest_bytes = context
                .referenced_bytes(
                    &file.manifest_path,
                    "manifest",
                    Some(file.manifest_length as u64),
                )
                .await?;
            preflight_avro(&manifest_bytes)?;
            let manifest = Manifest::parse_avro(&manifest_bytes)?;
            ensure!(
                manifest.metadata().content == ManifestContentType::Data
                    && manifest.metadata().format_version == FormatVersion::V2
                    && manifest.metadata().partition_spec.is_unpartitioned(),
                "UNSUPPORTED_MANIFEST"
            );
            columns(&serde_json::to_value(manifest.metadata().schema.as_ref())?)?;
            for entry in manifest.entries() {
                let data = entry.data_file();
                ensure!(
                    data.content_type() == DataContentType::Data
                        && data.file_format() == DataFileFormat::Parquet
                        && data.key_metadata().is_none()
                        && data.sort_order_id().is_none_or(|id| id == 0)
                        && data.partition().iter().len() == 0
                        && data.referenced_data_file().is_none()
                        && data.content_offset().is_none()
                        && data.content_size_in_bytes().is_none(),
                    "UNSUPPORTED_DATA_FILE"
                );
                context.reference(data.file_path(), "data", Some(data.file_size_in_bytes()))?;
                let db = context.spool.database()?;
                let previous: Option<(u64, u64)> = db
                    .query_row(
                        "SELECT bytes,rows FROM snapshot_files WHERE name=?1",
                        [data.file_path()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                if let Some(previous) = previous {
                    ensure!(
                        previous == (data.file_size_in_bytes(), data.record_count()),
                        "CONFLICTING_DATA_DECLARATION"
                    );
                } else {
                    drop(db);
                    if !context.discovery && !context.retained_subset {
                        validate_physical(
                            context,
                            data.file_path(),
                            data.file_size_in_bytes(),
                            data.record_count(),
                        )
                        .await?;
                    }
                    context.spool.database()?.execute(
                        "INSERT INTO snapshot_files VALUES(?1,?2,?3)",
                        rusqlite::params![
                            data.file_path(),
                            data.file_size_in_bytes(),
                            data.record_count()
                        ],
                    )?;
                }
            }
            context.finish_reference(&file.manifest_path)?;
        }
        let native = table.metadata().snapshot_by_id(id).unwrap().clone();
        let schema = serde_json::to_value(native.schema(&table.metadata())?.as_ref())?;
        if raw["current-snapshot-id"] == id {
            ensure!(
                schema["fields"]
                    == serde_json::to_value(table.metadata().current_schema().as_ref())?["fields"],
                "CURRENT_SCHEMA_PROJECTION_MISMATCH"
            );
        }
        let signature = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&json!([id, manifest_list, schema]))?)
        );
        let seen = context.spool.database()?.execute(
            "INSERT OR IGNORE INTO snapshot_scans VALUES(?1)",
            [&signature],
        )? == 0;
        if seen {
            continue;
        }
        let mut tasks = table
            .scan()
            .snapshot_id(id)
            .with_concurrency_limit(1)
            .with_manifest_entry_concurrency_limit(1)
            .build()?
            .plan_files()
            .await?;
        let descriptors = columns(&schema)?;
        let db = context.spool.database()?;
        db.execute(
            "DELETE FROM snapshot_definitions WHERE kind='scan-file'",
            [],
        )?;
        drop(db);
        while let Some(task) = tasks.try_next().await? {
            ensure!(task.deletes.is_empty(), "UNSUPPORTED_DELETE_FILES");
            ensure!(
                context.spool.database()?.execute(
                    "INSERT OR IGNORE INTO snapshot_definitions VALUES('scan-file',?1,'')",
                    [&task.data_file_path]
                )? == 1,
                "DUPLICATE_SCAN_FILE"
            );
            let mut batches = table
                .reader_builder()
                .with_batch_size(1000)
                .with_data_file_concurrency_limit(1)
                .with_range_fetch_concurrency(1)
                .build()
                .read(Box::pin(futures::stream::iter([Ok(task)])))?
                .stream();
            while let Some(batch) = batches.try_next().await? {
                validate_batch(&batch, &descriptors)?;
            }
        }
    }
    Ok(())
}

fn descriptor(kind: &Value, nullable: bool) -> Result<Value> {
    let mut result = match kind {
        Value::String(kind) => match kind.as_str() {
            "boolean" => json!({"type":"boolean"}),
            "int" | "long" => json!({"type":"int64"}),
            "float" | "double" => json!({"type":"float64"}),
            "string" => json!({"type":"string"}),
            "uuid" => json!({"type":"uid"}),
            "date" => json!({"type":"date"}),
            "timestamptz" => json!({"type":"timestamp"}),
            kind if kind.starts_with("decimal(") && kind.ends_with(')') => {
                let (precision, scale) = kind[8..kind.len() - 1]
                    .split_once(',')
                    .ok_or_else(|| anyhow::anyhow!("UNSUPPORTED_SCHEMA"))?;
                json!({"type":"decimal","precision":precision.trim().parse::<u64>()?,"scale":scale.trim().parse::<u64>()?})
            }
            _ => bail!("UNSUPPORTED_SCHEMA"),
        },
        Value::Object(_)
            if kind["type"] == "list"
                && kind["element-required"] == true
                && kind["element"].is_string() =>
        {
            json!({"type":"array","items":descriptor(&kind["element"],false)?})
        }
        _ => bail!("UNSUPPORTED_SCHEMA"),
    };
    result["nullable"] = nullable.into();
    values::descriptor(&result, &Value::Null, 0)
}

pub(crate) fn columns(schema: &Value) -> Result<BTreeMap<String, Value>> {
    let fields = schema["fields"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("UNSUPPORTED_SCHEMA"))?;
    ensure!(fields.len() <= 256, "COLUMN_LIMIT");
    let mut columns = BTreeMap::new();
    for field in fields {
        let name = field["name"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("UNSUPPORTED_SCHEMA"))?;
        ensure!(
            columns
                .insert(
                    name.into(),
                    descriptor(&field["type"], field["required"] != true)?
                )
                .is_none(),
            "DUPLICATE_COLUMN"
        );
    }
    Ok(columns)
}

pub(crate) fn validate_batch(
    batch: &arrow_array::RecordBatch,
    columns: &BTreeMap<String, Value>,
) -> Result<()> {
    ensure!(
        batch.get_array_memory_size() <= DECODED_MAX && batch.num_rows() <= 1000,
        "DECODED_BATCH_LIMIT"
    );
    for row in 0..batch.num_rows() {
        let mut size = 0;
        for (name, descriptor) in columns {
            let array = batch
                .column_by_name(name)
                .ok_or_else(|| anyhow::anyhow!("MISSING_COLUMN"))?;
            let value = values::arrow_value(array.as_ref(), row, descriptor)?;
            size += serde_json::to_vec(&value)?.len();
            ensure!(size <= 4 * 1024 * 1024, "DECODED_ROW_LIMIT");
        }
    }
    Ok(())
}

pub(crate) fn preflight_avro(bytes: &[u8]) -> Result<()> {
    use apache_avro::types::Value as A;
    fn count(value: &A, bytes: &mut usize, depth: usize) -> Result<()> {
        ensure!(depth <= 32, "AVRO_DEPTH_LIMIT");
        *bytes = bytes
            .checked_add(64)
            .ok_or_else(|| anyhow::anyhow!("AVRO_DECODE_LIMIT"))?;
        match value {
            A::Bytes(v) | A::Fixed(_, v) => *bytes += v.len(),
            A::String(v) | A::Enum(_, v) => *bytes += v.len(),
            A::Union(_, v) => count(v, bytes, depth + 1)?,
            A::Array(v) => {
                for v in v {
                    count(v, bytes, depth + 1)?;
                }
            }
            A::Map(v) => {
                for (k, v) in v {
                    *bytes += k.len();
                    count(v, bytes, depth + 1)?;
                }
            }
            A::Record(v) => {
                for (k, v) in v {
                    *bytes += k.len();
                    count(v, bytes, depth + 1)?;
                }
            }
            A::Null | A::Boolean(_) | A::Int(_) | A::Long(_) | A::Float(_) | A::Double(_) => {}
            _ => bail!("UNSUPPORTED_AVRO_VALUE"),
        }
        ensure!(*bytes <= DECODED_MAX, "AVRO_DECODE_LIMIT");
        Ok(())
    }
    let mut total = 0;
    for value in apache_avro::Reader::new(bytes)? {
        count(&value?, &mut total, 0)?;
    }
    Ok(())
}

fn denied() -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::DataInvalid, "Scoped closure I/O denied")
}

#[derive(Clone, Debug)]
struct ClosureStorage(Context);

impl Serialize for ClosureStorage {
    fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "Live closure grants cannot be serialized",
        ))
    }
}
impl<'de> Deserialize<'de> for ClosureStorage {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "Live closure grants must be injected",
        ))
    }
}
#[typetag::serde(name = "moria-closure-v1")]
impl StorageFactory for ClosureStorage {
    fn build(&self, _: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.clone()))
    }
}

#[derive(Debug)]
struct ClosureRead {
    context: Context,
    content: SealedObject,
    window: tokio::sync::Mutex<ObjectReadWindow>,
}

#[async_trait]
impl FileRead for ClosureRead {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        ensure_range(&range, self.content.size).map_err(|_| denied())?;
        let mut window = self.window.lock().await;
        let filling = !window.contains(&range);
        if filling {
            self.context.owner().map_err(|_| denied())?;
        }
        let bytes = window.read(range).await.map_err(|_| denied())?;
        if filling {
            self.context.owner().map_err(|_| denied())?;
        }
        Ok(bytes)
    }
}
fn ensure_range(range: &Range<u64>, size: u64) -> Result<()> {
    ensure!(
        range.start <= range.end
            && range.end <= size
            && range.end - range.start <= 16 * 1024 * 1024,
        "PARQUET_RANGE_LIMIT"
    );
    Ok(())
}

#[typetag::serde(name = "moria-closure-v1")]
#[async_trait]
impl Storage for ClosureStorage {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        self.metadata(path).await.map(|_| true)
    }
    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        self.0.current().await.map_err(|_| denied())?;
        Ok(FileMetadata {
            size: self
                .0
                .spool
                .lookup(path)
                .map_err(|_| denied())?
                .content
                .size,
        })
    }
    async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
        self.0
            .current()
            .await
            .map_err(|error| denied().with_source(error))?;
        let entry = self
            .0
            .spool
            .lookup(path)
            .map_err(|error| denied().with_source(error))?;
        self.0
            .bytes(&entry.content, METADATA_MAX)
            .await
            .map_err(|error| denied().with_source(error))
    }
    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        self.0.current().await.map_err(|_| denied())?;
        let content = self.0.spool.lookup(path).map_err(|_| denied())?.content;
        Ok(Box::new(ClosureRead {
            context: self.0.clone(),
            window: tokio::sync::Mutex::new(ObjectReadWindow::new(
                content.object.clone(),
                self.0.reader.clone(),
                self.0.authority.clone(),
                self.0.spool.cancellation(),
            )),
            content,
        }))
    }
    fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
        self.0.spool.lookup(path).map_err(|_| denied())?;
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
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
    async fn delete_stream(
        &self,
        _: futures::stream::BoxStream<'static, String>,
    ) -> iceberg::Result<()> {
        Err(denied())
    }
    fn new_output(&self, _: &str) -> iceberg::Result<OutputFile> {
        Err(denied())
    }
}

struct ParquetRead {
    context: Context,
    size: u64,
    window: ObjectReadWindow,
}
impl AsyncFileReader for ParquetRead {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            ensure_range(&range, self.size)
                .map_err(|e| parquet::errors::ParquetError::External(e.into()))?;
            if !self.window.contains(&range) {
                self.context
                    .owner()
                    .map_err(|error| parquet::errors::ParquetError::External(error.into()))?;
            }
            self.window
                .read(range)
                .await
                .map_err(|error| parquet::errors::ParquetError::External(error.into()))
        }
        .boxed()
    }
    fn get_metadata<'a>(
        &'a mut self,
        _: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let size = self.size;
            if size < 12 {
                return Err(parquet::errors::ParquetError::General(
                    "PARQUET_ALLOCATION_LIMIT".into(),
                ));
            }
            let footer = self.get_bytes(size - 8..size).await?;
            if &footer[4..] != b"PAR1" {
                return Err(parquet::errors::ParquetError::General(
                    "PARQUET_ALLOCATION_LIMIT".into(),
                ));
            }
            let length = u64::from(u32::from_le_bytes(footer[..4].try_into().unwrap()));
            if length > METADATA_MAX as u64 || length > size - 12 {
                return Err(parquet::errors::ParquetError::General(
                    "PARQUET_ALLOCATION_LIMIT".into(),
                ));
            }
            let raw = self.get_bytes(size - 8 - length..size - 8).await?;
            parquet_bounds::footer(&raw)
                .map_err(|error| parquet::errors::ParquetError::External(error.into()))?;
            let metadata = ParquetMetaDataReader::new()
                .load_and_finish(&mut *self, size)
                .await?;
            parquet_bounds::arrow_metadata(&metadata)
                .map_err(|error| parquet::errors::ParquetError::External(error.into()))?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

async fn validate_physical(
    context: &Context,
    name: &str,
    size: u64,
    expected_rows: u64,
) -> Result<()> {
    use arrow_schema::{DataType as D, TimeUnit};
    fn physical(
        field: &arrow_schema::Field,
        parquet: &parquet::schema::types::Type,
    ) -> Result<Value> {
        fn uuid(t: &parquet::schema::types::Type) -> bool {
            if t.is_primitive() {
                t.get_basic_info().logical_type_ref() == Some(&parquet::basic::LogicalType::Uuid)
            } else {
                t.get_fields().first().is_some_and(|f| uuid(f))
            }
        }
        let mut result = match field.data_type() {
            D::Boolean => json!({"type":"boolean"}),
            D::Int32 | D::Int64 => json!({"type":"int64"}),
            D::Float32 | D::Float64 => json!({"type":"float64"}),
            D::Utf8 | D::LargeUtf8 | D::Utf8View => json!({"type":"string"}),
            D::Date32 => json!({"type":"date"}),
            D::Timestamp(TimeUnit::Microsecond, Some(_)) => json!({"type":"timestamp"}),
            D::FixedSizeBinary(16) if uuid(parquet) => json!({"type":"uid"}),
            D::Decimal128(p, s) if *s >= 0 => json!({"type":"decimal","precision":p,"scale":s}),
            D::List(child) | D::LargeList(child)
                if !child.is_nullable()
                    && !matches!(
                        child.data_type(),
                        D::List(_) | D::LargeList(_) | D::Struct(_) | D::Map(_, _)
                    ) =>
            {
                let element = parquet
                    .get_fields()
                    .first()
                    .and_then(|f| f.get_fields().first())
                    .ok_or_else(|| anyhow::anyhow!("UNSUPPORTED_PHYSICAL_SCHEMA"))?;
                json!({"type":"array","items":physical(child,element)?})
            }
            _ => bail!("UNSUPPORTED_PHYSICAL_SCHEMA"),
        };
        result["nullable"] = field.is_nullable().into();
        values::descriptor(&result, &Value::Null, 0)
    }
    let builder = ParquetRecordBatchStreamBuilder::new(ParquetRead {
        context: context.clone(),
        size,
        window: ObjectReadWindow::new(
            context.spool.lookup(name)?.content.object,
            context.reader.clone(),
            context.authority.clone(),
            context.spool.cancellation(),
        ),
    })
    .await?;
    ensure!(
        u64::try_from(builder.metadata().file_metadata().num_rows())? == expected_rows,
        "PARQUET_COUNT_MISMATCH"
    );
    context.current().await?;
    let content = context.spool.lookup(name)?.content;
    let mut header_reader = ObjectReadWindow::new(
        content.object,
        context.reader.clone(),
        context.authority.clone(),
        context.spool.cancellation(),
    );
    for chunk in parquet_bounds::metadata(builder.metadata(), size)? {
        let mut walk = parquet_bounds::PageWalk::new();
        while walk.position() < chunk.size {
            let offset = chunk.offset + walk.position();
            let end = (offset + 64 * 1024).min(chunk.offset + chunk.size);
            let bytes = header_reader.read(offset..end).await?;
            walk.header(&bytes, &chunk)?;
        }
        walk.finish(&chunk)?;
    }
    header_reader.check_current().await?;
    context.current().await?;
    ensure!(builder.schema().fields().len() <= 256, "COLUMN_LIMIT");
    let physical_fields = builder
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema()
        .get_fields();
    ensure!(
        physical_fields.len() == builder.schema().fields().len(),
        "UNSUPPORTED_PHYSICAL_SCHEMA"
    );
    let mut descriptors = BTreeMap::new();
    for (field, native) in builder.schema().fields().iter().zip(physical_fields) {
        descriptors.insert(field.name().clone(), physical(field, native)?);
    }
    let mut stream = builder.with_batch_size(1000).build()?;
    let mut rows = 0_u64;
    while let Some(batch) = stream.try_next().await? {
        context.current().await?;
        validate_batch(&batch, &descriptors)?;
        rows += batch.num_rows() as u64;
    }
    ensure!(rows == expected_rows, "PARQUET_COUNT_MISMATCH");
    Ok(())
}
