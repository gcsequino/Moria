//! Qualified native v3 metadata and bounded physical IO. Standard Iceberg data/delete files remain truth.
use crate::{
    canonical::canonical_json,
    dataset_graph_read::{GraphFile, GraphReadContext, ReadableNode},
    persistent_directory::{NodeReference, read::ReadDirectory},
    storage::SealedObject,
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use iceberg::{
    io::{
        FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
        StorageConfig, StorageFactory,
    },
    spec::{FormatVersion, TableMetadata},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{ops::Range, sync::Arc};
use uuid::Uuid;
pub const PROFILE: &str = "iceberg-v2-native-mor-bounded-v3";
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LogicalFile {
    pub name: String,
    pub content: SealedObject,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct VisibilityIndex {
    pub rows: u64,
    pub hashes: Option<NodeReference>,
    pub locations: Option<NodeReference>,
}
impl VisibilityIndex {
    fn validate(&self, rows: u64) -> Result<()> {
        ensure!(
            self.rows == rows
                && self.rows <= 9_007_199_254_740_991
                && (self.rows == 0 || self.hashes.is_some())
                && self.hashes.is_some() == self.locations.is_some(),
            "DATASET_KEY_INDEX_INVALID"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RootDescriptor {
    pub version: u32,
    pub reader_profile: String,
    pub profile: Value,
    pub table_uuid: Uuid,
    pub snapshot_id: String,
    pub sequence: String,
    pub schema_hash: String,
    pub key_schema_hash: String,
    pub primary_key: Vec<String>,
    pub data_files: u64,
    pub rows: u64,
    pub selections: Option<NodeReference>,
    pub metadata: LogicalFile,
    pub files: NodeReference,
    pub visibility: Option<VisibilityIndex>,
}
#[derive(Clone, Debug)]
pub struct NativeSnapshot {
    context: GraphReadContext,
    root: ReadableNode,
    descriptor: RootDescriptor,
    files: ReadDirectory,
    metadata: Arc<TableMetadata>,
    // Only immutable metadata is retained; each lookup still checks live root/file authority.
    run_cache: Arc<crate::dataset_writer::key_run_set::RunCache>,
}
impl NativeSnapshot {
    pub async fn open(context: GraphReadContext, root: ReadableNode) -> Result<Self> {
        ensure!(root.kind() == "root", "DATASET_GRAPH_ANCESTRY_REQUIRED");
        let descriptor: RootDescriptor = serde_json::from_value(context.descriptor(&root).await?)?;
        ensure!(
            descriptor.version == 3
                && descriptor.reader_profile == PROFILE
                && &descriptor.profile == context.profile()
                && descriptor.profile["readerProfile"] == PROFILE
                && descriptor.profile["tableUuid"] == descriptor.table_uuid.to_string()
                && descriptor.profile["keySchemaHash"] == descriptor.key_schema_hash
                && context.snapshot_id() == &Value::String(descriptor.snapshot_id.clone()),
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let snapshot = descriptor.snapshot_id.parse::<i64>()?;
        ensure!(
            snapshot.to_string() == descriptor.snapshot_id,
            "SNAPSHOT_PIN_MISMATCH"
        );
        let sequence = descriptor.sequence.parse::<i64>()?;
        ensure!(
            sequence >= 0 && sequence.to_string() == descriptor.sequence,
            "DATASET_GRAPH_INVALID"
        );
        ensure!(
            descriptor.rows <= 9_007_199_254_740_991
                && root.entries() == descriptor.rows
                && (descriptor.data_files == 0) == descriptor.selections.is_none()
                && (descriptor.data_files != 0 || descriptor.rows == 0)
                && descriptor.primary_key.is_empty() == descriptor.visibility.is_none(),
            "DATASET_KEY_INDEX_INVALID"
        );
        if let Some(index) = &descriptor.visibility {
            index.validate(descriptor.rows)?;
        }
        let files_node = context.child(&root, "files").await?;
        let files =
            ReadDirectory::new(context.clone(), files_node, descriptor.files.clone()).await?;
        let file = Self::logical_file(&context, &files, &descriptor.metadata).await?;
        ensure!(file.content().size <= 8 * 1024 * 1024, "RESOURCE_LIMIT");
        let mut frames = file.stream().await?;
        let mut bytes = Vec::with_capacity(file.content().size as usize);
        while let Some(frame) = frames.next().await {
            let frame = frame?;
            ensure!(
                bytes
                    .len()
                    .checked_add(frame.len())
                    .is_some_and(|n| n <= file.content().size as usize),
                "CONTENT_MISMATCH"
            );
            bytes.extend_from_slice(&frame);
        }
        ensure!(
            bytes.len() as u64 == file.content().size,
            "CONTENT_MISMATCH"
        );
        let metadata: TableMetadata = serde_json::from_slice(&bytes)?;
        ensure!(
            metadata.format_version() == FormatVersion::V2
                && metadata.uuid() == descriptor.table_uuid
                && metadata.current_snapshot_id() == Some(snapshot)
                && metadata
                    .current_snapshot()
                    .is_some_and(|head| head.sequence_number() == sequence),
            "SNAPSHOT_PIN_MISMATCH"
        );
        let schema_hash = format!(
            "sha256:{:x}",
            Sha256::digest(
                canonical_json(&serde_json::to_value(metadata.current_schema().as_ref())?)?
                    .as_bytes()
            )
        );
        ensure!(schema_hash == descriptor.schema_hash, "SCHEMA_CHANGED");
        let columns =
            crate::snapshot::columns(&serde_json::to_value(metadata.current_schema().as_ref())?)?;
        ensure!(
            descriptor.primary_key.len() <= 256
                && descriptor.primary_key.iter().all(|key| columns
                    .get(key)
                    .is_some_and(|descriptor| descriptor["nullable"] == false))
                && descriptor
                    .primary_key
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    == descriptor.primary_key.len(),
            "DATASET_PRIMARY_KEY_INVALID"
        );
        let key_hash = crate::canonical::fingerprint(
            &serde_json::json!({"version":1,"keys":descriptor.primary_key.iter().map(|key|serde_json::json!({"name":key,"descriptor":columns[key]})).collect::<Vec<_>>()}),
        )?;
        ensure!(
            key_hash == descriptor.key_schema_hash,
            "DATASET_PRIMARY_KEY_INVALID"
        );
        context.check_node(&root).await?;
        Ok(Self {
            context,
            root,
            descriptor,
            files,
            metadata: Arc::new(metadata),
            run_cache: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }
    async fn logical_file(
        context: &GraphReadContext,
        files: &ReadDirectory,
        expected: &LogicalFile,
    ) -> Result<GraphFile> {
        ensure!(
            !expected.name.is_empty()
                && expected.name.len() <= 4096
                && expected.name.starts_with("moria://"),
            "DATASET_GRAPH_INVALID"
        );
        let value = files
            .lookup(&expected.name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_ANCESTRY_REQUIRED"))?;
        let logical: LogicalFile = serde_json::from_value(value.value)?;
        ensure!(
            logical.name == expected.name
                && logical.content == expected.content
                && value.links.len() == 1,
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let file = context.file(&value.links[0]).await?;
        ensure!(
            file.content() == &expected.content,
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        Ok(file)
    }
    pub async fn file(&self, name: &str) -> Result<GraphFile> {
        self.check_current().await?;
        ensure!(
            !name.is_empty() && name.len() <= 4096 && name.starts_with("moria://"),
            "DATASET_GRAPH_INVALID"
        );
        let value = self
            .files
            .lookup(name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_ANCESTRY_REQUIRED"))?;
        let logical: LogicalFile = serde_json::from_value(value.value)?;
        ensure!(
            logical.name == name && value.links.len() == 1,
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let file = self.context.file(&value.links[0]).await?;
        ensure!(
            file.content() == &logical.content,
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        self.check_current().await?;
        Ok(file)
    }
    pub fn metadata(&self) -> &Arc<TableMetadata> {
        &self.metadata
    }
    pub fn snapshot_id(&self) -> Result<i64> {
        Ok(self.descriptor.snapshot_id.parse()?)
    }
    pub async fn check_current(&self) -> Result<()> {
        self.context.check_node(&self.root).await
    }
    pub(crate) async fn index_directory(&self, kind: &str) -> Result<Option<ReadDirectory>> {
        self.check_current().await?;
        let reference = if kind == "selections" {
            &self.descriptor.selections
        } else {
            let visibility = self
                .descriptor
                .visibility
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("DATASET_WRITE_CAPABILITY_REQUIRED"))?;
            match kind {
                "hashes" => &visibility.hashes,
                "locations" => &visibility.locations,
                _ => anyhow::bail!("DATASET_KEY_INDEX_INVALID"),
            }
        };
        let Some(reference) = reference else {
            return Ok(None);
        };
        if kind == "selections" {
            ensure!(
                reference.entries == self.descriptor.data_files,
                "DATASET_KEY_INDEX_INVALID"
            );
        }
        let node = self.context.child(&self.root, kind).await?;
        Ok(Some(
            ReadDirectory::new(self.context.clone(), node, reference.clone()).await?,
        ))
    }
    pub(crate) async fn selection(
        &self,
        name: &str,
    ) -> Result<crate::dataset_writer::row_selection::ReadSelection> {
        self.check_current().await?;
        let directory = self
            .index_directory("selections")
            .await?
            .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
        let value = directory
            .lookup(name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Name {
            name: String,
        }
        let named: Name = serde_json::from_value(value.value)?;
        ensure!(
            named.name == name && value.links.len() == 1,
            "DATASET_KEY_INDEX_INVALID"
        );
        let selection = crate::dataset_writer::row_selection::ReadSelection::open(
            self.context.clone(),
            value.links[0].clone(),
        )
        .await?;
        ensure!(
            selection.name() == name && self.file(name).await?.content() == selection.content(),
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        self.check_current().await?;
        Ok(selection)
    }
    pub async fn lookup_key(
        &self,
        values: &[Value],
    ) -> Result<Option<crate::dataset_writer::key_run::Locator>> {
        self.check_current().await?;
        ensure!(
            !self.descriptor.primary_key.is_empty()
                && values.len() == self.descriptor.primary_key.len(),
            "DATASET_PRIMARY_KEY_INVALID"
        );
        let columns = crate::snapshot::columns(&serde_json::to_value(
            self.metadata.current_schema().as_ref(),
        )?)?;
        let mut normalized = Vec::with_capacity(values.len());
        for (name, value) in self.descriptor.primary_key.iter().zip(values) {
            let mut value = crate::values::normalize(value, &columns[name])?;
            ensure!(!value.is_null(), "DATASET_PRIMARY_KEY_NULL");
            if columns[name]["type"] == "float64" && value.as_f64() == Some(0.0) {
                value = serde_json::json!(0.0);
            }
            normalized.push(value);
        }
        let key = canonical_json(&Value::Array(normalized))?.into_bytes();
        ensure!(key.len() <= 4 * 1024 * 1024, "DATASET_PRIMARY_KEY_LIMIT");
        let Some(directory) = self.index_directory("locations").await? else {
            self.check_current().await?;
            return Ok(None);
        };
        let Some(entry) = directory
            .lookup_entry(&format!(
                "{:02x}",
                crate::dataset_writer::key_run::key_route(&key)[0]
            ))
            .await?
        else {
            self.check_current().await?;
            return Ok(None);
        };
        let result = crate::dataset_writer::key_run_set::lookup(
            &self.context,
            &self.run_cache,
            &entry,
            crate::dataset_writer::key_run::RunKind::Locations,
            &self.descriptor.key_schema_hash,
            &key,
        )
        .await?;
        self.check_current().await?;
        match result {
            None => Ok(None),
            Some(crate::dataset_writer::key_run::RunValue::Location(locator)) => Ok(Some(locator)),
            _ => anyhow::bail!("DATASET_KEY_INDEX_INVALID"),
        }
    }
    /// Each file is read once and filtered by its qualified physical row selection, avoiding unbounded delete predicates.
    pub async fn scan(
        self: Arc<Self>,
    ) -> Result<BoxStream<'static, Result<arrow_array::RecordBatch>>> {
        self.check_current().await?;
        let directory = self.index_directory("selections").await?;
        let files = directory.map_or_else(
            || {
                Box::pin(futures::stream::empty())
                    as BoxStream<
                        'static,
                        Result<(String, crate::persistent_directory::read::ReadEntry)>,
                    >
            },
            |directory| directory.entries(),
        );
        let table = Arc::new(
            iceberg::table::StaticTable::from_metadata(
                (*self.metadata).clone(),
                iceberg::TableIdent::from_strs(["moria", "qualified"])?,
                self.file_io(),
            )
            .await?,
        );
        struct Scan {
            snapshot: Arc<NativeSnapshot>,
            table: Arc<iceberg::table::StaticTable>,
            files:
                BoxStream<'static, Result<(String, crate::persistent_directory::read::ReadEntry)>>,
            batches: Option<BoxStream<'static, Result<arrow_array::RecordBatch>>>,
            selection: Option<crate::dataset_writer::row_selection::ReadSelection>,
            physical: u64,
            file_visible: u64,
            total: u64,
            files_seen: u64,
        }
        let state = Scan {
            snapshot: self,
            table,
            files,
            batches: None,
            selection: None,
            physical: 0,
            file_visible: 0,
            total: 0,
            files_seen: 0,
        };
        Ok(Box::pin(futures::stream::try_unfold(
            state,
            |mut state| async move {
                loop {
                    state.snapshot.check_current().await?;
                    if let Some(batches) = state.batches.as_mut() {
                        if let Some(batch) = batches.next().await {
                            let batch = batch?;
                            let selection = state.selection.as_ref().unwrap();
                            let selected =
                                selection.selected(state.physical, batch.num_rows()).await?;
                            state.physical += batch.num_rows() as u64;
                            let indices = selected
                                .iter()
                                .enumerate()
                                .filter_map(|(index, selected)| selected.then_some(index as u32))
                                .collect::<Vec<_>>();
                            state.file_visible += indices.len() as u64;
                            state.total += indices.len() as u64;
                            if indices.is_empty() {
                                continue;
                            }
                            let batch = if indices.len() == batch.num_rows() {
                                batch
                            } else {
                                let indices = arrow_array::UInt32Array::from(indices);
                                let arrays = batch
                                    .columns()
                                    .iter()
                                    .map(|array| {
                                        arrow_select::take::take(array.as_ref(), &indices, None)
                                    })
                                    .collect::<std::result::Result<Vec<_>, _>>()?;
                                arrow_array::RecordBatch::try_new(batch.schema(), arrays)?
                            };
                            crate::snapshot::validate_batch(
                                &batch,
                                &crate::snapshot::columns(&serde_json::to_value(
                                    state.snapshot.metadata.current_schema().as_ref(),
                                )?)?,
                            )?;
                            state.snapshot.check_current().await?;
                            return Ok(Some((batch, state)));
                        }
                        let selection = state.selection.as_ref().unwrap();
                        ensure!(
                            state.physical == selection.physical_rows()
                                && state.file_visible == selection.selected_rows(),
                            "PARQUET_COUNT_MISMATCH"
                        );
                        state.batches = None;
                        state.selection = None;
                    }
                    let Some((name, entry)) = state.files.next().await.transpose()? else {
                        ensure!(
                            state.total == state.snapshot.descriptor.rows
                                && state.files_seen == state.snapshot.descriptor.data_files,
                            "PARQUET_COUNT_MISMATCH"
                        );
                        state.snapshot.check_current().await?;
                        return Ok(None);
                    };
                    #[derive(Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Name {
                        name: String,
                    }
                    let named: Name = serde_json::from_value(entry.value.clone())?;
                    ensure!(
                        named.name == name && entry.link_count() == 1,
                        "DATASET_KEY_INDEX_INVALID"
                    );
                    let selection = crate::dataset_writer::row_selection::ReadSelection::open(
                        state.snapshot.context.clone(),
                        entry.link(0).await?,
                    )
                    .await?;
                    ensure!(
                        selection.name() == name
                            && state.snapshot.file(&name).await?.content() == selection.content(),
                        "DATASET_GRAPH_ANCESTRY_REQUIRED"
                    );
                    state.files_seen += 1;
                    if selection.selected_rows() == 0 {
                        continue;
                    }
                    let content = selection.content();
                    let task = iceberg::scan::FileScanTask::builder()
                        .with_file_size_in_bytes(content.size)
                        .with_start(0)
                        .with_length(content.size)
                        .with_record_count(Some(selection.physical_rows()))
                        .with_data_file_path(name)
                        .with_data_file_format(iceberg::spec::DataFileFormat::Parquet)
                        .with_schema(state.snapshot.metadata.current_schema().clone())
                        .with_project_field_ids(
                            state
                                .snapshot
                                .metadata
                                .current_schema()
                                .as_struct()
                                .fields()
                                .iter()
                                .map(|field| field.id)
                                .collect(),
                        )
                        .with_case_sensitive(true)
                        .build();
                    use futures::TryStreamExt;
                    let stream = state
                        .table
                        .reader_builder()
                        .with_batch_size(1000)
                        .with_data_file_concurrency_limit(1)
                        .with_range_fetch_concurrency(1)
                        .build()
                        .read(Box::pin(futures::stream::iter([Ok(task)])))?
                        .stream()
                        .map_err(anyhow::Error::from);
                    state.batches = Some(Box::pin(stream));
                    state.selection = Some(selection);
                    state.physical = 0;
                    state.file_visible = 0;
                }
            },
        )))
    }
    pub(crate) fn file_io(self: &Arc<Self>) -> FileIO {
        FileIOBuilder::new(Arc::new(GraphFiles(self.clone()))).build()
    }
}
#[derive(Clone, Debug)]
struct GraphFiles(Arc<NativeSnapshot>);
fn denied() -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::DataInvalid,
        "Qualified native snapshot IO denied",
    )
}
impl Serialize for GraphFiles {
    fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "Live Dataset graph custody cannot be serialized",
        ))
    }
}
impl<'de> Deserialize<'de> for GraphFiles {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "Live Dataset graph custody must be injected",
        ))
    }
}
#[typetag::serde(name = "moria-qualified-v3-files")]
impl StorageFactory for GraphFiles {
    fn build(&self, _: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.clone()))
    }
}
#[derive(Debug)]
struct FileHandle {
    window: tokio::sync::Mutex<crate::storage::ObjectReadWindow>,
}
#[async_trait]
impl FileRead for FileHandle {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        self.window
            .lock()
            .await
            .read(range)
            .await
            .map_err(|_| denied())
    }
}
#[typetag::serde(name = "moria-qualified-v3-files")]
#[async_trait]
impl Storage for GraphFiles {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        self.0.file(path).await.map(|_| true).map_err(|_| denied())
    }
    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        let file = self.0.file(path).await.map_err(|_| denied())?;
        Ok(FileMetadata {
            size: file.content().size,
        })
    }
    async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
        let file = self.0.file(path).await.map_err(|_| denied())?;
        if file.content().size > 8 * 1024 * 1024 {
            return Err(denied());
        }
        file.range(0..file.content().size)
            .await
            .map_err(|_| denied())
    }
    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        Ok(Box::new(FileHandle {
            window: tokio::sync::Mutex::new(
                self.0.file(path).await.map_err(|_| denied())?.window(),
            ),
        }))
    }
    fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
        if !path.starts_with("moria://") || path.len() > 4096 {
            return Err(denied());
        }
        Ok(InputFile::new(Arc::new(self.clone()), path.to_owned()))
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

#[cfg(test)]
mod index_tests {
    use super::*;
    #[test]
    fn empty_result_keeps_paired_tombstone_directories_without_inventing_live_rows() {
        let reference = NodeReference {
            content: SealedObject {
                object: crate::storage::LogicalObject::new("w", "t", "index").unwrap(),
                size: 1,
                sha256: format!("sha256:{}", "a".repeat(64)),
            },
            entries: 1,
            logical_bytes: 1,
            nodes: 1,
        };
        let mut index = VisibilityIndex {
            rows: 0,
            hashes: None,
            locations: None,
        };
        index.validate(0).unwrap();
        index.hashes = Some(reference.clone());
        assert!(index.validate(0).is_err());
        index.locations = Some(reference);
        index.validate(0).unwrap();
        assert!(index.validate(1).is_err());
        index.rows = 1;
        index.validate(1).unwrap();
        index.hashes = None;
        index.locations = None;
        assert!(index.validate(1).is_err());
        index.rows = 9_007_199_254_740_992;
        assert!(index.validate(index.rows).is_err());
    }
}
