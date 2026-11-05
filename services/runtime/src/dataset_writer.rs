//! Bounded native replace-output preparation; publication remains Catalog's atomic operation.
use crate::{
    canonical::canonical_json,
    catalog::{
        DatasetIoGrant, NativeSnapshotRef, prepare_snapshot, read_descriptor, snapshot_page,
        snapshot_root,
    },
    snapshot::{SnapshotPin, columns, validate_batch},
    storage::{
        Cancellation, ClosureEntry, ClosureLimits, ClosureSpool, LogicalObject, ObjectStore,
        ObjectWriter, PosixStore, SealedObject,
    },
    values,
};
use anyhow::{Result, ensure};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use iceberg::{
    arrow::schema_to_arrow_schema,
    io::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage},
    spec::{
        DataContentType, DataFileBuilder, DataFileFormat, ManifestListWriter,
        ManifestWriterBuilder, PartitionSpec, Schema,
    },
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Write,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

const PART_LOGICAL_BYTES: usize = 8 * 1024 * 1024;
const PART_ENCODED_BYTES: usize = 16 * 1024 * 1024;
const METADATA_BYTES: usize = 8 * 1024 * 1024;
const PAGE_BYTES: usize = 256 * 1024;
const MAX_MANIFESTS: usize = 4096;

fn supported(descriptor: &Value) -> bool {
    match descriptor["type"].as_str() {
        Some(
            "boolean" | "int64" | "float64" | "string" | "date" | "timestamp" | "decimal" | "uid",
        ) => true,
        Some("array") => {
            descriptor["items"]["nullable"] == false
                && descriptor["items"]["type"] != "array"
                && supported(&descriptor["items"])
        }
        _ => false,
    }
}
fn parquet_field(field: &Field, descriptor: &Value) -> Field {
    if descriptor["type"] == "uid" {
        return field
            .clone()
            .with_extension_type(arrow_schema::extension::Uuid);
    }
    match field.data_type() {
        DataType::List(child) => {
            field
                .clone()
                .with_data_type(DataType::List(Arc::new(parquet_field(
                    child,
                    &descriptor["items"],
                ))))
        }
        DataType::LargeList(child) => {
            field
                .clone()
                .with_data_type(DataType::LargeList(Arc::new(parquet_field(
                    child,
                    &descriptor["items"],
                ))))
        }
        _ => field.clone(),
    }
}
fn compatible_type(actual: &DataType, expected: &DataType) -> bool {
    match (actual, expected) {
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => {
            compatible_type(a.data_type(), b.data_type())
        }
        _ => actual == expected,
    }
}
/// Accepted flat Dataset Arrow schema, including canonical UUID metadata and nested field IDs.
pub(crate) fn arrow_schema(schema: &Schema) -> Result<Arc<ArrowSchema>> {
    let descriptors = columns(&serde_json::to_value(schema)?)?;
    ensure!(
        !descriptors.is_empty() && descriptors.values().all(supported),
        "UNSUPPORTED_OUTPUT_SCHEMA"
    );
    let original = schema_to_arrow_schema(schema)?;
    Ok(Arc::new(ArrowSchema::new_with_metadata(
        original
            .fields()
            .iter()
            .map(|field| parquet_field(field, &descriptors[field.name()]))
            .collect::<Vec<_>>(),
        original.metadata().clone(),
    )))
}

/// Rebind field metadata after logical validation; primitive types and list offsets stay exact.
pub(crate) fn rebind_batch(batch: RecordBatch, expected: Arc<ArrowSchema>) -> Result<RecordBatch> {
    ensure!(
        batch.num_columns() == expected.fields().len(),
        "SCHEMA_CHANGED"
    );
    for ((actual, field), array) in batch
        .schema()
        .fields()
        .iter()
        .zip(expected.fields())
        .zip(batch.columns())
    {
        ensure!(
            actual.name() == field.name()
                && compatible_type(actual.data_type(), field.data_type())
                && (field.is_nullable() || array.null_count() == 0),
            "SCHEMA_CHANGED"
        );
    }
    let arrays = batch
        .columns()
        .iter()
        .zip(expected.fields())
        .map(|(array, field)| {
            if array.data_type() == field.data_type() {
                return Ok(array.clone());
            }
            let data = array
                .to_data()
                .into_builder()
                .data_type(field.data_type().clone())
                .build()?;
            Ok(arrow_array::make_array(data))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(expected, arrays)?)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Part {
    name: String,
    content: SealedObject,
    rows: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PartPage {
    version: u32,
    ordinal: u64,
    previous: Option<SealedObject>,
    parts: Vec<Part>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PartRoot {
    version: u32,
    execution: Uuid,
    task: Uuid,
    owner: Uuid,
    fence: String,
    incarnation: Uuid,
    resource: String,
    schema_hash: String,
    #[serde(default)]
    contract: Option<WriteContract>,
    pages: u64,
    parts: u64,
    rows: u64,
    last_page: Option<SealedObject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    graph_files: Option<crate::persistent_directory::NodeReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    graph_pages: Option<crate::persistent_directory::NodeReference>,
}

pub(crate) fn schema_hash(schema: &Schema) -> Result<String> {
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(canonical_json(&serde_json::to_value(schema)?)?.as_bytes())
    ))
}
async fn prior_sequence(
    store: &dyn ObjectStore,
    grant: Arc<DatasetIoGrant>,
    schema: &Schema,
    spool: &mut ClosureSpool,
    limits: ClosureLimits,
    cancel: Cancellation,
) -> Result<Option<Value>> {
    let client = grant.database().get().await?;
    let head=client.query_one("SELECT revision,metadata_location FROM moria.dataset_heads WHERE environment=$1 AND dataset=$2",
        &[&grant.session().environment(),&grant.resource()]).await?;
    ensure!(
        head.get::<_, i64>(0) == grant.expected_revision()?,
        "HEAD_CONFLICT"
    );
    let location: Option<String> = head.get(1);
    drop(client);
    let Some(location) = location else {
        return Ok(None);
    };
    let reference = NativeSnapshotRef::decode(&location)?;
    let root = snapshot_root(store, grant.clone(), &reference, cancel.clone()).await?;
    ensure!(
        root.proof.metadata.size <= METADATA_BYTES as u64
            && root.proof.closure.entries <= limits.max_entries
            && root.proof.closure.referenced_bytes <= limits.max_referenced_bytes
            && root.pages <= limits.max_entries,
        "RESOURCE_LIMIT"
    );
    let mut stream = store
        .read_object(
            root.proof.metadata.object.clone(),
            grant.clone(),
            cancel.clone(),
        )
        .await?;
    let mut bytes = Vec::new();
    let mut hash = Sha256::new();
    while let Some(frame) = stream.next().await {
        let frame = frame?;
        ensure!(
            bytes
                .len()
                .checked_add(frame.len())
                .is_some_and(|n| n <= root.proof.metadata.size as usize),
            "CONTENT_MISMATCH"
        );
        hash.update(&frame);
        bytes.extend_from_slice(&frame);
    }
    ensure!(
        bytes.len() as u64 == root.proof.metadata.size
            && format!("sha256:{:x}", hash.finalize()) == root.proof.metadata.sha256,
        "CONTENT_MISMATCH"
    );
    let metadata: iceberg::spec::TableMetadata = serde_json::from_slice(&bytes)?;
    ensure!(
        metadata.uuid() == grant.table_uuid()? && metadata.current_schema().as_ref() == schema,
        "OUTPUT_SCHEMA_CHANGED"
    );
    let mut raw: Value = serde_json::from_slice(&bytes)?;
    let mut current = root.last_page;
    let mut count = 0;
    for ordinal in (0..root.pages).rev() {
        let page = snapshot_page(
            store,
            grant.clone(),
            &current
                .take()
                .ok_or_else(|| anyhow::anyhow!("INVALID_NATIVE_SNAPSHOT_ROOT"))?,
            ordinal,
            cancel.clone(),
        )
        .await?;
        count += page.entries.len() as u64;
        spool.append_page(&page.entries).await?;
        current = page.previous;
    }
    ensure!(
        current.is_none() && count == root.proof.closure.entries,
        "INVALID_NATIVE_SNAPSHOT_ROOT"
    );
    let name = logical(&grant, "metadata");
    spool
        .append_page(&[ClosureEntry {
            name: name.clone(),
            content: root.proof.metadata,
        }])
        .await?;
    let previous = json!({"timestamp-ms":raw["last-updated-ms"],"metadata-file":name});
    raw.as_object_mut()
        .unwrap()
        .entry("metadata-log")
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?
        .push(previous);
    Ok(Some(raw))
}
fn logical(grant: &DatasetIoGrant, kind: &str) -> String {
    format!(
        "moria://{}/{}/{}/{}",
        grant.namespace().warehouse_id,
        grant.namespace().table_id,
        kind,
        Uuid::new_v4()
    )
}
fn object(grant: &DatasetIoGrant) -> Result<LogicalObject> {
    LogicalObject::new(
        &grant.namespace().warehouse_id,
        &grant.namespace().table_id,
        &Uuid::new_v4().to_string(),
    )
}
async fn put(
    store: &dyn ObjectWriter,
    grant: Arc<DatasetIoGrant>,
    bytes: &[u8],
    cancel: Cancellation,
) -> Result<SealedObject> {
    let mut write = store
        .begin_object(
            object(&grant)?,
            bytes.len() as u64,
            format!("sha256:{:x}", Sha256::digest(bytes)),
            grant,
            cancel,
        )
        .await?;
    for chunk in bytes.chunks(1024 * 1024) {
        write.append(chunk).await?;
    }
    write.seal().await
}
async fn put_json<T: Serialize>(
    store: &dyn ObjectWriter,
    grant: Arc<DatasetIoGrant>,
    value: &T,
    max: usize,
    cancel: Cancellation,
) -> Result<SealedObject> {
    let bytes = canonical_json(&serde_json::to_value(value)?)?.into_bytes();
    ensure!(bytes.len() <= max, "RESOURCE_LIMIT");
    put(store, grant, &bytes, cancel).await
}

#[derive(Default)]
struct PartBuffer(Vec<u8>);
impl Write for PartBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .0
            .len()
            .checked_add(bytes.len())
            .is_none_or(|size| size > PART_ENCODED_BYTES)
        {
            return Err(std::io::Error::other("RESOURCE_LIMIT"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// One leased task's private immutable output. No catalog head changes during append/finish.
pub struct StreamingDatasetWriter {
    store: Arc<dyn ObjectStore>,
    grant: Arc<DatasetIoGrant>,
    cancel: Cancellation,
    schema: Schema,
    contract: WriteContract,
    arrow: Arc<ArrowSchema>,
    descriptors: BTreeMap<String, Value>,
    writer: Option<ArrowWriter<PartBuffer>>,
    part_bytes: usize,
    part_rows: u64,
    page: Vec<Part>,
    last_page: Option<SealedObject>,
    pages: u64,
    parts: u64,
    rows: u64,
    failed: bool,
    graph: Option<graph_output::GraphOutput>,
}
impl StreamingDatasetWriter {
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        grant: Arc<DatasetIoGrant>,
        schema: Schema,
        cancel: Cancellation,
    ) -> Result<Self> {
        ensure!(grant.lease().task.is_some(), "TASK_REQUIRED");
        grant.check_current().await?;
        grant.validate_store(store.as_ref())?;
        let contract = grant.write_contract(&schema).await?;
        ensure!(
            contract.mode == "replace",
            "DATASET_WRITE_CAPABILITY_REQUIRED"
        );
        grant.validate_schema(&schema)?;
        let raw = serde_json::to_value(&schema)?;
        let descriptors = columns(&raw)?;
        ensure!(!descriptors.is_empty(), "UNSUPPORTED_SCHEMA");
        for descriptor in descriptors.values() {
            ensure!(supported(descriptor), "UNSUPPORTED_OUTPUT_SCHEMA");
        }
        let arrow = arrow_schema(&schema)?;
        Ok(Self {
            store,
            grant,
            cancel,
            schema,
            contract,
            arrow,
            descriptors,
            writer: None,
            part_bytes: 0,
            part_rows: 0,
            page: Vec::new(),
            last_page: None,
            pages: 0,
            parts: 0,
            rows: 0,
            failed: false,
            graph: None,
        })
    }
    /// Native service entry for staged v3 output. Publication requires its separate graph candidate validator.
    pub async fn open_graph(
        store: Arc<dyn ObjectStore>,
        grant: Arc<DatasetIoGrant>,
        schema: Schema,
        cancel: Cancellation,
    ) -> Result<Self> {
        let graph =
            graph_output::GraphOutput::open(store.clone(), grant.clone(), cancel.clone()).await?;
        let mut writer = Self::open(store, grant, schema, cancel).await?;
        writer.graph = Some(graph);
        Ok(writer)
    }
    fn begin_part(&mut self) -> Result<()> {
        if self.writer.is_none() {
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(1000))
                .set_dictionary_enabled(false)
                .build();
            self.writer = Some(ArrowWriter::try_new(
                PartBuffer::default(),
                self.arrow.clone(),
                Some(props),
            )?);
        }
        Ok(())
    }
    pub async fn append(&mut self, batch: RecordBatch) -> Result<()> {
        ensure!(!self.failed, "WRITER_FAILED");
        self.failed = true;
        self.cancel.check()?;
        self.grant.check_current().await?;
        ensure!(
            batch.num_columns() == self.arrow.fields().len(),
            "SCHEMA_CHANGED"
        );
        for (actual, expected) in batch.schema().fields().iter().zip(self.arrow.fields()) {
            ensure!(
                actual.name() == expected.name()
                    && compatible_type(actual.data_type(), expected.data_type()),
                "SCHEMA_CHANGED"
            );
        }
        validate_batch(&batch, &self.descriptors)?;
        let batch = rebind_batch(batch, self.arrow.clone())?;
        let mut start = 0;
        for row in 0..batch.num_rows() {
            let mut bytes = 0;
            for (name, descriptor) in &self.descriptors {
                bytes += serde_json::to_vec(&values::arrow_value(
                    batch.column_by_name(name).unwrap().as_ref(),
                    row,
                    descriptor,
                )?)?
                .len();
            }
            ensure!(bytes <= 4 * 1024 * 1024, "DECODED_ROW_LIMIT");
            if self.part_bytes + bytes > PART_LOGICAL_BYTES {
                if row > start {
                    self.begin_part()?;
                    self.writer
                        .as_mut()
                        .unwrap()
                        .write(&batch.slice(start, row - start))?;
                }
                self.flush_part().await?;
                start = row;
            }
            self.part_bytes += bytes;
            self.part_rows += 1;
        }
        if start < batch.num_rows() {
            self.begin_part()?;
            self.writer
                .as_mut()
                .unwrap()
                .write(&batch.slice(start, batch.num_rows() - start))?;
        }
        self.grant.check_current().await?;
        self.failed = false;
        Ok(())
    }
    async fn flush_part(&mut self) -> Result<()> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };
        let bytes = writer.into_inner()?.0;
        let content = put(
            self.store.as_ref(),
            self.grant.clone(),
            &bytes,
            self.cancel.clone(),
        )
        .await?;
        let name = logical(&self.grant, "data");
        if let Some(graph) = self.graph.as_mut() {
            graph
                .file(
                    &name,
                    &content,
                    self.store.clone(),
                    self.grant.clone(),
                    self.cancel.clone(),
                )
                .await?;
        }
        self.page.push(Part {
            name,
            content,
            rows: self.part_rows,
        });
        self.parts = self
            .parts
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        self.rows = self
            .rows
            .checked_add(self.part_rows)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        ensure!(
            self.parts <= 1_048_576 && self.rows <= 9_007_199_254_740_991,
            "RESOURCE_LIMIT"
        );
        self.part_bytes = 0;
        self.part_rows = 0;
        if self.page.len() == 128 {
            self.flush_page().await?;
        }
        Ok(())
    }
    async fn flush_page(&mut self) -> Result<()> {
        if self.page.is_empty() {
            return Ok(());
        }
        let page = PartPage {
            version: 1,
            ordinal: self.pages,
            previous: self.last_page.take(),
            parts: std::mem::take(&mut self.page),
        };
        self.last_page = Some(if let Some(graph) = self.graph.as_mut() {
            graph.page(&page).await?
        } else {
            put_json(
                self.store.as_ref(),
                self.grant.clone(),
                &page,
                PAGE_BYTES,
                self.cancel.clone(),
            )
            .await?
        });
        self.pages += 1;
        Ok(())
    }
    pub async fn finish(mut self) -> Result<SealedObject> {
        ensure!(!self.failed, "WRITER_FAILED");
        self.flush_part().await?;
        self.flush_page().await?;
        let lease = self.grant.lease();
        let root = PartRoot {
            version: if self.graph.is_some() { 3 } else { 2 },
            execution: lease.execution,
            task: lease.task.unwrap(),
            owner: lease.owner,
            fence: lease.fence.clone(),
            incarnation: lease.incarnation,
            resource: self.grant.resource().into(),
            schema_hash: schema_hash(&self.schema)?,
            contract: Some(self.contract.clone()),
            pages: self.pages,
            parts: self.parts,
            rows: self.rows,
            last_page: self.last_page,
            graph_files: self
                .graph
                .as_ref()
                .and_then(graph_output::GraphOutput::files_root),
            graph_pages: self
                .graph
                .as_ref()
                .and_then(graph_output::GraphOutput::pages_root),
        };
        if let Some(graph) = self.graph {
            graph.finish(&root).await
        } else {
            put_json(self.store.as_ref(), self.grant, &root, 16384, self.cancel).await
        }
    }
}

#[derive(Clone, Debug)]
struct Capture(Arc<Mutex<Option<Vec<u8>>>>);
impl Serialize for Capture {
    fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("Private writer buffer"))
    }
}
impl<'de> Deserialize<'de> for Capture {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        Err(serde::de::Error::custom("Private writer buffer"))
    }
}
fn denied() -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::DataInvalid,
        "Private manifest output only",
    )
}
#[async_trait]
impl FileWrite for Capture {
    async fn write(&mut self, bytes: Bytes) -> iceberg::Result<()> {
        let mut state = self.0.lock().map_err(|_| denied())?;
        let output = state.as_mut().ok_or_else(denied)?;
        if output
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > METADATA_BYTES)
        {
            return Err(denied());
        }
        output.extend_from_slice(&bytes);
        Ok(())
    }
    async fn close(&mut self) -> iceberg::Result<()> {
        Ok(())
    }
}
#[typetag::serde(name = "moria-private-manifest-output-v1")]
#[async_trait]
impl Storage for Capture {
    async fn exists(&self, _: &str) -> iceberg::Result<bool> {
        Err(denied())
    }
    async fn metadata(&self, _: &str) -> iceberg::Result<FileMetadata> {
        Err(denied())
    }
    async fn read(&self, _: &str) -> iceberg::Result<Bytes> {
        Err(denied())
    }
    async fn reader(&self, _: &str) -> iceberg::Result<Box<dyn FileRead>> {
        Err(denied())
    }
    fn new_input(&self, _: &str) -> iceberg::Result<InputFile> {
        Err(denied())
    }
    async fn write(&self, _: &str, _: Bytes) -> iceberg::Result<()> {
        Err(denied())
    }
    async fn writer(&self, _: &str) -> iceberg::Result<Box<dyn FileWrite>> {
        Ok(Box::new(self.clone()))
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
    fn new_output(&self, path: &str) -> iceberg::Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.into()))
    }
}
impl Capture {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Some(Vec::new()))))
    }
    fn take(&self) -> Result<Vec<u8>> {
        self.0
            .lock()
            .map_err(|_| anyhow::anyhow!("WRITER_FAILED"))?
            .take()
            .ok_or_else(|| anyhow::anyhow!("WRITER_CLOSED"))
    }
}

/// A coordinator consumes only part sets stored by this execution's sealed, fenced tasks.
pub async fn prepare_replacement(
    store: Arc<dyn ObjectStore>,
    scratch: &PosixStore,
    grant: Arc<DatasetIoGrant>,
    schema: Schema,
    partsets: &[SealedObject],
    limits: ClosureLimits,
    cancel: Cancellation,
) -> Result<Uuid> {
    prepare_replacement_inner(
        store, scratch, grant, schema, partsets, limits, cancel, false,
    )
    .await
}
/// Staged native replacement; only actual version-three sealed task graph receipts are accepted.
pub async fn prepare_graph_replacement(
    store: Arc<dyn ObjectStore>,
    scratch: &PosixStore,
    grant: Arc<DatasetIoGrant>,
    schema: Schema,
    partsets: &[SealedObject],
    limits: ClosureLimits,
    cancel: Cancellation,
) -> Result<Uuid> {
    prepare_replacement_inner(
        store, scratch, grant, schema, partsets, limits, cancel, true,
    )
    .await
}
async fn prepare_replacement_inner(
    store: Arc<dyn ObjectStore>,
    scratch: &PosixStore,
    grant: Arc<DatasetIoGrant>,
    schema: Schema,
    partsets: &[SealedObject],
    limits: ClosureLimits,
    cancel: Cancellation,
    graph_format: bool,
) -> Result<Uuid> {
    ensure!(
        grant.lease().task.is_none() && !partsets.is_empty() && partsets.len() <= 4096,
        "OUTPUT_INVALID"
    );
    let contract = grant.write_contract(&schema).await?;
    ensure!(
        contract.mode == "replace",
        "DATASET_WRITE_CAPABILITY_REQUIRED"
    );
    let mut spool = ClosureSpool::create(
        scratch,
        object(&grant)?,
        limits,
        grant.clone(),
        cancel.clone(),
    )
    .await?;
    if graph_format {
        spool.seal(store.as_ref(), grant.clone()).await?;
    }
    let schema = Arc::new(schema);
    let (prior, prior_files) = if graph_format {
        let basis = replace_graph::basis(
            store.clone(),
            grant.clone(),
            schema.as_ref(),
            cancel.clone(),
        )
        .await?;
        (basis.raw, basis.files)
    } else {
        (
            prior_sequence(
                store.as_ref(),
                grant.clone(),
                schema.as_ref(),
                &mut spool,
                limits,
                cancel.clone(),
            )
            .await?,
            None,
        )
    };
    let sequence = prior
        .as_ref()
        .map(|raw| {
            raw["last-sequence-number"]
                .as_i64()
                .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))
        })
        .transpose()?
        .unwrap_or(0)
        .checked_add(1)
        .filter(|n| *n <= 9_007_199_254_740_991)
        .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
    let spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(0)
        .build()?;
    let hash = schema_hash(&schema)?;
    let snapshot_id = (Uuid::new_v4().as_u128() as u64 & ((1_u64 << 52) - 1)).max(1) as i64;
    let mut graph = if graph_format {
        Some(
            replace_graph::ReplacementGraph::open(
                store.clone(),
                grant.clone(),
                schema.clone(),
                spool.clone(),
                u64::try_from(sequence)?,
                prior_files,
                cancel.clone(),
            )
            .await?,
        )
    } else {
        None
    };
    let list_name = logical(&grant, "metadata");
    let list_capture = Capture::new();
    let parent_snapshot = prior
        .as_ref()
        .and_then(|raw| raw["current-snapshot-id"].as_i64());
    let mut list = ManifestListWriter::v2(
        Box::new(list_capture.clone()),
        snapshot_id,
        parent_snapshot,
        sequence,
    );
    let mut manifests = 0usize;
    let mut total_rows = 0u64;
    let mut total_parts = 0u64;
    let mut seen = std::collections::BTreeSet::new();
    for reference in partsets {
        let root: PartRoot = if let Some(graph) = graph.as_ref() {
            graph.task_root(reference).await?
        } else {
            read_descriptor(
                store.as_ref(),
                grant.clone(),
                reference,
                16384,
                cancel.clone(),
            )
            .await?
        };
        ensure!(
            ((graph_format && root.version == 3 && root.contract.as_ref() == Some(&contract))
                || (!graph_format
                    && root.graph_files.is_none()
                    && root.graph_pages.is_none()
                    && ((root.version == 2 && root.contract.as_ref() == Some(&contract))
                        || (root.version == 1
                            && root.contract.is_none()
                            && contract.mode == "replace"
                            && contract.coverage == "full"))))
                && root.execution == grant.lease().execution
                && root.resource == grant.resource()
                && root.schema_hash == hash
                && root.incarnation == grant.lease().incarnation
                && root.pages <= 8192
                && root.parts <= 1_048_576
                && seen.insert(root.task),
            "OUTPUT_INVALID"
        );
        let client = grant.database().get().await?;
        let receipt = serde_json::to_value(reference)?;
        let accepted=client.query_opt("SELECT 1 FROM moria.runtime_execution_tasks WHERE id=$1 AND execution=$2 AND status='sealed' AND owner=$3 AND fence=$4 AND result->'datasetOutputs'->$5=$6",
            &[&root.task,&root.execution,&root.owner,&root.fence.parse::<i64>()?,&root.resource,&receipt]).await?;
        ensure!(accepted.is_some(), "OUTPUT_ATTEMPT_CHANGED");
        drop(client);
        let mut page_ref = root.last_page.clone();
        let mut parts = 0;
        let mut rows = 0;
        for ordinal in (0..root.pages).rev() {
            let reference = page_ref
                .take()
                .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?;
            let page: PartPage = if let Some(graph) = graph.as_ref() {
                graph.task_page(&root, ordinal, &reference).await?
            } else {
                read_descriptor(
                    store.as_ref(),
                    grant.clone(),
                    &reference,
                    PAGE_BYTES,
                    cancel.clone(),
                )
                .await?
            };
            ensure!(
                page.version == 1
                    && page.ordinal == ordinal
                    && !page.parts.is_empty()
                    && page.parts.len() <= 128,
                "OUTPUT_INVALID"
            );
            let manifest_name = logical(&grant, "metadata");
            let capture = Capture::new();
            let output = OutputFile::new(Arc::new(capture.clone()), manifest_name.clone());
            let mut writer =
                ManifestWriterBuilder::new(output, Some(snapshot_id), schema.clone(), spec.clone())
                    .build_v2_data();
            let mut entries = Vec::new();
            for part in page.parts {
                ensure!(
                    part.content.size <= PART_ENCODED_BYTES as u64
                        && part.rows <= 9_007_199_254_740_991,
                    "OUTPUT_INVALID"
                );
                if let Some(graph) = graph.as_mut() {
                    graph.task_file(&root, &part).await?;
                }
                let file = DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(part.name.clone())
                    .file_format(DataFileFormat::Parquet)
                    .record_count(part.rows)
                    .file_size_in_bytes(part.content.size)
                    .build()?;
                writer.add_file(file, sequence)?;
                parts += 1;
                rows += part.rows;
                entries.push(ClosureEntry {
                    name: part.name,
                    content: part.content,
                });
            }
            if !graph_format {
                spool.append_page(&entries).await?;
            }
            let manifest = writer.write_manifest_file().await?;
            let content = put(
                store.as_ref(),
                grant.clone(),
                &capture.take()?,
                cancel.clone(),
            )
            .await?;
            if let Some(graph) = graph.as_mut() {
                graph.metadata_file(&manifest_name, &content).await?;
            }
            if !graph_format {
                spool
                    .append_page(&[ClosureEntry {
                        name: manifest_name,
                        content,
                    }])
                    .await?;
            }
            manifests += 1;
            ensure!(manifests <= MAX_MANIFESTS, "RESOURCE_LIMIT");
            list.add_manifests(std::iter::once(manifest))?;
            page_ref = page.previous;
        }
        ensure!(
            page_ref.is_none() && parts == root.parts && rows == root.rows,
            "OUTPUT_INVALID"
        );
        total_rows = total_rows
            .checked_add(rows)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        total_parts = total_parts
            .checked_add(parts)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        ensure!(total_rows <= 9_007_199_254_740_991, "RESOURCE_LIMIT");
    }
    list.close().await?;
    let list_content = put(
        store.as_ref(),
        grant.clone(),
        &list_capture.take()?,
        cancel.clone(),
    )
    .await?;
    if let Some(graph) = graph.as_mut() {
        graph.metadata_file(&list_name, &list_content).await?;
    }
    if !graph_format {
        spool
            .append_page(&[ClosureEntry {
                name: list_name.clone(),
                content: list_content,
            }])
            .await?;
    }
    let now = chrono::Utc::now().timestamp_millis().max(
        prior
            .as_ref()
            .and_then(|v| v["last-updated-ms"].as_i64())
            .unwrap_or(0),
    );
    let mut metadata = json!({"format-version":2,"table-uuid":grant.table_uuid()?.to_string(),
        "location":format!("moria://{}/{}",grant.namespace().warehouse_id,grant.namespace().table_id),
        "last-sequence-number":sequence,"last-updated-ms":now,"last-column-id":schema.highest_field_id(),
        "schemas":[serde_json::to_value(schema.as_ref())?],"current-schema-id":schema.schema_id(),
        "partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,"last-partition-id":999,
        "properties":{},"current-snapshot-id":snapshot_id,
        "snapshots":[{"snapshot-id":snapshot_id,"sequence-number":sequence,"timestamp-ms":now,"manifest-list":list_name,
            "summary":{"operation":"overwrite","total-records":total_rows.to_string(),"total-data-files":total_parts.to_string()},"schema-id":schema.schema_id()}],
        "snapshot-log":[{"timestamp-ms":now,"snapshot-id":snapshot_id}],"metadata-log":[],
        "sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0,
        "refs":{"main":{"snapshot-id":snapshot_id,"type":"branch"}}});
    if let Some(mut previous) = prior {
        metadata["snapshots"][0]["parent-snapshot-id"] =
            json!(parent_snapshot.ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?);
        for field in ["snapshots", "snapshot-log"] {
            let added = metadata[field].as_array().unwrap().clone();
            previous[field]
                .as_array_mut()
                .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?
                .extend(added);
        }
        for field in [
            "last-updated-ms",
            "last-sequence-number",
            "current-snapshot-id",
        ] {
            previous[field] = metadata[field].clone();
        }
        previous["refs"]["main"] = metadata["refs"]["main"].clone();
        metadata = previous;
    }
    let graph_index = if let Some(graph) = graph.as_mut() {
        Some(
            graph
                .summary(&mut metadata, snapshot_id, total_rows, total_parts)
                .await?,
        )
    } else {
        None
    };
    // Iceberg metadata uses exact signed64 JSON numbers, independently of the native C03 wire.
    let bytes = serde_json::to_vec(&metadata)?;
    ensure!(bytes.len() <= METADATA_BYTES, "RESOURCE_LIMIT");
    let metadata = put(store.as_ref(), grant.clone(), &bytes, cancel.clone()).await?;
    if let Some(mut graph) = graph {
        let name = logical(&grant, "metadata");
        graph.metadata_file(&name, &metadata).await?;
        return graph
            .finish(
                metadata,
                name,
                snapshot_id,
                graph_index.ok_or_else(|| anyhow::anyhow!("NATIVE_INDEX_SUMMARY_REQUIRED"))?,
            )
            .await;
    }
    spool.seal(store.as_ref(), grant.clone()).await?;
    prepare_snapshot(
        store,
        grant.clone(),
        &spool,
        SnapshotPin {
            metadata,
            table_uuid: grant.table_uuid()?,
            snapshot_id,
        },
        cancel,
    )
    .await
}

mod keys;
pub(crate) use keys::{encoded_json_key, encoded_key, validate_primary_keys};

mod contract;
pub use contract::{ExpectedHead, WriteContract};

/// Prepare only the semantics derived from the actual accepted job. Unsupported layouts fail before IO.
pub async fn prepare_output(
    store: Arc<dyn ObjectStore>,
    scratch: &PosixStore,
    grant: Arc<DatasetIoGrant>,
    schema: Schema,
    partsets: &[SealedObject],
    limits: ClosureLimits,
    cancel: Cancellation,
) -> Result<Uuid> {
    let contract = grant.write_contract(&schema).await?;
    ensure!(
        contract.mode == "replace",
        "DATASET_WRITE_CAPABILITY_REQUIRED"
    );
    prepare_replacement(store, scratch, grant, schema, partsets, limits, cancel).await
}

pub mod bootstrap_v2;
mod graph_builder;
mod graph_output;
pub mod key_run;
pub mod key_run_io;
pub mod key_run_merge;
pub mod key_run_set;
mod replace_graph;
pub mod row_selection;
pub mod rows;
pub mod visibility;
