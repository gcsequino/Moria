use crate::{
    authority::{Authority, VerifiedSession},
    catalog::{DatasetIoGrant, PinnedDatasetRead, read_pinned_dataset},
    database::Database,
    dataset_writer::StreamingDatasetWriter,
    engine_ipc,
    execution::Lease,
    storage::{Cancellation, IndexNamespace, SealedObject, configured_store_with_fingerprint},
};
use anyhow::{Result, ensure};
use bytes::Bytes;
use futures::TryStreamExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use uuid::Uuid;

pub(crate) struct EngineOutput {
    pub resource: String,
    pub namespace: IndexNamespace,
    pub schema: iceberg::spec::Schema,
}
pub(crate) struct EngineIoLimits {
    pub input_bytes: u64,
    pub output_bytes: u64,
}
pub(crate) struct EngineInput {
    pub scan: Uuid,
    pub schema: Vec<u8>,
}
struct Input {
    scan: Uuid,
    _retained: PinnedDatasetRead,
    stream: futures::stream::BoxStream<'static, Result<arrow_array::RecordBatch>>,
    schema: arrow_schema::SchemaRef,
    schema_prefix: Vec<u8>,
    fields: Vec<engine_ipc::Field>,
    pending: Bytes,
    held: Option<engine_ipc::CompactedBatch>,
    ended: bool,
}
struct Output {
    name: String,
    writer: StreamingDatasetWriter,
    schema: arrow_schema::SchemaRef,
    schema_prefix: Vec<u8>,
    fields: Option<Vec<engine_ipc::Field>>,
    pending: Vec<u8>,
    ended: bool,
}
pub(crate) struct NativeEngineIo {
    db: Database,
    session: VerifiedSession,
    lease: Lease,
    inputs: BTreeMap<String, String>,
    outputs: BTreeMap<String, EngineOutput>,
    limits: EngineIoLimits,
    cancel: Cancellation,
    opened: BTreeMap<String, Input>,
    writer: Option<Output>,
    sealed: BTreeMap<String, SealedObject>,
    input_bytes: u64,
    output_bytes: u64,
    input_rows_encoded: u64,
    output_rows_appended: u64,
    markings: Vec<String>,
    failed: bool,
}
impl NativeEngineIo {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        db: Database,
        session: VerifiedSession,
        lease: Lease,
        inputs: BTreeMap<String, String>,
        outputs: BTreeMap<String, EngineOutput>,
        limits: EngineIoLimits,
        cancel: Cancellation,
    ) -> Result<Self> {
        ensure!(
            inputs.len() <= 64
                && !outputs.is_empty()
                && outputs.len() <= 64
                && lease.task.is_some()
                && limits.input_bytes > 0
                && limits.output_bytes > 0
                && limits.input_bytes <= 1 << 50
                && limits.output_bytes <= 1 << 50,
            "ENGINE_IO_INVALID"
        );
        let names = inputs.keys().chain(outputs.keys());
        ensure!(
            names
                .clone()
                .all(|name| !name.is_empty() && name.len() <= 4096 && !name.contains('\0')),
            "ENGINE_IO_INVALID"
        );
        let mut resources = BTreeSet::new();
        for output in outputs.values() {
            ensure!(
                Uuid::parse_str(&output.resource).is_ok()
                    && resources.insert(output.resource.clone()),
                "ENGINE_IO_INVALID"
            );
        }
        let mut result = Self {
            db,
            session,
            lease,
            inputs,
            outputs,
            limits,
            cancel,
            opened: BTreeMap::new(),
            writer: None,
            sealed: BTreeMap::new(),
            input_bytes: 0,
            output_bytes: 0,
            input_rows_encoded: 0,
            output_rows_appended: 0,
            markings: vec![],
            failed: false,
        };
        result.check().await?;
        let client = result.db.get().await?;
        let mut labels = BTreeSet::new();
        for resource in result.inputs.values() {
            ensure!(Uuid::parse_str(resource).is_ok(), "ENGINE_IO_INVALID");
            let value: Vec<String> = client
                .query_one(
                    "SELECT moria.runtime_dataset_labels($1,$2)",
                    &[&result.session.session_hash(), resource],
                )
                .await?
                .get(0);
            labels.extend(value);
        }
        result.markings = labels.into_iter().collect();
        drop(client);
        result.check().await?;
        Ok(result)
    }
    pub(crate) fn consumed_markings(&self) -> Vec<String> {
        self.markings.clone()
    }
    pub(crate) fn metrics(&self) -> serde_json::Value {
        serde_json::json!({"inputBytes":self.input_bytes.to_string(),"outputBytes":self.output_bytes.to_string(),
            "inputRowsEncoded":self.input_rows_encoded.to_string(),"outputRowsAppended":self.output_rows_appended.to_string()})
    }
    async fn check(&mut self) -> Result<()> {
        self.cancel.check()?;
        ensure!(!self.failed, "WRITER_FAILED");
        let client = self.db.get().await?;
        Authority::current(&**client, &self.session).await?;
        client
            .query_one(
                "SELECT moria.runtime_task_check($1,$2,$3)",
                &[
                    &self.lease.task,
                    &self.lease.owner,
                    &self.lease.fence.parse::<i64>()?,
                ],
            )
            .await?;
        self.cancel.check()?;
        Ok(())
    }
    pub(crate) async fn open_input(&mut self, name: &str) -> Result<EngineInput> {
        self.check().await?;
        ensure!(!self.opened.contains_key(name), "ENGINE_INPUT_ALREADY_OPEN");
        let resource = self
            .inputs
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("UNDECLARED_DATASET"))?;
        let loaded = read_pinned_dataset(
            self.db.clone(),
            self.session.clone(),
            self.lease.clone(),
            resource,
            self.cancel.clone(),
        )
        .await?;
        ensure!(
            loaded
                .consumed_markings
                .iter()
                .all(|m| self.markings.contains(m)),
            "AUTHORIZATION_CHANGED"
        );
        let schema = crate::dataset_writer::arrow_schema(loaded.snapshot.schema().await?.as_ref())?;
        let prefix = engine_ipc::encode_schema(&schema)?;
        let stream = loaded.snapshot.scan(1).await?;
        let id = Uuid::new_v4();
        self.check().await?;
        self.opened.insert(
            name.into(),
            Input {
                scan: id,
                _retained: loaded,
                stream,
                fields: engine_ipc::schema(&prefix, &schema)?,
                schema,
                schema_prefix: prefix.clone(),
                pending: Bytes::copy_from_slice(&prefix),
                held: None,
                ended: false,
            },
        );
        let mut complete = prefix;
        complete.extend(engine_ipc::EOS);
        Ok(EngineInput {
            scan: id,
            schema: complete,
        })
    }
    pub(crate) async fn next_input(&mut self, name: &str, scan: Uuid) -> Result<Option<Vec<u8>>> {
        self.check().await?;
        let state = self
            .opened
            .get_mut(name)
            .ok_or_else(|| anyhow::anyhow!("ENGINE_SCAN_INVALID"))?;
        ensure!(state.scan == scan, "ENGINE_SCAN_INVALID");
        if state.pending.is_empty() && !state.ended {
            let mut batches = Vec::new();
            let mut memory = 0usize;
            let mut rows = 0usize;
            loop {
                self.cancel.check()?;
                let compact = if let Some(batch) = state.held.take() {
                    batch
                } else {
                    let Some(batch) = state.stream.try_next().await? else {
                        state.ended = true;
                        break;
                    };
                    ensure!(
                        batch.num_rows() <= 1 && batch.get_array_memory_size() <= 64 * 1024 * 1024,
                        "DECODED_BATCH_LIMIT"
                    );
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let batch = crate::dataset_writer::rebind_batch(batch, state.schema.clone())?;
                    engine_ipc::compact_batch(&state.schema_prefix, &state.fields, &batch)?
                };
                let size = compact.retained_bytes;
                if !batches.is_empty()
                    && (memory + size > 512 * 1024 || rows + compact.batch.num_rows() > 1000)
                {
                    state.held = Some(compact);
                    break;
                }
                memory += size;
                rows += compact.batch.num_rows();
                batches.push(compact.batch);
                if memory >= 512 * 1024 || rows == 1000 {
                    break;
                }
            }
            let mut encoded = if batches.is_empty() {
                Vec::new()
            } else {
                let batch = if batches.len() == 1 {
                    batches.pop().unwrap()
                } else {
                    arrow_select::concat::concat_batches(&state.schema, batches.iter())?
                };
                let encoded = engine_ipc::encode_batch(&batch)?;
                self.input_rows_encoded += batch.num_rows() as u64;
                encoded
            };
            if state.ended {
                encoded.extend(engine_ipc::EOS);
            }
            state.pending = Bytes::from(encoded);
        }
        if state.pending.is_empty() {
            return Ok(None);
        }
        let length = state.pending.len().min(engine_ipc::CHUNK_BYTES);
        let total = self
            .input_bytes
            .checked_add(length as u64)
            .ok_or_else(|| anyhow::anyhow!("ENGINE_INPUT_LIMIT"))?;
        ensure!(total <= self.limits.input_bytes, "ENGINE_INPUT_LIMIT");
        let chunk = state.pending.split_to(length).to_vec();
        self.input_bytes = total;
        self.check().await?;
        Ok(Some(chunk))
    }
    pub(crate) async fn open_output(&mut self, name: &str) -> Result<Vec<u8>> {
        self.check().await?;
        ensure!(
            self.writer.is_none() && !self.sealed.contains_key(name),
            "ENGINE_OUTPUT_ALREADY_OPEN"
        );
        let output = self
            .outputs
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("UNDECLARED_DATASET"))?;
        let schema = crate::dataset_writer::arrow_schema(&output.schema)?;
        let prefix = engine_ipc::encode_schema(&schema)?;
        let grant = Arc::new(
            DatasetIoGrant::acquire(
                self.db.clone(),
                self.session.clone(),
                self.lease.clone(),
                output.namespace.clone(),
            )
            .await?,
        );
        ensure!(grant.resource() == output.resource, "UNDECLARED_DATASET");
        let (store, _) = configured_store_with_fingerprint(&output.namespace)?;
        let writer =
            StreamingDatasetWriter::open(store, grant, output.schema.clone(), self.cancel.clone())
                .await?;
        self.check().await?;
        self.writer = Some(Output {
            name: name.into(),
            writer,
            schema,
            schema_prefix: prefix.clone(),
            fields: None,
            pending: Vec::new(),
            ended: false,
        });
        let mut complete = prefix;
        complete.extend(engine_ipc::EOS);
        Ok(complete)
    }
    pub(crate) async fn append_output(&mut self, name: &str, bytes: &[u8]) -> Result<()> {
        let result = self.append_output_inner(name, bytes).await;
        if result.is_err() {
            self.failed = true;
            self.writer = None;
        }
        result
    }
    async fn append_output_inner(&mut self, name: &str, bytes: &[u8]) -> Result<()> {
        self.check().await?;
        ensure!(
            !bytes.is_empty() && bytes.len() <= engine_ipc::CHUNK_BYTES,
            "ENGINE_FRAME_LIMIT"
        );
        let total = self
            .output_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| anyhow::anyhow!("ENGINE_OUTPUT_LIMIT"))?;
        ensure!(total <= self.limits.output_bytes, "ENGINE_OUTPUT_LIMIT");
        let state = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("ENGINE_OUTPUT_NOT_OPEN"))?;
        ensure!(
            state.name == name
                && !state.ended
                && state.pending.len() + bytes.len()
                    <= engine_ipc::MESSAGE_BYTES + engine_ipc::CHUNK_BYTES,
            "ENGINE_IPC_INVALID"
        );
        state.pending.extend(bytes);
        self.output_bytes = total;
        while let Some((length, metadata)) = engine_ipc::frame_length(&state.pending)? {
            if metadata == 0 {
                ensure!(
                    state.fields.is_some() && state.pending.len() == 8,
                    "ENGINE_IPC_INVALID"
                );
                state.ended = true;
                state.pending.clear();
                break;
            }
            if let Some(fields) = &state.fields {
                let batch = engine_ipc::decode_batch(
                    &state.schema_prefix,
                    &state.pending[..length],
                    fields,
                )?;
                let rows = batch.num_rows() as u64;
                state.writer.append(batch).await?;
                self.output_rows_appended += rows;
            } else {
                state.fields = Some(engine_ipc::schema(&state.pending[..length], &state.schema)?);
            }
            state.pending.drain(..length);
        }
        self.check().await
    }
    pub(crate) async fn finish_output(&mut self, name: &str) -> Result<SealedObject> {
        self.check().await?;
        if let Some(root) = self.sealed.get(name) {
            return Ok(root.clone());
        }
        let writer = self
            .writer
            .take()
            .ok_or_else(|| anyhow::anyhow!("ENGINE_OUTPUT_NOT_OPEN"))?;
        if writer.name != name || !writer.ended || !writer.pending.is_empty() {
            self.failed = true;
            anyhow::bail!("ENGINE_IPC_INCOMPLETE");
        }
        match writer.writer.finish().await {
            Ok(root) => {
                self.check().await?;
                self.sealed.insert(name.into(), root.clone());
                Ok(root)
            }
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }
    pub(crate) fn sealed_outputs(&self) -> Result<BTreeMap<String, SealedObject>> {
        ensure!(
            !self.failed && self.writer.is_none() && self.sealed.len() == self.outputs.len(),
            "ENGINE_OUTPUT_INCOMPLETE"
        );
        Ok(self
            .sealed
            .iter()
            .map(|(name, root)| (self.outputs[name].resource.clone(), root.clone()))
            .collect())
    }
}
