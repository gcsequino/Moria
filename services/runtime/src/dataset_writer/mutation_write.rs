//! Fenced sparse coordinator. Only a qualified candidate/publication path can expose its stage.
use super::{
    ClosureLimits, DatasetIoGrant, NativeSnapshotRef, ObjectStore, Part, PartBuffer, PosixStore,
    arrow_schema,
    graph_builder::GraphBuilder,
    key_run::Locator,
    key_run_set::update::CompactionBudget,
    keys::Keys,
    logical,
    mutation_index::IndexChanges,
    mutation_inputs::{Inputs, batches},
    mutation_standard::StandardMutation,
    mutation_witness::Witness,
    object,
    position_delete::{self, DeletePart},
    rebind_batch, replace_graph, row_selection,
    rows::{self, CanonicalRow},
    schema_hash,
};
use crate::{
    dataset_graph::{NativeGraph, QualifiedNode},
    native_snapshot_v3::{
        IndexSummary, LogicalFile, MutationBasis, MutationCounts, MutationFootprint,
        MutationFootprintRef, MutationMode, MutationProducer, MutationResult, NativeSnapshot,
        PROFILE, RootDescriptor, VerifiedPosition, VisibilityIndex,
    },
    persistent_directory::{
        NodeReference,
        graph::{GraphDirectory, GraphValue},
    },
    storage::{Cancellation, ClosureSpool, SealedObject},
};
use anyhow::{Result, ensure};
use arrow_array::{RecordBatch, UInt32Array};
use futures::TryStreamExt;
use iceberg::spec::Schema;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

const SAFE: u64 = 9_007_199_254_740_991;
const PAGE: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MutationBudget {
    pub(crate) max_input_rows: u64,
    pub(crate) max_input_bytes: u64,
    pub(crate) max_changed_rows: u64,
    pub(crate) max_changed_logical_bytes: u64,
    pub(crate) max_touched_files: u64,
    pub(crate) max_compaction_rows: u64,
    pub(crate) max_compaction_bytes: u64,
}
impl MutationBudget {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            (1..=SAFE).contains(&self.max_input_rows)
                && (1..=SAFE).contains(&self.max_input_bytes)
                && (1..=self.max_input_rows).contains(&self.max_changed_rows)
                && (1..=SAFE).contains(&self.max_changed_logical_bytes)
                && (1..=SAFE).contains(&self.max_touched_files)
                && self.max_compaction_rows <= SAFE
                && self.max_compaction_bytes <= SAFE,
            "DATASET_MUTATION_BUDGET_INVALID"
        );
        Ok(())
    }
}
fn add(counter: &mut u64, amount: u64, maximum: u64) -> Result<()> {
    *counter = counter
        .checked_add(amount)
        .filter(|value| *value <= maximum)
        .ok_or_else(|| anyhow::anyhow!("DATASET_MUTATION_BUDGET_EXCEEDED"))?;
    Ok(())
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MutationObserved {
    pub(super) input_rows: u64,
    pub(super) input_bytes: u64,
    pub(super) changed_rows: u64,
    pub(super) changed_logical_bytes: u64,
    pub(super) touched_files: u64,
    pub(super) compaction_rows: u64,
    pub(super) compaction_bytes: u64,
}

struct TouchedFiles {
    spool: ClosureSpool,
    table: String,
    count: u64,
    maximum: u64,
}
impl TouchedFiles {
    fn new(spool: ClosureSpool, maximum: u64) -> Result<Self> {
        let table = format!("mutation_touched_{}", Uuid::new_v4().simple());
        let db = spool.database()?;
        spool.assert_scope(&db)?;
        db.execute_batch(&format!(
            "CREATE TABLE {table}(name TEXT PRIMARY KEY) WITHOUT ROWID"
        ))?;
        Ok(Self {
            spool,
            table,
            count: 0,
            maximum,
        })
    }
    fn admit(&mut self, file: &str) -> Result<()> {
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let added = db.execute(
            &format!("INSERT OR IGNORE INTO {}(name)VALUES(?1)", self.table),
            [file],
        )?;
        add(&mut self.count, added as u64, self.maximum)
    }
}

pub(super) enum StagedMutation {
    Changes {
        graph: NativeGraph,
        stage: Uuid,
        reference: NativeSnapshotRef,
        observed: MutationObserved,
        basis: Option<NativeSnapshot>,
        contract: super::WriteContract,
        witness: Witness,
        index: Option<IndexChanges>,
    },
    NoEffect {
        graph: NativeGraph,
        receipt: QualifiedNode,
        contract: super::WriteContract,
    },
}

struct Writer {
    grant: Arc<DatasetIoGrant>,
    target: Arc<Schema>,
    columns: BTreeMap<String, Value>,
    column_json: Value,
    keys: Vec<String>,
    basis: Option<NativeSnapshot>,
    mode: MutationMode,
    standard: StandardMutation,
    builder: GraphBuilder,
    budget: MutationBudget,
    unique: Option<Keys>,
    index: Option<IndexChanges>,
    witness: Witness,
    touched: TouchedFiles,
    complete_dir: GraphDirectory,
    complete_root: Option<NodeReference>,
    data_page: Vec<(Part, QualifiedNode)>,
    delete_page: Vec<DeletePart>,
    input_rows: u64,
    input_bytes: u64,
    changed_rows: u64,
    changed_bytes: u64,
    basis_rows: u64,
    data_files: u64,
    counts: MutationCounts,
}

pub(super) async fn stage(
    store: Arc<dyn ObjectStore>,
    scratch: &PosixStore,
    grant: Arc<DatasetIoGrant>,
    schema: Schema,
    partsets: &[SealedObject],
    limits: ClosureLimits,
    cancel: Cancellation,
) -> Result<StagedMutation> {
    ensure!(grant.lease().task.is_none(), "COORDINATOR_REQUIRED");
    let contract = grant.write_contract(&schema).await?;
    let budget = contract
        .mutation_budget
        .clone()
        .ok_or_else(|| anyhow::anyhow!("DATASET_MUTATION_BUDGET_REQUIRED"))?;
    budget.validate()?;
    let mode = match contract.mode.as_str() {
        "append" => MutationMode::Append,
        "upsert" => MutationMode::Upsert,
        "delete" => MutationMode::Delete,
        _ => anyhow::bail!("DATASET_WRITE_MODE_INVALID"),
    };
    let input_schema = contract.input_schema(&schema)?;
    let schema = Arc::new(schema);
    let graph = NativeGraph::acquire(store.clone(), grant.clone(), cancel.clone()).await?;
    let mut inputs = Inputs::open(graph.clone(), &grant, contract.clone(), partsets).await?;
    let basis = replace_graph::basis(store.clone(), grant.clone(), &schema, cancel.clone()).await?;
    let old = basis.snapshot.clone();
    let prior = old.as_ref().map(NativeSnapshot::descriptor);
    let mut spool =
        ClosureSpool::create(scratch, object(&grant)?, limits, grant.clone(), cancel).await?;
    spool.seal(store.as_ref(), grant.clone()).await?;
    let keys = grant.primary_key()?;
    let key_hash = grant.key_schema_hash()?.to_owned();
    if let Some(prior) = prior {
        ensure!(
            prior.key_schema_hash == key_hash && prior.primary_key == keys,
            "DATASET_KEY_INDEX_INVALID"
        );
    }
    let files_root = basis.files.clone();
    let standard = StandardMutation::open(graph.clone(), schema.clone(), basis, &contract).await?;
    let builder = GraphBuilder {
        graph: graph.clone(),
        files: GraphDirectory::new(graph.clone(), vec!["files".into()])?,
        selections: GraphDirectory::new(graph.clone(), vec!["selections".into()])?,
        hashes: GraphDirectory::new(graph.clone(), vec!["hashes".into()])?,
        locations: GraphDirectory::new(graph.clone(), vec!["locations".into()])?,
        files_root,
        selections_root: prior.and_then(|root| root.selections.clone()),
        hashes_root: prior
            .and_then(|root| root.visibility.as_ref())
            .and_then(|index| index.hashes.clone()),
        locations_root: prior
            .and_then(|root| root.visibility.as_ref())
            .and_then(|index| index.locations.clone()),
        spool: spool.clone(),
        sequence: standard.sequence,
        key_hash,
    };
    let touched = TouchedFiles::new(spool.clone(), budget.max_touched_files)?;
    let witness = Witness::new(spool.clone())?;
    let unique = if keys.is_empty() {
        None
    } else {
        Some(Keys::new(&spool)?)
    };
    let index = if keys.is_empty() {
        None
    } else {
        Some(IndexChanges::new(
            spool.clone(),
            mode == MutationMode::Delete,
        )?)
    };
    let columns = crate::snapshot::columns(&serde_json::to_value(schema.as_ref())?)?;
    let column_json = serde_json::to_value(&columns)?;
    let basis_rows = prior.map_or(0, |root| root.rows);
    let data_files = prior.map_or(0, |root| root.data_files);
    let mut writer = Writer {
        grant,
        target: schema,
        columns,
        column_json,
        keys,
        basis: old,
        mode,
        standard,
        builder,
        budget,
        unique,
        index,
        witness,
        touched,
        complete_dir: GraphDirectory::new(graph, vec![])?,
        complete_root: None,
        data_page: vec![],
        delete_page: vec![],
        input_rows: 0,
        input_bytes: 0,
        changed_rows: 0,
        changed_bytes: 0,
        basis_rows,
        data_files,
        counts: MutationCounts {
            inserted: 0,
            replaced: 0,
            deleted: 0,
        },
    };
    while let Some((part, certificate)) = inputs.next_part().await? {
        add(
            &mut writer.input_bytes,
            part.content.size,
            writer.budget.max_input_bytes,
        )?;
        let mut reader = batches(&writer.builder.graph, &part, &certificate, &input_schema).await?;
        let mut offset = 0u64;
        while let Some(batch) = reader.try_next().await? {
            writer.builder.graph.check_current().await?;
            add(
                &mut writer.input_rows,
                batch.num_rows() as u64,
                writer.budget.max_input_rows,
            )?;
            writer.batch(&part, offset, &batch).await?;
            add(&mut offset, batch.num_rows() as u64, part.rows)?;
        }
        ensure!(offset == part.rows, "PARQUET_COUNT_MISMATCH");
        if mode == MutationMode::Append {
            writer.add_data(part, certificate, "insert").await?;
        }
    }
    writer
        .finish(contract.expected_head, inputs.finish()?)
        .await
}
/// Trusted sparse preparation entry. Ordinary routing remains disabled until qualification.
pub(super) async fn prepare(
    store: Arc<dyn ObjectStore>,
    scratch: &PosixStore,
    grant: Arc<DatasetIoGrant>,
    schema: Schema,
    partsets: &[SealedObject],
    limits: ClosureLimits,
    cancel: Cancellation,
) -> Result<Uuid> {
    let staged = stage(
        store.clone(),
        scratch,
        grant.clone(),
        schema.clone(),
        partsets,
        limits,
        cancel.clone(),
    )
    .await?;
    match staged {
        StagedMutation::Changes {
            graph,
            stage,
            reference,
            observed,
            basis,
            contract,
            witness,
            index,
        } => {
            let budget = contract
                .mutation_budget
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("DATASET_MUTATION_BUDGET_REQUIRED"))?;
            ensure!(
                grant.write_contract(&schema).await? == contract,
                "DATASET_WRITE_CONTRACT_REQUIRED"
            );
            let (context, node) = crate::dataset_graph_read::GraphReadContext::staged(
                store,
                grant.clone(),
                stage,
                cancel,
            )
            .await?;
            let snapshot = Arc::new(NativeSnapshot::open(context, node).await?);
            grant.validate_schema(snapshot.metadata().current_schema().as_ref())?;
            let footprint = super::mutation_validate::Validation {
                basis: basis.as_ref(),
                grant: &grant,
                contract: &contract,
                stage,
                witness: &witness,
                index: index.as_ref(),
                budget,
                observed: &observed,
            }
            .verify(snapshot.clone())
            .await?;
            let mut proof = snapshot.preparation_proof(stage, &reference)?;
            proof["mutationFootprint"] =
                serde_json::to_value(&snapshot.descriptor().mutation_footprint)?;
            proof["mutationBasis"] = serde_json::to_value(&footprint.basis)?;
            proof["mutationObserved"] = serde_json::to_value(observed)?;
            proof["budget"] = serde_json::to_value(budget)?;
            crate::catalog::record_graph_candidate(
                &grant,
                &graph,
                stage,
                &reference,
                &snapshot,
                proof,
                &contract.mode,
            )
            .await
        }
        StagedMutation::NoEffect {
            graph,
            receipt,
            contract,
        } => {
            let budget = contract
                .mutation_budget
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("DATASET_MUTATION_BUDGET_REQUIRED"))?;
            ensure!(
                grant.write_contract(&schema).await? == contract,
                "DATASET_WRITE_CONTRACT_REQUIRED"
            );
            let mut proof = graph.read_descriptor(&receipt).await?;
            let rows = proof["inputRows"]
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("DATASET_MUTATION_PROOF_CHANGED"))?;
            let bytes = proof["inputBytes"]
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("DATASET_MUTATION_PROOF_CHANGED"))?;
            ensure!(
                receipt.kind() == "partset"
                    && receipt.entries() == 0
                    && proof["version"].as_u64() == Some(1)
                    && proof["kind"] == "datasetMutationNoEffect"
                    && proof["mode"].as_str() == Some(contract.mode.as_str())
                    && proof["expectedHead"] == serde_json::to_value(&contract.expected_head)?
                    && proof["budget"] == serde_json::to_value(budget)?
                    && proof["execution"] == grant.lease().execution.to_string()
                    && proof["owner"] == grant.lease().owner.to_string()
                    && proof["fence"] == grant.lease().fence
                    && proof["incarnation"] == grant.lease().incarnation.to_string()
                    && proof["touchedFiles"].as_u64() == Some(0)
                    && rows <= budget.max_input_rows
                    && bytes <= budget.max_input_bytes
                    && (contract.mode == "delete" || rows == 0),
                "DATASET_MUTATION_PROOF_CHANGED"
            );
            let inputs: NodeReference = serde_json::from_value(proof["inputs"].clone())?;
            let child = graph.child(&receipt, "inputs").await?;
            ensure!(
                child.kind() == "directory"
                    && child.content() == &inputs.content
                    && child.entries() == inputs.entries
                    && child.logical_bytes() == inputs.logical_bytes
                    && inputs.entries > 0,
                "DATASET_MUTATION_PROOF_CHANGED"
            );
            proof["receipt"] = serde_json::to_value(receipt.content())?;
            crate::catalog::record_no_effect(&grant, &contract, &proof).await
        }
    }
}

impl Writer {
    fn charge(&mut self, count: usize, bytes: u64) -> Result<()> {
        add(
            &mut self.changed_rows,
            count as u64,
            self.budget.max_changed_rows,
        )?;
        add(
            &mut self.changed_bytes,
            bytes,
            self.budget.max_changed_logical_bytes,
        )
    }
    async fn batch(&mut self, part: &Part, offset: u64, batch: &RecordBatch) -> Result<()> {
        let mut after = 0;
        while after < batch.num_rows() {
            let (page, encoded) = rows::mutation_page(
                batch,
                after,
                &self.keys,
                &self.columns,
                &self.column_json,
                self.mode == MutationMode::Delete,
            )?;
            self.apply_batch(part, offset + after as u64, &page, encoded)
                .await?;
            after += page.num_rows();
        }
        Ok(())
    }
    async fn apply_batch(
        &mut self,
        part: &Part,
        offset: u64,
        batch: &RecordBatch,
        encoded: rows::MutationRows,
    ) -> Result<()> {
        let encoded = match encoded {
            rows::MutationRows::Keys(keys) => {
                ensure!(
                    self.mode == MutationMode::Delete,
                    "DATASET_WRITE_MODE_INVALID"
                );
                self.unique
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("DATASET_PRIMARY_KEY_MISSING"))?
                    .insert(&keys)?;
                let positions = self.positions(&keys).await?;
                let mut changes = Vec::new();
                let mut bytes = 0u64;
                let mut deleted = Vec::new();
                for (key, position) in keys.into_iter().zip(positions) {
                    if let Some(position) = position {
                        add(&mut bytes, key.len() as u64, SAFE)?;
                        changes.push((key, None));
                        deleted.push(position);
                    }
                }
                self.charge(changes.len(), bytes)?;
                add(&mut self.counts.deleted, changes.len() as u64, SAFE)?;
                if !changes.is_empty() {
                    self.hide(deleted).await?;
                    self.index
                        .as_mut()
                        .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?
                        .insert(changes)?;
                }
                return Ok(());
            }
            rows::MutationRows::Unkeyed(bytes) => {
                ensure!(
                    self.mode == MutationMode::Append && self.keys.is_empty(),
                    "DATASET_PRIMARY_KEY_MISSING"
                );
                self.charge(batch.num_rows(), bytes)?;
                add(&mut self.counts.inserted, batch.num_rows() as u64, SAFE)?;
                return Ok(());
            }
            rows::MutationRows::Complete(encoded) => encoded,
        };
        let keys: Vec<_> = encoded.iter().map(|row| row.key().to_vec()).collect();
        self.unique
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?
            .insert(&keys)?;
        let bytes = encoded.iter().try_fold(0u64, |mut bytes, row| {
            add(&mut bytes, row.bytes(), SAFE)?;
            Ok::<_, anyhow::Error>(bytes)
        })?;
        self.charge(encoded.len(), bytes)?;
        if self.mode == MutationMode::Append {
            if let Some(basis) = &self.basis {
                ensure!(
                    basis
                        .lookup_encoded_keys(&keys)
                        .await?
                        .iter()
                        .all(Option::is_none),
                    "DATASET_PRIMARY_KEY_DUPLICATE"
                );
            }
            let changes = encoded
                .iter()
                .enumerate()
                .map(|(index, row)| self.location(row, &part.name, offset + index as u64))
                .collect();
            self.index
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?
                .insert(changes)?;
            add(&mut self.counts.inserted, encoded.len() as u64, SAFE)?;
            return Ok(());
        }
        let positions = self.positions(&keys).await?;
        let mut inserted = Vec::new();
        let mut replaced = Vec::new();
        let mut hidden = Vec::new();
        for (index, position) in positions.into_iter().enumerate() {
            if let Some(position) = position {
                replaced.push(index);
                hidden.push(position);
            } else {
                inserted.push(index);
            }
        }
        self.hide(hidden).await?;
        for (indices, op) in [(inserted, "insert"), (replaced, "replacement")] {
            if indices.is_empty() {
                continue;
            }
            let (part, certificate) = self.write_rows(batch, &indices).await?;
            let changes = indices
                .iter()
                .enumerate()
                .map(|(row, index)| self.location(&encoded[*index], &part.name, row as u64))
                .collect();
            self.index
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?
                .insert(changes)?;
            if op == "insert" {
                add(&mut self.counts.inserted, part.rows, SAFE)?;
            } else {
                add(&mut self.counts.replaced, part.rows, SAFE)?;
            }
            self.add_data(part, certificate, op).await?;
        }
        Ok(())
    }
    async fn positions(&mut self, keys: &[Vec<u8>]) -> Result<Vec<Option<VerifiedPosition>>> {
        match &self.basis {
            Some(basis) => {
                let touched = &mut self.touched;
                basis
                    .lookup_verified_positions(keys, |file| touched.admit(file))
                    .await
            }
            None => Ok(vec![None; keys.len()]),
        }
    }
    fn location(&self, row: &CanonicalRow, file: &str, ordinal: u64) -> (Vec<u8>, Option<Locator>) {
        (
            row.key().to_vec(),
            Some(Locator {
                file: file.into(),
                row: ordinal,
                data_sequence: self.builder.sequence,
                row_hash: Some(row.row_hash()),
            }),
        )
    }
    async fn write_rows(
        &self,
        batch: &RecordBatch,
        indices: &[usize],
    ) -> Result<(Part, QualifiedNode)> {
        let indices = UInt32Array::from_iter_values(indices.iter().map(|index| *index as u32));
        let arrays = batch
            .columns()
            .iter()
            .map(|array| arrow_select::take::take(array.as_ref(), &indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let schema = arrow_schema(&self.target)?;
        let batch = rebind_batch(
            RecordBatch::try_new(batch.schema(), arrays)?,
            schema.clone(),
        )?;
        let mut writer = ArrowWriter::try_new(
            PartBuffer::default(),
            schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(1000))
                    .set_dictionary_enabled(false)
                    .build(),
            ),
        )?;
        writer.write(&batch)?;
        let (content, certificate) = self
            .builder
            .graph
            .write_parquet_bytes(&writer.into_inner()?.0)
            .await?;
        Ok((
            Part {
                name: logical(&self.grant, "data"),
                content,
                rows: batch.num_rows() as u64,
            },
            certificate,
        ))
    }
    async fn add_data(
        &mut self,
        part: Part,
        certificate: QualifiedNode,
        operation: &str,
    ) -> Result<()> {
        let graph = &self.builder.graph;
        let file = LogicalFile {
            name: part.name.clone(),
            content: part.content.clone(),
        };
        let selection = row_selection::write_all(
            graph,
            file.clone(),
            certificate.clone(),
            part.rows,
            self.builder.sequence,
        )
        .await?;
        self.builder.selections_root = self
            .builder
            .selections
            .apply(
                self.builder.selections_root.as_ref(),
                BTreeMap::from([(
                    part.name.clone(),
                    Some(GraphValue::new(json!({"name":part.name}), vec![selection])?),
                )]),
            )
            .await?;
        self.complete_root = self.complete_dir.apply(
            self.complete_root.as_ref(),
            BTreeMap::from([(part.name.clone(),Some(GraphValue::new(json!({
                "operation":operation,"file":file,"rows":part.rows,"dataSequence":self.builder.sequence.to_string(),
            }),vec![certificate.clone()])?))]),
        ).await?;
        add(&mut self.data_files, 1, SAFE)?;
        self.witness.file(
            &part.name,
            "data",
            Some(operation),
            part.rows,
            &part.content,
        )?;
        self.data_page.push((part, certificate));
        if self.data_page.len() == PAGE {
            self.standard
                .data(
                    &self.grant,
                    &mut self.builder,
                    std::mem::take(&mut self.data_page),
                )
                .await?;
        }
        Ok(())
    }
    async fn hide(&mut self, positions: Vec<VerifiedPosition>) -> Result<()> {
        let mut files = BTreeMap::<String, Vec<VerifiedPosition>>::new();
        for position in positions {
            files
                .entry(position.path().into())
                .or_default()
                .push(position);
        }
        for (file, mut positions) in files {
            positions.sort_unstable_by_key(VerifiedPosition::row);
            let rows: Vec<_> = positions.iter().map(VerifiedPosition::row).collect();
            self.witness.positions(&file, &rows)?;
            let root = self
                .builder
                .selections_root
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
            let (value, mut links) = self
                .builder
                .selections
                .lookup_with_links(root, &file)
                .await?
                .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
            ensure!(
                value == json!({"name":file}) && links.len() == 1,
                "DATASET_KEY_INDEX_INVALID"
            );
            let selected =
                row_selection::delete_rows(&self.builder.graph, links.remove(0), &rows).await?;
            self.builder.selections_root = self
                .builder
                .selections
                .apply(
                    self.builder.selections_root.as_ref(),
                    BTreeMap::from([(file, Some(GraphValue::new(value, vec![selected])?))]),
                )
                .await?;
            let part = position_delete::seal(&self.builder.graph, positions, self.builder.sequence)
                .await?;
            self.witness.file(
                &part.file.name,
                "delete",
                None,
                part.data_file.record_count(),
                &part.file.content,
            )?;
            self.delete_page.push(part);
            if self.delete_page.len() == PAGE {
                self.standard
                    .deletes(
                        &self.grant,
                        &mut self.builder,
                        std::mem::take(&mut self.delete_page),
                    )
                    .await?;
            }
        }
        Ok(())
    }
    async fn finish(
        mut self,
        contract: super::WriteContract,
        receipts: Vec<(String, Value)>,
    ) -> Result<StagedMutation> {
        let expected = contract.expected_head.clone();
        let graph = self.builder.graph.clone();
        let inputs_dir = GraphDirectory::new(graph.clone(), vec![])?;
        let inputs = inputs_dir
            .apply(
                None,
                receipts
                    .into_iter()
                    .map(|(key, value)| Ok((key, Some(GraphValue::new(value, vec![])?))))
                    .collect::<Result<BTreeMap<_, _>>>()?,
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?;
        let inputs_node = inputs_dir.qualified_root(&inputs).await?;
        if self.changed_rows == 0 {
            ensure!(
                self.mode != MutationMode::Upsert || self.input_rows == 0,
                "OUTPUT_INVALID"
            );
            let receipt = graph.write_descriptor("partset",&json!({
                "version":1,"kind":"datasetMutationNoEffect","mode":self.mode,
                "expectedHead":expected,"inputRows":self.input_rows,"inputBytes":self.input_bytes,
                "budget":self.budget,"touchedFiles":self.touched.count,
                "inputs":inputs,"execution":self.grant.lease().execution,"owner":self.grant.lease().owner,
                "fence":self.grant.lease().fence,"incarnation":self.grant.lease().incarnation,
            }),vec![("inputs".into(),inputs_node)],0,0).await?;
            return Ok(StagedMutation::NoEffect {
                graph,
                receipt,
                contract,
            });
        }
        if !self.data_page.is_empty() {
            self.standard
                .data(
                    &self.grant,
                    &mut self.builder,
                    std::mem::take(&mut self.data_page),
                )
                .await?;
        }
        if !self.delete_page.is_empty() {
            self.standard
                .deletes(
                    &self.grant,
                    &mut self.builder,
                    std::mem::take(&mut self.delete_page),
                )
                .await?;
        }
        let mut budget = CompactionBudget {
            remaining_rows: self.budget.max_compaction_rows,
            remaining_bytes: self.budget.max_compaction_bytes,
        };
        let delete_keys = match self.index.as_ref() {
            Some(index) => {
                index
                    .seal(&mut self.builder, self.changed_rows, &mut budget)
                    .await?
            }
            None => None,
        };
        let observed = MutationObserved {
            input_rows: self.input_rows,
            input_bytes: self.input_bytes,
            changed_rows: self.changed_rows,
            changed_logical_bytes: self.changed_bytes,
            touched_files: self.touched.count,
            compaction_rows: self.budget.max_compaction_rows - budget.remaining_rows,
            compaction_bytes: self.budget.max_compaction_bytes - budget.remaining_bytes,
        };
        let snapshot_id = self.standard.snapshot_id;
        let mut metadata = self.standard.finish(&self.grant, &mut self.builder).await?;
        let rows = self
            .basis_rows
            .checked_add(self.counts.inserted)
            .and_then(|rows| rows.checked_sub(self.counts.deleted))
            .filter(|rows| *rows <= SAFE)
            .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
        let visibility = (!self.keys.is_empty()).then(|| VisibilityIndex {
            rows,
            hashes: self.builder.hashes_root.clone(),
            locations: self.builder.locations_root.clone(),
        });
        let summary = IndexSummary {
            version: 1,
            table_uuid: self.grant.table_uuid()?,
            snapshot_id: snapshot_id.to_string(),
            sequence: self.builder.sequence.to_string(),
            schema_hash: schema_hash(&self.target)?,
            key_schema_hash: self.builder.key_hash.clone(),
            primary_key: self.keys.clone(),
            data_files: self.data_files,
            rows,
            selections: self.builder.selections_root.clone(),
            visibility: visibility.clone(),
        };
        let index = self
            .builder
            .seal_index_summary(&mut metadata, snapshot_id, summary.clone())
            .await?;
        let bytes = serde_json::to_vec(&metadata)?;
        ensure!(bytes.len() <= super::METADATA_BYTES, "RESOURCE_LIMIT");
        let (metadata_file, metadata_certificate) = graph.write_index_bytes(&bytes).await?;
        let name = logical(&self.grant, "metadata");
        let content = metadata_file.certificate().content().clone();
        self.builder
            .file_qualified(&name, content.clone(), metadata_certificate)
            .await?;
        let files = self
            .builder
            .files_root
            .clone()
            .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_INVALID"))?;
        let stage = Uuid::new_v4();
        let result_revision = expected
            .revision
            .parse::<i64>()?
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?
            .to_string();
        let basis_root = expected
            .metadata_location
            .as_ref()
            .map(|location| format!("{:x}", Sha256::digest(location.as_bytes())));
        let footprint = MutationFootprint {
            version: 1,
            table_uuid: summary.table_uuid,
            schema_hash: summary.schema_hash.clone(),
            key_schema_hash: summary.key_schema_hash.clone(),
            primary_key: self.keys.clone(),
            mode: self.mode,
            basis: MutationBasis {
                revision: expected.revision,
                snapshot: expected.snapshot,
                metadata: expected.metadata_location,
                root: basis_root,
                rows: self.basis_rows,
            },
            result: MutationResult {
                revision: result_revision,
                snapshot: snapshot_id.to_string(),
            },
            producer: MutationProducer {
                execution: self.grant.lease().execution,
                owner: self.grant.lease().owner,
                fence: self.grant.lease().fence.clone(),
                incarnation: self.grant.lease().incarnation,
                stage,
                inputs,
            },
            counts: self.counts,
            complete_rows: self.complete_root.clone(),
            delete_keys,
        };
        let mut footprint_children = vec![("inputs".into(), inputs_node)];
        for (label, reference) in [
            ("completeRows", &footprint.complete_rows),
            ("deleteKeys", &footprint.delete_keys),
        ] {
            if let Some(reference) = reference {
                let directory = GraphDirectory::new(graph.clone(), vec![])?;
                footprint_children.push((label.into(), directory.qualified_root(reference).await?));
            }
        }
        let footprint_node = graph
            .write_descriptor(
                "root",
                &serde_json::to_value(&footprint)?,
                footprint_children,
                self.changed_rows,
                0,
            )
            .await?;
        let footprint_ref = MutationFootprintRef {
            version: 1,
            node: NodeReference {
                content: footprint_node.content().clone(),
                entries: self.changed_rows,
                logical_bytes: 0,
                nodes: 1,
            },
        };
        let descriptor = RootDescriptor {
            version: 3,
            reader_profile: PROFILE.into(),
            profile: graph.profile().clone(),
            table_uuid: summary.table_uuid,
            snapshot_id: summary.snapshot_id,
            sequence: summary.sequence,
            schema_hash: summary.schema_hash,
            key_schema_hash: summary.key_schema_hash,
            primary_key: self.keys,
            data_files: self.data_files,
            rows,
            selections: self.builder.selections_root.clone(),
            metadata: LogicalFile { name, content },
            files: files.clone(),
            visibility,
            index_summary: Some(index.clone()),
            mutation_footprint: Some(footprint_ref.clone()),
        };
        footprint.validate(&descriptor, &footprint_ref)?;
        let mut children = self.builder.index_children().await?;
        children.push((
            "files".into(),
            self.builder.files.qualified_root(&files).await?,
        ));
        children.push((
            "index".into(),
            graph
                .known_node(&index.root)
                .await?
                .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_ANCESTRY_REQUIRED"))?,
        ));
        children.push(("mutation".into(), footprint_node));
        let root = graph
            .write_descriptor(
                "root",
                &serde_json::to_value(descriptor)?,
                children,
                rows,
                0,
            )
            .await?;
        graph.begin_stage(stage, &root).await?;
        while graph.advance(stage, 256).await?["ready"] != true {
            graph.check_current().await?;
        }
        let reference = crate::dataset_retention::register_graph_root(&graph, &root).await?;
        Ok(StagedMutation::Changes {
            graph,
            stage,
            reference,
            observed,
            basis: self.basis,
            contract,
            witness: self.witness,
            index: self.index,
        })
    }
}
