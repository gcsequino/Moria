//! Bounded ordinary Iceberg v2 metadata assembly for the native mutation producer.
use super::{
    Capture, MAX_MANIFESTS, NewSnapshot, Part, graph_builder::GraphBuilder, logical,
    position_delete::DeletePart, replace_graph::Basis, snapshot_metadata,
};
use crate::{
    catalog::{DatasetIoGrant, NativeSnapshotRef},
    dataset_graph::{NativeGraph, QualifiedNode},
    native_snapshot_v3::MutationMode,
};
use anyhow::{Result, ensure};
use iceberg::{
    io::OutputFile,
    spec::{
        DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, ManifestFile,
        ManifestList, ManifestListWriter, ManifestWriterBuilder, PartitionSpec, Schema,
    },
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};
use uuid::Uuid;

const SAFE: u64 = 9_007_199_254_740_991;
pub(super) struct StandardMutation {
    graph: NativeGraph,
    schema: Arc<Schema>,
    spec: PartitionSpec,
    mode: MutationMode,
    prior: Option<Value>,
    parent_snapshot: Option<i64>,
    manifests: Vec<ManifestFile>,
    pub(super) sequence: u64,
    pub(super) snapshot_id: i64,
    added_data: u64,
    added_rows: u64,
    added_deletes: u64,
    added_positions: u64,
}
fn add(counter: &mut u64, delta: u64) -> Result<()> {
    *counter = counter
        .checked_add(delta)
        .filter(|n| *n <= SAFE)
        .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
    Ok(())
}
impl StandardMutation {
    pub(super) async fn open(
        graph: NativeGraph,
        schema: Arc<Schema>,
        basis: Basis,
        contract: &super::WriteContract,
    ) -> Result<Self> {
        graph.check_current().await?;
        let mode = match contract.mode.as_str() {
            "append" => MutationMode::Append,
            "upsert" => MutationMode::Upsert,
            "delete" => MutationMode::Delete,
            _ => anyhow::bail!("DATASET_WRITE_MODE_INVALID"),
        };
        let mut manifests = Vec::new();
        let (parent_snapshot, sequence) = match &basis.snapshot {
            Some(snapshot) => {
                let location = contract
                    .expected_head
                    .metadata_location
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("INPUT_PIN_CHANGED"))?;
                ensure!(
                    NativeSnapshotRef::decode(location)?.version == 3
                        && basis.raw.is_some()
                        && basis.files.is_some(),
                    "DATASET_WRITE_CAPABILITY_REQUIRED"
                );
                let current = snapshot
                    .metadata()
                    .current_snapshot()
                    .ok_or_else(|| anyhow::anyhow!("SNAPSHOT_PIN_MISMATCH"))?;
                let current_id = current.snapshot_id().to_string();
                ensure!(
                    contract.expected_head.snapshot.as_deref() == Some(current_id.as_str()),
                    "INPUT_PIN_CHANGED"
                );
                let list = ManifestList::parse_with_version(
                    &snapshot.metadata_bytes(current.manifest_list()).await?,
                    FormatVersion::V2,
                )?;
                ensure!(list.entries().len() <= MAX_MANIFESTS, "RESOURCE_LIMIT");
                let mut paths = BTreeSet::new();
                for manifest in list.entries() {
                    ensure!(
                        manifest.partition_spec_id == 0
                            && manifest.key_metadata.is_none()
                            && manifest.manifest_path.starts_with("moria://")
                            && manifest.sequence_number >= 0
                            && manifest.sequence_number
                                <= snapshot.metadata().last_sequence_number()
                            && paths.insert(&manifest.manifest_path),
                        "DATASET_WRITE_CAPABILITY_REQUIRED"
                    );
                    // These exact immutable entries were already receiver-qualified on the
                    // captured basis. A sparse write does not reread all old manifests/data.
                    manifests.push(manifest.clone());
                }
                (Some(current.snapshot_id()), snapshot.next_sequence()?)
            }
            None => {
                ensure!(
                    contract.expected_head.revision == "0"
                        && contract.expected_head.snapshot.is_none()
                        && contract.expected_head.metadata_location.is_none()
                        && basis.raw.is_none()
                        && basis.files.is_none(),
                    "INPUT_PIN_CHANGED"
                );
                (None, 1)
            }
        };
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()?;
        let snapshot_id = (Uuid::new_v4().as_u128() as u64 & ((1_u64 << 52) - 1)).max(1) as i64;
        if let Some(snapshot) = &basis.snapshot {
            ensure!(
                snapshot.metadata().snapshot_by_id(snapshot_id).is_none(),
                "SNAPSHOT_ID_CONFLICT"
            );
        }
        Ok(Self {
            graph,
            schema,
            spec,
            mode,
            prior: basis.raw,
            parent_snapshot,
            manifests,
            sequence,
            snapshot_id,
            added_data: 0,
            added_rows: 0,
            added_deletes: 0,
            added_positions: 0,
        })
    }
    async fn metadata_file(
        &self,
        builder: &mut GraphBuilder,
        name: &str,
        bytes: &[u8],
    ) -> Result<()> {
        ensure!(bytes.len() <= super::METADATA_BYTES, "RESOURCE_LIMIT");
        crate::snapshot::preflight_avro(bytes)?;
        let (file, certificate) = self.graph.write_index_bytes(bytes).await?;
        builder
            .file_qualified(name, file.certificate().content().clone(), certificate)
            .await?;
        Ok(())
    }
    pub(super) async fn data(
        &mut self,
        grant: &DatasetIoGrant,
        builder: &mut GraphBuilder,
        parts: Vec<(Part, QualifiedNode)>,
    ) -> Result<()> {
        ensure!(
            self.mode != MutationMode::Delete && !parts.is_empty() && parts.len() <= 128,
            "OUTPUT_INVALID"
        );
        ensure!(
            self.manifests.len() < MAX_MANIFESTS,
            "DATASET_MANIFEST_COMPACTION_REQUIRED"
        );
        let name = logical(grant, "metadata");
        let capture = Capture::new();
        let output = OutputFile::new(Arc::new(capture.clone()), name.clone());
        let mut manifest = ManifestWriterBuilder::new(
            output,
            Some(self.snapshot_id),
            self.schema.clone(),
            self.spec.clone(),
        )
        .build_v2_data();
        for (part, certificate) in parts {
            ensure!(part.rows > 0 && part.rows <= SAFE, "OUTPUT_INVALID");
            builder
                .file_qualified(&part.name, part.content.clone(), certificate)
                .await?;
            let file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(part.name)
                .file_format(DataFileFormat::Parquet)
                .record_count(part.rows)
                .file_size_in_bytes(part.content.size)
                .build()?;
            manifest.add_file(file, i64::try_from(self.sequence)?)?;
            add(&mut self.added_data, 1)?;
            add(&mut self.added_rows, part.rows)?;
        }
        let manifest = manifest.write_manifest_file().await?;
        self.metadata_file(builder, &name, &capture.take()?).await?;
        self.manifests.push(manifest);
        Ok(())
    }
    pub(super) async fn deletes(
        &mut self,
        grant: &DatasetIoGrant,
        builder: &mut GraphBuilder,
        parts: Vec<DeletePart>,
    ) -> Result<()> {
        ensure!(
            self.mode != MutationMode::Append && !parts.is_empty() && parts.len() <= 128,
            "OUTPUT_INVALID"
        );
        ensure!(
            self.manifests.len() < MAX_MANIFESTS,
            "DATASET_MANIFEST_COMPACTION_REQUIRED"
        );
        let name = logical(grant, "metadata");
        let capture = Capture::new();
        let output = OutputFile::new(Arc::new(capture.clone()), name.clone());
        let mut manifest = ManifestWriterBuilder::new(
            output,
            Some(self.snapshot_id),
            self.schema.clone(),
            self.spec.clone(),
        )
        .build_v2_deletes();
        for part in parts {
            ensure!(
                part.data_file.content_type() == DataContentType::PositionDeletes
                    && part.data_file.file_path() == part.file.name
                    && part.data_file.file_size_in_bytes() == part.file.content.size
                    && part.data_file.record_count() > 0,
                "OUTPUT_INVALID"
            );
            builder
                .file_qualified(&part.file.name, part.file.content, part.certificate)
                .await?;
            add(&mut self.added_deletes, 1)?;
            add(&mut self.added_positions, part.data_file.record_count())?;
            manifest.add_file(part.data_file, i64::try_from(self.sequence)?)?;
        }
        let manifest = manifest.write_manifest_file().await?;
        self.metadata_file(builder, &name, &capture.take()?).await?;
        self.manifests.push(manifest);
        Ok(())
    }
    pub(super) async fn finish(
        mut self,
        grant: &DatasetIoGrant,
        builder: &mut GraphBuilder,
    ) -> Result<Value> {
        ensure!(
            self.added_rows > 0 || self.added_positions > 0,
            "DATASET_NO_EFFECT_RECEIPT_REQUIRED"
        );
        let sequence = i64::try_from(self.sequence)?;
        let name = logical(grant, "metadata");
        let capture = Capture::new();
        let mut list = ManifestListWriter::v2(
            Box::new(capture.clone()),
            self.snapshot_id,
            self.parent_snapshot,
            sequence,
        );
        list.add_manifests(std::mem::take(&mut self.manifests).into_iter())?;
        list.close().await?;
        self.metadata_file(builder, &name, &capture.take()?).await?;
        let operation = match self.mode {
            MutationMode::Append => "append",
            MutationMode::Upsert => "overwrite",
            MutationMode::Delete => "delete",
        };
        snapshot_metadata(
            grant,
            &self.schema,
            self.prior,
            NewSnapshot {
                sequence,
                snapshot_id: self.snapshot_id,
                parent_snapshot: self.parent_snapshot,
                list_name: &name,
                summary: json!({
                    "operation":operation, "added-data-files":self.added_data.to_string(),
                    "added-records":self.added_rows.to_string(), "added-delete-files":self.added_deletes.to_string(),
                    "added-position-deletes":self.added_positions.to_string(),
                }),
            },
        )
    }
}
