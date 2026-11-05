//! Verify staged sparse output against actual accepted effects, without rereading the population.
use super::{
    DatasetIoGrant, WriteContract,
    key_run::{Locator, RunKind, RunValue},
    mutation_index::IndexChanges,
    mutation_witness::Witness,
    mutation_write::{MutationBudget, MutationObserved},
    position_delete, rows,
};
use crate::native_snapshot_v3::{MutationFootprint, MutationMode, NativeSnapshot, RowOperation};
use anyhow::{Result, ensure};
use futures::TryStreamExt;
use iceberg::spec::{
    DataContentType, DataFileFormat, FormatVersion, Manifest, ManifestContentType, ManifestFile,
    ManifestList, ManifestStatus,
};
use std::{collections::BTreeSet, sync::Arc};
use uuid::Uuid;

const INVALID: &str = "DATASET_MUTATION_PROOF_CHANGED";
const SAFE: u64 = 9_007_199_254_740_991;

pub(super) struct Validation<'a> {
    pub(super) basis: Option<&'a NativeSnapshot>,
    pub(super) grant: &'a DatasetIoGrant,
    pub(super) contract: &'a WriteContract,
    pub(super) stage: Uuid,
    pub(super) witness: &'a Witness,
    pub(super) index: Option<&'a IndexChanges>,
    pub(super) budget: &'a MutationBudget,
    pub(super) observed: &'a MutationObserved,
}
fn add(value: &mut u64, amount: u64) -> Result<()> {
    *value = value
        .checked_add(amount)
        .filter(|value| *value <= SAFE)
        .ok_or_else(|| anyhow::anyhow!("{INVALID}"))?;
    Ok(())
}
async fn manifests(snapshot: &NativeSnapshot) -> Result<Vec<ManifestFile>> {
    let current = snapshot
        .metadata()
        .current_snapshot()
        .ok_or_else(|| anyhow::anyhow!("SNAPSHOT_PIN_MISMATCH"))?;
    let list = ManifestList::parse_with_version(
        &snapshot.metadata_bytes(current.manifest_list()).await?,
        FormatVersion::V2,
    )?;
    ensure!(
        list.entries().len() <= super::MAX_MANIFESTS,
        "RESOURCE_LIMIT"
    );
    Ok(list.entries().to_vec())
}

impl Validation<'_> {
    pub(super) async fn verify(&self, result: Arc<NativeSnapshot>) -> Result<MutationFootprint> {
        self.grant.check_current().await?;
        self.budget.validate()?;
        ensure!(
            self.observed.input_rows <= self.budget.max_input_rows
                && self.observed.input_bytes <= self.budget.max_input_bytes
                && self.observed.changed_rows <= self.budget.max_changed_rows
                && self.observed.changed_rows <= self.observed.input_rows
                && self.observed.changed_logical_bytes <= self.budget.max_changed_logical_bytes
                && self.observed.touched_files <= self.budget.max_touched_files
                && self.observed.compaction_rows <= self.budget.max_compaction_rows
                && self.observed.compaction_bytes <= self.budget.max_compaction_bytes,
            "{INVALID}"
        );
        let reference = result
            .descriptor()
            .mutation_footprint
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("{INVALID}"))?;
        let readable = result.read_mutation_footprint(reference).await?;
        let footprint = &readable.descriptor;
        let expected = &self.contract.expected_head;
        let mode = match footprint.mode {
            MutationMode::Append => "append",
            MutationMode::Upsert => "upsert",
            MutationMode::Delete => "delete",
        };
        ensure!(
            mode == self.contract.mode
                && footprint.basis.revision == expected.revision
                && footprint.basis.snapshot == expected.snapshot
                && footprint.basis.metadata == expected.metadata_location
                && footprint.basis.rows == self.basis.map_or(0, NativeSnapshot::row_count)
                && footprint.producer.execution == self.grant.lease().execution
                && footprint.producer.owner == self.grant.lease().owner
                && footprint.producer.fence == self.grant.lease().fence
                && footprint.producer.incarnation == self.grant.lease().incarnation
                && footprint.producer.stage == self.stage
                && footprint.schema_hash == self.contract.schema_hash
                && footprint.key_schema_hash == self.contract.key_schema_hash
                && footprint.primary_key == self.contract.primary_key
                && reference.node.entries == self.observed.changed_rows,
            "{INVALID}"
        );
        let columns = crate::snapshot::columns(&serde_json::to_value(
            result.metadata().current_schema().as_ref(),
        )?)?;
        let column_json = serde_json::to_value(&columns)?;
        let mut inserted = 0u64;
        let mut replaced = 0u64;
        let mut deleted = 0u64;
        let mut logical_bytes = 0u64;
        let mut data_files = 0u64;
        if let Some(complete) = readable.complete_rows {
            let mut entries = complete.entries();
            while let Some((name, entry)) = entries.try_next().await? {
                let (file, mut batches) = result.changed_rows(&name, entry).await?;
                self.witness.verify_footprint(
                    &name,
                    file.operation.name(),
                    file.rows,
                    &file.file.content,
                )?;
                let mut offset = 0u64;
                while let Some(batch) = batches.try_next().await? {
                    let mut after = 0;
                    while after < batch.num_rows() {
                        let (page, encoded) = rows::mutation_page(
                            &batch,
                            after,
                            &footprint.primary_key,
                            &columns,
                            &column_json,
                            false,
                        )?;
                        match encoded {
                            rows::MutationRows::Complete(rows) => {
                                let index =
                                    self.index.ok_or_else(|| anyhow::anyhow!("{INVALID}"))?;
                                let keys: Vec<_> =
                                    rows.iter().map(|row| row.key().to_vec()).collect();
                                let locators = result.lookup_encoded_keys(&keys).await?;
                                let hashes = result.lookup_runs(&keys, RunKind::Hashes).await?;
                                ensure!(
                                    hashes
                                        .into_iter()
                                        .zip(&rows)
                                        .all(|(hash, row)| hash
                                            == Some(RunValue::Hash(row.row_hash()))),
                                    "{INVALID}"
                                );
                                let sequence = file.data_sequence.parse()?;
                                let mut verified = Vec::with_capacity(rows.len());
                                for (row, (canonical, (key, locator))) in
                                    rows.iter().zip(keys.into_iter().zip(locators)).enumerate()
                                {
                                    let expected = Locator {
                                        file: name.clone(),
                                        row: offset + row as u64,
                                        data_sequence: sequence,
                                        row_hash: Some(canonical.row_hash()),
                                    };
                                    ensure!(locator.as_ref() == Some(&expected), "{INVALID}");
                                    add(&mut logical_bytes, canonical.bytes())?;
                                    verified.push((key, locator));
                                }
                                index.verify(&verified)?;
                            }
                            rows::MutationRows::Unkeyed(bytes) => {
                                ensure!(
                                    self.index.is_none()
                                        && footprint.primary_key.is_empty()
                                        && footprint.mode == MutationMode::Append,
                                    "{INVALID}"
                                );
                                add(&mut logical_bytes, bytes)?;
                            }
                            rows::MutationRows::Keys(_) => anyhow::bail!("{INVALID}"),
                        }
                        add(&mut offset, page.num_rows() as u64)?;
                        after += page.num_rows();
                    }
                }
                ensure!(offset == file.rows, "PARQUET_COUNT_MISMATCH");
                match file.operation {
                    RowOperation::Insert => add(&mut inserted, offset)?,
                    RowOperation::Replacement => add(&mut replaced, offset)?,
                }
                add(&mut data_files, 1)?;
            }
        }
        if let Some(keys) = readable.delete_keys {
            let index = self.index.ok_or_else(|| anyhow::anyhow!("{INVALID}"))?;
            let mut entries = keys.entries();
            while let Some((_, entry)) = entries.try_next().await? {
                let (partition, rows, run) = result.deleted_run(entry).await?;
                let mut entries = run.stream_partition(partition);
                let mut page = Vec::new();
                let mut bytes = 0usize;
                let mut seen = 0u64;
                while let Some(entry) = entries.try_next().await? {
                    ensure!(entry.value == RunValue::Deleted, "{INVALID}");
                    if page.len() == 1000 || bytes + entry.key.len() > 8 * 1024 * 1024 {
                        self.verify_deleted(&result, index, std::mem::take(&mut page))
                            .await?;
                        bytes = 0;
                    }
                    bytes += entry.key.len();
                    add(&mut logical_bytes, entry.key.len() as u64)?;
                    page.push(entry.key);
                    add(&mut seen, 1)?;
                }
                if !page.is_empty() {
                    self.verify_deleted(&result, index, page).await?;
                }
                ensure!(seen == rows, "{INVALID}");
                add(&mut deleted, seen)?;
            }
        }
        if let Some(index) = self.index {
            index.finish_verification()?;
        }
        ensure!(
            inserted == footprint.counts.inserted
                && replaced == footprint.counts.replaced
                && deleted == footprint.counts.deleted
                && logical_bytes == self.observed.changed_logical_bytes
                && result.data_file_count()
                    == self.basis.map_or(0, NativeSnapshot::data_file_count) + data_files,
            "{INVALID}"
        );
        self.standard(&result, footprint, data_files).await?;
        self.witness.finish()?;
        result.check_current().await?;
        self.grant.check_current().await?;
        Ok(readable.descriptor)
    }

    async fn verify_deleted(
        &self,
        result: &NativeSnapshot,
        index: &IndexChanges,
        keys: Vec<Vec<u8>>,
    ) -> Result<()> {
        ensure!(
            result
                .lookup_encoded_keys(&keys)
                .await?
                .iter()
                .all(Option::is_none)
                && result
                    .lookup_runs(&keys, RunKind::Hashes)
                    .await?
                    .iter()
                    .all(Option::is_none),
            "{INVALID}"
        );
        index.verify(&keys.into_iter().map(|key| (key, None)).collect::<Vec<_>>())
    }

    async fn standard(
        &self,
        result: &NativeSnapshot,
        footprint: &MutationFootprint,
        expected_files: u64,
    ) -> Result<()> {
        let before = match self.basis {
            Some(basis) => manifests(basis).await?,
            None => vec![],
        };
        let after = manifests(result).await?;
        ensure!(after.starts_with(&before), "{INVALID}");
        let sequence = result.descriptor().sequence.parse::<i64>()?;
        ensure!(
            sequence
                == i64::try_from(
                    self.basis
                        .map(NativeSnapshot::next_sequence)
                        .transpose()?
                        .unwrap_or(1)
                )?,
            "{INVALID}"
        );
        let snapshot = result.snapshot_id()?;
        let mut names = BTreeSet::new();
        for file in &after {
            ensure!(names.insert(&file.manifest_path), "{INVALID}");
        }
        let mut data = 0u64;
        let mut positions = 0u64;
        for file in &after[before.len()..] {
            ensure!(
                file.partition_spec_id == 0
                    && file.key_metadata.is_none()
                    && file.sequence_number == sequence
                    && file.added_snapshot_id == snapshot,
                "{INVALID}"
            );
            let bytes = result.metadata_bytes(&file.manifest_path).await?;
            ensure!(
                u64::try_from(file.manifest_length)? == bytes.len() as u64,
                "{INVALID}"
            );
            let manifest = Manifest::parse_avro(&bytes)?;
            ensure!(
                manifest.metadata().format_version == FormatVersion::V2
                    && manifest.metadata().partition_spec.is_unpartitioned()
                    && !manifest.entries().is_empty()
                    && manifest.entries().len() <= 128,
                "{INVALID}"
            );
            for entry in manifest.entries() {
                let item = entry.data_file();
                ensure!(
                    entry.status == ManifestStatus::Added
                        && entry.sequence_number.unwrap_or(sequence) == sequence
                        && entry.snapshot_id.unwrap_or(snapshot) == snapshot
                        && item.file_format() == DataFileFormat::Parquet
                        && item.partition().iter().len() == 0,
                    "{INVALID}"
                );
                let content = result.file(entry.file_path()).await?;
                ensure!(
                    content.content().size == item.file_size_in_bytes(),
                    "{INVALID}"
                );
                match (file.content, item.content_type()) {
                    (ManifestContentType::Data, DataContentType::Data) => {
                        self.witness.verify_manifest(
                            entry.file_path(),
                            "data",
                            item.record_count(),
                            content.content(),
                        )?;
                        add(&mut data, 1)?;
                    }
                    (ManifestContentType::Deletes, DataContentType::PositionDeletes) => {
                        self.witness.verify_manifest(
                            entry.file_path(),
                            "delete",
                            item.record_count(),
                            content.content(),
                        )?;
                        let (path, rows) =
                            position_delete::read(content, item.record_count()).await?;
                        self.witness.verify_positions(&path, &rows)?;
                        add(&mut positions, rows.len() as u64)?;
                    }
                    _ => anyhow::bail!("{INVALID}"),
                }
            }
        }
        ensure!(
            data == expected_files
                && positions == footprint.counts.deleted + footprint.counts.replaced,
            "{INVALID}"
        );
        let mut after = String::new();
        let mut files = 0u64;
        while let Some((name, count)) = self.witness.next_file(&after)? {
            let basis = self.basis.ok_or_else(|| anyhow::anyhow!("{INVALID}"))?;
            let old = basis.selection(&name).await?;
            let new = result.selection(&name).await?;
            let old_file = basis.file(&name).await?;
            let new_file = result.file(&name).await?;
            ensure!(
                old.content() == new.content()
                    && old.physical_rows() == new.physical_rows()
                    && old.data_sequence() == new.data_sequence()
                    && old.selected_rows().checked_sub(count) == Some(new.selected_rows())
                    && old_file.physical_version() == new_file.physical_version(),
                "{INVALID}"
            );
            let mut position = -1i64;
            loop {
                let page = self.witness.position_page(&name, position)?;
                if page.is_empty() {
                    break;
                }
                position = i64::try_from(*page.last().unwrap())?;
                old.verify_positions(&page, true).await?;
                new.verify_positions(&page, false).await?;
            }
            after = name;
            add(&mut files, 1)?;
        }
        ensure!(files == self.observed.touched_files, "{INVALID}");
        Ok(())
    }
}
