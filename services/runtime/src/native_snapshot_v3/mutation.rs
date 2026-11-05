//! Versioned mutation footprint reached only from a qualified actual result root.
use super::{NativeSnapshot, RootDescriptor};
use crate::{
    catalog::NativeSnapshotRef,
    persistent_directory::{NodeReference, read::ReadDirectory},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAX_ROWS: u64 = 9_007_199_254_740_991;
const INVALID: &str = "DATASET_DELTA_FOOTPRINT_INVALID";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MutationFootprintRef {
    pub(crate) version: u32,
    pub(crate) node: NodeReference,
}
impl MutationFootprintRef {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 1
                && self.node.nodes == 1
                && self.node.logical_bytes == 0
                && self.node.entries > 0
                && self.node.entries <= MAX_ROWS,
            "{INVALID}"
        );
        NativeSnapshotRef {
            version: 3,
            root: self.node.content.clone(),
        }
        .encode()?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum MutationMode {
    Append,
    Upsert,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MutationBasis {
    pub(crate) revision: String,
    pub(crate) snapshot: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) root: Option<String>,
    pub(crate) rows: u64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MutationResult {
    pub(crate) revision: String,
    pub(crate) snapshot: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MutationProducer {
    pub(crate) execution: Uuid,
    pub(crate) owner: Uuid,
    pub(crate) fence: String,
    pub(crate) incarnation: Uuid,
    pub(crate) stage: Uuid,
    pub(crate) inputs: NodeReference,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MutationCounts {
    pub(crate) inserted: u64,
    pub(crate) replaced: u64,
    pub(crate) deleted: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MutationFootprint {
    pub(crate) version: u32,
    pub(crate) table_uuid: Uuid,
    pub(crate) schema_hash: String,
    pub(crate) key_schema_hash: String,
    pub(crate) primary_key: Vec<String>,
    pub(crate) mode: MutationMode,
    pub(crate) basis: MutationBasis,
    pub(crate) result: MutationResult,
    pub(crate) producer: MutationProducer,
    pub(crate) counts: MutationCounts,
    pub(crate) complete_rows: Option<NodeReference>,
    pub(crate) delete_keys: Option<NodeReference>,
}
fn bigint(value: &str) -> Result<u64> {
    let number: u64 = value.parse()?;
    ensure!(
        number <= i64::MAX as u64 && number.to_string() == value,
        "{INVALID}"
    );
    Ok(number)
}
impl MutationFootprint {
    pub(crate) fn validate(
        &self,
        result: &RootDescriptor,
        reference: &MutationFootprintRef,
    ) -> Result<()> {
        reference.validate()?;
        let basis_revision = bigint(&self.basis.revision)?;
        let result_revision = bigint(&self.result.revision)?;
        ensure!(
            self.version == 1
                && self.table_uuid == result.table_uuid
                && self.schema_hash == result.schema_hash
                && self.key_schema_hash == result.key_schema_hash
                && self.primary_key == result.primary_key
                && self.result.snapshot == result.snapshot_id
                && basis_revision.checked_add(1) == Some(result_revision)
                && bigint(&self.producer.fence)? > 0
                && self.producer.inputs.entries > 0,
            "{INVALID}"
        );
        match (&self.basis.snapshot, &self.basis.metadata, &self.basis.root) {
            (None, None, None) => ensure!(basis_revision == 0 && self.basis.rows == 0, "{INVALID}"),
            (Some(snapshot), Some(metadata), Some(root)) => {
                NativeSnapshotRef::decode(metadata)?;
                ensure!(
                    basis_revision > 0
                        && snapshot.parse::<i64>()?.to_string() == *snapshot
                        && format!("{:x}", Sha256::digest(metadata.as_bytes())) == *root,
                    "{INVALID}"
                );
            }
            _ => anyhow::bail!("{INVALID}"),
        }
        let MutationCounts {
            inserted,
            replaced,
            deleted,
        } = self.counts;
        let complete = inserted
            .checked_add(replaced)
            .ok_or_else(|| anyhow::anyhow!("{INVALID}"))?;
        let effects = complete
            .checked_add(deleted)
            .ok_or_else(|| anyhow::anyhow!("{INVALID}"))?;
        ensure!(
            effects == reference.node.entries
                && self.basis.rows <= MAX_ROWS
                && replaced <= self.basis.rows
                && self
                    .basis
                    .rows
                    .checked_add(inserted)
                    .and_then(|rows| rows.checked_sub(deleted))
                    == Some(result.rows)
                && self.complete_rows.is_some() == (complete > 0)
                && self.delete_keys.is_some() == (deleted > 0)
                && self
                    .complete_rows
                    .as_ref()
                    .is_none_or(|dir| dir.entries > 0 && dir.entries <= complete)
                && self
                    .delete_keys
                    .as_ref()
                    .is_none_or(|dir| dir.entries > 0 && dir.entries <= deleted),
            "{INVALID}"
        );
        ensure!(
            match self.mode {
                MutationMode::Append => inserted > 0 && replaced == 0 && deleted == 0,
                MutationMode::Upsert =>
                    !self.primary_key.is_empty() && complete > 0 && deleted == 0,
                MutationMode::Delete =>
                    !self.primary_key.is_empty() && complete == 0 && deleted > 0,
            },
            "{INVALID}"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum RowOperation {
    Insert,
    Replacement,
}
impl RowOperation {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Replacement => "replacement",
        }
    }
}
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ChangedFile {
    pub(crate) operation: RowOperation,
    pub(crate) file: super::LogicalFile,
    pub(crate) rows: u64,
    pub(crate) data_sequence: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeletedRun {
    partition: u8,
    rows: u64,
    sequence: String,
    run: crate::storage::SealedObject,
}

pub(crate) struct ReadableMutation {
    pub(crate) descriptor: MutationFootprint,
    pub(crate) complete_rows: Option<ReadDirectory>,
    pub(crate) delete_keys: Option<ReadDirectory>,
    pub(crate) inputs: ReadDirectory,
}
impl NativeSnapshot {
    /// A footprint entry is already rooted in the accepted window or actual candidate.
    /// Re-resolve its certificate and the same file on this result before decoding.
    pub(crate) async fn changed_rows(
        self: &std::sync::Arc<Self>,
        name: &str,
        entry: crate::persistent_directory::read::ReadEntry,
    ) -> Result<(
        ChangedFile,
        futures::stream::BoxStream<'static, Result<arrow_array::RecordBatch>>,
    )> {
        self.check_current().await?;
        let changed: ChangedFile = serde_json::from_value(entry.value.clone())?;
        ensure!(
            changed.file.name == name
                && changed.rows > 0
                && changed.data_sequence == self.descriptor.sequence
                && entry.link_count() == 1,
            "{INVALID}"
        );
        let certificate = self.context.file(&entry.link(0).await?).await?;
        let file = self.file(name).await?;
        ensure!(
            certificate.content() == &changed.file.content
                && file.content() == certificate.content()
                && file.physical_version() == certificate.physical_version(),
            "{INVALID}"
        );
        let plan = super::GraphScanFile {
            name: name.into(),
            content: changed.file.content.clone(),
            physical_rows: changed.rows,
            selected_rows: changed.rows,
            data_sequence: bigint(&changed.data_sequence)?,
        };
        let stream = self.clone().scan_planned_file(&plan, 128).await?;
        Ok((changed, stream))
    }

    pub(crate) async fn deleted_run(
        &self,
        entry: crate::persistent_directory::read::ReadEntry,
    ) -> Result<(u8, u64, crate::dataset_writer::key_run_io::RunReader)> {
        use crate::dataset_writer::{key_run::RunKind, key_run_io::RunReader};
        self.check_current().await?;
        let deleted: DeletedRun = serde_json::from_value(entry.value.clone())?;
        ensure!(
            deleted.rows > 0
                && deleted.sequence == self.descriptor.sequence
                && entry.link_count() == 1,
            "{INVALID}"
        );
        let node = entry.link(0).await?;
        ensure!(
            node.content() == &deleted.run && node.entries() == deleted.rows,
            "{INVALID}"
        );
        let run = RunReader::open(
            &self.context,
            &node,
            &self.descriptor.key_schema_hash,
            RunKind::Hashes,
        )
        .await?;
        let descriptor = run.descriptor();
        ensure!(
            descriptor.sequence == bigint(&deleted.sequence)?
                && descriptor.entries == deleted.rows
                && descriptor.partitions.len() == 1
                && descriptor.partitions[0].partition == deleted.partition
                && descriptor.partitions[0].entries == deleted.rows,
            "{INVALID}"
        );
        self.check_current().await?;
        Ok((deleted.partition, deleted.rows, run))
    }
}

impl NativeSnapshot {
    /// The expected reference is read from the actual immutable journal-window member.
    /// It can only narrow an already qualified result-root context, never grant a read.
    pub(crate) async fn read_mutation_footprint(
        &self,
        expected: &MutationFootprintRef,
    ) -> Result<ReadableMutation> {
        self.check_current().await?;
        ensure!(
            self.context.catalog_reference()?.version == 3
                && self.descriptor.mutation_footprint.as_ref() == Some(expected),
            "{INVALID}"
        );
        expected.validate()?;
        let node = self.context.child(&self.root, "mutation").await?;
        ensure!(
            node.kind() == "root"
                && node.content() == &expected.node.content
                && node.entries() == expected.node.entries
                && node.logical_bytes() == expected.node.logical_bytes,
            "{INVALID}"
        );
        let descriptor: MutationFootprint =
            serde_json::from_value(self.context.descriptor(&node).await?)?;
        descriptor.validate(&self.descriptor, expected)?;
        let directory = |label: &'static str, reference: NodeReference| {
            let context = self.context.clone();
            let node = node.clone();
            async move {
                let child = context.child(&node, label).await?;
                ReadDirectory::new(context, child, reference).await
            }
        };
        let complete_rows = match &descriptor.complete_rows {
            Some(reference) => Some(directory("completeRows", reference.clone()).await?),
            None => None,
        };
        let delete_keys = match &descriptor.delete_keys {
            Some(reference) => Some(directory("deleteKeys", reference.clone()).await?),
            None => None,
        };
        let inputs = directory("inputs", descriptor.producer.inputs.clone()).await?;
        self.check_current().await?;
        Ok(ReadableMutation {
            descriptor,
            complete_rows,
            delete_keys,
            inputs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        native_snapshot_v3::{LogicalFile, PROFILE},
        storage::{LogicalObject, SealedObject},
    };

    fn reference(name: &str) -> NodeReference {
        NodeReference {
            content: SealedObject {
                object: LogicalObject::new("w", "t", name).unwrap(),
                size: 128,
                sha256: format!("sha256:{}", "a".repeat(64)),
            },
            entries: 1,
            logical_bytes: 0,
            nodes: 1,
        }
    }

    #[test]
    fn footprint_binds_real_edge_counts_and_preserves_exact_bigint_identity() {
        let root = RootDescriptor {
            version: 3,
            reader_profile: PROFILE.into(),
            profile: serde_json::json!({}),
            table_uuid: Uuid::new_v4(),
            snapshot_id: "100".into(),
            sequence: "1".into(),
            schema_hash: format!("sha256:{}", "b".repeat(64)),
            key_schema_hash: format!("sha256:{}", "c".repeat(64)),
            primary_key: vec!["id".into()],
            data_files: 1,
            rows: 1,
            selections: Some(reference("selections")),
            metadata: LogicalFile {
                name: "moria://w/t/metadata/head".into(),
                content: reference("metadata").content,
            },
            files: reference("files"),
            visibility: None,
            index_summary: None,
            mutation_footprint: None,
        };
        let footprint_ref = MutationFootprintRef {
            version: 1,
            node: reference("mutation"),
        };
        let footprint = MutationFootprint {
            version: 1,
            table_uuid: root.table_uuid,
            schema_hash: root.schema_hash.clone(),
            key_schema_hash: root.key_schema_hash.clone(),
            primary_key: root.primary_key.clone(),
            mode: MutationMode::Append,
            basis: MutationBasis {
                revision: "0".into(),
                snapshot: None,
                metadata: None,
                root: None,
                rows: 0,
            },
            result: MutationResult {
                revision: "1".into(),
                snapshot: root.snapshot_id.clone(),
            },
            producer: MutationProducer {
                execution: Uuid::new_v4(),
                owner: Uuid::new_v4(),
                fence: "9007199254740993".into(),
                incarnation: Uuid::new_v4(),
                stage: Uuid::new_v4(),
                inputs: reference("inputs"),
            },
            counts: MutationCounts {
                inserted: 1,
                replaced: 0,
                deleted: 0,
            },
            complete_rows: Some(reference("rows")),
            delete_keys: None,
        };
        footprint.validate(&root, &footprint_ref).unwrap();
        let mut wrong = footprint.clone();
        wrong.producer.fence = "09007199254740993".into();
        assert!(wrong.validate(&root, &footprint_ref).is_err());
        let mut wrong = footprint.clone();
        wrong.result.revision = "2".into();
        assert!(wrong.validate(&root, &footprint_ref).is_err());
        let mut wrong = footprint.clone();
        wrong.complete_rows = None;
        assert!(wrong.validate(&root, &footprint_ref).is_err());
        let mut wrong = footprint.clone();
        wrong.counts.inserted = 0;
        assert!(wrong.validate(&root, &footprint_ref).is_err());
        let mut wrong = footprint.clone();
        wrong.basis.snapshot = Some("99".into());
        assert!(wrong.validate(&root, &footprint_ref).is_err());

        let mut deleted = footprint;
        let metadata = NativeSnapshotRef {
            version: 3,
            root: reference("prior").content,
        }
        .encode()
        .unwrap();
        deleted.mode = MutationMode::Delete;
        deleted.basis = MutationBasis {
            revision: "1".into(),
            snapshot: Some("99".into()),
            root: Some(format!("{:x}", Sha256::digest(metadata.as_bytes()))),
            metadata: Some(metadata),
            rows: 1,
        };
        deleted.result.revision = "2".into();
        deleted.counts = MutationCounts {
            inserted: 0,
            replaced: 0,
            deleted: 1,
        };
        deleted.complete_rows = None;
        deleted.delete_keys = Some(reference("keys"));
        let mut empty = root;
        empty.rows = 0;
        deleted.validate(&empty, &footprint_ref).unwrap();
        deleted.basis.root = Some("0".repeat(64));
        assert!(deleted.validate(&empty, &footprint_ref).is_err());
    }
}
