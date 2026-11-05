//! Standard Iceberg replacement metadata must enumerate exactly the qualified graph selections.
use super::*;
use crate::{snapshot::preflight_avro, storage::ClosureSpool};
use iceberg::spec::{
    DataContentType, DataFileFormat, Manifest, ManifestContentType, ManifestList, ManifestStatus,
};
impl NativeSnapshot {
    pub(crate) async fn validate_replacement(&self, spool: &ClosureSpool) -> Result<()> {
        self.check_current().await?;
        let name = format!("candidate_standard_files_{}", Uuid::new_v4().simple());
        spool.database()?.execute_batch(&format!(
            "CREATE TABLE {name}(name TEXT PRIMARY KEY) WITHOUT ROWID;"
        ))?;
        let head = self
            .metadata
            .current_snapshot()
            .ok_or_else(|| anyhow::anyhow!("SNAPSHOT_PIN_MISMATCH"))?;
        let list = ManifestList::parse_with_version(
            &self.metadata_bytes(head.manifest_list()).await?,
            FormatVersion::V2,
        )?;
        ensure!(list.entries().len() <= 4096, "RESOURCE_LIMIT");
        let mut files = 0u64;
        let mut rows = 0u64;
        for file in list.entries() {
            ensure!(
                file.content == ManifestContentType::Data && file.key_metadata.is_none(),
                "DATASET_WRITE_CAPABILITY_REQUIRED"
            );
            let manifest = Manifest::parse_avro(&self.metadata_bytes(&file.manifest_path).await?)?;
            ensure!(
                manifest.metadata().format_version == FormatVersion::V2
                    && manifest.metadata().partition_spec.is_unpartitioned(),
                "DATASET_WRITE_CAPABILITY_REQUIRED"
            );
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let data = entry.data_file();
                ensure!(
                    data.content_type() == DataContentType::Data
                        && data.file_format() == DataFileFormat::Parquet
                        && data.partition().iter().len() == 0,
                    "DATASET_WRITE_CAPABILITY_REQUIRED"
                );
                let sequence = entry
                    .sequence_number
                    .or_else(|| {
                        if entry.status == ManifestStatus::Added || file.sequence_number == 0 {
                            Some(file.sequence_number)
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
                let selection = self.selection(entry.file_path()).await?;
                ensure!(
                    selection.content().size == data.file_size_in_bytes()
                        && selection.physical_rows() == data.record_count()
                        && selection.selected_rows() == data.record_count()
                        && selection.data_sequence() == u64::try_from(sequence)?
                        && sequence == head.sequence_number(),
                    "DATASET_KEY_INDEX_PROOF_REQUIRED"
                );
                spool.database()?.execute(
                    &format!("INSERT INTO {name}(name)VALUES(?1)"),
                    [entry.file_path()],
                )?;
                files = files
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
                rows = rows
                    .checked_add(data.record_count())
                    .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
            }
        }
        ensure!(
            files == self.descriptor.data_files && rows == self.descriptor.rows,
            "PARQUET_COUNT_MISMATCH"
        );
        self.check_current().await
    }
    async fn metadata_bytes(&self, name: &str) -> Result<Vec<u8>> {
        let file = self.file(name).await?;
        ensure!(file.content().size <= 8 * 1024 * 1024, "RESOURCE_LIMIT");
        let mut stream = file.stream().await?;
        let mut bytes = Vec::new();
        while let Some(frame) = stream.next().await {
            let frame = frame?;
            ensure!(
                bytes.len() + frame.len() <= 8 * 1024 * 1024,
                "RESOURCE_LIMIT"
            );
            bytes.extend_from_slice(&frame);
        }
        preflight_avro(&bytes)?;
        Ok(bytes)
    }
    pub(crate) fn preparation_proof(
        &self,
        stage: Uuid,
        reference: &crate::catalog::NativeSnapshotRef,
    ) -> Result<Value> {
        ensure!(
            reference.version == 3 && reference.root == *self.root.content(),
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        Ok(
            serde_json::json!({"version":3,"readerProfile":PROFILE,"root":reference.root,"index":self.descriptor.index_summary,"stage":stage,"tableUuid":self.descriptor.table_uuid,"snapshotId":self.descriptor.snapshot_id,"sequence":self.descriptor.sequence,"schemaHash":self.descriptor.schema_hash,"keySchemaHash":self.descriptor.key_schema_hash,"primaryKey":self.descriptor.primary_key,"dataFiles":self.descriptor.data_files,"rows":self.descriptor.rows}),
        )
    }
}
