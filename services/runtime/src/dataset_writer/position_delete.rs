//! Standard Iceberg v2 position-delete Parquet. Only a qualified touched-row proof may be sealed.
use super::{PART_LOGICAL_BYTES, PartBuffer};
use crate::{
    dataset_graph::{NativeGraph, QualifiedNode},
    native_snapshot_v3::{LogicalFile, PROFILE, RootDescriptor, VerifiedPosition},
    persistent_directory::graph::GraphDirectory,
    storage::{ObjectVersion, SealedObject},
};
use anyhow::{Result, ensure};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use iceberg::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum};
use parquet::{
    arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY},
    file::properties::WriterProperties,
};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

const FILE_PATH_ID: i32 = 2_147_483_546;
const POSITION_ID: i32 = 2_147_483_545;
const MAX_ROWS: usize = 1000;

pub(super) struct DeletePart {
    pub(super) file: LogicalFile,
    pub(super) data_file: DataFile,
    pub(super) certificate: QualifiedNode,
}

fn encode(rows: &[(String, i64)]) -> Result<Vec<u8>> {
    ensure!(!rows.is_empty() && rows.len() <= MAX_ROWS, "RESOURCE_LIMIT");
    ensure!(
        rows.windows(2).all(|pair| pair[0] < pair[1]),
        "DATASET_KEY_INDEX_INVALID"
    );
    rows.iter().try_fold(0usize, |bytes, (path, row)| {
        ensure!(
            path.starts_with("moria://") && path.len() <= 4096 && *row >= 0,
            "DATASET_KEY_INDEX_INVALID"
        );
        bytes
            .checked_add(path.len() + 8)
            .filter(|bytes| *bytes <= PART_LOGICAL_BYTES)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))
    })?;
    let field = |name, ty, id: i32| {
        Field::new(name, ty, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.into(),
            id.to_string(),
        )]))
    };
    let schema = Arc::new(Schema::new(vec![
        field("file_path", DataType::Utf8, FILE_PATH_ID),
        field("pos", DataType::Int64, POSITION_ID),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(path, _)| path.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, pos)| *pos),
            )),
        ],
    )?;
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(MAX_ROWS))
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(PartBuffer::default(), schema, Some(properties))?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?.0)
}

pub(super) async fn seal(
    graph: &NativeGraph,
    witnesses: Vec<VerifiedPosition>,
    sequence: u64,
) -> Result<DeletePart> {
    graph.check_current().await?;
    ensure!(
        !witnesses.is_empty() && witnesses.len() <= MAX_ROWS,
        "RESOURCE_LIMIT"
    );
    let basis = witnesses[0].basis().clone();
    ensure!(
        witnesses
            .iter()
            .all(|witness| witness.basis() == &basis && witness.next_sequence() == sequence),
        "DATASET_GRAPH_ANCESTRY_REQUIRED"
    );
    // The opaque witness names the actual captured root, and the coordinator must still
    // prove that exact root is its own expected head under current authority.
    let root = graph.admit_head(&[], &basis).await?;
    ensure!(root.kind() == "root", "DATASET_GRAPH_ANCESTRY_REQUIRED");
    let descriptor: RootDescriptor = serde_json::from_value(graph.read_descriptor(&root).await?)?;
    ensure!(
        descriptor.reader_profile == PROFILE && descriptor.sequence.parse::<u64>()? < sequence,
        "DATASET_KEY_INDEX_INVALID"
    );
    let mut target: Option<(String, SealedObject, ObjectVersion)> = None;
    let mut rows = Vec::with_capacity(witnesses.len());
    for witness in witnesses {
        let (name, row, data_sequence, content, version) = witness.into_parts();
        ensure!(data_sequence <= sequence, "DATASET_KEY_INDEX_INVALID");
        if let Some((prior_name, prior_content, prior_version)) = &target {
            ensure!(
                prior_name == &name && prior_content == &content && prior_version == &version,
                "DATASET_GRAPH_ANCESTRY_REQUIRED"
            );
        } else {
            target = Some((name.clone(), content, version));
        }
        rows.push((name, i64::try_from(row)?));
    }
    let (target_path, content, version) = target.unwrap();
    let directory = GraphDirectory::new(graph.clone(), vec!["files".into()])?;
    let (value, links) = directory
        .lookup_with_links(&descriptor.files, &target_path)
        .await?
        .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_ANCESTRY_REQUIRED"))?;
    let file: LogicalFile = serde_json::from_value(value)?;
    ensure!(
        file.name == target_path && file.content == content && links.len() == 1,
        "DATASET_GRAPH_ANCESTRY_REQUIRED"
    );
    let (proof, qualified_version, _, _) = graph.file_proof(&links[0]).await?.into_parts();
    ensure!(
        proof.content() == &content && qualified_version == version,
        "DATASET_GRAPH_ANCESTRY_REQUIRED"
    );
    rows.sort_unstable();
    let bytes = encode(&rows)?;
    let (content, certificate) = graph.write_parquet_bytes(&bytes).await?;
    let namespace = graph.grant().namespace();
    let name = format!(
        "moria://{}/{}/delete/{}",
        namespace.warehouse_id,
        namespace.table_id,
        Uuid::new_v4()
    );
    let data_file = DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(name.clone())
        .file_format(DataFileFormat::Parquet)
        .record_count(rows.len() as u64)
        .file_size_in_bytes(content.size)
        // Exact path metrics keep ordinary Iceberg planning from pairing this delete
        // file with every older file in the unpartitioned Dataset.
        .lower_bounds(HashMap::from([(FILE_PATH_ID, Datum::string(&target_path))]))
        .upper_bounds(HashMap::from([(FILE_PATH_ID, Datum::string(&target_path))]))
        .build()?;
    graph.check_current().await?;
    Ok(DeletePart {
        file: LogicalFile { name, content },
        data_file,
        certificate,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[test]
    fn standard_position_fields_are_required_exact_and_sorted() {
        let rows = vec![
            ("moria://w/t/data/a".to_owned(), 0),
            ("moria://w/t/data/a".to_owned(), 4096),
            ("moria://w/t/data/b".to_owned(), 1),
        ];
        assert!(encode(&[]).is_err());
        assert!(encode(&[rows[1].clone(), rows[0].clone()]).is_err());
        assert!(encode(&[rows[0].clone(), rows[0].clone()]).is_err());
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::from(encode(&rows).unwrap())).unwrap();
        let schema = builder.parquet_schema().root_schema().get_fields();
        assert_eq!(schema.len(), 2);
        for (field, name, id) in [(0, "file_path", FILE_PATH_ID), (1, "pos", POSITION_ID)] {
            assert_eq!(schema[field].name(), name);
            assert_eq!(schema[field].get_basic_info().id(), id);
            assert_eq!(
                schema[field].get_basic_info().repetition(),
                parquet::basic::Repetition::REQUIRED
            );
        }
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        let paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let positions = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(batch.num_rows(), rows.len());
        for (index, (path, position)) in rows.iter().enumerate() {
            assert_eq!(paths.value(index), path);
            assert_eq!(positions.value(index), *position);
        }
        assert!(reader.next().is_none());
    }
}
