//! Explicit bounded bootstrap from an independently verified native v2 snapshot.
//! This path scans standard data files, never an imported accelerator, before minting a v3 sidecar.
use super::graph_builder::GraphBuilder as Builder;
use super::{
    row_selection, rows,
    visibility::{BootstrapBudget, VisibilityBootstrap, VisibleRow},
};
use crate::{
    catalog::{DatasetIoGrant, NativeSnapshotRef, load_qualified_snapshot},
    dataset_graph::NativeGraph,
    native_snapshot_v3::{LogicalFile, PROFILE, RootDescriptor, VisibilityIndex},
    persistent_directory::graph::{GraphDirectory, GraphValue},
    snapshot::{columns, preflight_avro},
    storage::{Cancellation, ClosureLimits, ClosureSpool, ObjectStore, PosixStore},
};
use anyhow::{Result, ensure};
use futures::TryStreamExt;
use iceberg::{
    scan::FileScanTask,
    spec::{
        DataContentType, DataFileFormat, FormatVersion, Manifest, ManifestContentType,
        ManifestList, ManifestStatus,
    },
    table::StaticTable,
};
use rusqlite::params;
use serde_json::json;
use sha2::Digest;
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;
#[derive(Clone, Debug)]
pub struct BootstrapReceipt {
    pub root: String,
    pub graph_node: String,
    pub rows: u64,
}
/// Actual accepted producer and expected head are required; no bootstrap lease or issuance is invented.
pub async fn bootstrap_expected_head(
    store: Arc<dyn ObjectStore>,
    scratch: &PosixStore,
    grant: Arc<DatasetIoGrant>,
    limits: ClosureLimits,
    budget: BootstrapBudget,
    cancel: Cancellation,
) -> Result<BootstrapReceipt> {
    grant.check_current().await?;
    ensure!(grant.lease().task.is_none(), "COORDINATOR_REQUIRED");
    grant.validate_store(store.as_ref())?;
    ensure!(
        grant.expected_revision()? > 0,
        "DATASET_WRITE_CAPABILITY_REQUIRED"
    );
    let db = grant.database();
    let row=db.get().await?.query_one("SELECT revision,metadata_location,snapshot_id FROM moria.dataset_heads WHERE environment=$1 AND dataset=$2",&[&grant.session().environment(),&grant.resource()]).await?;
    ensure!(
        row.get::<_, i64>(0) == grant.expected_revision()?,
        "HEAD_CONFLICT"
    );
    let location: String = row.get(1);
    let snapshot: String = row.get(2);
    let reference = NativeSnapshotRef::decode(&location)?;
    let graph = NativeGraph::acquire(store.clone(), grant.clone(), cancel.clone()).await?;
    let loaded = load_qualified_snapshot(
        scratch,
        store.clone(),
        grant.clone(),
        &reference,
        grant.table_uuid()?,
        snapshot.parse()?,
        limits,
        cancel.clone(),
        &graph,
    )
    .await?;
    let table = loaded.table().await?;
    grant.validate_schema(table.metadata().current_schema().as_ref())?;
    let sequence = u64::try_from(
        table
            .metadata()
            .snapshot_by_id(snapshot.parse()?)
            .ok_or_else(|| anyhow::anyhow!("SNAPSHOT_PIN_MISMATCH"))?
            .sequence_number(),
    )?;
    let suffix = Uuid::new_v4().simple().to_string();
    let files_table = format!("bootstrap_files_{suffix}");
    let spool = loaded.spool().clone();
    {
        let db = spool.database()?;
        spool.assert_scope(&db)?;
        db.execute_batch(&format!("CREATE TABLE {files_table}(name TEXT PRIMARY KEY,rows INTEGER NOT NULL,sequence INTEGER NOT NULL,scanned INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID;"))?;
    }
    let file_count = inventory(
        &spool,
        &table,
        snapshot.parse()?,
        &files_table,
        budget.maximum_data_rows,
    )
    .await?;
    let mut builder = Builder {
        graph: graph.clone(),
        files: GraphDirectory::new(graph.clone(), vec!["files".into()])?,
        selections: GraphDirectory::new(graph.clone(), vec!["selections".into()])?,
        files_root: None,
        selections_root: None,
        spool: spool.clone(),
        hashes: GraphDirectory::new(graph.clone(), vec!["hashes".into()])?,
        locations: GraphDirectory::new(graph.clone(), vec!["locations".into()])?,
        hashes_root: None,
        locations_root: None,
        sequence,
        key_hash: grant.key_schema_hash()?.into(),
    };
    let keys = grant.primary_key()?;
    let descriptors = serde_json::to_value(columns(&serde_json::to_value(
        table.metadata().current_schema().as_ref(),
    )?)?)?;
    let mut visible = if keys.is_empty() {
        None
    } else {
        Some(VisibilityBootstrap::create(spool.clone(), grant.clone(), budget).await?)
    };
    let mut after = None;
    loop {
        spool.check_current(grant.as_ref()).await?;
        let entries = spool.read_page(after.as_deref(), grant.as_ref()).await?;
        if entries.is_empty() {
            break;
        }
        for entry in &entries {
            builder.file(&entry.name, entry.content.clone()).await?;
        }
        after = entries.last().map(|entry| entry.name.clone());
    }
    let metadata_name = format!(
        "moria://{}/{}/metadata/{}",
        grant.namespace().warehouse_id,
        grant.namespace().table_id,
        loaded.proof().metadata.object.object_id
    );
    let metadata = LogicalFile {
        name: metadata_name.clone(),
        content: loaded.proof().metadata.clone(),
    };
    builder
        .file(&metadata_name, metadata.content.clone())
        .await?;
    let mut after_file = None;
    let mut rows = 0u64;
    loop {
        grant.check_current().await?;
        let page = {
            let db = spool.database()?;
            let query = format!(
                "SELECT name,rows,sequence FROM {}{} ORDER BY name LIMIT 128",
                files_table,
                if after_file.is_some() {
                    " WHERE name>?1"
                } else {
                    ""
                }
            );
            let mut statement = db.prepare(&query)?;
            let map = |row: &rusqlite::Row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            };
            match &after_file {
                Some(after) => statement
                    .query_map([after], map)?
                    .collect::<rusqlite::Result<Vec<_>>>()?,
                None => statement
                    .query_map([], map)?
                    .collect::<rusqlite::Result<Vec<_>>>()?,
            }
        };
        if page.is_empty() {
            break;
        }
        for (name, count, data_sequence) in &page {
            let content = spool.lookup(name)?.content;
            let (_, mut links) = builder
                .files
                .lookup_with_links(
                    builder
                        .files_root
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_INVALID"))?,
                    name,
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_INVALID"))?;
            ensure!(links.len() == 1, "DATASET_GRAPH_ANCESTRY_REQUIRED");
            let certificate = links.remove(0);
            let selection = row_selection::write_all(
                &graph,
                LogicalFile {
                    name: name.clone(),
                    content: content.clone(),
                },
                certificate,
                u64::try_from(*count)?,
                u64::try_from(*data_sequence)?,
            )
            .await?;
            let value = GraphValue::new(json!({"name":name}), vec![selection])?;
            builder.selections_root = builder
                .selections
                .apply(
                    builder.selections_root.as_ref(),
                    BTreeMap::from([(name.clone(), Some(value))]),
                )
                .await?;
            let task = FileScanTask::builder()
                .with_file_size_in_bytes(content.size)
                .with_start(0)
                .with_length(content.size)
                .with_record_count(Some(u64::try_from(*count)?))
                .with_data_file_path(name.clone())
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(table.metadata().current_schema().clone())
                .with_project_field_ids(
                    table
                        .metadata()
                        .current_schema()
                        .as_struct()
                        .fields()
                        .iter()
                        .filter(|field| {
                            keys.is_empty() || keys.iter().any(|key| key == &field.name)
                        })
                        .map(|field| field.id)
                        .collect(),
                )
                .with_case_sensitive(true)
                .build();
            let mut batches = table
                .reader_builder()
                .with_batch_size(1000)
                .with_data_file_concurrency_limit(1)
                .with_range_fetch_concurrency(1)
                .build()
                .read(Box::pin(futures::stream::iter([Ok(task)])))?
                .stream();
            let mut physical = 0u64;
            while let Some(batch) = batches.try_next().await? {
                grant.check_current().await?;
                if let Some(visible) = visible.as_mut() {
                    let canonical = rows::encode_projected_keys(&batch, &keys, &descriptors)?;
                    let page = canonical
                        .iter()
                        .enumerate()
                        .map(|(index, row)| VisibleRow {
                            key: row.clone(),
                            file: name.clone(),
                            row: physical + index as u64,
                            data_sequence: u64::try_from(*data_sequence).unwrap(),
                            row_hash: None,
                        })
                        .collect::<Vec<_>>();
                    visible.data_page(&page).await?;
                } else {
                    crate::snapshot::validate_batch(
                        &batch,
                        &columns(&serde_json::to_value(
                            table.metadata().current_schema().as_ref(),
                        )?)?,
                    )?;
                }
                physical += batch.num_rows() as u64;
            }
            ensure!(physical == u64::try_from(*count)?, "PARQUET_COUNT_MISMATCH");
            rows = rows
                .checked_add(physical)
                .filter(|n| *n <= budget.maximum_data_rows)
                .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
            spool.database()?.execute(
                &format!("UPDATE {} SET scanned=1 WHERE name=?1", files_table),
                [name],
            )?;
        }
        after_file = page.last().map(|row| row.0.clone());
    }
    ensure!(
        spool.database()?.query_row(
            &format!("SELECT count(*) FROM {} WHERE scanned<>1", files_table),
            [],
            |row| row.get::<_, i64>(0)
        )? == 0,
        "DATASET_KEY_INDEX_PROOF_REQUIRED"
    );
    if let Some(visible) = visible.as_mut() {
        ensure!(visible.seal().await? == rows, "PARQUET_COUNT_MISMATCH");
        builder.index(visible).await?;
    }
    let visibility = if keys.is_empty() {
        None
    } else {
        Some(VisibilityIndex {
            rows,
            hashes: builder.hashes_root.clone(),
            locations: builder.locations_root.clone(),
        })
    };
    let files = builder
        .files_root
        .clone()
        .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_INVALID"))?;
    let descriptor = RootDescriptor {
        version: 3,
        reader_profile: PROFILE.into(),
        profile: graph.profile().clone(),
        table_uuid: grant.table_uuid()?,
        snapshot_id: snapshot,
        sequence: sequence.to_string(),
        schema_hash: super::schema_hash(table.metadata().current_schema().as_ref())?,
        key_schema_hash: grant.key_schema_hash()?.into(),
        primary_key: keys.clone(),
        data_files: file_count,
        rows,
        selections: builder.selections_root.clone(),
        metadata,
        files: files.clone(),
        visibility,
        index_summary: None,
        mutation_footprint: None,
    };
    let mut children = vec![("files".into(), builder.files.qualified_root(&files).await?)];
    for (name, reference, directory) in [
        ("hashes", &builder.hashes_root, &builder.hashes),
        ("locations", &builder.locations_root, &builder.locations),
        ("selections", &builder.selections_root, &builder.selections),
    ] {
        if let Some(reference) = reference {
            children.push((name.into(), directory.qualified_root(reference).await?));
        }
    }
    let root = graph
        .write_descriptor(
            "root",
            &serde_json::to_value(descriptor)?,
            children,
            rows,
            loaded.proof().closure.referenced_bytes,
        )
        .await?;
    let stage = Uuid::new_v4();
    graph.begin_stage(stage, &root).await?;
    while graph.advance(stage, 256).await?["ready"] != true {
        grant.check_current().await?;
    }
    let graph_node = graph.finish_bootstrap(stage).await?;
    // Concurrent duplicate bootstrap may retire its private redundant stage; finish its bounded propagation too.
    while graph.advance(stage, 256).await?["ready"] != true {
        grant.check_current().await?;
    }
    grant.check_current().await?;
    Ok(BootstrapReceipt {
        root: format!("{:x}", sha2::Sha256::digest(location.as_bytes())),
        graph_node,
        rows,
    })
}
async fn inventory(
    spool: &ClosureSpool,
    table: &StaticTable,
    snapshot: i64,
    table_name: &str,
    maximum_rows: u64,
) -> Result<u64> {
    let metadata = table.metadata();
    let io = table.clone().into_table().file_io().clone();
    let current = metadata
        .snapshot_by_id(snapshot)
        .ok_or_else(|| anyhow::anyhow!("SNAPSHOT_PIN_MISMATCH"))?;
    let bytes = io.new_input(current.manifest_list())?.read().await?;
    ensure!(bytes.len() <= 8 * 1024 * 1024, "RESOURCE_LIMIT");
    preflight_avro(&bytes)?;
    let list = ManifestList::parse_with_version(&bytes, FormatVersion::V2)?;
    ensure!(list.entries().len() <= 4096, "RESOURCE_LIMIT");
    let mut files = 0u64;
    let mut rows = 0u64;
    for file in list.entries() {
        ensure!(
            file.content == ManifestContentType::Data && file.key_metadata.is_none(),
            "DATASET_WRITE_CAPABILITY_REQUIRED"
        );
        let bytes = io.new_input(&file.manifest_path)?.read().await?;
        ensure!(bytes.len() <= 8 * 1024 * 1024, "RESOURCE_LIMIT");
        preflight_avro(&bytes)?;
        let manifest = Manifest::parse_avro(&bytes)?;
        ensure!(
            manifest.metadata().partition_spec.is_unpartitioned()
                && manifest.metadata().format_version == FormatVersion::V2,
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
            ensure!(
                sequence >= 0 && sequence <= current.sequence_number(),
                "DATASET_KEY_INDEX_INVALID"
            );
            let content = spool.lookup(entry.file_path())?.content;
            ensure!(
                content.size == data.file_size_in_bytes() && data.record_count() <= 8 * 1024 * 1024,
                "DATASET_KEY_INDEX_INVALID"
            );
            spool.database()?.execute(
                &format!(
                    "INSERT INTO {}(name,rows,sequence)VALUES(?1,?2,?3)",
                    table_name
                ),
                params![
                    entry.file_path(),
                    i64::try_from(data.record_count())?,
                    sequence
                ],
            )?;
            files += 1;
            rows = rows
                .checked_add(data.record_count())
                .filter(|rows| *rows <= maximum_rows)
                .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        }
    }
    Ok(files)
}
