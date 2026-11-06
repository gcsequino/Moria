//! Metadata-only pruning under an actual fenced maintenance run.
use super::{
    operator::{self, Expiration, Grant},
    *,
};
use crate::{
    dataset_graph::NativeGraph,
    dataset_graph_read::GraphReadContext,
    native_snapshot_v3::{LogicalFile, NativeSnapshot},
    persistent_directory::graph::{GraphDirectory, GraphValue},
    storage::{ClosureLimits, LogicalObject, PosixStore, StorageLimits},
};
use futures::StreamExt;
use iceberg::spec::{FormatVersion, Manifest, ManifestList};
use std::collections::BTreeMap;
use uuid::Uuid;

async fn bytes(snapshot: &NativeSnapshot, name: &str) -> Result<Vec<u8>> {
    let file = snapshot.file(name).await?;
    ensure!(
        file.content().size <= 8 * 1024 * 1024,
        "METADATA_BYTE_LIMIT"
    );
    let mut stream = file.stream().await?;
    let mut result = Vec::with_capacity(file.content().size as usize);
    while let Some(frame) = stream.next().await {
        let frame = frame?;
        ensure!(
            result.len() + frame.len() <= file.content().size as usize,
            "CONTENT_MISMATCH"
        );
        result.extend_from_slice(&frame);
    }
    ensure!(
        result.len() as u64 == file.content().size,
        "CONTENT_MISMATCH"
    );
    file.check_current().await?;
    Ok(result)
}
fn mark(spool: &ClosureSpool, name: &str) -> Result<()> {
    ensure!(
        !name.is_empty() && name.len() <= 4096 && name.starts_with("moria://"),
        "DATASET_GRAPH_INVALID"
    );
    spool.database()?.execute(
        "INSERT OR IGNORE INTO retained_standard_files(name)VALUES(?1)",
        [name],
    )?;
    Ok(())
}
async fn retained_files(
    snapshot: &NativeSnapshot,
    pruned: &Value,
    spool: &ClosureSpool,
) -> Result<()> {
    spool.database()?.execute_batch(
        "CREATE TABLE retained_standard_files(name TEXT PRIMARY KEY) WITHOUT ROWID;",
    )?;
    for head in pruned["snapshots"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("MISSING_SNAPSHOTS"))?
    {
        snapshot.check_current().await?;
        let list_name = head["manifest-list"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("MISSING_MANIFEST_LIST"))?;
        mark(spool, list_name)?;
        let encoded = bytes(snapshot, list_name).await?;
        crate::snapshot::preflight_avro(&encoded)?;
        let list = ManifestList::parse_with_version(&encoded, FormatVersion::V2)?;
        ensure!(list.entries().len() <= 4096, "RESOURCE_LIMIT");
        for entry in list.entries() {
            let first = spool.database()?.execute(
                "INSERT OR IGNORE INTO retained_standard_files(name)VALUES(?1)",
                [&entry.manifest_path],
            )?;
            if first == 0 {
                continue;
            }
            let encoded = bytes(snapshot, &entry.manifest_path).await?;
            ensure!(
                u64::try_from(entry.manifest_length)? == encoded.len() as u64,
                "CONTENT_MISMATCH"
            );
            crate::snapshot::preflight_avro(&encoded)?;
            let manifest = Manifest::parse_avro(&encoded)?;
            for data in manifest.entries() {
                let name = data.data_file().file_path();
                let file = snapshot.file(name).await?;
                ensure!(
                    file.content().size == data.data_file().file_size_in_bytes(),
                    "CONTENT_MISMATCH"
                );
                mark(spool, name)?;
            }
        }
    }
    Ok(())
}
pub(super) async fn expire(
    grant: Arc<Grant>,
    input: &Expiration,
    selected: &[String],
    store: Arc<dyn ObjectStore>,
) -> Result<Value> {
    let (context, root) = GraphReadContext::maintenance(
        store.clone(),
        grant.db.clone(),
        grant.id,
        grant.owner,
        grant.fence,
        grant.incarnation,
        grant.cancel.clone(),
    )
    .await?;
    let snapshot = NativeSnapshot::open(context, root).await?;
    let schema: iceberg::spec::Schema =
        serde_json::from_value(grant.state["icebergSchema"].clone())?;
    ensure!(
        snapshot.metadata().current_schema().as_ref() == &schema
            && snapshot.metadata().uuid().to_string() == grant.state["tableUuid"].as_str().unwrap(),
        "SCHEMA_CHANGED"
    );
    let original: Value =
        serde_json::from_slice(&bytes(&snapshot, &snapshot.metadata_file().name).await?)?;
    let keep =
        operator::retained_graph_snapshots(&grant, &original, snapshot.snapshot_id()?, selected)
            .await?;
    let pruned = operator::prune_metadata(original.clone(), &keep)?;
    if pruned == original {
        return operator::commit_without_head(grant, input, selected).await;
    }
    let graph = NativeGraph::maintenance(store.clone(), grant.clone()).await?;
    let scratch_root = std::env::var("MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT")
        .map_err(|_| anyhow::anyhow!("SNAPSHOT_SCRATCH_UNAVAILABLE"))?;
    let scratch = PosixStore::provision(
        std::path::Path::new(&scratch_root),
        &grant.namespace.warehouse_id,
        &grant.namespace.table_id,
        StorageLimits {
            max_object_bytes: 128 * 1024 * 1024,
            chunk_bytes: 1024 * 1024,
        },
    )?;
    let limits: ClosureLimits = serde_json::from_str(
        &std::env::var("MORIA_NATIVE_CLOSURE_LIMITS")
            .map_err(|_| anyhow::anyhow!("SNAPSHOT_BUDGET_UNAVAILABLE"))?,
    )?;
    let spool = ClosureSpool::create(
        &scratch,
        LogicalObject::new(
            &grant.namespace.warehouse_id,
            &grant.namespace.table_id,
            &Uuid::new_v4().to_string(),
        )?,
        limits,
        grant.clone(),
        grant.cancel.clone(),
    )
    .await?;
    retained_files(&snapshot, &pruned, &spool).await?;
    let metadata = operator::put(store.as_ref(), grant.clone(), &pruned).await?;
    let metadata_name = format!(
        "moria://{}/{}/metadata/{}.metadata.json",
        grant.namespace.warehouse_id,
        grant.namespace.table_id,
        Uuid::new_v4()
    );
    let certificate = graph.certify_new_file(metadata.clone()).await?;
    let directory = GraphDirectory::new(graph.clone(), vec!["files".into()])?;
    let mut files = Some(snapshot.files_reference().clone());
    let mut entries = snapshot.files().entries();
    let mut mutations = BTreeMap::new();
    // Stream only directory metadata; the retained name set is spill-backed and Parquet bytes are never scanned.
    while let Some(entry) = entries.next().await {
        let (name, _) = entry?;
        let keep: bool = spool.database()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM retained_standard_files WHERE name=?1)",
            [&name],
            |row| row.get(0),
        )?;
        if !keep {
            mutations.insert(name, None);
        }
        if mutations.len() == 256 {
            files = directory
                .apply(files.as_ref(), std::mem::take(&mut mutations))
                .await?;
        }
    }
    mutations.insert(
        metadata_name.clone(),
        Some(GraphValue::new(
            serde_json::to_value(LogicalFile {
                name: metadata_name.clone(),
                content: metadata.clone(),
            })?,
            vec![certificate],
        )?),
    );
    files = directory.apply(files.as_ref(), mutations).await?;
    let files = files.ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_INVALID"))?;
    let mut descriptor = snapshot.descriptor().clone();
    // A metadata-only edge keeps the standard snapshot and indexes, not a previous
    // mutation's revision-bound footprint. The original result root remains pinnable.
    descriptor.mutation_footprint = None;
    descriptor.metadata = LogicalFile {
        name: metadata_name,
        content: metadata,
    };
    descriptor.files = files.clone();
    let mut children = vec![("files".into(), directory.qualified_root(&files).await?)];
    for (label, content) in [
        (
            "selections",
            descriptor.selections.as_ref().map(|v| &v.content),
        ),
        (
            "hashes",
            descriptor
                .visibility
                .as_ref()
                .and_then(|v| v.hashes.as_ref())
                .map(|v| &v.content),
        ),
        (
            "locations",
            descriptor
                .visibility
                .as_ref()
                .and_then(|v| v.locations.as_ref())
                .map(|v| &v.content),
        ),
        ("index", descriptor.index_summary.as_ref().map(|v| &v.root)),
    ] {
        if let Some(content) = content {
            children.push((
                label.into(),
                graph.admit_head(&[label.into()], content).await?,
            ));
        }
    }
    let node = graph
        .write_descriptor(
            "root",
            &serde_json::to_value(&descriptor)?,
            children,
            descriptor.rows,
            0,
        )
        .await?;
    let stage = Uuid::new_v4();
    graph.begin_stage(stage, &node).await?;
    while graph.advance(stage, 256).await?["ready"] != true {
        graph.check_current().await?;
    }
    let reference = super::register_graph_root(&graph, &node).await?;
    operator::copy_key_certificate(&grant, &reference, snapshot.snapshot_id()?).await?;
    let location = reference.encode()?;
    let mut client = grant.db.get().await?;
    let tx = client.transaction().await?;
    super::admission(tx.client(), false).await?;
    graph.attach_maintenance(&tx, stage, &location).await?;
    tx.commit().await?;
    let (context, root) = GraphReadContext::staged_maintenance(store, grant.clone(), stage).await?;
    let qualified = NativeSnapshot::open(context, root).await?;
    ensure!(
        qualified.descriptor().index_summary == snapshot.descriptor().index_summary,
        "NATIVE_INDEX_SUMMARY_CHANGED"
    );
    ensure!(
        qualified.snapshot_id()? == snapshot.snapshot_id()?
            && qualified.row_count() == snapshot.row_count(),
        "SNAPSHOT_MISMATCH"
    );
    qualified.check_current().await?;
    operator::commit_pruned(
        grant,
        input,
        selected,
        &original,
        &pruned,
        snapshot.metadata(),
        &reference,
    )
    .await
}
