//! The same actual sealed task ancestry is used by replacement and sparse preparation.
use super::{Part, PartPage, PartRoot};
use crate::{
    dataset_graph::{NativeGraph, QualifiedNode},
    native_snapshot_v3::LogicalFile,
    persistent_directory::graph::GraphDirectory,
    storage::SealedObject,
};
use anyhow::{Result, ensure};
use serde_json::json;
use uuid::Uuid;

pub(super) async fn root(
    graph: &NativeGraph,
    task: Uuid,
    reference: &SealedObject,
) -> Result<PartRoot> {
    let node = graph.admit_task(task, &[], reference).await?;
    let root: PartRoot = serde_json::from_value(graph.read_descriptor(&node).await?)?;
    ensure!(
        root.version == 3
            && root.task == task
            && (root.parts == 0) == root.graph_files.is_none()
            && (root.pages == 0) == root.graph_pages.is_none()
            && root.graph_files.as_ref().map_or(0, |node| node.entries) == root.parts
            && root.graph_pages.as_ref().map_or(0, |node| node.entries) == root.pages
            && (root.parts == 0) == (root.pages == 0)
            && (root.parts != 0 || root.rows == 0),
        "OUTPUT_INVALID"
    );
    Ok(root)
}

pub(super) async fn page(
    graph: &NativeGraph,
    root: &PartRoot,
    ordinal: u64,
    reference: &SealedObject,
) -> Result<PartPage> {
    let directory = GraphDirectory::sealed_task(graph.clone(), vec!["pages".into()], root.task)?;
    let pages = root
        .graph_pages
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?;
    let (value, mut links) = directory
        .lookup_with_links(pages, &format!("{ordinal:016x}"))
        .await?
        .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?;
    ensure!(
        value == json!({"ordinal":ordinal}) && links.len() == 1,
        "OUTPUT_INVALID"
    );
    let node = links.remove(0);
    ensure!(
        node.kind() == "partset" && node.content() == reference,
        "OUTPUT_INVALID"
    );
    serde_json::from_value(graph.read_descriptor(&node).await?).map_err(Into::into)
}

pub(super) async fn certificate(
    graph: &NativeGraph,
    root: &PartRoot,
    part: &Part,
) -> Result<QualifiedNode> {
    let directory = GraphDirectory::sealed_task(graph.clone(), vec!["files".into()], root.task)?;
    let reference = root
        .graph_files
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?;
    let (value, mut links) = directory
        .lookup_with_links(reference, &part.name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?;
    let file: LogicalFile = serde_json::from_value(value)?;
    ensure!(
        file.name == part.name && file.content == part.content && links.len() == 1,
        "OUTPUT_INVALID"
    );
    Ok(links.remove(0))
}
