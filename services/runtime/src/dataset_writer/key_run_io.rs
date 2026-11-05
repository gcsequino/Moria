//! Qualified packed run IO. Imported descriptors cannot become live index state through this API.
use super::key_run::*;
use crate::{
    dataset_graph::{NativeGraph, QualifiedNode},
    dataset_graph_read::{GraphFile, GraphReadContext, ReadableNode},
    storage::SealedObject,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredRun {
    version: u32,
    descriptor: RunDescriptor,
    content: SealedObject,
}
/// Closed writer receipt. The native visibility qualifier must separately prove logical membership.
#[derive(Clone, Debug)]
pub struct QualifiedRun {
    descriptor: RunDescriptor,
    node: QualifiedNode,
}
impl QualifiedRun {
    pub fn descriptor(&self) -> &RunDescriptor {
        &self.descriptor
    }
    pub fn node(&self) -> &QualifiedNode {
        &self.node
    }
}
pub(crate) async fn write_run(
    graph: &NativeGraph,
    kind: RunKind,
    key_schema_hash: &str,
    sequence: u64,
    entries: &[RunEntry],
) -> Result<QualifiedRun> {
    ensure!(
        !entries.is_empty() && entries.len() <= 65536,
        "RESOURCE_LIMIT"
    );
    let mut writer = RunWriter::new(kind, key_schema_hash, sequence)?;
    for entry in entries {
        writer.push(entry)?;
    }
    let (descriptor, bytes) = writer.finish()?;
    // Independently decode every emitted block/fence before receiver registration.
    verify_complete_run(&descriptor, &bytes)?;
    let (file, certificate) = graph.write_index_bytes(&bytes).await?;
    let value = StoredRun {
        version: 2,
        descriptor: descriptor.clone(),
        content: file.certificate().content().clone(),
    };
    let node = graph
        .write_descriptor(
            "keyRun",
            &serde_json::to_value(value)?,
            vec![("certificate".into(), certificate)],
            descriptor.entries,
            bytes.len() as u64,
        )
        .await?;
    graph.check_current().await?;
    Ok(QualifiedRun { descriptor, node })
}
/// Root-path-bound run reader. Every independent operation freshly checks accepted authority.
pub struct RunReader {
    descriptor: RunDescriptor,
    file: GraphFile,
}
impl RunReader {
    pub async fn open(
        context: &GraphReadContext,
        node: &ReadableNode,
        key_schema_hash: &str,
        kind: RunKind,
    ) -> Result<Self> {
        ensure!(node.kind() == "keyRun", "DATASET_GRAPH_ANCESTRY_REQUIRED");
        let stored: StoredRun = serde_json::from_value(context.descriptor(node).await?)?;
        ensure!(
            stored.version == 2
                && stored.descriptor.kind == kind
                && stored.descriptor.key_schema_hash == key_schema_hash,
            "DATASET_KEY_INDEX_INVALID"
        );
        let certificate = context.child(node, "certificate").await?;
        let file = context.file(&certificate).await?;
        ensure!(
            file.content() == &stored.content
                && stored.descriptor.entries == node.entries()
                && stored.content.size == stored.descriptor.size,
            "DATASET_KEY_INDEX_INVALID"
        );
        let header = file.range(0..8).await?;
        validate_header(&stored.descriptor, &header, file.content().size)?;
        file.check_current().await?;
        Ok(Self {
            descriptor: stored.descriptor,
            file,
        })
    }
    pub fn descriptor(&self) -> &RunDescriptor {
        &self.descriptor
    }
    pub async fn lookup(&self, key: &[u8]) -> Result<Option<RunValue>> {
        self.file.check_current().await?;
        let mut result = None;
        for block in point_blocks(&self.descriptor, key)? {
            let bytes = self.file.range(block.start..block.end).await?;
            for entry in decode_block(&self.descriptor, block, &bytes)? {
                if entry.key == key {
                    ensure!(result.is_none(), "DATASET_PRIMARY_KEY_DUPLICATE");
                    result = Some(entry.value);
                }
            }
        }
        self.file.check_current().await?;
        Ok(result)
    }
    pub(crate) fn stream_partition(
        &self,
        bucket: u8,
    ) -> futures::stream::BoxStream<'_, Result<RunEntry>> {
        use std::collections::VecDeque;
        let blocks = self
            .descriptor
            .blocks
            .iter()
            .filter(|block| block.partition == bucket)
            .collect::<Vec<_>>();
        Box::pin(futures::stream::try_unfold(
            (0usize, VecDeque::<RunEntry>::new(), blocks),
            move |(mut ordinal, mut page, blocks)| async move {
                loop {
                    if let Some(entry) = page.pop_front() {
                        return Ok(Some((entry, (ordinal, page, blocks))));
                    }
                    let Some(block) = blocks.get(ordinal) else {
                        self.file.check_current().await?;
                        return Ok(None);
                    };
                    let bytes = self.file.range(block.start..block.end).await?;
                    page = decode_block(&self.descriptor, block, &bytes)?.into();
                    ordinal += 1;
                }
            },
        ))
    }
    /// Explicit finite bootstrap/compaction work, independently capped before allocating decoded entries.
    pub async fn partition(&self, bucket: u8) -> Result<Vec<RunEntry>> {
        self.file.check_current().await?;
        let Some(slice) = self
            .descriptor
            .partitions
            .iter()
            .find(|slice| slice.partition == bucket)
        else {
            return Ok(vec![]);
        };
        allocation_bound(slice)?;
        let bytes = self.file.range(slice.start..slice.end).await?;
        let rows = decode_partition(&self.descriptor, slice, &bytes)?;
        self.file.check_current().await?;
        Ok(rows)
    }
}
/// Point visibility needs bounded newest-first runs. Equal-sequence ambiguity is rejected.
pub async fn lookup_visible(readers: &[RunReader], key: &[u8]) -> Result<Option<RunValue>> {
    ensure!(!readers.is_empty() && readers.len() <= 8, "RESOURCE_LIMIT");
    let mut previous = None;
    for reader in readers {
        ensure!(
            previous.is_none_or(|sequence| sequence > reader.descriptor.sequence),
            "DATASET_KEY_INDEX_INVALID"
        );
        previous = Some(reader.descriptor.sequence);
        if let Some(value) = reader.lookup(key).await? {
            return Ok(if value == RunValue::Deleted {
                None
            } else {
                Some(value)
            });
        }
    }
    Ok(None)
}
/// Encode a run reference for a typed graph-directory value, retaining actual certificate custody.
pub(crate) fn directory_value(
    run: &QualifiedRun,
) -> Result<crate::persistent_directory::graph::GraphValue> {
    let value: Value = serde_json::to_value(run.descriptor())?;
    crate::persistent_directory::graph::GraphValue::new(value, vec![run.node.clone()])
}
