//! Receiver-qualified physical row selection. Fixed 4096-row chunks share the existing immutable graph.
use crate::{
    dataset_graph::{NativeGraph, QualifiedNode},
    dataset_graph_read::{GraphReadContext, ReadableNode},
    native_snapshot_v3::LogicalFile,
    persistent_directory::{
        NodeReference,
        graph::{GraphDirectory, GraphValue},
        read::ReadDirectory,
    },
    storage::SealedObject,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
const ROWS: u64 = 4096;
const MAX_ROWS: u64 = 8 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RowChunk {
    version: u32,
    file_sha256: String,
    physical_rows: u64,
    ordinal: u64,
    selected: u64,
    bits: Vec<u8>,
}
impl RowChunk {
    pub(crate) fn base(
        file: &SealedObject,
        physical_rows: u64,
        ordinal: u64,
        selected: bool,
    ) -> Result<Self> {
        ensure!(
            physical_rows > 0
                && physical_rows <= MAX_ROWS
                && ordinal < physical_rows.div_ceil(ROWS),
            "DATASET_KEY_INDEX_INVALID"
        );
        let count = (physical_rows - ordinal * ROWS).min(ROWS);
        let mut bits = vec![if selected { 255 } else { 0 }; count.div_ceil(8) as usize];
        if selected && count % 8 != 0 {
            *bits.last_mut().unwrap() = (1u8 << (count % 8)) - 1;
        }
        Ok(Self {
            version: 3,
            file_sha256: file.sha256.clone(),
            physical_rows,
            ordinal,
            selected: if selected { count } else { 0 },
            bits,
        })
    }
    fn validate(&self, file: &SealedObject, physical_rows: u64, ordinal: u64) -> Result<()> {
        ensure!(
            self.version == 3
                && self.file_sha256 == file.sha256
                && self.physical_rows == physical_rows
                && physical_rows > 0
                && physical_rows <= MAX_ROWS
                && self.ordinal == ordinal
                && ordinal < physical_rows.div_ceil(ROWS),
            "DATASET_KEY_INDEX_INVALID"
        );
        let count = (physical_rows - ordinal * ROWS).min(ROWS);
        ensure!(
            self.bits.len() == count.div_ceil(8) as usize
                && self.selected
                    == self
                        .bits
                        .iter()
                        .map(|byte| byte.count_ones() as u64)
                        .sum::<u64>()
                && (count % 8 == 0
                    || self
                        .bits
                        .last()
                        .is_some_and(|byte| byte & !((1u8 << (count % 8)) - 1) == 0)),
            "DATASET_KEY_INDEX_INVALID"
        );
        Ok(())
    }
    fn contains(&self, row: u64) -> Result<bool> {
        ensure!(
            row / ROWS == self.ordinal && row < self.physical_rows,
            "DATASET_KEY_INDEX_INVALID"
        );
        let offset = row % ROWS;
        Ok(self.bits[(offset / 8) as usize] & (1u8 << (offset % 8)) != 0)
    }
    pub(crate) fn apply(&mut self, changes: &[(u64, bool)]) -> Result<()> {
        ensure!(
            changes.len() <= 4096 && changes.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "DATASET_KEY_INDEX_INVALID"
        );
        for (row, selected) in changes {
            ensure!(
                self.contains(*row)? != *selected,
                "DATASET_KEY_INDEX_INVALID"
            );
        }
        for (row, selected) in changes {
            let offset = *row % ROWS;
            let byte = &mut self.bits[(offset / 8) as usize];
            if *selected {
                *byte |= 1u8 << (offset % 8);
                self.selected += 1;
            } else {
                *byte &= !(1u8 << (offset % 8));
                self.selected -= 1;
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SelectionDescriptor {
    version: u32,
    file: LogicalFile,
    physical_rows: u64,
    data_sequence: u64,
    selected_rows: u64,
    base_selected: bool,
    chunks: Option<NodeReference>,
}
impl SelectionDescriptor {
    fn validate(&self, entries: u64) -> Result<()> {
        ensure!(
            self.version == 3
                && self.physical_rows <= MAX_ROWS
                && self.data_sequence <= 9_007_199_254_740_991
                && self.selected_rows <= self.physical_rows
                && self.selected_rows == entries,
            "DATASET_KEY_INDEX_INVALID"
        );
        if self.chunks.is_none() {
            ensure!(
                self.selected_rows
                    == if self.base_selected {
                        self.physical_rows
                    } else {
                        0
                    },
                "DATASET_KEY_INDEX_INVALID"
            );
        }
        Ok(())
    }
}
/// All rows are selected only after the full receiver scan proves a new data file or a delete-free bootstrap.
pub(crate) async fn write_all(
    graph: &NativeGraph,
    file: LogicalFile,
    certificate: QualifiedNode,
    physical_rows: u64,
    data_sequence: u64,
) -> Result<QualifiedNode> {
    ensure!(
        physical_rows <= MAX_ROWS
            && data_sequence <= 9_007_199_254_740_991
            && certificate.kind() == "certificate",
        "DATASET_KEY_INDEX_INVALID"
    );
    let descriptor = SelectionDescriptor {
        version: 3,
        file,
        physical_rows,
        data_sequence,
        selected_rows: physical_rows,
        base_selected: true,
        chunks: None,
    };
    graph
        .write_descriptor(
            "directory",
            &serde_json::to_value(descriptor)?,
            vec![("certificate".into(), certificate)],
            physical_rows,
            physical_rows.div_ceil(8),
        )
        .await
}
/// Remove a bounded, sorted set of exact physical ordinals from a receiver-qualified
/// selection. The caller must first verify each current full-key/file/row/sequence witness,
/// spill larger touched sets and chain batches. Untouched bitmap chunks and the immutable
/// file certificate are shared by the new descriptor; this primitive confers no key authority.
pub(crate) async fn delete_rows(
    graph: &NativeGraph,
    source: QualifiedNode,
    rows: &[u64],
) -> Result<QualifiedNode> {
    graph.check_current().await?;
    ensure!(
        source.kind() == "directory"
            && rows.len() <= ROWS as usize
            && rows.windows(2).all(|pair| pair[0] < pair[1]),
        "DATASET_KEY_INDEX_INVALID"
    );
    let mut descriptor: SelectionDescriptor =
        serde_json::from_value(graph.read_descriptor(&source).await?)?;
    descriptor.validate(source.entries())?;
    ensure!(
        rows.last()
            .is_none_or(|row| *row < descriptor.physical_rows),
        "DATASET_KEY_INDEX_INVALID"
    );
    if rows.is_empty() {
        return Ok(source);
    }
    let certificate = graph.child(&source, "certificate").await?;
    let (proof, _, _, _) = graph.file_proof(&certificate).await?.into_parts();
    ensure!(
        proof.content() == &descriptor.file.content,
        "DATASET_GRAPH_ANCESTRY_REQUIRED"
    );
    let chunks = match &descriptor.chunks {
        Some(_) => {
            GraphDirectory::rooted(graph.clone(), graph.child(&source, "chunks").await?).await?
        }
        None => GraphDirectory::new(graph.clone(), vec![])?,
    };
    let mut after = 0;
    let mut mutations = BTreeMap::new();
    while after < rows.len() {
        let ordinal = rows[after] / ROWS;
        let end = rows[after..].partition_point(|row| *row / ROWS == ordinal) + after;
        let key = format!("{ordinal:016x}");
        let previous = match &descriptor.chunks {
            Some(root) => chunks.lookup_with_links(root, &key).await?,
            None => None,
        };
        let mut chunk = match previous {
            Some((_, links)) => {
                ensure!(links.len() == 1, "DATASET_KEY_INDEX_INVALID");
                serde_json::from_value(graph.read_descriptor(&links[0]).await?)?
            }
            None => RowChunk::base(
                &descriptor.file.content,
                descriptor.physical_rows,
                ordinal,
                descriptor.base_selected,
            )?,
        };
        chunk.validate(&descriptor.file.content, descriptor.physical_rows, ordinal)?;
        let changes: Vec<_> = rows[after..end].iter().map(|row| (*row, false)).collect();
        chunk.apply(&changes)?;
        let node = graph
            .write_descriptor(
                "directory",
                &serde_json::to_value(&chunk)?,
                vec![],
                chunk.selected,
                chunk.bits.len() as u64,
            )
            .await?;
        mutations.insert(
            key,
            Some(GraphValue::new(
                serde_json::json!({"ordinal":ordinal}),
                vec![node],
            )?),
        );
        after = end;
    }
    descriptor.chunks = chunks.apply(descriptor.chunks.as_ref(), mutations).await?;
    descriptor.selected_rows = descriptor
        .selected_rows
        .checked_sub(rows.len() as u64)
        .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
    let root = descriptor
        .chunks
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
    let updated = graph
        .write_descriptor(
            "directory",
            &serde_json::to_value(&descriptor)?,
            vec![
                ("certificate".into(), certificate),
                ("chunks".into(), chunks.qualified_root(root).await?),
            ],
            descriptor.selected_rows,
            descriptor.physical_rows.div_ceil(8),
        )
        .await?;
    graph.check_current().await?;
    Ok(updated)
}

#[derive(Clone, Debug)]
pub(crate) struct ReadSelection {
    context: GraphReadContext,
    node: ReadableNode,
    descriptor: SelectionDescriptor,
    chunks: Option<ReadDirectory>,
}
impl ReadSelection {
    pub(crate) async fn open(context: GraphReadContext, node: ReadableNode) -> Result<Self> {
        ensure!(
            node.kind() == "directory",
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let descriptor: SelectionDescriptor =
            serde_json::from_value(context.descriptor(&node).await?)?;
        descriptor.validate(node.entries())?;
        let certificate = context.child(&node, "certificate").await?;
        let file = context.file(&certificate).await?;
        ensure!(
            file.content() == &descriptor.file.content,
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let chunks = match &descriptor.chunks {
            Some(reference) => Some(
                ReadDirectory::new(
                    context.clone(),
                    context.child(&node, "chunks").await?,
                    reference.clone(),
                )
                .await?,
            ),
            None => {
                ensure!(
                    descriptor.selected_rows
                        == if descriptor.base_selected {
                            descriptor.physical_rows
                        } else {
                            0
                        },
                    "DATASET_KEY_INDEX_INVALID"
                );
                None
            }
        };
        context.check_node(&node).await?;
        Ok(Self {
            context,
            node,
            descriptor,
            chunks,
        })
    }
    pub(crate) fn name(&self) -> &str {
        &self.descriptor.file.name
    }
    pub(crate) fn content(&self) -> &SealedObject {
        &self.descriptor.file.content
    }
    pub(crate) fn physical_rows(&self) -> u64 {
        self.descriptor.physical_rows
    }
    pub(crate) fn selected_rows(&self) -> u64 {
        self.descriptor.selected_rows
    }
    pub(crate) fn data_sequence(&self) -> u64 {
        self.descriptor.data_sequence
    }
    async fn chunk(&self, ordinal: u64) -> Result<RowChunk> {
        if let Some(chunks) = &self.chunks {
            if let Some(value) = chunks.lookup(&format!("{ordinal:016x}")).await? {
                ensure!(value.links.len() == 1, "DATASET_KEY_INDEX_INVALID");
                let chunk: RowChunk =
                    serde_json::from_value(self.context.descriptor(&value.links[0]).await?)?;
                chunk.validate(
                    &self.descriptor.file.content,
                    self.descriptor.physical_rows,
                    ordinal,
                )?;
                return Ok(chunk);
            }
        }
        RowChunk::base(
            &self.descriptor.file.content,
            self.descriptor.physical_rows,
            ordinal,
            self.descriptor.base_selected,
        )
    }
    /// Verify distinct physical row witnesses with one lookup per touched bitmap chunk.
    pub(crate) async fn verify_selected(&self, rows: &[u64]) -> Result<()> {
        self.verify_positions(rows, true).await
    }
    pub(crate) async fn verify_positions(&self, rows: &[u64], selected: bool) -> Result<()> {
        ensure!(
            rows.len() <= 1000
                && rows.windows(2).all(|pair| pair[0] < pair[1])
                && rows
                    .last()
                    .is_none_or(|row| *row < self.descriptor.physical_rows),
            "DATASET_KEY_INDEX_INVALID"
        );
        self.context.check_node(&self.node).await?;
        let mut after = 0;
        while after < rows.len() {
            let ordinal = rows[after] / ROWS;
            let end = rows[after..].partition_point(|row| *row / ROWS == ordinal) + after;
            let chunk = self.chunk(ordinal).await?;
            for row in &rows[after..end] {
                ensure!(
                    chunk.contains(*row)? == selected,
                    "DATASET_KEY_INDEX_INVALID"
                );
            }
            after = end;
        }
        self.context.check_node(&self.node).await
    }
    pub(crate) async fn selected(&self, start: u64, count: usize) -> Result<Vec<bool>> {
        ensure!(
            count <= 1000
                && start
                    .checked_add(count as u64)
                    .is_some_and(|end| end <= self.descriptor.physical_rows),
            "RESOURCE_LIMIT"
        );
        self.context.check_node(&self.node).await?;
        let mut output = Vec::with_capacity(count);
        let mut cached: Option<(u64, RowChunk)> = None;
        for row in start..start + count as u64 {
            let ordinal = row / ROWS;
            if cached
                .as_ref()
                .is_none_or(|(current, _)| *current != ordinal)
            {
                cached = Some((ordinal, self.chunk(ordinal).await?));
            }
            output.push(cached.as_ref().unwrap().1.contains(row)?);
        }
        self.context.check_node(&self.node).await?;
        Ok(output)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LogicalObject;
    #[test]
    fn batched_deletes_preserve_prior_chunks_and_reject_partial_invalid_batches() {
        let file = SealedObject {
            object: LogicalObject::new("w", "t", "f").unwrap(),
            size: 1,
            sha256: format!("sha256:{}", "a".repeat(64)),
        };
        let prior = RowChunk::base(&file, 4100, 0, true).unwrap();
        let mut changed = prior.clone();
        changed.apply(&[(0, false), (4095, false)]).unwrap();
        changed.apply(&[(1, false), (4094, false)]).unwrap();
        changed.validate(&file, 4100, 0).unwrap();
        assert_eq!(changed.selected, 4092);
        assert!(prior.contains(0).unwrap());
        assert!(prior.contains(4095).unwrap());
        let before = changed.bits.clone();
        assert!(changed.apply(&[(2, false), (4094, false)]).is_err());
        assert_eq!(changed.bits, before);
        assert_eq!(changed.selected, 4092);
        assert!(changed.apply(&[(4, false), (3, false)]).is_err());
        assert_eq!(changed.bits, before);
        let mut tail = RowChunk::base(&file, 4100, 1, true).unwrap();
        tail.apply(&(4096..4100).map(|row| (row, false)).collect::<Vec<_>>())
            .unwrap();
        tail.validate(&file, 4100, 1).unwrap();
        assert_eq!(tail.selected, 0);
        assert_eq!(tail.bits, vec![0]);
        assert!(changed.contains(4093).unwrap());
    }
    #[test]
    fn selection_count_requires_chunks_for_any_nondefault_visibility() {
        let mut descriptor = SelectionDescriptor {
            version: 3,
            file: LogicalFile {
                name: "data/f.parquet".into(),
                content: SealedObject {
                    object: LogicalObject::new("w", "t", "f").unwrap(),
                    size: 1,
                    sha256: format!("sha256:{}", "a".repeat(64)),
                },
            },
            physical_rows: 4100,
            data_sequence: 7,
            selected_rows: 4100,
            base_selected: true,
            chunks: None,
        };
        descriptor.validate(4100).unwrap();
        descriptor.selected_rows = 4099;
        assert!(descriptor.validate(4099).is_err());
        descriptor.selected_rows = 0;
        assert!(descriptor.validate(0).is_err());
        descriptor.base_selected = false;
        descriptor.validate(0).unwrap();
        assert!(descriptor.validate(1).is_err());
        descriptor.physical_rows = MAX_ROWS + 1;
        assert!(descriptor.validate(0).is_err());
    }
    #[test]
    fn exact_bitmap_boundaries_and_duplicate_or_wrong_locator_rejection() {
        let file = SealedObject {
            object: LogicalObject::new("w", "t", "f").unwrap(),
            size: 1,
            sha256: format!("sha256:{}", "a".repeat(64)),
        };
        let mut first = RowChunk::base(&file, 4100, 0, true).unwrap();
        first.apply(&[(0, false), (4095, false)]).unwrap();
        assert!(!first.contains(0).unwrap());
        assert!(!first.contains(4095).unwrap());
        assert_eq!(first.selected, 4094);
        assert!(first.apply(&[(0, false)]).is_err());
        assert!(first.apply(&[(4096, false)]).is_err());
        let last = RowChunk::base(&file, 4100, 1, true).unwrap();
        last.validate(&file, 4100, 1).unwrap();
        assert_eq!(last.bits, vec![15]);
        assert!(last.contains(4100).is_err());
        let mut malformed = last.clone();
        malformed.bits[0] |= 128;
        assert!(malformed.validate(&file, 4100, 1).is_err());
        let mut wrong = file.clone();
        wrong.sha256 = format!("sha256:{}", "b".repeat(64));
        assert!(last.validate(&wrong, 4100, 1).is_err());
    }
}
