//! Receiver-qualified physical row selection. Fixed 4096-row chunks share the existing immutable graph.
use crate::{
    dataset_graph::{NativeGraph, QualifiedNode},
    dataset_graph_read::{GraphReadContext, ReadableNode},
    native_snapshot_v3::LogicalFile,
    persistent_directory::{NodeReference, read::ReadDirectory},
    storage::SealedObject,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
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
        ensure!(
            descriptor.version == 3
                && descriptor.physical_rows <= MAX_ROWS
                && descriptor.data_sequence <= 9_007_199_254_740_991
                && descriptor.selected_rows <= descriptor.physical_rows
                && descriptor.selected_rows == node.entries(),
            "DATASET_KEY_INDEX_INVALID"
        );
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
                let chunk = if let Some(chunks) = &self.chunks {
                    if let Some(value) = chunks.lookup(&format!("{ordinal:016x}")).await? {
                        ensure!(value.links.len() == 1, "DATASET_KEY_INDEX_INVALID");
                        let chunk: RowChunk = serde_json::from_value(
                            self.context.descriptor(&value.links[0]).await?,
                        )?;
                        chunk.validate(
                            &self.descriptor.file.content,
                            self.descriptor.physical_rows,
                            ordinal,
                        )?;
                        chunk
                    } else {
                        RowChunk::base(
                            &self.descriptor.file.content,
                            self.descriptor.physical_rows,
                            ordinal,
                            self.descriptor.base_selected,
                        )?
                    }
                } else {
                    RowChunk::base(
                        &self.descriptor.file.content,
                        self.descriptor.physical_rows,
                        ordinal,
                        self.descriptor.base_selected,
                    )?
                };
                cached = Some((ordinal, chunk));
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
