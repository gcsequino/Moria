//! Immutable bounded LSM bucket metadata. Levels define visibility priority; compacted run sequence alone does not.
use super::{
    key_run::{RunKind, RunValue, key_route},
    key_run_io::{QualifiedRun, RunReader},
};
use crate::{
    dataset_graph::{NativeGraph, QualifiedNode},
    dataset_graph_read::GraphReadContext,
    persistent_directory::{graph::GraphValue, read::ReadEntry},
    storage::SealedObject,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub(crate) type RunCache = tokio::sync::Mutex<Option<(SealedObject, Arc<RunReader>)>>;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Segment {
    minimum: [u8; 32],
    maximum: [u8; 32],
    run: SealedObject,
    sequence: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Bucket {
    version: u32,
    kind: RunKind,
    key_schema_hash: String,
    partition: u8,
    levels: Vec<Vec<Segment>>,
}
impl Bucket {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 3
                && self.levels.len() == 8
                && self.levels[0].len() <= 8
                && self.levels.iter().skip(1).all(|level| level.len() <= 16),
            "DATASET_KEY_INDEX_INVALID"
        );
        let mut total = 0;
        for (level, segments) in self.levels.iter().enumerate() {
            ensure!(
                segments
                    .iter()
                    .all(|segment| segment.minimum[0] == self.partition
                        && segment.maximum[0] == self.partition
                        && segment.minimum <= segment.maximum
                        && segment.sequence <= 9_007_199_254_740_991),
                "DATASET_KEY_INDEX_INVALID"
            );
            if level == 0 {
                ensure!(
                    segments
                        .windows(2)
                        .all(|pair| pair[0].sequence > pair[1].sequence),
                    "DATASET_KEY_INDEX_INVALID"
                );
            } else {
                ensure!(
                    segments
                        .windows(2)
                        .all(|pair| pair[0].maximum <= pair[1].minimum),
                    "DATASET_KEY_INDEX_INVALID"
                );
            }
            total += segments.len();
        }
        ensure!(total <= 120, "RESOURCE_LIMIT");
        Ok(())
    }
    async fn reader(
        &self,
        context: &GraphReadContext,
        cache: &RunCache,
        entry: &ReadEntry,
        ordinal: usize,
        segment: &Segment,
    ) -> Result<Arc<RunReader>> {
        let node = entry.link(ordinal).await?;
        ensure!(
            node.content() == &segment.run && node.kind() == "keyRun",
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let cached = cache
            .lock()
            .await
            .as_ref()
            .filter(|(content, _)| content == node.content())
            .map(|(_, reader)| reader.clone());
        let reader = match cached {
            Some(reader) => reader,
            None => {
                let reader = Arc::new(
                    RunReader::open(context, &node, &self.key_schema_hash, self.kind).await?,
                );
                *cache.lock().await = Some((node.content().clone(), reader.clone()));
                reader
            }
        };
        ensure!(
            reader.descriptor().kind == self.kind
                && reader.descriptor().key_schema_hash == self.key_schema_hash
                && reader.descriptor().entries == node.entries()
                && reader.descriptor().sequence == segment.sequence,
            "DATASET_KEY_INDEX_INVALID"
        );
        let blocks = reader
            .descriptor()
            .blocks
            .iter()
            .filter(|block| block.partition == self.partition)
            .collect::<Vec<_>>();
        ensure!(
            blocks
                .first()
                .is_some_and(|block| block.minimum == segment.minimum)
                && blocks
                    .last()
                    .is_some_and(|block| block.maximum == segment.maximum),
            "DATASET_KEY_INDEX_INVALID"
        );
        Ok(reader)
    }
}
pub(crate) fn segment(run: &QualifiedRun, partition: u8) -> Result<Segment> {
    let blocks = run
        .descriptor()
        .blocks
        .iter()
        .filter(|block| block.partition == partition)
        .collect::<Vec<_>>();
    ensure!(!blocks.is_empty(), "DATASET_KEY_INDEX_INVALID");
    Ok(Segment {
        minimum: blocks.first().unwrap().minimum,
        maximum: blocks.last().unwrap().maximum,
        run: run.node().content().clone(),
        sequence: run.descriptor().sequence,
    })
}
/// Initial independently proved runs occupy disjoint ranges in level one; finite overflow uses deeper disjoint levels.
pub(crate) async fn bootstrap_value(
    graph: &NativeGraph,
    kind: RunKind,
    key_schema_hash: &str,
    partition: u8,
    runs: Vec<(Segment, QualifiedNode)>,
) -> Result<GraphValue> {
    ensure!(runs.len() <= 112, "RESOURCE_LIMIT");
    let mut levels = vec![vec![]; 8];
    let mut links = vec![];
    for (index, (segment, node)) in runs.into_iter().enumerate() {
        ensure!(
            node.kind() == "keyRun" && node.content() == &segment.run,
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let qualified = graph
            .known_node(node.content())
            .await?
            .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_ANCESTRY_REQUIRED"))?;
        ensure!(
            qualified.id() == node.id(),
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        levels[1 + index / 16].push(segment);
        links.push(node);
    }
    let value = Bucket {
        version: 3,
        kind,
        key_schema_hash: key_schema_hash.into(),
        partition,
        levels,
    };
    value.validate()?;
    GraphValue::new(serde_json::to_value(value)?, links)
}
/// A bucket entry was obtained on the exact retained root path; only matching runs are opened.
pub(crate) async fn lookup(
    context: &GraphReadContext,
    cache: &RunCache,
    entry: &ReadEntry,
    kind: RunKind,
    key_schema_hash: &str,
    key: &[u8],
) -> Result<Option<RunValue>> {
    let bucket: Bucket = serde_json::from_value(entry.value.clone())?;
    bucket.validate()?;
    ensure!(
        bucket.kind == kind
            && bucket.key_schema_hash == key_schema_hash
            && bucket.partition == key_route(key)[0]
            && entry.link_count() == bucket.levels.iter().map(Vec::len).sum::<usize>(),
        "DATASET_KEY_INDEX_INVALID"
    );
    let hash = key_route(key);
    let mut link = 0;
    for (level, segments) in bucket.levels.iter().enumerate() {
        let mut found = None;
        let mut candidates = 0;
        for segment in segments {
            let ordinal = link;
            link += 1;
            if hash < segment.minimum || hash > segment.maximum {
                continue;
            }
            candidates += 1;
            ensure!(candidates <= 8, "RESOURCE_LIMIT");
            let reader = bucket
                .reader(context, cache, entry, ordinal, segment)
                .await?;
            if let Some(value) = reader.lookup(key).await? {
                if level == 0 {
                    return Ok(if value == RunValue::Deleted {
                        None
                    } else {
                        Some(value)
                    });
                }
                ensure!(found.is_none(), "DATASET_PRIMARY_KEY_DUPLICATE");
                found = Some(value);
            }
        }
        if let Some(value) = found {
            return Ok(if value == RunValue::Deleted {
                None
            } else {
                Some(value)
            });
        }
    }
    Ok(None)
}

/// Resolve a bounded single-partition batch without a provider/SQL operation per key.
/// Bucket levels retain their original visibility priority, including partial-compaction
/// tombstones; equal routing hashes are resolved by the full encoded key.
pub(crate) async fn lookup_batch(
    context: &GraphReadContext,
    cache: &RunCache,
    entry: &ReadEntry,
    kind: RunKind,
    key_schema_hash: &str,
    keys: &[Vec<u8>],
) -> Result<Vec<Option<RunValue>>> {
    ensure!(keys.len() <= 1000, "RESOURCE_LIMIT");
    keys.iter().try_fold(0usize, |bytes, key| {
        ensure!(
            !key.is_empty() && key.len() <= 4 * 1024 * 1024,
            "DATASET_KEY_INDEX_INVALID"
        );
        bytes
            .checked_add(key.len())
            .filter(|bytes| *bytes <= 8 * 1024 * 1024)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))
    })?;
    let bucket: Bucket = serde_json::from_value(entry.value.clone())?;
    bucket.validate()?;
    let hashes: Vec<_> = keys.iter().map(|key| key_route(key)).collect();
    ensure!(
        bucket.kind == kind
            && bucket.key_schema_hash == key_schema_hash
            && hashes.iter().all(|hash| hash[0] == bucket.partition)
            && entry.link_count() == bucket.levels.iter().map(Vec::len).sum::<usize>(),
        "DATASET_KEY_INDEX_INVALID"
    );
    let mut result = vec![None; keys.len()];
    let mut resolved = vec![false; keys.len()];
    let mut link = 0;
    for (level, segments) in bucket.levels.iter().enumerate() {
        let mut found = vec![None; keys.len()];
        let mut candidates = vec![0usize; keys.len()];
        for segment in segments {
            let ordinal = link;
            link += 1;
            let indices: Vec<_> = hashes
                .iter()
                .enumerate()
                .filter(|(index, hash)| {
                    !resolved[*index] && **hash >= segment.minimum && **hash <= segment.maximum
                })
                .map(|(index, _)| index)
                .collect();
            if indices.is_empty() {
                continue;
            }
            for index in &indices {
                candidates[*index] += 1;
                ensure!(candidates[*index] <= 8, "RESOURCE_LIMIT");
            }
            let reader = bucket
                .reader(context, cache, entry, ordinal, segment)
                .await?;
            let selected: Vec<_> = indices.iter().map(|index| keys[*index].clone()).collect();
            for (index, value) in indices
                .into_iter()
                .zip(reader.lookup_batch(&selected).await?)
            {
                if let Some(value) = value {
                    if level == 0 {
                        resolved[index] = true;
                        if value != RunValue::Deleted {
                            result[index] = Some(value);
                        }
                    } else {
                        ensure!(found[index].is_none(), "DATASET_PRIMARY_KEY_DUPLICATE");
                        found[index] = Some(value);
                    }
                }
            }
        }
        for (index, value) in found.into_iter().enumerate() {
            if let Some(value) = value {
                resolved[index] = true;
                if value != RunValue::Deleted {
                    result[index] = Some(value);
                }
            }
        }
        if resolved.iter().all(|resolved| *resolved) {
            break;
        }
    }
    Ok(result)
}
