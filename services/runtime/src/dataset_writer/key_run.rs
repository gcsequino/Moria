//! Sorted bounded native key runs. The enclosing qualified index supplies snapshot authority.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
const MAGIC: &[u8; 8] = b"MORIAKR3";
const LEGACY_MAGIC: &[u8; 8] = b"MORIAKR2";
const LIMIT: usize = 16 * 1024 * 1024;
const KEY_LIMIT: usize = 4 * 1024 * 1024;
const RECORD_LIMIT: usize = 65536;
const DECODED_LIMIT: usize = 64 * 1024 * 1024;
const NAME_LIMIT: usize = 4096;
const BLOCK_RECORDS: u64 = 64;
const BLOCK_BYTES: u64 = 64 * 1024;
const POINT_BLOCK_LIMIT: usize = 8;
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RunKind {
    Hashes,
    Locations,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Locator {
    pub file: String,
    pub row: u64,
    pub data_sequence: u64,
    pub row_hash: Option<[u8; 32]>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    tag = "kind",
    content = "value",
    deny_unknown_fields
)]
pub enum RunValue {
    Deleted,
    Hash([u8; 32]),
    DeferredHash,
    Location(Locator),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunEntry {
    pub key: Vec<u8>,
    pub value: RunValue,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PartitionSlice {
    pub partition: u8,
    pub start: u64,
    pub end: u64,
    pub entries: u64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BlockSlice {
    pub partition: u8,
    pub start: u64,
    pub end: u64,
    pub entries: u64,
    pub minimum: [u8; 32],
    pub maximum: [u8; 32],
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunDescriptor {
    pub version: u32,
    pub kind: RunKind,
    pub key_schema_hash: String,
    pub sequence: u64,
    pub entries: u64,
    pub size: u64,
    pub partitions: Vec<PartitionSlice>,
    pub blocks: Vec<BlockSlice>,
}
impl RunDescriptor {
    /// Descriptors are validated before callers use their ordered partition boundaries.
    pub(super) fn partition_blocks(&self, partition: u8) -> &[BlockSlice] {
        let start = self
            .blocks
            .partition_point(|block| block.partition < partition);
        let end = self
            .blocks
            .partition_point(|block| block.partition <= partition);
        &self.blocks[start..end]
    }
}
pub fn key_route(key: &[u8]) -> [u8; 32] {
    Sha256::digest(key).into()
}
fn partition(key: &[u8]) -> u8 {
    key_route(key)[0]
}
pub(crate) fn compare_keys(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    key_route(a).cmp(&key_route(b)).then_with(|| a.cmp(b))
}
fn valid_hash(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn append(out: &mut Vec<u8>, data: &[u8]) -> Result<()> {
    ensure!(
        out.len()
            .checked_add(data.len())
            .is_some_and(|size| size <= LIMIT),
        "RESOURCE_LIMIT"
    );
    out.extend_from_slice(data);
    Ok(())
}
/// Input must already be spill-sorted by (SHA256 routing bytes,full encoded key); hash collisions compare complete keys. Every call and whole run are bounded.
pub struct RunWriter {
    descriptor: RunDescriptor,
    bytes: Vec<u8>,
    previous: Option<([u8; 32], Vec<u8>)>,
    failed: bool,
}
impl RunWriter {
    pub fn new(kind: RunKind, key_schema_hash: &str, sequence: u64) -> Result<Self> {
        ensure!(
            valid_hash(key_schema_hash) && sequence <= 9_007_199_254_740_991,
            "DATASET_KEY_INDEX_INVALID"
        );
        Ok(Self {
            descriptor: RunDescriptor {
                version: 3,
                kind,
                key_schema_hash: key_schema_hash.into(),
                sequence,
                entries: 0,
                size: 0,
                partitions: vec![],
                blocks: vec![],
            },
            bytes: MAGIC.to_vec(),
            previous: None,
            failed: false,
        })
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.descriptor.entries == 0
    }
    pub fn would_fit(&self, entry: &RunEntry) -> bool {
        let extra = 4usize.checked_add(entry.key.len()).and_then(|n| {
            n.checked_add(match &entry.value {
                RunValue::Deleted => 1,
                RunValue::Hash(_) => 33,
                RunValue::DeferredHash => 1,
                RunValue::Location(value) => {
                    25 + value.file.len() + if value.row_hash.is_some() { 32 } else { 0 }
                }
            })
        });
        !self.failed
            && self.descriptor.entries < RECORD_LIMIT as u64
            && extra
                .and_then(|extra| self.bytes.len().checked_add(extra))
                .is_some_and(|n| n <= LIMIT)
    }
    pub fn push(&mut self, entry: &RunEntry) -> Result<()> {
        ensure!(!self.failed, "WRITER_FAILED");
        self.failed = true;
        self.push_checked(entry)?;
        self.failed = false;
        Ok(())
    }
    fn push_checked(&mut self, entry: &RunEntry) -> Result<()> {
        ensure!(
            self.descriptor.entries < (RECORD_LIMIT as u64),
            "RESOURCE_LIMIT"
        );
        ensure!(
            !entry.key.is_empty() && entry.key.len() <= KEY_LIMIT,
            "DATASET_PRIMARY_KEY_LIMIT"
        );
        let digest = key_route(&entry.key);
        let bucket = digest[0];
        ensure!(
            self.previous
                .as_ref()
                .is_none_or(|(route, key)| (route, key) < (&digest, &entry.key)),
            "DATASET_PRIMARY_KEY_DUPLICATE"
        );
        let start = self.bytes.len() as u64;
        append(&mut self.bytes, &(entry.key.len() as u32).to_le_bytes())?;
        append(&mut self.bytes, &entry.key)?;
        match (&self.descriptor.kind, &entry.value) {
            (_, RunValue::Deleted) => append(&mut self.bytes, &[0])?,
            (RunKind::Hashes, RunValue::Hash(hash)) => {
                append(&mut self.bytes, &[1])?;
                append(&mut self.bytes, hash)?;
            }
            (RunKind::Hashes, RunValue::DeferredHash) => append(&mut self.bytes, &[3])?,
            (RunKind::Locations, RunValue::Location(value)) => {
                ensure!(
                    !value.file.is_empty()
                        && value.file.len() <= NAME_LIMIT
                        && value.file.starts_with("moria://")
                        && value.row <= 9_007_199_254_740_991
                        && value.data_sequence <= 9_007_199_254_740_991,
                    "DATASET_KEY_INDEX_INVALID"
                );
                append(
                    &mut self.bytes,
                    &[if value.row_hash.is_some() { 2 } else { 4 }],
                )?;
                append(&mut self.bytes, &(value.file.len() as u32).to_le_bytes())?;
                append(&mut self.bytes, value.file.as_bytes())?;
                append(&mut self.bytes, &value.row.to_le_bytes())?;
                append(&mut self.bytes, &value.data_sequence.to_le_bytes())?;
                if let Some(hash) = &value.row_hash {
                    append(&mut self.bytes, hash)?;
                }
            }
            _ => anyhow::bail!("DATASET_KEY_INDEX_INVALID"),
        }
        let end = self.bytes.len() as u64;
        if self
            .descriptor
            .partitions
            .last()
            .is_none_or(|slice| slice.partition != bucket)
        {
            self.descriptor.partitions.push(PartitionSlice {
                partition: bucket,
                start,
                end,
                entries: 1,
            });
        } else {
            let slice = self.descriptor.partitions.last_mut().unwrap();
            slice.end = end;
            slice.entries += 1;
        }
        let record_bytes = end - start;
        if self.descriptor.blocks.last().is_none_or(|block| {
            block.partition != bucket
                || block.entries >= BLOCK_RECORDS
                || end - block.start > BLOCK_BYTES
        }) {
            self.descriptor.blocks.push(BlockSlice {
                partition: bucket,
                start,
                end,
                entries: 1,
                minimum: digest,
                maximum: digest,
            });
        } else {
            let block = self.descriptor.blocks.last_mut().unwrap();
            block.end = end;
            block.entries += 1;
            block.maximum = digest;
        }
        ensure!(
            record_bytes <= KEY_LIMIT as u64 + NAME_LIMIT as u64 + 64,
            "RESOURCE_LIMIT"
        );
        self.descriptor.entries += 1;
        self.previous = Some((digest, entry.key.clone()));
        Ok(())
    }
    pub fn finish(mut self) -> Result<(RunDescriptor, Vec<u8>)> {
        ensure!(
            !self.failed && self.descriptor.entries > 0,
            "DATASET_KEY_INDEX_INVALID"
        );
        self.descriptor.size = self.bytes.len() as u64;
        validate_descriptor(&self.descriptor)?;
        Ok((self.descriptor, self.bytes))
    }
}
fn take<'a>(bytes: &'a [u8], offset: &mut usize, count: usize) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(count)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
    let result = &bytes[*offset..end];
    *offset = end;
    Ok(result)
}
fn integer(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(take(bytes, offset, 4)?.try_into()?))
}
fn long(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let value = u64::from_le_bytes(take(bytes, offset, 8)?.try_into()?);
    ensure!(value <= 9_007_199_254_740_991, "DATASET_KEY_INDEX_INVALID");
    Ok(value)
}
#[cfg(test)]
thread_local! {
    static DESCRIPTOR_VALIDATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn validate_descriptor(descriptor: &RunDescriptor) -> Result<()> {
    #[cfg(test)]
    DESCRIPTOR_VALIDATIONS.with(|count| count.set(count.get() + 1));
    ensure!(
        matches!(descriptor.version, 2 | 3)
            && valid_hash(&descriptor.key_schema_hash)
            && descriptor.sequence <= 9_007_199_254_740_991
            && descriptor.entries > 0
            && descriptor.entries <= RECORD_LIMIT as u64
            && descriptor.size <= LIMIT as u64
            && !descriptor.partitions.is_empty()
            && descriptor.partitions.len() <= 256,
        "DATASET_KEY_INDEX_INVALID"
    );
    let mut offset = MAGIC.len() as u64;
    let mut previous = None;
    let mut count = 0u64;
    for slice in &descriptor.partitions {
        ensure!(
            previous.is_none_or(|value| value < slice.partition)
                && slice.start == offset
                && slice.start < slice.end
                && slice.end <= descriptor.size
                && slice.entries > 0,
            "DATASET_KEY_INDEX_INVALID"
        );
        count = count
            .checked_add(slice.entries)
            .filter(|count| *count <= descriptor.entries)
            .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
        previous = Some(slice.partition);
        offset = slice.end;
    }
    ensure!(
        offset == descriptor.size && count == descriptor.entries,
        "DATASET_KEY_INDEX_INVALID"
    );
    ensure!(
        !descriptor.blocks.is_empty() && descriptor.blocks.len() <= RECORD_LIMIT,
        "DATASET_KEY_INDEX_INVALID"
    );
    let mut block_index = 0;
    for slice in &descriptor.partitions {
        let mut offset = slice.start;
        let mut entries = 0u64;
        let mut previous = None;
        while let Some(block) = descriptor
            .blocks
            .get(block_index)
            .filter(|block| block.partition == slice.partition)
        {
            ensure!(
                block.start == offset
                    && block.end > block.start
                    && block.end <= slice.end
                    && block.entries > 0
                    && block.entries <= BLOCK_RECORDS
                    && block.minimum[0] == slice.partition
                    && block.maximum[0] == slice.partition
                    && block.minimum <= block.maximum
                    && previous.is_none_or(|hash| hash <= block.minimum)
                    && (block.end - block.start <= BLOCK_BYTES || block.entries == 1)
                    && block.end - block.start <= KEY_LIMIT as u64 + NAME_LIMIT as u64 + 64,
                "DATASET_KEY_INDEX_INVALID"
            );
            entries = entries
                .checked_add(block.entries)
                .filter(|n| *n <= slice.entries)
                .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
            offset = block.end;
            previous = Some(block.maximum);
            block_index += 1;
        }
        ensure!(
            offset == slice.end && entries == slice.entries,
            "DATASET_KEY_INDEX_INVALID"
        );
    }
    ensure!(
        block_index == descriptor.blocks.len(),
        "DATASET_KEY_INDEX_INVALID"
    );
    Ok(())
}
pub fn validate_header(descriptor: &RunDescriptor, header: &[u8], actual_size: u64) -> Result<()> {
    validate_descriptor(descriptor)?;
    ensure!(
        header
            == if descriptor.version == 2 {
                LEGACY_MAGIC
            } else {
                MAGIC
            }
            && actual_size == descriptor.size,
        "DATASET_KEY_INDEX_INVALID"
    );
    Ok(())
}
pub fn allocation_bound(slice: &PartitionSlice) -> Result<usize> {
    let records = usize::try_from(slice.entries)?;
    let wire = usize::try_from(
        slice
            .end
            .checked_sub(slice.start)
            .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?,
    )?;
    records
        .checked_mul(std::mem::size_of::<RunEntry>())
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| wire.checked_mul(2).and_then(|wire| bytes.checked_add(wire)))
        .filter(|bytes| *bytes <= DECODED_LIMIT)
        .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))
}
/// A receiver reads a single authenticated partition slice, never the whole historical run set in memory.
pub fn decode_partition(
    descriptor: &RunDescriptor,
    slice: &PartitionSlice,
    bytes: &[u8],
) -> Result<Vec<RunEntry>> {
    validate_descriptor(descriptor)?;
    ensure!(
        descriptor
            .partitions
            .iter()
            .any(|accepted| accepted == slice),
        "DATASET_KEY_INDEX_INVALID"
    );
    decode_slice(descriptor.kind, descriptor.version, slice, bytes)
}

fn decode_slice(
    kind: RunKind,
    version: u32,
    slice: &PartitionSlice,
    bytes: &[u8],
) -> Result<Vec<RunEntry>> {
    allocation_bound(slice)?;
    ensure!(
        slice.end - slice.start == bytes.len() as u64,
        "DATASET_KEY_INDEX_INVALID"
    );
    // Encoded minimum record length bounds output allocation before decoding untrusted counts.
    ensure!(
        slice.entries <= bytes.len() as u64 / 6,
        "DATASET_KEY_INDEX_INVALID"
    );
    let mut output = Vec::new();
    let mut offset = 0;
    for _ in 0..slice.entries {
        let length = integer(bytes, &mut offset)? as usize;
        ensure!(
            (1..=KEY_LIMIT).contains(&length),
            "DATASET_KEY_INDEX_INVALID"
        );
        let key = take(bytes, &mut offset, length)?.to_vec();
        ensure!(
            partition(&key) == slice.partition
                && output
                    .last()
                    .is_none_or(|entry: &RunEntry| compare_keys(&entry.key, &key).is_lt()),
            "DATASET_KEY_INDEX_INVALID"
        );
        let operation = take(bytes, &mut offset, 1)?[0];
        let value = match (operation, kind) {
            (0, _) => RunValue::Deleted,
            (1, RunKind::Hashes) => RunValue::Hash(take(bytes, &mut offset, 32)?.try_into()?),
            (3, RunKind::Hashes) if version == 3 => RunValue::DeferredHash,
            (operation @ (2 | 4), RunKind::Locations) if operation == 2 || version == 3 => {
                let length = integer(bytes, &mut offset)? as usize;
                ensure!(
                    (1..=NAME_LIMIT).contains(&length),
                    "DATASET_KEY_INDEX_INVALID"
                );
                let file = std::str::from_utf8(take(bytes, &mut offset, length)?)?.to_owned();
                ensure!(file.starts_with("moria://"), "DATASET_KEY_INDEX_INVALID");
                let row = long(bytes, &mut offset)?;
                let data_sequence = long(bytes, &mut offset)?;
                let row_hash = if operation == 2 {
                    Some(take(bytes, &mut offset, 32)?.try_into()?)
                } else {
                    None
                };
                RunValue::Location(Locator {
                    file,
                    row,
                    data_sequence,
                    row_hash,
                })
            }
            _ => anyhow::bail!("DATASET_KEY_INDEX_INVALID"),
        };
        output.push(RunEntry { key, value });
    }
    ensure!(offset == bytes.len(), "DATASET_KEY_INDEX_INVALID");
    Ok(output)
}
/// Merge already bounded sorted partition slices newest-first. Equal sequence runs are invalid;
/// compaction preserves tombstones for untouched older levels, with no implicit last-row wins inside a run.
pub fn merge_partition(
    kind: RunKind,
    bucket: u8,
    runs: &[(u64, Vec<RunEntry>)],
    maximum_entries: usize,
) -> Result<Vec<RunEntry>> {
    ensure!(
        !runs.is_empty() && runs.len() <= 8 && maximum_entries <= RECORD_LIMIT,
        "RESOURCE_LIMIT"
    );
    let mut previous = None;
    let mut visible = BTreeMap::new();
    let mut key_bytes = 0usize;
    let total = runs
        .iter()
        .flat_map(|(_, entries)| entries)
        .try_fold(0usize, |total, entry| {
            total
                .checked_add(std::mem::size_of::<RunEntry>())
                .and_then(|total| total.checked_add(entry.key.len()))
                .and_then(|total| {
                    total.checked_add(match &entry.value {
                        RunValue::Location(value) => value.file.len(),
                        _ => 0,
                    })
                })
                .filter(|total| *total <= DECODED_LIMIT)
                .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))
        })?;
    ensure!(total <= DECODED_LIMIT, "RESOURCE_LIMIT");
    for (sequence, entries) in runs {
        ensure!(
            previous.is_none_or(|previous| previous > *sequence),
            "DATASET_KEY_INDEX_INVALID"
        );
        previous = Some(*sequence);
        let mut previous_key: Option<&[u8]> = None;
        for entry in entries {
            ensure!(
                !entry.key.is_empty()
                    && entry.key.len() <= KEY_LIMIT
                    && partition(&entry.key) == bucket
                    && matches!(
                        (kind, &entry.value),
                        (_, RunValue::Deleted)
                            | (RunKind::Hashes, RunValue::Hash(_) | RunValue::DeferredHash)
                            | (RunKind::Locations, RunValue::Location(_))
                    ),
                "DATASET_KEY_INDEX_INVALID"
            );
            ensure!(
                previous_key.is_none_or(|key| compare_keys(key, &entry.key).is_lt()),
                "DATASET_KEY_INDEX_INVALID"
            );
            previous_key = Some(&entry.key);
            if !visible.contains_key(&entry.key) {
                key_bytes = key_bytes
                    .checked_add(entry.key.len())
                    .filter(|bytes| *bytes <= LIMIT)
                    .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
                ensure!(visible.len() < maximum_entries, "RESOURCE_LIMIT");
                visible.insert(entry.key.clone(), entry.value.clone());
            }
        }
    }
    let mut output = visible
        .into_iter()
        // A partial merge cannot prove that untouched older levels lack this key.
        .map(|(key, value)| RunEntry { key, value })
        .collect::<Vec<_>>();
    output.sort_by(|a, b| compare_keys(&a.key, &b.key));
    Ok(output)
}
/// Sparse bounds are trusted only after the enclosing receiver verifies the complete run descriptor.
/// Matching hash collisions remain bounded and every candidate compares complete key bytes.
pub fn point_blocks<'a>(descriptor: &'a RunDescriptor, key: &[u8]) -> Result<Vec<&'a BlockSlice>> {
    validate_descriptor(descriptor)?;
    point_blocks_in_validated_run(descriptor, key)
}

pub(super) fn point_blocks_in_validated_run<'a>(
    descriptor: &'a RunDescriptor,
    key: &[u8],
) -> Result<Vec<&'a BlockSlice>> {
    ensure!(
        !key.is_empty() && key.len() <= KEY_LIMIT,
        "DATASET_PRIMARY_KEY_LIMIT"
    );
    let digest = key_route(key);
    let first = descriptor
        .blocks
        .partition_point(|block| block.maximum < digest);
    let blocks = descriptor.blocks[first..]
        .iter()
        .take_while(|block| block.minimum <= digest)
        .take(POINT_BLOCK_LIMIT + 1)
        .collect::<Vec<_>>();
    ensure!(blocks.len() <= POINT_BLOCK_LIMIT, "RESOURCE_LIMIT");
    Ok(blocks)
}

pub fn decode_block(
    descriptor: &RunDescriptor,
    block: &BlockSlice,
    bytes: &[u8],
) -> Result<Vec<RunEntry>> {
    validate_descriptor(descriptor)?;
    ensure!(
        descriptor.blocks.contains(block),
        "DATASET_KEY_INDEX_INVALID"
    );
    decode_validated_block(descriptor.kind, descriptor.version, block, bytes)
}

/// The block must come from a descriptor checked by the reader or verifier.
pub(super) fn decode_validated_block(
    kind: RunKind,
    version: u32,
    block: &BlockSlice,
    bytes: &[u8],
) -> Result<Vec<RunEntry>> {
    let slice = PartitionSlice {
        partition: block.partition,
        start: block.start,
        end: block.end,
        entries: block.entries,
    };
    let rows = decode_slice(kind, version, &slice, bytes)?;
    ensure!(
        rows.first()
            .is_some_and(|row| key_route(&row.key) == block.minimum)
            && rows
                .last()
                .is_some_and(|row| key_route(&row.key) == block.maximum),
        "DATASET_KEY_INDEX_INVALID"
    );
    Ok(rows)
}
/// Receiver bootstrap/compaction independently verifies every byte interval and sparse fence.
/// This pure codec proof alone never grants Dataset membership or publication authority.
pub fn verify_complete_run(descriptor: &RunDescriptor, bytes: &[u8]) -> Result<()> {
    validate_header(
        descriptor,
        bytes
            .get(..8)
            .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?,
        bytes.len() as u64,
    )?;
    let mut previous: Option<Vec<u8>> = None;
    for block in &descriptor.blocks {
        for entry in decode_validated_block(
            descriptor.kind,
            descriptor.version,
            block,
            &bytes[block.start as usize..block.end as usize],
        )? {
            ensure!(
                previous
                    .as_ref()
                    .is_none_or(|key| compare_keys(key, &entry.key).is_lt()),
                "DATASET_KEY_INDEX_INVALID"
            );
            previous = Some(entry.key);
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn packed_partition_runs_roundtrip_and_delete_visibility() {
        let mut entries = (0u64..1000)
            .map(|i| RunEntry {
                key: format!("key-{i:06}").into_bytes(),
                value: RunValue::Hash(Sha256::digest(i.to_le_bytes()).into()),
            })
            .collect::<Vec<_>>();
        entries.sort_by(|a, b| compare_keys(&a.key, &b.key));
        let mut writer =
            RunWriter::new(RunKind::Hashes, &format!("sha256:{}", "a".repeat(64)), 7).unwrap();
        for entry in &entries {
            writer.push(entry).unwrap();
        }
        let (descriptor, bytes) = writer.finish().unwrap();
        assert_eq!(&bytes[..8], MAGIC);
        verify_complete_run(&descriptor, &bytes).unwrap();
        let mut actual = vec![];
        for slice in &descriptor.partitions {
            actual.extend(
                decode_partition(
                    &descriptor,
                    slice,
                    &bytes[slice.start as usize..slice.end as usize],
                )
                .unwrap(),
            );
        }
        assert_eq!(actual, entries);
        let key = b"key".to_vec();
        assert_eq!(
            merge_partition(
                RunKind::Hashes,
                partition(&key),
                &[
                    (
                        8,
                        vec![RunEntry {
                            key: key.clone(),
                            value: RunValue::Deleted
                        }]
                    ),
                    (
                        7,
                        vec![RunEntry {
                            key: key.clone(),
                            value: RunValue::Hash([1; 32])
                        }]
                    )
                ],
                10
            )
            .unwrap()[0]
                .value,
            RunValue::Deleted
        );
        assert_eq!(
            merge_partition(
                RunKind::Hashes,
                partition(&key),
                &[
                    (
                        9,
                        vec![RunEntry {
                            key: key.clone(),
                            value: RunValue::Hash([2; 32])
                        }]
                    ),
                    (
                        8,
                        vec![RunEntry {
                            key,
                            value: RunValue::Deleted
                        }]
                    )
                ],
                10
            )
            .unwrap()[0]
                .value,
            RunValue::Hash([2; 32])
        );
    }
    #[test]
    fn version_three_distinguishes_deferred_verified_zero_and_deleted_records() {
        let hash = format!("sha256:{}", "a".repeat(64));
        for (kind, values) in [
            (
                RunKind::Hashes,
                vec![
                    RunValue::Deleted,
                    RunValue::DeferredHash,
                    RunValue::Hash([0; 32]),
                ],
            ),
            (
                RunKind::Locations,
                vec![
                    RunValue::Deleted,
                    RunValue::Location(Locator {
                        file: "moria://w/t/data".into(),
                        row: 7,
                        data_sequence: 2,
                        row_hash: None,
                    }),
                    RunValue::Location(Locator {
                        file: "moria://w/t/data".into(),
                        row: 8,
                        data_sequence: 2,
                        row_hash: Some([0; 32]),
                    }),
                ],
            ),
        ] {
            let mut entries = values
                .into_iter()
                .enumerate()
                .map(|(i, value)| RunEntry {
                    key: vec![i as u8 + 1],
                    value,
                })
                .collect::<Vec<_>>();
            entries.sort_by(|a, b| compare_keys(&a.key, &b.key));
            let mut writer = RunWriter::new(kind, &hash, 2).unwrap();
            for entry in &entries {
                writer.push(entry).unwrap();
            }
            let (descriptor, bytes) = writer.finish().unwrap();
            assert_eq!(descriptor.version, 3);
            assert_eq!(&bytes[..8], b"MORIAKR3");
            verify_complete_run(&descriptor, &bytes).unwrap();
            let actual = descriptor
                .partitions
                .iter()
                .flat_map(|slice| {
                    decode_partition(
                        &descriptor,
                        slice,
                        &bytes[slice.start as usize..slice.end as usize],
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, entries);
            let mut legacy = descriptor.clone();
            legacy.version = 2;
            let mut legacy_bytes = bytes.clone();
            legacy_bytes[..8].copy_from_slice(LEGACY_MAGIC);
            assert!(verify_complete_run(&legacy, &legacy_bytes).is_err());
        }
    }
    #[test]
    fn version_two_known_records_remain_readable_and_unknown_versions_fail() {
        let mut writer =
            RunWriter::new(RunKind::Hashes, &format!("sha256:{}", "a".repeat(64)), 1).unwrap();
        writer
            .push(&RunEntry {
                key: b"key".to_vec(),
                value: RunValue::Hash([0; 32]),
            })
            .unwrap();
        let (mut descriptor, mut bytes) = writer.finish().unwrap();
        descriptor.version = 2;
        bytes[..8].copy_from_slice(LEGACY_MAGIC);
        verify_complete_run(&descriptor, &bytes).unwrap();
        assert_eq!(
            decode_partition(&descriptor, &descriptor.partitions[0], &bytes[8..]).unwrap()[0].value,
            RunValue::Hash([0; 32])
        );
        descriptor.version = 3;
        assert!(verify_complete_run(&descriptor, &bytes).is_err());
        bytes[..8].copy_from_slice(MAGIC);
        descriptor.version = 4;
        assert!(verify_complete_run(&descriptor, &bytes).is_err());
    }
    #[test]
    fn partial_compaction_retains_deletion_above_untouched_older_runs() {
        let key = b"retired".to_vec();
        let tombstone = RunEntry {
            key: key.clone(),
            value: RunValue::Deleted,
        };
        let partial = merge_partition(
            RunKind::Hashes,
            partition(&key),
            &[(3, vec![tombstone.clone()]), (2, vec![])],
            10,
        )
        .unwrap();
        let final_merge = merge_partition(
            RunKind::Hashes,
            partition(&key),
            &[
                (4, partial),
                (
                    1,
                    vec![RunEntry {
                        key,
                        value: RunValue::Hash([1; 32]),
                    }],
                ),
            ],
            10,
        )
        .unwrap();
        assert_eq!(final_merge, vec![tombstone]);
    }
    #[test]
    fn duplicates_malformed_counts_and_unknown_operations_rejected() {
        let hash = format!("sha256:{}", "a".repeat(64));
        let entry = RunEntry {
            key: b"one".to_vec(),
            value: RunValue::Hash([0; 32]),
        };
        let mut writer = RunWriter::new(RunKind::Hashes, &hash, 1).unwrap();
        writer.push(&entry).unwrap();
        assert!(writer.push(&entry).is_err());
        assert!(writer.finish().is_err());
        let mut writer = RunWriter::new(RunKind::Hashes, &hash, 1).unwrap();
        writer.push(&entry).unwrap();
        let (descriptor, bytes) = writer.finish().unwrap();
        let slice = &descriptor.partitions[0];
        let mut body = bytes[slice.start as usize..slice.end as usize].to_vec();
        body[4 + entry.key.len()] = 99;
        assert!(decode_partition(&descriptor, slice, &body).is_err());
        let mut wrong = descriptor.clone();
        wrong.partitions[0].entries = u64::MAX;
        assert!(decode_partition(&wrong, &wrong.partitions[0], &body).is_err());
        assert!(
            merge_partition(
                RunKind::Hashes,
                partition(&entry.key),
                &[(1, vec![entry.clone()]), (1, vec![entry])],
                10
            )
            .is_err()
        );
    }
    #[test]
    fn sparse_point_fences_bound_reads_and_reject_omitted_or_shifted_ranges() {
        let mut rows = (0..50000u64)
            .map(|i| RunEntry {
                key: i.to_be_bytes().to_vec(),
                value: RunValue::Hash([7; 32]),
            })
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| compare_keys(&a.key, &b.key));
        let mut writer =
            RunWriter::new(RunKind::Hashes, &format!("sha256:{}", "b".repeat(64)), 3).unwrap();
        for row in &rows {
            writer.push(row).unwrap();
        }
        let (descriptor, bytes) = writer.finish().unwrap();
        assert!(descriptor.blocks.len() > 500);
        DESCRIPTOR_VALIDATIONS.with(|count| count.set(0));
        verify_complete_run(&descriptor, &bytes).unwrap();
        assert_eq!(DESCRIPTOR_VALIDATIONS.with(|count| count.get()), 1);
        let wanted = &rows[24001];
        let blocks = point_blocks(&descriptor, &wanted.key).unwrap();
        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].end - blocks[0].start <= BLOCK_BYTES);
        let decoded = decode_block(
            &descriptor,
            blocks[0],
            &bytes[blocks[0].start as usize..blocks[0].end as usize],
        )
        .unwrap();
        assert!(decoded.iter().any(|row| row == wanted));
        assert!(decoded.len() <= 64);
        let mut wrong = descriptor.clone();
        wrong.blocks.remove(0);
        assert!(verify_complete_run(&wrong, &bytes).is_err());
        let mut wrong = descriptor.clone();
        wrong.blocks[0].minimum = wrong.blocks[0].maximum;
        assert!(verify_complete_run(&wrong, &bytes).is_err());
        let mut wrong = descriptor.clone();
        wrong.blocks[0].end -= 1;
        assert!(verify_complete_run(&wrong, &bytes).is_err());
    }
}
