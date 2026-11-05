//! Bounded level compaction: spill full keys and keep tombstones while any older ranges survive.
use super::*;
use crate::{
    dataset_writer::{
        key_run::{RunDescriptor, RunEntry, RunWriter},
        key_run_io::{producer_partition, seal_run},
    },
    storage::ClosureSpool,
};
use futures::TryStreamExt;
use rusqlite::params;
use uuid::Uuid;

const LEVELS: usize = 8;
const NEWEST_RUNS: usize = 8;
const LEVEL_RUNS: usize = 16;

pub(crate) struct BucketWrite<'a> {
    pub(crate) kind: RunKind,
    pub(crate) key_schema_hash: &'a str,
    pub(crate) partition: u8,
    pub(crate) sequence: u64,
}
pub(crate) struct CompactionBudget {
    pub(crate) remaining_rows: u64,
    pub(crate) remaining_bytes: u64,
}
impl CompactionBudget {
    fn admit(&mut self, rows: u64, bytes: u64) -> Result<()> {
        ensure!(
            rows <= self.remaining_rows && bytes <= self.remaining_bytes,
            "DATASET_KEY_INDEX_COMPACTION_REQUIRED"
        );
        self.remaining_rows -= rows;
        self.remaining_bytes -= bytes;
        Ok(())
    }
}

#[derive(Clone)]
struct AdmittedSegment {
    summary: Segment,
    node: QualifiedNode,
}
impl AdmittedSegment {
    fn validate(&self, descriptor: &RunDescriptor, partition: u8) -> Result<()> {
        let mut blocks = descriptor
            .blocks
            .iter()
            .filter(|block| block.partition == partition);
        let first = blocks
            .next()
            .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
        let last = blocks.last().unwrap_or(first);
        ensure!(
            self.node.content() == &self.summary.run
                && self.node.kind() == "keyRun"
                && descriptor.sequence == self.summary.sequence
                && first.minimum == self.summary.minimum
                && last.maximum == self.summary.maximum,
            "DATASET_KEY_INDEX_INVALID"
        );
        Ok(())
    }
}

type Cursor = (Vec<u8>, Vec<u8>);
struct Spill<'a> {
    spool: &'a ClosureSpool,
    table: String,
}
impl<'a> Spill<'a> {
    fn new(spool: &'a ClosureSpool) -> Result<Self> {
        let db = spool.database()?;
        spool.assert_scope(&db)?;
        let table = format!("key_merge_{}", Uuid::new_v4().simple());
        db.execute_batch(&format!(
            "CREATE TABLE {table}(route BLOB NOT NULL,key BLOB NOT NULL,value TEXT NOT NULL,priority INTEGER NOT NULL,PRIMARY KEY(route,key)) WITHOUT ROWID"
        ))?;
        Ok(Self { spool, table })
    }
    fn insert(&self, rows: Vec<RunEntry>, priority: i64, partition: u8) -> Result<()> {
        let mut db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let tx = db.transaction()?;
        {
            let mut insert = tx.prepare(&format!(
                "INSERT INTO {}(route,key,value,priority) VALUES(?1,?2,?3,?4) ON CONFLICT(route,key) DO NOTHING",
                self.table
            ))?;
            let mut earlier = tx.prepare(&format!(
                "SELECT priority FROM {} WHERE route=?1 AND key=?2",
                self.table
            ))?;
            for entry in rows {
                let route = key_route(&entry.key);
                ensure!(route[0] == partition, "DATASET_KEY_INDEX_INVALID");
                let value = serde_json::to_string(&entry.value)?;
                let inserted =
                    insert.execute(params![route.as_slice(), &entry.key, value, priority]);
                let inserted = match inserted {
                    Err(rusqlite::Error::SqliteFailure(code, _))
                        if code.code == rusqlite::ErrorCode::DiskFull =>
                    {
                        anyhow::bail!("RESOURCE_LIMIT")
                    }
                    result => result?,
                };
                if inserted == 0 {
                    let first: i64 = earlier
                        .query_row(params![route.as_slice(), &entry.key], |row| row.get(0))?;
                    // Newest priority is inserted first. Equal-priority duplicates are invalid.
                    ensure!(first < priority, "DATASET_PRIMARY_KEY_DUPLICATE");
                }
            }
        }
        tx.commit()?;
        Ok(())
    }
    async fn add(
        &self,
        graph: &NativeGraph,
        segment: &AdmittedSegment,
        write: &BucketWrite<'_>,
        priority: i64,
        budget: &mut CompactionBudget,
    ) -> Result<()> {
        let (descriptor, mut stream) = producer_partition(
            graph,
            &segment.node,
            write.key_schema_hash,
            write.kind,
            write.partition,
        )
        .await?;
        segment.validate(&descriptor, write.partition)?;
        let slice = descriptor
            .partitions
            .iter()
            .find(|slice| slice.partition == write.partition)
            .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
        budget.admit(
            slice.entries,
            slice
                .end
                .checked_sub(slice.start)
                .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?,
        )?;
        let mut seen = 0u64;
        while let Some(rows) = stream.try_next().await? {
            seen = seen
                .checked_add(rows.len() as u64)
                .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
            ensure!(seen <= slice.entries, "DATASET_KEY_INDEX_INVALID");
            self.insert(rows, priority, write.partition)?;
        }
        ensure!(seen == slice.entries, "DATASET_KEY_INDEX_INVALID");
        graph.check_current().await
    }
    fn bounds(&self) -> Result<([u8; 32], [u8; 32])> {
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let first: Vec<u8> = db.query_row(
            &format!(
                "SELECT route FROM {} ORDER BY route,key LIMIT 1",
                self.table
            ),
            [],
            |row| row.get(0),
        )?;
        let last: Vec<u8> = db.query_row(
            &format!(
                "SELECT route FROM {} ORDER BY route DESC,key DESC LIMIT 1",
                self.table
            ),
            [],
            |row| row.get(0),
        )?;
        Ok((
            first
                .try_into()
                .map_err(|_| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?,
            last.try_into()
                .map_err(|_| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?,
        ))
    }
    fn plan(&self, write: &BucketWrite<'_>, capacity: usize) -> Result<Option<Vec<usize>>> {
        if capacity == 0 {
            return Ok(None);
        }
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let mut query = db.prepare(&format!(
            "SELECT key,value FROM {} ORDER BY route,key",
            self.table
        ))?;
        let mut rows = query.query([])?;
        let mut plan = Vec::new();
        let mut writer = RunWriter::new(write.kind, write.key_schema_hash, write.sequence)?;
        let mut count = 0usize;
        while let Some(row) = rows.next()? {
            let entry = RunEntry {
                key: row.get(0)?,
                value: serde_json::from_str(&row.get::<_, String>(1)?)?,
            };
            if !writer.would_fit(&entry) {
                ensure!(count > 0, "RESOURCE_LIMIT");
                plan.push(count);
                if plan.len() == capacity {
                    return Ok(None);
                }
                writer = RunWriter::new(write.kind, write.key_schema_hash, write.sequence)?;
                count = 0;
            }
            writer.push(&entry)?;
            count += 1;
        }
        ensure!(count > 0, "DATASET_KEY_INDEX_INVALID");
        plan.push(count);
        Ok(Some(plan))
    }
    fn load(&self, writer: &mut RunWriter, cursor: &Cursor, count: usize) -> Result<Cursor> {
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let mut query = db.prepare(&format!(
            "SELECT route,key,value FROM {} WHERE (route,key)>(?1,?2) ORDER BY route,key LIMIT ?3",
            self.table
        ))?;
        let mut rows = query.query(params![&cursor.0, &cursor.1, count as i64])?;
        let mut last = None;
        let mut seen = 0;
        while let Some(row) = rows.next()? {
            let route: Vec<u8> = row.get(0)?;
            let key: Vec<u8> = row.get(1)?;
            ensure!(
                key_route(&key).as_slice() == route.as_slice(),
                "DATASET_KEY_INDEX_INVALID"
            );
            writer.push(&RunEntry {
                key: key.clone(),
                value: serde_json::from_str(&row.get::<_, String>(2)?)?,
            })?;
            last = Some((route, key));
            seen += 1;
        }
        ensure!(seen == count, "DATASET_KEY_INDEX_INVALID");
        last.ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))
    }
}
impl Drop for Spill<'_> {
    fn drop(&mut self) {
        if let Ok(db) = self.spool.database() {
            let _ = db.execute_batch(&format!("DROP TABLE IF EXISTS {}", self.table));
        }
    }
}

/// The interval includes all boundary-equal ranges so untouched siblings are disjoint even
/// when two distinct full keys happen to share a routing hash.
fn overlap_bounds(
    segments: impl Clone + Iterator<Item = ([u8; 32], [u8; 32])>,
    mut minimum: [u8; 32],
    mut maximum: [u8; 32],
) -> ([u8; 32], [u8; 32]) {
    loop {
        let previous = (minimum, maximum);
        for (left, right) in segments.clone() {
            if right >= minimum && left <= maximum {
                minimum = minimum.min(left);
                maximum = maximum.max(right);
            }
        }
        if previous == (minimum, maximum) {
            return previous;
        }
    }
}

fn open(
    previous: Option<(serde_json::Value, Vec<QualifiedNode>)>,
    write: &BucketWrite<'_>,
) -> Result<Vec<Vec<AdmittedSegment>>> {
    let Some((value, links)) = previous else {
        return Ok(vec![vec![]; LEVELS]);
    };
    let bucket: Bucket = serde_json::from_value(value)?;
    bucket.validate()?;
    ensure!(
        bucket.kind == write.kind
            && bucket.key_schema_hash == write.key_schema_hash
            && bucket.partition == write.partition
            && bucket.levels.iter().map(Vec::len).sum::<usize>() == links.len(),
        "DATASET_KEY_INDEX_INVALID"
    );
    let mut links = links.into_iter();
    bucket
        .levels
        .into_iter()
        .map(|level| {
            level
                .into_iter()
                .map(|summary| {
                    let node = links
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
                    ensure!(
                        node.kind() == "keyRun"
                            && node.content() == &summary.run
                            && summary.sequence < write.sequence,
                        "DATASET_KEY_INDEX_INVALID"
                    );
                    Ok(AdmittedSegment { summary, node })
                })
                .collect()
        })
        .collect()
}
fn value(levels: Vec<Vec<AdmittedSegment>>, write: &BucketWrite<'_>) -> Result<GraphValue> {
    let mut links = Vec::new();
    let levels = levels
        .into_iter()
        .map(|level| {
            level
                .into_iter()
                .map(|segment| {
                    links.push(segment.node);
                    segment.summary
                })
                .collect()
        })
        .collect();
    let bucket = Bucket {
        version: 3,
        kind: write.kind,
        key_schema_hash: write.key_schema_hash.into(),
        partition: write.partition,
        levels,
    };
    bucket.validate()?;
    GraphValue::new(serde_json::to_value(bucket)?, links)
}

/// The fast path appends one new run. Bounded overflow compacts the newest runs and only
/// overlapping ranges at each touched older level; it never scans other key partitions.
pub(crate) async fn update_value(
    graph: &NativeGraph,
    spool: &ClosureSpool,
    previous: Option<(serde_json::Value, Vec<QualifiedNode>)>,
    runs: Vec<QualifiedRun>,
    write: BucketWrite<'_>,
    budget: &mut CompactionBudget,
) -> Result<GraphValue> {
    graph.check_current().await?;
    ensure!(!runs.is_empty() && runs.len() <= 128, "RESOURCE_LIMIT");
    let mut levels = open(previous, &write)?;
    let mut incoming = Vec::new();
    for run in runs {
        ensure!(
            run.descriptor().kind == write.kind
                && run.descriptor().key_schema_hash == write.key_schema_hash
                && run.descriptor().sequence == write.sequence,
            "DATASET_KEY_INDEX_INVALID"
        );
        incoming.push(AdmittedSegment {
            summary: segment(&run, write.partition)?,
            node: run.node().clone(),
        });
    }
    if incoming.len() == 1 && levels[0].len() < NEWEST_RUNS {
        levels[0].insert(0, incoming.pop().unwrap());
        return value(levels, &write);
    }
    let spill = Spill::new(spool)?;
    for item in incoming {
        spill.add(graph, &item, &write, 0, budget).await?;
    }
    for (index, item) in std::mem::take(&mut levels[0]).into_iter().enumerate() {
        spill
            .add(graph, &item, &write, 1 + index as i64, budget)
            .await?;
    }
    for level in 1..LEVELS {
        let (minimum, maximum) = spill.bounds()?;
        let (minimum, maximum) = overlap_bounds(
            levels[level]
                .iter()
                .map(|item| (item.summary.minimum, item.summary.maximum)),
            minimum,
            maximum,
        );
        let (overlap, keep): (Vec<_>, Vec<_>) = std::mem::take(&mut levels[level])
            .into_iter()
            .partition(|item| item.summary.maximum >= minimum && item.summary.minimum <= maximum);
        levels[level] = keep;
        for item in overlap {
            spill
                .add(graph, &item, &write, 9 + level as i64, budget)
                .await?;
        }
        let Some(plan) = spill.plan(&write, LEVEL_RUNS - levels[level].len())? else {
            continue;
        };
        let mut cursor = (vec![], vec![]);
        for count in plan {
            let mut writer = RunWriter::new(write.kind, write.key_schema_hash, write.sequence)?;
            cursor = spill.load(&mut writer, &cursor, count)?;
            let run = seal_run(graph, writer).await?;
            levels[level].push(AdmittedSegment {
                summary: segment(&run, write.partition)?,
                node: run.node().clone(),
            });
        }
        levels[level].sort_by_key(|item| item.summary.minimum);
        graph.check_current().await?;
        return value(levels, &write);
    }
    anyhow::bail!("DATASET_KEY_INDEX_COMPACTION_REQUIRED")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundary_equal_hash_ranges_compact_transitively_without_touching_disjoint_ranges() {
        let route = |byte| {
            let mut key = [0u8; 32];
            key[31] = byte;
            key
        };
        let ranges = [
            (route(1), route(4)),
            (route(4), route(8)),
            (route(8), route(10)),
            (route(20), route(30)),
        ];
        assert_eq!(
            overlap_bounds(ranges.into_iter(), route(7), route(7)),
            (route(1), route(10))
        );
        assert_eq!(
            overlap_bounds(ranges.into_iter(), route(11), route(19)),
            (route(11), route(19))
        );
    }

    #[derive(Debug)]
    struct ScratchOnly(crate::storage::LogicalObject);
    #[async_trait::async_trait]
    impl crate::storage::WriteAuthority for ScratchOnly {
        fn scope_key(&self) -> String {
            "bounded-run-merge-test".into()
        }
        async fn check_write(&self, object: &crate::storage::LogicalObject) -> Result<()> {
            ensure!(object == &self.0, "BINDING_MISMATCH");
            Ok(())
        }
    }

    #[tokio::test]
    async fn partial_compaction_emits_tombstone_even_when_older_ranges_are_unread() -> Result<()> {
        use crate::{
            dataset_writer::key_run::decode_partition,
            storage::{Cancellation, ClosureLimits, LogicalObject, PosixStore, StorageLimits},
        };
        let root = tempfile::tempdir()?;
        let store = PosixStore::provision(
            root.path(),
            "w",
            "t",
            StorageLimits {
                max_object_bytes: 4 * 1024 * 1024,
                chunk_bytes: 1024 * 1024,
            },
        )?;
        let object = LogicalObject::new("w", "t", "merge")?;
        let spool = ClosureSpool::create(
            &store,
            object.clone(),
            ClosureLimits {
                max_entries: 1000,
                max_referenced_bytes: 4 * 1024 * 1024,
                max_spool_bytes: 4 * 1024 * 1024,
            },
            Arc::new(ScratchOnly(object)),
            Cancellation::default(),
        )
        .await?;
        let merge = Spill::new(&spool)?;
        let key = br#"["one"]"#.to_vec();
        let key_hash = format!("sha256:{}", "a".repeat(64));
        let write = BucketWrite {
            kind: RunKind::Hashes,
            key_schema_hash: &key_hash,
            partition: key_route(&key)[0],
            sequence: 9,
        };
        let deleted = RunEntry {
            key: key.clone(),
            value: RunValue::Deleted,
        };
        merge.insert(vec![deleted.clone()], 0, write.partition)?;
        assert!(
            merge
                .insert(vec![deleted.clone()], 0, write.partition)
                .is_err()
        );
        merge.insert(
            vec![RunEntry {
                key: key.clone(),
                value: RunValue::Hash([7; 32]),
            }],
            1,
            write.partition,
        )?;
        // The partial merge has no proof about untouched deeper levels. Keep the delete.
        assert_eq!(merge.plan(&write, 1)?, Some(vec![1]));
        let mut writer = RunWriter::new(write.kind, write.key_schema_hash, write.sequence)?;
        merge.load(&mut writer, &(vec![], vec![]), 1)?;
        let (descriptor, bytes) = writer.finish()?;
        let slice = &descriptor.partitions[0];
        assert_eq!(
            decode_partition(
                &descriptor,
                slice,
                &bytes[slice.start as usize..slice.end as usize]
            )?,
            vec![deleted]
        );
        Ok(())
    }

    #[test]
    fn compaction_budget_rejects_overflow_without_partial_reservation() {
        let mut budget = CompactionBudget {
            remaining_rows: 5,
            remaining_bytes: 10,
        };
        assert!(budget.admit(3, 11).is_err());
        assert_eq!((budget.remaining_rows, budget.remaining_bytes), (5, 10));
        budget.admit(3, 9).unwrap();
        assert_eq!((budget.remaining_rows, budget.remaining_bytes), (2, 1));
        assert!(budget.admit(3, 1).is_err());
    }
}
