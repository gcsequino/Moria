//! Spill only new changed keys, then seal packed touched-bucket updates.
use super::{
    graph_builder::GraphBuilder,
    key_run::{Locator, RunEntry, RunKind, RunValue, RunWriter, key_route},
    key_run_io::{QualifiedRun, seal_run},
    key_run_set::update::{BucketWrite, CompactionBudget, update_value},
};
use crate::{
    persistent_directory::{
        NodeReference,
        graph::{GraphDirectory, GraphValue},
    },
    storage::ClosureSpool,
};
use anyhow::{Result, ensure};
use rusqlite::params;
use serde_json::json;
use std::collections::BTreeMap;
use uuid::Uuid;

const MAX_PAGE: usize = 1000;
const MAX_KEY_BYTES: usize = 8 * 1024 * 1024;
type Cursor = (Vec<u8>, Vec<u8>);
struct Packed {
    after: Cursor,
    hashes: RunWriter,
    locations: RunWriter,
    rows: u64,
}

pub(super) struct IndexChanges {
    spool: ClosureSpool,
    table: String,
    delete: bool,
    rows: u64,
}
impl IndexChanges {
    pub(super) fn new(spool: ClosureSpool, delete: bool) -> Result<Self> {
        let db = spool.database()?;
        spool.assert_scope(&db)?;
        let table = format!("mutation_keys_{}", Uuid::new_v4().simple());
        db.execute_batch(&format!(
            "CREATE TABLE {table}(partition INTEGER NOT NULL,route BLOB NOT NULL,key BLOB NOT NULL,
                locator TEXT,verified INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(partition,route,key)) WITHOUT ROWID"
        ))?;
        Ok(Self {
            spool,
            table,
            delete,
            rows: 0,
        })
    }
    pub(super) fn insert(&mut self, changes: Vec<(Vec<u8>, Option<Locator>)>) -> Result<()> {
        ensure!(
            changes.len() <= MAX_PAGE
                && changes
                    .iter()
                    .try_fold(0usize, |bytes, (key, _)| bytes.checked_add(key.len()))
                    .is_some_and(|bytes| bytes <= MAX_KEY_BYTES),
            "RESOURCE_LIMIT"
        );
        let mut db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let tx = db.transaction()?;
        {
            let mut insert = tx.prepare(&format!(
                "INSERT INTO {}(partition,route,key,locator) VALUES(?1,?2,?3,?4)",
                self.table
            ))?;
            for (key, locator) in &changes {
                ensure!(
                    !key.is_empty()
                        && key.len() <= 4 * 1024 * 1024
                        && locator.is_none() == self.delete
                        && locator
                            .as_ref()
                            .is_none_or(|locator| locator.row_hash.is_some()),
                    "DATASET_KEY_INDEX_INVALID"
                );
                let route = key_route(key);
                let locator = locator.as_ref().map(serde_json::to_string).transpose()?;
                match insert.execute(params![route[0], route.as_slice(), key, locator]) {
                    Err(rusqlite::Error::SqliteFailure(code, _))
                        if code.code == rusqlite::ErrorCode::ConstraintViolation =>
                    {
                        anyhow::bail!("DATASET_PRIMARY_KEY_DUPLICATE")
                    }
                    Err(rusqlite::Error::SqliteFailure(code, _))
                        if code.code == rusqlite::ErrorCode::DiskFull =>
                    {
                        anyhow::bail!("RESOURCE_LIMIT")
                    }
                    result => {
                        result?;
                    }
                }
            }
        }
        let rows = self
            .rows
            .checked_add(changes.len() as u64)
            .filter(|rows| *rows <= 9_007_199_254_740_991)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        tx.commit()?;
        self.rows = rows;
        Ok(())
    }
    pub(super) fn verify(&self, entries: &[(Vec<u8>, Option<Locator>)]) -> Result<()> {
        ensure!(entries.len() <= MAX_PAGE, "RESOURCE_LIMIT");
        let mut db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let tx = db.transaction()?;
        {
            let mut verify = tx.prepare(&format!(
                "UPDATE {} SET verified=1 WHERE partition=?1 AND route=?2 AND key=?3 AND locator IS ?4 AND verified=0",self.table))?;
            for (key, locator) in entries {
                let route = key_route(key);
                let locator = locator.as_ref().map(serde_json::to_string).transpose()?;
                ensure!(
                    verify.execute(params![route[0], route.as_slice(), key, locator])? == 1,
                    "DATASET_MUTATION_PROOF_CHANGED"
                );
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub(super) fn finish_verification(&self) -> Result<()> {
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let missing: bool = db.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM {} WHERE verified=0)",
                self.table
            ),
            [],
            |row| row.get(0),
        )?;
        ensure!(!missing, "DATASET_MUTATION_PROOF_CHANGED");
        Ok(())
    }
    fn pack(
        &self,
        partition: u8,
        after: &Cursor,
        key_hash: &str,
        sequence: u64,
    ) -> Result<Option<Packed>> {
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let mut query = db.prepare(&format!(
            "SELECT route,key,locator FROM {} WHERE partition=?1 AND (route,key)>(?2,?3) ORDER BY route,key", self.table
        ))?;
        let mut source = query.query(params![partition, &after.0, &after.1])?;
        let mut hashes = RunWriter::new(RunKind::Hashes, key_hash, sequence)?;
        let mut locations = RunWriter::new(RunKind::Locations, key_hash, sequence)?;
        let mut rows = 0u64;
        let mut last = None;
        while let Some(row) = source.next()? {
            let route: Vec<u8> = row.get(0)?;
            let key: Vec<u8> = row.get(1)?;
            ensure!(
                key_route(&key).as_slice() == route.as_slice(),
                "DATASET_KEY_INDEX_INVALID"
            );
            let locator: Option<Locator> = row
                .get::<_, Option<String>>(2)?
                .map(|value| serde_json::from_str(&value))
                .transpose()?;
            let hash = match &locator {
                Some(locator) => {
                    ensure!(
                        locator.data_sequence == sequence,
                        "DATASET_KEY_INDEX_INVALID"
                    );
                    RunValue::Hash(
                        locator
                            .row_hash
                            .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?,
                    )
                }
                None => RunValue::Deleted,
            };
            let location = locator.map_or(RunValue::Deleted, RunValue::Location);
            let hash = RunEntry {
                key: key.clone(),
                value: hash,
            };
            let location = RunEntry {
                key: key.clone(),
                value: location,
            };
            if !hashes.would_fit(&hash) || !locations.would_fit(&location) {
                ensure!(rows > 0, "RESOURCE_LIMIT");
                break;
            }
            hashes.push(&hash)?;
            locations.push(&location)?;
            rows += 1;
            last = Some((route, key));
        }
        Ok(last.map(|after| Packed {
            after,
            hashes,
            locations,
            rows,
        }))
    }
    pub(super) async fn seal(
        &self,
        builder: &mut GraphBuilder,
        expected_rows: u64,
        budget: &mut CompactionBudget,
    ) -> Result<Option<NodeReference>> {
        ensure!(self.rows == expected_rows, "DATASET_KEY_INDEX_INVALID");
        let graph = builder.graph.clone();
        let delete_dir = GraphDirectory::new(graph.clone(), vec![])?;
        let mut delete_root = None;
        let mut seen = 0u64;
        for partition in 0..=255u8 {
            let mut after = (vec![], vec![]);
            let mut hash_runs = Vec::<QualifiedRun>::new();
            let mut location_runs = Vec::<QualifiedRun>::new();
            while let Some(packed) =
                self.pack(partition, &after, &builder.key_hash, builder.sequence)?
            {
                graph.check_current().await?;
                ensure!(
                    hash_runs.len() < 128,
                    "DATASET_KEY_INDEX_COMPACTION_REQUIRED"
                );
                let Packed {
                    after: next,
                    hashes,
                    locations,
                    rows,
                } = packed;
                let hash = seal_run(&graph, hashes).await?;
                let location = seal_run(&graph, locations).await?;
                ensure!(
                    hash.descriptor().entries == rows && location.descriptor().entries == rows,
                    "DATASET_KEY_INDEX_INVALID"
                );
                if self.delete {
                    let key = format!("{partition:02x}-{:04x}", hash_runs.len());
                    delete_root = delete_dir.apply(delete_root.as_ref(), BTreeMap::from([(
                        key, Some(GraphValue::new(json!({
                            "partition":partition, "rows":rows, "sequence":builder.sequence.to_string(),
                            "run":hash.node().content(),
                        }), vec![hash.node().clone()])?),
                    )])).await?;
                }
                hash_runs.push(hash);
                location_runs.push(location);
                seen = seen
                    .checked_add(rows)
                    .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
                after = next;
            }
            if hash_runs.is_empty() {
                continue;
            }
            let key = format!("{partition:02x}");
            for (kind, directory, root, runs) in [
                (
                    RunKind::Hashes,
                    &builder.hashes,
                    &mut builder.hashes_root,
                    hash_runs,
                ),
                (
                    RunKind::Locations,
                    &builder.locations,
                    &mut builder.locations_root,
                    location_runs,
                ),
            ] {
                let previous = match root.as_ref() {
                    Some(reference) => directory.lookup_with_links(reference, &key).await?,
                    None => None,
                };
                let value = update_value(
                    &graph,
                    &builder.spool,
                    previous,
                    runs,
                    BucketWrite {
                        kind,
                        key_schema_hash: &builder.key_hash,
                        partition,
                        sequence: builder.sequence,
                    },
                    budget,
                )
                .await?;
                *root = directory
                    .apply(root.as_ref(), BTreeMap::from([(key.clone(), Some(value))]))
                    .await?;
            }
        }
        ensure!(
            seen == expected_rows && (delete_root.is_some() == (self.delete && seen > 0)),
            "DATASET_KEY_INDEX_INVALID"
        );
        graph.check_current().await?;
        Ok(delete_root)
    }
}
impl Drop for IndexChanges {
    fn drop(&mut self) {
        if let Ok(db) = self.spool.database() {
            let _ = db.execute_batch(&format!("DROP TABLE IF EXISTS {}", self.table));
        }
    }
}
