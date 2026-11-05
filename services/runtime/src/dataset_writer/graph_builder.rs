//! Shared bounded graph construction for qualified initial bootstrap and new replacement output.
use super::{
    key_run::{Locator, RunEntry, RunKind, RunValue, RunWriter},
    key_run_io::{QualifiedRun, write_run},
    key_run_set,
    visibility::VisibilityBootstrap,
};
use crate::{
    dataset_graph::{NativeGraph, QualifiedNode},
    native_snapshot_v3::{INDEX_SUMMARY_PROPERTY, IndexSummary, IndexSummaryRef, LogicalFile},
    persistent_directory::{
        NodeReference,
        graph::{GraphDirectory, GraphValue},
    },
    storage::{ClosureSpool, SealedObject},
};
use anyhow::{Result, ensure};
use rusqlite::params;
use std::collections::BTreeMap;
pub(super) struct GraphBuilder {
    pub(super) graph: NativeGraph,
    pub(super) files: GraphDirectory,
    pub(super) selections: GraphDirectory,
    pub(super) files_root: Option<NodeReference>,
    pub(super) selections_root: Option<NodeReference>,
    pub(super) spool: ClosureSpool,
    pub(super) hashes: GraphDirectory,
    pub(super) locations: GraphDirectory,
    pub(super) hashes_root: Option<NodeReference>,
    pub(super) locations_root: Option<NodeReference>,
    pub(super) sequence: u64,
    pub(super) key_hash: String,
}
impl GraphBuilder {
    pub(super) async fn index_children(&self) -> Result<Vec<(String, QualifiedNode)>> {
        let mut children = Vec::new();
        for (label, reference, directory) in [
            ("selections", &self.selections_root, &self.selections),
            ("hashes", &self.hashes_root, &self.hashes),
            ("locations", &self.locations_root, &self.locations),
        ] {
            if let Some(reference) = reference {
                children.push((label.into(), directory.qualified_root(reference).await?));
            }
        }
        Ok(children)
    }

    pub(super) async fn seal_index_summary(
        &self,
        metadata: &mut serde_json::Value,
        snapshot: i64,
        summary: IndexSummary,
    ) -> Result<IndexSummaryRef> {
        ensure!(
            summary.snapshot_id == snapshot.to_string()
                && summary.sequence == self.sequence.to_string(),
            "NATIVE_INDEX_SUMMARY_CHANGED"
        );
        let node = self
            .graph
            .write_descriptor(
                "root",
                &serde_json::to_value(&summary)?,
                self.index_children().await?,
                summary.rows,
                0,
            )
            .await?;
        let reference = IndexSummaryRef {
            version: 1,
            root: node.content().clone(),
        };
        let snapshot = metadata["snapshots"]
            .as_array_mut()
            .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?
            .iter_mut()
            .find(|head| head["snapshot-id"] == snapshot)
            .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?;
        snapshot["summary"][INDEX_SUMMARY_PROPERTY] =
            serde_json::Value::String(reference.encode()?);
        Ok(reference)
    }
    pub(super) async fn file(
        &mut self,
        name: &str,
        content: SealedObject,
    ) -> Result<QualifiedNode> {
        self.graph.check_current().await?;
        let proof = self.graph.certify_retained_member(content.clone()).await?;
        let certificate = self.graph.register_certificate(&proof).await?;
        self.file_qualified(name, content, certificate).await
    }
    pub(super) async fn file_qualified(
        &mut self,
        name: &str,
        content: SealedObject,
        certificate: QualifiedNode,
    ) -> Result<QualifiedNode> {
        self.graph.check_current().await?;
        ensure!(
            certificate.kind() == "certificate",
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let proof: crate::authenticated_file::FileCertificate =
            serde_json::from_value(self.graph.read_descriptor(&certificate).await?)?;
        ensure!(proof.content() == &content, "CONTENT_MISMATCH");
        let value = GraphValue::new(
            serde_json::to_value(LogicalFile {
                name: name.into(),
                content,
            })?,
            vec![certificate.clone()],
        )?;
        self.files_root = self
            .files
            .apply(
                self.files_root.as_ref(),
                BTreeMap::from([(name.into(), Some(value))]),
            )
            .await?;
        Ok(certificate)
    }
    async fn record_run(&self, runs_table: &str, kind: RunKind, run: QualifiedRun) -> Result<()> {
        self.graph.check_current().await?;
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        for slice in &run.descriptor().partitions {
            let segment = key_run_set::segment(&run, slice.partition)?;
            db.execute(
                &format!(
                    "INSERT INTO {}(kind,partition,minimum,segment,content)VALUES(?1,?2,?3,?4,?5)",
                    runs_table
                ),
                params![
                    match kind {
                        RunKind::Hashes => "hashes",
                        RunKind::Locations => "locations",
                    },
                    slice.partition,
                    run.descriptor()
                        .blocks
                        .iter()
                        .find(|block| block.partition == slice.partition)
                        .unwrap()
                        .minimum
                        .as_slice(),
                    serde_json::to_string(&segment)?,
                    serde_json::to_string(run.node().content())?
                ],
            )?;
        }
        Ok(())
    }
    pub(super) async fn index(&mut self, visible: &VisibilityBootstrap) -> Result<()> {
        let runs_table = format!("bootstrap_runs_{}", uuid::Uuid::new_v4().simple());
        {
            let db = self.spool.database()?;
            self.spool.assert_scope(&db)?;
            db.execute_batch(&format!(
                "CREATE TABLE {runs_table}(kind TEXT NOT NULL,partition INTEGER NOT NULL,minimum BLOB NOT NULL,
                 segment TEXT NOT NULL,content TEXT NOT NULL,PRIMARY KEY(kind,partition,minimum,content)) WITHOUT ROWID"
            ))?;
        }
        let mut after = None;
        let mut hash_rows = Vec::new();
        let mut location_rows = Vec::new();
        let mut sizing = RunWriter::new(RunKind::Locations, &self.key_hash, self.sequence)?;
        loop {
            let page = visible.route_page(after.as_deref()).await?;
            if page.is_empty() {
                break;
            }
            for row in &page {
                let location = RunEntry {
                    key: row.key.clone(),
                    value: RunValue::Location(Locator {
                        file: row.file.clone(),
                        row: row.row,
                        data_sequence: row.data_sequence,
                        row_hash: row.row_hash,
                    }),
                };
                if !sizing.would_fit(&location) {
                    ensure!(!location_rows.is_empty(), "RESOURCE_LIMIT");
                    self.flush_runs(&runs_table, &hash_rows, &location_rows)
                        .await?;
                    hash_rows.clear();
                    location_rows.clear();
                    sizing = RunWriter::new(RunKind::Locations, &self.key_hash, self.sequence)?;
                }
                sizing.push(&location)?;
                hash_rows.push(RunEntry {
                    key: row.key.clone(),
                    value: row.row_hash.map_or(RunValue::DeferredHash, RunValue::Hash),
                });
                location_rows.push(location);
            }
            after = page.last().map(|row| row.key.clone());
        }
        if !location_rows.is_empty() {
            self.flush_runs(&runs_table, &hash_rows, &location_rows)
                .await?;
        }
        // Each bucket contains only finite run descriptors; the full population remains spill-backed.
        for kind in [RunKind::Hashes, RunKind::Locations] {
            let mut mutations = BTreeMap::new();
            for partition in 0..=255u8 {
                let records = {
                    let db = self.spool.database()?;
                    self.spool.assert_scope(&db)?;
                    let mut statement=db.prepare(&format!("SELECT segment,content FROM {} WHERE kind=?1 AND partition=?2 ORDER BY minimum,content LIMIT 113",runs_table))?;
                    statement
                        .query_map(
                            params![
                                match kind {
                                    RunKind::Hashes => "hashes",
                                    RunKind::Locations => "locations",
                                },
                                partition
                            ],
                            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                        )?
                        .collect::<rusqlite::Result<Vec<_>>>()?
                };
                if records.is_empty() {
                    continue;
                }
                self.graph.check_current().await?;
                ensure!(records.len() <= 112, "RESOURCE_LIMIT");
                let mut nodes = Vec::with_capacity(records.len());
                for (segment, content) in records {
                    let content: SealedObject = serde_json::from_str(&content)?;
                    let node = self
                        .graph
                        .known_node(&content)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_ANCESTRY_REQUIRED"))?;
                    nodes.push((serde_json::from_str(&segment)?, node));
                }
                let value = key_run_set::bootstrap_value(
                    &self.graph,
                    kind,
                    &self.key_hash,
                    partition,
                    nodes,
                )
                .await?;
                mutations.insert(format!("{partition:02x}"), Some(value));
                // Bound temporary metadata while avoiding a new immutable root for every bucket.
                if mutations.len() == 16 {
                    self.apply_index(kind, std::mem::take(&mut mutations))
                        .await?;
                }
            }
            if !mutations.is_empty() {
                self.apply_index(kind, mutations).await?;
            }
        }
        Ok(())
    }
    async fn apply_index(
        &mut self,
        kind: RunKind,
        mutations: BTreeMap<String, Option<GraphValue>>,
    ) -> Result<()> {
        match kind {
            RunKind::Hashes => {
                self.hashes_root = self
                    .hashes
                    .apply(self.hashes_root.as_ref(), mutations)
                    .await?
            }
            RunKind::Locations => {
                self.locations_root = self
                    .locations
                    .apply(self.locations_root.as_ref(), mutations)
                    .await?
            }
        }
        Ok(())
    }
    async fn flush_runs(
        &self,
        runs_table: &str,
        hashes: &[RunEntry],
        locations: &[RunEntry],
    ) -> Result<()> {
        self.record_run(
            runs_table,
            RunKind::Hashes,
            write_run(
                &self.graph,
                RunKind::Hashes,
                &self.key_hash,
                self.sequence,
                hashes,
            )
            .await?,
        )
        .await?;
        self.record_run(
            runs_table,
            RunKind::Locations,
            write_run(
                &self.graph,
                RunKind::Locations,
                &self.key_hash,
                self.sequence,
                locations,
            )
            .await?,
        )
        .await
    }
}
