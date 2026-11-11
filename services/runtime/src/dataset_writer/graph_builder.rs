//! Shared bounded graph construction for qualified initial bootstrap and new replacement output.
use super::{
    key_run::{Locator, RunEntry, RunKind, RunValue, RunWriter},
    key_run_io::{QualifiedRun, seal_run},
    key_run_set,
    visibility::VisibilityBootstrap,
};
use crate::{
    dataset_graph::{NativeGraph, QualifiedNode},
    native_snapshot_v3::LogicalFile,
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
    pub(super) runs_table: String,
    pub(super) hashes: GraphDirectory,
    pub(super) locations: GraphDirectory,
    pub(super) hashes_root: Option<NodeReference>,
    pub(super) locations_root: Option<NodeReference>,
    pub(super) sequence: u64,
    pub(super) key_hash: String,
}
impl GraphBuilder {
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
    async fn record_run(&self, kind: RunKind, run: QualifiedRun) -> Result<()> {
        self.graph.check_current().await?;
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        for slice in &run.descriptor().partitions {
            let segment = key_run_set::segment(&run, slice.partition)?;
            db.execute(
                &format!(
                    "INSERT INTO {}(kind,partition,minimum,segment,content)VALUES(?1,?2,?3,?4,?5)",
                    self.runs_table
                ),
                params![
                    match kind {
                        RunKind::Hashes => "hashes",
                        RunKind::Locations => "locations",
                    },
                    slice.partition,
                    segment.minimum.as_slice(),
                    serde_json::to_string(&segment)?,
                    serde_json::to_string(run.node().content())?
                ],
            )?;
        }
        Ok(())
    }
    pub(super) async fn index(&mut self, visible: &VisibilityBootstrap) -> Result<()> {
        let writer = |kind| RunWriter::new(kind, &self.key_hash, self.sequence);
        let mut hashes = writer(RunKind::Hashes)?;
        let mut locations = writer(RunKind::Locations)?;
        let mut after = None;
        loop {
            let page = visible.route_page(after.as_deref()).await?;
            if page.is_empty() {
                break;
            }
            after = page.last().map(|row| row.key.clone());
            for row in page {
                let mut entry = RunEntry {
                    key: row.key,
                    value: RunValue::Location(Locator {
                        file: row.file,
                        row: row.row,
                        data_sequence: row.data_sequence,
                        row_hash: row.row_hash,
                    }),
                };
                if !locations.would_fit(&entry) {
                    ensure!(!locations.is_empty(), "RESOURCE_LIMIT");
                    self.flush_runs(hashes, locations).await?;
                    hashes = writer(RunKind::Hashes)?;
                    locations = writer(RunKind::Locations)?;
                }
                locations.push(&entry)?;
                entry.value = row.row_hash.map_or(RunValue::DeferredHash, RunValue::Hash);
                hashes.push(&entry)?;
            }
        }
        if !locations.is_empty() {
            self.flush_runs(hashes, locations).await?;
        }
        // Each bucket contains only finite run descriptors; the full population remains spill-backed.
        for kind in [RunKind::Hashes, RunKind::Locations] {
            let mut mutations = BTreeMap::new();
            for partition in 0..=255u8 {
                let records = {
                    let db = self.spool.database()?;
                    self.spool.assert_scope(&db)?;
                    let mut statement=db.prepare(&format!("SELECT segment,content FROM {} WHERE kind=?1 AND partition=?2 ORDER BY minimum,content LIMIT 113",self.runs_table))?;
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
    async fn flush_runs(&self, hashes: RunWriter, locations: RunWriter) -> Result<()> {
        self.record_run(RunKind::Hashes, seal_run(&self.graph, hashes).await?)
            .await?;
        self.record_run(RunKind::Locations, seal_run(&self.graph, locations).await?)
            .await
    }
}
