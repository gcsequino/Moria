//! Reader dispatch and task plans derive from the actual accepted indexing build.
use super::*;
use crate::{
    dataset_graph_read::GraphReadContext,
    native_snapshot_v3::{GraphScanFile, NativeSnapshot, PROFILE},
    storage::{
        Cancellation, IndexNamespace, IndexNamespaceReadAuthority, ReadAuthority,
        configured_reader_with_fingerprint,
    },
};
use futures::{StreamExt, stream::BoxStream};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub(super) enum Source {
    Table(StaticTable),
    Graph(Arc<NativeSnapshot>),
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphPlan {
    kind: String,
    version: u32,
    reader_profile: String,
    file: Option<GraphScanFile>,
}
impl GraphPlan {
    fn new(file: Option<GraphScanFile>) -> Self {
        Self {
            kind: "scan".into(),
            version: 1,
            reader_profile: PROFILE.into(),
            file,
        }
    }
    fn decode(plan: &Value) -> Result<Self> {
        ensure!(plan.get("file").is_some(), "INVALID_SCAN_PROTOCOL");
        let plan: Self = serde_json::from_value(plan.clone())?;
        ensure!(
            plan.kind == "scan" && plan.version == 1 && plan.reader_profile == PROFILE,
            "INVALID_SCAN_PROTOCOL"
        );
        Ok(plan)
    }
}
impl Source {
    pub(super) fn schema(&self) -> Arc<iceberg::spec::Schema> {
        match self {
            Self::Table(t) => t.metadata().current_schema().clone(),
            Self::Graph(g) => g.metadata().current_schema().clone(),
        }
    }
    pub(super) async fn plans(
        &self,
        snapshot: i64,
        layout: &SourceLayout,
    ) -> Result<BoxStream<'static, Result<(Value, Option<Vec<u8>>)>>> {
        match self {
            Self::Table(table) => {
                let scan = table
                    .scan()
                    .snapshot_id(snapshot)
                    .select(layout.columns.keys().map(String::as_str))
                    .build()
                    .context("INDEXING_PLAN_SCAN")?;
                let tasks = scan.plan_files().await.context("INDEXING_PLAN_FILES")?;
                Ok(Box::pin(tasks.map(|task| {
                    let mut task = task?;
                    ensure!(
                        task.deletes.is_empty(),
                        "Iceberg delete files are unsupported by this reader release"
                    );
                    ensure!(
                        task.partition.as_ref().is_none_or(|p| p.iter().len() == 0)
                            && task
                                .partition_spec
                                .as_ref()
                                .is_none_or(|p| p.is_unpartitioned())
                            && task.name_mapping.is_none(),
                        "Unsupported Iceberg partition/name mapping"
                    );
                    task.partition = None;
                    task.partition_spec = None;
                    Ok((
                        json!({"kind":"scan"}),
                        Some(crate::wire::scan_plan(task).context("INDEXING_PLAN_WIRE")?),
                    ))
                })))
            }
            Self::Graph(graph) => {
                ensure!(graph.snapshot_id()? == snapshot, "SNAPSHOT_PIN_MISMATCH");
                if graph.data_file_count() == 0 {
                    ensure!(graph.row_count() == 0, "PARQUET_COUNT_MISMATCH");
                    graph.check_current().await?;
                    return Ok(Box::pin(stream::once(async {
                        Ok((serde_json::to_value(GraphPlan::new(None))?, None))
                    })));
                }
                let plans = graph.clone().scan_plans().await?;
                Ok(Box::pin(plans.map(|plan| {
                    Ok((serde_json::to_value(GraphPlan::new(Some(plan?)))?, None))
                })))
            }
        }
    }
    pub(super) async fn batches(
        self,
        plan: &Value,
        wire: Option<&[u8]>,
    ) -> Result<(
        Option<u64>,
        BoxStream<'static, Result<arrow_array::RecordBatch>>,
    )> {
        match self {
            Self::Table(table) => {
                ensure!(plan == &json!({"kind":"scan"}), "INVALID_SCAN_PROTOCOL");
                let scan: FileScanTask = crate::wire::read_scan(
                    wire.ok_or_else(|| anyhow::anyhow!("Missing scan protocol"))?,
                )?;
                ensure!(
                    scan.deletes.is_empty()
                        && scan.partition.is_none()
                        && scan.name_mapping.is_none(),
                    "Unsupported persisted task"
                );
                let expected = scan.record_count;
                let batches = table
                    .reader_builder()
                    .with_batch_size(1000)
                    .with_data_file_concurrency_limit(1)
                    .with_range_fetch_concurrency(1)
                    .build()
                    .read(Box::pin(stream::iter([Ok(scan)])))?
                    .stream()
                    .map_err(anyhow::Error::from);
                Ok((expected, Box::pin(batches)))
            }
            Self::Graph(graph) => {
                ensure!(wire.is_none(), "INVALID_SCAN_PROTOCOL");
                let plan = GraphPlan::decode(plan)?;
                if let Some(file) = plan.file {
                    let expected = file.selected_rows;
                    Ok((Some(expected), graph.scan_planned_file(&file, 1000).await?))
                } else {
                    graph.check_current().await?;
                    ensure!(
                        graph.data_file_count() == 0 && graph.row_count() == 0,
                        "INVALID_SCAN_PROTOCOL"
                    );
                    Ok((Some(0), Box::pin(stream::empty())))
                }
            }
        }
    }
}
impl Worker {
    pub(super) async fn source(&self, row: &Row, scope: Scope) -> Result<Source> {
        let metadata_location: String = row.get("metadata_location");
        if !metadata_location.starts_with("moria-snapshot-v3:") {
            return Ok(Source::Table(self.table(row, scope).await?));
        }
        let definition: Value = row.get("definition");
        ensure!(
            definition["version"] == 1 && definition["valueVersion"] == 1,
            "Unsupported indexing version"
        );
        let reference = crate::catalog::NativeSnapshotRef::decode(&metadata_location)?;
        let binding=self.db.get().await?.query_one("SELECT n.revision,n.iceberg_uuid FROM moria.native_storage_namespaces n JOIN moria.indexing_builds b ON b.id=$1 WHERE n.environment=b.environment AND n.resource=b.source AND n.purpose='dataset-source' AND n.enabled AND n.warehouse_id=$2 AND n.table_id=$3",&[&scope.build,&reference.root.object.warehouse_id,&reference.root.object.table_id]).await?;
        let uuid: Option<Uuid> = binding.get("iceberg_uuid");
        let uuid = uuid.ok_or_else(|| anyhow::anyhow!("INVALID_DATASET_BINDING"))?;
        let namespace = IndexNamespace {
            warehouse_id: reference.root.object.warehouse_id.clone(),
            table_id: reference.root.object.table_id.clone(),
            revision: binding.get("revision"),
        };
        IndexNamespaceReadAuthority {
            scope: scope.clone(),
            namespace: namespace.clone(),
        }
        .check_read(&reference.root.object)
        .await?;
        let (reader, _) = configured_reader_with_fingerprint(&namespace)?;
        let managed:Option<Value>=self.db.get().await?.query_one("SELECT moria.runtime_dataset_binding($1,moria.runtime_data_resource($1,$2),$3,$4,$5)",&[&row.get::<_,String>("environment"),&row.get::<_,String>("source"),&namespace.warehouse_id,&namespace.table_id,&namespace.revision]).await?.get(0);
        let managed = managed.ok_or_else(|| anyhow::anyhow!("DATASET_BINDING_INVALID"))?;
        ensure!(
            managed["physicalBindingFingerprint"].as_str()
                == Some(reader.physical_fingerprint()?.as_str()),
            "DATASET_BINDING_INVALID"
        );
        let (context, root) =
            GraphReadContext::indexing(reader, scope, namespace, Cancellation::default()).await?;
        ensure!(
            context.catalog_reference()? == reference,
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let graph = Arc::new(NativeSnapshot::open(context, root).await?);
        let expected: iceberg::spec::Schema =
            serde_json::from_value(managed["icebergSchema"].clone())?;
        ensure!(
            graph.metadata().uuid() == uuid
                && graph.snapshot_id()? == row.get::<_, String>("snapshot_id").parse::<i64>()?
                && &expected == graph.metadata().current_schema().as_ref(),
            "DATASET_BINDING_INVALID"
        );
        Ok(Source::Graph(graph))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn graph_plan_requires_explicit_versioned_profile_and_empty_marker() {
        let plan = serde_json::to_value(GraphPlan::new(None)).unwrap();
        assert!(GraphPlan::decode(&plan).unwrap().file.is_none());
        for (field, value) in [
            ("kind", json!("reduce")),
            ("version", json!(2)),
            ("readerProfile", json!("iceberg-v2-cow-flat-bounded-v2")),
            ("unexpected", json!(true)),
        ] {
            let mut changed = plan.clone();
            changed[field] = value;
            assert!(GraphPlan::decode(&changed).is_err());
        }
        let mut missing = plan;
        missing.as_object_mut().unwrap().remove("file");
        assert!(GraphPlan::decode(&missing).is_err());
    }
}
