//! Actual sealed sparse input parts, traversed one bounded page/file at a time.
use super::{Part, PartRoot, WriteContract, compatible_type, task_graph};
use crate::{
    catalog::DatasetIoGrant,
    dataset_graph::{NativeGraph, QualifiedNode},
    snapshot::{parquet_bounds, parquet_read::BoundedParquetRead},
    storage::SealedObject,
};
use anyhow::{Result, ensure};
use arrow_array::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use uuid::Uuid;

const MAX_TASKS: usize = 4096;
struct Task {
    id: Uuid,
    owner: Uuid,
    fence: i64,
    reference: SealedObject,
}
struct Cursor {
    root: PartRoot,
    ordinal: u64,
    next_page: Option<SealedObject>,
    parts: std::vec::IntoIter<Part>,
    seen_parts: u64,
    seen_rows: u64,
}
pub(super) struct Inputs {
    graph: NativeGraph,
    contract: WriteContract,
    execution: Uuid,
    incarnation: Uuid,
    resource: String,
    tasks: std::vec::IntoIter<Task>,
    current: Option<Cursor>,
    receipts: Vec<(String, Value)>,
}
impl Inputs {
    pub(super) async fn open(
        graph: NativeGraph,
        grant: &DatasetIoGrant,
        contract: WriteContract,
        partsets: &[SealedObject],
    ) -> Result<Self> {
        graph.check_current().await?;
        ensure!(
            grant.lease().task.is_none()
                && !partsets.is_empty()
                && partsets.len() <= MAX_TASKS
                && matches!(contract.mode.as_str(), "append" | "upsert" | "delete"),
            "OUTPUT_INVALID"
        );
        let mut supplied = BTreeSet::new();
        for reference in partsets {
            ensure!(
                supplied.insert(serde_json::to_string(reference)?),
                "OUTPUT_INVALID"
            );
        }
        let client = grant.database().get().await?;
        let accepted = client
            .query(
                "SELECT id,owner,fence,status,result->'datasetOutputs'->($2::text) AS output
             FROM moria.runtime_execution_tasks WHERE execution=$1 ORDER BY id LIMIT 4097",
                &[&grant.lease().execution, &grant.resource()],
            )
            .await?;
        drop(client);
        ensure!(
            accepted.len() <= MAX_TASKS && accepted.len() == partsets.len(),
            "OUTPUT_ATTEMPT_CHANGED"
        );
        let mut tasks = Vec::with_capacity(accepted.len());
        for row in accepted {
            ensure!(
                row.get::<_, String>("status") == "sealed",
                "OUTPUT_ATTEMPT_CHANGED"
            );
            let reference: SealedObject = serde_json::from_value(
                row.get::<_, Option<Value>>("output")
                    .ok_or_else(|| anyhow::anyhow!("OUTPUT_ATTEMPT_CHANGED"))?,
            )?;
            ensure!(
                supplied.remove(&serde_json::to_string(&reference)?),
                "OUTPUT_ATTEMPT_CHANGED"
            );
            tasks.push(Task {
                id: row.get("id"),
                owner: row.try_get("owner")?,
                fence: row.try_get("fence")?,
                reference,
            });
        }
        ensure!(supplied.is_empty(), "OUTPUT_ATTEMPT_CHANGED");
        graph.check_current().await?;
        Ok(Self {
            graph,
            contract,
            execution: grant.lease().execution,
            incarnation: grant.lease().incarnation,
            resource: grant.resource().into(),
            tasks: tasks.into_iter(),
            current: None,
            receipts: vec![],
        })
    }
    pub(super) async fn next_part(&mut self) -> Result<Option<(Part, QualifiedNode)>> {
        self.graph.check_current().await?;
        loop {
            if let Some(cursor) = self.current.as_mut() {
                if let Some(part) = cursor.parts.next() {
                    ensure!(
                        part.rows > 0 && part.content.size <= super::PART_ENCODED_BYTES as u64,
                        "OUTPUT_INVALID"
                    );
                    cursor.seen_parts = cursor
                        .seen_parts
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
                    cursor.seen_rows = cursor
                        .seen_rows
                        .checked_add(part.rows)
                        .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
                    ensure!(
                        cursor.seen_parts <= cursor.root.parts
                            && cursor.seen_rows <= cursor.root.rows,
                        "OUTPUT_INVALID"
                    );
                    let certificate =
                        task_graph::certificate(&self.graph, &cursor.root, &part).await?;
                    return Ok(Some((part, certificate)));
                }
                if cursor.ordinal > 0 {
                    cursor.ordinal -= 1;
                    let reference = cursor
                        .next_page
                        .take()
                        .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?;
                    let page =
                        task_graph::page(&self.graph, &cursor.root, cursor.ordinal, &reference)
                            .await?;
                    ensure!(
                        page.version == 1
                            && page.ordinal == cursor.ordinal
                            && !page.parts.is_empty()
                            && page.parts.len() <= 128,
                        "OUTPUT_INVALID"
                    );
                    cursor.next_page = page.previous;
                    cursor.parts = page.parts.into_iter();
                    continue;
                }
                ensure!(
                    cursor.next_page.is_none()
                        && cursor.seen_parts == cursor.root.parts
                        && cursor.seen_rows == cursor.root.rows,
                    "OUTPUT_INVALID"
                );
                self.current = None;
            }
            let Some(task) = self.tasks.next() else {
                self.graph.check_current().await?;
                return Ok(None);
            };
            let root = task_graph::root(&self.graph, task.id, &task.reference).await?;
            ensure!(
                root.execution == self.execution
                    && root.incarnation == self.incarnation
                    && root.resource == self.resource
                    && root.owner == task.owner
                    && root.fence == task.fence.to_string()
                    && root.fence.parse::<i64>()? > 0
                    && root.schema_hash == self.contract.schema_hash
                    && root.contract.as_ref() == Some(&self.contract)
                    && root.pages <= 8192
                    && root.parts <= 1_048_576
                    && root.rows <= 9_007_199_254_740_991,
                "OUTPUT_ATTEMPT_CHANGED"
            );
            self.receipts.push((
                task.id.to_string(),
                json!({
                    "task":task.id,"owner":task.owner,"fence":root.fence,
                    "incarnation":root.incarnation,"partset":task.reference,"rows":root.rows,
                }),
            ));
            self.current = Some(Cursor {
                ordinal: root.pages,
                next_page: root.last_page.clone(),
                root,
                parts: vec![].into_iter(),
                seen_parts: 0,
                seen_rows: 0,
            });
        }
    }
    pub(super) fn finish(self) -> Result<Vec<(String, Value)>> {
        ensure!(
            self.current.is_none() && self.tasks.len() == 0,
            "OUTPUT_INVALID"
        );
        Ok(self.receipts)
    }
}

pub(super) async fn batches(
    graph: &NativeGraph,
    part: &Part,
    certificate: &QualifiedNode,
    input_schema: &iceberg::spec::Schema,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let (content, window) = graph.file_window(certificate).await?;
    ensure!(content == part.content, "DATASET_GRAPH_ANCESTRY_REQUIRED");
    let builder =
        ParquetRecordBatchStreamBuilder::new(BoundedParquetRead::new(window, content.size)).await?;
    ensure!(
        u64::try_from(builder.metadata().file_metadata().num_rows())? == part.rows,
        "PARQUET_COUNT_MISMATCH"
    );
    let expected = super::arrow_schema(input_schema)?;
    ensure!(
        builder.schema().fields().len() == expected.fields().len()
            && builder
                .schema()
                .fields()
                .iter()
                .zip(expected.fields())
                .all(|(actual, expected)| {
                    actual.name() == expected.name()
                        && compatible_type(actual.data_type(), expected.data_type())
                }),
        "SCHEMA_CHANGED"
    );
    let chunks = parquet_bounds::metadata(builder.metadata(), content.size)?;
    let (_, mut headers) = graph.file_window(certificate).await?;
    for chunk in chunks {
        let mut walk = parquet_bounds::PageWalk::new();
        while walk.position() < chunk.size {
            let offset = chunk.offset + walk.position();
            let bytes = headers
                .read(offset..(offset + 64 * 1024).min(chunk.offset + chunk.size))
                .await?;
            walk.header(&bytes, &chunk)?;
        }
        walk.finish(&chunk)?;
    }
    headers.check_current().await?;
    Ok(builder
        .with_batch_size(128)
        .build()?
        .map(|batch| batch.map_err(Into::into))
        .boxed())
}
