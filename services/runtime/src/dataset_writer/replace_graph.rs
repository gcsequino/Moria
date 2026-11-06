//! Qualified whole-replacement graph preparation; this is distinct from sparse mutation.
use super::*;
use super::{
    graph_builder::GraphBuilder,
    visibility::{BootstrapBudget, VisibilityBootstrap, VisibleRow},
};
use crate::{
    authenticated_file::{AuthenticatedFileReader, QualifiedFile},
    dataset_graph::{NativeGraph, QualifiedNode},
    dataset_graph_read::{GraphReadContext, ReadBasis},
    native_snapshot_v3::{
        INDEX_SUMMARY_PROPERTY, IndexSummary, IndexSummaryRef, LogicalFile, NativeSnapshot,
        PROFILE, RootDescriptor, VisibilityIndex,
    },
    persistent_directory::{
        NodeReference,
        graph::{GraphDirectory, GraphValue},
    },
    snapshot::{parquet_bounds, parquet_read::BoundedParquetRead},
    storage::ObjectReadWindow,
};
use futures::TryStreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

pub(super) struct Basis {
    pub raw: Option<Value>,
    pub files: Option<NodeReference>,
    pub snapshot: Option<NativeSnapshot>,
}
pub(super) async fn basis(
    store: Arc<dyn ObjectStore>,
    grant: Arc<DatasetIoGrant>,
    schema: &Schema,
    cancel: Cancellation,
) -> Result<Basis> {
    let head=grant.database().get().await?.query_one("SELECT revision,metadata_location FROM moria.dataset_heads WHERE environment=$1 AND dataset=$2",&[&grant.session().environment(),&grant.resource()]).await?;
    ensure!(
        head.get::<_, i64>(0) == grant.expected_revision()?,
        "HEAD_CONFLICT"
    );
    if head.get::<_, Option<String>>(1).is_none() {
        ensure!(grant.expected_revision()? == 0, "INPUT_PIN_CHANGED");
        return Ok(Basis {
            raw: None,
            files: None,
            snapshot: None,
        });
    }
    // A legacy flat head needs its explicit, already qualified bootstrap; no hidden scan fallback.
    let (context, root) =
        GraphReadContext::execution(store, grant.clone(), ReadBasis::ExpectedHead, cancel).await?;
    let snapshot = NativeSnapshot::open(context, root).await?;
    ensure!(
        snapshot.metadata().current_schema().as_ref() == schema,
        "OUTPUT_SCHEMA_CHANGED"
    );
    let file = snapshot.metadata_file();
    let mut stream = snapshot.file(&file.name).await?.stream().await?;
    let mut bytes = Vec::new();
    while let Some(frame) = stream.next().await {
        let frame = frame?;
        ensure!(
            bytes.len() + frame.len() <= METADATA_BYTES,
            "RESOURCE_LIMIT"
        );
        bytes.extend_from_slice(&frame);
    }
    let mut raw: Value = serde_json::from_slice(&bytes)?;
    let previous = json!({"timestamp-ms":raw["last-updated-ms"],"metadata-file":file.name});
    raw.as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?
        .entry("metadata-log")
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?
        .push(previous);
    Ok(Basis {
        raw: Some(raw),
        files: Some(snapshot.files_reference().clone()),
        snapshot: Some(snapshot),
    })
}
pub(super) struct ReplacementGraph {
    pub graph: NativeGraph,
    builder: GraphBuilder,
    visible: Option<VisibilityBootstrap>,
    store: Arc<dyn ObjectStore>,
    grant: Arc<DatasetIoGrant>,
    cancel: Cancellation,
    schema: Arc<Schema>,
    columns: BTreeMap<String, Value>,
    keys: Vec<String>,
    rows: u64,
    files: u64,
}
impl ReplacementGraph {
    pub(super) async fn open(
        store: Arc<dyn ObjectStore>,
        grant: Arc<DatasetIoGrant>,
        schema: Arc<Schema>,
        spool: ClosureSpool,
        sequence: u64,
        prior: Option<NodeReference>,
        cancel: Cancellation,
    ) -> Result<Self> {
        let graph = NativeGraph::acquire(store.clone(), grant.clone(), cancel.clone()).await?;
        let runs_table = format!("replacement_runs_{}", Uuid::new_v4().simple());
        spool.database()?.execute_batch(&format!("CREATE TABLE {runs_table}(kind TEXT NOT NULL,partition INTEGER NOT NULL,minimum BLOB NOT NULL,segment TEXT NOT NULL,content TEXT NOT NULL,PRIMARY KEY(kind,partition,minimum,content)) WITHOUT ROWID;"))?;
        let keys = grant.primary_key()?;
        let visible = if keys.is_empty() {
            None
        } else {
            Some(
                VisibilityBootstrap::create(
                    spool.clone(),
                    grant.clone(),
                    BootstrapBudget {
                        maximum_data_rows: 9_007_199_254_740_991,
                        maximum_delete_rows: 0,
                    },
                )
                .await?,
            )
        };
        let builder = GraphBuilder {
            graph: graph.clone(),
            files: GraphDirectory::new(graph.clone(), vec!["files".into()])?,
            selections: GraphDirectory::new(graph.clone(), vec!["selections".into()])?,
            files_root: prior,
            selections_root: None,
            spool,
            runs_table,
            hashes: GraphDirectory::new(graph.clone(), vec!["hashes".into()])?,
            locations: GraphDirectory::new(graph.clone(), vec!["locations".into()])?,
            hashes_root: None,
            locations_root: None,
            sequence,
            key_hash: grant.key_schema_hash()?.into(),
        };
        let columns = columns(&serde_json::to_value(schema.as_ref())?)?;
        Ok(Self {
            graph,
            builder,
            visible,
            store,
            grant,
            cancel,
            schema,
            columns,
            keys,
            rows: 0,
            files: 0,
        })
    }
    pub(super) async fn task_root(&self, reference: &SealedObject) -> Result<PartRoot> {
        let task:Uuid=self.grant.database().get().await?.query_one("SELECT id FROM moria.runtime_execution_tasks WHERE execution=$1 AND status='sealed' AND result->'datasetOutputs'->$2=$3",&[&self.grant.lease().execution,&self.grant.resource(),&serde_json::to_value(reference)?]).await?.get(0);
        super::task_graph::root(&self.graph, task, reference).await
    }
    pub(super) async fn task_page(
        &self,
        root: &PartRoot,
        ordinal: u64,
        reference: &SealedObject,
    ) -> Result<PartPage> {
        super::task_graph::page(&self.graph, root, ordinal, reference).await
    }
    pub(super) async fn task_file(&mut self, root: &PartRoot, part: &Part) -> Result<()> {
        let certificate = super::task_graph::certificate(&self.graph, root, part).await?;
        self.data(part, certificate).await
    }
    async fn data(&mut self, part: &Part, certificate: QualifiedNode) -> Result<()> {
        self.builder
            .file_qualified(&part.name, part.content.clone(), certificate.clone())
            .await?;
        let selected = super::row_selection::write_all(
            &self.graph,
            LogicalFile {
                name: part.name.clone(),
                content: part.content.clone(),
            },
            certificate.clone(),
            part.rows,
            self.builder.sequence,
        )
        .await?;
        self.builder.selections_root = self
            .builder
            .selections
            .apply(
                self.builder.selections_root.as_ref(),
                BTreeMap::from([(
                    part.name.clone(),
                    Some(GraphValue::new(json!({"name":part.name}), vec![selected])?),
                )]),
            )
            .await?;
        let proof = self.graph.file_proof(&certificate).await?;
        let reader = Arc::new(AuthenticatedFileReader::from_qualified_file(
            self.store.clone(),
            QualifiedFile::from_producer_graph(proof)?,
        )?);
        let window = ObjectReadWindow::new(
            part.content.object.clone(),
            reader.clone(),
            self.grant.clone(),
            self.cancel.clone(),
        );
        let builder = ParquetRecordBatchStreamBuilder::new(BoundedParquetRead::new(
            window,
            part.content.size,
        ))
        .await?;
        ensure!(
            u64::try_from(builder.metadata().file_metadata().num_rows())? == part.rows,
            "PARQUET_COUNT_MISMATCH"
        );
        let chunks = parquet_bounds::metadata(builder.metadata(), part.content.size)?;
        let mut headers = ObjectReadWindow::new(
            part.content.object.clone(),
            reader,
            self.grant.clone(),
            self.cancel.clone(),
        );
        for chunk in chunks {
            let mut walk = parquet_bounds::PageWalk::new();
            while walk.position() < chunk.size {
                let offset = chunk.offset + walk.position();
                let end = (offset + 64 * 1024).min(chunk.offset + chunk.size);
                let bytes = headers.read(offset..end).await?;
                walk.header(&bytes, &chunk)?;
            }
            walk.finish(&chunk)?;
        }
        let mut stream = builder.with_batch_size(1000).build()?;
        let mut physical = 0u64;
        let column_json = serde_json::to_value(&self.columns)?;
        while let Some(batch) = stream.try_next().await? {
            self.graph.check_current().await?;
            ensure!(
                batch.get_array_memory_size() <= 64 * 1024 * 1024,
                "RESOURCE_LIMIT"
            );
            let batch = rebind_batch(batch, arrow_schema(self.schema.as_ref())?)?;
            validate_batch(&batch, &self.columns)?;
            if let Some(visible) = self.visible.as_mut() {
                let rows = super::rows::encode_rows(&batch, &self.keys, &column_json)?;
                let page = rows
                    .iter()
                    .enumerate()
                    .map(|(index, row)| VisibleRow {
                        key: row.key().into(),
                        file: part.name.clone(),
                        row: physical + index as u64,
                        data_sequence: self.builder.sequence,
                        row_hash: Some(row.row_hash()),
                    })
                    .collect::<Vec<_>>();
                visible.data_page(&page).await?;
            }
            physical = physical
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        }
        headers.check_current().await?;
        ensure!(physical == part.rows, "PARQUET_COUNT_MISMATCH");
        self.rows = self
            .rows
            .checked_add(physical)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        self.files += 1;
        Ok(())
    }
    pub(super) async fn metadata_file(&mut self, name: &str, content: &SealedObject) -> Result<()> {
        let proof = crate::authenticated_file::certify_new_file(
            self.store.clone(),
            self.grant.clone(),
            content.clone(),
            self.cancel.clone(),
        )
        .await?;
        let certificate = self.graph.register_certificate(&proof).await?;
        self.builder
            .file_qualified(name, content.clone(), certificate)
            .await?;
        Ok(())
    }
    pub(super) async fn summary(
        &mut self,
        metadata: &mut Value,
        snapshot: i64,
        total_rows: u64,
        total_files: u64,
    ) -> Result<IndexSummaryRef> {
        ensure!(
            self.rows == total_rows && self.files == total_files,
            "PARQUET_COUNT_MISMATCH"
        );
        if let Some(visible) = self.visible.as_mut() {
            ensure!(
                visible.seal().await? == total_rows,
                "PARQUET_COUNT_MISMATCH"
            );
            self.builder.index(visible).await?;
        }
        let summary = IndexSummary {
            version: 1,
            table_uuid: self.grant.table_uuid()?,
            snapshot_id: snapshot.to_string(),
            sequence: self.builder.sequence.to_string(),
            schema_hash: schema_hash(self.schema.as_ref())?,
            key_schema_hash: self.grant.key_schema_hash()?.into(),
            primary_key: self.keys.clone(),
            data_files: self.files,
            rows: self.rows,
            selections: self.builder.selections_root.clone(),
            visibility: self.visibility(),
        };
        let node = self
            .graph
            .write_descriptor(
                "root",
                &serde_json::to_value(summary)?,
                self.index_children().await?,
                self.rows,
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
        snapshot["summary"][INDEX_SUMMARY_PROPERTY] = Value::String(reference.encode()?);
        Ok(reference)
    }
    fn visibility(&self) -> Option<VisibilityIndex> {
        (!self.keys.is_empty()).then(|| VisibilityIndex {
            rows: self.rows,
            hashes: self.builder.hashes_root.clone(),
            locations: self.builder.locations_root.clone(),
        })
    }
    async fn index_children(&self) -> Result<Vec<(String, QualifiedNode)>> {
        let mut children = Vec::new();
        for (label, reference, directory) in [
            (
                "selections",
                &self.builder.selections_root,
                &self.builder.selections,
            ),
            ("hashes", &self.builder.hashes_root, &self.builder.hashes),
            (
                "locations",
                &self.builder.locations_root,
                &self.builder.locations,
            ),
        ] {
            if let Some(reference) = reference {
                children.push((label.into(), directory.qualified_root(reference).await?));
            }
        }
        Ok(children)
    }
    pub(super) async fn finish(
        self,
        metadata: SealedObject,
        metadata_name: String,
        snapshot: i64,
        index: IndexSummaryRef,
    ) -> Result<Uuid> {
        let files = self
            .builder
            .files_root
            .clone()
            .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_INVALID"))?;
        let descriptor = RootDescriptor {
            version: 3,
            reader_profile: PROFILE.into(),
            profile: self.graph.profile().clone(),
            table_uuid: self.grant.table_uuid()?,
            snapshot_id: snapshot.to_string(),
            sequence: self.builder.sequence.to_string(),
            schema_hash: schema_hash(self.schema.as_ref())?,
            key_schema_hash: self.grant.key_schema_hash()?.into(),
            primary_key: self.keys.clone(),
            data_files: self.files,
            rows: self.rows,
            selections: self.builder.selections_root.clone(),
            metadata: LogicalFile {
                name: metadata_name,
                content: metadata,
            },
            files: files.clone(),
            visibility: self.visibility(),
            index_summary: Some(index.clone()),
            mutation_footprint: None,
        };
        let mut children = self.index_children().await?;
        children.push((
            "files".into(),
            self.builder.files.qualified_root(&files).await?,
        ));
        let index_node = self
            .graph
            .known_node(&index.root)
            .await?
            .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_ANCESTRY_REQUIRED"))?;
        children.push(("index".into(), index_node));
        let node = self
            .graph
            .write_descriptor(
                "root",
                &serde_json::to_value(descriptor)?,
                children,
                self.rows,
                0,
            )
            .await?;
        let stage = Uuid::new_v4();
        self.graph.begin_stage(stage, &node).await?;
        while self.graph.advance(stage, 256).await?["ready"] != true {
            self.graph.check_current().await?;
        }
        let reference = crate::dataset_retention::register_graph_root(&self.graph, &node).await?;
        crate::catalog::prepare_graph_candidate(
            self.store,
            self.grant,
            &self.graph,
            stage,
            reference,
            &self.builder.spool,
            self.cancel,
        )
        .await
    }
}
