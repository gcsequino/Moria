//! Whole-root and planned-file scans retain physical ordinals before qualified visibility selection.
use super::*;
use crate::dataset_writer::row_selection::ReadSelection;
use futures::{TryStreamExt, stream};

/// A task witness, never read authority. Tasks re-resolve it from their actual pinned root.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GraphScanFile {
    pub name: String,
    pub content: SealedObject,
    pub physical_rows: u64,
    pub selected_rows: u64,
    pub data_sequence: u64,
}
impl GraphScanFile {
    fn from_selection(s: &ReadSelection) -> Self {
        Self {
            name: s.name().into(),
            content: s.content().clone(),
            physical_rows: s.physical_rows(),
            selected_rows: s.selected_rows(),
            data_sequence: s.data_sequence(),
        }
    }
    fn verify(&self, s: &ReadSelection) -> Result<()> {
        ensure!(
            self.name == s.name()
                && &self.content == s.content()
                && self.physical_rows == s.physical_rows()
                && self.selected_rows == s.selected_rows()
                && self.data_sequence == s.data_sequence(),
            "DATASET_SCAN_PLAN_CHANGED"
        );
        Ok(())
    }
}
async fn selections(
    snapshot: Arc<NativeSnapshot>,
) -> Result<BoxStream<'static, Result<ReadSelection>>> {
    snapshot.check_current().await?;
    let Some(directory) = snapshot.index_directory("selections").await? else {
        ensure!(
            snapshot.descriptor.data_files == 0 && snapshot.descriptor.rows == 0,
            "DATASET_KEY_INDEX_INVALID"
        );
        return Ok(Box::pin(stream::empty()));
    };
    Ok(Box::pin(directory.entries().then(move |entry| {
        let snapshot = snapshot.clone();
        async move {
            let (name, entry) = entry?;
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Name {
                name: String,
            }
            let named: Name = serde_json::from_value(entry.value.clone())?;
            ensure!(
                named.name == name && entry.link_count() == 1,
                "DATASET_KEY_INDEX_INVALID"
            );
            let selection =
                ReadSelection::open(snapshot.context.clone(), entry.link(0).await?).await?;
            ensure!(
                selection.name() == name
                    && snapshot.file(&name).await?.content() == selection.content(),
                "DATASET_GRAPH_ANCESTRY_REQUIRED"
            );
            snapshot.check_current().await?;
            Ok(selection)
        }
    })))
}
pub(super) async fn plans(
    snapshot: Arc<NativeSnapshot>,
) -> Result<BoxStream<'static, Result<GraphScanFile>>> {
    struct State {
        snapshot: Arc<NativeSnapshot>,
        selections: BoxStream<'static, Result<ReadSelection>>,
        files: u64,
        rows: u64,
    }
    let state = State {
        selections: selections(snapshot.clone()).await?,
        snapshot,
        files: 0,
        rows: 0,
    };
    Ok(Box::pin(stream::try_unfold(
        state,
        |mut state| async move {
            state.snapshot.check_current().await?;
            let Some(selection) = state.selections.try_next().await? else {
                ensure!(
                    state.files == state.snapshot.descriptor.data_files
                        && state.rows == state.snapshot.descriptor.rows,
                    "PARQUET_COUNT_MISMATCH"
                );
                return Ok(None);
            };
            state.files = state
                .files
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
            state.rows = state
                .rows
                .checked_add(selection.selected_rows())
                .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
            Ok(Some((GraphScanFile::from_selection(&selection), state)))
        },
    )))
}
pub(super) async fn all(
    snapshot: Arc<NativeSnapshot>,
    batch_size: usize,
) -> Result<BoxStream<'static, Result<arrow_array::RecordBatch>>> {
    let files = snapshot.descriptor.data_files;
    let rows = snapshot.descriptor.rows;
    let selected = selections(snapshot.clone()).await?;
    batches(snapshot, selected, files, rows, batch_size).await
}
pub(super) async fn one(
    snapshot: Arc<NativeSnapshot>,
    plan: &GraphScanFile,
    batch_size: usize,
) -> Result<BoxStream<'static, Result<arrow_array::RecordBatch>>> {
    let selection = snapshot.selection(&plan.name).await?;
    plan.verify(&selection)?;
    let expected = selection.selected_rows();
    batches(
        snapshot,
        Box::pin(stream::once(async move { Ok(selection) })),
        1,
        expected,
        batch_size,
    )
    .await
}
async fn batches(
    snapshot: Arc<NativeSnapshot>,
    selections: BoxStream<'static, Result<ReadSelection>>,
    expected_files: u64,
    expected_rows: u64,
    batch_size: usize,
) -> Result<BoxStream<'static, Result<arrow_array::RecordBatch>>> {
    ensure!((1..=1000).contains(&batch_size), "RESOURCE_LIMIT");
    let table = Arc::new(
        iceberg::table::StaticTable::from_metadata(
            (*snapshot.metadata).clone(),
            iceberg::TableIdent::from_strs(["moria", "qualified"])?,
            snapshot.file_io(),
        )
        .await?,
    );
    struct State {
        snapshot: Arc<NativeSnapshot>,
        table: Arc<iceberg::table::StaticTable>,
        files: BoxStream<'static, Result<ReadSelection>>,
        batches: Option<BoxStream<'static, Result<arrow_array::RecordBatch>>>,
        selection: Option<ReadSelection>,
        physical: u64,
        file_visible: u64,
        total: u64,
        files_seen: u64,
        expected_files: u64,
        expected_rows: u64,
        batch_size: usize,
        columns: std::collections::BTreeMap<String, Value>,
    }
    let columns = crate::snapshot::columns(&serde_json::to_value(
        snapshot.metadata.current_schema().as_ref(),
    )?)?;
    let state = State {
        snapshot,
        table,
        files: selections,
        batches: None,
        selection: None,
        physical: 0,
        file_visible: 0,
        total: 0,
        files_seen: 0,
        expected_files,
        expected_rows,
        batch_size,
        columns,
    };
    Ok(Box::pin(stream::try_unfold(
        state,
        |mut state| async move {
            loop {
                state.snapshot.check_current().await?;
                if let Some(batches) = state.batches.as_mut() {
                    if let Some(batch) = batches.try_next().await? {
                        let selection = state.selection.as_ref().unwrap();
                        let selected = selection.selected(state.physical, batch.num_rows()).await?;
                        state.physical = state
                            .physical
                            .checked_add(batch.num_rows() as u64)
                            .ok_or_else(|| anyhow::anyhow!("PARQUET_COUNT_MISMATCH"))?;
                        let indices = selected
                            .iter()
                            .enumerate()
                            .filter_map(|(i, selected)| selected.then_some(i as u32))
                            .collect::<Vec<_>>();
                        state.file_visible += indices.len() as u64;
                        state.total += indices.len() as u64;
                        if indices.is_empty() {
                            continue;
                        }
                        let batch = if indices.len() == batch.num_rows() {
                            batch
                        } else {
                            let indices = arrow_array::UInt32Array::from(indices);
                            let arrays = batch
                                .columns()
                                .iter()
                                .map(|a| arrow_select::take::take(a.as_ref(), &indices, None))
                                .collect::<std::result::Result<Vec<_>, _>>()?;
                            arrow_array::RecordBatch::try_new(batch.schema(), arrays)?
                        };
                        ensure!(batch.num_columns() == state.columns.len(), "SCHEMA_CHANGED");
                        crate::snapshot::validate_batch(&batch, &state.columns)?;
                        state.snapshot.check_current().await?;
                        return Ok(Some((batch, state)));
                    }
                    let selection = state.selection.as_ref().unwrap();
                    ensure!(
                        state.physical == selection.physical_rows()
                            && state.file_visible == selection.selected_rows(),
                        "PARQUET_COUNT_MISMATCH"
                    );
                    state.batches = None;
                    state.selection = None;
                }
                let Some(selection) = state.files.try_next().await? else {
                    ensure!(
                        state.total == state.expected_rows
                            && state.files_seen == state.expected_files,
                        "PARQUET_COUNT_MISMATCH"
                    );
                    state.snapshot.check_current().await?;
                    return Ok(None);
                };
                state.files_seen += 1;
                if selection.selected_rows() == 0 {
                    continue;
                }
                let content = selection.content();
                let task = iceberg::scan::FileScanTask::builder()
                    .with_file_size_in_bytes(content.size)
                    .with_start(0)
                    .with_length(content.size)
                    .with_record_count(Some(selection.physical_rows()))
                    .with_data_file_path(selection.name().to_owned())
                    .with_data_file_format(iceberg::spec::DataFileFormat::Parquet)
                    .with_schema(state.snapshot.metadata.current_schema().clone())
                    .with_project_field_ids(
                        state
                            .snapshot
                            .metadata
                            .current_schema()
                            .as_struct()
                            .fields()
                            .iter()
                            .map(|f| f.id)
                            .collect(),
                    )
                    .with_case_sensitive(true)
                    .build();
                let batches = state
                    .table
                    .reader_builder()
                    .with_batch_size(state.batch_size)
                    .with_data_file_concurrency_limit(1)
                    .with_range_fetch_concurrency(1)
                    .build()
                    .read(Box::pin(stream::iter([Ok(task)])))?
                    .stream()
                    .map_err(anyhow::Error::from);
                state.batches = Some(Box::pin(batches));
                state.selection = Some(selection);
                state.physical = 0;
                state.file_visible = 0;
            }
        },
    )))
}
