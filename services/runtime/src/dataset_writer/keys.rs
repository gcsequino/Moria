//! Exact declared-key validation using the already admitted private spill database.
use crate::{
    canonical::canonical_json,
    catalog::DatasetIoGrant,
    snapshot::{SnapshotProof, columns},
    storage::ClosureSpool,
    values,
};
use anyhow::{Result, ensure};
use futures::TryStreamExt;
use serde_json::{Value, json};
use uuid::Uuid;
const KEY_MAX: usize = 4 * 1024 * 1024;
const BATCH_MAX: usize = 8 * 1024 * 1024;
struct Keys {
    db: rusqlite::Connection,
    table: String,
}
impl Keys {
    fn new(spool: &ClosureSpool) -> Result<Self> {
        let db = spool.database()?;
        spool.assert_scope(&db)?;
        let table = format!("dataset_keys_{}", Uuid::new_v4().simple());
        db.execute_batch(&format!(
            "CREATE TABLE {table}(value BLOB PRIMARY KEY NOT NULL) WITHOUT ROWID"
        ))?;
        Ok(Self { db, table })
    }
    fn insert(&mut self, keys: &[Vec<u8>]) -> Result<()> {
        ensure!(
            keys.len() <= 1000 && keys.iter().map(Vec::len).sum::<usize>() <= BATCH_MAX,
            "RESOURCE_LIMIT"
        );
        let tx = self.db.transaction()?;
        {
            let mut insert =
                tx.prepare(&format!("INSERT INTO {}(value) VALUES(?1)", self.table))?;
            for key in keys {
                if let Err(error) = insert.execute([key]) {
                    if matches!(&error,rusqlite::Error::SqliteFailure(code,_) if code.code==rusqlite::ErrorCode::ConstraintViolation)
                    {
                        anyhow::bail!("DATASET_PRIMARY_KEY_DUPLICATE");
                    }
                    if matches!(&error,rusqlite::Error::SqliteFailure(code,_) if code.code==rusqlite::ErrorCode::DiskFull)
                    {
                        anyhow::bail!("RESOURCE_LIMIT");
                    }
                    return Err(error.into());
                }
            }
        }
        tx.commit()?;
        Ok(())
    }
}
impl Drop for Keys {
    fn drop(&mut self) {
        let _ = self
            .db
            .execute_batch(&format!("DROP TABLE IF EXISTS {}", self.table));
    }
}
pub(crate) fn encoded_key(
    batch: &arrow_array::RecordBatch,
    row: usize,
    keys: &[String],
    columns: &Value,
) -> Result<Vec<u8>> {
    let mut values = Vec::with_capacity(keys.len());
    for key in keys {
        let array = batch
            .column_by_name(key)
            .ok_or_else(|| anyhow::anyhow!("DATASET_PRIMARY_KEY_MISSING"))?;
        let mut value = values::arrow_value(array.as_ref(), row, &columns[key])?;
        ensure!(!value.is_null(), "DATASET_PRIMARY_KEY_NULL");
        if columns[key]["type"] == "float64" && value.as_f64() == Some(0.0) {
            value = json!(0.0);
        }
        values.push(value);
    }
    let bytes = canonical_json(&json!(values))?.into_bytes();
    ensure!(bytes.len() <= KEY_MAX, "DATASET_PRIMARY_KEY_LIMIT");
    Ok(bytes)
}
pub(crate) async fn validate_primary_keys(
    spool: &ClosureSpool,
    table: &iceberg::table::StaticTable,
    grant: &DatasetIoGrant,
    proof: &SnapshotProof,
) -> Result<Option<u64>> {
    let keys = grant.primary_key()?;
    if keys.is_empty() {
        return Ok(None);
    }
    ensure!(keys.len() <= 256, "DATASET_PRIMARY_KEY_INVALID");
    let columns = serde_json::to_value(columns(&serde_json::to_value(
        table.metadata().current_schema().as_ref(),
    )?)?)?;
    ensure!(
        keys.iter().all(|key| columns.get(key).is_some()),
        "DATASET_PRIMARY_KEY_MISSING"
    );
    spool.check_current(grant).await?;
    let mut index = Keys::new(spool)?;
    let scan = table
        .scan()
        .snapshot_id(proof.snapshot_id)
        .select(keys.iter().map(String::as_str))
        .build()?;
    let mut batches = scan.to_arrow().await?;
    let mut rows = 0u64;
    while let Some(batch) = batches.try_next().await? {
        spool.check_current(grant).await?;
        let mut page = Vec::new();
        let mut bytes = 0usize;
        for row in 0..batch.num_rows() {
            let key = encoded_key(&batch, row, &keys, &columns)?;
            if !page.is_empty() && (page.len() == 1000 || bytes + key.len() > BATCH_MAX) {
                index.insert(&page)?;
                page.clear();
                bytes = 0;
            }
            bytes += key.len();
            page.push(key);
            rows = rows
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        }
        if !page.is_empty() {
            index.insert(&page)?;
        }
    }
    ensure!(rows <= proof.physical_rows, "PARQUET_COUNT_MISMATCH");
    spool.check_current(grant).await?;
    Ok(Some(rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    #[test]
    fn complete_keys_are_typed_ordered_null_rejecting_and_unique_across_batches() -> Result<()> {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("sequence", arrow_schema::DataType::Int64, false),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::StringArray::from(vec![
                    Some("東京"),
                    Some("東京"),
                    None,
                ])),
                Arc::new(arrow_array::Int64Array::from(vec![7, 7, 9])),
            ],
        )?;
        let columns = json!({"id":{"type":"string","nullable":true},"sequence":{"type":"int64","nullable":false}});
        let key = encoded_key(&batch, 0, &["id".into(), "sequence".into()], &columns)?;
        assert_ne!(
            key,
            encoded_key(&batch, 0, &["sequence".into(), "id".into()], &columns)?
        );
        assert!(
            encoded_key(&batch, 2, &["id".into()], &columns)
                .unwrap_err()
                .to_string()
                .contains("DATASET_PRIMARY_KEY_NULL")
        );
        let db = rusqlite::Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE keys(value BLOB PRIMARY KEY NOT NULL) WITHOUT ROWID")?;
        let mut keys = Keys {
            db,
            table: "keys".into(),
        };
        keys.insert(&[key])?;
        let error = keys
            .insert(&[encoded_key(
                &batch,
                1,
                &["id".into(), "sequence".into()],
                &columns,
            )?])
            .unwrap_err();
        assert!(error.to_string().contains("DATASET_PRIMARY_KEY_DUPLICATE"));
        Ok(())
    }
    #[test]
    fn float_zero_keys_use_the_same_logical_identity() -> Result<()> {
        let batch = arrow_array::RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(arrow_array::Float64Array::from(vec![0.0, -0.0])) as arrow_array::ArrayRef,
        )])?;
        let columns = json!({"value":{"type":"float64","nullable":false}});
        assert_eq!(
            encoded_key(&batch, 0, &["value".into()], &columns)?,
            encoded_key(&batch, 1, &["value".into()], &columns)?
        );
        Ok(())
    }
}
