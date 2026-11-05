//! Canonical full-row and ordered-key identity is computed from actual typed Arrow values.
use crate::{canonical::canonical_json, snapshot::validate_batch, values};
use anyhow::{Result, ensure};
use arrow_array::RecordBatch;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
const PAGE_ROWS: usize = 1000;
const PAGE_BYTES: usize = 8 * 1024 * 1024;
const ROW_BYTES: usize = 4 * 1024 * 1024;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalRow {
    key: Vec<u8>,
    hash: [u8; 32],
    bytes: u64,
}
impl CanonicalRow {
    pub fn key(&self) -> &[u8] {
        &self.key
    }
    pub fn row_hash(&self) -> [u8; 32] {
        self.hash
    }
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}
/// Full target schema and complete ordered PK are mandatory; no column normalization or omission.
pub(crate) fn encode_rows(
    batch: &RecordBatch,
    keys: &[String],
    columns: &Value,
) -> Result<Vec<CanonicalRow>> {
    ensure!(
        batch.num_rows() <= PAGE_ROWS && !keys.is_empty() && keys.len() <= 256,
        "RESOURCE_LIMIT"
    );
    let descriptors = columns
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("SCHEMA_CHANGED"))?
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    validate_batch(batch, &descriptors)?;
    ensure!(
        keys.iter().all(|key| descriptors.contains_key(key)),
        "DATASET_PRIMARY_KEY_MISSING"
    );
    let mut bytes = 0usize;
    let mut output = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let canonical = canonical_payload(batch, row, &descriptors)?;
        let key = super::keys::encoded_key(batch, row, keys, columns)?;
        bytes = bytes
            .checked_add(key.len() + 32)
            .filter(|bytes| *bytes <= PAGE_BYTES)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        output.push(CanonicalRow {
            key,
            hash: Sha256::digest(&canonical).into(),
            bytes: canonical.len() as u64,
        });
    }
    Ok(output)
}
fn canonical_payload(
    batch: &RecordBatch,
    row: usize,
    descriptors: &BTreeMap<String, Value>,
) -> Result<Vec<u8>> {
    let mut value = serde_json::Map::new();
    for (name, descriptor) in descriptors {
        let array = batch
            .column_by_name(name)
            .ok_or_else(|| anyhow::anyhow!("SCHEMA_CHANGED"))?;
        value.insert(
            name.clone(),
            values::arrow_value(array.as_ref(), row, descriptor)?,
        );
    }
    let canonical = canonical_json(&Value::Object(value))?.into_bytes();
    ensure!(canonical.len() <= ROW_BYTES, "DECODED_ROW_LIMIT");
    Ok(canonical)
}
/// Count actual logical rows for an unkeyed append without manufacturing row identity.
pub(crate) fn payload_bytes(batch: &RecordBatch, columns: &BTreeMap<String, Value>) -> Result<u64> {
    ensure!(batch.num_rows() <= PAGE_ROWS, "RESOURCE_LIMIT");
    validate_batch(batch, columns)?;
    let mut bytes = 0u64;
    for row in 0..batch.num_rows() {
        bytes = bytes
            .checked_add(canonical_payload(batch, row, columns)?.len() as u64)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
    }
    Ok(bytes)
}
/// Bootstrap validates only projected key columns; it does not invent a digest for unread payloads.
pub(crate) fn encode_projected_keys(
    batch: &RecordBatch,
    keys: &[String],
    columns: &Value,
) -> Result<Vec<Vec<u8>>> {
    ensure!(
        batch.num_rows() <= PAGE_ROWS && !keys.is_empty() && keys.len() <= 256,
        "RESOURCE_LIMIT"
    );
    ensure!(batch.num_columns() == keys.len(), "SCHEMA_CHANGED");
    let mut descriptors = BTreeMap::new();
    for key in keys {
        let descriptor = columns
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("DATASET_PRIMARY_KEY_MISSING"))?;
        ensure!(
            descriptor["nullable"] == false
                && descriptors
                    .insert(key.clone(), descriptor.clone())
                    .is_none(),
            "DATASET_PRIMARY_KEY_INVALID"
        );
    }
    validate_batch(batch, &descriptors)?;
    let mut bytes = 0usize;
    let mut output = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let key = super::keys::encoded_key(batch, row, keys, columns)?;
        bytes = bytes
            .checked_add(key.len())
            .filter(|bytes| *bytes <= PAGE_BYTES)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        output.push(key);
    }
    Ok(output)
}
/// Delete inputs are key-only in the accepted PK order; full-row deletes cannot silently discard fields.
pub(crate) fn encode_delete_keys(
    batch: &RecordBatch,
    keys: &[String],
    columns: &Value,
) -> Result<Vec<Vec<u8>>> {
    ensure!(
        batch.num_rows() <= PAGE_ROWS && !keys.is_empty() && keys.len() <= 256,
        "RESOURCE_LIMIT"
    );
    ensure!(
        batch.num_columns() == keys.len()
            && batch
                .schema()
                .fields()
                .iter()
                .zip(keys)
                .all(|(field, key)| field.name() == key),
        "DATASET_WRITE_MODE_INVALID"
    );
    let mut bytes = 0usize;
    let mut output = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let key = super::keys::encoded_key(batch, row, keys, columns)?;
        bytes = bytes
            .checked_add(key.len())
            .filter(|bytes| *bytes <= PAGE_BYTES)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        output.push(key);
    }
    Ok(output)
}
pub(super) enum MutationRows {
    Complete(Vec<CanonicalRow>),
    Keys(Vec<Vec<u8>>),
    Unkeyed(u64),
}
/// Split only the codec's actual key-byte page overflow, before any mutation IO.
pub(super) fn mutation_page(
    batch: &RecordBatch,
    start: usize,
    keys: &[String],
    columns: &BTreeMap<String, Value>,
    column_json: &Value,
    key_only: bool,
) -> Result<(RecordBatch, MutationRows)> {
    ensure!(
        start < batch.num_rows() && batch.num_rows() <= PAGE_ROWS,
        "RESOURCE_LIMIT"
    );
    let mut count = batch.num_rows() - start;
    loop {
        let page = batch.slice(start, count);
        let encoded = if key_only {
            encode_delete_keys(&page, keys, column_json).map(MutationRows::Keys)
        } else if keys.is_empty() {
            payload_bytes(&page, columns).map(MutationRows::Unkeyed)
        } else {
            encode_rows(&page, keys, column_json).map(MutationRows::Complete)
        };
        match encoded {
            Ok(encoded) => return Ok((page, encoded)),
            Err(error) if count > 1 && error.to_string() == "RESOURCE_LIMIT" => count /= 2,
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;
    fn batch(arrays: &[Option<Vec<&str>>]) -> RecordBatch {
        let mut builder =
            arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new())
                .with_field(Arc::new(arrow_schema::Field::new(
                    "item",
                    arrow_schema::DataType::Utf8,
                    false,
                )));
        for value in arrays {
            if let Some(items) = value {
                for item in items {
                    builder.values().append_value(item);
                }
                builder.append(true);
            } else {
                builder.append(false);
            }
        }
        RecordBatch::try_from_iter(vec![(
            "parts",
            Arc::new(builder.finish()) as arrow_array::ArrayRef,
        )])
        .unwrap()
    }
    #[test]
    fn array_keys_preserve_order_duplicates_and_actual_null_rejection() {
        let columns = json!({"parts":{"type":"array","nullable":false,"items":{"type":"string","nullable":false}}});
        let keys = vec!["parts".into()];
        let rows = encode_rows(
            &batch(&[
                Some(vec!["東京", "😀"]),
                Some(vec!["😀", "東京"]),
                Some(vec!["東京", "😀"]),
                Some(vec![]),
            ]),
            &keys,
            &columns,
        )
        .unwrap();
        let projected =
            encode_projected_keys(&batch(&[Some(vec!["東京", "😀"])]), &keys, &columns).unwrap();
        assert_eq!(projected[0], rows[0].key());
        assert!(encode_projected_keys(&batch(&[None]), &keys, &columns).is_err());
        assert_ne!(rows[0].key(), rows[1].key());
        assert_eq!(rows[0], rows[2]);
        assert_ne!(rows[0].key(), rows[3].key());
        assert!(encode_rows(&batch(&[None]), &keys, &columns).is_err());
        let keyrows =
            encode_delete_keys(&batch(&[Some(vec!["東京", "😀"])]), &keys, &columns).unwrap();
        assert_eq!(keyrows[0], rows[0].key());
    }
    #[test]
    fn wide_valid_keys_split_without_losing_rows() {
        use arrow_array::{ArrayRef, StringArray};
        use std::sync::Arc;
        let values = [
            "a".repeat(3 * 1024 * 1024),
            "b".repeat(3 * 1024 * 1024),
            "c".repeat(3 * 1024 * 1024),
        ];
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(StringArray::from(
                values.iter().map(String::as_str).collect::<Vec<_>>(),
            )) as ArrayRef,
        )])
        .unwrap();
        let columns = BTreeMap::from([(
            "id".into(),
            serde_json::json!({"type":"string","nullable":false}),
        )]);
        let keys = vec!["id".into()];
        let column_json = serde_json::to_value(&columns).unwrap();
        let (first, first_keys) =
            mutation_page(&batch, 0, &keys, &columns, &column_json, true).unwrap();
        let (last, last_keys) = mutation_page(
            &batch,
            first.num_rows(),
            &keys,
            &columns,
            &column_json,
            true,
        )
        .unwrap();
        let (MutationRows::Keys(first_keys), MutationRows::Keys(last_keys)) =
            (first_keys, last_keys)
        else {
            panic!("expected key pages");
        };
        assert_eq!((first.num_rows(), last.num_rows()), (1, 2));
        assert_eq!(
            first_keys.into_iter().chain(last_keys).collect::<Vec<_>>(),
            values
                .iter()
                .map(|value| serde_json::to_vec(&[value]).unwrap())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn delete_schema_is_exact_key_only_and_full_rows_do_not_omit_columns() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(arrow_array::StringArray::from(vec!["one"])) as arrow_array::ArrayRef,
            ),
            (
                "note",
                Arc::new(arrow_array::StringArray::from(vec![Some("kept")]))
                    as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let columns = json!({"id":{"type":"string","nullable":false},"note":{"type":"string","nullable":true}});
        let keys = vec!["id".into()];
        assert!(encode_delete_keys(&batch, &keys, &columns).is_err());
        assert!(encode_rows(&batch.project(&[0]).unwrap(), &keys, &columns).is_err());
        let rows = encode_rows(&batch, &keys, &columns).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(encode_projected_keys(&batch, &keys, &columns).is_err());
        let projected =
            encode_projected_keys(&batch.project(&[0]).unwrap(), &keys, &columns).unwrap();
        assert_eq!(projected[0], rows[0].key());
    }
}
