//! Bounded snapshot-bound key batches. Directory, segment and block IO is shared by the batch.
use super::*;
use crate::dataset_writer::{
    key_run::{Locator, RunKind, RunValue, key_route},
    key_run_set,
};
use std::collections::BTreeMap;

impl NativeSnapshot {
    pub async fn lookup_keys(&self, values: &[Vec<Value>]) -> Result<Vec<Option<Locator>>> {
        self.check_current().await?;
        ensure!(values.len() <= 1000, "RESOURCE_LIMIT");
        let columns = crate::snapshot::columns(&serde_json::to_value(
            self.metadata.current_schema().as_ref(),
        )?)?;
        let columns = serde_json::to_value(columns)?;
        let mut keys = Vec::with_capacity(values.len());
        let mut bytes = 0usize;
        for row in values {
            ensure!(
                !self.descriptor.primary_key.is_empty()
                    && row.len() == self.descriptor.primary_key.len(),
                "DATASET_PRIMARY_KEY_INVALID"
            );
            let raw = Value::Object(
                self.descriptor
                    .primary_key
                    .iter()
                    .cloned()
                    .zip(row.iter().cloned())
                    .collect(),
            );
            let key = crate::dataset_writer::encoded_json_key(
                &raw,
                &self.descriptor.primary_key,
                &columns,
            )?;
            bytes = bytes
                .checked_add(key.len())
                .filter(|bytes| *bytes <= 8 * 1024 * 1024)
                .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
            keys.push(key);
        }
        self.lookup_encoded_keys(&keys).await
    }

    /// Input keys are derived from the actual accepted target schema through the shared C03 codec.
    /// Returned locators remain hints until the mutation receiver verifies file/row/key/sequence.
    pub(crate) async fn lookup_encoded_keys(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Locator>>> {
        self.check_current().await?;
        ensure!(keys.len() <= 1000, "RESOURCE_LIMIT");
        keys.iter().try_fold(0usize, |bytes, key| {
            ensure!(
                !key.is_empty() && key.len() <= 4 * 1024 * 1024,
                "DATASET_KEY_INDEX_INVALID"
            );
            bytes
                .checked_add(key.len())
                .filter(|bytes| *bytes <= 8 * 1024 * 1024)
                .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))
        })?;
        let mut result = vec![None; keys.len()];
        if keys.is_empty() {
            return Ok(result);
        }
        let Some(directory) = self.index_directory("locations").await? else {
            self.check_current().await?;
            return Ok(result);
        };
        let mut partitions = BTreeMap::<u8, Vec<usize>>::new();
        for (index, key) in keys.iter().enumerate() {
            partitions.entry(key_route(key)[0]).or_default().push(index);
        }
        for (partition, indices) in partitions {
            let Some(entry) = directory.lookup_entry(&format!("{partition:02x}")).await? else {
                continue;
            };
            let selected: Vec<_> = indices
                .iter()
                .map(|&index| keys[index].as_slice())
                .collect();
            let resolved = key_run_set::lookup_batch(
                &self.context,
                &self.run_cache,
                &entry,
                RunKind::Locations,
                &self.descriptor.key_schema_hash,
                &selected,
            )
            .await?;
            for (index, value) in indices.into_iter().zip(resolved) {
                result[index] = match value {
                    None => None,
                    Some(RunValue::Location(locator)) => Some(locator),
                    _ => anyhow::bail!("DATASET_KEY_INDEX_INVALID"),
                };
            }
        }
        self.check_current().await?;
        Ok(result)
    }
}
