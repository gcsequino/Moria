//! Full-build Source key maps share the already reduced immutable row population.
use crate::{dataset_writer::encoded_json_key, io::Scope};
use anyhow::{Result, ensure};
use serde::Deserialize;
use serde_json::Value;
use tokio_postgres::GenericClient;
use uuid::Uuid;

#[derive(Deserialize)]
pub(crate) struct SourceStore {
    keys: Vec<String>,
    columns: Value,
}
impl SourceStore {
    pub(crate) async fn task<C: GenericClient + Sync>(
        client: &C,
        scope: &Scope,
    ) -> Result<Option<Self>> {
        let metadata: Option<Value> = client
            .query_one(
                "SELECT moria.runtime_source_store_task($1,$2,$3,$4)",
                &[&scope.build, &scope.task, &scope.owner, &scope.fence],
            )
            .await?
            .get(0);
        metadata
            .map(serde_json::from_value)
            .transpose()
            .map_err(Into::into)
    }
    pub(crate) async fn keys<C: GenericClient + Sync>(
        &self,
        client: &C,
        scope: &Scope,
        ids: &[Uuid],
        values: &[Value],
        sources: &[Option<Value>],
    ) -> Result<()> {
        // Unkeyed Datasets retain full replacement semantics; an Object UID is not a Dataset key.
        if self.keys.is_empty() {
            return Ok(());
        }
        ensure!(
            ids.len() == values.len() && ids.len() == sources.len(),
            "SOURCE_KEY_MAP_INVALID"
        );
        let keys = values
            .iter()
            .zip(sources)
            .map(|(mapped, raw)| {
                encoded_json_key(raw.as_ref().unwrap_or(mapped), &self.keys, &self.columns)
            })
            .collect::<Result<Vec<_>>>()?;
        client
            .query_one(
                "SELECT moria.runtime_source_store_keys($1,$2,$3,$4,$5,$6)",
                &[
                    &scope.build,
                    &scope.task,
                    &scope.owner,
                    &scope.fence,
                    &ids,
                    &keys,
                ],
            )
            .await?;
        Ok(())
    }
}
