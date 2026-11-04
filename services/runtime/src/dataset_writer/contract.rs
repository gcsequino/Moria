use crate::{canonical::fingerprint, catalog::DatasetIoGrant};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExpectedHead {
    pub revision: String,
    pub snapshot: Option<String>,
    pub metadata_location: Option<String>,
}

/// Immutable accepted writer semantics. This descriptor is not an IO capability.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WriteContract {
    pub version: u32,
    pub mode: String,
    pub coverage: String,
    pub input_kind: String,
    pub schema_hash: String,
    pub key_schema_hash: String,
    pub primary_key: Vec<String>,
    pub expected_head: ExpectedHead,
}

fn semantics(definition: Option<&Value>) -> Result<(&str, &str)> {
    let Some(definition) = definition else {
        return Ok(("replace", "full"));
    };
    match definition["kind"].as_str() {
        Some("Transform") => Ok(("replace", "full")),
        Some("Connection") => {
            let mode = definition["spec"]["mode"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("DATASET_WRITE_MODE_INVALID"))?;
            let coverage = definition["spec"]["coverage"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("DATASET_WRITE_MODE_INVALID"))?;
            ensure!(
                ["replace", "append", "upsert", "delete"].contains(&mode),
                "DATASET_WRITE_MODE_INVALID"
            );
            ensure!(
                match coverage {
                    "full" => true,
                    "incremental" => mode != "replace",
                    "appendOnly" => mode == "append",
                    _ => false,
                },
                "DATASET_WRITE_MODE_INVALID"
            );
            Ok((mode, coverage))
        }
        _ => anyhow::bail!("DATASET_WRITE_MODE_INVALID"),
    }
}

impl DatasetIoGrant {
    /// Derive semantics from the accepted execution, never from a guest output frame.
    pub async fn write_contract(&self, schema: &iceberg::spec::Schema) -> Result<WriteContract> {
        self.check_current().await?;
        self.validate_schema(schema)?;
        let client = self.database().get().await?;
        let accepted=client.query_opt("SELECT definition,outputs FROM moria.runtime_pipeline_executions WHERE execution=$1",&[&self.lease().execution]).await?;
        let definition = accepted
            .as_ref()
            .map(|row| row.get::<_, Value>("definition"));
        if let Some(accepted) = accepted.as_ref() {
            let outputs: Value = accepted.get("outputs");
            ensure!(
                outputs.as_object().is_some_and(|outputs| outputs
                    .values()
                    .any(|output| output["resource"].as_str() == Some(self.resource()))),
                "UNDECLARED_DATASET"
            );
        }
        let (mode, coverage) = semantics(definition.as_ref())?;
        self.require_write_mode(mode)?;
        let primary_key = self.primary_key()?;
        ensure!(
            !["upsert", "delete"].contains(&mode) || !primary_key.is_empty(),
            "DATASET_PRIMARY_KEY_MISSING"
        );
        let row=client.query_one("SELECT revision,snapshot_id,metadata_location FROM moria.dataset_heads WHERE environment=$1 AND dataset=$2",&[&self.session().environment(),&self.resource()]).await?;
        let revision: i64 = row.get("revision");
        ensure!(revision == self.expected_revision()?, "HEAD_CONFLICT");
        let key_schema_hash = if self.is_managed() {
            self.key_schema_hash()?.to_owned()
        } else {
            fingerprint(&serde_json::json!({"version":1,"keys":[]}))?
        };
        let result = WriteContract {
            version: 2,
            mode: mode.to_owned(),
            coverage: coverage.to_owned(),
            input_kind: if mode == "delete" { "keys" } else { "rows" }.into(),
            schema_hash: super::schema_hash(schema)?,
            key_schema_hash,
            primary_key,
            expected_head: ExpectedHead {
                revision: revision.to_string(),
                snapshot: row.get("snapshot_id"),
                metadata_location: row.get("metadata_location"),
            },
        };
        drop(client);
        self.check_current().await?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn coverage_and_mode_are_independent_and_closed() {
        for coverage in ["full", "incremental", "appendOnly"] {
            for mode in ["replace", "append", "upsert", "delete"] {
                let accepted = semantics(Some(
                    &json!({"kind":"Connection","spec":{"mode":mode,"coverage":coverage}}),
                ))
                .is_ok();
                assert_eq!(
                    accepted,
                    coverage == "full"
                        || (coverage == "incremental" && mode != "replace")
                        || (coverage == "appendOnly" && mode == "append")
                );
            }
        }
        assert_eq!(semantics(None).unwrap(), ("replace", "full"));
        assert_eq!(
            semantics(Some(&json!({"kind":"Transform"}))).unwrap(),
            ("replace", "full")
        );
        assert!(
            semantics(Some(
                &json!({"kind":"Connection","spec":{"mode":"merge","coverage":"full"}})
            ))
            .is_err()
        );
    }
}
