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

impl WriteContract {
    /// The task's physical part schema is derived from the accepted full target schema.
    /// Delete input keeps exact key order and Iceberg field IDs; its partset still binds
    /// the complete target schema hash.
    pub(crate) fn input_schema(
        &self,
        target: &iceberg::spec::Schema,
    ) -> Result<iceberg::spec::Schema> {
        ensure!(self.version == 2, "DATASET_WRITE_CONTRACT_REQUIRED");
        match (self.mode.as_str(), self.input_kind.as_str()) {
            ("replace" | "append" | "upsert", "rows") => Ok(target.clone()),
            ("delete", "keys") => {
                ensure!(!self.primary_key.is_empty(), "DATASET_PRIMARY_KEY_MISSING");
                let fields = self
                    .primary_key
                    .iter()
                    .map(|key| {
                        let field = target
                            .as_struct()
                            .fields()
                            .iter()
                            .find(|field| field.name == *key)
                            .ok_or_else(|| anyhow::anyhow!("DATASET_PRIMARY_KEY_MISSING"))?;
                        ensure!(field.required, "DATASET_PRIMARY_KEY_NULL");
                        Ok(field.clone())
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(iceberg::spec::Schema::builder()
                    .with_schema_id(target.schema_id())
                    .with_fields(fields)
                    .build()?)
            }
            _ => anyhow::bail!("DATASET_WRITE_CONTRACT_REQUIRED"),
        }
    }
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
    fn delete_projection_preserves_ordered_array_key_ids_and_excludes_payload() {
        let target: iceberg::spec::Schema = serde_json::from_value(json!({
            "type":"struct", "schema-id":5,
            "fields":[
                {"id":1,"name":"id","required":true,"type":"long"},
                {"id":2,"name":"tags","required":true,"type":{"type":"list","element-id":3,"element-required":true,"element":"string"}},
                {"id":4,"name":"note","required":false,"type":"string"},
            ]
        })).unwrap();
        let mut contract = WriteContract {
            version: 2,
            mode: "delete".into(),
            coverage: "incremental".into(),
            input_kind: "keys".into(),
            schema_hash: crate::dataset_writer::schema_hash(&target).unwrap(),
            key_schema_hash: format!("sha256:{}", "a".repeat(64)),
            primary_key: vec!["tags".into(), "id".into()],
            expected_head: ExpectedHead {
                revision: "0".into(),
                snapshot: None,
                metadata_location: None,
            },
        };
        let input = contract.input_schema(&target).unwrap();
        assert_eq!(input.schema_id(), target.schema_id());
        assert_eq!(
            input.as_struct().fields(),
            &[
                target.as_struct().fields()[1].clone(),
                target.as_struct().fields()[0].clone()
            ]
        );
        contract.primary_key.push("note".into());
        assert!(contract.input_schema(&target).is_err());
        contract.primary_key = vec!["missing".into()];
        assert!(contract.input_schema(&target).is_err());
        contract.input_kind = "rows".into();
        assert!(contract.input_schema(&target).is_err());
        contract.mode = "append".into();
        contract.primary_key.clear();
        assert_eq!(contract.input_schema(&target).unwrap(), target);
    }

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
