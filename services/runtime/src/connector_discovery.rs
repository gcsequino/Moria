use crate::{canonical, source_runtime};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const MAX_PROPOSAL_BYTES: usize = 32 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Capability {
    BatchRead,
    SchemaDiscovery,
}

impl Capability {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::BatchRead => "batchRead",
            Self::SchemaDiscovery => "schemaDiscovery",
        }
    }
}

pub(crate) fn capability_supported(
    declared: &Value,
    mode: &Value,
    coverage: &Value,
    requested: Option<Capability>,
) -> bool {
    let Some(declared) = declared.as_array() else {
        return false;
    };
    let batch = declared.iter().any(|value| value == "batchRead")
        && mode == "replace"
        && coverage == "full";
    let discovery = declared.iter().any(|value| value == "schemaDiscovery");
    match requested {
        Some(Capability::BatchRead) => batch,
        Some(Capability::SchemaDiscovery) => discovery,
        None => batch || discovery,
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SchemaProposal {
    version: u32,
    stream: String,
    columns: Value,
    primary_key: Vec<String>,
}

impl SchemaProposal {
    pub(crate) fn parse(bytes: &[u8], configured_stream: &str) -> Result<Value> {
        let parsed = (|| {
            ensure!(
                bytes.len() <= MAX_PROPOSAL_BYTES,
                "PIPELINE_DISCOVERY_INVALID"
            );
            let mut proposal: Self = serde_json::from_slice(bytes)?;
            ensure!(
                proposal.version == 1 && proposal.stream == configured_stream,
                "PIPELINE_DISCOVERY_INVALID"
            );
            ensure!(
                proposal
                    .columns
                    .as_object()
                    .is_some_and(|columns| !columns.is_empty() && columns.len() <= 256),
                "PIPELINE_DISCOVERY_INVALID"
            );
            let (columns, _) = source_runtime::dataset_schema(&proposal.columns)?;
            source_runtime::validate_dataset_primary_key(&columns, &proposal.primary_key)?;
            proposal.columns = columns;
            let value = serde_json::to_value(proposal)?;
            ensure!(
                canonical::canonical_json(&value)?.len() <= MAX_PROPOSAL_BYTES,
                "PIPELINE_DISCOVERY_INVALID"
            );
            Ok(value)
        })();
        parsed.map_err(|_: anyhow::Error| anyhow::anyhow!("PIPELINE_DISCOVERY_INVALID"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn proposal() -> Value {
        json!({"version":1,"stream":"customers","columns":{
            "id":{"type":"uid","nullable":false},
            "tags":{"type":"array","nullable":true,"items":{"type":"string","nullable":false}},
            "balance":{"type":"decimal","nullable":true,"precision":20,"scale":4}
        },"primaryKey":["id"]})
    }

    #[test]
    fn configuration_and_readiness_require_an_executable_capability_pair() {
        for declared in [
            json!(["batchRead"]),
            json!(["schemaDiscovery"]),
            json!(["schemaDiscovery", "batchRead"]),
            json!(["writeback"]),
        ] {
            for mode in ["replace", "append"] {
                for coverage in ["full", "incremental"] {
                    let batch = declared
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|value| value == "batchRead")
                        && mode == "replace"
                        && coverage == "full";
                    let discovery = declared
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|value| value == "schemaDiscovery");
                    assert_eq!(
                        capability_supported(&declared, &json!(mode), &json!(coverage), None),
                        batch || discovery
                    );
                    assert_eq!(
                        capability_supported(
                            &declared,
                            &json!(mode),
                            &json!(coverage),
                            Some(Capability::BatchRead)
                        ),
                        batch
                    );
                    assert_eq!(
                        capability_supported(
                            &declared,
                            &json!(mode),
                            &json!(coverage),
                            Some(Capability::SchemaDiscovery)
                        ),
                        discovery
                    );
                }
            }
        }
    }

    #[test]
    fn typed_proposals_reuse_dataset_schema_and_key_semantics() {
        let value = proposal();
        assert_eq!(
            SchemaProposal::parse(&serde_json::to_vec(&value).unwrap(), "customers").unwrap(),
            value
        );
        let mut keyless = value;
        keyless["primaryKey"] = json!([]);
        assert!(SchemaProposal::parse(&serde_json::to_vec(&keyless).unwrap(), "customers").is_ok());
    }

    #[test]
    fn wrong_stream_invalid_types_and_keys_never_become_proposals() {
        let mut invalid = Vec::new();
        let mut value = proposal();
        value["stream"] = json!("other");
        invalid.push(value);
        let mut value = proposal();
        value["columns"]["id"]["nullable"] = json!(true);
        invalid.push(value);
        let mut value = proposal();
        value["primaryKey"] = json!(["id", "id"]);
        invalid.push(value);
        let mut value = proposal();
        value["primaryKey"] = json!(["missing"]);
        invalid.push(value);
        let mut value = proposal();
        value["columns"]["balance"]["scale"] = json!(21);
        invalid.push(value);
        let mut value = proposal();
        value["columns"]["bad.name"] = json!({"type":"string","nullable":false});
        invalid.push(value);
        let mut value = proposal();
        value["endpoint"] = json!("https://guest.invalid");
        invalid.push(value);
        for value in invalid {
            assert_eq!(
                SchemaProposal::parse(&serde_json::to_vec(&value).unwrap(), "customers")
                    .unwrap_err()
                    .to_string(),
                "PIPELINE_DISCOVERY_INVALID"
            );
        }
    }

    #[test]
    fn proposal_bytes_and_column_counts_are_bounded() {
        assert!(SchemaProposal::parse(&vec![b' '; MAX_PROPOSAL_BYTES + 1], "customers").is_err());
        let mut value = proposal();
        let columns = (0..257)
            .map(|n| {
                (
                    format!("field{n}"),
                    json!({"type":"string","nullable":false}),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        value["columns"] = Value::Object(columns);
        value["primaryKey"] = json!([]);
        assert!(SchemaProposal::parse(&serde_json::to_vec(&value).unwrap(), "customers").is_err());
    }
}
