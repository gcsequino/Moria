//! Native-only publication identity and metadata-pruning equivalence receipts.
use crate::canonical::canonical_json;
use anyhow::{Result, ensure};
use iceberg::spec::Schema;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Semantics {
    producer_kind: ProducerKind,
    change_kind: ChangeKind,
    mode: Option<WriteMode>,
    unchanged_reason: Option<UnchangedReason>,
    equivalence_receipt: Option<Value>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum ProducerKind {
    Execution,
    Maintenance,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum ChangeKind {
    Replace,
    Mutation,
    Unchanged,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum WriteMode {
    Replace,
    Append,
    Upsert,
    Delete,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum UnchangedReason {
    MetadataPrune,
    Compaction,
    NoOp,
}

/// Decode the actual persisted producer result before acknowledging its transaction.
pub(crate) fn validate_semantics(value: Value) -> Result<()> {
    let wire: Semantics = serde_json::from_value(value)?;
    match wire.producer_kind {
        ProducerKind::Execution => ensure!(wire.mode.is_some(), "DATASET_DELTA_MODE_REQUIRED"),
        ProducerKind::Maintenance => ensure!(
            wire.mode.is_none() && matches!(wire.change_kind, ChangeKind::Unchanged),
            "DATASET_DELTA_MODE_INVALID"
        ),
    }
    match wire.change_kind {
        ChangeKind::Replace => ensure!(
            matches!(wire.mode, Some(WriteMode::Replace))
                && wire.unchanged_reason.is_none()
                && wire.equivalence_receipt.is_none(),
            "DATASET_DELTA_MODE_INVALID"
        ),
        ChangeKind::Mutation => ensure!(
            matches!(
                wire.mode,
                Some(WriteMode::Append | WriteMode::Upsert | WriteMode::Delete)
            ) && wire.unchanged_reason.is_none()
                && wire.equivalence_receipt.is_none(),
            "DATASET_DELTA_MODE_INVALID"
        ),
        ChangeKind::Unchanged => ensure!(
            wire.unchanged_reason.is_some()
                && wire
                    .equivalence_receipt
                    .as_ref()
                    .is_some_and(Value::is_object),
            "DATASET_DELTA_EQUIVALENCE_REQUIRED"
        ),
    }
    Ok(())
}

fn hash(value: &Value) -> Result<String> {
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(canonical_json(value)?.as_bytes())
    ))
}

pub(crate) fn profile(
    schema: &Schema,
    accepted_schema: &Value,
    binding_revision: &str,
    keys: &[String],
    key_hash: &str,
) -> Result<Value> {
    let accepted: Schema = serde_json::from_value(accepted_schema.clone())?;
    ensure!(&accepted == schema, "SCHEMA_CHANGED");
    Ok(
        json!({"datasetSchemaHash":crate::dataset_writer::schema_hash(&accepted)?,
        "datasetSchema":serde_json::to_value(&accepted)?,"bindingRevision":binding_revision,
        "keySchemaHash":key_hash,"orderedPrimaryKey":keys}),
    )
}

/// Only the authenticated before/after metadata of the native pruning operation is accepted.
/// Preserved current snapshot and all non-history table metadata establish logical equivalence.
pub(crate) fn metadata_prune_receipt(
    original: &Value,
    pruned: &Value,
    basis_root: &str,
    result_root: &str,
) -> Result<Value> {
    let snapshot = original["current-snapshot-id"]
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("SNAPSHOT_MISMATCH"))?;
    ensure!(
        pruned["current-snapshot-id"].as_i64() == Some(snapshot),
        "DATASET_DELTA_EQUIVALENCE_REQUIRED"
    );
    let current = |value: &Value| -> Result<Value> {
        let matches: Vec<_> = value["snapshots"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("MISSING_SNAPSHOTS"))?
            .iter()
            .filter(|entry| entry["snapshot-id"].as_i64() == Some(snapshot))
            .collect();
        ensure!(matches.len() == 1, "SNAPSHOT_MISMATCH");
        Ok(matches[0].clone())
    };
    let before = current(original)?;
    ensure!(
        current(pruned)? == before,
        "DATASET_DELTA_EQUIVALENCE_REQUIRED"
    );
    let stable = |value: &Value| -> Result<Value> {
        let mut value = value
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("INVALID_METADATA"))?
            .clone();
        for key in [
            "snapshots",
            "snapshot-log",
            "metadata-log",
            "last-updated-ms",
        ] {
            value.remove(key);
        }
        Ok(Value::Object(value))
    };
    ensure!(
        stable(original)? == stable(pruned)?,
        "DATASET_DELTA_EQUIVALENCE_REQUIRED"
    );
    Ok(
        json!({"version":1,"kind":"metadataPrune","basisRoot":basis_root,
        "resultRoot":result_root,"snapshot":snapshot.to_string(),"currentSnapshotHash":hash(&before)?}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accepted_schema_uses_the_same_typed_normalization_and_hash_as_task_partsets() {
        let raw = json!({"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","type":"long","required":true}],"identifier-field-ids":[]});
        let schema: Schema = serde_json::from_value(raw.clone()).unwrap();
        let output = profile(&schema, &raw, "binding", &["id".into()], "key").unwrap();
        assert_eq!(
            output["datasetSchemaHash"],
            crate::dataset_writer::schema_hash(&schema).unwrap()
        );
        assert_eq!(
            output["datasetSchema"],
            serde_json::to_value(&schema).unwrap()
        );
        let mut wrong = raw.clone();
        wrong["fields"][0]["required"] = json!(false);
        assert!(profile(&schema, &wrong, "binding", &["id".into()], "key").is_err());
        let mut wrong = raw;
        wrong["fields"][0]["type"] = json!("string");
        assert!(profile(&schema, &wrong, "binding", &["id".into()], "key").is_err());
    }
    #[test]
    fn maintenance_has_no_fabricated_write_mode_and_execution_keeps_its_mode() {
        let valid = json!({"producerKind":"maintenance","changeKind":"unchanged","mode":null,
            "unchangedReason":"metadataPrune","equivalenceReceipt":{"proof":"qualified"}});
        validate_semantics(valid.clone()).unwrap();
        let mut wrong = valid.clone();
        wrong["mode"] = json!("replace");
        assert!(validate_semantics(wrong).is_err());
        let mut wrong = valid.clone();
        wrong["changeKind"] = json!("mutation");
        assert!(validate_semantics(wrong).is_err());
        let mut wrong = valid.clone();
        wrong["producerKind"] = json!("execution");
        assert!(validate_semantics(wrong).is_err());
        let mut valid_execution = valid;
        valid_execution["producerKind"] = json!("execution");
        valid_execution["mode"] = json!("upsert");
        valid_execution["unchangedReason"] = json!("noOp");
        validate_semantics(valid_execution).unwrap();
        assert!(
            validate_semantics(
                json!({"producerKind":"execution","changeKind":"replace","mode":"append"})
            )
            .is_err()
        );
    }
    #[test]
    fn metadata_equivalence_is_not_inferred_from_counts_or_snapshot_id() {
        let original = json!({"current-snapshot-id":2,"current-schema-id":0,"last-updated-ms":1,
            "snapshots":[{"snapshot-id":1,"manifest-list":"old"},{"snapshot-id":2,"manifest-list":"current","summary":{"total-records":"8"}}],
            "snapshot-log":[1,2],"metadata-log":[1]});
        let mut pruned = original.clone();
        pruned["snapshots"] = json!([original["snapshots"][1].clone()]);
        pruned["snapshot-log"] = json!([2]);
        pruned["metadata-log"] = json!([]);
        pruned["last-updated-ms"] = json!(2);
        assert_eq!(
            metadata_prune_receipt(&original, &pruned, "basis", "result").unwrap()["kind"],
            "metadataPrune"
        );
        let mut wrong = pruned.clone();
        wrong["snapshots"][0]["manifest-list"] = json!("substituted");
        assert!(metadata_prune_receipt(&original, &wrong, "basis", "result").is_err());
        let mut wrong = pruned.clone();
        wrong["current-schema-id"] = json!(1);
        assert!(metadata_prune_receipt(&original, &wrong, "basis", "result").is_err());
        let mut wrong = pruned.clone();
        wrong["snapshots"] = json!([
            pruned["snapshots"][0].clone(),
            pruned["snapshots"][0].clone()
        ]);
        assert!(metadata_prune_receipt(&original, &wrong, "basis", "result").is_err());
    }
}
