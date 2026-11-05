//! Metadata-independent index binding. Its reference may be embedded in standard snapshot summaries.
use super::{RootDescriptor, VisibilityIndex};
use crate::{
    canonical::canonical_json, persistent_directory::NodeReference, storage::SealedObject,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const INDEX_SUMMARY_PROPERTY: &str = "moria.native.index.v1";
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct IndexSummaryRef {
    pub version: u32,
    pub root: SealedObject,
}
impl IndexSummaryRef {
    pub(crate) fn encode(&self) -> Result<String> {
        self.validate()?;
        canonical_json(&serde_json::to_value(self)?)
    }
    pub(crate) fn decode(encoded: &str) -> Result<Self> {
        ensure!(encoded.len() <= 4096, "INVALID_NATIVE_INDEX_SUMMARY");
        let value: Self = serde_json::from_str(encoded)?;
        value.validate()?;
        ensure!(
            value.encode()? == encoded,
            "NONCANONICAL_NATIVE_INDEX_SUMMARY"
        );
        Ok(value)
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 1 && self.root.size > 0 && self.root.size <= 16 * 1024 * 1024,
            "INVALID_NATIVE_INDEX_SUMMARY"
        );
        // Reuse the closed logical-object/hash checks; a reference is never read authority.
        crate::catalog::NativeSnapshotRef {
            version: 3,
            root: self.root.clone(),
        }
        .encode()?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct IndexSummary {
    pub version: u32,
    pub table_uuid: Uuid,
    pub snapshot_id: String,
    pub sequence: String,
    pub schema_hash: String,
    pub key_schema_hash: String,
    pub primary_key: Vec<String>,
    pub data_files: u64,
    pub rows: u64,
    pub selections: Option<NodeReference>,
    pub visibility: Option<VisibilityIndex>,
}
impl IndexSummary {
    pub(crate) fn matches(&self, root: &RootDescriptor) -> Result<()> {
        ensure!(
            self.version == 1
                && self.table_uuid == root.table_uuid
                && self.snapshot_id == root.snapshot_id
                && self.sequence == root.sequence
                && self.schema_hash == root.schema_hash
                && self.key_schema_hash == root.key_schema_hash
                && self.primary_key == root.primary_key
                && self.data_files == root.data_files
                && self.rows == root.rows
                && self.selections == root.selections
                && self.visibility == root.visibility,
            "NATIVE_INDEX_SUMMARY_CHANGED"
        );
        Ok(())
    }
}

pub(super) async fn validate_binding(
    context: &crate::dataset_graph_read::GraphReadContext,
    root: &crate::dataset_graph_read::ReadableNode,
    descriptor: &RootDescriptor,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<()> {
    let reference = context.catalog_reference()?;
    if reference.version == 3 {
        ensure!(
            reference.root == *root.content() && descriptor.index_summary.is_some(),
            "NATIVE_INDEX_SUMMARY_REQUIRED"
        );
    }
    let Some(expected) = &descriptor.index_summary else {
        return Ok(());
    };
    let encoded = metadata
        .current_snapshot()
        .and_then(|snapshot| {
            snapshot
                .summary()
                .additional_properties
                .get(INDEX_SUMMARY_PROPERTY)
        })
        .ok_or_else(|| anyhow::anyhow!("NATIVE_INDEX_SUMMARY_REQUIRED"))?;
    ensure!(
        IndexSummaryRef::decode(encoded)? == *expected,
        "NATIVE_INDEX_SUMMARY_CHANGED"
    );
    let node = context.child(root, "index").await?;
    ensure!(
        node.kind() == "root"
            && node.content() == &expected.root
            && node.entries() == descriptor.rows,
        "NATIVE_INDEX_SUMMARY_CHANGED"
    );
    let summary: IndexSummary = serde_json::from_value(context.descriptor(&node).await?)?;
    summary.matches(descriptor)?;
    for (label, reference) in [
        ("selections", descriptor.selections.as_ref()),
        (
            "hashes",
            descriptor
                .visibility
                .as_ref()
                .and_then(|index| index.hashes.as_ref()),
        ),
        (
            "locations",
            descriptor
                .visibility
                .as_ref()
                .and_then(|index| index.locations.as_ref()),
        ),
    ] {
        if let Some(reference) = reference {
            let linked = context.child(&node, label).await?;
            ensure!(
                linked.kind() == "directory"
                    && linked.content() == &reference.content
                    && linked.entries() == reference.entries
                    && linked.logical_bytes() == reference.logical_bytes,
                "NATIVE_INDEX_SUMMARY_CHANGED"
            );
        }
    }
    context.check_node(&node).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        native_snapshot_v3::{LogicalFile, PROFILE},
        storage::LogicalObject,
    };
    use serde_json::json;
    fn object() -> SealedObject {
        SealedObject {
            object: LogicalObject::new("warehouse", "table", "summary").unwrap(),
            size: 10,
            sha256: format!("sha256:{}", "a".repeat(64)),
        }
    }
    #[test]
    fn reference_format_is_canonical_and_distinct_from_catalog_version() {
        let reference = IndexSummaryRef {
            version: 1,
            root: object(),
        };
        let encoded = reference.encode().unwrap();
        assert_eq!(IndexSummaryRef::decode(&encoded).unwrap(), reference);
        assert!(IndexSummaryRef::decode(&format!(" {encoded}")).is_err());
        let mut wrong = reference.clone();
        wrong.version = 3;
        assert!(wrong.encode().is_err());
        let mut value = serde_json::to_value(reference).unwrap();
        value["unexpected"] = json!(true);
        assert!(IndexSummaryRef::decode(&canonical_json(&value).unwrap()).is_err());
    }
    #[test]
    fn summary_binds_physical_visibility_and_schema_without_metadata_hash_dependency() {
        let root = RootDescriptor {
            version: 3,
            reader_profile: PROFILE.into(),
            profile: json!({}),
            table_uuid: Uuid::new_v4(),
            snapshot_id: "2".into(),
            sequence: "3".into(),
            schema_hash: "schema".into(),
            key_schema_hash: "key".into(),
            primary_key: vec!["id".into()],
            data_files: 0,
            rows: 0,
            selections: None,
            metadata: LogicalFile {
                name: "moria://w/t/meta".into(),
                content: object(),
            },
            files: NodeReference {
                content: object(),
                entries: 0,
                logical_bytes: 0,
                nodes: 1,
            },
            visibility: Some(VisibilityIndex {
                rows: 0,
                hashes: None,
                locations: None,
            }),
            index_summary: None,
        };
        let summary = IndexSummary {
            version: 1,
            table_uuid: root.table_uuid,
            snapshot_id: root.snapshot_id.clone(),
            sequence: root.sequence.clone(),
            schema_hash: root.schema_hash.clone(),
            key_schema_hash: root.key_schema_hash.clone(),
            primary_key: root.primary_key.clone(),
            data_files: 0,
            rows: 0,
            selections: None,
            visibility: root.visibility.clone(),
        };
        summary.matches(&root).unwrap();
        let mut metadata_changed = root.clone();
        metadata_changed.metadata.content.sha256 = format!("sha256:{}", "b".repeat(64));
        summary.matches(&metadata_changed).unwrap();
        let mut wrong = summary.clone();
        wrong.sequence = "4".into();
        assert!(wrong.matches(&root).is_err());
        let mut wrong = summary.clone();
        wrong.primary_key.reverse();
        wrong.primary_key.push("other".into());
        assert!(wrong.matches(&root).is_err());
        let mut wrong = summary;
        wrong.rows = 1;
        assert!(wrong.matches(&root).is_err());
    }
}
