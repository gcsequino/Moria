//! Bounded path-copy directory storage. Directory membership is not a Dataset qualification.
use crate::{
    canonical::canonical_json,
    catalog::{DatasetIoGrant, read_descriptor},
    storage::{Cancellation, LogicalObject, ObjectStore, SealedObject},
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

const MAX_DEPTH: usize = 64;
const MAX_LEAF_ENTRIES: usize = 128;
const MAX_NODE_BYTES: usize = 16 * 1024 * 1024;
const MAX_KEY_BYTES: usize = 4 * 1024 * 1024;
const MAX_MUTATIONS: usize = 4096;
const MAX_MUTATION_BYTES: usize = 16 * 1024 * 1024;
const MAX_SAFE: u64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeReference {
    pub content: SealedObject,
    pub entries: u64,
    pub logical_bytes: u64,
    pub nodes: u64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Record {
    key: String,
    value: Value,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum Node {
    Leaf {
        version: u32,
        prefix: String,
        records: Vec<Record>,
    },
    Branch {
        version: u32,
        prefix: String,
        children: BTreeMap<String, NodeReference>,
    },
}
fn route(key: &str) -> String {
    format!("{:x}", Sha256::digest(key.as_bytes()))
}
fn bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(canonical_json(&serde_json::to_value(value)?)?.into_bytes())
}
fn safe_add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b)
        .filter(|v| *v <= MAX_SAFE)
        .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))
}
fn summary(node: &Node, prefix: &str) -> Result<(u64, u64, u64)> {
    ensure!(
        prefix.len() <= MAX_DEPTH
            && prefix
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
        "INVALID_NATIVE_DIRECTORY"
    );
    match node {
        Node::Leaf {
            version,
            prefix: actual,
            records,
        } => {
            ensure!(
                *version == 3
                    && actual == prefix
                    && !records.is_empty()
                    && records.len() <= MAX_LEAF_ENTRIES,
                "INVALID_NATIVE_DIRECTORY"
            );
            let mut logical = 0;
            let mut previous: Option<&str> = None;
            for record in records {
                ensure!(
                    !record.key.is_empty()
                        && record.key.len() <= MAX_KEY_BYTES
                        && route(&record.key).starts_with(prefix)
                        && previous.is_none_or(|key| key < record.key.as_str()),
                    "INVALID_NATIVE_DIRECTORY"
                );
                logical = safe_add(logical, bytes(record)?.len() as u64)?;
                previous = Some(&record.key);
            }
            Ok((records.len() as u64, logical, 1))
        }
        Node::Branch {
            version,
            prefix: actual,
            children,
        } => {
            ensure!(
                *version == 3
                    && actual == prefix
                    && prefix.len() < MAX_DEPTH
                    && !children.is_empty()
                    && children.len() <= 16,
                "INVALID_NATIVE_DIRECTORY"
            );
            let (mut entries, mut logical, mut nodes) = (0, 0, 1);
            for (digit, child) in children {
                ensure!(
                    digit.len() == 1
                        && digit
                            .bytes()
                            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                        && child.entries > 0
                        && child.nodes > 0
                        && child.content.size > 0
                        && child.content.size <= MAX_NODE_BYTES as u64,
                    "INVALID_NATIVE_DIRECTORY"
                );
                entries = safe_add(entries, child.entries)?;
                logical = safe_add(logical, child.logical_bytes)?;
                nodes = safe_add(nodes, child.nodes)?;
            }
            Ok((entries, logical, nodes))
        }
    }
}
#[async_trait]
trait Nodes: Send + Sync {
    async fn read(&self, reference: &NodeReference, prefix: &str) -> Result<Node>;
    async fn write(&self, node: Node, prefix: &str) -> Result<NodeReference>;
}

/// Storage mechanics used by closure and key-run directories. Actual head/profile ancestry
/// must be receiver-qualified by Catalog before a root can be reused in publication.
pub struct DatasetDirectory {
    store: Arc<dyn ObjectStore>,
    grant: Arc<DatasetIoGrant>,
    cancel: Cancellation,
}
impl DatasetDirectory {
    pub async fn new(
        store: Arc<dyn ObjectStore>,
        grant: Arc<DatasetIoGrant>,
        cancel: Cancellation,
    ) -> Result<Self> {
        grant.check_current().await?;
        grant.validate_store(store.as_ref())?;
        cancel.check()?;
        Ok(Self {
            store,
            grant,
            cancel,
        })
    }
    pub async fn lookup(&self, root: &NodeReference, key: &str) -> Result<Option<Value>> {
        ensure!(
            !key.is_empty() && key.len() <= MAX_KEY_BYTES,
            "RESOURCE_LIMIT"
        );
        self.grant.check_current().await?;
        let result = lookup(self, root, key).await?;
        self.grant.check_current().await?;
        Ok(result)
    }
    /// A batch is bounded independently of the existing population. None removes only the exact full key.
    pub async fn apply(
        &self,
        root: Option<&NodeReference>,
        mutations: BTreeMap<String, Option<Value>>,
    ) -> Result<Option<NodeReference>> {
        self.grant.check_current().await?;
        validate_mutations(&mutations)?;
        let result = apply(self, root.cloned(), String::new(), mutations).await?;
        self.grant.check_current().await?;
        Ok(result)
    }
    /// Page traversal returns authenticated node descriptors; it does not flatten the graph.
    pub async fn inspect(&self, reference: &NodeReference, prefix: &str) -> Result<Value> {
        let node = self.read(reference, prefix).await?;
        self.grant.check_current().await?;
        Ok(serde_json::to_value(node)?)
    }
}
#[async_trait]
impl Nodes for DatasetDirectory {
    async fn read(&self, reference: &NodeReference, prefix: &str) -> Result<Node> {
        self.cancel.check()?;
        self.grant.check_current().await?;
        let namespace = self.grant.namespace();
        ensure!(
            reference.content.object.warehouse_id == namespace.warehouse_id
                && reference.content.object.table_id == namespace.table_id,
            "BINDING_MISMATCH"
        );
        let node: Node = read_descriptor(
            self.store.as_ref(),
            self.grant.clone(),
            &reference.content,
            MAX_NODE_BYTES,
            self.cancel.clone(),
        )
        .await?;
        ensure!(
            summary(&node, prefix)?
                == (reference.entries, reference.logical_bytes, reference.nodes),
            "INVALID_NATIVE_DIRECTORY"
        );
        self.grant.check_current().await?;
        Ok(node)
    }
    async fn write(&self, node: Node, prefix: &str) -> Result<NodeReference> {
        self.cancel.check()?;
        let (entries, logical_bytes, nodes) = summary(&node, prefix)?;
        let encoded = bytes(&node)?;
        ensure!(encoded.len() <= MAX_NODE_BYTES, "RESOURCE_LIMIT");
        let namespace = self.grant.namespace();
        let object = LogicalObject::new(
            &namespace.warehouse_id,
            &namespace.table_id,
            &Uuid::new_v4().to_string(),
        )?;
        let mut writer = self
            .store
            .begin_object(
                object,
                encoded.len() as u64,
                format!("sha256:{:x}", Sha256::digest(&encoded)),
                self.grant.clone(),
                self.cancel.clone(),
            )
            .await?;
        for chunk in encoded.chunks(1024 * 1024) {
            writer.append(chunk).await?;
        }
        let content = writer.seal().await?;
        self.grant.check_current().await?;
        Ok(NodeReference {
            content,
            entries,
            logical_bytes,
            nodes,
        })
    }
}
fn validate_mutations(mutations: &BTreeMap<String, Option<Value>>) -> Result<()> {
    ensure!(mutations.len() <= MAX_MUTATIONS, "RESOURCE_LIMIT");
    let mut total = 0usize;
    for (key, value) in mutations {
        ensure!(
            !key.is_empty() && key.len() <= MAX_KEY_BYTES,
            "RESOURCE_LIMIT"
        );
        total = total
            .checked_add(key.len())
            .and_then(|n| {
                n.checked_add(
                    value
                        .as_ref()
                        .map_or(0, |value| bytes(value).map_or(usize::MAX, |v| v.len())),
                )
            })
            .filter(|n| *n <= MAX_MUTATION_BYTES)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
    }
    Ok(())
}
async fn lookup(io: &dyn Nodes, root: &NodeReference, key: &str) -> Result<Option<Value>> {
    let route = route(key);
    let mut reference = root.clone();
    let mut prefix = String::new();
    loop {
        match io.read(&reference, &prefix).await? {
            Node::Leaf { records, .. } => {
                return Ok(records
                    .binary_search_by(|record| record.key.as_str().cmp(key))
                    .ok()
                    .map(|at| records[at].value.clone()));
            }
            Node::Branch { children, .. } => {
                let digit = &route[prefix.len()..prefix.len() + 1];
                let Some(child) = children.get(digit) else {
                    return Ok(None);
                };
                reference = child.clone();
                prefix.push_str(digit);
            }
        }
    }
}
fn build<'a>(
    io: &'a dyn Nodes,
    prefix: String,
    records: BTreeMap<String, Value>,
) -> BoxFuture<'a, Result<Option<NodeReference>>> {
    Box::pin(async move {
        if records.is_empty() {
            return Ok(None);
        }
        if records.len() <= MAX_LEAF_ENTRIES
            && records
                .values()
                .map(|value| {
                    value
                        .get("links")
                        .and_then(Value::as_array)
                        .map_or(0, Vec::len)
                })
                .sum::<usize>()
                <= 256
        {
            let node = Node::Leaf {
                version: 3,
                prefix: prefix.clone(),
                records: records
                    .iter()
                    .map(|(key, value)| Record {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            };
            if bytes(&node)?.len() <= MAX_NODE_BYTES {
                return Ok(Some(io.write(node, &prefix).await?));
            }
        }
        ensure!(prefix.len() < MAX_DEPTH, "RESOURCE_LIMIT");
        let mut groups: BTreeMap<String, BTreeMap<String, Value>> = BTreeMap::new();
        for (key, value) in records {
            let hash = route(&key);
            groups
                .entry(hash[prefix.len()..prefix.len() + 1].to_owned())
                .or_default()
                .insert(key, value);
        }
        let mut children = BTreeMap::new();
        for (digit, group) in groups {
            let child = build(io, format!("{prefix}{digit}"), group)
                .await?
                .ok_or_else(|| anyhow::anyhow!("INVALID_NATIVE_DIRECTORY"))?;
            children.insert(digit, child);
        }
        Ok(Some(
            io.write(
                Node::Branch {
                    version: 3,
                    prefix: prefix.clone(),
                    children,
                },
                &prefix,
            )
            .await?,
        ))
    })
}
fn apply<'a>(
    io: &'a dyn Nodes,
    root: Option<NodeReference>,
    prefix: String,
    mutations: BTreeMap<String, Option<Value>>,
) -> BoxFuture<'a, Result<Option<NodeReference>>> {
    Box::pin(async move {
        if mutations.is_empty() {
            return Ok(root);
        }
        let Some(reference) = root else {
            return build(
                io,
                prefix,
                mutations
                    .into_iter()
                    .filter_map(|(key, value)| value.map(|value| (key, value)))
                    .collect(),
            )
            .await;
        };
        let node = io.read(&reference, &prefix).await?;
        match node {
            Node::Leaf { records, .. } => {
                let mut records = records
                    .into_iter()
                    .map(|record| (record.key, record.value))
                    .collect::<BTreeMap<_, _>>();
                if mutations
                    .iter()
                    .all(|(key, value)| records.get(key) == value.as_ref())
                {
                    return Ok(Some(reference));
                }
                for (key, value) in mutations {
                    match value {
                        Some(value) => {
                            records.insert(key, value);
                        }
                        None => {
                            records.remove(&key);
                        }
                    }
                }
                build(io, prefix, records).await
            }
            Node::Branch { mut children, .. } => {
                let old = children.clone();
                let mut groups: BTreeMap<String, BTreeMap<String, Option<Value>>> = BTreeMap::new();
                for (key, value) in mutations {
                    let hash = route(&key);
                    groups
                        .entry(hash[prefix.len()..prefix.len() + 1].to_owned())
                        .or_default()
                        .insert(key, value);
                }
                for (digit, group) in groups {
                    let child = apply(
                        io,
                        children.get(&digit).cloned(),
                        format!("{prefix}{digit}"),
                        group,
                    )
                    .await?;
                    match child {
                        Some(child) => {
                            children.insert(digit, child);
                        }
                        None => {
                            children.remove(&digit);
                        }
                    }
                }
                if children == old {
                    return Ok(Some(reference));
                }
                if children.is_empty() {
                    return Ok(None);
                }
                // Never flatten unchanged siblings merely because a deletion made the branch small.
                Ok(Some(
                    io.write(
                        Node::Branch {
                            version: 3,
                            prefix: prefix.clone(),
                            children,
                        },
                        &prefix,
                    )
                    .await?,
                ))
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    #[derive(Default)]
    struct Memory {
        nodes: Mutex<BTreeMap<String, Node>>,
        reads: AtomicUsize,
        writes: AtomicUsize,
    }
    #[async_trait]
    impl Nodes for Memory {
        async fn read(&self, reference: &NodeReference, prefix: &str) -> Result<Node> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let node = self
                .nodes
                .lock()
                .unwrap()
                .get(&reference.content.sha256)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("MISSING_OBJECT"))?;
            ensure!(
                summary(&node, prefix)?
                    == (reference.entries, reference.logical_bytes, reference.nodes),
                "INVALID_NATIVE_DIRECTORY"
            );
            Ok(node)
        }
        async fn write(&self, node: Node, prefix: &str) -> Result<NodeReference> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            let (entries, logical_bytes, nodes) = summary(&node, prefix)?;
            let encoded = bytes(&node)?;
            let sha256 = format!("sha256:{:x}", Sha256::digest(&encoded));
            let content = SealedObject {
                object: LogicalObject::new("warehouse", "table", &Uuid::new_v4().to_string())?,
                size: encoded.len() as u64,
                sha256: sha256.clone(),
            };
            self.nodes.lock().unwrap().insert(sha256, node);
            Ok(NodeReference {
                content,
                entries,
                logical_bytes,
                nodes,
            })
        }
    }
    #[tokio::test]
    async fn sparse_changes_copy_paths_and_keep_old_roots() {
        let io = Memory::default();
        let mut root = None;
        for start in (0..16384).step_by(1024) {
            let changes = (start..start + 1024)
                .map(|i| (format!("key-{i:06}"), Some(Value::from(i))))
                .collect();
            validate_mutations(&changes).unwrap();
            root = apply(&io, root, String::new(), changes).await.unwrap();
        }
        let old = root.unwrap();
        assert_eq!(old.entries, 16384);
        io.reads.store(0, Ordering::Relaxed);
        io.writes.store(0, Ordering::Relaxed);
        let changes = BTreeMap::from([
            ("key-000010".into(), Some(Value::from(99999))),
            ("key-010000".into(), None),
            ("new-key".into(), Some(Value::from(7))),
        ]);
        let new = apply(&io, Some(old.clone()), String::new(), changes)
            .await
            .unwrap()
            .unwrap();
        let reads = io.reads.load(Ordering::Relaxed);
        let writes = io.writes.load(Ordering::Relaxed);
        assert!(reads <= 3 * 65 && writes <= 3 * 65);
        assert!(reads < old.nodes as usize / 2 && writes < old.nodes as usize / 2);
        assert_eq!(new.entries, 16384);
        assert_eq!(
            lookup(&io, &old, "key-000010").await.unwrap(),
            Some(Value::from(10))
        );
        assert_eq!(
            lookup(&io, &new, "key-000010").await.unwrap(),
            Some(Value::from(99999))
        );
        assert_eq!(lookup(&io, &new, "key-010000").await.unwrap(), None);
        assert_eq!(
            lookup(&io, &old, "key-010000").await.unwrap(),
            Some(Value::from(10000))
        );
        assert_eq!(
            lookup(&io, &new, "new-key").await.unwrap(),
            Some(Value::from(7))
        );
        io.writes.store(0, Ordering::Relaxed);
        let same = apply(
            &io,
            Some(new.clone()),
            String::new(),
            BTreeMap::from([
                ("key-000010".into(), Some(Value::from(99999))),
                ("absent".into(), None),
            ]),
        )
        .await
        .unwrap();
        assert_eq!(same, Some(new));
        assert_eq!(io.writes.load(Ordering::Relaxed), 0);
    }
    #[tokio::test]
    async fn malformed_paths_summaries_and_budgets_fail_closed() {
        let io = Memory::default();
        let root = apply(
            &io,
            None,
            String::new(),
            BTreeMap::from([("one".into(), Some(Value::from(1)))]),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(io.read(&root, "a").await.is_err());
        let mut wrong = root.clone();
        wrong.entries += 1;
        assert!(io.read(&wrong, "").await.is_err());
        assert!(
            validate_mutations(
                &(0..4097)
                    .map(|i| (i.to_string(), Some(Value::Null)))
                    .collect()
            )
            .is_err()
        );
        assert!(validate_mutations(&BTreeMap::from([("".into(), None)])).is_err());
    }
}

pub mod graph;

pub mod read;
