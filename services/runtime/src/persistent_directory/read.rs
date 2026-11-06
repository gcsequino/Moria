//! Exact-root read-only directory traversal. Serialized references never grant membership.
use super::*;
use crate::dataset_graph_read::{GraphReadContext, ReadableNode};
#[derive(Clone, Debug)]
pub struct ReadValue {
    pub value: Value,
    pub links: Vec<ReadableNode>,
}
#[derive(Clone, Debug)]
pub struct ReadEntry {
    pub value: Value,
    links: Vec<SealedObject>,
    context: GraphReadContext,
    node: ReadableNode,
    ordinal: usize,
}
impl ReadEntry {
    pub fn link_count(&self) -> usize {
        self.links.len()
    }
    pub async fn link(&self, index: usize) -> Result<ReadableNode> {
        let content = self
            .links
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("INVALID_NATIVE_DIRECTORY"))?;
        let node = self
            .context
            .child(&self.node, &format!("entry/{}/{index}", self.ordinal))
            .await?;
        ensure!(node.content() == content, "DATASET_GRAPH_ANCESTRY_REQUIRED");
        Ok(node)
    }
}
#[derive(Clone, Debug)]
pub struct ReadDirectory {
    context: GraphReadContext,
    root: ReadableNode,
    reference: NodeReference,
}
impl ReadDirectory {
    pub(crate) fn entries(
        &self,
    ) -> futures::stream::BoxStream<'static, Result<(String, ReadEntry)>> {
        struct Walk {
            directory: ReadDirectory,
            stack: Vec<(ReadableNode, NodeReference, String)>,
            leaf: Option<ReadableNode>,
            records: std::collections::VecDeque<(usize, Record)>,
        }
        let state = Walk {
            directory: self.clone(),
            stack: vec![(self.root.clone(), self.reference.clone(), String::new())],
            leaf: None,
            records: std::collections::VecDeque::new(),
        };
        Box::pin(futures::stream::try_unfold(state, |mut state| async move {
            loop {
                if let Some((ordinal, record)) = state.records.pop_front() {
                    #[derive(Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Encoded {
                        value: Value,
                        links: Vec<SealedObject>,
                    }
                    let encoded: Encoded = serde_json::from_value(record.value)?;
                    ensure!(encoded.links.len() <= 128, "INVALID_NATIVE_DIRECTORY");
                    let entry = ReadEntry {
                        value: encoded.value,
                        links: encoded.links,
                        context: state.directory.context.clone(),
                        node: state.leaf.as_ref().unwrap().clone(),
                        ordinal,
                    };
                    return Ok(Some(((record.key, entry), state)));
                }
                let Some((node, reference, prefix)) = state.stack.pop() else {
                    return Ok(None);
                };
                match state.directory.node(&node, &reference, &prefix).await? {
                    Node::Leaf { records, .. } => {
                        state.leaf = Some(node);
                        state.records = records.into_iter().enumerate().collect();
                    }
                    Node::Branch { children, .. } => {
                        ensure!(state.stack.len() + children.len() <= 1024, "RESOURCE_LIMIT");
                        for (digit, reference) in children.into_iter().rev() {
                            let child = state.directory.context.child(&node, &digit).await?;
                            state
                                .stack
                                .push((child, reference, format!("{prefix}{digit}")));
                        }
                    }
                }
            }
        }))
    }
    pub(crate) async fn new(
        context: GraphReadContext,
        root: ReadableNode,
        reference: NodeReference,
    ) -> Result<Self> {
        ensure!(
            root.kind() == "directory"
                && root.content() == &reference.content
                && root.entries() == reference.entries
                && root.logical_bytes() == reference.logical_bytes,
            "INVALID_NATIVE_DIRECTORY"
        );
        let directory = Self {
            context,
            root,
            reference,
        };
        directory
            .node(&directory.root, &directory.reference, "")
            .await?;
        Ok(directory)
    }
    async fn node(
        &self,
        node: &ReadableNode,
        reference: &NodeReference,
        prefix: &str,
    ) -> Result<Node> {
        ensure!(
            node.content() == &reference.content
                && node.kind() == "directory"
                && node.entries() == reference.entries
                && node.logical_bytes() == reference.logical_bytes,
            "INVALID_NATIVE_DIRECTORY"
        );
        let parsed: Node = serde_json::from_value(self.context.descriptor(node).await?)?;
        ensure!(
            summary(&parsed, prefix)?
                == (reference.entries, reference.logical_bytes, reference.nodes),
            "INVALID_NATIVE_DIRECTORY"
        );
        Ok(parsed)
    }
    pub async fn lookup(&self, key: &str) -> Result<Option<ReadValue>> {
        let Some(entry) = self.lookup_entry(key).await? else {
            return Ok(None);
        };
        let mut links = Vec::with_capacity(entry.link_count());
        for index in 0..entry.link_count() {
            links.push(entry.link(index).await?);
        }
        Ok(Some(ReadValue {
            value: entry.value,
            links,
        }))
    }
    pub async fn lookup_entry(&self, key: &str) -> Result<Option<ReadEntry>> {
        ensure!(
            !key.is_empty() && key.len() <= MAX_KEY_BYTES,
            "RESOURCE_LIMIT"
        );
        let hash = route(key);
        let mut prefix = String::new();
        let mut reference = self.reference.clone();
        let mut node = self.root.clone();
        loop {
            match self.node(&node, &reference, &prefix).await? {
                Node::Leaf { records, .. } => {
                    let Ok(ordinal) =
                        records.binary_search_by(|record| record.key.as_str().cmp(key))
                    else {
                        return Ok(None);
                    };
                    #[derive(Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Encoded {
                        value: Value,
                        links: Vec<SealedObject>,
                    }
                    let encoded: Encoded = serde_json::from_value(records[ordinal].value.clone())?;
                    ensure!(encoded.links.len() <= 128, "INVALID_NATIVE_DIRECTORY");
                    return Ok(Some(ReadEntry {
                        value: encoded.value,
                        links: encoded.links,
                        context: self.context.clone(),
                        node,
                        ordinal,
                    }));
                }
                Node::Branch { children, .. } => {
                    let digit = &hash[prefix.len()..prefix.len() + 1];
                    let Some(child) = children.get(digit) else {
                        return Ok(None);
                    };
                    node = self.context.child(&node, digit).await?;
                    reference = child.clone();
                    prefix.push_str(digit);
                }
            }
        }
    }
}
