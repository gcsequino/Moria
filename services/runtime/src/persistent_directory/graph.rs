//! Directory nodes participate in receiver-qualified graph custody, including leaf value links.
use super::*;
use crate::dataset_graph::{NativeGraph, QualifiedNode};

#[derive(Clone, Debug)]
pub struct GraphValue {
    value: Value,
    links: Vec<QualifiedNode>,
}
impl GraphValue {
    pub fn new(value: Value, links: Vec<QualifiedNode>) -> Result<Self> {
        ensure!(links.len() <= 128, "RESOURCE_LIMIT");
        Ok(Self { value, links })
    }
    fn encoded(&self) -> Result<Value> {
        Ok(
            serde_json::json!({"value":self.value,"links":self.links.iter().map(|node|node.content()).collect::<Vec<_>>()}),
        )
    }
}
/// One writer's path-copy operation. Only actual issued or ancestry-admitted nodes can be linked.
/// The expected path is a fixed native root-codec field, never a caller-selected authority flag.
pub struct GraphDirectory {
    graph: NativeGraph,
    expected_path: Vec<String>,
    source_task: Option<uuid::Uuid>,
}
impl GraphDirectory {
    pub(crate) fn new(graph: NativeGraph, expected_path: Vec<String>) -> Result<Self> {
        ensure!(expected_path.len() <= 64, "RESOURCE_LIMIT");
        Ok(Self {
            graph,
            expected_path,
            source_task: None,
        })
    }
    pub(crate) fn sealed_task(
        graph: NativeGraph,
        expected_path: Vec<String>,
        task: uuid::Uuid,
    ) -> Result<Self> {
        let mut directory = Self::new(graph, expected_path)?;
        directory.source_task = Some(task);
        Ok(directory)
    }
    async fn admit(&self, path: &[String], content: &SealedObject) -> Result<QualifiedNode> {
        match self.source_task {
            Some(task) => self.graph.admit_task(task, path, content).await,
            None => self.graph.admit_head(path, content).await,
        }
    }
    fn path(&self, prefix: &str) -> Vec<String> {
        let mut path = self.expected_path.clone();
        path.extend(prefix.chars().map(|c| c.to_string()));
        path
    }
    async fn node_at(&self, reference: &NodeReference, prefix: &str) -> Result<QualifiedNode> {
        let known = self.graph.known_node(&reference.content).await?;
        let node = match known {
            Some(node) => node,
            None => self.admit(&self.path(prefix), &reference.content).await?,
        };
        ensure!(
            node.kind() == "directory"
                && node.entries() == reference.entries
                && node.logical_bytes() == reference.logical_bytes,
            "INVALID_NATIVE_DIRECTORY"
        );
        Ok(node)
    }
    pub async fn apply(
        &self,
        root: Option<&NodeReference>,
        mutations: BTreeMap<String, Option<GraphValue>>,
    ) -> Result<Option<NodeReference>> {
        self.graph.check_current().await?;
        let mut encoded = BTreeMap::new();
        ensure!(mutations.len() <= MAX_MUTATIONS, "RESOURCE_LIMIT");
        for (key, value) in mutations {
            if let Some(value) = &value {
                for node in &value.links {
                    let admitted = self
                        .graph
                        .known_node(node.content())
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_ANCESTRY_REQUIRED"))?;
                    ensure!(
                        admitted.id() == node.id(),
                        "DATASET_GRAPH_ANCESTRY_REQUIRED"
                    );
                }
            }
            encoded.insert(key, value.map(|value| value.encoded()).transpose()?);
        }
        validate_mutations(&encoded)?;
        let result = super::apply(self, root.cloned(), String::new(), encoded).await?;
        self.graph.check_current().await?;
        Ok(result)
    }
    pub async fn lookup(&self, root: &NodeReference, key: &str) -> Result<Option<Value>> {
        ensure!(
            !key.is_empty() && key.len() <= MAX_KEY_BYTES,
            "RESOURCE_LIMIT"
        );
        let result = super::lookup(self, root, key).await?;
        self.graph.check_current().await?;
        result
            .map(|value| decode_value(&value).map(|(value, _)| value))
            .transpose()
    }
    pub(crate) async fn lookup_with_links(
        &self,
        root: &NodeReference,
        key: &str,
    ) -> Result<Option<(Value, Vec<QualifiedNode>)>> {
        ensure!(
            !key.is_empty() && key.len() <= MAX_KEY_BYTES,
            "RESOURCE_LIMIT"
        );
        let Some(encoded) = super::lookup(self, root, key).await? else {
            return Ok(None);
        };
        let (value, links) = decode_value(&encoded)?;
        let mut nodes = Vec::with_capacity(links.len());
        for content in links {
            nodes.push(
                self.graph
                    .known_node(&content)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_ANCESTRY_REQUIRED"))?,
            );
        }
        self.graph.check_current().await?;
        Ok(Some((value, nodes)))
    }
    pub(crate) async fn qualified_root(&self, root: &NodeReference) -> Result<QualifiedNode> {
        self.node_at(root, "").await
    }
}
fn decode_value(value: &Value) -> Result<(Value, Vec<SealedObject>)> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Encoded {
        value: Value,
        links: Vec<SealedObject>,
    }
    let decoded: Encoded = serde_json::from_value(value.clone())?;
    ensure!(decoded.links.len() <= 128, "INVALID_NATIVE_DIRECTORY");
    Ok((decoded.value, decoded.links))
}
#[async_trait]
impl Nodes for GraphDirectory {
    async fn read(&self, reference: &NodeReference, prefix: &str) -> Result<Node> {
        let qualified = self.node_at(reference, prefix).await?;
        let node: Node = serde_json::from_value(self.graph.read_descriptor(&qualified).await?)?;
        ensure!(
            summary(&node, prefix)?
                == (reference.entries, reference.logical_bytes, reference.nodes),
            "INVALID_NATIVE_DIRECTORY"
        );
        if let Node::Leaf { records, .. } = &node {
            for (ordinal, record) in records.iter().enumerate() {
                let (_, links) = decode_value(&record.value)?;
                for (link, content) in links.iter().enumerate() {
                    if self.graph.known_node(content).await?.is_none() {
                        let mut path = self.path(prefix);
                        path.push(format!("entry/{ordinal}/{link}"));
                        self.admit(&path, content).await?;
                    }
                }
            }
        }
        Ok(node)
    }
    async fn write(&self, node: Node, prefix: &str) -> Result<NodeReference> {
        let (entries, logical_bytes, nodes) = summary(&node, prefix)?;
        let mut children = vec![];
        match &node {
            Node::Leaf { records, .. } => {
                for (ordinal, record) in records.iter().enumerate() {
                    let (_, links) = decode_value(&record.value)?;
                    for (link, content) in links.iter().enumerate() {
                        let child =
                            self.graph.known_node(content).await?.ok_or_else(|| {
                                anyhow::anyhow!("DATASET_GRAPH_ANCESTRY_REQUIRED")
                            })?;
                        children.push((format!("entry/{ordinal}/{link}"), child));
                    }
                }
            }
            Node::Branch { children: refs, .. } => {
                for (digit, reference) in refs {
                    children.push((
                        digit.clone(),
                        self.node_at(reference, &format!("{prefix}{digit}")).await?,
                    ));
                }
            }
        }
        ensure!(children.len() <= 256, "RESOURCE_LIMIT");
        let qualified = self
            .graph
            .write_descriptor(
                "directory",
                &serde_json::to_value(&node)?,
                children,
                entries,
                logical_bytes,
            )
            .await?;
        Ok(NodeReference {
            content: qualified.content().clone(),
            entries,
            logical_bytes,
            nodes,
        })
    }
}
