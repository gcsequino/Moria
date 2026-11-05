//! Actual task-owned graph custody for immutable replacement partsets.
use super::*;
use crate::{
    dataset_graph::{NativeGraph, QualifiedNode},
    native_snapshot_v3::LogicalFile,
    persistent_directory::{
        NodeReference,
        graph::{GraphDirectory, GraphValue},
    },
};

pub(super) struct GraphOutput {
    graph: NativeGraph,
    files: GraphDirectory,
    files_root: Option<NodeReference>,
    last_page: Option<QualifiedNode>,
    pages: GraphDirectory,
    pages_root: Option<NodeReference>,
}
impl GraphOutput {
    pub(super) async fn open(
        store: Arc<dyn ObjectStore>,
        grant: Arc<DatasetIoGrant>,
        cancel: Cancellation,
    ) -> Result<Self> {
        ensure!(
            grant.is_managed() && grant.lease().task.is_some(),
            "TASK_REQUIRED"
        );
        let graph = NativeGraph::acquire(store, grant, cancel).await?;
        let files = GraphDirectory::new(graph.clone(), vec!["files".into()])?;
        let pages = GraphDirectory::new(graph.clone(), vec!["pages".into()])?;
        Ok(Self {
            graph,
            files,
            files_root: None,
            last_page: None,
            pages,
            pages_root: None,
        })
    }
    pub(super) async fn file(
        &mut self,
        name: &str,
        content: &SealedObject,
        store: Arc<dyn ObjectStore>,
        grant: Arc<DatasetIoGrant>,
        cancel: Cancellation,
    ) -> Result<()> {
        let qualified =
            crate::authenticated_file::certify_new_file(store, grant, content.clone(), cancel)
                .await?;
        let certificate = self.graph.register_certificate(&qualified).await?;
        let value = GraphValue::new(
            serde_json::to_value(LogicalFile {
                name: name.into(),
                content: content.clone(),
            })?,
            vec![certificate],
        )?;
        self.files_root = self
            .files
            .apply(
                self.files_root.as_ref(),
                BTreeMap::from([(name.into(), Some(value))]),
            )
            .await?;
        Ok(())
    }
    pub(super) async fn page(&mut self, page: &PartPage) -> Result<SealedObject> {
        ensure!(
            page.previous.as_ref() == self.last_page.as_ref().map(QualifiedNode::content),
            "OUTPUT_INVALID"
        );
        let root = self
            .graph
            .write_descriptor(
                "partset",
                &serde_json::to_value(page)?,
                vec![],
                page.parts.len() as u64,
                page.parts.iter().map(|part| part.content.size).sum(),
            )
            .await?;
        let value = GraphValue::new(json!({"ordinal":page.ordinal}), vec![root.clone()])?;
        self.pages_root = self
            .pages
            .apply(
                self.pages_root.as_ref(),
                BTreeMap::from([(format!("{:016x}", page.ordinal), Some(value))]),
            )
            .await?;
        let content = root.content().clone();
        self.last_page = Some(root);
        Ok(content)
    }
    pub(super) fn files_root(&self) -> Option<NodeReference> {
        self.files_root.clone()
    }
    pub(super) fn pages_root(&self) -> Option<NodeReference> {
        self.pages_root.clone()
    }
    pub(super) async fn finish(self, root: &PartRoot) -> Result<SealedObject> {
        ensure!(
            root.version == 3
                && root.graph_files == self.files_root
                && root.graph_pages == self.pages_root
                && root.last_page.as_ref() == self.last_page.as_ref().map(QualifiedNode::content)
                && (root.parts == 0) == self.files_root.is_none(),
            "OUTPUT_INVALID"
        );
        let mut children = Vec::new();
        if let Some(reference) = self.files_root {
            children.push(("files".into(), self.files.qualified_root(&reference).await?));
        }
        if let Some(reference) = self.pages_root {
            children.push(("pages".into(), self.pages.qualified_root(&reference).await?));
        }
        let node = self
            .graph
            .write_descriptor(
                "partset",
                &serde_json::to_value(root)?,
                children,
                root.rows,
                0,
            )
            .await?;
        self.graph.register_task_root(&node).await?;
        Ok(node.content().clone())
    }
}
