//! Read-only receiver custody derived from an actual accepted Dataset pin or indexing build.
use crate::{
    authenticated_file::{AuthenticatedFileReader, FileCertificate, QualifiedFile},
    catalog::DatasetIoGrant,
    database::Database,
    io::Scope,
    storage::{
        Cancellation, IndexNamespace, LogicalObject, ObjectReader, ObjectVersion, ReadAuthority,
        SealedObject, versioned_stream,
    },
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{ops::Range, sync::Arc};
use uuid::Uuid;

#[derive(Clone, Copy)]
pub enum ReadBasis {
    Input,
    ExpectedHead,
}
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodeState {
    node: String,
    content: SealedObject,
    physical_version: ObjectVersion,
    kind: String,
    height: u32,
    entries: u64,
    logical_bytes: u64,
    profile: Value,
}
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolvedNode {
    identity: Value,
    node: NodeState,
}
/// Private fields prevent imported JSON, qualifications or arbitrary node IDs becoming read custody.
#[derive(Clone, Debug)]
pub struct ReadableNode {
    context: Value,
    path: Vec<String>,
    state: NodeState,
}
impl ReadableNode {
    pub fn content(&self) -> &SealedObject {
        &self.state.content
    }
    pub fn kind(&self) -> &str {
        &self.state.kind
    }
    pub fn entries(&self) -> u64 {
        self.state.entries
    }
    pub fn logical_bytes(&self) -> u64 {
        self.state.logical_bytes
    }
}
#[derive(Clone, Debug)]
pub struct GraphReadContext {
    db: Database,
    claims: Value,
    identity: Value,
    inner: Arc<dyn ObjectReader>,
    cancel: Cancellation,
}
impl GraphReadContext {
    pub(crate) fn profile(&self) -> &Value {
        &self.identity["profile"]
    }
    pub(crate) fn snapshot_id(&self) -> &Value {
        &self.identity["snapshot"]
    }
    pub(crate) async fn check_node(&self, node: &ReadableNode) -> Result<()> {
        self.current(node).await
    }
    /// Uses the actual execution's immutable input or expected-head declaration; never creates a producer.
    pub async fn execution(
        inner: Arc<dyn ObjectReader>,
        grant: Arc<DatasetIoGrant>,
        basis: ReadBasis,
        cancel: Cancellation,
    ) -> Result<(Self, ReadableNode)> {
        grant.check_current().await?;
        grant.validate_store(inner.as_ref())?;
        let lease = grant.lease();
        let ns = grant.namespace();
        let claims = json!({"kind":"execution","basis":match basis {ReadBasis::Input=>"input",ReadBasis::ExpectedHead=>"expectedHead"},"session":grant.session().session_hash(),"execution":lease.execution,"task":lease.task,"owner":lease.owner,"fence":lease.fence,"incarnation":lease.incarnation,"warehouse":ns.warehouse_id,"table":ns.table_id,"revision":ns.revision});
        Self::open(grant.database().clone(), inner, claims, cancel).await
    }
    /// Uses the exact immutable source revision/snapshot/root of a real live indexing build.
    pub async fn indexing(
        inner: Arc<dyn ObjectReader>,
        scope: Scope,
        namespace: IndexNamespace,
        cancel: Cancellation,
    ) -> Result<(Self, ReadableNode)> {
        scope.check().await?;
        let claims = json!({"kind":"indexing","build":scope.build,"task":scope.task,"owner":scope.owner,"fence":scope.fence,"warehouse":namespace.warehouse_id,"table":namespace.table_id,"revision":namespace.revision});
        Self::open(scope.db, inner, claims, cancel).await
    }
    async fn open(
        db: Database,
        inner: Arc<dyn ObjectReader>,
        claims: Value,
        cancel: Cancellation,
    ) -> Result<(Self, ReadableNode)> {
        cancel.check()?;
        let resolved = Self::resolve(&db, &claims, &[]).await?;
        ensure!(
            resolved.node.kind == "root"
                && inner.physical_fingerprint()? == resolved.identity["physicalFingerprint"],
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let context = Self {
            db,
            claims,
            identity: resolved.identity.clone(),
            inner,
            cancel,
        };
        let root = ReadableNode {
            context: resolved.identity,
            path: vec![],
            state: resolved.node,
        };
        context.current(&root).await?;
        Ok((context, root))
    }
    async fn resolve(db: &Database, claims: &Value, path: &[String]) -> Result<ResolvedNode> {
        ensure!(
            path.len() <= 128
                && path.iter().all(|s| !s.is_empty()
                    && s.len() <= 96
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_./:-".contains(&b))),
            "DATASET_GRAPH_INVALID"
        );
        let value: Value = db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_graph_read($1,$2)",
                &[claims, &path],
            )
            .await?
            .get(0);
        let resolved: ResolvedNode = serde_json::from_value(value)?;
        ensure!(
            resolved.node.profile == resolved.identity["profile"]
                && resolved.node.height <= 128
                && resolved.node.content.size > 0
                && resolved.node.content.size <= 16 * 1024 * 1024
                && resolved.node.physical_version.size == resolved.node.content.size,
            "DATASET_GRAPH_INVALID"
        );
        Ok(resolved)
    }
    fn belongs(&self, node: &ReadableNode) -> Result<()> {
        ensure!(
            node.context == self.identity
                && self.inner.physical_fingerprint()? == self.identity["physicalFingerprint"],
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        Ok(())
    }
    async fn current(&self, node: &ReadableNode) -> Result<()> {
        self.cancel.check()?;
        self.belongs(node)?;
        let resolved = Self::resolve(&self.db, &self.claims, &node.path).await?;
        ensure!(
            resolved.identity == self.identity && resolved.node == node.state,
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        self.cancel.check()
    }
    pub async fn child(&self, parent: &ReadableNode, label: &str) -> Result<ReadableNode> {
        self.current(parent).await?;
        let mut path = parent.path.clone();
        path.push(label.into());
        let resolved = Self::resolve(&self.db, &self.claims, &path).await?;
        ensure!(
            resolved.identity == self.identity,
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let node = ReadableNode {
            context: resolved.identity,
            path,
            state: resolved.node,
        };
        self.current(&node).await?;
        Ok(node)
    }
    /// Full descriptor bytes are hashed before any parser or caller can consume them.
    pub async fn descriptor(&self, node: &ReadableNode) -> Result<Value> {
        self.current(node).await?;
        ensure!(node.kind() != "content", "DATASET_GRAPH_INVALID");
        let authority = Arc::new(NodeAuthority {
            context: self.clone(),
            node: node.clone(),
        });
        let mut frames = versioned_stream(
            self.inner.clone(),
            node.state.content.object.clone(),
            node.state.physical_version.clone(),
            authority,
            self.cancel.clone(),
        )
        .await?;
        let mut bytes = Vec::with_capacity(node.state.content.size as usize);
        let mut hash = Sha256::new();
        while let Some(frame) = frames.next().await {
            let frame = frame?;
            ensure!(
                bytes
                    .len()
                    .checked_add(frame.len())
                    .is_some_and(|n| n <= node.state.content.size as usize),
                "CONTENT_MISMATCH"
            );
            hash.update(&frame);
            bytes.extend_from_slice(&frame);
        }
        ensure!(
            bytes.len() as u64 == node.state.content.size
                && format!("sha256:{:x}", hash.finalize()) == node.state.content.sha256,
            "CONTENT_MISMATCH"
        );
        let value: Value = serde_json::from_slice(&bytes)?;
        ensure!(
            crate::canonical::canonical_json(&value)?.as_bytes() == bytes,
            "NONCANONICAL_NATIVE_SNAPSHOT_OBJECT"
        );
        self.current(node).await?;
        Ok(value)
    }
    pub async fn file(&self, certificate_node: &ReadableNode) -> Result<GraphFile> {
        ensure!(
            certificate_node.kind() == "certificate",
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let certificate: FileCertificate =
            serde_json::from_value(self.descriptor(certificate_node).await?)?;
        let file_node = self.child(certificate_node, "file").await?;
        ensure!(
            file_node.kind() == "content" && certificate.content() == file_node.content(),
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let proof = GraphCertificateProof {
            certificate,
            physical_version: file_node.state.physical_version.clone(),
            physical_fingerprint: self.identity["physicalFingerprint"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_INVALID"))?
                .into(),
            incarnation: self.identity["incarnation"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_INVALID"))?
                .parse()?,
        };
        let reader = AuthenticatedFileReader::from_qualified_file(
            self.inner.clone(),
            QualifiedFile::from_graph_proof(proof)?,
        )?;
        self.current(certificate_node).await?;
        self.current(&file_node).await?;
        Ok(GraphFile {
            reader,
            authority: Arc::new(NodeAuthority {
                context: self.clone(),
                node: file_node,
            }),
            cancel: self.cancel.clone(),
        })
    }
}
/// Constructed only after hashing a receiver-qualified certificate descriptor on an actual root path.
pub(crate) struct GraphCertificateProof {
    certificate: FileCertificate,
    physical_version: ObjectVersion,
    physical_fingerprint: String,
    incarnation: Uuid,
}
impl GraphCertificateProof {
    pub(crate) fn into_parts(self) -> (FileCertificate, ObjectVersion, String, Uuid) {
        (
            self.certificate,
            self.physical_version,
            self.physical_fingerprint,
            self.incarnation,
        )
    }
}
#[derive(Clone, Debug)]
struct NodeAuthority {
    context: GraphReadContext,
    node: ReadableNode,
}
#[async_trait]
impl ReadAuthority for NodeAuthority {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        ensure!(
            object == &self.node.state.content.object,
            "BINDING_MISMATCH"
        );
        self.context.current(&self.node).await
    }
}
#[derive(Clone, Debug)]
pub struct GraphFile {
    reader: AuthenticatedFileReader,
    authority: Arc<NodeAuthority>,
    cancel: Cancellation,
}
impl GraphFile {
    pub(crate) fn window(&self) -> crate::storage::ObjectReadWindow {
        crate::storage::ObjectReadWindow::new(
            self.content().object.clone(),
            Arc::new(self.reader.clone()),
            self.authority.clone(),
            self.cancel.clone(),
        )
    }
    pub async fn check_current(&self) -> Result<()> {
        self.authority.check_read(&self.content().object).await
    }
    pub fn content(&self) -> &SealedObject {
        self.authority.node.content()
    }
    pub async fn range(&self, range: Range<u64>) -> Result<Bytes> {
        self.reader
            .range(
                &self.content().object,
                range,
                self.authority.as_ref(),
                &self.cancel,
            )
            .await
    }
    pub async fn stream(&self) -> Result<BoxStream<'static, Result<Bytes>>> {
        self.reader
            .read_object(
                self.content().object.clone(),
                self.authority.clone(),
                self.cancel.clone(),
            )
            .await
    }
}
