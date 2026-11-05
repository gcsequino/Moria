//! Receiver-side graph custody under a real native Dataset producer.
use crate::{
    authenticated_file::QualifiedFile,
    canonical::canonical_json,
    catalog::DatasetIoGrant,
    storage::{
        Cancellation, LogicalObject, ObjectStore, ObjectVersion, SealedObject, versioned_stream,
    },
};
use anyhow::{Result, ensure};
use futures::StreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

/// No Deserialize implementation: a descriptor becomes a graph node only through real scoped SQL.
#[derive(Clone, Debug)]
pub struct QualifiedNode {
    id: String,
    environment: String,
    dataset: String,
    content: SealedObject,
    physical_version: ObjectVersion,
    kind: String,
    height: u32,
    entries: u64,
    logical_bytes: u64,
    profile: Value,
    physical_fingerprint: String,
    incarnation: Uuid,
}
impl QualifiedNode {
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn content(&self) -> &SealedObject {
        &self.content
    }
    pub fn physical_version(&self) -> &ObjectVersion {
        &self.physical_version
    }
    pub fn kind(&self) -> &str {
        &self.kind
    }
    pub fn height(&self) -> u32 {
        self.height
    }
    pub fn entries(&self) -> u64 {
        self.entries
    }
    pub fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }
    pub fn profile(&self) -> &Value {
        &self.profile
    }
    pub fn physical_fingerprint(&self) -> &str {
        &self.physical_fingerprint
    }
    pub fn incarnation(&self) -> Uuid {
        self.incarnation
    }
}
#[derive(Clone)]
pub struct NativeGraph {
    store: Arc<dyn ObjectStore>,
    grant: Arc<DatasetIoGrant>,
    claims: Value,
    scope: Value,
    cancel: Cancellation,
}
struct NewNode {
    content: SealedObject,
    physical_version: ObjectVersion,
    kind: String,
    entries: u64,
    logical_bytes: u64,
    children: Vec<(String, QualifiedNode)>,
}
impl NativeGraph {
    pub(crate) fn grant(&self) -> &Arc<DatasetIoGrant> {
        &self.grant
    }
    pub(crate) fn profile(&self) -> &Value {
        &self.scope["graphProfile"]
    }
    pub(crate) async fn finish_bootstrap(&self, id: Uuid) -> Result<String> {
        self.check_current().await?;
        let node: String = self
            .grant
            .database()
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_graph_bootstrap_finish($1,$2)",
                &[&self.claims, &id],
            )
            .await?
            .get(0);
        self.check_current().await?;
        Ok(node)
    }
    pub async fn acquire(
        store: Arc<dyn ObjectStore>,
        grant: Arc<DatasetIoGrant>,
        cancel: Cancellation,
    ) -> Result<Self> {
        grant.check_current().await?;
        grant.validate_store(store.as_ref())?;
        cancel.check()?;
        let lease = grant.lease();
        let namespace = grant.namespace();
        let claims = json!({"kind":"execution","session":grant.session().session_hash(),"execution":lease.execution,"task":lease.task,"owner":lease.owner,"fence":lease.fence,"incarnation":lease.incarnation,"warehouse":namespace.warehouse_id,"table":namespace.table_id,"revision":namespace.revision});
        let scope: Value = grant
            .database()
            .get()
            .await?
            .query_one("SELECT moria.runtime_dataset_graph_scope($1)", &[&claims])
            .await?
            .get(0);
        Ok(Self {
            store,
            grant,
            claims,
            scope,
            cancel,
        })
    }
    pub async fn check_current(&self) -> Result<()> {
        self.cancel.check()?;
        self.grant.check_current().await?;
        let scope: Value = self
            .grant
            .database()
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_graph_scope($1)",
                &[&self.claims],
            )
            .await?
            .get(0);
        ensure!(scope == self.scope, "AUTHORIZATION_CHANGED");
        Ok(())
    }
    fn check_node(&self, node: &QualifiedNode) -> Result<()> {
        ensure!(
            node.environment == self.scope["environment"]
                && node.dataset == self.scope["resource"]
                && node.profile == self.scope["graphProfile"]
                && node.physical_fingerprint == self.scope["physicalFingerprint"]
                && node.incarnation == self.grant.lease().incarnation,
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        Ok(())
    }
    async fn register(&self, input: NewNode) -> Result<QualifiedNode> {
        self.check_current().await?;
        ensure!(input.children.len() <= 256, "RESOURCE_LIMIT");
        for (_, child) in &input.children {
            self.check_node(child)?;
        }
        let height = input
            .children
            .iter()
            .map(|(_, node)| node.height + 1)
            .max()
            .unwrap_or(0);
        let payload = json!({"content":input.content,"physicalVersion":input.physical_version,"kind":input.kind,"height":height,"entries":input.entries,"logicalBytes":input.logical_bytes,"edges":input.children.iter().map(|(label,node)|json!({"label":label,"node":node.id})).collect::<Vec<_>>()});
        let id: String = self
            .grant
            .database()
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_graph_register($1,$2)",
                &[&self.claims, &payload],
            )
            .await?
            .get(0);
        self.check_current().await?;
        Ok(QualifiedNode {
            id,
            environment: self.scope["environment"].as_str().unwrap().into(),
            dataset: self.scope["resource"].as_str().unwrap().into(),
            content: input.content,
            physical_version: input.physical_version,
            kind: input.kind,
            height,
            entries: input.entries,
            logical_bytes: input.logical_bytes,
            profile: self.scope["graphProfile"].clone(),
            physical_fingerprint: self.scope["physicalFingerprint"]
                .as_str()
                .unwrap()
                .to_owned(),
            incarnation: self.grant.lease().incarnation,
        })
    }
    /// The byte proof must have been constructed by an actual native issued-file read.
    pub(crate) async fn certify_retained_member(
        &self,
        content: SealedObject,
    ) -> Result<QualifiedFile> {
        self.check_current().await?;
        crate::authenticated_file::certify_retained_member(
            self.store.clone(),
            self.grant.clone(),
            content,
            &self.claims,
            self.cancel.clone(),
        )
        .await
    }
    pub async fn register_file(&self, file: &QualifiedFile) -> Result<QualifiedNode> {
        ensure!(
            file.physical_fingerprint() == self.scope["physicalFingerprint"].as_str().unwrap()
                && file.incarnation() == self.grant.lease().incarnation,
            "BINDING_MISMATCH"
        );
        self.register(NewNode {
            content: file.certificate().content().clone(),
            physical_version: file.physical_version().clone(),
            kind: "content".into(),
            entries: 1,
            logical_bytes: file.certificate().content().size,
            children: vec![],
        })
        .await
    }
    /// This crate-private encoder is used only by closed native directory/root/index codecs.
    pub(crate) async fn write_descriptor(
        &self,
        kind: &str,
        value: &Value,
        children: Vec<(String, QualifiedNode)>,
        entries: u64,
        logical_bytes: u64,
    ) -> Result<QualifiedNode> {
        self.check_current().await?;
        ensure!(
            ["root", "directory", "keyRun", "partset"].contains(&kind),
            "DATASET_GRAPH_INVALID"
        );
        let encoded = canonical_json(value)?.into_bytes();
        ensure!(
            !encoded.is_empty() && encoded.len() <= 16 * 1024 * 1024,
            "RESOURCE_LIMIT"
        );
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
        let physical_version = self
            .store
            .version(&content.object, self.grant.as_ref(), &self.cancel)
            .await?;
        self.verify_bytes(&content, &physical_version).await?;
        self.register(NewNode {
            content,
            physical_version,
            kind: kind.into(),
            entries,
            logical_bytes,
            children,
        })
        .await
    }
    pub(crate) async fn write_index_bytes(
        &self,
        bytes: &[u8],
    ) -> Result<(QualifiedFile, QualifiedNode)> {
        self.check_current().await?;
        ensure!(
            !bytes.is_empty() && bytes.len() <= 16 * 1024 * 1024,
            "RESOURCE_LIMIT"
        );
        let ns = self.grant.namespace();
        let object =
            LogicalObject::new(&ns.warehouse_id, &ns.table_id, &Uuid::new_v4().to_string())?;
        let mut writer = self
            .store
            .begin_object(
                object,
                bytes.len() as u64,
                format!("sha256:{:x}", Sha256::digest(bytes)),
                self.grant.clone(),
                self.cancel.clone(),
            )
            .await?;
        for chunk in bytes.chunks(1024 * 1024) {
            writer.append(chunk).await?;
        }
        let content = writer.seal().await?;
        let file = crate::authenticated_file::certify_new_index_file(
            self.store.clone(),
            self.grant.clone(),
            content,
            self.cancel.clone(),
        )
        .await?;
        let certificate = self.register_certificate(&file).await?;
        Ok((file, certificate))
    }
    pub async fn register_certificate(&self, file: &QualifiedFile) -> Result<QualifiedNode> {
        let content_node = self.register_file(file).await?;
        // The private typed file proof binds full SHA256, chunk hashes and exact provider version.
        // No caller-supplied certificate JSON can enter this path.
        let value = serde_json::to_value(file.certificate())?;
        self.write_certificate_descriptor(&value, content_node)
            .await
    }
    async fn write_certificate_descriptor(
        &self,
        value: &Value,
        content_node: QualifiedNode,
    ) -> Result<QualifiedNode> {
        self.check_node(&content_node)?;
        let encoded = canonical_json(value)?.into_bytes();
        ensure!(encoded.len() <= 64 * 1024, "RESOURCE_LIMIT");
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
        writer.append(&encoded).await?;
        let content = writer.seal().await?;
        let physical_version = self
            .store
            .version(&content.object, self.grant.as_ref(), &self.cancel)
            .await?;
        self.verify_bytes(&content, &physical_version).await?;
        self.register(NewNode {
            content,
            physical_version,
            kind: "certificate".into(),
            entries: 1,
            logical_bytes: content_node.logical_bytes,
            children: vec![("file".into(), content_node)],
        })
        .await
    }
    async fn verify_bytes(&self, content: &SealedObject, version: &ObjectVersion) -> Result<()> {
        ensure!(
            version.size == content.size && content.size <= 16 * 1024 * 1024,
            "CONTENT_MISMATCH"
        );
        let mut stream = versioned_stream(
            self.store.clone(),
            content.object.clone(),
            version.clone(),
            self.grant.clone(),
            self.cancel.clone(),
        )
        .await?;
        let mut total = 0u64;
        let mut hash = Sha256::new();
        while let Some(frame) = stream.next().await {
            let frame = frame?;
            total = total
                .checked_add(frame.len() as u64)
                .filter(|size| *size <= content.size)
                .ok_or_else(|| anyhow::anyhow!("CONTENT_MISMATCH"))?;
            hash.update(&frame);
        }
        ensure!(
            total == content.size && format!("sha256:{:x}", hash.finalize()) == content.sha256,
            "CONTENT_MISMATCH"
        );
        self.check_current().await
    }
    pub async fn read_descriptor(&self, node: &QualifiedNode) -> Result<Value> {
        self.check_node(node)?;
        self.check_current().await?;
        ensure!(node.content.size <= 16 * 1024 * 1024, "RESOURCE_LIMIT");
        let mut stream = versioned_stream(
            self.store.clone(),
            node.content.object.clone(),
            node.physical_version.clone(),
            self.grant.clone(),
            self.cancel.clone(),
        )
        .await?;
        let mut encoded = Vec::with_capacity(node.content.size as usize);
        let mut hash = Sha256::new();
        while let Some(frame) = stream.next().await {
            let frame = frame?;
            ensure!(
                encoded
                    .len()
                    .checked_add(frame.len())
                    .is_some_and(|len| len <= node.content.size as usize),
                "CONTENT_MISMATCH"
            );
            hash.update(&frame);
            encoded.extend_from_slice(&frame);
        }
        ensure!(
            encoded.len() as u64 == node.content.size
                && format!("sha256:{:x}", hash.finalize()) == node.content.sha256,
            "CONTENT_MISMATCH"
        );
        let value: Value = serde_json::from_slice(&encoded)?;
        ensure!(
            canonical_json(&value)?.as_bytes() == encoded,
            "NONCANONICAL_NATIVE_SNAPSHOT_OBJECT"
        );
        self.check_current().await?;
        Ok(value)
    }
    fn admitted_node(&self, value: Value) -> Result<QualifiedNode> {
        ensure!(
            value["profile"] == self.scope["graphProfile"],
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        Ok(QualifiedNode {
            id: value["node"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_INVALID"))?
                .into(),
            environment: self.scope["environment"].as_str().unwrap().into(),
            dataset: self.scope["resource"].as_str().unwrap().into(),
            content: serde_json::from_value(value["content"].clone())?,
            physical_version: serde_json::from_value(value["physicalVersion"].clone())?,
            kind: value["kind"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("DATASET_GRAPH_INVALID"))?
                .into(),
            height: serde_json::from_value(value["height"].clone())?,
            entries: serde_json::from_value(value["entries"].clone())?,
            logical_bytes: serde_json::from_value(value["logicalBytes"].clone())?,
            profile: value["profile"].clone(),
            physical_fingerprint: self.scope["physicalFingerprint"].as_str().unwrap().into(),
            incarnation: self.grant.lease().incarnation,
        })
    }
    pub(crate) async fn known_node(&self, wanted: &SealedObject) -> Result<Option<QualifiedNode>> {
        self.check_current().await?;
        let wanted = serde_json::to_value(wanted)?;
        let value: Option<Value> = self
            .grant
            .database()
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_graph_known($1,$2)",
                &[&self.claims, &wanted],
            )
            .await?
            .get(0);
        let node = value.map(|value| self.admitted_node(value)).transpose()?;
        self.check_current().await?;
        Ok(node)
    }
    pub(crate) async fn admit_head(
        &self,
        path: &[String],
        wanted: &SealedObject,
    ) -> Result<QualifiedNode> {
        self.check_current().await?;
        let wanted = serde_json::to_value(wanted)?;
        let value: Value = self
            .grant
            .database()
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_graph_reuse_head($1,$2,$3)",
                &[&self.claims, &path, &wanted],
            )
            .await?
            .get(0);
        let node = self.admitted_node(value)?;
        self.check_current().await?;
        Ok(node)
    }
    pub(crate) async fn admit_task(
        &self,
        task: Uuid,
        path: &[String],
        wanted: &SealedObject,
    ) -> Result<QualifiedNode> {
        self.check_current().await?;
        let wanted = serde_json::to_value(wanted)?;
        let value: Value = self
            .grant
            .database()
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_graph_reuse_task($1,$2,$3,$4)",
                &[&self.claims, &task, &path, &wanted],
            )
            .await?
            .get(0);
        let node = self.admitted_node(value)?;
        self.check_current().await?;
        Ok(node)
    }
    pub(crate) async fn child(&self, parent: &QualifiedNode, label: &str) -> Result<QualifiedNode> {
        self.check_node(parent)?;
        self.check_current().await?;
        let value: Value = self
            .grant
            .database()
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_graph_child($1,$2,$3)",
                &[&self.claims, &parent.id(), &label],
            )
            .await?
            .get(0);
        let child = self.admitted_node(value)?;
        self.check_current().await?;
        Ok(child)
    }
    pub(crate) async fn file_proof(
        &self,
        certificate: &QualifiedNode,
    ) -> Result<ProducerFileProof> {
        self.check_node(certificate)?;
        ensure!(
            certificate.kind() == "certificate",
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let proof: crate::authenticated_file::FileCertificate =
            serde_json::from_value(self.read_descriptor(certificate).await?)?;
        let file = self.child(certificate, "file").await?;
        ensure!(
            file.kind() == "content" && file.content() == proof.content(),
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        Ok(ProducerFileProof {
            certificate: proof,
            physical_version: file.physical_version,
            physical_fingerprint: file.physical_fingerprint,
            incarnation: file.incarnation,
        })
    }
    pub async fn register_task_root(&self, node: &QualifiedNode) -> Result<()> {
        self.check_node(node)?;
        self.check_current().await?;
        self.grant
            .database()
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_graph_task_root($1,$2)",
                &[&self.claims, &node.id],
            )
            .await?;
        self.check_current().await
    }
    pub(crate) async fn attach_candidate(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        stage: Uuid,
        candidate: Uuid,
    ) -> Result<()> {
        tx.query_one(
            "SELECT moria.runtime_dataset_graph_attach($1,$2,$3)",
            &[&self.claims, &stage, &candidate],
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn begin_stage(&self, id: Uuid, node: &QualifiedNode) -> Result<()> {
        self.check_node(node)?;
        self.check_current().await?;
        ensure!(self.grant.lease().task.is_none(), "COORDINATOR_REQUIRED");
        self.grant
            .database()
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_graph_stage($1,$2,$3)",
                &[&self.claims, &id, &node.id],
            )
            .await?;
        self.check_current().await
    }
    pub(crate) async fn advance(&self, id: Uuid, maximum: i32) -> Result<Value> {
        self.check_current().await?;
        let value: Value = self
            .grant
            .database()
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_graph_advance($1,$2,$3)",
                &[&self.claims, &id, &maximum],
            )
            .await?
            .get(0);
        self.check_current().await?;
        Ok(value)
    }
}

/// Constructed only from an actual admitted parent and its immutable, qualified file edge.
pub(crate) struct ProducerFileProof {
    certificate: crate::authenticated_file::FileCertificate,
    physical_version: ObjectVersion,
    physical_fingerprint: String,
    incarnation: Uuid,
}
impl ProducerFileProof {
    pub(crate) fn into_parts(
        self,
    ) -> (
        crate::authenticated_file::FileCertificate,
        ObjectVersion,
        String,
        Uuid,
    ) {
        (
            self.certificate,
            self.physical_version,
            self.physical_fingerprint,
            self.incarnation,
        )
    }
}
#[async_trait::async_trait]
impl crate::storage::VerifiedVersionAuthority for NativeGraph {
    async fn verified_version(&self, content: &SealedObject) -> Result<ObjectVersion> {
        self.check_current().await?;
        self.grant.validate_store(self.store.as_ref())?;
        let wanted = serde_json::to_value(content)?;
        let state: Value = self
            .grant
            .database()
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_graph_bootstrap_member($1,$2)",
                &[&self.claims, &wanted],
            )
            .await?
            .get(0);
        ensure!(
            state["content"] == wanted
                && state["physicalFingerprint"] == self.store.physical_fingerprint()?
                && state["incarnation"] == self.grant.lease().incarnation.to_string(),
            "DATASET_GRAPH_ANCESTRY_REQUIRED"
        );
        let version: ObjectVersion = serde_json::from_value(state["physicalVersion"].clone())?;
        ensure!(version.size == content.size, "CONTENT_MISMATCH");
        self.check_current().await?;
        Ok(version)
    }
}
