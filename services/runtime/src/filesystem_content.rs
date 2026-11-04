use super::{ENTRY, Entry, NativeFilesystem, identifier, key, name, revision};
use crate::{
    authority::{Authority, VerifiedSession},
    canonical::fingerprint,
    database::Database,
    sandbox::{HttpCall, SandboxContext},
    serving::Admission,
    storage::{
        self, Cancellation, IndexNamespace, LogicalObject, ObjectWrite, ReadAuthority,
        SealedObject, WriteAuthority,
    },
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fmt, sync::Arc};
use tokio_postgres::Client;
use uuid::Uuid;

const FILE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileContent {
    pub location: LogicalObject,
    pub digest: String,
    pub size: u64,
    pub media_type: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "op",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum FileWrite {
    Create {
        parent_id: Uuid,
        expected_parent_revision: String,
        name: String,
        content: ContentDeclaration,
    },
    Write {
        id: Uuid,
        expected_revision: String,
        content: ContentDeclaration,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContentDeclaration {
    pub digest: String,
    pub size: u64,
    pub media_type: String,
}
impl FileWrite {
    fn target(&self) -> Uuid {
        match self {
            Self::Create { parent_id, .. } => *parent_id,
            Self::Write { id, .. } => *id,
        }
    }
    fn content(&self) -> &ContentDeclaration {
        match self {
            Self::Create { content, .. } | Self::Write { content, .. } => content,
        }
    }
    fn expected(&self) -> &str {
        match self {
            Self::Create {
                expected_parent_revision,
                ..
            } => expected_parent_revision,
            Self::Write {
                expected_revision, ..
            } => expected_revision,
        }
    }
    fn create(&self) -> bool {
        matches!(self, Self::Create { .. })
    }
    fn validate(&self) -> Result<()> {
        revision(self.expected())?;
        if let Self::Create {
            name: entry_name, ..
        } = self
        {
            name(entry_name)?;
        }
        let content = self.content();
        ensure!(
            content.size <= FILE_BYTES
                && content.digest.len() == 71
                && content.digest.starts_with("sha256:")
                && content.digest[7..]
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
            "CONTENT_MISMATCH"
        );
        let parts = content.media_type.split('/').collect::<Vec<_>>();
        ensure!(
            content.media_type.len() <= 128
                && parts.len() == 2
                && parts.iter().all(|part| !part.is_empty()
                    && part.bytes().all(|c| c.is_ascii_lowercase()
                        || c.is_ascii_digit()
                        || b"!#$&^_.+-".contains(&c))),
            "FILESYSTEM_INVALID"
        );
        Ok(())
    }
    fn change(&self, content: Value) -> Value {
        match self {
            Self::Create {
                parent_id,
                expected_parent_revision,
                name,
                ..
            } => {
                json!({"op":"create","parentId":parent_id,"expectedParentRevision":expected_parent_revision,"name":name,"kind":"file","content":content})
            }
            Self::Write {
                id,
                expected_revision,
                ..
            } => {
                json!({"op":"write","id":id,"expectedRevision":expected_revision,"content":content})
            }
        }
    }
    fn request_hash(&self) -> Result<String> {
        fingerprint(&json!([self.change(json!(self.content()))]))
    }
}
fn namespace() -> Result<IndexNamespace> {
    let revision = std::env::var("MORIA_NATIVE_FILESYSTEM_STORAGE_REVISION")
        .map_err(|_| anyhow::anyhow!("UNSUPPORTED_CAPABILITY"))?;
    ensure!(
        !revision.is_empty()
            && revision.len() <= 128
            && revision
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c)),
        "FILESYSTEM_INVALID"
    );
    Ok(IndexNamespace {
        warehouse_id: "filesystem".into(),
        table_id: "content".into(),
        revision,
    })
}
/// Operator configuration pins a physical identity without issuing byte authority or resolving credentials.
pub async fn configure_storage(db: crate::database::Database) -> Result<()> {
    let namespace = namespace()?;
    let binding = fingerprint(&storage::configured_physical_binding(&namespace)?)?;
    let admission = crate::serving::Admission::global_filesystem_configuration(&db).await?;
    let result=async {
        let client=admission.client();
        client.execute("INSERT INTO moria.fs_storage_bindings(warehouse_id,table_id,binding_hash) VALUES('filesystem','content',$1) ON CONFLICT DO NOTHING",&[&binding]).await?;
        ensure!(client.query_one("SELECT binding_hash FROM moria.fs_storage_bindings WHERE warehouse_id='filesystem' AND table_id='content' FOR SHARE",&[]).await?.get::<_,String>(0)==binding,"FILESYSTEM_CONFLICT");
        Ok(())
    }.await;
    admission.complete(result).await
}
async fn physical(
    client: &Client,
    namespace: &IndexNamespace,
    expected: Option<&str>,
) -> Result<String> {
    let current = fingerprint(&storage::configured_physical_binding(namespace)?)?;
    if let Some(expected) = expected {
        ensure!(expected == current, "FILESYSTEM_CONFLICT");
    }
    let row=client.query_opt("SELECT binding_hash FROM moria.fs_storage_bindings WHERE warehouse_id='filesystem' AND table_id='content'",&[]).await?.ok_or_else(||anyhow::anyhow!("FILESYSTEM_CONFLICT"))?;
    ensure!(row.get::<_, String>(0) == current, "FILESYSTEM_CONFLICT");
    Ok(current)
}
async fn target(
    client: &Client,
    session: &VerifiedSession,
    request: &FileWrite,
    consumed: &[String],
    expected: bool,
) -> Result<(Entry, Vec<String>)> {
    let mut labels = NativeFilesystem::access(client, session, request.target(), "read").await?;
    let destination = NativeFilesystem::access(
        client,
        session,
        request.target(),
        if request.create() { "create" } else { "write" },
    )
    .await?;
    ensure!(
        consumed.iter().all(|mark| destination.contains(mark)),
        "MARKING_FLOW"
    );
    labels.extend(destination);
    labels.sort();
    labels.dedup();
    let entry = NativeFilesystem::entry(client, request.target(), false).await?;
    ensure!(
        entry.kind
            == if request.create() {
                "directory"
            } else {
                "file"
            },
        "FILESYSTEM_INVALID"
    );
    if expected {
        ensure!(entry.revision == request.expected(), "FILESYSTEM_CONFLICT");
    }
    Ok((entry, labels))
}
async fn metadata_evidence(
    client: &Client,
    session: &VerifiedSession,
    request: &super::FilesystemRequest<'_>,
    value: &Value,
) -> Result<(
    super::FilesystemEvidence,
    Vec<crate::sandbox::ConsumedMarking>,
)> {
    let (evidence, labels) = NativeFilesystem::evidence(client, session, request, value).await?;
    Ok((
        evidence,
        labels
            .into_iter()
            .map(|marking| crate::sandbox::ConsumedMarking {
                environment: session.environment().into(),
                marking,
            })
            .collect(),
    ))
}

#[derive(Clone)]
struct UploadAuthority {
    db: Database,
    session: VerifiedSession,
    namespace: IndexNamespace,
    physical: String,
    upload: Uuid,
    owner: Uuid,
    fence: i64,
    object: LogicalObject,
    request: FileWrite,
    consumed: Vec<String>,
    context: Option<SandboxContext>,
    call: Option<HttpCall>,
    compiler_source: Option<super::source::CompilerUploadSource>,
}
impl fmt::Debug for UploadAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FilesystemUploadAuthority")
    }
}
impl UploadAuthority {
    fn consumed(&self) -> Vec<String> {
        self.consumed
            .iter()
            .cloned()
            .chain(
                self.context
                    .as_ref()
                    .into_iter()
                    .flat_map(|context| context.consumed_markings())
                    .map(|mark| mark.marking),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
    async fn check_in(&self, client: &Client, renew: bool) -> Result<()> {
        self.check_in_mode(client, renew, true).await
    }
    async fn check_in_mode(&self, client: &Client, renew: bool, expected: bool) -> Result<()> {
        let snapshot = Authority::current(client, &self.session).await?;
        if let Some(context) = &self.context {
            context.check_in(client).await?;
        }
        let row=client.query_opt("SELECT source_markings FROM moria.native_filesystem_uploads WHERE id=$1 AND owner=$2 AND fence=$3 AND state IN ('writing','sealed') AND incarnation=(SELECT incarnation FROM moria.indexing_instance) AND NOT EXISTS(SELECT 1 FROM moria.native_filesystem_upload_tombstones t WHERE t.upload=native_filesystem_uploads.id) AND lease_until>clock_timestamp()",&[&self.upload,&self.owner,&self.fence]).await?.ok_or_else(||anyhow::anyhow!("FILESYSTEM_LEASE_INVALID"))?;
        let mut labels = row
            .get::<_, Vec<String>>(0)
            .into_iter()
            .chain(self.consumed())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if self.compiler_source.is_some() {
            let marks: Value = client
                .query_one(
                    "SELECT moria.native_compiler_upload_access($1,$2,$3,$4)",
                    &[
                        &self.session.session_hash(),
                        &self.upload,
                        &self.owner,
                        &self.fence,
                    ],
                )
                .await?
                .get(0);
            labels.extend(serde_json::from_value::<Vec<String>>(marks)?);
            labels.sort();
            labels.dedup();
        }
        ensure!(
            labels.len() <= 256
                && labels
                    .iter()
                    .all(|mark| snapshot.identity.markings.contains(mark)),
            "FORBIDDEN"
        );
        target(
            client,
            &self.session,
            &self.request,
            if self.compiler_source.is_some() {
                &[]
            } else {
                &labels
            },
            expected,
        )
        .await?;
        physical(client, &self.namespace, Some(&self.physical)).await?;
        let sql = if renew {
            "UPDATE moria.native_filesystem_uploads SET lease_until=clock_timestamp()+interval '30 seconds',source_markings=$4 WHERE id=$1 AND owner=$2 AND fence=$3 AND state IN ('writing','sealed') AND incarnation=(SELECT incarnation FROM moria.indexing_instance) AND NOT EXISTS(SELECT 1 FROM moria.native_filesystem_upload_tombstones t WHERE t.upload=native_filesystem_uploads.id) AND lease_until>clock_timestamp() RETURNING id"
        } else {
            "UPDATE moria.native_filesystem_uploads SET source_markings=$4 WHERE id=$1 AND owner=$2 AND fence=$3 AND state IN ('writing','sealed') AND incarnation=(SELECT incarnation FROM moria.indexing_instance) AND NOT EXISTS(SELECT 1 FROM moria.native_filesystem_upload_tombstones t WHERE t.upload=native_filesystem_uploads.id) AND lease_until>clock_timestamp() RETURNING id"
        };
        ensure!(
            client
                .query_opt(sql, &[&self.upload, &self.owner, &self.fence, &labels])
                .await?
                .is_some(),
            "FILESYSTEM_LEASE_INVALID"
        );
        Ok(())
    }
    async fn multipart(
        &self,
        object: &LogicalObject,
        physical: &str,
        upload_id: Option<&str>,
    ) -> Result<()> {
        ensure!(
            object == &self.object && physical == self.physical,
            "FILESYSTEM_CONFLICT"
        );
        ensure!(
            upload_id.is_none_or(|id| !id.is_empty()
                && id.len() <= 1024
                && id.bytes().all(|byte| byte.is_ascii_graphic())),
            "STORAGE_PROVIDER_ERROR"
        );
        let admission = Admission::begin(&self.db, &self.session, false).await?;
        let outcome=async {
            let client=admission.client();self.check_in(client,true).await?;
            if let Some(id)=upload_id {
                ensure!(client.query_opt("UPDATE moria.native_filesystem_multipart_intents SET upload_id=$4 WHERE upload=$1 AND producer_fence=$2 AND owner=$3 AND (upload_id IS NULL OR upload_id=$4) RETURNING upload",&[&self.upload,&self.fence,&self.owner,&id]).await?.is_some(),"FILESYSTEM_LEASE_INVALID");
            }else {
                client.execute("INSERT INTO moria.native_filesystem_multipart_intents(upload,producer_fence,owner,physical_hash,location) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",&[&self.upload,&self.fence,&self.owner,&self.physical,&json!(self.object)]).await?;
                ensure!(client.query_one("SELECT owner=$3 AND physical_hash=$4 AND location=$5::jsonb FROM moria.native_filesystem_multipart_intents WHERE upload=$1 AND producer_fence=$2",&[&self.upload,&self.fence,&self.owner,&self.physical,&json!(self.object)]).await?.get::<_,bool>(0),"FILESYSTEM_LEASE_INVALID");
            }
            Ok(())
        }.await;
        admission.complete(outcome).await
    }
    async fn check(&self, object: &LogicalObject) -> Result<()> {
        ensure!(object == &self.object, "FORBIDDEN");
        let admission = Admission::begin(&self.db, &self.session, false).await?;
        let outcome = self.check_in(admission.client(), true).await;
        admission.complete(outcome).await
    }
}
#[async_trait]
impl ReadAuthority for UploadAuthority {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        self.check(object).await
    }
}
#[async_trait]
impl WriteAuthority for UploadAuthority {
    fn scope_key(&self) -> String {
        format!("filesystem/{}/{}", self.upload, self.fence)
    }
    async fn check_write(&self, object: &LogicalObject) -> Result<()> {
        self.check(object).await
    }
    async fn prepare_multipart(&self, object: &LogicalObject, physical: &str) -> Result<()> {
        self.multipart(object, physical, None).await
    }
    async fn record_multipart(
        &self,
        object: &LogicalObject,
        upload_id: &str,
        physical: &str,
    ) -> Result<()> {
        self.multipart(object, physical, Some(upload_id)).await
    }
}

pub enum UploadStart {
    Receipt(Value),
    Upload(Box<FileUpload>),
}
pub struct FileUpload {
    guard: Arc<UploadAuthority>,
    writer: Option<Box<dyn ObjectWrite>>,
    sealed: Option<SealedObject>,
    cancel: Cancellation,
    size: u64,
    hash: Sha256,
    compiler_bytes: Option<Vec<u8>>,
}
impl Drop for FileUpload {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
impl FileUpload {
    pub async fn append(&mut self, bytes: &[u8]) -> Result<()> {
        ensure!(
            self.size.saturating_add(bytes.len() as u64) <= self.guard.request.content().size,
            "CONTENT_MISMATCH"
        );
        self.guard.check(&self.guard.object).await?;
        if let Some(buffer) = &mut self.compiler_bytes {
            ensure!(
                buffer.len() + bytes.len() <= 16 * 1024 * 1024,
                "RESOURCE_LIMIT"
            );
            buffer.extend_from_slice(bytes);
        }
        for chunk in bytes.chunks(1024 * 1024) {
            if let Some(writer) = &mut self.writer {
                writer.append(chunk).await?;
            }
            self.hash.update(chunk);
            self.size += chunk.len() as u64;
        }
        Ok(())
    }
    pub async fn abort(mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.abort().await?;
        }
        self.cancel.cancel();
        Ok(())
    }
    pub async fn finish(mut self) -> Result<Value> {
        ensure!(
            self.size == self.guard.request.content().size
                && format!("sha256:{:x}", self.hash.clone().finalize())
                    == self.guard.request.content().digest,
            "CONTENT_MISMATCH"
        );
        let artifact_marks = if let Some(source) = &self.guard.compiler_source {
            let parsed = (|| -> Result<Vec<String>> {
                let artifact: super::CompiledSourceArtifact = serde_json::from_slice(
                    self.compiler_bytes
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
                )
                .map_err(|_| anyhow::anyhow!("SOURCE_INVALID"))?;
                ensure!(
                    artifact.version == 1
                        && artifact.root_id == source.root_id
                        && artifact.source_digest == source.source_digest,
                    "SOURCE_INVALID"
                );
                let resources = artifact.compiled.manifest["resources"]
                    .as_array()
                    .filter(|resources| !resources.is_empty() && resources.len() <= 10000)
                    .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
                let mut marks = BTreeSet::new();
                for resource in resources {
                    if let Some(labels) = resource["metadata"].get("markings") {
                        for label in labels
                            .as_array()
                            .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?
                        {
                            let label = label
                                .as_str()
                                .filter(|label| !label.is_empty() && label.len() <= 512)
                                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
                            marks.insert(label.to_owned());
                            ensure!(marks.len() <= 256, "RESOURCE_LIMIT");
                        }
                    }
                }
                Ok(marks.into_iter().collect())
            })();
            match parsed {
                Ok(marks) => marks,
                Err(error) => {
                    let _ = self.abort().await;
                    return Err(error);
                }
            }
        } else {
            vec![]
        };
        let sealed = if let Some(sealed) = self.sealed.take() {
            sealed
        } else {
            self.writer
                .take()
                .ok_or_else(|| anyhow::anyhow!("CONTENT_MISMATCH"))?
                .seal()
                .await?
        };
        ensure!(
            sealed.object == self.guard.object
                && sealed.size == self.size
                && sealed.sha256 == self.guard.request.content().digest,
            "CONTENT_MISMATCH"
        );
        let admission =
            Admission::begin_filesystem(&self.guard.db, &self.guard.session, true, true).await?;
        let outcome=async {
            let client=admission.client();self.guard.check_in(client,false).await?;
            if let Some(source)=&self.guard.compiler_source {ensure!(super::NativeSources::capture(client,&self.guard.session,source.root_id).await?.digest==source.source_digest,"FILESYSTEM_CONFLICT");}
            let snapshot=Authority::current(client,&self.guard.session).await?;
            if !artifact_marks.is_empty(){let mut marks:Vec<String>=client.query_one("SELECT source_markings FROM moria.native_filesystem_uploads WHERE id=$1",&[&self.guard.upload]).await?.get(0);marks.extend(artifact_marks.clone());marks.sort();marks.dedup();ensure!(marks.len()<=256&&marks.iter().all(|mark|snapshot.identity.markings.contains(mark)),"FORBIDDEN");client.execute("UPDATE moria.native_filesystem_uploads SET source_markings=$2 WHERE id=$1",&[&self.guard.upload,&marks]).await?;}
            let anchor=if let Some(context)=&self.guard.context {
                let marks=crate::platform::NativePlatformBroker::prepared_ledger(&self.guard.db,client,&self.guard.session,context.lease().execution,true).await?;
                context.record_consumed_markings(&marks)?;Some(context.mutation_anchor_in(client).await?)
            }else{None};
            let consumed=self.guard.consumed();let (entry,_)=target(client,&self.guard.session,&self.guard.request,if self.guard.compiler_source.is_some(){&[]}else{&consumed},true).await?;
            let content=FileContent{location:self.guard.object.clone(),digest:self.guard.request.content().digest.clone(),size:self.size,media_type:self.guard.request.content().media_type.clone()};
            let mutation=Uuid::new_v4();let mut touched=vec![];
            match &self.guard.request {
                FileWrite::Create{name,..}=>{
                    NativeFilesystem::placement(client,entry.id,name,None).await?;
                    let row=client.query_one(&format!("INSERT INTO moria.fs_entries(id,parent_id,name,kind,project_id,environment_id,content) VALUES($1,$2,$3,'file',$4,$5,$6) RETURNING {ENTRY}"),&[&Uuid::new_v4(),&entry.id,&name,&entry.project_id,&entry.environment_id,&json!(content)]).await?;
                    NativeFilesystem::remember(client,serde_json::from_value(row.get(0))?,mutation,&mut touched).await?;
                    NativeFilesystem::touch(client,entry.id,mutation,&mut touched).await?;
                },
                FileWrite::Write{..}=>{
                    let row=client.query_one(&format!("UPDATE moria.fs_entries SET content=$2,revision=revision+1 WHERE id=$1 RETURNING {ENTRY}"),&[&entry.id,&json!(content)]).await?;
                    NativeFilesystem::remember(client,serde_json::from_value(row.get(0))?,mutation,&mut touched).await?;
                },
            }
            if let Some(source)=&self.guard.compiler_source {
                let artifact=touched.iter().find(|entry|entry.kind=="file").ok_or_else(||anyhow::anyhow!("SOURCE_INVALID"))?.id;
                let markings:Vec<String>=client.query_one("SELECT source_markings FROM moria.native_filesystem_uploads WHERE id=$1",&[&self.guard.upload]).await?.get(0);
                let markings=json!(markings.into_iter().map(|marking|json!({"environment":self.guard.session.environment(),"marking":marking})).collect::<Vec<_>>());
                client.execute("INSERT INTO moria.fs_policies(entry_id,markings) VALUES($1,$2)",&[&artifact,&markings]).await?;
                client.execute("INSERT INTO moria.native_compiler_artifacts(artifact,root,source_digest) VALUES($1,$2,$3)",&[&artifact,&source.root_id,&source.source_digest]).await?;
                for proof in &source.sources {client.execute("INSERT INTO moria.native_compiler_artifact_sources(artifact,source,root,operation) VALUES($1,$2,$3,$4)",&[&artifact,&proof.id,&proof.root_id,&proof.operation]).await?;client.execute("INSERT INTO moria.fs_provenance(entry_id,source_id) VALUES($1,$2) ON CONFLICT DO NOTHING",&[&artifact,&proof.id]).await?;}
            }
            for entry in &touched {NativeFilesystem::access(client,&self.guard.session,entry.id,"read").await?;}
            let receipt=json!({"mutationId":mutation,"entries":touched});
            let row=client.query_one("SELECT scope_hash,request_key,request_hash FROM moria.native_filesystem_uploads WHERE id=$1",&[&self.guard.upload]).await?;
            let actor=json!({"environment":self.guard.session.environment(),"principal":self.guard.session.principal(),"application":self.guard.session.application(),"delegation":[]});
            client.execute("INSERT INTO moria.fs_mutations(id,scope_hash,request_key,request_hash,actor,receipt) VALUES($1,$2,$3,$4,$5,$6)",&[&mutation,&row.get::<_,String>(0),&row.get::<_,String>(1),&row.get::<_,String>(2),&actor,&receipt]).await?;
            self.guard.check_in_mode(client,false,false).await?;
            client.execute("UPDATE moria.native_filesystem_uploads SET state='committed',receipt=$2 WHERE id=$1",&[&self.guard.upload,&receipt]).await?;
            if let Some(context)=&self.guard.context {
                crate::platform::NativePlatformBroker::committed_ledger(client,&self.guard.session,context.lease().execution).await?;
                for (index,entry) in touched.iter().enumerate() {
                    let parts=vec!["entries".into(),entry.id.to_string()];
                    let request=super::FilesystemRequest{method:"GET",parts:&parts,query:&[],input:&Value::Null,key:None,consumed:&[]};
                    let(evidence,labels)=metadata_evidence(client,&self.guard.session,&request,&json!(entry)).await?;context.record_consumed_markings(&labels)?;
                    crate::platform::NativePlatformBroker::record_filesystem(client,&self.guard.session,context.lease().execution,Some(context.attempt_id()),&format!("file-written/{}/{index}",self.guard.upload),&[evidence],&labels).await?;
                }
                context.acknowledge_own_mutation_in(client,anchor.as_ref().unwrap()).await?;context.check_in(client).await?;
                if let Some(call)=&self.guard.call {
                    crate::platform::NativePlatformBroker::trace(client,context,call,None,&Ok(receipt.clone()),false).await?;
                    client.execute("UPDATE moria.native_function_calls SET action_key=$3 WHERE execution=$1 AND call_id=$2",&[&context.lease().execution,&call.id,&row.get::<_,String>(1)]).await?;
                }
            }
            Authority::finish(client,&self.guard.session,&snapshot).await?;
            Ok(receipt)
        }.await;
        admission
            .complete(outcome.map_err(super::mutation_error))
            .await
    }
}

#[derive(Clone)]
struct FileAuthority {
    db: Database,
    session: VerifiedSession,
    namespace: IndexNamespace,
    physical: String,
    id: Uuid,
    revision: String,
    content: FileContent,
    context: Option<SandboxContext>,
}
impl fmt::Debug for FileAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FilesystemReadAuthority")
    }
}
#[async_trait]
impl ReadAuthority for FileAuthority {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        ensure!(object == &self.content.location, "FORBIDDEN");
        let admission = Admission::begin(&self.db, &self.session, false).await?;
        let outcome = async {
            if let Some(context) = &self.context {
                context.check_in(admission.client()).await?;
            }
            let snapshot = Authority::current(admission.client(), &self.session).await?;
            let version = NativeFilesystem::metadata_in(
                admission.client(),
                &self.session,
                self.id,
                "version",
                Some(&self.revision),
            )
            .await?;
            ensure!(
                version["content"] == json!(self.content),
                "CONTENT_MISMATCH"
            );
            physical(admission.client(), &self.namespace, Some(&self.physical)).await?;
            Authority::finish(admission.client(), &self.session, &snapshot).await
        }
        .await;
        admission.complete(outcome).await
    }
}
pub struct FileRead {
    pub entry: Entry,
    pub stream: BoxStream<'static, Result<Bytes>>,
}

impl NativeFilesystem {
    pub async fn begin_file(
        &self,
        session: &VerifiedSession,
        key_value: &str,
        request: FileWrite,
        context: Option<SandboxContext>,
        call: Option<HttpCall>,
    ) -> Result<UploadStart> {
        self.begin_file_admitted(session, key_value, request, context, call, None)
            .await
    }
    pub(crate) async fn begin_file_admitted(
        &self,
        session: &VerifiedSession,
        key_value: &str,
        request: FileWrite,
        context: Option<SandboxContext>,
        call: Option<HttpCall>,
        compiler: Option<(super::source::CompilerUploadSource, Vec<String>)>,
    ) -> Result<UploadStart> {
        key(key_value)?;
        request.validate()?;
        if compiler.is_some() {
            ensure!(
                request.create()
                    && context.is_none()
                    && request.content().media_type == "application/json"
                    && request.content().size <= 16 * 1024 * 1024,
                "SOURCE_INVALID"
            );
        }
        if let Some(context) = &context {
            ensure!(context.effect() == "readWrite", "FUNCTION_EFFECT");
        }
        let namespace = namespace()?;
        let (store, _) = storage::configured_store_with_fingerprint(&namespace)?;
        let actor = json!({"environment":session.environment(),"principal":session.principal(),"application":session.application(),"delegation":[]});
        let scope = fingerprint(&actor)?;
        let digest = if let Some((source, _)) = &compiler {
            fingerprint(
                &json!({"request":request.change(json!(request.content())),"compilerSource":{"rootId":source.root_id,"sourceDigest":source.source_digest}}),
            )?
        } else {
            request.request_hash()?
        };
        let compiler_json = compiler.as_ref().map(|(source, _)| json!(source));
        let owner = Uuid::new_v4();
        let admission = Admission::begin_filesystem(&self.db, session, true, true).await?;
        let outcome=async {
            let client=admission.client();let snapshot=Authority::current(client,session).await?;
            if let Some(context)=&context {context.check_in(client).await?;}
            let mut consumed=context.as_ref().map_or(vec![],|context|context.consumed_markings().into_iter().map(|mark|mark.marking).collect::<Vec<_>>());
            if let Some((_,marks))=&compiler{consumed.extend(marks.clone());}
            if let Some(row)=client.query_opt("SELECT request_hash,receipt FROM moria.fs_mutations WHERE scope_hash=$1 AND request_key=$2",&[&scope,&key_value]).await? {
                ensure!(row.get::<_,String>(0)==digest,"IDEMPOTENCY_CONFLICT");target(client,session,&request,if compiler.is_some(){&[]}else{&consumed},false).await?;
                let receipt:Value=row.get(1);for entry in receipt["entries"].as_array().ok_or_else(||anyhow::anyhow!("FILESYSTEM_INVALID"))? {NativeFilesystem::access(client,session,identifier(entry["id"].as_str().ok_or_else(||anyhow::anyhow!("FILESYSTEM_INVALID"))?)?,"read").await?;}
                if let (Some(context),Some(call))=(&context,&call) {crate::platform::NativePlatformBroker::trace(client,context,call,None,&Ok(receipt.clone()),false).await?;client.execute("UPDATE moria.native_function_calls SET action_key=$3 WHERE execution=$1 AND call_id=$2",&[&context.lease().execution,&call.id,&key_value]).await?;}
                Authority::finish(client,session,&snapshot).await?;return Ok(Err(receipt));
            }
            if let Some((source,_))=&compiler {ensure!(super::NativeSources::capture(client,session,source.root_id).await?.digest==source.source_digest,"FILESYSTEM_CONFLICT");for proof in &source.sources{consumed.extend(NativeFilesystem::access(client,session,proof.id,&proof.operation).await?);}}
            let(_,labels)=target(client,session,&request,if compiler.is_some(){&[]}else{&consumed},true).await?;
            consumed.extend(labels.clone());consumed.sort();consumed.dedup();ensure!(consumed.len()<=256,"RESOURCE_LIMIT");
            ensure!(consumed.iter().all(|mark|snapshot.identity.markings.contains(mark)),"FORBIDDEN");
            if let Some(context)=&context {context.record_consumed_markings(&labels.into_iter().map(|marking|crate::sandbox::ConsumedMarking{environment:session.environment().into(),marking}).collect::<Vec<_>>())?;}
            let physical=physical(client,&namespace,None).await?;
            let(id,object,fence)=if let Some(row)=client.query_opt("SELECT id,request_hash,location,physical_hash,fence,lease_until<=clock_timestamp(),source_markings,compiler_source,NOT EXISTS(SELECT 1 FROM moria.native_filesystem_upload_tombstones t WHERE t.upload=native_filesystem_uploads.id),incarnation=(SELECT incarnation FROM moria.indexing_instance) FROM moria.native_filesystem_uploads WHERE scope_hash=$1 AND request_key=$2 FOR UPDATE",&[&scope,&key_value]).await? {
                ensure!(row.get::<_,bool>(8),"UPLOAD_RETIRED");ensure!(row.get::<_,bool>(9),"FILESYSTEM_LEASE_INVALID");ensure!(row.get::<_,String>(1)==digest,"IDEMPOTENCY_CONFLICT");ensure!(row.get::<_,String>(3)==physical,"FILESYSTEM_CONFLICT");ensure!(row.get::<_,bool>(5),"RESOURCE_BUSY");
                let id:Uuid=row.get(0);let fence:i64=row.get(4);
                ensure!(row.get::<_,Option<Value>>(7)==compiler_json,"IDEMPOTENCY_CONFLICT");consumed.extend(row.get::<_,Vec<String>>(6));consumed.sort();consumed.dedup();ensure!(consumed.len()<=256&&consumed.iter().all(|mark|snapshot.identity.markings.contains(mark)),"FORBIDDEN");target(client,session,&request,if compiler.is_some(){&[]}else{&consumed},true).await?;
                client.execute("UPDATE moria.native_filesystem_uploads SET owner=$2,fence=fence+1,session_hash=$3,source_markings=$4,lease_until=clock_timestamp()+interval '30 seconds' WHERE id=$1",&[&id,&owner,&session.session_hash(),&consumed]).await?;
                (id,serde_json::from_value::<LogicalObject>(row.get(2))?,fence.checked_add(1).ok_or_else(||anyhow::anyhow!("RESOURCE_LIMIT"))?)
            }else {
                let id=Uuid::new_v4();let object=LogicalObject::new("filesystem","content",&Uuid::new_v4().to_string())?;
                client.execute("INSERT INTO moria.native_filesystem_uploads(id,scope_hash,request_key,request_hash,environment,principal,application,session_hash,request,location,physical_hash,source_markings,owner,fence,lease_until,state,compiler_source) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,1,clock_timestamp()+interval '30 seconds','writing',$14)",&[&id,&scope,&key_value,&digest,&session.environment(),&session.principal(),&session.application(),&session.session_hash(),&json!(request),&json!(object),&physical,&consumed,&owner,&compiler_json]).await?;
                client.execute("INSERT INTO moria.fs_content_candidates(object_id,location) VALUES($1,$2)",&[&object.object_id,&json!(object)]).await?;(id,object,1)
            };
            Authority::finish(client,session,&snapshot).await?;
            Ok(Ok(Arc::new(UploadAuthority{db:self.db.clone(),session:session.clone(),namespace:namespace.clone(),physical,upload:id,owner,fence,object,request:request.clone(),consumed,context:context.clone(),call:call.clone(),compiler_source:compiler.as_ref().map(|(source,_)|source.clone())})))
        }.await;
        let guard = match admission.complete(outcome).await? {
            Err(receipt) => return Ok(UploadStart::Receipt(receipt)),
            Ok(guard) => guard,
        };
        let cancel = Cancellation::default();
        let sealed = store
            .recover_object(
                guard.object.clone(),
                request.content().size,
                request.content().digest.clone(),
                guard.clone(),
                guard.clone(),
                cancel.clone(),
            )
            .await?;
        let writer = if sealed.is_none() {
            Some(
                store
                    .begin_object(
                        guard.object.clone(),
                        request.content().size,
                        request.content().digest.clone(),
                        guard.clone(),
                        cancel.clone(),
                    )
                    .await?,
            )
        } else {
            None
        };
        let compiler_bytes = guard.compiler_source.as_ref().map(|_| vec![]);
        Ok(UploadStart::Upload(Box::new(FileUpload {
            guard,
            writer,
            sealed,
            cancel,
            size: 0,
            hash: Sha256::new(),
            compiler_bytes,
        })))
    }
    pub async fn read_file(
        &self,
        session: &VerifiedSession,
        id: Uuid,
        requested_revision: Option<&str>,
        context: Option<SandboxContext>,
    ) -> Result<FileRead> {
        let namespace = namespace()?;
        let (store, _) = storage::configured_store_with_fingerprint(&namespace)?;
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome = async {
            let client = admission.client();
            let snapshot = Authority::current(client, session).await?;
            if let Some(context) = &context {
                context.check_in(client).await?;
            }
            let entry: Entry = if let Some(revision) = requested_revision {
                serde_json::from_value(
                    Self::metadata_in(client, session, id, "version", Some(revision)).await?,
                )?
            } else {
                Self::get_in(client, session, id).await?.0
            };
            ensure!(entry.kind == "file", "FILESYSTEM_INVALID");
            let content: FileContent = serde_json::from_value(
                entry
                    .content
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("FILESYSTEM_INVALID"))?,
            )?;
            ensure!(
                content.location.warehouse_id == "filesystem"
                    && content.location.table_id == "content"
                    && content.location.version == 1
                    && content.size <= FILE_BYTES,
                "CONTENT_MISMATCH"
            );
            FileWrite::Write {
                id,
                expected_revision: entry.revision.clone(),
                content: ContentDeclaration {
                    digest: content.digest.clone(),
                    size: content.size,
                    media_type: content.media_type.clone(),
                },
            }
            .validate()?;
            let physical = physical(client, &namespace, None).await?;
            if let Some(context) = &context {
                let parts = vec!["entries".into(), id.to_string(), "version".into()];
                let query = vec![("revision".into(), entry.revision.clone())];
                let request = super::FilesystemRequest {
                    method: "GET",
                    parts: &parts,
                    query: &query,
                    input: &Value::Null,
                    key: None,
                    consumed: &[],
                };
                let (evidence, labels) =
                    metadata_evidence(client, session, &request, &json!(entry)).await?;
                context.record_consumed_markings(&labels)?;
                crate::platform::NativePlatformBroker::record_filesystem(
                    client,
                    session,
                    context.lease().execution,
                    Some(context.attempt_id()),
                    &format!("file-read/{id}/{}", entry.revision),
                    &[evidence],
                    &labels,
                )
                .await?;
            }
            Authority::finish(client, session, &snapshot).await?;
            let guard = Arc::new(FileAuthority {
                db: self.db.clone(),
                session: session.clone(),
                namespace: namespace.clone(),
                physical,
                id,
                revision: entry.revision.clone(),
                content: content.clone(),
                context: context.clone(),
            });
            Ok((entry, content, guard))
        }
        .await;
        let (entry, content, guard) = admission.complete(outcome).await?;
        let cancel = Cancellation::default();
        let version = store
            .version(&content.location, guard.as_ref(), &cancel)
            .await?;
        ensure!(version.size == content.size, "CONTENT_MISMATCH");
        let stream = futures::stream::try_unfold(
            (
                store,
                guard,
                content,
                version,
                cancel,
                0u64,
                Sha256::new(),
                tokio::time::Instant::now() + std::time::Duration::from_secs(1800),
            ),
            |(store, guard, content, version, cancel, position, mut hash, deadline)| async move {
                ensure!(tokio::time::Instant::now() < deadline, "QUERY_TIMEOUT");
                if position == content.size {
                    guard.check_read(&content.location).await?;
                    ensure!(
                        format!("sha256:{:x}", hash.finalize()) == content.digest,
                        "CONTENT_MISMATCH"
                    );
                    return Ok(None);
                }
                let end = (position + store.chunk_bytes() as u64).min(content.size);
                let bytes = store
                    .range_at_version(
                        &content.location,
                        position..end,
                        &version,
                        guard.as_ref(),
                        &cancel,
                    )
                    .await?;
                ensure!(bytes.len() as u64 == end - position, "CONTENT_MISMATCH");
                hash.update(&bytes);
                Ok(Some((
                    bytes,
                    (store, guard, content, version, cancel, end, hash, deadline),
                )))
            },
        );
        Ok(FileRead {
            entry,
            stream: Box::pin(stream),
        })
    }
}
