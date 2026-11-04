use super::{ENTRY, Entry, NativeFilesystem, identifier, revision};
use crate::{
    authority::{Authority, VerifiedSession},
    canonical::fingerprint,
    database::Database,
    serving::Admission,
};
use anyhow::{Result, ensure};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use tokio_postgres::Client;
use uuid::Uuid;
#[path = "filesystem_source_activation.rs"]
mod activation;
#[path = "filesystem_source_validation.rs"]
mod validation;
pub(crate) fn validate_configuration(schema: &Value, configuration: &Value) -> Result<()> {
    validation::configuration(schema, configuration)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceDependency {
    pub root_id: Uuid,
    pub digest: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceDescriptor {
    pub version: u32,
    pub name: String,
    pub namespace: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
    #[serde(default)]
    pub dependencies: Vec<SourceDependency>,
}
fn digest(value: &str) -> Result<()> {
    ensure!(
        value.len() == 71
            && value.starts_with("sha256:")
            && value[7..]
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
        "SOURCE_INVALID"
    );
    Ok(())
}
impl SourceDescriptor {
    pub fn parse(kind: &str, value: &Value) -> Result<Self> {
        ensure!(["ontology", "repository"].contains(&kind), "SOURCE_INVALID");
        if let Some(dependencies) = value.get("dependencies").and_then(Value::as_array) {
            for dependency in dependencies {
                identifier(
                    dependency["rootId"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
                )?;
            }
        }
        ensure!(
            kind != "ontology" || value.get("runtime").is_none(),
            "SOURCE_INVALID"
        );
        let mut normalized = value.clone();
        if normalized.get("dependencies").is_some_and(Value::is_null) {
            normalized
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?
                .remove("dependencies");
        }
        let descriptor: Self =
            serde_json::from_value(normalized).map_err(|_| anyhow::anyhow!("SOURCE_INVALID"))?;
        ensure!(
            descriptor.version == 1
                && !descriptor.name.is_empty()
                && descriptor.name.encode_utf16().count() <= 128
                && !descriptor.namespace.is_empty()
                && descriptor.namespace.len() <= 128
                && (descriptor.namespace.as_bytes()[0].is_ascii_alphabetic()
                    || descriptor.namespace.starts_with('_'))
                && descriptor
                    .namespace
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c)),
            "SOURCE_INVALID"
        );
        ensure!(
            if kind == "repository" {
                descriptor
                    .runtime
                    .as_deref()
                    .is_some_and(|value| ["typescript-v1", "python-duckdb-v1"].contains(&value))
            } else {
                descriptor.runtime.is_none()
            },
            "SOURCE_INVALID"
        );
        ensure!(
            descriptor.dependencies.len() <= 16
                && descriptor
                    .dependencies
                    .iter()
                    .map(|dependency| dependency.root_id)
                    .collect::<BTreeSet<_>>()
                    .len()
                    == descriptor.dependencies.len(),
            "SOURCE_INVALID"
        );
        for dependency in &descriptor.dependencies {
            digest(&dependency.digest)?;
        }
        Ok(descriptor)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceRole {
    pub entry_id: Uuid,
    pub kind: String,
    pub project_id: Uuid,
    pub deployment_unit_id: Uuid,
    pub namespace: String,
    pub configuration: Value,
    pub revision: String,
    pub latest_release: Option<String>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceFile {
    pub path: String,
    pub entry: Entry,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceDirectory {
    pub path: String,
    pub id: Uuid,
    pub revision: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceCheckout {
    pub version: u32,
    pub root_id: Uuid,
    pub role: SourceRole,
    pub digest: String,
    pub files: Vec<SourceFile>,
    pub directories: Vec<SourceDirectory>,
}
impl SourceCheckout {
    fn hash(&self) -> Result<String> {
        fingerprint(
            &json!({"rootId":self.root_id,"projectId":self.role.project_id,"deploymentUnitId":self.role.deployment_unit_id,"namespace":self.role.namespace,"files":self.files.iter().map(|file|json!({"path":file.path,"id":file.entry.id,"revision":file.entry.revision,"digest":file.entry.content.as_ref().map(|content|content["digest"].clone())})).collect::<Vec<_>>(),"directories":self.directories}),
        )
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompiledManifest {
    pub manifest: Value,
    pub fingerprint: String,
    pub order: Vec<String>,
    pub policy_types_checked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attribute_types_fingerprint: Option<String>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompilerContext {
    pub principal_attributes: crate::authorization::ValueMap,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompiledSourceArtifact {
    pub version: u32,
    pub root_id: Uuid,
    pub source_digest: String,
    pub descriptor: SourceDescriptor,
    pub compiled: CompiledManifest,
    pub owned: Vec<String>,
    pub compiler_context: CompilerContext,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompilerArtifactRef {
    pub entry_id: Uuid,
    pub revision: String,
    pub digest: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CompilerUploadSource {
    pub root_id: Uuid,
    pub source_digest: String,
    pub sources: Vec<CompilerSourceAccess>,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CompilerSourceAccess {
    pub id: Uuid,
    pub root_id: Uuid,
    pub operation: String,
}

#[derive(Clone)]
pub struct NativeSources {
    pub db: Database,
}
impl NativeSources {
    /// The caller holds normal domain admission and independently proves its operation.
    /// These helpers inspect actual declaration files, not data bytes or secret material.
    pub(crate) async fn declaration_labels_in(
        client: &Client,
        session: &VerifiedSession,
        resource: Uuid,
        release: &str,
    ) -> Result<Vec<String>> {
        let before = Authority::current(client, session).await?;
        let labels: Value = client
            .query_one(
                "SELECT moria.native_source_declaration_labels($1,$2,$3)",
                &[&session.session_hash(), &resource, &release],
            )
            .await?
            .get(0);
        Authority::finish(client, session, &before).await?;
        Ok(serde_json::from_value(labels)?)
    }
    /// Metadata only. The caller retains its normal final authority/fence check before release.
    pub(crate) async fn declaration_labels_batch_in(
        client: &Client,
        session: &VerifiedSession,
        declarations: &[(Uuid, String)],
    ) -> Result<Vec<String>> {
        if declarations.is_empty() {
            return Ok(Vec::new());
        }
        ensure!(declarations.len() <= 32, "RESOURCE_LIMIT");
        let requests = json!(
            declarations
                .iter()
                .map(|(resource, release)| json!({"resource":resource,"release":release}))
                .collect::<Vec<_>>()
        );
        let labels: Value = client
            .query_one(
                "SELECT moria.native_source_declaration_labels_batch($1,$2)",
                &[&session.session_hash(), &requests],
            )
            .await?
            .get(0);
        let labels: Vec<String> = serde_json::from_value(labels)?;
        ensure!(labels.len() <= 256, "RESOURCE_LIMIT");
        Ok(labels)
    }
    /// Existing immutable service purpose supplies metadata authority, never material IO.
    pub(crate) async fn registered_declaration_labels_batch_in(
        client: &Client,
        service: &crate::jobs::RegisteredService,
        declarations: &[(Uuid, String)],
    ) -> Result<Vec<String>> {
        if declarations.is_empty() {
            return Ok(Vec::new());
        }
        ensure!(declarations.len() <= 32, "RESOURCE_LIMIT");
        let requests = json!(
            declarations
                .iter()
                .map(|(resource, release)| json!({"resource":resource,"release":release}))
                .collect::<Vec<_>>()
        );
        let labels: Value = client
            .query_one(
                "SELECT moria.native_registered_declaration_labels_batch($1,$2,$3,$4,$5,$6,$7)",
                &[
                    &service.id(),
                    &service.environment(),
                    &service.target(),
                    &service.domain(),
                    &service.binding_revision(),
                    &service.resource_class(),
                    &requests,
                ],
            )
            .await?
            .get(0);
        let labels: Vec<String> = serde_json::from_value(labels)?;
        ensure!(labels.len() <= 256, "RESOURCE_LIMIT");
        Ok(labels)
    }
    pub(crate) async fn registered_declaration_labels_in(
        client: &Client,
        service: &crate::jobs::RegisteredService,
        resource: Uuid,
        release: &str,
    ) -> Result<Vec<String>> {
        let before = service.state_in(client).await?;
        let labels: Value = client
            .query_one(
                "SELECT moria.native_registered_declaration_labels($1,$2,$3,$4,$5,$6,$7,$8)",
                &[
                    &service.id(),
                    &service.environment(),
                    &service.target(),
                    &service.domain(),
                    &service.binding_revision(),
                    &service.resource_class(),
                    &resource,
                    &release,
                ],
            )
            .await?
            .get(0);
        let after = service.state_in(client).await?;
        ensure!(
            before.security_epoch == after.security_epoch
                && before.instance_epoch == after.instance_epoch,
            "AUTHORIZATION_CHANGED"
        );
        Ok(serde_json::from_value(labels)?)
    }
    /// Resolve only immutable identity/definition; callers must separately authorize current and pinned files.
    pub(crate) async fn definition_in(
        client: &Client,
        release: &str,
        resource: Uuid,
    ) -> Result<(String, Value)> {
        ensure!(
            !release.is_empty() && release.len() <= 256,
            "SOURCE_INVALID"
        );
        let releases=client.query("WITH RECURSIVE releases(digest) AS (SELECT digest FROM moria.fs_source_releases WHERE digest=$1 UNION SELECT child.digest FROM releases parent JOIN moria.fs_source_releases r ON r.digest=parent.digest CROSS JOIN LATERAL jsonb_array_elements(r.snapshot->'descriptor'->'dependencies') dependency JOIN moria.fs_source_releases child ON child.digest=dependency->>'digest') SELECT digest FROM releases LIMIT 257",&[&release]).await?;
        ensure!(
            !releases.is_empty() && releases.len() <= 256,
            "SOURCE_INVALID"
        );
        let digests = releases
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>();
        let owned=client.query("SELECT r.digest,i.reference,item.value FROM moria.fs_resource_identities i JOIN moria.fs_source_releases r ON r.root_id=i.root_id AND r.digest=ANY($2) AND r.owned ? i.reference CROSS JOIN LATERAL jsonb_array_elements(r.compiled->'manifest'->'resources') item WHERE i.resource_id=$1 AND concat_ws('/',item.value->'metadata'->>'namespace',item.value->>'kind',item.value->'metadata'->>'name')=i.reference LIMIT 2",&[&resource,&digests]).await?;
        ensure!(owned.len() == 1, "SOURCE_INVALID");
        Ok((owned[0].get(0), owned[0].get(2)))
    }
    /// Current declaration identity only; callers separately authorize the resource operation.
    pub(crate) async fn active_definition_in(
        client: &Client,
        environment: &str,
        resource: Uuid,
    ) -> Result<Option<(Uuid, String, Value)>> {
        let row = client.query_opt("SELECT b.entry_id,b.digest,d.value FROM moria.fs_entries e JOIN moria.fs_resource_bindings b ON b.entry_id=e.id AND b.active JOIN moria.fs_roles r ON r.entry_id=b.root_id AND NOT r.retired JOIN moria.fs_active_releases a ON a.root_id=b.root_id AND a.environment=b.environment AND a.digest=b.digest CROSS JOIN LATERAL jsonb_array_elements(a.resources) d WHERE e.resource_id=$1 AND e.environment_id=$2 AND NOT e.deleted AND concat(d.value->'metadata'->>'namespace','/',d.value->>'kind','/',d.value->'metadata'->>'name')=b.reference", &[&resource,&environment]).await?;
        Ok(row.map(|row| (row.get(0), row.get(1), row.get(2))))
    }
    /// Resolve a declaration against the immutable source/dependency closure.
    /// This establishes identity only; callers still authorize the current resource.
    pub(crate) async fn resource_in(
        client: &Client,
        release: &str,
        reference: &str,
    ) -> Result<String> {
        if let Ok(id) = Uuid::parse_str(reference) {
            ensure!(id.to_string() == reference, "SOURCE_INVALID");
            return Ok(reference.to_owned());
        }
        ensure!(reference.len() <= 4096, "SOURCE_INVALID");
        let releases = client.query(
            "WITH RECURSIVE releases AS(SELECT digest,root_id,snapshot,owned FROM moria.fs_source_releases WHERE digest=$1 UNION SELECT d.digest,d.root_id,d.snapshot,d.owned FROM releases r CROSS JOIN LATERAL jsonb_array_elements(r.snapshot->'descriptor'->'dependencies') dependency JOIN moria.fs_source_releases d ON d.digest=dependency->>'digest') SELECT root_id,owned FROM releases LIMIT 257",
            &[&release],
        ).await?;
        ensure!(
            !releases.is_empty() && releases.len() <= 256,
            "SOURCE_INVALID"
        );
        let mut roots = BTreeSet::new();
        for row in releases {
            let owned: Vec<String> = serde_json::from_value(row.get(1))?;
            ensure!(owned.len() <= 2000, "SOURCE_INVALID");
            if owned.iter().any(|value| value == reference) {
                roots.insert(row.get::<_, Uuid>(0));
            }
        }
        ensure!(roots.len() == 1, "SOURCE_INVALID");
        let root = roots.into_iter().next().unwrap();
        let row = client.query_opt(
            "SELECT resource_id FROM moria.fs_resource_identities WHERE root_id=$1 AND reference=$2",
            &[&root, &reference],
        ).await?.ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
        Ok(row.get::<_, Uuid>(0).to_string())
    }
    pub async fn begin_artifact(
        &self,
        session: &VerifiedSession,
        root: Uuid,
        expected: &str,
        key: &str,
        request: super::FileWrite,
    ) -> Result<super::UploadStart> {
        digest(expected)?;
        let parent = match &request {
            super::FileWrite::Create { parent_id, .. } => *parent_id,
            _ => anyhow::bail!("SOURCE_INVALID"),
        };
        let admission = Admission::begin(&self.db, session, false).await?;
        let result=async{let state=Authority::current(admission.client(),session).await?;ensure!(state.private_purpose.is_none(),"FORBIDDEN");let scope=fingerprint(&json!({"environment":session.environment(),"principal":session.principal(),"application":session.application(),"delegation":[]}))?;
            let row=admission.client().query_opt("SELECT compiler_source FROM moria.native_filesystem_uploads WHERE scope_hash=$1 AND request_key=$2",&[&scope,&key]).await?;
            let proof=row.and_then(|row|row.get::<_,Option<Value>>(0)).map(serde_json::from_value::<CompilerUploadSource>).transpose()?;
            if let Some(proof)=&proof{ensure!(proof.root_id==root&&proof.source_digest==expected,"IDEMPOTENCY_CONFLICT");}Authority::finish(admission.client(),session,&state).await?;Ok(proof)}.await;
        let proof = admission.complete(result).await?;
        let compiler = if let Some(proof) = proof {
            (proof, vec![])
        } else {
            self.compilation_proof(session, root, expected, parent)
                .await?
        };
        super::NativeFilesystem {
            db: self.db.clone(),
        }
        .begin_file_admitted(session, key, request, None, None, Some(compiler))
        .await
    }
    pub(crate) async fn compilation_proof(
        &self,
        session: &VerifiedSession,
        root: Uuid,
        expected: &str,
        parent: Uuid,
    ) -> Result<(CompilerUploadSource, Vec<String>)> {
        digest(expected)?;
        let checkout = self.checkout(session, root).await?;
        ensure!(checkout.digest == expected, "FILESYSTEM_CONFLICT");
        let file = checkout
            .files
            .iter()
            .find(|file| file.path == format!(".{}.json", checkout.role.kind))
            .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
        let descriptor = SourceDescriptor::parse(
            &checkout.role.kind,
            &self.file_json(session, &file.entry).await?,
        )?;
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome=async{
            let client=admission.client();let state=Authority::current(client,session).await?;ensure!(state.private_purpose.is_none(),"FORBIDDEN");ensure!(Self::capture(client,session,root).await?.digest==expected,"FILESYSTEM_CONFLICT");
            ensure!(client.query_opt("WITH RECURSIVE ancestors AS(SELECT id,parent_id FROM moria.fs_entries WHERE id=$1 UNION ALL SELECT e.id,e.parent_id FROM moria.fs_entries e JOIN ancestors a ON e.id=a.parent_id) SELECT 1 FROM ancestors a JOIN moria.fs_roles r ON r.entry_id=a.id WHERE r.kind IN ('ontology','repository') AND NOT r.retired LIMIT 1",&[&parent]).await?.is_none(),"SOURCE_INVALID");
            NativeFilesystem::access(client,session,parent,"create").await?;
            let mut sources=BTreeSet::new();let mut labels=BTreeSet::new();
            sources.insert(CompilerSourceAccess{id:root,root_id:root,operation:"export".into()});
            for id in checkout.files.iter().map(|file|file.entry.id).chain(checkout.directories.iter().map(|directory|directory.id)){sources.insert(CompilerSourceAccess{id,root_id:root,operation:"read".into()});}
            let mut pending=descriptor.dependencies.iter().map(|dependency|(dependency.root_id,dependency.digest.clone())).collect::<Vec<_>>();let mut seen=BTreeSet::new();
            while let Some((expected_root,digest))=pending.pop(){if !seen.insert(digest.clone()){continue;}ensure!(seen.len()<=64,"RESOURCE_LIMIT");let (release,marks)=Self::release_in(client,session,&digest).await?;ensure!(release["rootId"]==json!(expected_root),"SOURCE_INVALID");labels.extend(marks);let snapshot=&release["snapshot"];
                sources.insert(CompilerSourceAccess{id:expected_root,root_id:expected_root,operation:"read".into()});
                for entry in snapshot["files"].as_array().ok_or_else(||anyhow::anyhow!("SOURCE_INVALID"))?{sources.insert(CompilerSourceAccess{id:identifier(entry["entry"]["id"].as_str().ok_or_else(||anyhow::anyhow!("SOURCE_INVALID"))?)?,root_id:expected_root,operation:"read".into()});}
                for entry in snapshot["directories"].as_array().ok_or_else(||anyhow::anyhow!("SOURCE_INVALID"))?{sources.insert(CompilerSourceAccess{id:identifier(entry["id"].as_str().ok_or_else(||anyhow::anyhow!("SOURCE_INVALID"))?)?,root_id:expected_root,operation:"read".into()});}
                for dependency in snapshot["descriptor"]["dependencies"].as_array().into_iter().flatten(){pending.push((identifier(dependency["rootId"].as_str().ok_or_else(||anyhow::anyhow!("SOURCE_INVALID"))?)?,dependency["digest"].as_str().ok_or_else(||anyhow::anyhow!("SOURCE_INVALID"))?.to_owned()));}
                ensure!(sources.len()<=10000,"RESOURCE_LIMIT");
            }
            for source in &sources{labels.extend(NativeFilesystem::access(client,session,source.id,&source.operation).await?);}
            ensure!(labels.len()<=256,"RESOURCE_LIMIT");Authority::finish(client,session,&state).await?;
            Ok((CompilerUploadSource{root_id:root,source_digest:expected.into(),sources:sources.into_iter().collect()},labels.into_iter().collect()))
        }.await;
        admission.complete(outcome).await
    }
    pub(super) async fn compiler_context_in(
        client: &Client,
        session: &VerifiedSession,
    ) -> Result<Value> {
        let snapshot = Authority::current(client, session).await?;
        ensure!(
            snapshot.private_purpose.as_deref() != Some("indexing"),
            "FORBIDDEN"
        );
        crate::authorization::authorize(&snapshot.identity, "filesystem", "read", &[])?;
        let row=client.query_opt("SELECT c.fingerprint,c.principal_attributes FROM moria.native_source_compiler_current p JOIN moria.native_source_compiler_contexts c USING(environment,fingerprint) WHERE p.environment=$1",&[&session.environment()]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        Ok(
            json!({"revision":row.get::<_,String>(0),"fingerprint":row.get::<_,String>(0),"principalAttributes":row.get::<_,Value>(1)}),
        )
    }
    pub(super) async fn register_compiler_context_in(
        client: &Client,
        session: &VerifiedSession,
        input: &Value,
    ) -> Result<Value> {
        ensure!(
            input.as_object().is_some_and(
                |object| object.len() == 1 && object.contains_key("principalAttributes")
            ),
            "SOURCE_INVALID"
        );
        let mut snapshot = Authority::current(client, session).await?;
        crate::authorization::authorize(&snapshot.identity, "instance", "manage", &[])?;
        let context: CompilerContext =
            serde_json::from_value(input.clone()).map_err(|_| anyhow::anyhow!("SOURCE_INVALID"))?;
        let normalized = crate::values::descriptor(
            &json!({"type":"object","properties":context.principal_attributes}),
            &Value::Null,
            0,
        )?;
        let attributes: crate::authorization::ValueMap =
            serde_json::from_value(normalized["properties"].clone())?;
        crate::authorization::CompiledExpression::compile(
            &json!({"literal":true}),
            crate::authorization::ExpressionTypes {
                attributes: attributes.clone(),
                ..Default::default()
            },
        )?;
        let fingerprint = fingerprint(&json!(attributes))?;
        let prior=client.query_opt("SELECT c.fingerprint,c.principal_attributes FROM moria.native_source_compiler_current p JOIN moria.native_source_compiler_contexts c USING(environment,fingerprint) WHERE p.environment=$1 FOR UPDATE OF p",&[&session.environment()]).await?;
        if let Some(prior) = &prior {
            let old: crate::authorization::ValueMap = serde_json::from_value(prior.get(1))?;
            ensure!(
                old.iter()
                    .all(|(name, schema)| attributes.get(name) == Some(schema)),
                "SCHEMA_CHANGED"
            );
            if prior.get::<_, String>(0) == fingerprint {
                return Self::compiler_context_in(client, session).await;
            }
        }
        client.execute("INSERT INTO moria.native_source_compiler_contexts(environment,fingerprint,principal_attributes) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",&[&session.environment(),&fingerprint,&json!(attributes)]).await?;
        client.execute("INSERT INTO moria.native_source_compiler_current(environment,fingerprint) VALUES($1,$2) ON CONFLICT(environment) DO UPDATE SET fingerprint=EXCLUDED.fingerprint",&[&session.environment(),&fingerprint]).await?;
        snapshot.security_epoch=client.query_one("UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=$1 RETURNING security_epoch::text",&[&session.environment()]).await?.get(0);
        let old = prior.as_ref().map(|row| row.get::<_, String>(0));
        client.execute("INSERT INTO moria.native_source_compiler_audit(environment,principal,application,prior_fingerprint,fingerprint,epoch) VALUES($1,$2,$3,$4,$5,$6::text::bigint)",&[&session.environment(),&session.principal(),&session.application(),&old,&fingerprint,&snapshot.security_epoch]).await?;
        Authority::finish(client, session, &snapshot).await?;
        Self::compiler_context_in(client, session).await
    }
    pub(super) async fn role(client: &Client, id: Uuid) -> Result<SourceRole> {
        let row=client.query_opt("SELECT jsonb_build_object('entryId',entry_id,'kind',kind,'projectId',project_id,'deploymentUnitId',deployment_unit_id,'namespace',namespace,'configuration',configuration,'revision',revision::text,'latestRelease',latest_release) FROM moria.fs_roles WHERE entry_id=$1 AND NOT retired AND kind IN ('ontology','repository')",&[&id]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        let role: SourceRole =
            serde_json::from_value(row.get(0)).map_err(|_| anyhow::anyhow!("SOURCE_INVALID"))?;
        SourceDescriptor::parse(&role.kind, &role.configuration)?;
        Ok(role)
    }
    pub(super) async fn capture(
        client: &Client,
        session: &VerifiedSession,
        id: Uuid,
    ) -> Result<SourceCheckout> {
        NativeFilesystem::get_in(client, session, id).await?;
        NativeFilesystem::access(client, session, id, "export").await?;
        let role = Self::role(client, id).await?;
        let rows=client.query(&format!("WITH RECURSIVE tree AS (SELECT id,parent_id,name,kind,revision,project_id,environment_id,resource_id,content,deleted,''::text COLLATE \"C\" AS path FROM moria.fs_entries WHERE id=$1 AND NOT deleted UNION ALL SELECT e.id,e.parent_id,e.name,e.kind,e.revision,e.project_id,e.environment_id,e.resource_id,e.content,e.deleted,CASE WHEN t.path='' THEN e.name ELSE t.path||'/'||e.name END FROM moria.fs_entries e JOIN tree t ON e.parent_id=t.id WHERE NOT e.deleted) SELECT path,{ENTRY} FROM tree WHERE jsonb_typeof(moria.native_filesystem_access($2,id,'read'))='array' LIMIT 2001"),&[&id,&session.session_hash()]).await?;
        ensure!(rows.len() <= 2000, "RESOURCE_LIMIT");
        let mut files = vec![];
        let mut directories = vec![];
        let mut bytes = 0u64;
        for row in rows {
            let path: String = row.get(0);
            let entry: Entry = serde_json::from_value(row.get(1))?;
            ensure!(entry.kind != "resource", "SOURCE_INVALID");
            if entry.kind == "file" {
                let size = entry
                    .content
                    .as_ref()
                    .and_then(|value| value["size"].as_u64())
                    .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
                bytes = bytes
                    .checked_add(size)
                    .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
                files.push(SourceFile { path, entry });
            } else {
                directories.push(SourceDirectory {
                    path,
                    id: entry.id,
                    revision: entry.revision,
                });
            }
        }
        ensure!(bytes <= 64 * 1024 * 1024, "RESOURCE_LIMIT");
        files.sort_by(|a, b| a.path.cmp(&b.path));
        directories.sort_by(|a, b| a.path.cmp(&b.path));
        let mut snapshot = SourceCheckout {
            version: 1,
            root_id: id,
            role,
            digest: String::new(),
            files,
            directories,
        };
        snapshot.digest = snapshot.hash()?;
        Ok(snapshot)
    }
    pub async fn checkout(&self, session: &VerifiedSession, id: Uuid) -> Result<SourceCheckout> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome = async {
            let snapshot = Authority::current(admission.client(), session).await?;
            let value = Self::capture(admission.client(), session, id).await?;
            Authority::finish(admission.client(), session, &snapshot).await?;
            Ok(value)
        }
        .await;
        admission.complete(outcome).await
    }
    async fn file_json(&self, session: &VerifiedSession, entry: &Entry) -> Result<Value> {
        ensure!(
            entry
                .content
                .as_ref()
                .and_then(|value| value["size"].as_u64())
                .is_some_and(|size| size <= 16 * 1024 * 1024),
            "SOURCE_INVALID"
        );
        let mut read = NativeFilesystem {
            db: self.db.clone(),
        }
        .read_file(session, entry.id, Some(&entry.revision), None)
        .await?;
        let mut bytes = vec![];
        while let Some(chunk) = read.stream.try_next().await? {
            ensure!(
                bytes.len() + chunk.len() <= 16 * 1024 * 1024,
                "RESOURCE_LIMIT"
            );
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("SOURCE_INVALID"))
    }
    pub async fn register(
        &self,
        session: &VerifiedSession,
        id: Uuid,
        expected_revision: &str,
        kind: &str,
    ) -> Result<SourceRole> {
        revision(expected_revision)?;
        ensure!(["ontology", "repository"].contains(&kind), "SOURCE_INVALID");
        let admission = Admission::begin(&self.db, session, false).await?;
        let candidate=async {let client=admission.client();let snapshot=Authority::current(client,session).await?;NativeFilesystem::access(client,session,id,"register").await?;let root=NativeFilesystem::entry(client,id,false).await?;ensure!(root.kind=="directory"&&root.project_id.as_deref().is_some_and(|value|identifier(value).is_ok())&&root.environment_id.is_none(),"SOURCE_INVALID");let row=client.query_opt("SELECT id FROM moria.fs_entries WHERE parent_id=$1 AND name=$2 AND kind='file' AND NOT deleted",&[&id,&format!(".{kind}.json")]).await?.ok_or_else(||anyhow::anyhow!("SOURCE_INVALID"))?;let descriptor=NativeFilesystem::get_in(client,session,row.get(0)).await?.0;Authority::finish(client,session,&snapshot).await?;Ok(descriptor)}.await;
        let file = admission.complete(candidate).await?;
        let descriptor = SourceDescriptor::parse(kind, &self.file_json(session, &file).await?)?;
        let admission = Admission::begin_filesystem(&self.db, session, true, true).await?;
        let outcome=async {let client=admission.client();let snapshot=Authority::current(client,session).await?;NativeFilesystem::access(client,session,id,"register").await?;let root=NativeFilesystem::entry(client,id,false).await?;let current=NativeFilesystem::get_in(client,session,file.id).await?.0;ensure!(root.revision==expected_revision&&current.revision==file.revision,"FILESYSTEM_CONFLICT");let nested=client.query_opt("WITH RECURSIVE ancestors AS(SELECT id,parent_id FROM moria.fs_entries WHERE id=$1 UNION ALL SELECT e.id,e.parent_id FROM moria.fs_entries e JOIN ancestors a ON e.id=a.parent_id),descendants AS(SELECT id FROM moria.fs_entries WHERE id=$1 UNION ALL SELECT e.id FROM moria.fs_entries e JOIN descendants d ON e.parent_id=d.id WHERE NOT e.deleted) SELECT 1 FROM moria.fs_roles WHERE NOT retired AND kind IN ('ontology','repository') AND (entry_id IN(SELECT id FROM ancestors) OR entry_id IN(SELECT id FROM descendants)) LIMIT 1",&[&id]).await?;ensure!(nested.is_none(),"SOURCE_INVALID");client.execute("INSERT INTO moria.fs_roles(entry_id,kind,project_id,deployment_unit_id,namespace,configuration) VALUES($1,$2,$3::text::uuid,$4,$5,$6)",&[&id,&kind,&root.project_id,&Uuid::new_v4(),&descriptor.namespace,&json!(descriptor)]).await?;Authority::finish(client,session,&snapshot).await?;Self::role(client,id).await}.await;
        admission
            .complete(outcome.map_err(super::mutation_error))
            .await
    }
    pub(super) async fn release_in(
        client: &Client,
        session: &VerifiedSession,
        requested: &str,
    ) -> Result<(Value, Vec<String>)> {
        digest(requested)?;
        let mut pending = vec![(requested.to_owned(), BTreeSet::<String>::new())];
        let mut releases = BTreeMap::new();
        let mut consumed = BTreeSet::new();
        while let Some((digest_value, mut ancestors)) = pending.pop() {
            ensure!(!ancestors.contains(&digest_value), "SOURCE_INVALID");
            if releases.contains_key(&digest_value) {
                continue;
            }
            ensure!(releases.len() < 64, "RESOURCE_LIMIT");
            ancestors.insert(digest_value.clone());
            let row=client.query_opt("SELECT root_id,snapshot,compiled,owned,markings FROM moria.fs_source_releases WHERE digest=$1",&[&digest_value]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
            let root: Uuid = row.get(0);
            NativeFilesystem::get_in(client, session, root).await?;
            Self::role(client, root).await?;
            let stored_snapshot: Value = row.get(1);
            let descriptor_value = stored_snapshot["descriptor"].clone();
            let mut snapshot_value = stored_snapshot.clone();
            snapshot_value
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?
                .remove("descriptor");
            let snapshot: SourceCheckout = serde_json::from_value(snapshot_value)
                .map_err(|_| anyhow::anyhow!("SOURCE_INVALID"))?;
            ensure!(
                snapshot.version == 1
                    && snapshot.root_id == root
                    && snapshot.digest == snapshot.hash()?
                    && snapshot.files.len() + snapshot.directories.len() <= 2000,
                "SOURCE_INVALID"
            );
            let descriptor = SourceDescriptor::parse(&snapshot.role.kind, &descriptor_value)?;
            ensure!(
                descriptor.namespace == snapshot.role.namespace,
                "SOURCE_INVALID"
            );
            let mut labels = BTreeSet::new();
            for directory in &snapshot.directories {
                labels
                    .extend(NativeFilesystem::access(client, session, directory.id, "read").await?);
            }
            for file in &snapshot.files {
                labels.extend(
                    NativeFilesystem::access(client, session, file.entry.id, "read").await?,
                );
            }
            let markings: Value = row.get(4);
            for mark in markings
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?
            {
                ensure!(mark["environment"] == session.environment(), "FORBIDDEN");
                labels.insert(
                    mark["marking"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?
                        .to_owned(),
                );
            }
            let state = Authority::current(client, session).await?;
            crate::authorization::authorize(
                &state.identity,
                "filesystem",
                "read",
                &labels.iter().cloned().collect::<Vec<_>>(),
            )?;
            consumed.extend(labels);
            let compiled: CompiledManifest = serde_json::from_value(row.get(2))
                .map_err(|_| anyhow::anyhow!("SOURCE_INVALID"))?;
            ensure!(
                compiled.fingerprint == fingerprint(&compiled.manifest)?
                    && digest_value
                        == fingerprint(
                            &json!({"source":snapshot.digest,"compiled":compiled.fingerprint})
                        )?,
                "SOURCE_INVALID"
            );
            let owned: Value = row.get(3);
            let release = json!({"digest":digest_value,"rootId":root,"snapshot":stored_snapshot,"compiled":compiled,"owned":owned,"markings":markings});
            for dependency in &descriptor.dependencies {
                let expected: Uuid = client
                    .query_opt(
                        "SELECT root_id FROM moria.fs_source_releases WHERE digest=$1",
                        &[&dependency.digest],
                    )
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?
                    .get(0);
                ensure!(
                    expected == dependency.root_id && expected != root,
                    "SOURCE_INVALID"
                );
                pending.push((dependency.digest.clone(), ancestors.clone()));
            }
            releases.insert(digest_value, release);
        }
        Ok((
            releases
                .remove(requested)
                .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?,
            consumed.into_iter().collect(),
        ))
    }
    pub async fn release(&self, session: &VerifiedSession, digest: &str) -> Result<Value> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome = async {
            let state = Authority::current(admission.client(), session).await?;
            let value = Self::release_in(admission.client(), session, digest)
                .await?
                .0;
            Authority::finish(admission.client(), session, &state).await?;
            Ok(value)
        }
        .await;
        admission.complete(outcome).await
    }
    async fn runtime_status_in(
        &self,
        client: &Client,
        session: &VerifiedSession,
        root: Uuid,
    ) -> Result<crate::source_runtime::RuntimeStatus> {
        let state = Authority::current(client, session).await?;
        crate::authorization::authorize(&state.identity, "instance", "manage", &[])?;
        NativeFilesystem::get_in(client, session, root).await?;
        Self::role(client, root).await?;
        let row=client.query_opt("SELECT digest,resources FROM moria.fs_active_releases WHERE root_id=$1 AND environment=$2", &[&root,&session.environment()]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        let release = Self::release_in(client, session, &row.get::<_, String>(0))
            .await?
            .0;
        let resources: Vec<Value> = serde_json::from_value(row.get(1))?;
        let status =
            crate::source_runtime::status_in(&self.db, client, session, &release, &resources)
                .await?;
        Authority::finish(client, session, &state).await?;
        Ok(status)
    }
    pub async fn runtime_status(&self, session: &VerifiedSession, root: Uuid) -> Result<Value> {
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.runtime_status_attempt(session, root),
        )
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")))
    }
    async fn runtime_status_attempt(&self, session: &VerifiedSession, root: Uuid) -> Result<Value> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome = self
            .runtime_status_in(admission.client(), session, root)
            .await;
        let captured = admission.complete(outcome).await?;
        let mut available = BTreeMap::new();
        for (reference, artifact, adapter) in &captured.artifacts {
            let ready = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                crate::sandbox::preloaded_artifact_available(artifact, adapter.as_ref()),
            )
            .await
            .map_err(|_| anyhow::anyhow!("QUERY_TIMEOUT"))??;
            available.insert(reference.clone(), ready);
        }
        for (reference, resource, artifact) in &captured.frontends {
            let ready = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                crate::app_host::NativeAppHost::frontend_available(*resource, artifact),
            )
            .await
            .map_err(|_| anyhow::anyhow!("QUERY_TIMEOUT"))??;
            available.insert(reference.clone(), ready);
        }
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome=async {
            let mut current=self.runtime_status_in(admission.client(),session,root).await?;
            ensure!(current.anchor==captured.anchor,"SOURCE_RUNTIME_CHANGED");
            for component in current.value["components"].as_array_mut().unwrap() {
                if let Some(ready)=available.get(component["reference"].as_str().unwrap()) {
                    component["readiness"]=json!({"ready":ready,"reasons":if *ready{vec![]}else{vec!["ARTIFACT_VERIFICATION_REQUIRED"]}});
                }
            }
            if available.values().any(|ready|!*ready){current.value["ready"]=false.into();}
            Ok(current.value)
        }.await;
        admission.complete(outcome).await
    }
    pub async fn publish(
        &self,
        session: &VerifiedSession,
        id: Uuid,
        expected: &str,
        artifact_ref: &CompilerArtifactRef,
    ) -> Result<Value> {
        static COMPILATIONS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
            std::sync::OnceLock::new();
        let permit = COMPILATIONS
            .get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(4)))
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow::anyhow!("RESOURCE_BUSY"))?;
        digest(expected)?;
        digest(&artifact_ref.digest)?;
        revision(&artifact_ref.revision)?;
        let checkout = self.checkout(session, id).await?;
        ensure!(checkout.digest == expected, "FILESYSTEM_CONFLICT");
        ensure!(
            !checkout
                .files
                .iter()
                .any(|file| file.entry.id == artifact_ref.entry_id)
                && !checkout
                    .directories
                    .iter()
                    .any(|directory| directory.id == artifact_ref.entry_id),
            "SOURCE_INVALID"
        );
        let admission = Admission::begin(&self.db, session, false).await?;
        let candidate = async {
            NativeFilesystem::access(admission.client(),session,id,"publish").await?;
            let entry =
                NativeFilesystem::get_in(admission.client(), session, artifact_ref.entry_id)
                    .await?
                    .0;
            ensure!(
                entry.kind == "file"
                    && entry.revision == artifact_ref.revision
                    && entry
                        .content
                        .as_ref()
                        .is_some_and(|content| content["digest"] == artifact_ref.digest),
                "FILESYSTEM_CONFLICT"
            );
            ensure!(admission.client().query_opt("SELECT 1 FROM moria.native_compiler_artifacts WHERE artifact=$1 AND root=$2 AND source_digest=$3",&[&entry.id,&id,&expected]).await?.is_some(),"SOURCE_INVALID");
            Ok(entry)
        }
        .await;
        let entry = admission.complete(candidate).await?;
        let artifact: CompiledSourceArtifact =
            serde_json::from_value(self.file_json(session, &entry).await?)
                .map_err(|_| anyhow::anyhow!("SOURCE_INVALID"))?;
        ensure!(
            artifact.version == 1
                && artifact.root_id == id
                && artifact.source_digest == expected
                && artifact.compiled.policy_types_checked,
            "SOURCE_INVALID"
        );
        let artifact = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            validation::validate(
                &artifact.compiled.manifest,
                &artifact.compiler_context.principal_attributes,
            )
            .map_err(|_| anyhow::anyhow!("SOURCE_INVALID"))?;
            Ok::<_, anyhow::Error>(artifact)
        })
        .await
        .map_err(|_| anyhow::anyhow!("SOURCE_INVALID"))??;
        let descriptor = SourceDescriptor::parse(&checkout.role.kind, &json!(artifact.descriptor))?;
        ensure!(
            descriptor.namespace == checkout.role.namespace
                && descriptor.runtime
                    == SourceDescriptor::parse(&checkout.role.kind, &checkout.role.configuration)?
                        .runtime,
            "SOURCE_INVALID"
        );
        let mut declared = vec![
            json!({"apiVersion":"moria/v1alpha2","kind":"Namespace","metadata":{"namespace":"_project","name":descriptor.namespace},"spec":{"description":descriptor.name}}),
        ];
        let mut found_descriptor = false;
        let mut resource_files: BTreeMap<String, BTreeSet<Uuid>> = BTreeMap::new();
        for file in &checkout.files {
            if file.path.ends_with(".ontology.json") || file.path.ends_with(".repository.json") {
                ensure!(
                    file.path == format!(".{}.json", checkout.role.kind),
                    "SOURCE_INVALID"
                );
            }
            if file.path == format!(".{}.json", checkout.role.kind) {
                let actual = SourceDescriptor::parse(
                    &checkout.role.kind,
                    &self.file_json(session, &file.entry).await?,
                )?;
                ensure!(
                    fingerprint(&json!(actual))? == fingerprint(&json!(descriptor))?,
                    "SOURCE_INVALID"
                );
                found_descriptor = true;
                continue;
            }
            let extension = ["object", "link", "action", "resource"]
                .into_iter()
                .find(|extension| file.path.ends_with(&format!(".{extension}.json")));
            if let Some(extension) = extension {
                let resource = self.file_json(session, &file.entry).await?;
                ensure!(
                    resource["metadata"]["namespace"] == descriptor.namespace,
                    "SOURCE_INVALID"
                );
                if extension == "resource" {
                    ensure!(
                        ![
                            "Namespace",
                            "ObjectType",
                            "LinkType",
                            "Action",
                            "Function",
                            "Transform",
                            "Agent",
                            "AgentAdapter",
                            "ConnectorImplementation"
                        ]
                        .contains(&resource["kind"].as_str().unwrap_or_default()),
                        "SOURCE_INVALID"
                    );
                } else {
                    let expected = match extension {
                        "object" => "ObjectType",
                        "link" => "LinkType",
                        _ => "Action",
                    };
                    ensure!(
                        checkout.role.kind == "ontology" && resource["kind"] == expected,
                        "SOURCE_INVALID"
                    );
                }
                resource_files
                    .entry(validation::reference(&resource)?)
                    .or_default()
                    .insert(file.entry.id);
                declared.push(resource);
            } else {
                let mut read = NativeFilesystem {
                    db: self.db.clone(),
                }
                .read_file(session, file.entry.id, Some(&file.entry.revision), None)
                .await?;
                while read.stream.try_next().await?.is_some() {}
            }
        }
        ensure!(found_descriptor, "SOURCE_INVALID");
        let manifest = &artifact.compiled.manifest;
        ensure!(
            manifest["projectId"] == json!(checkout.role.project_id)
                && manifest["deploymentUnitId"] == json!(checkout.role.deployment_unit_id),
            "SOURCE_INVALID"
        );
        let owned_set = artifact.owned.iter().cloned().collect::<BTreeSet<_>>();
        ensure!(
            owned_set.len() == artifact.owned.len() && artifact.owned.len() <= 2001,
            "SOURCE_INVALID"
        );
        let declared_set = declared
            .iter()
            .map(validation::reference)
            .collect::<Result<BTreeSet<_>>>()?;
        ensure!(declared_set.len() == declared.len(), "DUPLICATE_RESOURCE");
        for resource in manifest["resources"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?
        {
            let reference = validation::reference(resource)?;
            if !owned_set.contains(&reference) || declared_set.contains(&reference) {
                continue;
            }
            let kind = resource["kind"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
            ensure!(
                checkout.role.kind == "repository"
                    && resource["metadata"]["namespace"] == descriptor.namespace
                    && [
                        "Function",
                        "Transform",
                        "Agent",
                        "AgentAdapter",
                        "ConnectorImplementation"
                    ]
                    .contains(&kind),
                "SOURCE_INVALID"
            );
            if kind == "Transform" {
                ensure!(
                    descriptor.runtime.as_deref() == Some("python-duckdb-v1"),
                    "SOURCE_INVALID"
                );
            } else if kind != "ConnectorImplementation" {
                ensure!(
                    descriptor.runtime.as_deref() == Some("typescript-v1"),
                    "SOURCE_INVALID"
                );
            }
            declared.push(resource.clone());
        }
        let owned = declared
            .iter()
            .map(validation::reference)
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            owned.iter().cloned().collect::<BTreeSet<_>>() == owned_set,
            "SOURCE_INVALID"
        );
        let context_fingerprint =
            fingerprint(&json!(artifact.compiler_context.principal_attributes))?;
        ensure!(
            artifact.compiled.attribute_types_fingerprint.as_deref() == Some(&context_fingerprint)
                && artifact.compiled.fingerprint == fingerprint(manifest)?,
            "SOURCE_INVALID"
        );
        let order = manifest["resources"]
            .as_array()
            .unwrap()
            .iter()
            .map(validation::reference)
            .collect::<Result<Vec<_>>>()?;
        let mut sorted = order.clone();
        sorted.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
        ensure!(sorted == artifact.compiled.order, "SOURCE_INVALID");
        let descriptor_id = checkout
            .files
            .iter()
            .find(|file| file.path == format!(".{}.json", checkout.role.kind))
            .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?
            .entry
            .id;
        for reference in &owned {
            let source = resource_files.entry(reference.clone()).or_default();
            if checkout.role.kind == "repository" && !declared_set.contains(reference) {
                source.extend(
                    checkout
                        .files
                        .iter()
                        .filter(|file| {
                            !["object", "link", "action", "resource"]
                                .iter()
                                .any(|extension| file.path.ends_with(&format!(".{extension}.json")))
                        })
                        .map(|file| file.entry.id),
                );
            }
            source.insert(descriptor_id);
        }
        let release_digest =
            fingerprint(&json!({"source":expected,"compiled":artifact.compiled.fingerprint}))?;
        let admission = Admission::begin_filesystem(&self.db, session, true, true).await?;
        let outcome=async {
            let client=admission.client();let state=Authority::current(client,session).await?;NativeFilesystem::access(client,session,id,"publish").await?;
            ensure!(Self::capture(client,session,id).await?.digest==expected,"FILESYSTEM_CONFLICT");
            let current_artifact=NativeFilesystem::get_in(client,session,entry.id).await?.0;ensure!(current_artifact.revision==entry.revision&&current_artifact.content==entry.content,"FILESYSTEM_CONFLICT");
            let registered=client.query_opt("SELECT principal_attributes FROM moria.native_source_compiler_contexts WHERE environment=$1 AND fingerprint=$2",&[&session.environment(),&context_fingerprint]).await?.ok_or_else(||anyhow::anyhow!("SCHEMA_CHANGED"))?;
            ensure!(registered.get::<_,Value>(0)==json!(artifact.compiler_context.principal_attributes),"SCHEMA_CHANGED");
            let current_context=Self::compiler_context_in(client,session).await?;let current=fields_context(&current_context["principalAttributes"])?;ensure!(artifact.compiler_context.principal_attributes.iter().all(|(key,value)|current.get(key)==Some(value)),"SCHEMA_CHANGED");
            let mut combined=declared.clone();let mut resources=declared.iter().map(|resource|Ok((validation::reference(resource)?,resource.clone()))).collect::<Result<BTreeMap<_,_>>>()?;let mut labels=BTreeSet::new();
            for file in &checkout.files{labels.extend(NativeFilesystem::access(client,session,file.entry.id,"read").await?);}for directory in &checkout.directories{labels.extend(NativeFilesystem::access(client,session,directory.id,"read").await?);}
            labels.extend(NativeFilesystem::access(client,session,entry.id,"read").await?);
            for dependency in &descriptor.dependencies {
                let (release,marks)=Self::release_in(client,session,&dependency.digest).await?;ensure!(release["rootId"]==json!(dependency.root_id)&&dependency.root_id!=id,"SOURCE_INVALID");labels.extend(marks);
                for resource in release["compiled"]["manifest"]["resources"].as_array().ok_or_else(||anyhow::anyhow!("SOURCE_INVALID"))?{let reference=validation::reference(resource)?;if let Some(previous)=resources.get(&reference){ensure!(!owned_set.contains(&reference)&&fingerprint(previous)?==fingerprint(resource)?,"SOURCE_INVALID");}else{resources.insert(reference,resource.clone());combined.push(resource.clone());}}
            }
            let expected_manifest=json!({"apiVersion":"moria/v1alpha2","projectId":checkout.role.project_id,"deploymentUnitId":checkout.role.deployment_unit_id,"resources":combined});
            ensure!(fingerprint(&expected_manifest)?==artifact.compiled.fingerprint,"SOURCE_INVALID");
            for resource in manifest["resources"].as_array().unwrap(){for marking in resource["metadata"]["markings"].as_array().into_iter().flatten(){labels.insert(marking.as_str().ok_or_else(||anyhow::anyhow!("SOURCE_INVALID"))?.to_owned());}}
            crate::authorization::authorize(&state.identity,"filesystem","publish",&labels.iter().cloned().collect::<Vec<_>>())?;
            let markings=labels.into_iter().map(|marking|json!({"environment":session.environment(),"marking":marking})).collect::<Vec<_>>();
            let mut snapshot=json!(checkout);snapshot.as_object_mut().unwrap().insert("descriptor".into(),json!(descriptor));
            let created_by=client.query_one("SELECT moria.native_serving_session($1)->>'instanceId'",&[&session.session_hash()]).await?.get::<_,Option<String>>(0).ok_or_else(||anyhow::anyhow!("FORBIDDEN"))?;
            client.execute("INSERT INTO moria.fs_source_releases(digest,root_id,snapshot,compiled,owned,markings,created_by) VALUES($1,$2,$3,$4,$5,$6,$7::text::uuid) ON CONFLICT(digest) DO NOTHING",&[&release_digest,&id,&snapshot,&json!(artifact.compiled),&json!(owned),&json!(markings),&created_by]).await?;
            for (reference,files) in &resource_files {for file in files {
                client.execute("INSERT INTO moria.fs_source_resource_files(digest,reference,entry_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",&[&release_digest,&reference,&file]).await?;
            }}
            for reference in &owned{client.execute("INSERT INTO moria.fs_resource_identities(root_id,reference,resource_id) VALUES($1,$2,$3) ON CONFLICT(root_id,reference) DO NOTHING",&[&id,&reference,&Uuid::new_v4()]).await?;}
            client.execute("UPDATE moria.fs_roles SET latest_release=$2 WHERE entry_id=$1",&[&id,&release_digest]).await?;Authority::finish(client,session,&state).await?;
            let rows=client.query("SELECT reference,resource_id FROM moria.fs_resource_identities WHERE root_id=$1 AND reference=ANY($2::text[]) ORDER BY reference COLLATE \"C\"",&[&id,&owned]).await?;
            Ok(json!({"rootId":id,"digest":release_digest,"sourceDigest":expected,"resources":rows.iter().map(|row|json!({"reference":row.get::<_,String>(0),"id":row.get::<_,Uuid>(1)})).collect::<Vec<_>>()}))
        }.await;
        admission.complete(outcome).await
    }
    pub async fn retire(
        &self,
        session: &VerifiedSession,
        id: Uuid,
        expected: &str,
    ) -> Result<Value> {
        revision(expected)?;
        let admission = Admission::begin_filesystem(&self.db, session, true, true).await?;
        let outcome=async {let client=admission.client();let mut snapshot=Authority::current(client,session).await?;NativeFilesystem::access(client,session,id,"administer").await?;NativeFilesystem::access(client,session,id,"publish").await?;let role=Self::role(client,id).await?;ensure!(role.revision==expected,"FILESYSTEM_CONFLICT");if client.query_opt("SELECT 1 FROM moria.fs_resource_bindings WHERE root_id=$1 AND environment<>$2 AND active LIMIT 1",&[&id,&session.environment()]).await?.is_some(){crate::authorization::authorize(&snapshot.identity,"instance","manage",&[])?;}
            let depends=client.query_opt("WITH RECURSIVE dependencies AS(SELECT a.root_id AS active_root,r.root_id,r.digest,r.snapshot FROM moria.fs_active_releases a JOIN moria.fs_roles role ON role.entry_id=a.root_id AND NOT role.retired JOIN moria.fs_source_releases r ON r.digest=a.digest UNION SELECT p.active_root,d.root_id,d.digest,d.snapshot FROM dependencies p CROSS JOIN LATERAL jsonb_array_elements(p.snapshot->'descriptor'->'dependencies') dependency JOIN moria.fs_source_releases d ON d.digest=dependency->>'digest') SELECT 1 FROM dependencies WHERE root_id=$1 AND active_root<>$1::uuid LIMIT 1",&[&id]).await?;ensure!(depends.is_none(),"SOURCE_INVALID");
            client.execute("UPDATE moria.fs_roles SET retired=true,revision=revision+1 WHERE entry_id=$1",&[&id]).await?;client.execute("UPDATE moria.fs_resource_bindings SET active=false WHERE root_id=$1",&[&id]).await?;snapshot.instance_epoch=super::governance::audit(client,session,"retireSource",&id.to_string()).await?;Authority::finish(client,session,&snapshot).await?;Ok(json!({"entryId":id,"retired":true}))
        }.await;
        admission.complete(outcome).await
    }
}
fn fields_context(value: &Value) -> Result<crate::authorization::ValueMap> {
    serde_json::from_value(value.clone()).map_err(|_| anyhow::anyhow!("SOURCE_INVALID"))
}
