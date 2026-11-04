use crate::{
    authority::{Authority, VerifiedSession},
    canonical::fingerprint,
    database::Database,
    filesystem::{FilesystemEvidence, FilesystemRequest, NativeFilesystem},
    functions::{ChildProgress, FunctionPolicy, NativeFunctions},
    sandbox::{ConsumedMarking, HttpCall, HttpResponse, PlatformBroker, SandboxContext},
    serving::{Admission, NativeServing, ReadEvidence, error_code},
    values,
};
use anyhow::{Result, bail, ensure};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio_postgres::{Client, Transaction};
use uuid::Uuid;
#[path = "platform_links.rs"]
mod links;

#[derive(Clone, Copy)]
enum LedgerIntent {
    Artifact,
    Control,
}

#[derive(Clone)]
pub struct NativePlatformBroker {
    db: Database,
}
impl NativePlatformBroker {
    fn status(code: &str) -> u16 {
        match code {
            "NOT_FOUND" => 404,
            "UNAUTHENTICATED" => 401,
            "FORBIDDEN" | "PROPERTY_FORBIDDEN" | "MARKING_FLOW" | "FUNCTION_EFFECT" => 403,
            "CURSOR_EXPIRED" | "RESULT_EXPIRED" => 410,
            "RESOURCE_LIMIT" => 429,
            "UNSUPPORTED_CAPABILITY" | "RESPONSE_TOO_LARGE" => 422,
            "FUNCTION_INPUT_CHANGED"
            | "SOURCE_FILE_PROOF_REQUIRED"
            | "FUNCTION_OBSERVATION_INCOMPLETE"
            | "LINK_CARDINALITY"
            | "LINK_CONSTRAINT_CHANGED"
            | "LINK_CONSTRAINT_NOT_READY"
            | "IDEMPOTENCY_CONFLICT"
            | "ACTION_CONDITION_FAILED"
            | "FILESYSTEM_CONFLICT"
            | "FILESYSTEM_LEASE_INVALID"
            | "CONTENT_MISMATCH"
            | "SCHEMA_CHANGED"
            | "AUTHORIZATION_CHANGED" => 409,
            "QUERY_TIMEOUT" => 504,
            "INTERNAL" | "POLICY_MISMATCH" | "BINDING_INVALID" => 500,
            _ => 400,
        }
    }
    async fn http_content(
        &self,
        context: &SandboxContext,
        mut call: HttpCall,
    ) -> Result<HttpResponse> {
        use crate::filesystem::{ContentDeclaration, FileWrite, UploadStart};
        context.check().await?;
        let session = self.session(context).await?;
        let mut headers = BTreeMap::new();
        for (name, value) in call.headers {
            ensure!(
                headers.insert(name.to_ascii_lowercase(), value).is_none(),
                "FUNCTION_HTTP"
            );
        }
        call.headers = headers;
        ensure!(
            call.headers.keys().all(|key| [
                "content-type",
                "idempotency-key",
                "accept",
                "x-moria-value-version",
                "x-moria-size",
                "x-moria-digest"
            ]
            .contains(&key.as_str())),
            "FUNCTION_HTTP"
        );
        ensure!(
            call.headers
                .get("x-moria-value-version")
                .is_none_or(|value| value == "1"),
            "UNSUPPORTED_VALUE_VERSION"
        );
        ensure!(
            !call.path.contains('#') && !call.path.contains('\\') && !call.path.starts_with("//"),
            "FUNCTION_HTTP"
        );
        let uri: axum::http::Uri = call
            .path
            .parse()
            .map_err(|_| anyhow::anyhow!("FUNCTION_HTTP"))?;
        ensure!(
            uri.scheme().is_none() && uri.authority().is_none(),
            "FUNCTION_HTTP"
        );
        let mut parts = uri
            .path()
            .split('/')
            .filter(|part| !part.is_empty())
            .map(crate::serving::decode)
            .collect::<Result<Vec<_>>>()?;
        if parts.first().is_some_and(|part| part == "api") {
            parts.remove(0);
        }
        ensure!(
            parts.len() == 6 && parts[..4] == ["management", "v1", "fs", "entries"],
            "NOT_FOUND"
        );
        let id = Uuid::parse_str(&parts[4]).map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?;
        ensure!(id.to_string() == parts[4], "INPUT_INVALID");
        let query = axum::extract::Query::<Vec<(String, String)>>::try_from_uri(&uri)
            .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
            .0;
        let write = call.method == "POST";
        ensure!(!write || context.effect() == "readWrite", "FUNCTION_EFFECT");
        let admission = Admission::begin(&self.db, &session, false).await?;
        let preflight=async {
            let client=admission.client();context.check_in(client).await?;
            let root_parts=vec!["entries".into(),id.to_string()];let request=FilesystemRequest{method:"GET",parts:&root_parts,query:&[],input:&Value::Null,key:None,consumed:&[]};
            client.batch_execute("SAVEPOINT native_file_root").await?;
            let observed=NativeFilesystem::route_in(client,&session,&request).await;
            let callback=match observed {
                Ok(value)=>{client.batch_execute("RELEASE SAVEPOINT native_file_root").await?;let(evidence,labels)=NativeFilesystem::evidence(client,&session,&request,&value).await?;let labels=Self::labels(session.environment(),labels);context.record_consumed_markings(&labels)?;Self::record_filesystem(client,&session,context.lease().execution,Some(context.attempt_id()),&format!("file-root/{}",call.id),if write{std::slice::from_ref(&evidence)}else{&[]},&labels).await?;Ok(value)},
                Err(error)=>{client.batch_execute("ROLLBACK TO SAVEPOINT native_file_root; RELEASE SAVEPOINT native_file_root").await?;let proof=NativeFilesystem::failed_evidence(&request,error_code(&error));Self::record_filesystem(client,&session,context.lease().execution,Some(context.attempt_id()),&format!("file-failure/{}",call.id),&[proof],&context.consumed_markings()).await?;Err(error)},
            };
            if callback.is_ok(){Self::trace(client,context,&call,None,&Ok(Value::Null),true).await?;}else{Self::trace(client,context,&call,None,&callback,false).await?;}context.check_in(client).await?;Ok(callback)
        }.await;
        admission.complete(preflight).await??;
        let filesystem = NativeFilesystem {
            db: self.db.clone(),
        };
        let result = async {
            if call.method == "GET" && parts[5] == "content" {
                ensure!(
                    call.body.is_none()
                        && query.len() <= 1
                        && query.iter().all(|(key, _)| key == "revision"),
                    "INPUT_INVALID"
                );
                let mut read = filesystem
                    .read_file(
                        &session,
                        id,
                        query.first().map(|(_, value)| value.as_str()),
                        Some(context.clone()),
                    )
                    .await?;
                let content = read
                    .entry
                    .content
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("CONTENT_MISMATCH"))?;
                ensure!(
                    content["size"].as_u64().is_some_and(|size| size <= 262144),
                    "FUNCTION_HTTP_LIMIT"
                );
                let mut bytes = Vec::new();
                while let Some(chunk) = futures::TryStreamExt::try_next(&mut read.stream).await? {
                    ensure!(bytes.len() + chunk.len() <= 262144, "FUNCTION_HTTP_LIMIT");
                    bytes.extend_from_slice(&chunk);
                }
                return Ok(HttpResponse {
                    status: 200,
                    headers: BTreeMap::from([
                        (
                            "content-type".into(),
                            content["mediaType"]
                                .as_str()
                                .unwrap_or("application/octet-stream")
                                .into(),
                        ),
                        (
                            "x-moria-digest".into(),
                            content["digest"].as_str().unwrap_or("").into(),
                        ),
                        ("x-moria-revision".into(), read.entry.revision),
                    ]),
                    body: STANDARD.encode(bytes),
                });
            }
            ensure!(
                write && ["content", "files"].contains(&parts[5].as_str()),
                "NOT_FOUND"
            );
            let text = |name: &str| {
                call.headers
                    .get(name)
                    .map(String::as_str)
                    .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))
            };
            let bytes = STANDARD
                .decode(call.body.as_deref().unwrap_or(""))
                .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?;
            ensure!(bytes.len() <= 32768, "FUNCTION_HTTP_LIMIT");
            let size = text("x-moria-size")?
                .parse::<u64>()
                .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?;
            ensure!(
                size.to_string() == text("x-moria-size")? && size == bytes.len() as u64,
                "CONTENT_MISMATCH"
            );
            let content = ContentDeclaration {
                digest: text("x-moria-digest")?.into(),
                size,
                media_type: text("content-type")?.into(),
            };
            let expected = query
                .iter()
                .find(|(key, _)| key == "expectedRevision")
                .map(|(_, value)| value.clone())
                .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
            let request = if parts[5] == "files" {
                ensure!(
                    query.len() == 2
                        && query
                            .iter()
                            .all(|(key, _)| ["name", "expectedRevision"].contains(&key.as_str())),
                    "INPUT_INVALID"
                );
                FileWrite::Create {
                    parent_id: id,
                    expected_parent_revision: expected,
                    name: query
                        .iter()
                        .find(|(key, _)| key == "name")
                        .map(|(_, value)| value.clone())
                        .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?,
                    content,
                }
            } else {
                ensure!(query.len() == 1, "INPUT_INVALID");
                FileWrite::Write {
                    id,
                    expected_revision: expected,
                    content,
                }
            };
            let key = Self::action_key(
                context.lease().execution,
                call.headers.get("idempotency-key").unwrap_or(&call.id),
                "filesystem/content",
            )?;
            let value = match filesystem
                .begin_file(
                    &session,
                    &key,
                    request,
                    Some(context.clone()),
                    Some(call.clone()),
                )
                .await?
            {
                UploadStart::Receipt(value) => value,
                UploadStart::Upload(mut upload) => {
                    if let Err(error) = upload.append(&bytes).await {
                        let _ = upload.abort().await;
                        return Err(error);
                    }
                    upload.finish().await?
                }
            };
            Ok(HttpResponse {
                status: 200,
                headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
                body: STANDARD.encode(serde_json::to_vec(&value)?),
            })
        }
        .await;
        // Writes seal their successful trace inside the metadata receipt transaction. Reads and
        // failed transfers retain a completed observation before the guest receives a response.
        if !write || result.is_err() {
            let admission = Admission::begin(&self.db, &session, false).await?;
            let traced=async {let client=admission.client();context.check_in(client).await?;let value=result.as_ref().map(|response|json!({"status":response.status,"body":response.body,"headers":response.headers})).map_err(|error|anyhow::anyhow!(error_code(error)));Self::trace(client,context,&call,None,&value,false).await?;context.check_in(client).await?;Ok(())}.await;
            admission.complete(traced).await?;
        }
        result
    }
    pub(crate) async fn trace(
        client: &Client,
        context: &SandboxContext,
        call: &HttpCall,
        action: Option<(&str, &str)>,
        response: &Result<Value>,
        pending: bool,
    ) -> Result<()> {
        let code = response.as_ref().err().map(error_code);
        let status = if pending {
            102
        } else {
            code.map_or(200, Self::status)
        } as i32;
        let response_hash = match response {
            Ok(value) => fingerprint(value)?,
            Err(_) => fingerprint(&json!({"error":code}))?,
        };
        let request_hash = fingerprint(
            &json!({"method":call.method,"path":call.path,"headers":call.headers,"body":call.body}),
        )?;
        let action_name = action.map(|(action, _)| action);
        let action_key = action.map(|(_, key)| key);
        let row=client.query_one("INSERT INTO moria.native_function_calls(execution,call_id,attempt,method,path,request_hash,action,action_key,status,error,response_hash,markings) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT(execution,call_id) DO UPDATE SET request_hash=moria.native_function_calls.request_hash,status=CASE WHEN moria.native_function_calls.status=102 THEN EXCLUDED.status ELSE moria.native_function_calls.status END,error=CASE WHEN moria.native_function_calls.status=102 THEN EXCLUDED.error ELSE moria.native_function_calls.error END,response_hash=CASE WHEN moria.native_function_calls.status=102 THEN EXCLUDED.response_hash ELSE moria.native_function_calls.response_hash END,markings=CASE WHEN moria.native_function_calls.status=102 THEN EXCLUDED.markings ELSE moria.native_function_calls.markings END RETURNING request_hash,response_hash,status",&[&context.lease().execution,&call.id,&context.attempt_id(),&call.method,&call.path,&request_hash,&action_name,&action_key,&status,&code,&response_hash,&json!(context.consumed_markings())]).await?;
        ensure!(
            row.get::<_, String>(0) == request_hash
                && row.get::<_, String>(1) == response_hash
                && row.get::<_, i32>(2) == status,
            "FUNCTION_INPUT_CHANGED"
        );
        if !pending {
            context.complete_observation_in(client, &call.id).await?;
        }
        Ok(())
    }
    pub fn new(db: Database) -> Self {
        Self { db }
    }
    async fn session(&self, context: &SandboxContext) -> Result<VerifiedSession> {
        Authority {
            db: self.db.clone(),
        }
        .sandbox_session(context)
        .await
    }
    fn labels(environment: &str, labels: impl IntoIterator<Item = String>) -> Vec<ConsumedMarking> {
        labels
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|marking| ConsumedMarking {
                environment: environment.into(),
                marking,
            })
            .collect()
    }
    async fn ledger_lock(client: &Client, execution: Uuid) -> Result<()> {
        client.query_one("SELECT pg_advisory_xact_lock(hashtextextended('moria/function-ledger/'||$1::uuid::text,0))", &[&execution]).await?;
        Ok(())
    }
    async fn ledger_budget(client: &Client, execution: Uuid) -> Result<()> {
        let row=client.query_one("SELECT count(*)<=512 AND COALESCE(sum(bytes),0)<=8388608 FROM (SELECT octet_length(evidence::text)::bigint+octet_length(markings::text) AS bytes FROM moria.native_function_reads WHERE execution=$1 UNION ALL SELECT octet_length(evidence::text)::bigint+octet_length(markings::text) FROM moria.native_function_filesystem_reads WHERE execution=$1 UNION ALL SELECT octet_length(evidence::text)::bigint+octet_length(markings::text) FROM moria.native_function_failed_actions WHERE execution=$1) records", &[&execution]).await?;
        ensure!(row.get::<_, bool>(0), "FUNCTION_READ_LIMIT");
        Ok(())
    }
    async fn record(
        client: &Client,
        session: &VerifiedSession,
        execution: Uuid,
        attempt: Option<Uuid>,
        key: &str,
        evidence: &[ReadEvidence],
        markings: &[ConsumedMarking],
    ) -> Result<()> {
        Self::ledger_lock(client, execution).await?;
        let value = json!(evidence);
        let labels = json!(markings);
        ensure!(
            evidence.len() <= 256 && serde_json::to_vec(&value)?.len() <= 1024 * 1024,
            "FUNCTION_READ_LIMIT"
        );
        let digest = fingerprint(&json!({"evidence":value,"markings":labels}))?;
        let row=client.query_one("INSERT INTO moria.native_function_reads(execution,attempt,call_key,evidence,markings,fingerprint) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(execution,call_key) DO UPDATE SET fingerprint=moria.native_function_reads.fingerprint RETURNING fingerprint",&[&execution,&attempt,&key,&value,&labels,&digest]).await?;
        ensure!(row.get::<_, String>(0) == digest, "FUNCTION_INPUT_CHANGED");
        ensure!(
            client
                .query_one(
                    "SELECT (SELECT count(*) FROM moria.native_function_reads WHERE execution=$1)+(SELECT count(*) FROM moria.native_function_filesystem_reads WHERE execution=$1)",
                    &[&execution]
                )
                .await?
                .get::<_, i64>(0)
                <= 512,
            "FUNCTION_READ_LIMIT"
        );
        Self::ledger_budget(client, execution).await?;
        let snapshot = Authority::current(client, session).await?;
        ensure!(
            markings
                .iter()
                .all(|label| label.environment == session.environment()
                    && snapshot.identity.markings.contains(&label.marking)),
            "FORBIDDEN"
        );
        Ok(())
    }
    pub(crate) async fn record_filesystem(
        client: &Client,
        session: &VerifiedSession,
        execution: Uuid,
        attempt: Option<Uuid>,
        key: &str,
        evidence: &[FilesystemEvidence],
        markings: &[ConsumedMarking],
    ) -> Result<()> {
        Self::ledger_lock(client, execution).await?;
        let value = json!(evidence);
        let labels = json!(markings);
        ensure!(
            evidence.len() <= 256 && serde_json::to_vec(&value)?.len() <= 1024 * 1024,
            "FUNCTION_READ_LIMIT"
        );
        let digest = fingerprint(&json!({"evidence":value,"markings":labels}))?;
        let row=client.query_one("INSERT INTO moria.native_function_filesystem_reads(execution,attempt,call_key,evidence,markings,fingerprint) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(execution,call_key) DO UPDATE SET fingerprint=moria.native_function_filesystem_reads.fingerprint RETURNING fingerprint",&[&execution,&attempt,&key,&value,&labels,&digest]).await?;
        ensure!(row.get::<_, String>(0) == digest, "FUNCTION_INPUT_CHANGED");
        let count:i64=client.query_one("SELECT (SELECT count(*) FROM moria.native_function_reads WHERE execution=$1)+(SELECT count(*) FROM moria.native_function_filesystem_reads WHERE execution=$1)",&[&execution]).await?.get(0);
        ensure!(count <= 512, "FUNCTION_READ_LIMIT");
        Self::ledger_budget(client, execution).await?;
        let snapshot = Authority::current(client, session).await?;
        ensure!(
            markings
                .iter()
                .all(|mark| mark.environment == session.environment()
                    && snapshot.identity.markings.contains(&mark.marking)),
            "FORBIDDEN"
        );
        Ok(())
    }
    async fn copy_filesystem(
        client: &Client,
        session: &VerifiedSession,
        from: Uuid,
        to: Uuid,
        attempt: Option<Uuid>,
        labels: &[ConsumedMarking],
    ) -> Result<()> {
        let rows=client.query("SELECT call_key,evidence FROM moria.native_function_filesystem_reads WHERE execution=$1 ORDER BY call_key",&[&from]).await?;
        for row in rows {
            let evidence: Vec<FilesystemEvidence> = serde_json::from_value(row.get(1))?;
            Self::record_filesystem(
                client,
                session,
                to,
                attempt,
                &format!("inherited/{from}/{}", row.get::<_, String>(0)),
                &evidence,
                labels,
            )
            .await?;
        }
        Ok(())
    }
    async fn record_failed_action(
        client: &Client,
        context: &SandboxContext,
        key: &str,
        evidence: &Value,
    ) -> Result<()> {
        Self::ledger_lock(client, context.lease().execution).await?;
        ensure!(
            serde_json::to_vec(evidence)?.len() <= 1024 * 1024,
            "FUNCTION_READ_LIMIT"
        );
        let markings = json!(context.consumed_markings());
        let digest = fingerprint(&json!({"evidence":evidence,"markings":markings}))?;
        let row=client.query_one("INSERT INTO moria.native_function_failed_actions(execution,attempt,call_key,evidence,markings,fingerprint) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(execution,call_key) DO UPDATE SET fingerprint=moria.native_function_failed_actions.fingerprint RETURNING fingerprint",&[&context.lease().execution,&context.attempt_id(),&key,&evidence,&markings,&digest]).await?;
        ensure!(row.get::<_, String>(0) == digest, "FUNCTION_INPUT_CHANGED");
        Self::ledger_budget(client, context.lease().execution).await
    }
    async fn verify_failed_actions(
        db: &Database,
        client: &Client,
        session: &VerifiedSession,
        execution: Uuid,
    ) -> Result<()> {
        let rows=client.query("SELECT evidence FROM moria.native_function_failed_actions WHERE execution=$1 ORDER BY call_key",&[&execution]).await?;
        ensure!(rows.len() <= 32, "FUNCTION_READ_LIMIT");
        for row in rows {
            let evidence: Value = row.get(0);
            let records = evidence
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_INPUT_CHANGED"))?;
            ensure!(records.len() == 1, "FUNCTION_INPUT_CHANGED");
            let record = &records[0];
            let text = |field: &str| {
                record[field]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("FUNCTION_INPUT_CHANGED"))
            };
            let action = text("action")?;
            ensure!(
                Self::action_hash(client, session, action).await?.0 == text("contract")?,
                "SCHEMA_CHANGED"
            );
            let consumed: Vec<String> = serde_json::from_value(record["consumed"].clone())?;
            ensure!(consumed.len() <= 256, "FUNCTION_READ_LIMIT");
            let prepared = if let Some(id) = record["preparedExecution"].as_str() {
                let functions = NativeFunctions::new(db.clone(), Arc::new(Self { db: db.clone() }));
                let function = Box::pin(functions.load_prepared_in(
                    client,
                    session,
                    Uuid::parse_str(id)?,
                    text("preparedHash")?,
                ))
                .await?;
                Some(crate::serving::PreparedAction {
                    function,
                    contract: text("contract")?.into(),
                })
            } else {
                None
            };
            client
                .batch_execute("SAVEPOINT native_failed_action_replay")
                .await?;
            let outcome = Box::pin(NativeServing::invoke_marked_prepared(
                db,
                client,
                session,
                action,
                text("key")?,
                &record["input"],
                (&consumed, prepared.as_ref()),
            ))
            .await;
            client.batch_execute("ROLLBACK TO SAVEPOINT native_failed_action_replay; RELEASE SAVEPOINT native_failed_action_replay").await?;
            if let Err(error) = outcome {
                let code = error_code(&error);
                if [
                    "FORBIDDEN",
                    "PROPERTY_FORBIDDEN",
                    "UNAUTHENTICATED",
                    "IDENTITY_STALE",
                    "AUTHORIZATION_CHANGED",
                ]
                .contains(&code)
                {
                    return Err(error);
                }
                ensure!(
                    code == record["error"].as_str().unwrap_or_default(),
                    "FUNCTION_INPUT_CHANGED"
                );
            } else {
                anyhow::bail!("FUNCTION_INPUT_CHANGED");
            }
        }
        Ok(())
    }
    async fn ledger(
        db: &Database,
        client: &Client,
        session: &VerifiedSession,
        execution: Uuid,
        strict: bool,
    ) -> Result<Vec<ConsumedMarking>> {
        Self::ledger_in(
            db,
            client,
            session,
            execution,
            strict,
            LedgerIntent::Artifact,
        )
        .await
    }
    async fn ledger_in(
        db: &Database,
        client: &Client,
        session: &VerifiedSession,
        execution: Uuid,
        strict: bool,
        intent: LedgerIntent,
    ) -> Result<Vec<ConsumedMarking>> {
        ensure!(!client.query_one("SELECT EXISTS(SELECT 1 FROM moria.native_function_observation_blocks WHERE execution=$1)", &[&execution]).await?.get::<_,bool>(0),"FUNCTION_INPUT_CHANGED");
        Self::ledger_budget(client, execution).await?;
        Self::verify_failed_actions(db, client, session, execution).await?;
        let rows=client.query("SELECT evidence,markings FROM moria.native_function_reads WHERE execution=$1 ORDER BY call_key",&[&execution]).await?;
        ensure!(rows.len() <= 512, "FUNCTION_READ_LIMIT");
        let mut labels = BTreeSet::new();
        for row in rows {
            let evidence: Vec<ReadEvidence> = serde_json::from_value(row.get(0))?;
            for read in evidence {
                labels.extend(NativeServing::verify_read(client, session, &read, strict).await?);
            }
            let stored: Value = row.get(1);
            for label in stored
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
            {
                ensure!(
                    label["environment"] == session.environment(),
                    "FUNCTION_MARKINGS_INVALID"
                );
                labels.insert(
                    label["marking"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
                        .to_owned(),
                );
            }
        }
        let dependencies=client.query("SELECT e.resource::text,f.markings,e.environment,e.principal,e.application,e.id,f.release,EXISTS(SELECT 1 FROM moria.runtime_sandbox_attempts attempt WHERE attempt.execution=e.id AND (attempt.observation_pending IS NOT NULL OR attempt.observation_blocked)) incomplete FROM moria.native_function_dependencies d JOIN moria.runtime_executions e ON e.id=d.child JOIN moria.runtime_function_invocations f ON f.execution=e.id JOIN moria.fs_entries entry ON entry.id=f.entry_id AND entry.resource_id=e.resource AND NOT entry.deleted JOIN moria.fs_resource_bindings b ON b.entry_id=entry.id AND b.active JOIN moria.fs_roles role ON role.entry_id=b.root_id AND NOT role.retired WHERE d.execution=$1",&[&execution]).await?;
        let count: i64 = client
            .query_one(
                "SELECT count(*) FROM moria.native_function_dependencies WHERE execution=$1",
                &[&execution],
            )
            .await?
            .get(0);
        ensure!(
            count <= 32 && dependencies.len() == count as usize,
            "AUTHORIZATION_CHANGED"
        );
        for dependency in dependencies {
            ensure!(
                dependency.get::<_, String>(2) == session.environment()
                    && dependency.get::<_, String>(3) == session.principal()
                    && dependency.get::<_, String>(4) == session.application(),
                "FUNCTION_ACTOR_MISMATCH"
            );
            if matches!(intent, LedgerIntent::Artifact) {
                ensure!(
                    !dependency.get::<_, bool>(7),
                    "FUNCTION_OBSERVATION_INCOMPLETE"
                );
                labels.extend(
                    crate::sandbox::retained_observations_in(client, dependency.get(5))
                        .await?
                        .into_iter()
                        .map(|label| label.marking),
                );
            }

            let markings: Value = dependency.get(1);
            let mut inherited = Vec::new();
            for label in markings
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
            {
                ensure!(
                    label["environment"] == session.environment(),
                    "FUNCTION_MARKINGS_INVALID"
                );
                inherited.push(
                    label["marking"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
                        .to_owned(),
                );
            }
            inherited.extend(
                Authority::resource_release_access(
                    client,
                    session,
                    &dependency.get::<_, String>(0),
                    "invoke",
                    &dependency.get::<_, String>(6),
                )
                .await?,
            );
            let snapshot = Authority::current(client, session).await?;
            ensure!(
                inherited
                    .iter()
                    .all(|mark| snapshot.identity.markings.contains(mark)),
                "FORBIDDEN"
            );
            labels.extend(inherited);
        }
        let calls = client
            .query(
                "SELECT action,markings FROM moria.native_function_calls WHERE execution=$1",
                &[&execution],
            )
            .await?;
        ensure!(calls.len() <= 32, "FUNCTION_READ_LIMIT");
        for call in calls {
            if let Some(action) = call.get::<_, Option<String>>(0) {
                labels.extend(NativeServing::action_markings(client, session, &action).await?);
            }
            let marks: Value = call.get(1);
            for mark in marks
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
            {
                ensure!(
                    mark["environment"] == session.environment(),
                    "FUNCTION_MARKINGS_INVALID"
                );
                labels.insert(
                    mark["marking"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
                        .to_owned(),
                );
            }
        }
        let snapshot = Authority::current(client, session).await?;
        let fs_reads=client.query("SELECT evidence,markings FROM moria.native_function_filesystem_reads WHERE execution=$1",&[&execution]).await?;
        ensure!(fs_reads.len() <= 512, "FUNCTION_READ_LIMIT");
        for fs_read in fs_reads {
            let evidence: Vec<crate::filesystem::FilesystemEvidence> =
                serde_json::from_value(fs_read.get(0))?;
            for read in evidence {
                labels.extend(
                    crate::filesystem::NativeFilesystem::verify_evidence(
                        client, session, &read, true,
                    )
                    .await?,
                );
            }
            let inherited: Value = fs_read.get(1);
            for mark in inherited
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
            {
                ensure!(
                    mark["environment"] == session.environment(),
                    "FUNCTION_MARKINGS_INVALID"
                );
                labels.insert(
                    mark["marking"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
                        .to_owned(),
                );
            }
        }
        ensure!(
            labels
                .iter()
                .all(|label| snapshot.identity.markings.contains(label)),
            "FORBIDDEN"
        );
        Ok(Self::labels(session.environment(), labels))
    }
    fn references(value: &Value, schema: &Value, output: &mut Vec<(String, Uuid)>) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        ensure!(output.len() <= 256, "FUNCTION_READ_LIMIT");
        match schema["type"].as_str() {
            Some("ontologyObject") => output.push((
                schema["objectType"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("VALUE_INVALID"))?
                    .into(),
                Uuid::parse_str(
                    value["uid"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("VALUE_INVALID"))?,
                )?,
            )),
            Some("object") => {
                for (name, field) in schema["properties"]
                    .as_object()
                    .ok_or_else(|| anyhow::anyhow!("VALUE_INVALID"))?
                {
                    if let Some(value) = value.get(name) {
                        Self::references(value, field, output)?;
                    }
                }
            }
            Some("array") => {
                for value in value
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("VALUE_INVALID"))?
                {
                    Self::references(value, &schema["items"], output)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    async fn references_in(
        client: &Client,
        session: &VerifiedSession,
        execution: Uuid,
        attempt: Option<Uuid>,
        phase: &str,
        value: &Value,
        fields: &Value,
    ) -> Result<Vec<ConsumedMarking>> {
        let schema = values::descriptor(
            &json!({"type":"object","properties":fields}),
            &Value::Null,
            0,
        )?;
        let normalized = values::normalize(value, &schema)?;
        let mut references = vec![];
        Self::references(&normalized, &schema, &mut references)?;
        ensure!(references.len() <= 256, "FUNCTION_READ_LIMIT");
        let mut labels = BTreeSet::new();
        for (index, (reference, uid)) in references.into_iter().enumerate() {
            let reference = if reference.contains('/') {
                let release = client
                    .query_opt(
                        "SELECT release FROM moria.runtime_sandbox_metadata WHERE execution=$1",
                        &[&execution],
                    )
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?
                    .get::<_, String>(0);
                crate::filesystem::NativeSources::resource_in(client, &release, &reference).await?
            } else {
                reference
            };
            let key = format!(
                "{phase}/{index}/{}",
                fingerprint(&json!([reference, uid]))?.trim_start_matches("sha256:")
            );
            if let Some(row)=client.query_opt("SELECT evidence FROM moria.native_function_reads WHERE execution=$1 AND call_key=$2",&[&execution,&key]).await? {
                let existing:Vec<ReadEvidence>=serde_json::from_value(row.get(0))?;
                for read in existing {labels.extend(NativeServing::verify_read(client,session,&read,false).await?);}
                continue;
            }
            let read = NativeServing::read_for_broker(
                client,
                session,
                &reference,
                Some(uid),
                "read",
                &json!({"projection":[]}),
            )
            .await?;
            let markings = Self::labels(session.environment(), read.markings);
            Self::record(
                client,
                session,
                execution,
                attempt,
                &key,
                &read.evidence,
                &markings,
            )
            .await?;
            labels.extend(markings.into_iter().map(|label| label.marking));
        }
        Ok(Self::labels(session.environment(), labels))
    }
    async fn output(
        client: &Client,
        session: &VerifiedSession,
        context: &SandboxContext,
        result: &Value,
    ) -> Result<Vec<ConsumedMarking>> {
        Self::references_in(
            client,
            session,
            context.lease().execution,
            Some(context.attempt_id()),
            "output",
            result,
            context.output_fields(),
        )
        .await
    }
    fn action_key(execution: Uuid, key: &str, action: &str) -> Result<String> {
        ensure!(
            !key.is_empty() && key.len() <= 256,
            "IDEMPOTENCY_KEY_INVALID"
        );
        Ok(format!(
            "function/{}",
            fingerprint(&json!([execution, key, action]))?.trim_start_matches("sha256:")
        ))
    }
    async fn action_hash(
        client: &Client,
        session: &VerifiedSession,
        action: &str,
    ) -> Result<(String, Value, String)> {
        NativeServing::action_contract(client, session, action).await
    }
    async fn consume_child(
        &self,
        context: &SandboxContext,
        session: &VerifiedSession,
        child: Uuid,
        call_id: Option<&str>,
        progress: Option<&ChildProgress>,
    ) -> Result<()> {
        let result = self
            .consume_child_attempt(context, session, child, call_id, progress)
            .await;
        if result.is_err() {
            context.block_release();
        }
        result
    }
    async fn consume_child_attempt(
        &self,
        context: &SandboxContext,
        session: &VerifiedSession,
        child: Uuid,
        call_id: Option<&str>,
        progress: Option<&ChildProgress>,
    ) -> Result<()> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome=async {
            let client=admission.client();context.check_in(client).await?;
            ensure!(client.query_opt("SELECT 1 FROM moria.runtime_function_invocations f JOIN moria.runtime_executions e ON e.id=f.execution WHERE f.execution=$1 AND f.parent_id=$2 AND e.environment=$3 AND e.principal=$4 AND e.application=$5", &[&child,&context.lease().execution,&session.environment(),&session.principal(),&session.application()]).await?.is_some(),"FUNCTION_RECURSION");
            Self::ledger_lock(client,context.lease().execution).await?;Self::ledger_lock(client,child).await?;
            let groups=client.query("SELECT DISTINCT value FROM (SELECT markings FROM moria.runtime_function_invocations WHERE execution=$1 UNION ALL SELECT markings FROM moria.runtime_function_receipts WHERE execution=$1 UNION ALL SELECT consumed_markings FROM moria.runtime_sandbox_attempts WHERE execution=$1 UNION ALL SELECT markings FROM moria.native_function_reads WHERE execution=$1 UNION ALL SELECT markings FROM moria.native_function_filesystem_reads WHERE execution=$1 UNION ALL SELECT markings FROM moria.native_function_calls WHERE execution=$1) observations CROSS JOIN LATERAL jsonb_array_elements(markings) AS labels(value) LIMIT 257", &[&child]).await?;
            ensure!(groups.len()<=256,"FUNCTION_READ_LIMIT");
            let mut labels=BTreeSet::new();
            for group in groups {let value:Value=group.get(0);ensure!(value["environment"]==session.environment(),"FUNCTION_MARKINGS_INVALID");labels.insert(ConsumedMarking{environment:session.environment().into(),marking:value["marking"].as_str().ok_or_else(||anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?.into()});}
            let labels=labels.into_iter().collect::<Vec<_>>();context.record_consumed_markings(&labels)?;
            client.execute("INSERT INTO moria.native_function_dependencies(execution,child) VALUES($1,$2) ON CONFLICT DO NOTHING", &[&context.lease().execution,&child]).await?;
            client.execute("INSERT INTO moria.native_function_dependencies(execution,child) SELECT $1,child FROM moria.native_function_dependencies WHERE execution=$2 ON CONFLICT DO NOTHING", &[&context.lease().execution,&child]).await?;
            let mark_json=json!(labels);let empty=json!([]);let digest=fingerprint(&json!({"evidence":empty,"markings":mark_json}))?;
            client.execute("INSERT INTO moria.native_function_reads(execution,attempt,call_key,evidence,markings,fingerprint) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(execution,call_key) DO UPDATE SET markings=EXCLUDED.markings,fingerprint=EXCLUDED.fingerprint", &[&context.lease().execution,&context.attempt_id(),&format!("child-labels/{child}"),&empty,&mark_json,&digest]).await?;
            client.batch_execute("SAVEPOINT native_child_observation").await?;
            let observed=async {
                Self::ledger_budget(client,child).await?;
                // Only durable native records from this exact child are copied. No guest JSON establishes evidence.
                for (table,prefix) in [("native_function_reads","child"),("native_function_filesystem_reads","inherited"),("native_function_failed_actions","failed-action")] {
                    client.execute(&format!("INSERT INTO moria.{table}(execution,attempt,call_key,evidence,markings,fingerprint) SELECT $1,$2,$4||'/'||$3::uuid::text||'/'||call_key,evidence,markings,fingerprint FROM moria.{table} WHERE execution=$3 ON CONFLICT(execution,call_key) DO UPDATE SET evidence=EXCLUDED.evidence,markings=EXCLUDED.markings,fingerprint=EXCLUDED.fingerprint"), &[&context.lease().execution,&context.attempt_id(),&child,&prefix]).await?;
                }
                Self::ledger_budget(client,context.lease().execution).await?;
                let intent=if progress.is_some_and(ChildProgress::is_pending){LedgerIntent::Control}else{LedgerIntent::Artifact};
                let observed=Self::ledger_in(&self.db,client,session,context.lease().execution,false,intent).await?;
                context.record_consumed_markings(&observed)?;context.check_in(client).await?;Ok(())
            }.await;
            if observed.is_err(){client.batch_execute("ROLLBACK TO SAVEPOINT native_child_observation").await?;client.execute("INSERT INTO moria.native_function_observation_blocks(execution,reason) VALUES($1,'CHILD_OBSERVATION_INVALID') ON CONFLICT DO NOTHING", &[&context.lease().execution]).await?;}
            client.batch_execute("RELEASE SAVEPOINT native_child_observation").await?;
            if observed.is_ok() && let Some(call_id)=call_id {context.complete_observation_in(client,call_id).await?;}
            Ok(observed)
        }.await;
        admission.complete(outcome).await?
    }
    /// Agent-origin model receipts reuse source evidence without recursing into model receipts.
    /// The caller holds its protected admission and an active database transaction.
    pub(crate) async fn retained_sources_in(
        db: &Database,
        client: &Client,
        session: &VerifiedSession,
        execution: Uuid,
    ) -> Result<Vec<ConsumedMarking>> {
        let row=client.query_opt("SELECT e.resource,a.release,(e.finished_at IS NOT NULL AND e.finished_at<=clock_timestamp()-interval '30 days') expired FROM moria.runtime_executions e JOIN moria.runtime_agent_executions a ON a.execution=e.id WHERE e.id=$1 AND e.spec->>'domain'='agent' AND e.environment=$2 AND e.principal=$3 AND e.application=$4",&[&execution,&session.environment(),&session.principal(),&session.application()]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        ensure!(!row.get::<_, bool>("expired"), "RESULT_EXPIRED");
        let journal=client.query_one("SELECT COALESCE(bool_or(observation_pending IS NOT NULL),false),COALESCE(bool_or(observation_blocked),false) FROM moria.runtime_sandbox_attempts WHERE execution=$1",&[&execution]).await?;
        ensure!(
            !journal.get::<_, bool>(1),
            "FUNCTION_OBSERVATION_INCOMPLETE"
        );
        if journal.get::<_, bool>(0) {
            let origin: Option<Value> = client
                .query_one(
                    "SELECT moria.runtime_job_session_origin($1)",
                    &[&session.session_hash()],
                )
                .await?
                .get(0);
            ensure!(
                origin
                    .as_ref()
                    .is_some_and(|proof| proof["execution"] == json!(execution)),
                "FUNCTION_OBSERVATION_INCOMPLETE"
            );
        }
        let mut labels = crate::agents::current_labels(client, session, execution).await?;
        labels.extend(crate::automations::retained_in(client, session, execution).await?);
        labels.extend(crate::evaluations::retained_input_in(client, session, execution).await?);
        labels.extend(crate::sandbox::retained_observations_in(client, execution).await?);
        labels.extend(
            crate::sandbox::released_source_markings_in(
                client,
                session,
                row.get("resource"),
                "invoke",
                &row.get::<_, String>("release"),
            )
            .await?,
        );
        labels.extend(Self::ledger(db, client, session, execution, false).await?);
        let labels = labels.into_iter().collect::<BTreeSet<_>>();
        ensure!(
            labels.len() <= 256
                && labels
                    .iter()
                    .all(|mark| mark.environment == session.environment()),
            "FUNCTION_MARKINGS_INVALID"
        );
        let snapshot = Authority::current(client, session).await?;
        crate::execution::invocation_permission(
            &snapshot.identity,
            row.get("resource"),
            "agents",
            "invoke",
            &labels
                .iter()
                .map(|mark| mark.marking.clone())
                .collect::<Vec<_>>(),
        )?;
        Ok(labels.into_iter().collect())
    }

    pub(crate) async fn prepared_ledger(
        db: &Database,
        client: &Client,
        session: &VerifiedSession,
        execution: Uuid,
        versions: bool,
    ) -> Result<Vec<ConsumedMarking>> {
        Self::ledger(db, client, session, execution, versions).await
    }
    pub(crate) async fn committed_ledger(
        client: &Client,
        session: &VerifiedSession,
        execution: Uuid,
    ) -> Result<()> {
        Self::ledger_lock(client, execution).await?;
        Self::ledger_budget(client, execution).await?;
        let rows=client.query("SELECT call_key,evidence,markings FROM moria.native_function_reads WHERE execution=$1 ORDER BY call_key",&[&execution]).await?;
        for row in rows {
            let mut evidence: Vec<ReadEvidence> = serde_json::from_value(row.get(1))?;
            let mut labels = BTreeSet::new();
            let stored: Value = row.get(2);
            for label in stored
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
            {
                ensure!(
                    label["environment"] == session.environment(),
                    "FUNCTION_MARKINGS_INVALID"
                );
                labels.insert(
                    label["marking"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
                        .to_owned(),
                );
            }
            for read in &mut evidence {
                labels.extend(NativeServing::refresh_read(client, session, read).await?);
            }
            let markings = Self::labels(session.environment(), labels);
            Self::ledger_lock(client, execution).await?;
            let value = json!(evidence);
            let markings = json!(markings);
            ensure!(
                serde_json::to_vec(&value)?.len() <= 1024 * 1024,
                "FUNCTION_INPUT_CHANGED"
            );
            let digest = fingerprint(&json!({"evidence":value,"markings":markings}))?;
            client.execute("UPDATE moria.native_function_reads SET evidence=$3,markings=$4,fingerprint=$5 WHERE execution=$1 AND call_key=$2",&[&execution,&row.get::<_,String>(0),&value,&markings,&digest]).await?;
        }
        let rows=client.query("SELECT call_key,evidence,markings FROM moria.native_function_filesystem_reads WHERE execution=$1 ORDER BY call_key",&[&execution]).await?;
        for row in rows {
            let mut evidence: Vec<FilesystemEvidence> = serde_json::from_value(row.get(1))?;
            let stored: Value = row.get(2);
            let mut labels = stored
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
                .iter()
                .map(|mark| {
                    ensure!(
                        mark["environment"] == session.environment(),
                        "FUNCTION_MARKINGS_INVALID"
                    );
                    Ok(mark["marking"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
                        .to_owned())
                })
                .collect::<Result<BTreeSet<_>>>()?;
            for read in &mut evidence {
                labels.extend(NativeFilesystem::refresh_evidence(client, session, read).await?);
            }
            Self::ledger_lock(client, execution).await?;
            let value = json!(evidence);
            let markings = json!(Self::labels(session.environment(), labels));
            ensure!(
                serde_json::to_vec(&value)?.len() <= 1024 * 1024,
                "FUNCTION_INPUT_CHANGED"
            );
            let digest = fingerprint(&json!({"evidence":value,"markings":markings}))?;
            client.execute("UPDATE moria.native_function_filesystem_reads SET evidence=$3,markings=$4,fingerprint=$5 WHERE execution=$1 AND call_key=$2",&[&execution,&row.get::<_,String>(0),&value,&markings,&digest]).await?;
        }
        Self::ledger_budget(client, execution).await?;
        Ok(())
    }
    async fn complete_child_control(
        &self,
        context: &SandboxContext,
        session: &VerifiedSession,
        child: Uuid,
        call: &HttpCall,
        value: &Value,
    ) -> Result<()> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome=async {
            let client=admission.client();context.check_in(client).await?;
            let snapshot=Authority::current(client,session).await?;
            ensure!(value.as_object().is_some_and(|object|object.len()==2 && object.get("id").and_then(Value::as_str)==Some(child.to_string().as_str()) && object.get("cancelRequested")==Some(&Value::Bool(true))),"FUNCTION_OBSERVATION_INCOMPLETE");
            ensure!(client.query_opt("SELECT 1 FROM moria.runtime_function_invocations f JOIN moria.runtime_executions e ON e.id=f.execution WHERE f.execution=$1 AND f.parent_id=$2 AND e.environment=$3 AND e.principal=$4 AND e.application=$5",&[&child,&context.lease().execution,&session.environment(),&session.principal(),&session.application()]).await?.is_some(),"FUNCTION_RECURSION");
            // The constant command receipt adds no observation of the child's dynamic artifacts.
            // Existing parent read evidence and the command's current Source proof remain mandatory.
            let labels=Self::ledger_in(&self.db,client,session,context.lease().execution,context.point_reads_only(),LedgerIntent::Control).await?;
            context.record_consumed_markings(&labels)?;
            let release:String=client.query_one("SELECT release FROM moria.runtime_function_invocations WHERE execution=$1",&[&child]).await?.get(0);
            let resource:String=client.query_one("SELECT resource::text FROM moria.runtime_executions WHERE id=$1",&[&child]).await?.get(0);
            let source=Authority::resource_release_access(client,session,&resource,"invoke",&release).await?;
            context.record_consumed_markings(&Self::labels(session.environment(),source))?;
            Self::trace(client,context,call,None,&Ok(value.clone()),false).await?;
            Authority::finish(client,session,&snapshot).await
        }.await;
        let result = admission.complete(outcome).await;
        if result.is_err() {
            context.block_release();
        }
        result
    }
    async fn static_effect_denial(
        &self,
        context: &SandboxContext,
        session: &VerifiedSession,
        call: &HttpCall,
    ) -> Result<()> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome = async {
            let client = admission.client();
            context.check_in(client).await?;
            let snapshot = Authority::current(client, session).await?;
            let denied: Result<Value> = Err(anyhow::anyhow!("FUNCTION_EFFECT"));
            Self::trace(client, context, call, None, &denied, false).await?;
            Authority::finish(client, session, &snapshot).await
        }
        .await;
        let result = admission.complete(outcome).await;
        if result.is_err() {
            context.block_release();
        }
        result
    }
    async fn action_static_admission(
        &self,
        context: &SandboxContext,
        session: &VerifiedSession,
        action: &str,
        call: &HttpCall,
    ) -> Result<()> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome = async {
            let client = admission.client();
            context.check_in(client).await?;
            let snapshot = Authority::current(client, session).await?;
            let resource: String = client
                .query_one(
                    "SELECT moria.runtime_data_resource($1,$2)",
                    &[&session.environment(), &action],
                )
                .await?
                .get(0);
            let code = if context.effect() == "readOnly" || context.prepare_only() {
                Some("FUNCTION_EFFECT")
            } else if crate::authorization::authorize(&snapshot.identity, &resource, "invoke", &[])
                .is_err()
            {
                Some("FORBIDDEN")
            } else {
                None
            };
            if let Some(code) = code {
                let callback: Result<Value> = Err(anyhow::anyhow!(code));
                Self::trace(client, context, call, None, &callback, false).await?;
                Authority::finish(client, session, &snapshot).await?;
                return Ok(Err(anyhow::anyhow!(code)));
            }
            Authority::finish(client, session, &snapshot).await?;
            Ok(Ok(()))
        }
        .await;
        admission.complete(outcome).await?
    }
    async fn action(
        &self,
        context: &SandboxContext,
        session: &VerifiedSession,
        action: &str,
        key: &str,
        input: &Value,
        call: &HttpCall,
    ) -> Result<Value> {
        self.action_static_admission(context, session, action, call)
            .await?;
        let key = Self::action_key(context.lease().execution, key, action)?;
        let function_backed = self
            .action_preflight(context, session, action, &key, call)
            .await?;
        let prepared = if function_backed {
            NativeServing {
                db: self.db.clone(),
            }
            .prepare_child_action(session, action, &key, input, context)
            .await?
        } else {
            None
        };
        let admission = Admission::begin(&self.db, session, true).await?;
        let outcome=async {
            let client=admission.client();
            context.check_in(client).await?;
            ensure!(!context.prepare_only(),"FUNCTION_EFFECT");
            let mut labels=Self::ledger(&self.db,client,session,context.lease().execution,true).await?;
            labels.extend(context.consumed_markings());
            labels.extend(Self::labels(session.environment(),NativeServing::action_markings(client,session,action).await?));
            context.record_consumed_markings(&labels)?;
            let(binding_hash,definition,resource)=Self::action_hash(client,session,action).await?;
            let normalized=values::normalize(input,&json!({"type":"object","properties":definition["inputs"]}))?;
            let parameter=definition["targetParameter"].as_str().ok_or_else(||anyhow::anyhow!("BINDING_INVALID"))?;
            let targets=if parameter.is_empty(){vec![]}else{
                let target=normalized.get(parameter).ok_or_else(||anyhow::anyhow!("VALUE_INVALID"))?;
                target.as_array().cloned().unwrap_or_else(||vec![target.clone()])
            };
            ensure!(targets.len()<=100&&targets.iter().map(Value::to_string).collect::<BTreeSet<_>>().len()==targets.len(),"ACTION_LIMIT");
            let mut fields=vec![];crate::serving::query_fields(&definition["condition"],&mut fields);
            if let Some(edits)=definition["typedEdits"].as_array(){for edit in edits{
                for key in ["uid","identityKey","sourceUid","targetUid","value"]{crate::serving::query_fields(&edit[key],&mut fields);}
                if let Some(values)=edit["values"].as_object(){for expression in values.values(){crate::serving::query_fields(expression,&mut fields);}}
            }}
            fields.sort();fields.dedup();
            for(index,uid) in targets.into_iter().enumerate() {
                let uid=Uuid::parse_str(uid.as_str().ok_or_else(||anyhow::anyhow!("VALUE_INVALID"))?)?;
                client.batch_execute("SAVEPOINT native_action_read").await?;
                let observed=NativeServing::read_for_broker(client,session,&resource,Some(uid),"read",&json!({"projection":fields})).await;
                let read=match observed {
                    Ok(read)=>{client.batch_execute("RELEASE SAVEPOINT native_action_read").await?;read},
                    Err(error)=>{
                        client.batch_execute("ROLLBACK TO SAVEPOINT native_action_read; RELEASE SAVEPOINT native_action_read").await?;
                        let mut root=NativeServing::root_for_broker(client,session,&resource).await?;
                        NativeServing::failed_read(&mut root.evidence[0],Some(uid),"read",&json!({"projection":fields}),error_code(&error));
                        let marks=Self::labels(session.environment(),root.markings);
                        Self::record(client,session,context.lease().execution,Some(context.attempt_id()),&format!("action-failure/{}/{index}",call.id),&root.evidence,&marks).await?;
                        context.record_consumed_markings(&marks)?;
                        let callback=Err(error);Self::trace(client,context,call,Some((action,&key)),&callback,false).await?;context.check_in(client).await?;
                        return Ok(callback);
                    }
                };
                let marks=Self::labels(session.environment(),read.markings);
                Self::record(client,session,context.lease().execution,Some(context.attempt_id()),&format!("action-read/{}/{index}",call.id),&read.evidence,&marks).await?;
                context.record_consumed_markings(&marks)?;
            }
            let consumed=context.consumed_markings().into_iter().map(|label|label.marking).collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>();
            client.batch_execute("SAVEPOINT native_action_business").await?;
            let mut action_committed=false;
            let callback=async {
                if context.effect()=="editProposal" {
                    let digest=fingerprint(&json!({"action":action,"input":normalized,"binding":binding_hash}))?;
                    if let Some(row)=client.query_opt("SELECT input_hash FROM moria.native_function_proposals WHERE execution=$1 AND key=$2",&[&context.lease().execution,&key]).await? {
                        ensure!(row.get::<_,String>(0)==digest,"IDEMPOTENCY_CONFLICT");
                    }else{
                        client.batch_execute("SAVEPOINT native_proposal_validation").await?;
                        let preview=NativeServing::invoke_marked_prepared(&self.db,client,session,action,&key,&normalized,(&consumed,prepared.as_ref())).await;
                        client.batch_execute("ROLLBACK TO SAVEPOINT native_proposal_validation; RELEASE SAVEPOINT native_proposal_validation").await?;
                        preview?;
                        let prepared_id=prepared.as_ref().map(|value|value.function.id());
                        let prepared_hash=prepared.as_ref().map(|value|value.function.hash().to_owned());
                        client.execute("INSERT INTO moria.native_function_proposals(execution,attempt,key,action,input,binding_hash,input_hash,prepared_execution,prepared_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",&[&context.lease().execution,&context.attempt_id(),&key,&action,&normalized,&binding_hash,&digest,&prepared_id,&prepared_hash]).await?;
                    }
                    let revision:String=client.query_one("SELECT revision::text FROM moria.environments WHERE id=$1",&[&session.environment()]).await?.get(0);
                    Ok(json!({"proposed":true,"revision":revision}))
                }else{
                    let result=NativeServing::invoke_marked_prepared(&self.db,client,session,action,&key,&normalized,(&consumed,prepared.as_ref())).await?;
                    action_committed=true;
                    Self::committed_ledger(client,session,context.lease().execution).await?;
                    Ok(result)
                }
            }.await;
            if callback.is_err()&&!action_committed{client.batch_execute("ROLLBACK TO SAVEPOINT native_action_business").await?;}
            client.batch_execute("RELEASE SAVEPOINT native_action_business").await?;
            if let Err(error)=&callback && !action_committed {
                let evidence=json!([{"action":action,"key":key,"input":normalized,"contract":binding_hash,"error":error_code(error),"consumed":consumed,"preparedExecution":prepared.as_ref().map(|value|value.function.id()),"preparedHash":prepared.as_ref().map(|value|value.function.hash())}]);
                Self::record_failed_action(client,context,&call.id,&evidence).await?;
            }
            Self::trace(client,context,call,Some((action,&key)),&callback,false).await?;
            context.check_in(client).await?;
            Ok(callback)
        }.await;
        admission.complete(outcome).await?
    }
    async fn action_preflight(
        &self,
        context: &SandboxContext,
        session: &VerifiedSession,
        action: &str,
        key: &str,
        call: &HttpCall,
    ) -> Result<bool> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome = async {
            let client = admission.client();
            context.check_in(client).await?;
            ensure!(!context.prepare_only(), "FUNCTION_EFFECT");
            let (_, definition, resource) = Self::action_hash(client, session, action).await?;
            let root = NativeServing::root_for_broker(client, session, &resource).await?;
            let mut labels = root.markings;
            labels.extend(NativeServing::action_markings(client, session, action).await?);
            let labels = Self::labels(session.environment(), labels);
            context.record_consumed_markings(&labels)?;
            Self::record(
                client,
                session,
                context.lease().execution,
                Some(context.attempt_id()),
                &format!("action-root/{}", call.id),
                &root.evidence,
                &labels,
            )
            .await?;
            Self::trace(
                client,
                context,
                call,
                Some((action, key)),
                &Ok(Value::Null),
                true,
            )
            .await?;
            context.check_in(client).await?;
            Ok(definition.get("function").is_some_and(Value::is_string))
        }
        .await;
        admission.complete(outcome).await
    }
    async fn filesystem(
        &self,
        context: &SandboxContext,
        session: &VerifiedSession,
        request: &FilesystemRequest<'_>,
        call: &HttpCall,
    ) -> Result<Value> {
        let write = request.writes();
        ensure!(!write || context.effect() == "readWrite", "FUNCTION_EFFECT");
        if context.point_reads_only() {
            ensure!(
                !request
                    .parts
                    .first()
                    .is_some_and(|part| ["sources", "releases"].contains(&part.as_str())),
                "FUNCTION_EFFECT"
            );
            ensure!(
                !request
                    .parts
                    .get(2)
                    .is_some_and(|part| ["children", "history"].contains(&part.as_str())),
                "FUNCTION_EFFECT"
            );
        }
        let admission = Admission::begin_filesystem(&self.db, session, write, write).await?;
        let outcome=async {
            let client=admission.client();context.check_in(client).await?;
            let prior=Self::ledger(&self.db,client,session,context.lease().execution,true).await?;
            context.record_consumed_markings(&prior)?;
            let mut ids=NativeFilesystem::request_entries(request)?.into_iter().collect::<BTreeSet<_>>();
            let mut visited=BTreeSet::new();
            while let Some(id)=ids.pop_first() {
                if !visited.insert(id){continue;}
                ensure!(visited.len()<=256,"FUNCTION_READ_LIMIT");
                let parts=vec!["entries".into(),id.to_string()];
                let root_request=FilesystemRequest{method:"GET",parts:&parts,query:&[],input:&Value::Null,key:None,consumed:&[]};
                let value=NativeFilesystem::route_in(client,session,&root_request).await?;
                let(evidence,labels)=NativeFilesystem::evidence(client,session,&root_request,&value).await?;
                let labels=Self::labels(session.environment(),labels);context.record_consumed_markings(&labels)?;
                Self::record_filesystem(client,session,context.lease().execution,Some(context.attempt_id()),&format!("fs-root/{}/{}",call.id,visited.len()),&[evidence],&labels).await?;
                if request.input["changes"].as_array().is_some_and(|changes|changes.iter().any(|change|change["id"]==id.to_string()))
                    && let Some(parent)=value["parentId"].as_str(){ids.insert(Uuid::parse_str(parent)?);}
            }
            client.batch_execute("SAVEPOINT native_fs_labels").await?;
            let root_labels=match NativeFilesystem::request_labels(client,session,request).await {
                Ok(labels)=>{client.batch_execute("RELEASE SAVEPOINT native_fs_labels").await?;labels},
                Err(_)=>{client.batch_execute("ROLLBACK TO SAVEPOINT native_fs_labels; RELEASE SAVEPOINT native_fs_labels").await?;vec![]},
            };
            let labels=Self::labels(session.environment(),root_labels);context.record_consumed_markings(&labels)?;
            Self::record_filesystem(client,session,context.lease().execution,Some(context.attempt_id()),&format!("fs-class/{}",call.id),&[],&labels).await?;
            if write&&request.parts.len()==3&&request.parts[0]=="entries"&&request.parts[2]=="policy" {
                let read=FilesystemRequest{method:"GET",parts:request.parts,query:&[],input:&Value::Null,key:None,consumed:&[]};
                let value=NativeFilesystem::route_in(client,session,&read).await?;let(evidence,labels)=NativeFilesystem::evidence(client,session,&read,&value).await?;
                let labels=Self::labels(session.environment(),labels);context.record_consumed_markings(&labels)?;
                Self::record_filesystem(client,session,context.lease().execution,Some(context.attempt_id()),&format!("fs-policy/{}",call.id),&[evidence],&labels).await?;
            }
            let consumed=context.consumed_markings().into_iter().map(|label|label.marking).collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>();
            let key=Self::action_key(context.lease().execution,request.key.unwrap_or(&call.id),"filesystem/changes")?;
            let request=FilesystemRequest{method:request.method,parts:request.parts,query:request.query,input:request.input,key:if write{Some(&key)}else{None},consumed:&consumed};
            Self::trace(client,context,call,None,&Ok(Value::Null),true).await?;
            client.batch_execute("SAVEPOINT native_fs_business").await?;
            let callback=async {
                let value=NativeFilesystem::route_in_context(client,session,&request,if write{Some(context)}else{None}).await?;
                if write {
                    Self::committed_ledger(client,session,context.lease().execution).await?;
                    ensure!(value["entries"].is_array()||(request.parts.len()==3&&request.parts[2]=="policy"&&value["revision"].is_string()),"FILESYSTEM_INVALID");
                    for(index,entry) in value["entries"].as_array().map_or(&[][..],Vec::as_slice).iter().enumerate() {
                        if entry["deleted"]==true{continue;}
                        let parts=vec!["entries".into(),entry["id"].as_str().ok_or_else(||anyhow::anyhow!("FILESYSTEM_INVALID"))?.into()];
                        let observed=FilesystemRequest{method:"GET",parts:&parts,query:&[],input:&Value::Null,key:None,consumed:&[]};
                        let current=NativeFilesystem::route_in(client,session,&observed).await?;
                        let(evidence,labels)=NativeFilesystem::evidence(client,session,&observed,&current).await?;
                        let labels=Self::labels(session.environment(),labels);context.record_consumed_markings(&labels)?;
                        Self::record_filesystem(client,session,context.lease().execution,Some(context.attempt_id()),&format!("fs-written/{}/{index}",call.id),&[evidence],&labels).await?;
                    }
                }else{
                    let(evidence,labels)=NativeFilesystem::evidence(client,session,&request,&value).await?;
                    let labels=Self::labels(session.environment(),labels);context.record_consumed_markings(&labels)?;
                    Self::record_filesystem(client,session,context.lease().execution,Some(context.attempt_id()),&format!("fs-read/{}",call.id),&[evidence],&labels).await?;
                }
                Ok(value)
            }.await;
            if let Err(error)=&callback {
                client.batch_execute("ROLLBACK TO SAVEPOINT native_fs_business").await?;
                let evidence=NativeFilesystem::failed_evidence(&request,error_code(error));
                let labels=context.consumed_markings();
                Self::record_filesystem(client,session,context.lease().execution,Some(context.attempt_id()),&format!("fs-failure/{}",call.id),&[evidence],&labels).await?;
            }
            client.batch_execute("RELEASE SAVEPOINT native_fs_business").await?;
            Self::trace(client,context,call,None,&callback,false).await?;
            if write{client.execute("UPDATE moria.native_function_calls SET action_key=$3 WHERE execution=$1 AND call_id=$2",&[&context.lease().execution,&call.id,&key]).await?;}
            context.check_in(client).await?;Ok(callback)
        }.await;
        admission.complete(outcome).await?
    }
    async fn http_inner(&self, context: &SandboxContext, mut call: HttpCall) -> Result<Value> {
        context.check().await?;
        let session = self.session(context).await?;
        let mut headers = BTreeMap::new();
        for (key, value) in call.headers {
            ensure!(
                headers.insert(key.to_ascii_lowercase(), value).is_none(),
                "FUNCTION_HTTP"
            );
        }
        call.headers = headers;
        ensure!(
            call.headers
                .get("x-moria-value-version")
                .is_none_or(|version| version == "1"),
            "UNSUPPORTED_VALUE_VERSION"
        );
        ensure!(
            !call.path.contains('#') && !call.path.contains('\\') && !call.path.starts_with("//"),
            "FUNCTION_HTTP"
        );
        let uri: axum::http::Uri = call
            .path
            .parse()
            .map_err(|_| anyhow::anyhow!("FUNCTION_HTTP"))?;
        ensure!(
            uri.scheme().is_none() && uri.authority().is_none(),
            "FUNCTION_HTTP"
        );
        let mut parts = uri
            .path()
            .split('/')
            .filter(|part| !part.is_empty())
            .map(crate::serving::decode)
            .collect::<Result<Vec<_>>>()?;
        if parts.first().is_some_and(|part| part == "api") {
            parts.remove(0);
        }
        ensure!(
            call.headers.iter().all(|(key, _)| [
                "content-type",
                "idempotency-key",
                "accept",
                "x-moria-value-version"
            ]
            .contains(&key.as_str())),
            "FUNCTION_HTTP"
        );
        let input = if let Some(body) = &call.body {
            let bytes = STANDARD
                .decode(body)
                .map_err(|_| anyhow::anyhow!("FUNCTION_HTTP"))?;
            serde_json::from_slice::<Value>(&bytes).map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
        } else {
            json!({})
        };
        if parts.starts_with(&["management".into(), "v1".into(), "fs".into()]) {
            ensure!(call.method != "GET" || call.body.is_none(), "INPUT_INVALID");
            let query = axum::extract::Query::<Vec<(String, String)>>::try_from_uri(&uri)
                .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
                .0;
            return self
                .filesystem(
                    context,
                    &session,
                    &FilesystemRequest {
                        method: &call.method,
                        parts: &parts[3..],
                        query: &query,
                        input: &input,
                        key: call.headers.get("idempotency-key").map(String::as_str),
                        consumed: &[],
                    },
                    &call,
                )
                .await;
        }
        ensure!(
            (parts.len() == 3 || parts.len() == 4) && parts[0] == "v1",
            "NOT_FOUND"
        );
        if ["objects", "actions", "links", "functions"].contains(&parts[1].as_str())
            && parts[2].contains('/')
        {
            let release = context
                .source_release()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
            let client = self.db.get().await?;
            parts[2] =
                crate::filesystem::NativeSources::resource_in(&client, release, &parts[2]).await?;
        }
        let functions = NativeFunctions::new(self.db.clone(), Arc::new(self.clone()));
        if parts.len() == 4
            && parts[1] == "functions"
            && parts[3] == "invoke"
            && call.method == "POST"
        {
            ensure!(
                uri.query().is_none()
                    && input
                        .as_object()
                        .is_some_and(|object| object.contains_key("input")
                            && object
                                .keys()
                                .all(|key| ["input", "wait"].contains(&key.as_str())))
                    && input.get("wait").is_none_or(Value::is_boolean),
                "INPUT_INVALID"
            );
            let resource =
                Uuid::parse_str(&parts[2]).map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?;
            ensure!(resource.to_string() == parts[2], "INPUT_INVALID");
            let key = Self::action_key(
                context.lease().execution,
                call.headers.get("idempotency-key").unwrap_or(&call.id),
                &parts[2],
            )?;
            let value = functions
                .invoke(
                    &session,
                    resource,
                    &key,
                    input["input"].clone(),
                    Some(context),
                    false,
                )
                .await?;
            let child = Uuid::parse_str(
                value["id"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("INTERNAL"))?,
            )?;
            let outcome = if input.get("wait").and_then(Value::as_bool) != Some(false) {
                functions.wait_child(context, child, 25000).await
            } else {
                functions.child_progress(context, child).await
            };
            self.consume_child(
                context,
                &session,
                child,
                Some(&call.id),
                outcome.as_ref().ok(),
            )
            .await?;
            return outcome.map(ChildProgress::into_value);
        }
        if parts[1] == "invocations" {
            let child = Uuid::parse_str(&parts[2]).map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?;
            ensure!(
                child.to_string() == parts[2] && child != context.lease().execution,
                "FUNCTION_RECURSION"
            );
            let client = self.db.get().await?;
            ensure!(client.query_opt("SELECT 1 FROM moria.runtime_function_invocations WHERE execution=$1 AND parent_id=$2",&[&child,&context.lease().execution]).await?.is_some(),"FUNCTION_RECURSION");
            drop(client);
            if call.method == "GET" && parts.len() == 3 {
                ensure!(
                    uri.query().is_none() && call.body.is_none(),
                    "INPUT_INVALID"
                );
                let outcome = functions.child_progress(context, child).await;
                self.consume_child(
                    context,
                    &session,
                    child,
                    Some(&call.id),
                    outcome.as_ref().ok(),
                )
                .await?;
                return outcome.map(ChildProgress::into_value);
            }
            let outcome = if call.method == "GET" && parts.len() == 4 && parts[3] == "events" {
                ensure!(call.body.is_none(), "INPUT_INVALID");
                let query = axum::extract::Query::<Vec<(String, String)>>::try_from_uri(&uri)
                    .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
                    .0;
                ensure!(
                    query.len() <= 1 && query.iter().all(|(key, _)| key == "after"),
                    "INPUT_INVALID"
                );
                functions
                    .events(
                        &session,
                        child,
                        query.first().map_or("0", |(_, value)| value.as_str()),
                    )
                    .await
                    .map(|events| json!({"events":events}))
            } else {
                ensure!(
                    call.method == "POST"
                        && parts.len() == 4
                        && parts[3] == "cancel"
                        && uri.query().is_none()
                        && input.as_object().is_some_and(|value| value.is_empty()),
                    "FUNCTION_EFFECT"
                );
                if context.effect() != "readWrite" {
                    self.static_effect_denial(context, &session, &call).await?;
                    bail!("FUNCTION_EFFECT");
                }
                let value = functions.cancel(&session, child).await?;
                self.complete_child_control(context, &session, child, &call, &value)
                    .await?;
                return Ok(value);
            };
            self.consume_child(context, &session, child, Some(&call.id), None)
                .await?;
            return outcome;
        }
        ensure!(parts.len() == 4, "NOT_FOUND");
        if call.method == "POST" && parts[1] == "actions" && parts[3] == "invoke" {
            ensure!(
                uri.query().is_none()
                    && input
                        .as_object()
                        .is_some_and(|object| object.len() == 1 && object.contains_key("input")),
                "INPUT_INVALID"
            );
            return self
                .action(
                    context,
                    &session,
                    &parts[2],
                    call.headers.get("idempotency-key").unwrap_or(&call.id),
                    &input["input"],
                    &call,
                )
                .await;
        }
        if call.method == "POST" && parts[1] == "links" && parts[3] == "traverse" {
            if context.point_reads_only() {
                self.static_effect_denial(context, &session, &call).await?;
                bail!("FUNCTION_EFFECT");
            }
            ensure!(uri.query().is_none(), "INPUT_INVALID");
            return self
                .link_read(context, &session, &parts[2], &input, &call)
                .await;
        }
        ensure!(parts[1] == "objects", "UNSUPPORTED_CAPABILITY");
        let (uid, operation, input) = if call.method == "GET" {
            ensure!(call.body.is_none(), "INPUT_INVALID");
            let uid = Uuid::parse_str(&parts[3])?;
            ensure!(uid.to_string() == parts[3], "INPUT_INVALID");
            let query = axum::extract::Query::<Vec<(String, String)>>::try_from_uri(&uri)
                .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
                .0;
            ensure!(
                query.len() <= 1 && query.iter().all(|(name, _)| name == "properties"),
                "INPUT_INVALID"
            );
            (Some(uid),"read",query.first().map_or(json!({}),|(_,value)|json!({"projection":if value.is_empty(){vec![]}else{value.split(',').collect::<Vec<_>>()}})))
        } else {
            ensure!(
                call.method == "POST"
                    && ["query", "aggregate"].contains(&parts[3].as_str())
                    && uri.query().is_none(),
                "NOT_FOUND"
            );
            if context.point_reads_only() {
                self.static_effect_denial(context, &session, &call).await?;
                bail!("FUNCTION_EFFECT");
            }
            (None, parts[3].as_str(), input)
        };
        let admission = Admission::begin(&self.db, &session, false).await?;
        let outcome = async {
            let client=admission.client();context.check_in(client).await?;
            let mut root=NativeServing::root_for_broker(client,&session,&parts[2]).await?;
            let root_marks=Self::labels(session.environment(),root.markings);
            Self::record(client,&session,context.lease().execution,Some(context.attempt_id()),&format!("root/{}",call.id),&root.evidence,&root_marks).await?;
            context.record_consumed_markings(&root_marks)?;
            client.batch_execute("SAVEPOINT native_broker_read").await?;
            let observed = NativeServing::read_for_broker(
                client,
                &session,
                &parts[2],
                uid,
                operation,
                &input,
            )
            .await;
            let callback=match observed {
                Ok(read)=>{
                    client.batch_execute("RELEASE SAVEPOINT native_broker_read").await?;
                    let markings = Self::labels(session.environment(), read.markings);
                    Self::record(client,
                &session,
                context.lease().execution,
                Some(context.attempt_id()),
                &format!("read/{}", call.id),
                &read.evidence,
                &markings,
            )
                    .await?;
                    context.record_consumed_markings(&markings)?;
                    Ok(read.value)
                },Err(error)=>{
                    client.batch_execute("ROLLBACK TO SAVEPOINT native_broker_read; RELEASE SAVEPOINT native_broker_read").await?;
                    NativeServing::failed_read(&mut root.evidence[0],uid,operation,&input,error_code(&error));
                    Self::record(client,&session,context.lease().execution,Some(context.attempt_id()),&format!("failure/{}",call.id),&root.evidence,&root_marks).await?;
                    Err(error)
                }
            };
            Self::trace(client,context,&call,None,&callback,false).await?;
            context.check_in(client).await?;
            Ok(callback)
        }
        .await;
        admission.complete(outcome).await?
    }
}

#[async_trait]
impl PlatformBroker for NativePlatformBroker {
    async fn start(&self, context: &SandboxContext) -> Result<()> {
        let session = self.session(context).await?;
        let admission = Admission::begin(&self.db, &session, false).await?;
        let outcome = async {
            let client = admission.client();
            context.check_in(client).await?;
            let labels = Self::ledger(
                &self.db,
                client,
                &session,
                context.lease().execution,
                context.point_reads_only(),
            )
            .await?;
            context.record_consumed_markings(&labels)?;
            context.check_in(client).await?;
            context.complete_current_observation_in(client).await?;
            Ok(())
        }
        .await;
        admission.complete(outcome).await
    }
    async fn http(&self, context: &SandboxContext, call: HttpCall) -> Result<HttpResponse> {
        let path = call.path.split('?').next().unwrap_or("");
        if path.contains("/management/v1/fs/entries/")
            && ["/content", "/files"]
                .iter()
                .any(|suffix| path.ends_with(suffix))
        {
            return match self.http_content(context, call).await {
                Ok(response) => Ok(response),
                Err(error) => {
                    let code = error_code(&error);
                    Ok(HttpResponse {
                        status: Self::status(code),
                        headers: BTreeMap::from([(
                            "content-type".into(),
                            "application/json".into(),
                        )]),
                        body: STANDARD.encode(serde_json::to_vec(
                            &json!({"error":{"code":code,"message":code,"details":null}}),
                        )?),
                    })
                }
            };
        }
        let function_admission = call.method == "POST"
            && call.path.contains("/functions/")
            && call.path.ends_with("/invoke");
        let (status, value) = match self.http_inner(context, call).await {
            Ok(value) => (
                if function_admission
                    && ["accepted", "running"].contains(&value["status"].as_str().unwrap_or(""))
                {
                    202
                } else {
                    200
                },
                value,
            ),
            Err(error) => {
                let code = error_code(&error);
                let status = Self::status(code);
                (
                    status,
                    json!({"error":{"code":code,"message":code,"details":null}}),
                )
            }
        };
        let bytes = serde_json::to_vec(&value)?;
        ensure!(bytes.len() <= 262144, "FUNCTION_HTTP_LIMIT");
        Ok(HttpResponse {
            status,
            headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
            body: STANDARD.encode(bytes),
        })
    }
    async fn log(
        &self,
        context: &SandboxContext,
        message: &str,
        _consumed: &[ConsumedMarking],
    ) -> Result<()> {
        ensure!(
            message.len() <= 4096 && !message.contains('\0'),
            "FUNCTION_LOG_LIMIT"
        );
        let session = self.session(context).await?;
        let admission = Admission::begin(&self.db, &session, false).await?;
        let outcome=async{
            admission.client().query_one("SELECT id FROM moria.runtime_executions WHERE id=$1 FOR UPDATE",&[&context.lease().execution]).await?;
            let mut labels=Self::ledger(&self.db,admission.client(),&session,context.lease().execution,false).await?;
            labels.extend(context.consumed_markings());context.record_consumed_markings(&labels)?;context.check_in(admission.client()).await?;
            admission.client().query_one("SELECT moria.runtime_execution_event($1,'log',jsonb_build_object('message',$2::text,'markings',$3::jsonb))",&[&context.lease().execution,&message,&json!(context.consumed_markings())]).await?;
            context.check_in(admission.client()).await?;context.complete_current_observation_in(admission.client()).await?;Ok(())
        }.await;
        admission.complete(outcome).await
    }
    async fn finish(&self, context: &SandboxContext, result: &Value) -> Result<()> {
        let session = self.session(context).await?;
        let admission = Admission::begin(&self.db, &session, false).await?;
        let outcome = async {
            admission
                .client()
                .query_one(
                    "SELECT id FROM moria.runtime_executions WHERE id=$1 FOR UPDATE",
                    &[&context.lease().execution],
                )
                .await?;
            let mut labels = Self::ledger(
                &self.db,
                admission.client(),
                &session,
                context.lease().execution,
                context.effect() == "editProposal",
            )
            .await?;
            labels.extend(Self::output(admission.client(), &session, context, result).await?);
            context.record_consumed_markings(&labels)?;
            context.check_in(admission.client()).await?;
            context
                .complete_current_observation_in(admission.client())
                .await?;
            Ok(())
        }
        .await;
        admission.complete(outcome).await
    }
}

#[async_trait]
impl FunctionPolicy for NativePlatformBroker {
    async fn observe_child(
        &self,
        parent: &SandboxContext,
        session: &VerifiedSession,
        child: Uuid,
    ) -> Result<()> {
        self.consume_child(parent, session, child, None, None).await
    }
    async fn prepared(
        &self,
        client: &Client,
        session: &VerifiedSession,
        execution: Uuid,
        versions: bool,
    ) -> Result<Vec<ConsumedMarking>> {
        Self::prepared_ledger(&self.db, client, session, execution, versions).await
    }
    async fn committed_prepared(
        &self,
        client: &Client,
        session: &VerifiedSession,
        execution: Uuid,
    ) -> Result<()> {
        Self::committed_ledger(client, session, execution).await
    }
    async fn admit(
        &self,
        tx: &Transaction<'_>,
        session: &VerifiedSession,
        execution: Uuid,
        input: &Value,
        inputs: &Value,
    ) -> Result<Vec<ConsumedMarking>> {
        let mut labels = Self::references_in(
            tx.client(),
            session,
            execution,
            None,
            "input",
            input,
            inputs,
        )
        .await?;
        let parent: Option<Uuid> = tx
            .query_opt(
                "SELECT parent_id FROM moria.runtime_function_invocations WHERE execution=$1",
                &[&execution],
            )
            .await?
            .and_then(|row| row.get(0));
        if let Some(parent) = parent {
            labels.extend(Self::ledger(&self.db, tx.client(), session, parent, false).await?);
            Self::copy_filesystem(tx.client(), session, parent, execution, None, &labels).await?;
            let rows=tx.query("SELECT evidence FROM moria.native_function_reads WHERE execution=$1 ORDER BY call_key",&[&parent]).await?;
            for (index, row) in rows.into_iter().enumerate() {
                let evidence: Vec<ReadEvidence> = serde_json::from_value(row.get(0))?;
                Self::record(
                    tx.client(),
                    session,
                    execution,
                    None,
                    &format!("parent/{index}"),
                    &evidence,
                    &labels,
                )
                .await?;
            }
            tx.execute("INSERT INTO moria.native_function_failed_actions(execution,attempt,call_key,evidence,markings,fingerprint) SELECT $1,NULL,'parent/'||$2::uuid::text||'/'||call_key,evidence,markings,fingerprint FROM moria.native_function_failed_actions WHERE execution=$2 ON CONFLICT DO NOTHING",&[&execution,&parent]).await?;
            tx.execute("INSERT INTO moria.native_function_dependencies(execution,child) SELECT $1,child FROM moria.native_function_dependencies WHERE execution=$2 ON CONFLICT DO NOTHING",&[&execution,&parent]).await?;
        }
        Ok(labels)
    }
    async fn publish(
        &self,
        tx: &Transaction<'_>,
        context: &SandboxContext,
        result: &Value,
    ) -> Result<()> {
        tx.query_one(
            "SELECT id FROM moria.runtime_executions WHERE id=$1 FOR UPDATE",
            &[&context.lease().execution],
        )
        .await?;
        let session = Authority::sandbox_session_in(tx.client(), context).await?;
        let mut labels = Self::ledger(
            &self.db,
            tx.client(),
            &session,
            context.lease().execution,
            context.effect() == "editProposal",
        )
        .await?;
        labels.extend(Self::output(tx.client(), &session, context, result).await?);
        context.record_consumed_markings(&labels)?;
        let consumed = context
            .consumed_markings()
            .into_iter()
            .map(|label| label.marking)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let rows=tx.query("SELECT key,action,input,binding_hash,prepared_execution,prepared_hash FROM moria.native_function_proposals WHERE execution=$1 ORDER BY ordinal",&[&context.lease().execution]).await?;
        ensure!(
            rows.is_empty() || context.effect() == "editProposal",
            "FUNCTION_EFFECT"
        );
        for row in rows {
            let action: String = row.get(1);
            ensure!(
                Self::action_hash(tx.client(), &session, &action).await?.0
                    == row.get::<_, String>(3),
                "SCHEMA_CHANGED"
            );
            let prepared = if let Some(execution) = row.get::<_, Option<Uuid>>(4) {
                let function = NativeFunctions::new(self.db.clone(), Arc::new(self.clone()))
                    .load_prepared_in(tx.client(), &session, execution, &row.get::<_, String>(5))
                    .await?;
                Some(crate::serving::PreparedAction {
                    function,
                    contract: row.get(3),
                })
            } else {
                None
            };
            NativeServing::invoke_marked_prepared(
                &self.db,
                tx.client(),
                &session,
                &action,
                &row.get::<_, String>(0),
                &row.get::<_, Value>(2),
                (&consumed, prepared.as_ref()),
            )
            .await?;
        }
        if context.effect() == "editProposal" {
            Self::committed_ledger(tx.client(), &session, context.lease().execution).await?;
        }
        context.check_in(tx.client()).await?;
        Ok(())
    }
    async fn retained(
        &self,
        tx: &Transaction<'_>,
        session: &VerifiedSession,
        execution: Uuid,
    ) -> Result<Vec<ConsumedMarking>> {
        Self::ledger(&self.db, tx.client(), session, execution, false).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn action_keys_bind_parent_action_and_exact_guest_key() {
        let parent = Uuid::new_v4();
        let key = NativePlatformBroker::action_key(parent, "write/1", "edit").unwrap();
        assert_eq!(key.len(), 73);
        assert_eq!(
            key,
            NativePlatformBroker::action_key(parent, "write/1", "edit").unwrap()
        );
        assert_ne!(
            key,
            NativePlatformBroker::action_key(parent, "write/2", "edit").unwrap()
        );
        assert_ne!(
            key,
            NativePlatformBroker::action_key(parent, "write/1", "other").unwrap()
        );
        assert_ne!(
            key,
            NativePlatformBroker::action_key(Uuid::new_v4(), "write/1", "edit").unwrap()
        );
    }
    #[test]
    fn typed_references_include_nested_arrays_and_skip_nullable_absence() {
        let object_type = Uuid::new_v4().to_string();
        let uid = Uuid::new_v4();
        let schema = json!({"type":"object","properties":{"items":{"type":"array","nullable":false,"items":{"type":"ontologyObject","nullable":true,"objectType":object_type}}}});
        let value = json!({"items":[null,{"objectType":object_type,"uid":uid}]});
        values::normalize(&value, &schema).unwrap();
        let mut refs = vec![];
        NativePlatformBroker::references(&value, &schema, &mut refs).unwrap();
        assert_eq!(refs, vec![(object_type, uid)]);
    }
}
