use crate::{
    authority::{Authority, VerifiedSession},
    canonical,
    database::Database,
    execution::Lease,
    sandbox::{ConsumedMarking, PlatformBroker, SandboxContext, SandboxSupervisor},
    values,
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, env, sync::Arc, time::Duration};
use tokio::sync::watch;
use tokio_postgres::{Row, Transaction};
use uuid::Uuid;

/// Required native policy composition for input references, publication and retained data.
#[async_trait]
pub trait FunctionPolicy: PlatformBroker {
    async fn observe_child(
        &self,
        parent: &SandboxContext,
        session: &VerifiedSession,
        child: Uuid,
    ) -> Result<()>;
    async fn admit(
        &self,
        tx: &Transaction<'_>,
        session: &VerifiedSession,
        execution: Uuid,
        input: &Value,
        inputs: &Value,
    ) -> Result<Vec<ConsumedMarking>>;
    async fn publish(
        &self,
        tx: &Transaction<'_>,
        context: &SandboxContext,
        result: &Value,
    ) -> Result<VerifiedSession>;
    async fn retained(
        &self,
        tx: &Transaction<'_>,
        session: &VerifiedSession,
        execution: Uuid,
    ) -> Result<Vec<ConsumedMarking>>;
    async fn prepared(
        &self,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        execution: Uuid,
        versions: bool,
    ) -> Result<Vec<ConsumedMarking>>;
    async fn committed_prepared(
        &self,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        execution: Uuid,
    ) -> Result<()>;
}

#[derive(Clone)]
pub struct PreparedFunction {
    execution: Uuid,
    resource: Uuid,
    values: Value,
    hash: String,
}
impl PreparedFunction {
    pub fn id(&self) -> Uuid {
        self.execution
    }
    pub fn resource(&self) -> Uuid {
        self.resource
    }
    pub fn values(&self) -> &Value {
        &self.values
    }
    pub fn hash(&self) -> &str {
        &self.hash
    }
}

pub(crate) struct Admission {
    pub(crate) client: Option<deadpool_postgres::Object>,
}
impl Drop for Admission {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            drop(deadpool_postgres::Object::take(client));
        }
    }
}
impl Admission {
    pub(crate) async fn begin(
        db: &Database,
        environment: &str,
        write: bool,
        key: Option<&str>,
    ) -> Result<Self> {
        let admission = Self {
            client: Some(db.get().await?),
        };
        let client = admission.client.as_ref().unwrap();
        client
            .query_one(
                if write {
                    "SELECT pg_advisory_lock(hashtextextended($1,0))"
                } else {
                    "SELECT pg_advisory_lock_shared(hashtextextended($1,0))"
                },
                &[&format!("moria/environment/{environment}")],
            )
            .await?;
        client
            .query_one(
                "SELECT pg_advisory_lock_shared(hashtextextended('moria/filesystem',0))",
                &[],
            )
            .await?;
        if let Some(key) = key {
            client
                .query_one("SELECT pg_advisory_lock(hashtextextended($1,0))", &[&key])
                .await?;
        }
        Ok(admission)
    }
    pub(crate) async fn release(mut self) -> Result<()> {
        self.client
            .as_ref()
            .unwrap()
            .batch_execute("SELECT pg_advisory_unlock_all()")
            .await?;
        drop(self.client.take());
        Ok(())
    }
    pub(crate) async fn read_transaction(&mut self) -> Result<deadpool_postgres::Transaction<'_>> {
        Ok(self
            .client
            .as_mut()
            .unwrap()
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .start()
            .await?)
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Limits {
    timeout_seconds: u32,
    #[serde(default = "default_memory", rename = "memoryMiB")]
    memory_mi_b: u32,
    #[serde(default = "default_cpu")]
    cpu_millis: u32,
}
fn default_memory() -> u32 {
    256
}
fn default_cpu() -> u32 {
    1000
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    uri: String,
    digest: String,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    runtime: String,
    artifact: Artifact,
    inputs: Value,
    outputs: Value,
    effect: String,
    limits: Limits,
}
fn spec(definition: &Value) -> Result<Spec> {
    ensure!(
        definition["kind"] == "Function" && definition["apiVersion"] == "moria/v1alpha2",
        "FUNCTION_INVALID"
    );
    let spec: Spec = serde_json::from_value(definition["spec"].clone())
        .map_err(|_| anyhow::anyhow!("FUNCTION_INVALID"))?;
    ensure!(
        spec.runtime == "typescript-v1"
            && ["readOnly", "readWrite", "editProposal"].contains(&spec.effect.as_str())
            && (1..=30).contains(&spec.limits.timeout_seconds)
            && (32..=1024).contains(&spec.limits.memory_mi_b)
            && (1..=30000).contains(&spec.limits.cpu_millis),
        "FUNCTION_INVALID"
    );
    ensure!(
        spec.artifact.uri.starts_with("oci://")
            && spec.artifact.uri.len() <= 256
            && spec.artifact.digest.len() == 71
            && spec.artifact.digest.starts_with("sha256:")
            && spec.artifact.digest[7..]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "FUNCTION_ARTIFACT_INVALID"
    );
    for fields in [&spec.inputs, &spec.outputs] {
        ensure!(fields.is_object(), "FUNCTION_INVALID");
        values::descriptor(&json!({"type":"object","properties":fields}), &json!({}), 0)?;
    }
    Ok(spec)
}

pub(crate) fn source_profile(definition: &Value) -> Result<Value> {
    let parsed = spec(definition)?;
    crate::sandbox::artifact_image(&definition["spec"]["artifact"])?;
    for fields in [&parsed.inputs, &parsed.outputs] {
        values::descriptor(&json!({"type":"object","properties":fields}), &json!({}), 0)?;
    }
    Ok(
        json!({"domain":"function","artifact":definition["spec"]["artifact"],"limits":{"timeoutSeconds":parsed.limits.timeout_seconds,"memoryMiB":parsed.limits.memory_mi_b,"cpuMillis":parsed.limits.cpu_millis}}),
    )
}
fn key(value: &str) -> Result<()> {
    ensure!(
        value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphanumeric())
            && value.len() <= 128
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.:/-".contains(&b)),
        "FUNCTION_INVALID"
    );
    Ok(())
}
fn input(value: &Value, spec: &Spec) -> Result<(Value, String)> {
    ensure!(
        canonical::canonical_json(value)?.len() <= 32768,
        "FUNCTION_INPUT_INVALID"
    );
    let descriptor = values::descriptor(
        &json!({"type":"object","properties":spec.inputs}),
        &json!({}),
        0,
    )?;
    let mut value = value.clone();
    let object = value
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("FUNCTION_INPUT_INVALID"))?;
    for (name, property) in descriptor["properties"].as_object().unwrap() {
        if !object.contains_key(name) && property["nullable"] == true {
            object.insert(name.clone(), Value::Null);
        }
    }
    let value = values::normalize(&value, &descriptor)
        .map_err(|_| anyhow::anyhow!("FUNCTION_INPUT_INVALID"))?;
    ensure!(
        canonical::canonical_json(&value)?.len() <= 32768,
        "FUNCTION_INPUT_INVALID"
    );
    let hash = canonical::value_fingerprint(&value, &descriptor, &json!({}))?;
    Ok((value, hash.strip_prefix("sha256:").unwrap().into()))
}
fn labels(groups: &[Vec<ConsumedMarking>], environment: &str) -> Result<Vec<ConsumedMarking>> {
    let combined: BTreeSet<_> = groups.iter().flatten().cloned().collect();
    ensure!(
        combined.len() <= 256
            && combined.iter().all(|m| m.environment == environment
                && !m.marking.is_empty()
                && m.marking.len() <= 180
                && !m.marking.contains('\0')),
        "FUNCTION_MARKINGS_INVALID"
    );
    Ok(combined.into_iter().collect())
}
fn stored_labels(value: &Value) -> Result<Vec<ConsumedMarking>> {
    value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
        .iter()
        .map(|m| {
            Ok(ConsumedMarking {
                environment: m["environment"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
                    .into(),
                marking: m["marking"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
                    .into(),
            })
        })
        .collect()
}

#[derive(Clone)]
pub struct NativeFunctions {
    pub db: Database,
    policy: Arc<dyn FunctionPolicy>,
}
/// Constructed only after a live parent and its exact accepted child are checked.
pub(crate) enum ChildProgress {
    Pending(Value),
    Complete(Value),
}
impl ChildProgress {
    pub(crate) fn is_pending(&self) -> bool {
        matches!(self, Self::Pending(_))
    }
    pub(crate) fn into_value(self) -> Value {
        match self {
            Self::Pending(value) | Self::Complete(value) => value,
        }
    }
}
impl NativeFunctions {
    pub fn new(db: Database, policy: Arc<dyn FunctionPolicy>) -> Self {
        Self { db, policy }
    }
    fn prepared_value(row: &Row) -> Result<PreparedFunction> {
        ensure!(
            row.get::<_, bool>("prepare_only")
                && row.get::<_, String>("status") == "succeeded"
                && !row.get::<_, bool>("expired"),
            "FUNCTION_PREPARATION_INVALID"
        );
        Ok(PreparedFunction {
            execution: row.get("id"),
            resource: row.get("resource"),
            values: row
                .get::<_, Option<Value>>("function_result")
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_RECEIPT_MISSING"))?,
            hash: row
                .get::<_, Option<String>>("function_hash")
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_RECEIPT_MISSING"))?,
        })
    }
    pub async fn prepare_action(
        &self,
        session: &VerifiedSession,
        resource: Uuid,
        action_key: &str,
        input: Value,
    ) -> Result<PreparedFunction> {
        self.prepare_action_inner(session, resource, action_key, input, None)
            .await
    }
    pub(crate) async fn prepare_action_child(
        &self,
        session: &VerifiedSession,
        resource: Uuid,
        action_key: &str,
        input: Value,
        parent: &SandboxContext,
    ) -> Result<PreparedFunction> {
        self.prepare_action_inner(session, resource, action_key, input, Some(parent))
            .await
    }
    async fn prepare_action_inner(
        &self,
        session: &VerifiedSession,
        resource: Uuid,
        action_key: &str,
        input: Value,
        parent: Option<&SandboxContext>,
    ) -> Result<PreparedFunction> {
        if let Some(parent) = parent {
            parent.check().await?;
        }
        ensure!(
            !action_key.is_empty() && action_key.len() <= 1024 && !action_key.contains('\0'),
            "FUNCTION_INVALID"
        );
        let hash = canonical::fingerprint(&match parent {
            Some(context) => json!(["child", resource, action_key, context.lease().execution]),
            None => json!([resource, action_key]),
        })?;
        let key = format!("action/{}", hash.strip_prefix("sha256:").unwrap());
        let accepted = self
            .invoke(session, resource, &key, input, parent, true)
            .await?;
        let id: Uuid = accepted["id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_INVALID"))?
            .parse()?;
        let finished = if let Some(parent) = parent {
            self.wait_child(parent, id, 25000)
                .await
                .map(ChildProgress::into_value)
        } else {
            self.wait(session, id, 25000).await
        };
        if let Some(parent) = parent {
            self.policy.observe_child(parent, session, id).await?;
        }
        let finished = finished?;
        if let Some(parent) = parent {
            parent.check().await?;
        }
        ensure!(
            finished["status"] == "succeeded",
            "FUNCTION_PREPARATION_FAILED"
        );
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        let row = Self::row(tx.client(), session, id).await?;
        self.retained(&tx, session, &row).await?;
        if let Some(parent) = parent {
            parent.check_in(tx.client()).await?;
        }
        let prepared = Self::prepared_value(&row)?;
        tx.commit().await?;
        admission.release().await?;
        Ok(prepared)
    }
    pub async fn check_prepared_in(
        &self,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        prepared: &PreparedFunction,
    ) -> Result<Vec<ConsumedMarking>> {
        let row = Self::row(client, session, prepared.execution).await?;
        let stored = Self::prepared_value(&row)?;
        ensure!(
            stored.hash == prepared.hash
                && stored.values == prepared.values
                && stored.resource == prepared.resource,
            "FUNCTION_PREPARATION_CHANGED"
        );
        self.retained_prepared_row(client, session, &row).await
    }
    pub(crate) async fn load_prepared_in(
        &self,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        execution: Uuid,
        expected_hash: &str,
    ) -> Result<PreparedFunction> {
        ensure!(
            expected_hash.len() == 64
                && expected_hash
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "FUNCTION_PREPARATION_INVALID"
        );
        let row = Self::row(client, session, execution).await?;
        let prepared = Self::prepared_value(&row)?;
        ensure!(
            prepared.hash == expected_hash,
            "FUNCTION_PREPARATION_CHANGED"
        );
        let outputs:Value=client.query_one("SELECT b.outputs FROM moria.runtime_sandbox_bindings b JOIN moria.runtime_executions e ON e.binding=b.binding WHERE e.id=$1 AND b.domain='function'",&[&execution]).await?.get(0);
        let hash = canonical::value_fingerprint(
            &prepared.values,
            &json!({"type":"object","properties":outputs}),
            &json!({}),
        )?;
        ensure!(
            hash.strip_prefix("sha256:") == Some(expected_hash),
            "FUNCTION_PREPARATION_CHANGED"
        );
        self.retained_prepared_row(client, session, &row).await?;
        Ok(prepared)
    }
    async fn retained_prepared_row(
        &self,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        row: &Row,
    ) -> Result<Vec<ConsumedMarking>> {
        Self::prepared_value(row)?;
        let persisted = labels(
            &[
                stored_labels(&row.get::<_, Value>("function_markings"))?,
                crate::evaluations::retained_input_in(client, session, row.get("id")).await?,
                crate::automations::retained_in(client, session, row.get("id")).await?,
                crate::sandbox::retained_observations_in(client, row.get("id")).await?,
                crate::sandbox::released_source_markings_in(
                    client,
                    session,
                    row.get("resource"),
                    "invoke",
                    &row.get::<_, String>("release"),
                )
                .await?,
            ],
            session.environment(),
        )?;
        Self::authorize(client, session, row.get("resource"), &persisted).await?;
        let strict = row.get::<_, Option<Value>>("proposal_commit").is_none();
        let observed = self
            .policy
            .prepared(client, session, row.get("id"), strict)
            .await?;
        Self::authorize(
            client,
            session,
            row.get("resource"),
            &labels(&[persisted, observed], session.environment())?,
        )
        .await
    }
    pub async fn retained_prepared_in(
        &self,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        execution: Uuid,
    ) -> Result<Vec<ConsumedMarking>> {
        let row = Self::row(client, session, execution).await?;
        self.retained_prepared_row(client, session, &row).await
    }
    pub async fn committed_prepared_in(
        &self,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        prepared: &PreparedFunction,
        receipt: &Value,
    ) -> Result<()> {
        let row = Self::row(client, session, prepared.execution).await?;
        let stored = Self::prepared_value(&row)?;
        ensure!(
            stored.hash == prepared.hash && stored.values == prepared.values,
            "FUNCTION_PREPARATION_CHANGED"
        );
        if row.get::<_, Option<Value>>("proposal_commit").is_none() {
            self.policy
                .committed_prepared(client, session, prepared.execution)
                .await?;
            client.execute("UPDATE moria.runtime_function_invocations SET proposal_commit=$2 WHERE execution=$1 AND proposal_commit IS NULL", &[&prepared.execution,&receipt]).await?;
        }
        Ok(())
    }
    async fn authorize(
        tx: &tokio_postgres::Client,
        session: &VerifiedSession,
        resource: Uuid,
        marks: &[ConsumedMarking],
    ) -> Result<Vec<ConsumedMarking>> {
        ensure!(tx.query_opt("SELECT 1 FROM moria.fs_entries e JOIN moria.fs_resource_bindings b ON b.entry_id=e.id AND b.active JOIN moria.fs_roles r ON r.entry_id=b.root_id AND NOT r.retired WHERE e.resource_id=$1 AND e.environment_id=$2 AND e.kind='resource' AND NOT e.deleted", &[&resource,&session.environment()]).await?.is_some(),"NOT_FOUND");
        let (snapshot, source) =
            Authority::current_resource_for_operation(tx, session, &resource.to_string(), "invoke")
                .await?;
        let source = source
            .into_iter()
            .map(|marking| ConsumedMarking {
                environment: session.environment().into(),
                marking,
            })
            .collect();
        let marks = labels(&[marks.to_vec(), source], session.environment())?;
        crate::execution::invocation_permission(
            &snapshot.identity,
            resource,
            "functions",
            "invoke",
            &marks.iter().map(|m| m.marking.clone()).collect::<Vec<_>>(),
        )?;
        Authority::finish(tx, session, &snapshot).await?;
        Ok(marks)
    }
    async fn row(tx: &tokio_postgres::Client, session: &VerifiedSession, id: Uuid) -> Result<Row> {
        let row=tx.query_opt("SELECT e.*,f.entry_id,f.release,f.definition,f.parent_id,f.prepare_only,f.markings AS function_markings,r.result AS function_result,r.result_hash AS function_hash,f.proposal_commit,r.committed_at,r.markings AS receipt_markings,f.attempt_id,f.correlation_id,(e.finished_at IS NOT NULL AND e.finished_at<clock_timestamp()-interval '30 days') AS expired,to_jsonb(e)->>'created_at' AS created_string,to_jsonb(e)->>'finished_at' AS finished_string,COALESCE((SELECT jsonb_agg(jsonb_build_object('key',c.call_id,'path',c.path,'status',CASE WHEN c.status<200 THEN 'started' WHEN c.status>=400 THEN 'failed' WHEN EXISTS(SELECT 1 FROM moria.native_function_proposals p WHERE p.execution=c.execution AND p.key=c.action_key) THEN CASE WHEN r.execution IS NULL THEN 'proposed' ELSE 'committed' END ELSE 'succeeded' END) ORDER BY c.created_at,c.call_id) FROM moria.native_function_calls c WHERE c.execution=e.id),'[]') AS public_calls FROM moria.runtime_function_invocations f JOIN moria.runtime_executions e ON e.id=f.execution LEFT JOIN moria.runtime_function_receipts r ON r.execution=e.id WHERE e.id=$1 AND e.environment=$2 AND e.principal=$3 AND e.application=$4", &[&id,&session.environment(),&session.principal(),&session.application()]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        if !row.get::<_, bool>("expired")
            && row
                .get::<_, Value>("context")
                .get("serviceParentExecution")
                .is_some()
        {
            tx.query_one(
                "SELECT moria.runtime_sandbox_service_child_retained($1,$2)",
                &[&row.get::<_, Uuid>("binding"), &session.session_hash()],
            )
            .await?;
        }
        Ok(row)
    }
    fn public(row: &Row, marks: Vec<ConsumedMarking>, include_result: bool) -> Result<Value> {
        ensure!(!row.get::<_, bool>("expired"), "RESULT_EXPIRED");
        let definition: Value = row.get("definition");
        let status: String = row.get("status");
        let mut value = json!({"valueVersion":1,"id":row.get::<_,Uuid>("id"),"resourceId":row.get::<_,Uuid>("resource"),"release":row.get::<_,String>("release"),"artifact":definition["spec"]["artifact"],"status":match status.as_str(){"queued"=>"accepted","preparing"=>"running",other=>other},"effect":definition["spec"]["effect"],"createdAt":row.get::<_,String>("created_string"),"finishedAt":row.get::<_,Option<String>>("finished_string"),"markings":marks});
        value["calls"] = row.get::<_, Value>("public_calls");
        if let Some(attempt) = row.get::<_, Option<Uuid>>("attempt_id") {
            value["attemptId"] = json!(attempt);
        }
        if let Some(correlation) = row.get::<_, Option<Uuid>>("correlation_id") {
            value["correlationId"] = json!(correlation);
        }
        if let Some(parent) = row.get::<_, Option<Uuid>>("parent_id") {
            value["parentId"] = json!(parent);
        }
        if row.get::<_, bool>("prepare_only") {
            value["prepareOnly"] = json!(true);
        }
        if let Some(receipt) = row.get::<_, Option<Value>>("proposal_commit") {
            value["proposalCommit"] = receipt;
        }
        if include_result && status == "succeeded" {
            value["result"] = row
                .get::<_, Option<Value>>("function_result")
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_RECEIPT_MISSING"))?;
        }
        if status == "failed" {
            let code = row
                .get::<_, Option<String>>("error")
                .unwrap_or_else(|| "FUNCTION_FAILED".into());
            let code = match code.as_str() {
                "WORKER_LOST" => "FUNCTION_WORKER_LOST",
                "DEADLINE_EXCEEDED" => "FUNCTION_TIMEOUT",
                other => other,
            };
            value["error"] = json!({"code":code,"message":"Function execution failed"});
        }
        Ok(value)
    }
    async fn retained(
        &self,
        tx: &Transaction<'_>,
        session: &VerifiedSession,
        row: &Row,
    ) -> Result<Vec<ConsumedMarking>> {
        ensure!(!row.get::<_, bool>("expired"), "RESULT_EXPIRED");
        let persisted = labels(
            &[
                stored_labels(&row.get::<_, Value>("function_markings"))?,
                crate::evaluations::retained_input_in(tx.client(), session, row.get("id")).await?,
                crate::automations::retained_in(tx.client(), session, row.get("id")).await?,
                crate::sandbox::retained_observations_in(tx.client(), row.get("id")).await?,
                crate::sandbox::released_source_markings_in(
                    tx.client(),
                    session,
                    row.get("resource"),
                    "invoke",
                    &row.get::<_, String>("release"),
                )
                .await?,
            ],
            session.environment(),
        )?;
        Self::authorize(tx.client(), session, row.get("resource"), &persisted).await?;
        crate::sandbox::require_clean_observations_in(tx.client(), row.get("id")).await?;
        let observed = self.policy.retained(tx, session, row.get("id")).await?;
        Self::authorize(
            tx.client(),
            session,
            row.get("resource"),
            &labels(&[persisted, observed], session.environment())?,
        )
        .await
    }

    pub async fn invoke(
        &self,
        session: &VerifiedSession,
        resource: Uuid,
        request_key: &str,
        payload: Value,
        parent: Option<&SandboxContext>,
        prepare_only: bool,
    ) -> Result<Value> {
        key(request_key)?;
        let lock = format!(
            "moria/function/request/{}",
            canonical::fingerprint(&json!([
                session.environment(),
                resource,
                session.principal(),
                session.application(),
                request_key
            ]))?
        );
        if let Some(parent) = parent {
            parent.check().await?;
            ensure!(
                ["function", "agent"].contains(&parent.domain())
                    && parent.environment() == session.environment()
                    && parent.principal() == session.principal()
                    && parent.application() == session.application()
                    && parent.session_hash() == Some(session.session_hash()),
                "FUNCTION_ACTOR_MISMATCH"
            );
        }
        let mut admission =
            Admission::begin(&self.db, session.environment(), false, Some(&lock)).await?;
        let tx = admission
            .client
            .as_mut()
            .unwrap()
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .start()
            .await?;
        let source=tx.query_opt("SELECT b.entry_id,b.digest,b.reference,a.resources FROM moria.fs_entries e JOIN moria.fs_resource_bindings b ON b.entry_id=e.id AND b.active JOIN moria.fs_roles role ON role.entry_id=b.root_id AND NOT role.retired JOIN moria.fs_active_releases a ON a.root_id=b.root_id AND a.environment=b.environment AND a.digest=b.digest WHERE e.resource_id=$1 AND e.environment_id=$2 AND e.kind='resource' AND NOT e.deleted", &[&resource,&session.environment()]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        let source_marks = Self::authorize(tx.client(), session, resource, &[]).await?;
        if let Some(existing)=tx.query_opt("SELECT e.id,f.definition,f.prepare_only FROM moria.runtime_executions e JOIN moria.runtime_function_invocations f ON f.execution=e.id WHERE e.environment=$1 AND e.resource=$2 AND e.principal=$3 AND e.application=$4 AND e.request_key=$5", &[&session.environment(),&resource,&session.principal(),&session.application(),&request_key]).await? {
            ensure!(existing.get::<_,bool>("prepare_only")==prepare_only,"IDEMPOTENCY_CONFLICT");
            let (normalized,hash)=input(&payload,&spec(&existing.get::<_,Value>("definition"))?).map_err(|_|anyhow::anyhow!("IDEMPOTENCY_CONFLICT"))?;
            let old=Self::row(tx.client(),session,existing.get("id")).await?;
            ensure!(old.get::<_,Value>("input")==normalized && old.get::<_,String>("input_hash")==hash,"IDEMPOTENCY_CONFLICT");
            let marks=self.retained(&tx,session,&old).await?;let value=Self::public(&old,marks,false)?;
            tx.commit().await?;admission.release().await?;return Ok(value);
        }
        let reference: String = source.get("reference");
        let resources: Value = source.get("resources");
        let definition = resources
            .as_array()
            .and_then(|resources| {
                resources.iter().find(|r| {
                    format!(
                        "{}/{}/{}",
                        r["metadata"]["namespace"].as_str().unwrap_or(""),
                        r["kind"].as_str().unwrap_or(""),
                        r["metadata"]["name"].as_str().unwrap_or("")
                    ) == reference
                })
            })
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_INVALID"))?;
        let resolved = spec(&definition)?;
        let (normalized, hash) = input(&payload, &resolved)?;
        ensure!(
            !prepare_only || resolved.effect == "editProposal",
            "FUNCTION_EFFECT"
        );
        let mut parent_id = None;
        let mut inherited = Vec::new();
        let mut parent_deadline = None::<String>;
        if let Some(parent) = parent {
            let chain=tx.query("WITH RECURSIVE ancestors AS (SELECT e.id,e.resource,e.status,e.deadline,e.spec->>'domain' AS domain,d.parent_execution,1 AS depth FROM moria.runtime_executions e JOIN moria.runtime_job_delegations d ON d.execution=e.id WHERE e.id=$1 UNION ALL SELECT e.id,e.resource,e.status,e.deadline,e.spec->>'domain',d.parent_execution,a.depth+1 FROM ancestors a JOIN moria.runtime_job_delegations d ON d.execution=a.parent_execution JOIN moria.runtime_executions e ON e.id=d.execution WHERE a.depth<9) SELECT *,deadline::text AS deadline_string FROM ancestors ORDER BY depth LIMIT 9", &[&parent.lease().execution]).await?;
            ensure!(
                !chain.is_empty()
                    && chain.len() <= 8
                    && chain
                        .iter()
                        .filter(|row| ["function", "agent"]
                            .contains(&row.get::<_, String>("domain").as_str()))
                        .count()
                        < 4
                    && chain.iter().all(|r| ["function", "agent", "evaluation"]
                        .contains(&r.get::<_, String>("domain").as_str())
                        && r.get::<_, Uuid>("resource") != resource
                        && ["queued", "running", "preparing"]
                            .contains(&r.get::<_, String>("status").as_str())),
                "FUNCTION_RECURSION"
            );
            ensure!(
                parent.effect() == "readWrite"
                    || resolved.effect == "readOnly"
                    || (prepare_only
                        && parent.effect() == "editProposal"
                        && resolved.effect == "editProposal"),
                "FUNCTION_EFFECT"
            );
            parent_id = Some(parent.lease().execution);
            inherited = parent.consumed_markings();
            parent_deadline = Some(chain[0].get("deadline_string"));
        }
        let release: String = source.get("digest");
        let release_hash = release
            .strip_prefix("sha256:")
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_INVALID"))?;
        let marks = labels(&[source_marks, inherited], session.environment())?;
        let snapshot = Authority::current(&*tx, session).await?;
        let execution_spec = json!({"version":1,"domain":"function","release":release_hash,"resourceClass":"standard","timeoutSeconds":resolved.limits.timeout_seconds,"retryPolicy":"never","inputSchema":{"type":"object","properties":resolved.inputs},"pinnedInputs":[],"expectedHeads":{}});
        let context = json!({"environment":session.environment(),"principal":session.principal(),"application":session.application(),"sessionHash":session.session_hash(),"parentDeadline":parent_deadline});
        let operation = crate::execution::invocation_permission(
            &snapshot.identity,
            resource,
            "functions",
            "invoke",
            &marks.iter().map(|m| m.marking.clone()).collect::<Vec<_>>(),
        )?;
        let purpose: Option<Value> = if parent.is_some() {
            tx.query_one(
                "SELECT moria.runtime_sandbox_service_child_metadata($1,$2)",
                &[&session.session_hash(), &resource],
            )
            .await?
            .get(0)
        } else {
            None
        };
        let (binding, marks) = if let Some(metadata) = purpose {
            let parent = parent.unwrap();
            ensure!(
                metadata["parent"] == json!(parent.lease().execution)
                    && metadata["task"] == json!(parent.lease().task)
                    && metadata["owner"] == json!(parent.lease().owner)
                    && metadata["fence"] == json!(parent.lease().fence),
                "JOB_PARENT_INACTIVE"
            );
            let service = crate::jobs::RegisteredService::load_current_in(
                tx.client(),
                session.environment(),
                resource,
                "function",
                metadata["revision"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("SERVICE_PURPOSE_REVOKED"))?,
            )
            .await?;
            ensure!(
                metadata["authority"] == json!(service.id()),
                "SERVICE_PURPOSE_REVOKED"
            );
            let marks = Self::authorize(
                tx.client(),
                session,
                resource,
                &labels(
                    &[marks, stored_labels(&metadata["markings"])?],
                    session.environment(),
                )?,
            )
            .await?;
            let binding = service
                .bind_sandbox_child_in(
                    tx.client(),
                    &execution_spec,
                    &[format!("{resource}:invoke")],
                    &marks
                        .iter()
                        .map(|mark| mark.marking.clone())
                        .collect::<Vec<_>>(),
                    session,
                    &metadata,
                )
                .await?;
            (binding.id, marks)
        } else {
            let binding:Uuid=tx.query_one("INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$8,$5,$6,$7,incarnation FROM moria.indexing_instance RETURNING id", &[&session.environment(),&resource,&execution_spec,&context,&marks.iter().map(|m|m.marking.clone()).collect::<Vec<_>>(),&snapshot.security_epoch.parse::<i64>()?,&snapshot.instance_epoch.parse::<i64>()?,&vec![operation]]).await?.get(0);
            (binding, marks)
        };
        tx.execute("INSERT INTO moria.runtime_function_bindings(binding,artifact_uri,artifact_digest,outputs,effect,memory_mib,cpu_millis,timeout_seconds) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", &[&binding,&resolved.artifact.uri,&resolved.artifact.digest,&resolved.outputs,&resolved.effect,&(resolved.limits.memory_mi_b as i32),&(resolved.limits.cpu_millis as i32),&(resolved.limits.timeout_seconds as i32)]).await?;
        let id: Uuid = tx
            .query_one(
                "SELECT moria.runtime_execution_request($1,$2,$3,$4,$5)",
                &[
                    &binding,
                    &request_key,
                    &normalized,
                    &hash,
                    &json!([{"key":"function","partition":null}]),
                ],
            )
            .await?
            .get(0);
        tx.execute("INSERT INTO moria.runtime_function_invocations(execution,entry_id,release,definition,parent_id,prepare_only,markings) VALUES($1,$2,$3,$4,$5,$6,$7)", &[&id,&source.get::<_,Uuid>("entry_id"),&release,&definition,&parent_id,&prepare_only,&serde_json::to_value(&marks)?]).await?;
        let references = self
            .policy
            .admit(&tx, session, id, &normalized, &resolved.inputs)
            .await?;
        let marks = Self::authorize(
            tx.client(),
            session,
            resource,
            &labels(&[marks, references], session.environment())?,
        )
        .await?;
        tx.execute(
            "UPDATE moria.runtime_function_invocations SET markings=$2 WHERE execution=$1",
            &[&id, &serde_json::to_value(&marks)?],
        )
        .await?;
        let row = Self::row(tx.client(), session, id).await?;
        let value = Self::public(&row, marks, false)?;
        Authority::finish(&*tx, session, &snapshot).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(value)
    }

    pub async fn get(&self, session: &VerifiedSession, id: Uuid) -> Result<Value> {
        self.db
            .get()
            .await?
            .query_one("SELECT moria.runtime_execution_reap($1)", &[&id])
            .await?;
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission
            .client
            .as_mut()
            .unwrap()
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .start()
            .await?;
        let row = Self::row(tx.client(), session, id).await?;
        let marks = self.retained(&tx, session, &row).await?;
        let value = Self::public(&row, marks, true)?;
        tx.commit().await?;
        admission.release().await?;
        Ok(value)
    }
    pub async fn events(&self, session: &VerifiedSession, id: Uuid, after: &str) -> Result<Value> {
        let sequence: i64 = after.parse()?;
        ensure!(
            sequence >= 0 && sequence.to_string() == after,
            "FUNCTION_INVALID"
        );
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.read_transaction().await?;
        let row = Self::row(tx.client(), session, id).await?;
        self.retained(&tx, session, &row).await?;
        let events=tx.query("SELECT e.sequence::text,e.kind,e.payload,to_jsonb(e)->>'created_at' AS created FROM moria.runtime_execution_events e WHERE e.execution=$1 AND e.sequence>$2 ORDER BY e.sequence LIMIT 128", &[&id,&sequence]).await?;
        let values:Vec<Value>=events.into_iter().filter_map(|event|{let kind:String=event.get("kind");let payload:Value=event.get("payload");let message=if kind=="log" {payload["message"].as_str()?.to_owned()} else {match kind.as_str(){"accepted"=>"accepted","claimed"|"sealed"|"preparing"=>"running","succeeded"|"completed"=>"succeeded","cancelled"=>"cancelled","failed"=>"failed",_=>return None}.into()};Some(json!({"sequence":event.get::<_,String>("sequence"),"type":if kind=="log"{"log"}else{"state"},"message":message,"createdAt":event.get::<_,String>("created")}))}).collect();
        tx.commit().await?;
        admission.release().await?;
        Ok(json!(values))
    }
    pub async fn cancel(&self, session: &VerifiedSession, id: Uuid) -> Result<Value> {
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        let row=tx.query_opt("SELECT e.binding,e.resource,f.release FROM moria.runtime_executions e JOIN moria.runtime_function_invocations f ON f.execution=e.id WHERE e.id=$1 AND e.environment=$2 AND e.principal=$3 AND e.application=$4 FOR UPDATE OF e",&[&id,&session.environment(),&session.principal(),&session.application()]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        crate::execution::cancellation_authority_in(tx.client(), session, &row, "invoke").await?;
        crate::execution::cancel_authorized_in(tx.client(), session, id).await?;
        tx.commit().await?;
        admission.release().await?;
        crate::execution::ExecutionAuthority {
            db: self.db.clone(),
        }
        .reap_cancellations(32)
        .await?;
        Ok(json!({"id":id,"cancelRequested":true}))
    }
    pub(crate) async fn child_progress(
        &self,
        parent: &SandboxContext,
        id: Uuid,
    ) -> Result<ChildProgress> {
        // Child startup needs a shared parent row while progress authorization is running.
        let session = {
            let client = self.db.get().await?;
            Authority::sandbox_session_in(&client, parent).await?
        };
        let mut admission = Admission::begin(&self.db, parent.environment(), false, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        let row = Self::row(tx.client(), &session, id).await?;
        ensure!(
            row.get::<_, Option<Uuid>>("parent_id") == Some(parent.lease().execution)
                && id != parent.lease().execution,
            "FUNCTION_RECURSION"
        );
        let status: String = row.get("status");
        let progress = if ["queued", "running", "preparing"].contains(&status.as_str()) {
            let accepted: Vec<String> = tx
                .query_one(
                    "SELECT markings FROM moria.runtime_execution_bindings WHERE id=$1",
                    &[&row.get::<_, Uuid>("binding")],
                )
                .await?
                .get(0);
            let mut marks = accepted
                .into_iter()
                .map(|marking| ConsumedMarking {
                    environment: session.environment().into(),
                    marking,
                })
                .collect::<Vec<_>>();
            marks.extend(
                crate::sandbox::released_source_markings_in(
                    tx.client(),
                    &session,
                    row.get("resource"),
                    "invoke",
                    &row.get::<_, String>("release"),
                )
                .await?,
            );
            let marks = Self::authorize(tx.client(), &session, row.get("resource"), &marks).await?;
            let mut value = Self::public(&row, marks, false)?;
            value["status"] = json!("accepted");
            value["finishedAt"] = Value::Null;
            value["calls"] = json!([]);
            ChildProgress::Pending(value)
        } else {
            let marks = self.retained(&tx, &session, &row).await?;
            ChildProgress::Complete(Self::public(&row, marks, true)?)
        };
        parent.check_in(tx.client()).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(progress)
    }
    pub(crate) async fn wait_child(
        &self,
        parent: &SandboxContext,
        id: Uuid,
        milliseconds: u64,
    ) -> Result<ChildProgress> {
        ensure!(milliseconds <= 25000, "FUNCTION_INVALID");
        let until = tokio::time::Instant::now() + Duration::from_millis(milliseconds);
        loop {
            let progress = self.child_progress(parent, id).await?;
            if !progress.is_pending() || tokio::time::Instant::now() >= until {
                return Ok(progress);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    pub async fn wait(
        &self,
        session: &VerifiedSession,
        id: Uuid,
        milliseconds: u64,
    ) -> Result<Value> {
        ensure!(milliseconds <= 25000, "FUNCTION_INVALID");
        let until = tokio::time::Instant::now() + Duration::from_millis(milliseconds);
        loop {
            let value = match self.get(session, id).await {
                Ok(value) => value,
                Err(error)
                    if crate::database::safe_error(&error) == "FUNCTION_OBSERVATION_INCOMPLETE"
                        && tokio::time::Instant::now() < until =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if !["accepted", "running"].contains(&value["status"].as_str().unwrap_or(""))
                || tokio::time::Instant::now() >= until
            {
                return Ok(value);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn worker_once(&self, host: Uuid, owner: Uuid) -> Result<bool> {
        let (_sender, cancel) = watch::channel(false);
        self.worker_once_cancel(host, owner, cancel).await
    }
    async fn refresh_candidate(
        &self,
        owner: Uuid,
        publication: bool,
        target: Option<Uuid>,
    ) -> Result<Option<Uuid>> {
        for retry in 0..4 {
            let result = self
                .refresh_candidate_once(owner, publication, target)
                .await;
            if result
                .as_ref()
                .is_err_and(crate::execution::transaction_conflict)
                && retry < 3
            {
                tokio::time::sleep(Duration::from_millis(10 << retry)).await;
            } else {
                return result;
            }
        }
        unreachable!()
    }
    async fn refresh_candidate_once(
        &self,
        owner: Uuid,
        publication: bool,
        target: Option<Uuid>,
    ) -> Result<Option<Uuid>> {
        let candidates=self.db.get().await?.query("SELECT e.id,e.environment FROM moria.runtime_executions e JOIN moria.runtime_function_invocations f ON f.execution=e.id JOIN moria.runtime_resource_classes c ON c.name=e.resource_class AND c.enabled JOIN moria.runtime_function_workers w ON w.owner=$1 AND e.resource_class=ANY(w.resource_classes) WHERE moria.runtime_environment_cell_available(e.environment) AND e.spec->>'domain'='function' AND ($3::uuid IS NULL OR e.id=$3) AND e.deadline>clock_timestamp() AND ((NOT $2 AND e.status='queued' AND EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.execution=e.id AND t.status='pending' AND t.attempts=0)) OR ($2 AND e.status='running' AND NOT EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.execution=e.id AND t.status<>'sealed'))) AND moria.runtime_resource_in_use(e.resource_class)<c.maximum_tasks AND moria.runtime_resource_in_use(e.resource_class,e.environment)<c.maximum_environment_tasks ORDER BY e.created_at LIMIT 16", &[&owner,&publication,&target]).await?;
        for candidate in candidates {
            let id: Uuid = candidate.get(0);
            let environment: String = candidate.get(1);
            let mut admission = Admission::begin(&self.db, &environment, false, None).await?;
            let tx = admission
                .client
                .as_mut()
                .unwrap()
                .build_transaction()
                .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
                .start()
                .await?;
            let Some(row)=tx.query_opt("SELECT e.binding,e.context,e.principal,e.application FROM moria.runtime_executions e WHERE e.id=$1 AND e.deadline>clock_timestamp() AND e.status=$2 FOR UPDATE SKIP LOCKED", &[&id,&if publication{"running"}else{"queued"}]).await? else {continue};
            let task=tx.query_one("SELECT id,status,owner,fence,attempts FROM moria.runtime_execution_tasks WHERE execution=$1 FOR UPDATE", &[&id]).await?;
            if (!publication
                && (task.get::<_, String>("status") != "pending"
                    || task.get::<_, i32>("attempts") != 0))
                || (publication && task.get::<_, String>("status") != "sealed")
            {
                continue;
            }
            let result=async {
                let actor:Value=row.get("context");
                if actor["sessionHash"].is_null(){tx.query_one("SELECT moria.runtime_sandbox_claim_authority($1)",&[&id]).await?;return Ok(());}
                tx.batch_execute("SAVEPOINT original_session_admission").await?;
                let session=match Authority::stored_session_in(tx.client(),actor["sessionHash"].as_str().ok_or_else(||anyhow::anyhow!("UNAUTHENTICATED"))?,&environment,&row.get::<_,String>("principal"),&row.get::<_,String>("application")).await {
                    Ok(session)=>{tx.batch_execute("RELEASE SAVEPOINT original_session_admission").await?;session},
                    Err(_)=>{tx.batch_execute("ROLLBACK TO SAVEPOINT original_session_admission; RELEASE SAVEPOINT original_session_admission").await?;tx.query_one("SELECT moria.native_serving_job_delegation($1)",&[&id]).await?;return Ok(());}
                };
                let metadata=Self::row(tx.client(),&session,id).await?;
                let marks=self.retained(&tx,&session,&metadata).await?;
                let snapshot=Authority::current(&*tx,&session).await?;
                tx.query_one("SELECT moria.runtime_binding_authority($1,$2,$3,$4)",&[&row.get::<_,Uuid>("binding"),&snapshot.security_epoch.parse::<i64>()?,&snapshot.instance_epoch.parse::<i64>()?,&id]).await?;
                let task_owner:Option<Uuid>=task.get("owner");let task_fence=if task_owner.is_some(){Some(task.get::<_,i64>("fence"))}else{None};
                tx.execute("INSERT INTO moria.runtime_function_epochs(execution,binding,task,owner,fence,incarnation,security_epoch,instance_epoch) SELECT $1,$2,$3,$4,$5,incarnation,$6,$7 FROM moria.indexing_instance ON CONFLICT(execution) DO UPDATE SET owner=EXCLUDED.owner,fence=EXCLUDED.fence,security_epoch=EXCLUDED.security_epoch,instance_epoch=EXCLUDED.instance_epoch", &[&id,&row.get::<_,Uuid>("binding"),&task.get::<_,Uuid>("id"),&task_owner,&task_fence,&snapshot.security_epoch.parse::<i64>()?,&snapshot.instance_epoch.parse::<i64>()?]).await?;
                tx.execute("INSERT INTO moria.runtime_function_epoch_events(execution,owner,fence,security_epoch,instance_epoch,reason) VALUES($1,$2,$3,$4,$5,$6)", &[&id,&task_owner,&task_fence,&snapshot.security_epoch.parse::<i64>()?,&snapshot.instance_epoch.parse::<i64>()?,&if publication{"publication"}else{"admission"}]).await?;
                tx.execute("UPDATE moria.runtime_function_invocations SET markings=$2 WHERE execution=$1", &[&id,&serde_json::to_value(&marks)?]).await?;
                Authority::finish(&*tx,&session,&snapshot).await?;
                Ok::<_,anyhow::Error>(())
            }.await;
            if result.is_ok() {
                tx.commit().await?;
                admission.release().await?;
                return Ok(Some(id));
            }
            tx.rollback().await?;
            admission.release().await?;
            if let Err(error) = result
                && crate::execution::transaction_conflict(&error)
            {
                return Err(error);
            }
            let mut client = self.db.get().await?;
            let failed = client.transaction().await?;
            let changed=failed.execute("UPDATE moria.runtime_executions SET status='failed',error='FUNCTION_AUTHORIZATION_CHANGED',finished_at=clock_timestamp() WHERE id=$1 AND status=$2 AND NOT EXISTS(SELECT 1 FROM moria.runtime_execution_tasks WHERE execution=$1 AND status='running')", &[&id,&if publication{"running"}else{"queued"}]).await?;
            if changed > 0 {
                failed.execute("UPDATE moria.runtime_execution_tasks SET status='cancelled',lease_until=NULL WHERE execution=$1 AND status='pending'", &[&id]).await?;
                failed.query_one("SELECT moria.runtime_execution_event($1,'failed',jsonb_build_object('error','FUNCTION_AUTHORIZATION_CHANGED'))", &[&id]).await?;
            }
            failed.commit().await?;
        }
        Ok(None)
    }
    async fn worker_once_cancel(
        &self,
        host: Uuid,
        owner: Uuid,
        cancel: watch::Receiver<bool>,
    ) -> Result<bool> {
        self.db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_function_worker_check($1,$2)",
                &[&owner, &host],
            )
            .await?;
        self.db
            .get()
            .await?
            .query_one("SELECT moria.runtime_execution_reap()", &[])
            .await?;
        let supervisor = SandboxSupervisor::new(self.db.clone(), host, self.policy.clone())?;
        supervisor.cleanup_abandoned().await?;
        if let Some(id) = self.refresh_candidate(owner, true, None).await?
            && let Some(claim) = supervisor.claim_publication(owner, Some(id)).await?
        {
            self.finalize(&supervisor, claim.lease).await?;
            return Ok(true);
        }
        let Some(id) = self.refresh_candidate(owner, false, None).await? else {
            return Ok(false);
        };
        let Some(claim) = supervisor.claim_for(owner, Some(id)).await? else {
            return Ok(false);
        };
        let id = claim.lease.execution;
        if supervisor
            .execute(claim.lease, cancel.clone())
            .await
            .is_ok()
            && !*cancel.borrow()
            && let Some(id) = self.refresh_candidate(owner, true, Some(id)).await?
            && let Some(claim) = supervisor.claim_publication(owner, Some(id)).await?
        {
            self.finalize(&supervisor, claim.lease).await?;
        }
        Ok(true)
    }
    async fn finalize(&self, supervisor: &SandboxSupervisor, lease: Lease) -> Result<()> {
        let remaining=tokio::time::timeout(Duration::from_secs(5),async {
            Ok::<_,anyhow::Error>(self.db.get().await?.query_opt("SELECT GREATEST(1,LEAST(30000,floor(extract(epoch FROM(LEAST(deadline,lease_until)-clock_timestamp()))*1000)::bigint)) FROM moria.runtime_executions WHERE id=$1 AND owner=$2 AND fence=$3 AND status='preparing'", &[&lease.execution,&lease.owner,&lease.fence.parse::<i64>()?]).await?.map(|row|row.get::<_,i64>(0)))
        }).await??;
        let Some(remaining) = remaining else {
            return Ok(());
        };
        let published = tokio::time::timeout(Duration::from_millis(remaining as u64), async {
            for retry in 0..4 {
                let result = self.publish(supervisor, lease.clone()).await;
                if result
                    .as_ref()
                    .is_err_and(crate::execution::transaction_conflict)
                    && retry < 3
                {
                    tokio::time::sleep(Duration::from_millis(10 << retry)).await;
                } else {
                    return result;
                }
            }
            unreachable!()
        })
        .await;
        if !matches!(published, Ok(Ok(()))) {
            let (cause, state) = match &published {
                Ok(Err(error)) => (
                    crate::database::safe_error(error),
                    error.chain().find_map(|cause| {
                        cause
                            .downcast_ref::<tokio_postgres::Error>()
                            .and_then(tokio_postgres::Error::code)
                            .map(|state| state.code().to_owned())
                    }),
                ),
                Err(_) => ("DEADLINE_EXCEEDED", None),
                Ok(Ok(())) => unreachable!(),
            };
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            let changed=tx.execute("UPDATE moria.runtime_executions SET status='failed',error='FUNCTION_PUBLICATION_FAILED',lease_until=NULL,finished_at=clock_timestamp() WHERE id=$1 AND status='preparing' AND owner=$2 AND fence=$3 AND NOT EXISTS(SELECT 1 FROM moria.runtime_function_receipts WHERE execution=$1)", &[&lease.execution,&lease.owner,&lease.fence.parse::<i64>()?]).await?;
            if changed > 0 {
                tx.query_one("SELECT moria.runtime_execution_event($1,'failed',$2)", &[&lease.execution,&json!({"error":"FUNCTION_PUBLICATION_FAILED","cause":cause,"sqlState":state})]).await?;
            }
            tx.commit().await?;
        }
        Ok(())
    }
    async fn publish(&self, supervisor: &SandboxSupervisor, lease: Lease) -> Result<()> {
        let context = supervisor.publication_context(lease.clone()).await?;
        let mut admission = Admission::begin(
            &self.db,
            context.environment(),
            context.effect() == "editProposal",
            None,
        )
        .await?;
        let tx = admission
            .client
            .as_mut()
            .unwrap()
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::Serializable)
            .start()
            .await?;
        ensure!(tx.query_opt("SELECT 1 FROM moria.runtime_executions WHERE id=$1 AND status='preparing' AND owner=$2 AND fence=$3 AND lease_until>clock_timestamp() FOR UPDATE", &[&lease.execution,&lease.owner,&lease.fence.parse::<i64>()?]).await?.is_some(),"FUNCTION_LEASE_INVALID");
        tx.query_one(
            "SELECT moria.runtime_execution_check($1)",
            &[&lease.execution],
        )
        .await?;
        let row=tx.query_one("SELECT t.result,f.outputs FROM moria.runtime_execution_tasks t JOIN moria.runtime_executions e ON e.id=t.execution JOIN moria.runtime_function_bindings f ON f.binding=e.binding WHERE e.id=$1 AND t.status='sealed'", &[&lease.execution]).await?;
        let result: Value = row.get("result");
        let outputs: Value = row.get("outputs");
        self.policy.publish(&tx, &context, &result).await?;
        let marks = context.consumed_markings();
        let hash = canonical::value_fingerprint(
            &result,
            &json!({"type":"object","properties":outputs}),
            &json!({}),
        )?;
        tx.execute("INSERT INTO moria.runtime_function_receipts(execution,result,result_hash,markings) VALUES($1,$2,$3,$4)", &[&lease.execution,&result,&hash.strip_prefix("sha256:").unwrap(),&serde_json::to_value(&marks)?]).await?;
        tx.execute(
            "UPDATE moria.runtime_function_invocations SET markings=$2 WHERE execution=$1",
            &[&lease.execution, &serde_json::to_value(&marks)?],
        )
        .await?;
        tx.query_one(
            "SELECT moria.runtime_execution_complete($1,$2,$3,$4)",
            &[
                &lease.execution,
                &lease.owner,
                &lease.fence.parse::<i64>()?,
                &json!({"valueVersion":1,"invocationId":lease.execution,"resultHash":hash}),
            ],
        )
        .await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(())
    }
}

pub async fn cli(db: Database, args: Vec<String>) -> Result<()> {
    ensure!(
        args.len() == 1 && ["worker", "once", "cleanup"].contains(&args[0].as_str()),
        "FUNCTION_COMMAND_INVALID"
    );
    let host: Uuid = env::var("MORIA_FUNCTION_HOST_ID")?.parse()?;
    let owner: Uuid = env::var("MORIA_FUNCTION_WORKER_OWNER")?.parse()?;
    db.get()
        .await?
        .query_one(
            "SELECT moria.runtime_function_worker_check($1,$2)",
            &[&owner, &host],
        )
        .await?;
    let policy = Arc::new(crate::platform::NativePlatformBroker::new(db.clone()));
    let functions = NativeFunctions::new(db.clone(), policy.clone());
    let (sender, cancel) = watch::channel(false);
    let shutdown = sender.clone();
    let signal = tokio::spawn(async move {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("Signal handler");
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}}
        let _ = sender.send(true);
    });
    if args[0] == "cleanup" {
        let count = SandboxSupervisor::new(db, host, policy)?
            .cleanup_abandoned()
            .await?;
        println!("{}", json!({"cleaned":count}));
        signal.abort();
        return Ok(());
    }
    if args[0] == "once" {
        let worked = functions.worker_once_cancel(host, owner, cancel).await?;
        println!("{}", json!({"worked":worked}));
        signal.abort();
        return Ok(());
    }
    let maximum: usize = env::var("MORIA_FUNCTION_WORKER_CONCURRENCY")
        .unwrap_or_else(|_| "4".into())
        .parse()?;
    ensure!((1..=32).contains(&maximum), "FUNCTION_WORKER_LIMIT_INVALID");
    println!(
        "{}",
        json!({"event":"function-worker","protocolVersion":1,"host":host,"owner":owner,"concurrency":maximum})
    );
    let mut tasks = tokio::task::JoinSet::new();
    let mut cancelled = cancel.clone();
    let mut failed = false;
    loop {
        if *cancelled.borrow() {
            break;
        }
        while tasks.len() < maximum {
            let functions = functions.clone();
            let cancel = cancel.clone();
            tasks.spawn(async move {
                let result = functions.worker_once_cancel(host, owner, cancel).await;
                if matches!(result, Ok(false)) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                result
            });
        }
        tokio::select! {
            _=cancelled.changed()=>break,
            result=tasks.join_next()=>if let Some(result)=result && !matches!(result,Ok(Ok(_))) {failed=true;let _=shutdown.send(true);break;}
        }
    }
    while let Some(result) = tasks.join_next().await {
        if !matches!(result, Ok(Ok(_))) {
            failed = true;
        }
    }
    signal.abort();
    ensure!(!failed, "FUNCTION_WORKER_FAILED");
    Ok(())
}
