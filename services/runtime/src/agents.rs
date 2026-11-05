use crate::{
    authority::{Authority, VerifiedSession},
    authorization::{DestinationPolicy, authorize, authorize_destination},
    canonical,
    database::Database,
    execution::ExecutionAuthority,
    functions::{Admission, FunctionPolicy},
    models::NativeModels,
    platform::NativePlatformBroker,
    sandbox::{
        ConsumedMarking, HttpCall, HttpResponse, PlatformBroker, SandboxContext, SandboxSupervisor,
    },
    values,
};
use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeSet, env, sync::Arc, time::Duration};
use tokio::sync::watch;
use tokio_postgres::{Client, Row, Transaction};
use uuid::Uuid;

fn publication_aborted(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<tokio_postgres::Error>()
            .and_then(tokio_postgres::Error::as_db_error)
            .is_some_and(|error| {
                error.code() == &tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE
                    || error.code() == &tokio_postgres::error::SqlState::T_R_DEADLOCK_DETECTED
            })
    })
}

fn marking_values(value: &Value) -> Result<Vec<ConsumedMarking>> {
    value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("AGENT_MARKINGS_INVALID"))?
        .iter()
        .map(|label| {
            Ok(ConsumedMarking {
                environment: label["environment"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("AGENT_MARKINGS_INVALID"))?
                    .into(),
                marking: label["marking"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("AGENT_MARKINGS_INVALID"))?
                    .into(),
            })
        })
        .collect()
}

async fn source_labels(
    client: &Client,
    session: &VerifiedSession,
    id: Uuid,
) -> Result<Vec<ConsumedMarking>> {
    ensure!(client.query_opt("SELECT 1 FROM moria.fs_entries e JOIN moria.fs_resource_bindings b ON b.entry_id=e.id AND b.active JOIN moria.fs_roles r ON r.entry_id=b.root_id AND NOT r.retired WHERE e.resource_id=$1 AND e.environment_id=$2 AND NOT e.deleted",&[&id,&session.environment()]).await?.is_some(),"NOT_FOUND");
    Ok(
        Authority::resource_access_for_operation(client, session, &id.to_string(), "invoke")
            .await?
            .into_iter()
            .map(|marking| ConsumedMarking {
                environment: session.environment().into(),
                marking,
            })
            .collect(),
    )
}

pub(crate) async fn current_labels(
    client: &Client,
    session: &VerifiedSession,
    execution: Uuid,
) -> Result<Vec<ConsumedMarking>> {
    let row=client.query_one("SELECT e.resource,e.binding,EXISTS(SELECT 1 FROM moria.runtime_execution_services s WHERE s.binding=e.binding) AS service_origin,a.adapter_id,a.application_id,a.model_resource,a.model_binding,a.model_secret_approval,a.markings FROM moria.runtime_executions e JOIN moria.runtime_agent_executions a ON a.execution=e.id WHERE e.id=$1 AND e.spec->>'domain'='agent' AND e.environment=$2 AND e.principal=$3 AND e.application=$4",&[&execution,&session.environment(),&session.principal(),&session.application()]).await?;
    if row.get::<_, bool>("service_origin") {
        client
            .query_one(
                "SELECT moria.runtime_service_binding_guard($1)",
                &[&row.get::<_, Uuid>("binding")],
            )
            .await?;
    }
    ensure!(
        row.get::<_, Uuid>("application_id").to_string() == session.application(),
        "AGENT_APPLICATION_MISMATCH"
    );
    let mut labels = marking_values(&row.get::<_, Value>("markings"))?;
    labels.extend(source_labels(client, session, row.get("resource")).await?);
    let model_resource: String = row.get("model_resource");
    let model=client.query_opt("SELECT definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&model_resource]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
    let definition: Value = model.get(0);
    let admission = NativeModels::admission_in(client, session, &model_resource).await?;
    ensure!(
        admission
            .secret_approval
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?
            == row.get::<_, Option<Value>>("model_secret_approval"),
        "SECRET_APPROVAL_REVOKED"
    );
    labels.extend(
        admission
            .markings
            .into_iter()
            .map(|marking| ConsumedMarking {
                environment: session.environment().into(),
                marking,
            }),
    );
    // A changed route fails closed; the accepted graph cannot silently use a new model.
    ensure!(
        canonical::fingerprint(&definition)?
            == canonical::fingerprint(&row.get::<_, Value>("model_binding"))?,
        "AGENT_MODEL_CHANGED"
    );
    let labels: BTreeSet<_> = labels.into_iter().collect();
    ensure!(
        labels.len() <= 256
            && labels
                .iter()
                .all(|label| label.environment == session.environment()),
        "AGENT_MARKINGS_INVALID"
    );
    let snapshot = Authority::current(client, session).await?;
    crate::execution::invocation_permission(
        &snapshot.identity,
        row.get("resource"),
        "agents",
        "invoke",
        &labels
            .iter()
            .map(|label| label.marking.clone())
            .collect::<Vec<_>>(),
    )?;
    authorize(
        &snapshot.identity,
        &model_resource,
        "invoke",
        &labels
            .iter()
            .map(|label| label.marking.clone())
            .collect::<Vec<_>>(),
    )?;
    let destination = definition["destination"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("MODEL_BINDING_INVALID"))?;
    let policy: DestinationPolicy =
        serde_json::from_value(definition["destinationPolicy"].clone())?;
    authorize_destination(
        &snapshot.identity,
        &labels.iter().map(|label| label.marking.clone()).collect(),
        Some(&policy),
        destination,
    )?;
    Ok(labels.into_iter().collect())
}

pub(crate) async fn check_context(client: &Client, context: &SandboxContext) -> Result<()> {
    ensure!(context.domain() == "agent", "AGENT_CONTEXT_INVALID");
    let hash = context
        .session_hash()
        .ok_or_else(|| anyhow::anyhow!("UNAUTHENTICATED"))?;
    let session = Authority::stored_session_in(
        client,
        hash,
        context.environment(),
        context.principal(),
        context.application(),
    )
    .await?;
    let labels = current_labels(client, &session, context.lease().execution).await?;
    context.record_consumed_markings(&labels)?;
    let resource: Uuid = client
        .query_one(
            "SELECT resource FROM moria.runtime_executions WHERE id=$1",
            &[&context.lease().execution],
        )
        .await?
        .get(0);
    context.record_consumed_markings(
        &crate::sandbox::released_source_markings_in(
            client,
            &session,
            resource,
            "invoke",
            context
                .source_release()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_FILE_PROOF_REQUIRED"))?,
        )
        .await?,
    )?;
    let snapshot = Authority::current(client, &session).await?;
    crate::execution::invocation_permission(
        &snapshot.identity,
        resource,
        "agents",
        "invoke",
        &context
            .consumed_markings()
            .into_iter()
            .map(|label| label.marking)
            .collect::<Vec<_>>(),
    )?;
    Ok(())
}

pub(crate) async fn adapter_image(client: &Client, execution: Uuid) -> Result<String> {
    let definition: Value = client
        .query_one(
            "SELECT adapter_definition FROM moria.runtime_agent_executions WHERE execution=$1",
            &[&execution],
        )
        .await?
        .get(0);
    let artifact = &definition["spec"]["artifact"];
    let uri = artifact["uri"]
        .as_str()
        .filter(|uri| uri.starts_with("oci://") && uri.len() <= 256)
        .ok_or_else(|| anyhow::anyhow!("AGENT_ADAPTER_INVALID"))?;
    let digest = artifact["digest"]
        .as_str()
        .filter(|digest| {
            digest.len() == 71
                && digest.starts_with("sha256:")
                && digest[7..]
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        .ok_or_else(|| anyhow::anyhow!("AGENT_ADAPTER_INVALID"))?;
    Ok(format!("{}@{digest}", &uri[6..]))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    uri: String,
    digest: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Limits {
    timeout_seconds: u32,
    #[serde(rename = "memoryMiB")]
    memory_mib: u32,
    cpu_millis: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentSpec {
    artifact: Artifact,
    adapter: String,
    application: String,
    model: String,
    inputs: Value,
    outputs: Value,
    limits: Limits,
}
fn agent_spec(definition: &Value) -> Result<AgentSpec> {
    ensure!(
        definition["apiVersion"] == "moria/v1alpha2" && definition["kind"] == "Agent",
        "AGENT_INVALID"
    );
    let spec: AgentSpec = serde_json::from_value(definition["spec"].clone())
        .map_err(|_| anyhow::anyhow!("AGENT_INVALID"))?;
    ensure!(
        (1..=600).contains(&spec.limits.timeout_seconds)
            && (32..=1024).contains(&spec.limits.memory_mib)
            && (1..=600000).contains(&spec.limits.cpu_millis),
        "AGENT_LIMIT_INVALID"
    );
    ensure!(
        spec.artifact.uri.starts_with("oci://")
            && spec.artifact.uri.len() <= 256
            && spec.artifact.digest.len() == 71
            && spec.artifact.digest.starts_with("sha256:")
            && spec.artifact.digest[7..]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "AGENT_ARTIFACT_INVALID"
    );
    for fields in [&spec.inputs, &spec.outputs] {
        ensure!(fields.is_object(), "AGENT_INVALID");
        values::descriptor(&json!({"type":"object","properties":fields}), &json!({}), 0)?;
    }
    Ok(spec)
}
fn normalize(input: &Value, fields: &Value) -> Result<(Value, String)> {
    ensure!(
        canonical::canonical_json(input)?.len() <= 32768,
        "AGENT_INPUT_INVALID"
    );
    let descriptor =
        values::descriptor(&json!({"type":"object","properties":fields}), &json!({}), 0)?;
    let mut input = input.clone();
    let object = input
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("AGENT_INPUT_INVALID"))?;
    for (name, property) in descriptor["properties"].as_object().unwrap() {
        if !object.contains_key(name) && property["nullable"] == true {
            object.insert(name.clone(), Value::Null);
        }
    }
    let input = values::normalize(&input, &descriptor)?;
    ensure!(
        canonical::canonical_json(&input)?.len() <= 32768,
        "AGENT_INPUT_INVALID"
    );
    let hash = canonical::value_fingerprint(&input, &descriptor, &json!({}))?;
    Ok((input, hash.strip_prefix("sha256:").unwrap().into()))
}

pub(crate) fn source_profile(definition: &Value, adapter: &Value) -> Result<Value> {
    agent_spec(definition)?;
    ensure!(
        adapter["kind"] == "AgentAdapter"
            && adapter["spec"]["abiVersion"] == "agent/v1"
            && adapter["spec"]["capabilities"]
                .as_array()
                .is_some_and(|caps| caps.contains(&json!("events"))
                    && caps.contains(&json!("cancel"))
                    && caps.iter().all(|cap| cap == "events" || cap == "cancel")),
        "AGENT_ADAPTER_UNSUPPORTED"
    );
    crate::sandbox::artifact_image(&definition["spec"]["artifact"])?;
    crate::sandbox::artifact_image(&adapter["spec"]["artifact"])?;
    Ok(
        json!({"domain":"agent","artifact":definition["spec"]["artifact"],"adapter":adapter["spec"]["artifact"],"limits":definition["spec"]["limits"]}),
    )
}
fn logical_key(key: &str) -> Result<()> {
    ensure!(
        !key.is_empty()
            && key.len() <= 128
            && key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.:/-".contains(&b)),
        "AGENT_KEY_INVALID"
    );
    Ok(())
}
fn union(groups: &[Vec<ConsumedMarking>], environment: &str) -> Result<Vec<ConsumedMarking>> {
    let labels: BTreeSet<_> = groups.iter().flatten().cloned().collect();
    ensure!(
        labels.len() <= 256
            && labels.iter().all(|label| label.environment == environment
                && !label.marking.is_empty()
                && label.marking.len() <= 180),
        "AGENT_MARKINGS_INVALID"
    );
    Ok(labels.into_iter().collect())
}
async fn source(client: &Client, session: &VerifiedSession, resource: Uuid) -> Result<Row> {
    client.query_opt("SELECT b.entry_id,b.digest,b.reference,a.resources FROM moria.fs_entries e JOIN moria.fs_resource_bindings b ON b.entry_id=e.id AND b.active JOIN moria.fs_roles role ON role.entry_id=b.root_id AND NOT role.retired JOIN moria.fs_active_releases a ON a.root_id=b.root_id AND a.environment=b.environment AND a.digest=b.digest WHERE e.resource_id=$1 AND e.environment_id=$2 AND e.kind='resource' AND NOT e.deleted",&[&resource,&session.environment()]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))
}
fn declaration(row: &Row) -> Result<Value> {
    let resources: Value = row.get("resources");
    let reference: String = row.get("reference");
    resources
        .as_array()
        .and_then(|resources| {
            resources.iter().find(|resource| {
                format!(
                    "{}/{}/{}",
                    resource["metadata"]["namespace"].as_str().unwrap_or(""),
                    resource["kind"].as_str().unwrap_or(""),
                    resource["metadata"]["name"].as_str().unwrap_or("")
                ) == reference
            })
        })
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("AGENT_SOURCE_INVALID"))
}
async fn referenced(
    client: &Client,
    session: &VerifiedSession,
    release: &str,
    reference: &str,
    kind: &str,
) -> Result<(Uuid, Value)> {
    ensure!(
        reference.split('/').nth(1) == Some(kind),
        "AGENT_REFERENCE_INVALID"
    );
    let id: Uuid = crate::filesystem::NativeSources::resource_in(client, release, reference)
        .await?
        .parse()?;
    let definition = declaration(&source(client, session, id).await?)?;
    ensure!(definition["kind"] == kind, "AGENT_REFERENCE_INVALID");
    Ok((id, definition))
}

#[derive(Clone)]
pub struct NativeAgents {
    db: Database,
    platform: Arc<NativePlatformBroker>,
    models: NativeModels,
}
impl NativeAgents {
    pub fn new(db: Database) -> Result<Self> {
        Ok(Self {
            platform: Arc::new(NativePlatformBroker::new(db.clone())),
            models: NativeModels::new(db.clone())?,
            db,
        })
    }
    /// Metadata checks for an approved Agent purpose; no session or provider call.
    pub(crate) async fn registered_model_admission_in(
        client: &Client,
        service: &crate::jobs::RegisteredService,
    ) -> Result<crate::models::ModelAdmission> {
        ensure!(service.domain() == "agent", "SERVICE_PURPOSE_REVOKED");
        let (_, release, definition) = crate::filesystem::NativeSources::active_definition_in(
            client,
            service.environment(),
            service.target(),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("AGENT_SOURCE_INVALID"))?;
        let spec = agent_spec(&definition)?;
        let model =
            crate::filesystem::NativeSources::resource_in(client, &release, &spec.model).await?;
        let mut admission = NativeModels::registered_admission_in(client, service, &model).await?;
        admission.markings.extend(
            service
                .resource_labels_in(client, &service.target().to_string(), "invoke")
                .await?,
        );
        admission.markings.extend(
            crate::filesystem::NativeSources::registered_declaration_labels_in(
                client,
                service,
                service.target(),
                &release,
            )
            .await?,
        );
        let state = service.state_in(client).await?;
        authorize(
            &state.identity,
            &model,
            "invoke",
            &admission.markings.iter().cloned().collect::<Vec<_>>(),
        )?;
        let binding: Value = client.query_one("SELECT definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled", &[&service.environment(),&model]).await?.get(0);
        authorize_destination(
            &state.identity,
            &admission.markings,
            Some(&serde_json::from_value(
                binding["destinationPolicy"].clone(),
            )?),
            binding["destination"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("MODEL_BINDING_INVALID"))?,
        )?;
        Ok(admission)
    }
    pub(crate) async fn pin_in(
        client: &Client,
        session: &VerifiedSession,
        resource: Uuid,
    ) -> Result<Value> {
        let row = source(client, session, resource).await?;
        let definition = declaration(&row)?;
        let spec = agent_spec(&definition)?;
        let release: String = row.get("digest");
        let (adapter_id, adapter) =
            referenced(client, session, &release, &spec.adapter, "AgentAdapter").await?;
        ensure!(
            adapter["spec"]["abiVersion"] == "agent/v1"
                && adapter["spec"]["capabilities"]
                    .as_array()
                    .is_some_and(|caps| caps.contains(&json!("events"))
                        && caps.contains(&json!("cancel"))
                        && caps.iter().all(|cap| cap == "events" || cap == "cancel")),
            "AGENT_ADAPTER_UNSUPPORTED"
        );
        let (application_id, _) =
            referenced(client, session, &release, &spec.application, "Application").await?;
        ensure!(
            session.application() == application_id.to_string(),
            "AGENT_APPLICATION_MISMATCH"
        );
        let (model_id, _) = referenced(client, session, &release, &spec.model, "Model").await?;
        let model_resource = model_id.to_string();
        let model_binding:Value=client.query_opt("SELECT definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&model_resource]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?.get(0);
        let model_admission = NativeModels::admission_in(client, session, &model_resource).await?;
        let mut labels = Vec::new();
        labels.extend(source_labels(client, session, resource).await?);
        labels.extend(
            model_admission
                .markings
                .iter()
                .map(|marking| ConsumedMarking {
                    environment: session.environment().into(),
                    marking: marking.clone(),
                }),
        );
        let labels = union(&[labels], session.environment())?;
        let state = Authority::current(client, session).await?;
        let markings = labels
            .iter()
            .map(|label| label.marking.clone())
            .collect::<Vec<_>>();
        crate::execution::invocation_permission(
            &state.identity,
            resource,
            "agents",
            "invoke",
            &markings,
        )?;
        authorize(&state.identity, &model_resource, "invoke", &markings)?;
        authorize_destination(
            &state.identity,
            &markings.iter().cloned().collect(),
            Some(&serde_json::from_value(
                model_binding["destinationPolicy"].clone(),
            )?),
            model_binding["destination"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("MODEL_BINDING_INVALID"))?,
        )?;
        Ok(
            json!({"resource":resource,"entryId":row.get::<_,Uuid>("entry_id"),"release":row.get::<_,String>("digest"),"definition":definition,"adapterId":adapter_id,"adapterDefinition":adapter,"applicationId":application_id,"modelResource":model_resource,"modelBinding":model_binding,"modelSecretApproval":model_admission.secret_approval,"modelSecretOperation":model_admission.secret_operation,"markings":labels}),
        )
    }
    pub(crate) fn normalize_pinned(pin: &Value, input: &Value) -> Result<(Value, String)> {
        normalize(input, &agent_spec(&pin["definition"])?.inputs)
    }
    /// Only a live accepted evaluation case can select its persisted Agent pin.
    pub(crate) async fn invoke_evaluation_case(
        &self,
        session: &VerifiedSession,
        case: Uuid,
    ) -> Result<Uuid> {
        let client = self.db.get().await?;
        let existing: Option<Uuid> = client
            .query_one(
                "SELECT moria.runtime_evaluation_agent_resume($1,$2)",
                &[&session.session_hash(), &case],
            )
            .await?
            .get(0);
        if let Some(child) = existing {
            drop(client);
            self.evaluation_progress(session, case, child).await?;
            return Ok(child);
        }
        let row=client.query_opt("SELECT r.agent_resource,c.input FROM moria.runtime_evaluation_cases c JOIN moria.runtime_evaluations r ON r.execution=c.evaluation JOIN moria.runtime_executions e ON e.id=c.execution JOIN moria.runtime_job_sessions s ON s.execution=e.id AND s.token_hash=$2 WHERE c.execution=$1 AND e.environment=$3 AND e.principal=$4 AND e.application=$5 AND e.status='running'",&[&case,&session.session_hash(),&session.environment(),&session.principal(),&session.application()]).await?.ok_or_else(||anyhow::anyhow!("EVALUATION_CASE_AUTHORITY"))?;
        let resource = row.get(0);
        let input: Value = row.get(1);
        drop(client);
        let accepted = self
            .invoke_inner(
                session,
                resource,
                &format!("evaluation/{case}/agent"),
                json!({"input":input,"wait":false}),
                Some(case),
            )
            .await?;
        accepted["id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("EVALUATION_AGENT_INVALID"))?
            .parse()
            .map_err(Into::into)
    }
    async fn row(client: &Client, session: &VerifiedSession, id: Uuid) -> Result<Row> {
        client.query_opt("SELECT e.*,a.definition,a.markings AS agent_markings,a.attempt_id,a.correlation_id,a.release,a.model_resource,a.model_binding,r.result AS agent_result,(e.finished_at IS NOT NULL AND e.finished_at<=clock_timestamp()-interval '30 days') AS expired FROM moria.runtime_executions e JOIN moria.runtime_agent_executions a ON a.execution=e.id LEFT JOIN moria.runtime_agent_receipts r ON r.execution=e.id WHERE e.id=$1 AND e.environment=$2 AND e.principal=$3 AND e.application=$4",&[&id,&session.environment(),&session.principal(),&session.application()]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))
    }
    async fn retained(
        &self,
        tx: &Transaction<'_>,
        session: &VerifiedSession,
        row: &Row,
    ) -> Result<Vec<ConsumedMarking>> {
        ensure!(!row.get::<_, bool>("expired"), "RESULT_EXPIRED");
        let execution: Uuid = row.get("id");
        let mut labels = current_labels(tx.client(), session, execution).await?;
        labels
            .extend(crate::evaluations::retained_input_in(tx.client(), session, execution).await?);
        labels.extend(crate::automations::retained_in(tx.client(), session, execution).await?);
        crate::sandbox::require_clean_observations_in(tx.client(), execution).await?;
        labels.extend(crate::sandbox::retained_observations_in(tx.client(), execution).await?);
        labels.extend(
            crate::sandbox::released_source_markings_in(
                tx.client(),
                session,
                row.get("resource"),
                "invoke",
                &row.get::<_, String>("release"),
            )
            .await?,
        );
        crate::execution::invocation_permission(
            &Authority::current(tx, session).await?.identity,
            row.get("resource"),
            "agents",
            "invoke",
            &labels
                .iter()
                .map(|label| label.marking.clone())
                .collect::<Vec<_>>(),
        )?;
        labels.extend(self.platform.retained(tx, session, execution).await?);
        let calls=tx.query("SELECT model_resource,model_key,markings,status FROM moria.runtime_agent_model_calls WHERE execution=$1 ORDER BY logical_id LIMIT 33",&[&execution]).await?;
        ensure!(calls.len() <= 32, "AGENT_CALL_LIMIT");
        for call in calls {
            labels.extend(marking_values(&call.get::<_, Value>(2))?);
            let resource: String = call.get(0);
            let key: String = call.get(1);
            let exists=tx.query_opt("SELECT 1 FROM moria.native_model_calls WHERE environment=$1 AND principal=$2 AND application=$3 AND resource=$4 AND call_key=$5",&[&session.environment(),&session.principal(),&session.application(),&resource,&key]).await?.is_some();
            if exists {
                labels.extend(
                    self.models
                        .validate_retained_in(tx.client(), session, &resource, &key)
                        .await?,
                );
            } else {
                // The protected intent commits before gateway admission. Its pending or rejected
                // structural status contains no provider result; current source/model/destination
                // authority and complete retained labels have already been checked above.
                ensure!(
                    call.get::<_, i32>(3) != 200
                        && !(row.get::<_, String>("status") == "succeeded"
                            && call.get::<_, i32>(3) == 102),
                    "AGENT_MODEL_RECEIPT_REQUIRED"
                );
            }
        }
        let labels = union(&[labels], session.environment())?;
        let snapshot = Authority::current(tx, session).await?;
        crate::execution::invocation_permission(
            &snapshot.identity,
            row.get("resource"),
            "agents",
            "invoke",
            &labels
                .iter()
                .map(|label| label.marking.clone())
                .collect::<Vec<_>>(),
        )?;
        let binding: Value = row.get("model_binding");
        let model_resource: String = row.get("model_resource");
        let names = labels
            .iter()
            .map(|label| label.marking.clone())
            .collect::<Vec<_>>();
        authorize(&snapshot.identity, &model_resource, "invoke", &names)?;
        let policy: DestinationPolicy =
            serde_json::from_value(binding["destinationPolicy"].clone())?;
        authorize_destination(
            &snapshot.identity,
            &names.into_iter().collect(),
            Some(&policy),
            binding["destination"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("MODEL_BINDING_INVALID"))?,
        )?;
        Ok(labels)
    }
    pub(crate) async fn retained_in(
        &self,
        tx: &Transaction<'_>,
        session: &VerifiedSession,
        id: Uuid,
    ) -> Result<Vec<ConsumedMarking>> {
        let row = Self::row(tx.client(), session, id).await?;
        self.retained(tx, session, &row).await
    }
    /// A controller can wait for its exact child without inspecting an unfinished
    /// callback. Terminal data still passes the ordinary retained release gate.
    pub(crate) async fn evaluation_progress(
        &self,
        session: &VerifiedSession,
        case: Uuid,
        child: Uuid,
    ) -> Result<Option<Value>> {
        self.db
            .get()
            .await?
            .query_one("SELECT moria.runtime_execution_reap($1)", &[&child])
            .await?;
        for retry in 0..4 {
            let result = self.evaluation_progress_once(session, case, child).await;
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
    async fn evaluation_progress_once(
        &self,
        session: &VerifiedSession,
        case: Uuid,
        child: Uuid,
    ) -> Result<Option<Value>> {
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.read_transaction().await?;
        let state = Authority::current(&*tx, session).await?;
        ensure!(tx.query_opt("SELECT 1 FROM moria.runtime_job_sessions s JOIN moria.runtime_evaluation_cases c ON c.execution=s.execution AND c.agent_execution=$3 JOIN moria.runtime_executions p ON p.id=c.execution JOIN moria.runtime_executions e ON e.id=c.agent_execution JOIN moria.runtime_job_delegations d ON d.execution=e.id AND d.parent_execution=c.execution WHERE s.token_hash=$1 AND s.execution=$2 AND p.status='running' AND ROW(e.environment,e.principal,e.application)=ROW(p.environment,p.principal,p.application) AND e.spec->>'domain'='agent'",&[&session.session_hash(),&case,&child]).await?.is_some(),"EVALUATION_CASE_AUTHORITY");
        let mut marks = current_labels(tx.client(), session, child).await?;
        marks.extend(crate::evaluations::retained_input_in(tx.client(), session, child).await?);
        let row = Self::row(tx.client(), session, child).await?;
        marks.extend(
            crate::sandbox::released_source_markings_in(
                tx.client(),
                session,
                row.get("resource"),
                "invoke",
                &row.get::<_, String>("release"),
            )
            .await?,
        );
        crate::execution::invocation_permission(
            &state.identity,
            row.get("resource"),
            "agents",
            "invoke",
            &marks
                .iter()
                .map(|mark| mark.marking.clone())
                .collect::<Vec<_>>(),
        )?;
        let pending =
            ["queued", "running", "preparing"].contains(&row.get::<_, String>("status").as_str());
        Authority::finish(&*tx, session, &state).await?;
        tx.commit().await?;
        admission.release().await?;
        if pending {
            Ok(None)
        } else {
            self.get(session, child).await.map(Some)
        }
    }
    async fn public(client: &Client, row: &Row, labels: Vec<ConsumedMarking>) -> Result<Value> {
        let status: String = row.get("status");
        let id: Uuid = row.get("id");
        let mut result = json!({"valueVersion":1,"id":id,"resourceId":row.get::<_,Uuid>("resource"),"status":if status=="preparing"{"running"}else{status.as_str()},"release":row.get::<_,String>("release"),"attemptId":row.get::<_,Uuid>("attempt_id"),"correlationId":row.get::<_,Uuid>("correlation_id"),"markings":labels});
        if status == "succeeded" {
            result["result"] = row
                .get::<_, Option<Value>>("agent_result")
                .ok_or_else(|| anyhow::anyhow!("AGENT_RECEIPT_MISSING"))?;
        }
        if status == "failed" {
            let error = row
                .get::<_, Option<String>>("error")
                .unwrap_or_else(|| "AGENT_FAILED".into());
            result["error"] = json!({"code":error,"message":"Agent execution failed"});
        }
        let calls=client.query("SELECT logical_id,status,usage,error FROM moria.runtime_agent_model_calls WHERE execution=$1 ORDER BY created_at,logical_id LIMIT 32",&[&id]).await?;
        result["modelCalls"]=json!(calls.into_iter().map(|call|json!({"id":call.get::<_,String>(0),"status":call.get::<_,i32>(1),"usage":call.get::<_,Option<Value>>(2),"error":call.get::<_,Option<String>>(3)})).collect::<Vec<_>>());
        Ok(result)
    }
    pub async fn invoke(
        &self,
        session: &VerifiedSession,
        resource: Uuid,
        key: &str,
        payload: Value,
    ) -> Result<Value> {
        self.invoke_inner(session, resource, key, payload, None)
            .await
    }
    async fn invoke_inner(
        &self,
        session: &VerifiedSession,
        resource: Uuid,
        key: &str,
        payload: Value,
        evaluation_case: Option<Uuid>,
    ) -> Result<Value> {
        for retry in 0..4 {
            let result = self
                .invoke_inner_once(session, resource, key, payload.clone(), evaluation_case)
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
    async fn invoke_inner_once(
        &self,
        session: &VerifiedSession,
        resource: Uuid,
        key: &str,
        payload: Value,
        evaluation_case: Option<Uuid>,
    ) -> Result<Value> {
        logical_key(key)?;
        ensure!(
            payload.as_object().is_some_and(|object| object
                .keys()
                .all(|key| ["input", "wait"].contains(&key.as_str())))
                && payload.get("wait").is_none_or(Value::is_boolean),
            "AGENT_REQUEST_INVALID"
        );
        let lock = format!(
            "moria/agent/request/{}",
            canonical::fingerprint(&json!([
                session.environment(),
                resource,
                session.principal(),
                session.application(),
                key
            ]))?
        );
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
        let source = source(tx.client(), session, resource).await?;
        let source_release: String = source.get("digest");
        let source_marks = source_labels(tx.client(), session, resource).await?;
        let snapshot = Authority::current(&*tx, session).await?;
        crate::execution::invocation_permission(
            &snapshot.identity,
            resource,
            "agents",
            "invoke",
            &source_marks
                .iter()
                .map(|label| label.marking.clone())
                .collect::<Vec<_>>(),
        )?;
        if let Some(existing)=tx.query_opt("SELECT e.id,a.definition FROM moria.runtime_executions e JOIN moria.runtime_agent_executions a ON a.execution=e.id WHERE e.environment=$1 AND e.resource=$2 AND e.principal=$3 AND e.application=$4 AND e.request_key=$5",&[&session.environment(),&resource,&session.principal(),&session.application(),&key]).await? {
            let resolved=agent_spec(&existing.get::<_,Value>(1))?;let(_,hash)=normalize(&payload["input"],&resolved.inputs)?;
            let row=Self::row(tx.client(),session,existing.get(0)).await?;
            ensure!(row.get::<_,String>("input_hash")==hash,"IDEMPOTENCY_CONFLICT");
            let labels=self.retained(&tx,session,&row).await?;let result=Self::public(tx.client(),&row,labels).await?;
            Authority::finish(&*tx,session,&snapshot).await?;tx.commit().await?;admission.release().await?;return Ok(result);
        }
        let pinned = if let Some(case) = evaluation_case {
            let row=tx.query_opt("SELECT r.agent_pin,e.deadline,c.markings FROM moria.runtime_evaluation_cases c JOIN moria.runtime_evaluations r ON r.execution=c.evaluation JOIN moria.runtime_executions e ON e.id=c.execution JOIN moria.runtime_job_sessions s ON s.execution=e.id AND s.token_hash=$2 WHERE c.execution=$1 AND r.agent_resource=$3 AND e.environment=$4 AND e.principal=$5 AND e.application=$6 AND e.status='running'",&[&case,&session.session_hash(),&resource,&session.environment(),&session.principal(),&session.application()]).await?.ok_or_else(||anyhow::anyhow!("EVALUATION_CASE_AUTHORITY"))?;
            let mut pin = row.get::<_, Value>(0);
            pin["markings"] = json!(union(
                &[
                    marking_values(&pin["markings"])?,
                    marking_values(&row.get::<_, Value>("markings"))?
                ],
                session.environment()
            )?);
            Some(pin)
        } else {
            None
        };
        let definition = if let Some(pin) = &pinned {
            pin["definition"].clone()
        } else {
            declaration(&source)?
        };
        let spec = agent_spec(&definition)?;
        let (normalized, hash) = normalize(&payload["input"], &spec.inputs)?;
        let (adapter_id, adapter) = if let Some(pin) = &pinned {
            (
                pin["adapterId"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("EVALUATION_PIN_INVALID"))?
                    .parse()?,
                pin["adapterDefinition"].clone(),
            )
        } else {
            referenced(
                tx.client(),
                session,
                &source_release,
                &spec.adapter,
                "AgentAdapter",
            )
            .await?
        };
        ensure!(
            adapter["spec"]["abiVersion"] == "agent/v1"
                && adapter["spec"]["capabilities"]
                    .as_array()
                    .is_some_and(|capabilities| capabilities.contains(&json!("events"))
                        && capabilities.contains(&json!("cancel"))
                        && capabilities
                            .iter()
                            .all(|capability| capability == "events" || capability == "cancel")),
            "AGENT_ADAPTER_UNSUPPORTED"
        );
        let application_id: Uuid = if let Some(pin) = &pinned {
            pin["applicationId"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("EVALUATION_PIN_INVALID"))?
                .parse()?
        } else {
            referenced(
                tx.client(),
                session,
                &source_release,
                &spec.application,
                "Application",
            )
            .await?
            .0
        };
        ensure!(
            session.application() == application_id.to_string(),
            "AGENT_APPLICATION_MISMATCH"
        );
        let model_id: Uuid = if let Some(pin) = &pinned {
            pin["modelResource"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("EVALUATION_PIN_INVALID"))?
                .parse()?
        } else {
            referenced(tx.client(), session, &source_release, &spec.model, "Model")
                .await?
                .0
        };
        let model_resource = model_id.to_string();
        let current_model:Value=tx.query_opt("SELECT definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&model_resource]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?.get(0);
        let model_binding = if let Some(pin) = &pinned {
            ensure!(
                canonical::fingerprint(&pin["modelBinding"])?
                    == canonical::fingerprint(&current_model)?,
                "AGENT_MODEL_CHANGED"
            );
            pin["modelBinding"].clone()
        } else {
            current_model
        };
        let model_admission =
            NativeModels::admission_in(tx.client(), session, &model_resource).await?;
        let model_secret_approval = model_admission
            .secret_approval
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?;
        if let Some(pin) = &pinned {
            ensure!(
                model_secret_approval.as_ref().unwrap_or(&Value::Null)
                    == &pin["modelSecretApproval"],
                "SECRET_APPROVAL_REVOKED"
            );
        }
        let mut labels = source_marks;
        if let Some(pin) = &pinned {
            labels.extend(marking_values(&pin["markings"])?);
        }
        labels.extend(
            model_admission
                .markings
                .iter()
                .map(|marking| ConsumedMarking {
                    environment: session.environment().into(),
                    marking: marking.clone(),
                }),
        );
        let labels = union(&[labels], session.environment())?;
        crate::execution::invocation_permission(
            &snapshot.identity,
            resource,
            "agents",
            "invoke",
            &labels
                .iter()
                .map(|label| label.marking.clone())
                .collect::<Vec<_>>(),
        )?;
        let release: String = if let Some(pin) = &pinned {
            pin["release"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("EVALUATION_PIN_INVALID"))?
                .into()
        } else {
            source.get("digest")
        };
        let spec_json = json!({"version":1,"domain":"agent","release":release.strip_prefix("sha256:").ok_or_else(||anyhow::anyhow!("AGENT_SOURCE_INVALID"))?,"resourceClass":"standard","timeoutSeconds":spec.limits.timeout_seconds,"retryPolicy":"never","inputSchema":{"type":"object","properties":spec.inputs},"pinnedInputs":[],"expectedHeads":{}});
        let mut context = json!({"environment":session.environment(),"principal":session.principal(),"application":session.application(),"sessionHash":session.session_hash()});
        if let Some(case) = evaluation_case {
            let deadline: String = tx
                .query_one(
                    "SELECT deadline::text FROM moria.runtime_executions WHERE id=$1",
                    &[&case],
                )
                .await?
                .get(0);
            context["parentExecution"] = json!(case);
            context["parentDeadline"] = json!(deadline);
        }
        let operation = crate::execution::invocation_permission(
            &snapshot.identity,
            resource,
            "agents",
            "invoke",
            &labels
                .iter()
                .map(|label| label.marking.clone())
                .collect::<Vec<_>>(),
        )?;
        let mut operations = vec![operation.clone(), format!("{model_resource}:invoke")];
        operations.extend(model_admission.secret_operation);
        let consumed = labels
            .iter()
            .map(|label| label.marking.clone())
            .collect::<Vec<_>>();
        let dispatch = if evaluation_case.is_some() {
            crate::evaluations::child_binding_in(
                tx.client(),
                session,
                resource,
                &spec_json,
                &operations,
                &consumed,
            )
            .await?
        } else {
            crate::automations::dispatch_binding_in(
                tx.client(),
                session,
                resource,
                &spec_json,
                &operations,
                &consumed,
            )
            .await?
        };
        let (binding, labels) = if let Some((binding, marks)) = dispatch {
            (
                binding.id,
                union(
                    &[
                        labels,
                        marks
                            .into_iter()
                            .map(|marking| ConsumedMarking {
                                environment: session.environment().into(),
                                marking,
                            })
                            .collect(),
                    ],
                    session.environment(),
                )?,
            )
        } else {
            let binding:Uuid=tx.query_one("INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$8,$5,$6,$7,incarnation FROM moria.indexing_instance RETURNING id",&[&session.environment(),&resource,&spec_json,&context,&labels.iter().map(|label|label.marking.clone()).collect::<Vec<_>>(),&snapshot.security_epoch.parse::<i64>()?,&snapshot.instance_epoch.parse::<i64>()?,&operations]).await?.get(0);
            (binding, labels)
        };
        tx.execute("INSERT INTO moria.runtime_sandbox_bindings(binding,domain,artifact_uri,artifact_digest,outputs,effect,memory_mib,cpu_millis,timeout_seconds) VALUES($1,'agent',$2,$3,$4,'readWrite',$5,$6,$7)",&[&binding,&spec.artifact.uri,&spec.artifact.digest,&spec.outputs,&(spec.limits.memory_mib as i32),&(spec.limits.cpu_millis as i32),&(spec.limits.timeout_seconds as i32)]).await?;
        let id: Uuid = tx
            .query_one(
                "SELECT moria.runtime_execution_request($1,$2,$3,$4,$5)",
                &[
                    &binding,
                    &key,
                    &normalized,
                    &hash,
                    &json!([{"key":"agent","partition":null}]),
                ],
            )
            .await?
            .get(0);
        tx.execute("INSERT INTO moria.runtime_agent_executions(execution,entry_id,release,definition,adapter_id,adapter_definition,application_id,model_resource,model_binding,markings,model_secret_approval) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",&[&id,&source.get::<_,Uuid>("entry_id"),&release,&definition,&adapter_id,&adapter,&application_id,&model_resource,&model_binding,&json!(labels),&model_secret_approval]).await?;
        let references = self
            .platform
            .admit(&tx, session, id, &normalized, &spec.inputs)
            .await?;
        let labels = union(&[labels, references], session.environment())?;
        tx.execute(
            "UPDATE moria.runtime_agent_executions SET markings=$2 WHERE execution=$1",
            &[&id, &json!(labels)],
        )
        .await?;
        if let Some(case) = evaluation_case {
            let linked=tx.execute("UPDATE moria.runtime_evaluation_cases SET agent_execution=$2 WHERE execution=$1 AND (agent_execution IS NULL OR agent_execution=$2) AND outcome IN('pending','running')",&[&case,&id]).await?;
            ensure!(linked == 1, "EVALUATION_CASE_AUTHORITY");
        }
        let row = Self::row(tx.client(), session, id).await?;
        let result = Self::public(tx.client(), &row, labels).await?;
        Authority::finish(&*tx, session, &snapshot).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(result)
    }
    pub async fn get(&self, session: &VerifiedSession, id: Uuid) -> Result<Value> {
        self.db
            .get()
            .await?
            .query_one("SELECT moria.runtime_execution_reap($1)", &[&id])
            .await?;
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.read_transaction().await?;
        let snapshot = Authority::current(&*tx, session).await?;
        let row = Self::row(tx.client(), session, id).await?;
        let labels = self.retained(&tx, session, &row).await?;
        let result = Self::public(tx.client(), &row, labels).await?;
        Authority::finish(&*tx, session, &snapshot).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(result)
    }
    pub async fn events(&self, session: &VerifiedSession, id: Uuid, after: &str) -> Result<Value> {
        let cursor = after
            .parse::<i64>()
            .ok()
            .filter(|cursor| *cursor >= 0 && cursor.to_string() == after)
            .ok_or_else(|| anyhow::anyhow!("EVENT_CURSOR_INVALID"))?;
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.read_transaction().await?;
        let snapshot = Authority::current(&*tx, session).await?;
        let row = Self::row(tx.client(), session, id).await?;
        let labels = self.retained(&tx, session, &row).await?;
        let rows=tx.query("SELECT e.sequence::text,e.kind,e.payload FROM moria.runtime_execution_events e WHERE e.execution=$1 AND e.sequence>$2 ORDER BY e.sequence LIMIT 128",&[&id,&cursor]).await?;
        let events=rows.iter().map(|row|json!({"sequence":row.get::<_,String>(0),"type":row.get::<_,String>(1),"payload":row.get::<_,Value>(2),"markings":labels})).collect::<Vec<_>>();
        let result = json!({"events":events,"after":rows.last().map(|row|row.get::<_,String>(0)).unwrap_or_else(||after.into())});
        Authority::finish(&*tx, session, &snapshot).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(result)
    }
    pub async fn cancel(&self, session: &VerifiedSession, id: Uuid) -> Result<Value> {
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        let row=tx.query_opt("SELECT e.binding,e.resource,a.release FROM moria.runtime_executions e JOIN moria.runtime_agent_executions a ON a.execution=e.id WHERE e.id=$1 AND e.environment=$2 AND e.principal=$3 AND e.application=$4 FOR UPDATE OF e",&[&id,&session.environment(),&session.principal(),&session.application()]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        crate::execution::cancellation_authority_in(tx.client(), session, &row, "invoke").await?;
        crate::execution::cancel_authorized_in(tx.client(), session, id).await?;
        tx.commit().await?;
        admission.release().await?;
        ExecutionAuthority {
            db: self.db.clone(),
        }
        .reap_cancellations(32)
        .await?;
        Ok(json!({"id":id,"cancelRequested":true}))
    }

    async fn candidate(
        &self,
        owner: Uuid,
        publication: bool,
        target: Option<Uuid>,
    ) -> Result<Option<Uuid>> {
        let rows=self.db.get().await?.query("SELECT e.id,e.environment FROM moria.runtime_executions e JOIN moria.runtime_agent_executions a ON a.execution=e.id JOIN moria.runtime_resource_classes c ON c.name=e.resource_class AND c.enabled JOIN moria.runtime_function_workers w ON w.owner=$1 AND e.resource_class=ANY(w.resource_classes) AND 'agent'=ANY(w.domains) WHERE moria.runtime_environment_cell_available(e.environment) AND e.spec->>'domain'='agent' AND ($3::uuid IS NULL OR e.id=$3) AND e.deadline>clock_timestamp() AND ((NOT $2 AND e.status='queued' AND EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.execution=e.id AND t.status='pending' AND t.attempts=0)) OR ($2 AND e.status='running' AND NOT EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.execution=e.id AND t.status<>'sealed'))) AND moria.runtime_resource_in_use(e.resource_class)<c.maximum_tasks AND moria.runtime_resource_in_use(e.resource_class,e.environment)<c.maximum_environment_tasks ORDER BY e.created_at LIMIT 16",&[&owner,&publication,&target]).await?;
        for candidate in rows {
            let id: Uuid = candidate.get(0);
            let environment: String = candidate.get(1);
            let mut admission = Admission::begin(&self.db, &environment, false, None).await?;
            let tx = admission.client.as_mut().unwrap().transaction().await?;
            let Some(row)=tx.query_opt("SELECT binding,context,principal,application FROM moria.runtime_executions WHERE id=$1 AND status=$2 AND deadline>clock_timestamp() FOR UPDATE SKIP LOCKED",&[&id,&if publication{"running"}else{"queued"}]).await? else {continue};
            let task=tx.query_one("SELECT id,status,owner,fence,attempts FROM moria.runtime_execution_tasks WHERE execution=$1 FOR UPDATE",&[&id]).await?;
            if (!publication
                && (task.get::<_, String>("status") != "pending"
                    || task.get::<_, i32>("attempts") != 0))
                || (publication && task.get::<_, String>("status") != "sealed")
            {
                continue;
            }
            let checked=async {
                let actor:Value=row.get("context");
                if actor["sessionHash"].is_null(){tx.query_one("SELECT moria.runtime_sandbox_claim_authority($1)",&[&id]).await?;return Ok(());}
                tx.batch_execute("SAVEPOINT original_session_admission").await?;
                let session=match Authority::stored_session_in(tx.client(),actor["sessionHash"].as_str().ok_or_else(||anyhow::anyhow!("UNAUTHENTICATED"))?,&environment,&row.get::<_,String>("principal"),&row.get::<_,String>("application")).await {
                    Ok(session)=>{tx.batch_execute("RELEASE SAVEPOINT original_session_admission").await?;session},
                    Err(_)=>{tx.batch_execute("ROLLBACK TO SAVEPOINT original_session_admission; RELEASE SAVEPOINT original_session_admission").await?;tx.query_one("SELECT moria.native_serving_job_delegation($1)",&[&id]).await?;return Ok(());}
                };
                let metadata=Self::row(tx.client(),&session,id).await?;let labels=self.retained(&tx,&session,&metadata).await?;
                let state=Authority::current(&*tx,&session).await?;
                let task_owner:Option<Uuid>=task.get("owner");let fence=task_owner.map(|_|task.get::<_,i64>("fence"));
                tx.execute("INSERT INTO moria.runtime_agent_epochs(execution,binding,task,owner,fence,incarnation,security_epoch,instance_epoch) SELECT $1,$2,$3,$4,$5,incarnation,$6,$7 FROM moria.indexing_instance ON CONFLICT(execution) DO UPDATE SET owner=EXCLUDED.owner,fence=EXCLUDED.fence,security_epoch=EXCLUDED.security_epoch,instance_epoch=EXCLUDED.instance_epoch",&[&id,&row.get::<_,Uuid>("binding"),&task.get::<_,Uuid>("id"),&task_owner,&fence,&state.security_epoch.parse::<i64>()?,&state.instance_epoch.parse::<i64>()?]).await?;
                tx.execute("UPDATE moria.runtime_agent_executions SET markings=$2 WHERE execution=$1",&[&id,&json!(labels)]).await?;
                Authority::finish(&*tx,&session,&state).await?;Ok::<_,anyhow::Error>(())
            }.await;
            if checked.is_ok() {
                tx.commit().await?;
                admission.release().await?;
                return Ok(Some(id));
            }
            tx.rollback().await?;
            admission.release().await?;
            if let Err(error) = checked
                && publication_aborted(&error)
            {
                return Err(error);
            }
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            let changed=tx.execute("UPDATE moria.runtime_executions SET status='failed',error='AGENT_AUTHORIZATION_CHANGED',finished_at=clock_timestamp() WHERE id=$1 AND status=$2 AND NOT EXISTS(SELECT 1 FROM moria.runtime_execution_tasks WHERE execution=$1 AND status='running')",&[&id,&if publication{"running"}else{"queued"}]).await?;
            if changed > 0 {
                tx.execute("UPDATE moria.runtime_execution_tasks SET status='cancelled',lease_until=NULL WHERE execution=$1 AND status='pending'",&[&id]).await?;
                tx.query_one("SELECT moria.runtime_execution_event($1,'failed',jsonb_build_object('error','AGENT_AUTHORIZATION_CHANGED'))",&[&id]).await?;
            }
            tx.commit().await?;
        }
        Ok(None)
    }
    async fn publish(
        &self,
        supervisor: &SandboxSupervisor,
        lease: crate::execution::Lease,
    ) -> Result<()> {
        let context = supervisor.publication_context(lease.clone()).await?;
        ensure!(context.domain() == "agent", "AGENT_CONTEXT_INVALID");
        let mut admission = Admission::begin(&self.db, context.environment(), false, None).await?;
        let tx = admission
            .client
            .as_mut()
            .unwrap()
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::Serializable)
            .start()
            .await?;
        ensure!(tx.query_opt("SELECT 1 FROM moria.runtime_executions WHERE id=$1 AND status='preparing' AND owner=$2 AND fence=$3 AND lease_until>clock_timestamp() FOR UPDATE", &[&lease.execution,&lease.owner,&lease.fence.parse::<i64>()?]).await?.is_some(),"FUNCTION_LEASE_INVALID");
        context.check_in(tx.client()).await?;
        let row=tx.query_one("SELECT result FROM moria.runtime_execution_tasks WHERE execution=$1 AND status='sealed'",&[&lease.execution]).await?;
        let result: Value = row.get(0);
        let session = self.platform.publish(&tx, &context, &result).await?;
        let metadata = Self::row(tx.client(), &session, lease.execution).await?;
        context.record_consumed_markings(&self.retained(&tx, &session, &metadata).await?)?;
        let labels = context.consumed_markings();
        let hash = canonical::value_fingerprint(
            &result,
            &json!({"type":"object","properties":context.output_fields()}),
            &json!({}),
        )?;
        tx.execute("INSERT INTO moria.runtime_agent_receipts(execution,result,result_hash,markings) VALUES($1,$2,$3,$4)",&[&lease.execution,&result,&hash.strip_prefix("sha256:").unwrap(),&json!(labels)]).await?;
        tx.execute(
            "UPDATE moria.runtime_agent_executions SET markings=$2 WHERE execution=$1",
            &[&lease.execution, &json!(labels)],
        )
        .await?;
        tx.query_one(
            "SELECT moria.runtime_execution_complete($1,$2,$3,$4)",
            &[
                &lease.execution,
                &lease.owner,
                &lease.fence.parse::<i64>()?,
                &json!({"valueVersion":1,"executionId":lease.execution,"resultHash":hash}),
            ],
        )
        .await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(())
    }
    async fn finalize(
        &self,
        supervisor: &SandboxSupervisor,
        lease: crate::execution::Lease,
    ) -> Result<()> {
        let remaining=self.db.get().await?.query_opt("SELECT GREATEST(1,LEAST(30000,floor(extract(epoch FROM(LEAST(deadline,lease_until)-clock_timestamp()))*1000)::bigint)) FROM moria.runtime_executions WHERE id=$1 AND owner=$2 AND fence=$3 AND status='preparing'",&[&lease.execution,&lease.owner,&lease.fence.parse::<i64>()?]).await?.map(|row|row.get::<_,i64>(0));
        let Some(remaining) = remaining else {
            return Ok(());
        };
        // PostgreSQL explicitly aborts a serializable publication when a parent lease
        // changes after its snapshot. Only the sealed-result transaction is retried.
        let publish = async {
            for attempt in 0..5u32 {
                match self.publish(supervisor, lease.clone()).await {
                    Ok(()) => return Ok(()),
                    Err(error) if attempt < 4 && publication_aborted(&error) => {
                        tokio::time::sleep(Duration::from_millis(20u64 << attempt)).await;
                    }
                    Err(error) => return Err(error),
                }
            }
            unreachable!()
        };
        let outcome = tokio::time::timeout(Duration::from_millis(remaining as u64), publish).await;
        let (cause, sqlstate) = match outcome {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => {
                let state = error.chain().find_map(|cause| {
                    cause
                        .downcast_ref::<tokio_postgres::Error>()
                        .and_then(tokio_postgres::Error::as_db_error)
                        .map(|error| error.code().code().to_owned())
                });
                (crate::database::safe_error(&error), state)
            }
            Err(_) => ("DEADLINE_EXCEEDED", None),
        };
        {
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            let changed=tx.execute("UPDATE moria.runtime_executions SET status='failed',error='AGENT_PUBLICATION_FAILED',finished_at=clock_timestamp(),lease_until=NULL WHERE id=$1 AND owner=$2 AND fence=$3 AND status='preparing' AND NOT EXISTS(SELECT 1 FROM moria.runtime_agent_receipts WHERE execution=$1)",&[&lease.execution,&lease.owner,&lease.fence.parse::<i64>()?]).await?;
            if changed > 0 {
                tx.query_one("SELECT moria.runtime_execution_event($1,'failed',$2)",&[&lease.execution,&json!({"error":"AGENT_PUBLICATION_FAILED","cause":cause,"sqlstate":sqlstate})]).await?;
            }
            tx.commit().await?;
        }
        Ok(())
    }
    pub async fn worker_once(
        &self,
        owner: Uuid,
        host: Uuid,
        cancelled: watch::Receiver<bool>,
    ) -> Result<bool> {
        self.db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_function_worker_check($1,$2,NULL,'agent')",
                &[&owner, &host],
            )
            .await?;
        let broker = Arc::new(NativeAgentBroker {
            db: self.db.clone(),
            platform: self.platform.clone(),
            models: self.models.clone(),
        });
        let supervisor = SandboxSupervisor::new(self.db.clone(), host, broker)?;
        supervisor
            .cleanup_abandoned()
            .await
            .context("AGENT_WORKER_CLEANUP")?;
        if let Some(id) = self.candidate(owner, true, None).await?
            && let Some(claim) = supervisor.claim_agent(owner, true, Some(id)).await?
        {
            self.finalize(&supervisor, claim.lease).await?;
            return Ok(true);
        }
        let Some(id) = self.candidate(owner, false, None).await? else {
            return Ok(false);
        };
        let Some(claim) = supervisor.claim_agent(owner, false, Some(id)).await? else {
            return Ok(false);
        };
        if supervisor
            .execute(claim.lease, cancelled.clone())
            .await
            .is_ok()
            && !*cancelled.borrow()
            && let Some(id) = self.candidate(owner, true, Some(id)).await?
            && let Some(claim) = supervisor.claim_agent(owner, true, Some(id)).await?
        {
            self.finalize(&supervisor, claim.lease).await?;
        }
        Ok(true)
    }
}

#[derive(Clone)]
struct NativeAgentBroker {
    db: Database,
    platform: Arc<NativePlatformBroker>,
    models: NativeModels,
}
impl NativeAgentBroker {
    async fn model_marks(
        &self,
        client: &Client,
        context: &SandboxContext,
        session: &VerifiedSession,
        resource: &str,
    ) -> Result<()> {
        let admission = NativeModels::admission_in(client, session, resource).await?;
        let accepted: Option<Value> = client.query_one("SELECT model_secret_approval FROM moria.runtime_agent_executions WHERE execution=$1", &[&context.lease().execution]).await?.get(0);
        ensure!(
            admission
                .secret_approval
                .as_ref()
                .map(serde_json::to_value)
                .transpose()?
                == accepted,
            "SECRET_APPROVAL_REVOKED"
        );
        let labels = admission
            .markings
            .into_iter()
            .map(|marking| ConsumedMarking {
                environment: session.environment().into(),
                marking,
            })
            .collect::<Vec<_>>();
        context.record_consumed_markings(&labels)?;
        Ok(())
    }
    async fn model(&self, context: &SandboxContext, call: HttpCall) -> Result<HttpResponse> {
        ensure!(call.method == "POST", "AGENT_MODEL_REQUEST_INVALID");
        let logical = call
            .headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("idempotency-key"))
            .map(|(_, value)| value.as_str())
            .ok_or_else(|| anyhow::anyhow!("AGENT_MODEL_KEY_REQUIRED"))?;
        logical_key(logical)?;
        let request: Value = serde_json::from_slice(
            &STANDARD.decode(
                call.body
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("AGENT_MODEL_REQUEST_INVALID"))?,
            )?,
        )?;
        let hash = canonical::fingerprint(&request)?;
        let hash = hash.strip_prefix("sha256:").unwrap().to_owned();
        let key = format!(
            "agent/{}/{}",
            context.lease().execution,
            canonical::fingerprint(&json!(logical))?
        );
        let mut admission = Admission::begin(&self.db, context.environment(), false, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        let session = Authority::sandbox_session_in(tx.client(), context)
            .await
            .context("AGENT_MODEL_SESSION")?;
        let resource: String = tx
            .query_one(
                "SELECT model_resource FROM moria.runtime_agent_executions WHERE execution=$1",
                &[&context.lease().execution],
            )
            .await?
            .get(0);
        context.record_consumed_markings(
            &self
                .platform
                .prepared(tx.client(), &session, context.lease().execution, false)
                .await
                .context("AGENT_MODEL_READ_EVIDENCE")?,
        )?;
        self.model_marks(tx.client(), context, &session, &resource)
            .await
            .context("AGENT_MODEL_MARKINGS")?;
        let row=tx.query_one("INSERT INTO moria.runtime_agent_model_calls(execution,logical_id,attempt,model_resource,model_key,request_hash,markings) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(execution,logical_id) DO UPDATE SET request_hash=moria.runtime_agent_model_calls.request_hash RETURNING request_hash",&[&context.lease().execution,&logical,&context.attempt_id(),&resource,&key,&hash,&json!(context.consumed_markings())]).await.context("AGENT_MODEL_TRACE")?;
        ensure!(row.get::<_, String>(0) == hash, "IDEMPOTENCY_CONFLICT");
        ensure!(
            tx.query_one(
                "SELECT count(*) FROM moria.runtime_agent_model_calls WHERE execution=$1",
                &[&context.lease().execution]
            )
            .await?
            .get::<_, i64>(0)
                <= 32,
            "AGENT_CALL_LIMIT"
        );
        tx.execute(
            "UPDATE moria.runtime_agent_executions SET markings=$2 WHERE execution=$1",
            &[
                &context.lease().execution,
                &json!(context.consumed_markings()),
            ],
        )
        .await?;
        context.check_in(tx.client()).await?;
        tx.commit().await?;
        admission.release().await?;
        let result = self
            .models
            .chat_sandbox(
                context,
                &resource,
                &key,
                &request,
                context
                    .consumed_markings()
                    .into_iter()
                    .map(|label| label.marking)
                    .collect(),
            )
            .await;
        let mut admission = Admission::begin(&self.db, context.environment(), false, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        // Take the eventual fence lock before private session resolution can take
        // a shared execution lock. Observed labels still precede policy checks.
        tx.query_one(
            "SELECT 1 FROM moria.runtime_executions WHERE id=$1 FOR UPDATE",
            &[&context.lease().execution],
        )
        .await?;
        // Classify the observed callback before testing current authority or returning it.
        if let Some(row)=tx.query_opt("SELECT markings FROM moria.native_model_calls WHERE environment=$1 AND application=$2 AND principal=$3 AND resource=$4 AND call_key=$5",&[&context.environment(),&context.application(),&context.principal(),&resource,&key]).await? {
            context.record_consumed_markings(&row.get::<_,Vec<String>>(0).into_iter().map(|marking|ConsumedMarking{environment:context.environment().into(),marking}).collect::<Vec<_>>())?;
        }
        self.model_marks(tx.client(), context, &session, &resource)
            .await?;
        let (status, body, usage, error) = match result {
            Ok(value) => (200, json!(value), value.get("usage").cloned(), None),
            Err(error) => {
                let code = crate::model_http::error_code(&error);
                (
                    502,
                    json!({"error":{"code":code,"message":"Agent model call failed"}}),
                    None,
                    Some(code),
                )
            }
        };
        let serialized = serde_json::to_vec(&body)?;
        ensure!(serialized.len() <= 32768, "AGENT_MODEL_RESPONSE_LIMIT");
        tx.execute("UPDATE moria.runtime_agent_model_calls SET status=$3,markings=$4,usage=$5,result_hash=$6,error=$7,finished_at=clock_timestamp() WHERE execution=$1 AND logical_id=$2",&[&context.lease().execution,&logical,&status,&json!(context.consumed_markings()),&usage,&canonical::fingerprint(&body)?,&error]).await?;
        tx.execute(
            "UPDATE moria.runtime_agent_executions SET markings=$2 WHERE execution=$1",
            &[
                &context.lease().execution,
                &json!(context.consumed_markings()),
            ],
        )
        .await?;
        context.complete_current_observation_in(tx.client()).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(HttpResponse {
            status: status as u16,
            headers: std::collections::BTreeMap::from([(
                "content-type".into(),
                "application/json".into(),
            )]),
            body: STANDARD.encode(serialized),
        })
    }
}
#[async_trait]
impl PlatformBroker for NativeAgentBroker {
    async fn start(&self, context: &SandboxContext) -> Result<()> {
        self.platform.start(context).await
    }
    async fn http(&self, context: &SandboxContext, call: HttpCall) -> Result<HttpResponse> {
        context.check().await?;
        if call.path == "/api/v1/agent/model" {
            let result = self.model(context, call).await;
            if let Err(error) = &result {
                let code = match error.to_string().as_str() {
                    "AGENT_MODEL_SESSION" => "AGENT_MODEL_SESSION",
                    "AGENT_MODEL_READ_EVIDENCE" => "AGENT_MODEL_READ_EVIDENCE",
                    "AGENT_MODEL_MARKINGS" => "AGENT_MODEL_MARKINGS",
                    "AGENT_MODEL_TRACE" => "AGENT_MODEL_TRACE",
                    _ => "AGENT_MODEL_BROKER_FAILED",
                };
                let sql_state = error
                    .chain()
                    .find_map(|cause| cause.downcast_ref::<tokio_postgres::Error>())
                    .and_then(|error| error.as_db_error())
                    .map(|error| error.code().code());
                let mut client = self.db.get().await?;
                let tx = client.transaction().await?;
                context.check_in(tx.client()).await?;
                tx.query_one(
                    "SELECT moria.runtime_execution_event($1,'brokerFailed',$2)",
                    &[
                        &context.lease().execution,
                        &json!({"kind":"model","code":code,"sqlState":sql_state}),
                    ],
                )
                .await?;
                tx.commit().await?;
            }
            return result;
        }
        ensure!(
            !call.path.contains("/agent-executions/"),
            "AGENT_CAPABILITY_UNSUPPORTED"
        );
        ensure!(
            !(call.path.starts_with("/api/management/v1/fs/")
                && call.method != "GET"
                && !call.path.ends_with("/resolve")),
            "AGENT_CAPABILITY_UNSUPPORTED"
        );
        let response = self.platform.http(context, call).await?;
        if response.status >= 400 {
            let code = STANDARD
                .decode(&response.body)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .and_then(|body| body["error"]["code"].as_str().map(str::to_owned))
                .unwrap_or_else(|| "AGENT_CAPABILITY_UNSUPPORTED".into());
            let code = crate::database::safe_error(&anyhow::anyhow!(code));
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            context.check_in(tx.client()).await?;
            tx.query_one("SELECT moria.runtime_execution_event($1,'brokerDenied',$2)",&[&context.lease().execution,&json!({"status":response.status,"code":code,"markings":context.consumed_markings()})]).await?;
            tx.commit().await?;
        }
        Ok(response)
    }
    async fn log(
        &self,
        context: &SandboxContext,
        message: &str,
        consumed: &[ConsumedMarking],
    ) -> Result<()> {
        self.platform.log(context, message, consumed).await
    }
    async fn finish(&self, context: &SandboxContext, result: &Value) -> Result<()> {
        self.platform.finish(context, result).await
    }
}

pub async fn cli(db: Database, arguments: Vec<String>) -> Result<()> {
    let operation = arguments.first().map(String::as_str).unwrap_or("");
    if ["worker", "once", "cleanup"].contains(&operation) {
        let owner: Uuid = env::var("MORIA_AGENT_WORKER_OWNER")?.parse()?;
        let host: Uuid = env::var("MORIA_AGENT_HOST_ID")?.parse()?;
        db.get()
            .await?
            .query_one(
                "SELECT moria.runtime_function_worker_check($1,$2,NULL,'agent')",
                &[&owner, &host],
            )
            .await?;
        let service = NativeAgents::new(db.clone())?;
        let (cancel, cancelled) = watch::channel(false);
        if operation == "cleanup" {
            let supervisor = SandboxSupervisor::new(
                db,
                host,
                Arc::new(NativeAgentBroker {
                    db: service.db.clone(),
                    platform: service.platform.clone(),
                    models: service.models.clone(),
                }),
            )?;
            println!(
                "{}",
                json!({"cleaned":supervisor.cleanup_abandoned().await?})
            );
            return Ok(());
        }
        if operation == "once" {
            println!(
                "{}",
                json!({"worked":service.worker_once(owner,host,cancelled).await?})
            );
            return Ok(());
        }
        let concurrency = env::var("MORIA_AGENT_CONCURRENCY")
            .unwrap_or_else(|_| "4".into())
            .parse::<usize>()?;
        ensure!(
            (1..=32).contains(&concurrency),
            "AGENT_WORKER_CONFIG_INVALID"
        );
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let mut jobs = tokio::task::JoinSet::new();
        println!(
            "{}",
            json!({"event":"agent-worker","host":host,"owner":owner,"concurrency":concurrency})
        );
        loop {
            while jobs.len() < concurrency {
                let service = service.clone();
                let cancelled = cancelled.clone();
                jobs.spawn(async move {
                    let mut attempts=0;
                    let worked=loop {
                        match service.worker_once(owner,host,cancelled.clone()).await {
                            Err(error) if attempts<3 && publication_aborted(&error)=>{
                                attempts+=1;
                                eprintln!("{}",json!({"event":"agentWorkerTransactionRetry","attempt":attempts,"sqlstate":error.chain().find_map(|cause|cause.downcast_ref::<tokio_postgres::Error>().and_then(tokio_postgres::Error::as_db_error).map(|error|error.code().code()))}));
                                tokio::time::sleep(Duration::from_millis(20u64<<attempts)).await;
                            },
                            result=>break result,
                        }
                    };
                    if let Err(error)=&worked {eprintln!("{}",json!({"event":"agentWorkerFailure","code":crate::database::safe_error(error),"sandboxCode":crate::sandbox::error_code(error),"stage":if error.chain().any(|cause|cause.to_string()=="AGENT_WORKER_CLEANUP"){"cleanup"}else{"other"},"sqlstate":error.chain().find_map(|cause|cause.downcast_ref::<tokio_postgres::Error>().and_then(tokio_postgres::Error::as_db_error).map(|error|error.code().code()))}));}
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    worked
                });
            }
            tokio::select! {_=terminate.recv()=>break,_=tokio::signal::ctrl_c()=>break,result=jobs.join_next()=>{if let Some(result)=result{result??;}}}
        }
        cancel.send(true)?;
        while let Some(result) = jobs.join_next().await {
            result??;
        }
        return Ok(());
    }
    use tokio::io::{AsyncReadExt, stdin};
    let mut bytes = Vec::new();
    stdin().take(65537).read_to_end(&mut bytes).await?;
    ensure!(bytes.len() <= 65536, "AGENT_REQUEST_INVALID");
    let value: Value = serde_json::from_slice(&bytes)?;
    let session = Authority { db: db.clone() }
        .authenticate(
            value["token"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("UNAUTHENTICATED"))?,
        )
        .await?;
    let service = NativeAgents::new(db)?;
    let id = || -> Result<Uuid> {
        value["id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("AGENT_REQUEST_INVALID"))?
            .parse::<Uuid>()
            .map_err(Into::into)
    };
    let result = match operation {
        "invoke" => {
            service
                .invoke(
                    &session,
                    value["resource"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("AGENT_REQUEST_INVALID"))?
                        .parse()?,
                    value["key"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("AGENT_REQUEST_INVALID"))?,
                    json!({"input":value["input"],"wait":false}),
                )
                .await?
        }
        "get" => service.get(&session, id()?).await?,
        "events" => {
            service
                .events(&session, id()?, value["after"].as_str().unwrap_or("0"))
                .await?
        }
        "cancel" => service.cancel(&session, id()?).await?,
        _ => bail!("AGENT_COMMAND_INVALID"),
    };
    println!("{result}");
    Ok(())
}
