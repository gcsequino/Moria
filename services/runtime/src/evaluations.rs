use crate::{
    agents::NativeAgents,
    authority::{Authority, VerifiedSession},
    canonical, catalog,
    database::Database,
    execution::{self, Domain, ExecutionAuthority, Lease},
    functions::Admission,
    sandbox::{
        ConsumedMarking, HttpCall, HttpResponse, PlatformBroker, SandboxContext, SandboxSupervisor,
    },
    storage::Cancellation,
};
use anyhow::{Result, bail, ensure};
use arrow_array::{Array, StringArray};
use async_trait::async_trait;
use futures::TryStreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    env,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::watch;
use tokio_postgres::{Client, Row};
use uuid::Uuid;

pub(crate) fn domain_error_code(error: &anyhow::Error) -> Option<&'static str> {
    let message = error
        .chain()
        .find_map(|cause| {
            cause
                .downcast_ref::<tokio_postgres::Error>()
                .and_then(tokio_postgres::Error::as_db_error)
                .map(|error| error.message().to_owned())
        })
        .unwrap_or_else(|| error.root_cause().to_string());
    [
        "EVALUATION_ENVIRONMENT_REQUIRED",
        "EVALUATION_REQUEST_INVALID",
        "EVALUATION_KEY_INVALID",
        "EVALUATION_CURSOR_INVALID",
        "EVALUATION_NOT_READY",
        "EVALUATION_CASE_AUTHORITY",
        "EVALUATION_SERVICE_REVOKED",
        "EVALUATION_CHILD_ALREADY_ACCEPTED",
        "EVALUATION_SERVICE_PROFILE_INVALID",
        "EVALUATION_PIN_INVALID",
        "EVALUATION_RESULT_INVALID",
        "EVALUATION_CASE_LIMIT",
        "EVALUATION_FIXTURE_SCHEMA",
        "EVALUATION_FIXTURE_JSON",
        "EVALUATION_SOURCE_INVALID",
        "EVALUATION_REFERENCE_INVALID",
        "EVALUATION_MARKINGS_INVALID",
        "EVALUATION_LIMIT_INVALID",
        "EVALUATION_ENVELOPE_LIMIT",
        "EVALUATION_FAILED",
        "EVALUATION_WORKER_CONFIG_INVALID",
        "EVALUATION_CASE_ID",
        "EVALUATION_AGENT_INVALID",
        "EVALUATOR_CONTEXT_INVALID",
        "EVALUATOR_RESULT_INVALID",
        "EVALUATOR_OPERATION_DENIED",
        "EVALUATOR_ARTIFACT_INVALID",
    ]
    .into_iter()
    .find(|code| message == *code)
}

pub(crate) fn source_profile(definition: &Value) -> Result<Value> {
    ensure!(
        definition["apiVersion"] == "moria/v1alpha2"
            && definition["kind"] == "EvaluationSuite"
            && definition["spec"]["repeats"]
                .as_u64()
                .is_some_and(|count| (1..=16).contains(&count)),
        "EVALUATION_LIMIT_INVALID"
    );
    crate::sandbox::artifact_image(&definition["spec"]["evaluator"])?;
    Ok(
        json!({"domain":"evaluator","artifact":definition["spec"]["evaluator"],"limits":{"timeoutSeconds":30,"memoryMiB":128,"cpuMillis":10000}}),
    )
}

fn agent_model_operations(pin: &Value) -> Result<Vec<String>> {
    let resource = pin["modelResource"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("EVALUATION_PIN_INVALID"))?;
    Uuid::parse_str(resource)?;
    let mut operations = vec![format!("{resource}:invoke")];
    if !pin["modelSecretOperation"].is_null() {
        let operation = pin["modelSecretOperation"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("EVALUATION_PIN_INVALID"))?;
        let secret = operation
            .strip_suffix(":use")
            .ok_or_else(|| anyhow::anyhow!("EVALUATION_PIN_INVALID"))?;
        ensure!(
            Uuid::parse_str(secret)?.to_string() == secret,
            "EVALUATION_PIN_INVALID"
        );
        operations.push(operation.to_owned());
    }
    Ok(operations)
}

fn labels(value: &Value) -> Result<Vec<ConsumedMarking>> {
    value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("EVALUATION_MARKINGS_INVALID"))?
        .iter()
        .map(|label| {
            Ok(ConsumedMarking {
                environment: label["environment"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("EVALUATION_MARKINGS_INVALID"))?
                    .into(),
                marking: label["marking"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("EVALUATION_MARKINGS_INVALID"))?
                    .into(),
            })
        })
        .collect()
}
fn union(groups: &[Vec<ConsumedMarking>], environment: &str) -> Result<Vec<ConsumedMarking>> {
    let result: BTreeSet<_> = groups.iter().flatten().cloned().collect();
    ensure!(
        result.len() <= 256
            && result.iter().all(|label| label.environment == environment
                && !label.marking.is_empty()
                && label.marking.len() <= 180),
        "EVALUATION_MARKINGS_INVALID"
    );
    Ok(result.into_iter().collect())
}
fn digest(value: &Value) -> Result<String> {
    Ok(canonical::fingerprint(value)?
        .strip_prefix("sha256:")
        .unwrap()
        .into())
}
fn empty_hash(spec: &Value) -> Result<String> {
    Ok(execution::normalized(&serde_json::from_value(spec.clone())?, &json!({}))?.1)
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServiceProfile {
    service_authority: Uuid,
    suite_service_revision: String,
    agent_service_revision: String,
    evaluator_service_revision: String,
}
/// Trusted release/operator metadata registration. It grants no session or IO.
pub(crate) async fn install_service_in(
    client: &Client,
    environment: &str,
    resource: Uuid,
    configuration: &Value,
) -> Result<()> {
    let profile: ServiceProfile = serde_json::from_value(configuration.clone())
        .map_err(|_| anyhow::anyhow!("EVALUATION_SERVICE_PROFILE_INVALID"))?;
    ensure!(
        [
            &profile.suite_service_revision,
            &profile.agent_service_revision,
            &profile.evaluator_service_revision
        ]
        .iter()
        .all(|revision| !revision.is_empty() && revision.len() <= 128),
        "EVALUATION_SERVICE_PROFILE_INVALID"
    );
    let row=client.query_opt("SELECT b.digest,b.reference,a.resources FROM moria.fs_resource_bindings b JOIN moria.fs_entries e ON e.id=b.entry_id AND e.resource_id=$1 AND e.environment_id=$2 AND NOT e.deleted JOIN moria.fs_roles r ON r.entry_id=b.root_id AND NOT r.retired JOIN moria.fs_active_releases a ON a.root_id=b.root_id AND a.environment=b.environment AND a.digest=b.digest WHERE b.active",&[&resource,&environment]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
    let reference: String = row.get(1);
    let resources: Value = row.get(2);
    let definition = resources
        .as_array()
        .and_then(|items| {
            items.iter().find(|item| {
                format!(
                    "{}/{}/{}",
                    item["metadata"]["namespace"].as_str().unwrap_or(""),
                    item["kind"].as_str().unwrap_or(""),
                    item["metadata"]["name"].as_str().unwrap_or("")
                ) == reference
            })
        })
        .ok_or_else(|| anyhow::anyhow!("EVALUATION_SOURCE_INVALID"))?;
    source_profile(definition)?;
    let agent: Uuid = crate::filesystem::NativeSources::resource_in(
        client,
        &row.get::<_, String>(0),
        definition["spec"]["agent"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("EVALUATION_SOURCE_INVALID"))?,
    )
    .await?
    .parse()?;
    for (target, domain, revision, class) in [
        (
            resource,
            "evaluation",
            profile.suite_service_revision.as_str(),
            "evaluation-control",
        ),
        (
            agent,
            "agent",
            profile.agent_service_revision.as_str(),
            "standard",
        ),
        (
            resource,
            "evaluator",
            profile.evaluator_service_revision.as_str(),
            "standard",
        ),
    ] {
        let service = crate::jobs::RegisteredService::load_current_in(
            client,
            environment,
            target,
            domain,
            revision,
        )
        .await?;
        ensure!(
            service.id() == profile.service_authority && service.resource_class() == class,
            "EVALUATION_SERVICE_PROFILE_INVALID"
        );
        service
            .resource_labels_in(
                client,
                &target.to_string(),
                if domain == "agent" { "invoke" } else { "run" },
            )
            .await?;
        if domain == "agent" {
            NativeAgents::registered_model_admission_in(client, &service).await?;
        }
    }
    client.execute("UPDATE moria.runtime_evaluation_service_profiles SET enabled=false WHERE environment=$1 AND resource=$2 AND enabled AND NOT(authority=$3 AND suite_revision=$4)",&[&environment,&resource,&profile.service_authority,&profile.suite_service_revision]).await?;
    ensure!(client.query_opt("INSERT INTO moria.runtime_evaluation_service_profiles(environment,resource,authority,suite_revision,agent_revision,evaluator_revision) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(environment,resource,authority,suite_revision) DO UPDATE SET enabled=true WHERE runtime_evaluation_service_profiles.agent_revision=EXCLUDED.agent_revision AND runtime_evaluation_service_profiles.evaluator_revision=EXCLUDED.evaluator_revision RETURNING id",&[&environment,&resource,&profile.service_authority,&profile.suite_service_revision,&profile.agent_service_revision,&profile.evaluator_service_revision]).await?.is_some(),"EVALUATION_SERVICE_PROFILE_INVALID");
    Ok(())
}
/// Metadata availability only; no lease, session, provider or OCI operation.
pub(crate) async fn service_readiness_in(
    client: &Client,
    environment: &str,
    resource: Uuid,
) -> Result<Value> {
    let rows=client.query("SELECT * FROM moria.runtime_evaluation_service_profiles WHERE environment=$1 AND resource=$2 AND enabled FOR SHARE",&[&environment,&resource]).await?;
    if rows.is_empty() {
        return Ok(json!({"configured":false,"purposeReady":false,"workerReady":false}));
    }
    ensure!(rows.len() == 1, "EVALUATION_SERVICE_PROFILE_INVALID");
    let profile = &rows[0];
    let row=client.query_one("SELECT b.digest,b.reference,a.resources FROM moria.fs_resource_bindings b JOIN moria.fs_entries e ON e.id=b.entry_id AND e.resource_id=$1 AND e.environment_id=$2 AND NOT e.deleted JOIN moria.fs_active_releases a ON a.root_id=b.root_id AND a.environment=b.environment AND a.digest=b.digest WHERE b.active",&[&resource,&environment]).await?;
    let reference: String = row.get(1);
    let resources: Value = row.get(2);
    let definition = resources
        .as_array()
        .and_then(|items| {
            items.iter().find(|item| {
                format!(
                    "{}/{}/{}",
                    item["metadata"]["namespace"].as_str().unwrap_or(""),
                    item["kind"].as_str().unwrap_or(""),
                    item["metadata"]["name"].as_str().unwrap_or("")
                ) == reference
            })
        })
        .ok_or_else(|| anyhow::anyhow!("EVALUATION_SOURCE_INVALID"))?;
    source_profile(definition)?;
    let agent: Uuid = crate::filesystem::NativeSources::resource_in(
        client,
        &row.get::<_, String>(0),
        definition["spec"]["agent"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("EVALUATION_SOURCE_INVALID"))?,
    )
    .await?
    .parse()?;
    for (target, domain, revision, class) in [
        (
            resource,
            "evaluation",
            profile.get::<_, String>("suite_revision"),
            "evaluation-control",
        ),
        (agent, "agent", profile.get("agent_revision"), "standard"),
        (
            resource,
            "evaluator",
            profile.get("evaluator_revision"),
            "standard",
        ),
    ] {
        let service = crate::jobs::RegisteredService::load_current_in(
            client,
            environment,
            target,
            domain,
            &revision,
        )
        .await?;
        ensure!(
            service.id() == profile.get::<_, Uuid>("authority")
                && service.resource_class() == class,
            "EVALUATION_SERVICE_PROFILE_INVALID"
        );
        service
            .resource_labels_in(
                client,
                &target.to_string(),
                if domain == "agent" { "invoke" } else { "run" },
            )
            .await?;
        if domain == "agent" {
            NativeAgents::registered_model_admission_in(client, &service).await?;
        }
    }
    let worker:bool=client.query_one("SELECT bool_and(EXISTS(SELECT 1 FROM moria.runtime_workers w JOIN moria.indexing_instance i ON i.incarnation=w.incarnation JOIN moria.runtime_resource_classes c ON c.name=required.class AND c.enabled WHERE w.enabled AND w.expires_at>clock_timestamp() AND required.class=ANY(w.resource_classes) AND required.domain=ANY(w.domains))) FROM (VALUES('evaluation','evaluation-control'),('agent','standard'),('evaluator','standard')) required(domain,class)",&[]).await?.get(0);
    Ok(
        json!({"configured":true,"purposeReady":true,"workerReady":worker,"generation":profile.get::<_,i64>("generation").to_string()}),
    )
}

/// Purpose composition requires a real case lease; ordinary evaluation callers
/// continue through their original accepted delegation.
pub(crate) async fn child_binding_in(
    client: &Client,
    session: &VerifiedSession,
    target: Uuid,
    spec: &Value,
    operations: &[String],
    markings: &[String],
) -> Result<Option<(crate::jobs::ServiceBinding, Vec<String>)>> {
    let metadata: Option<Value> = client
        .query_one(
            "SELECT moria.runtime_evaluation_service_metadata($1,$2,$3,$4)",
            &[
                &session.session_hash(),
                &target,
                &spec["domain"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("EVALUATION_PIN_INVALID"))?,
                &spec["resourceClass"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("EVALUATION_PIN_INVALID"))?,
            ],
        )
        .await?
        .get(0);
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let service = crate::jobs::RegisteredService::load_current_in(
        client,
        session.environment(),
        target,
        spec["domain"].as_str().unwrap(),
        metadata["revision"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("EVALUATION_SERVICE_REVOKED"))?,
    )
    .await?;
    ensure!(
        metadata["authority"].as_str() == Some(service.id().to_string().as_str()),
        "EVALUATION_SERVICE_REVOKED"
    );
    let mut consumed: BTreeSet<String> = markings.iter().cloned().collect();
    consumed.extend(
        labels(&metadata["markings"])?
            .into_iter()
            .map(|label| label.marking),
    );
    let consumed: Vec<String> = consumed.into_iter().collect();
    let current = Authority::current(client, session).await?;
    ensure!(
        operations.iter().all(
            |operation| current.identity.granted_operations.contains(operation)
                && current.identity.application_scopes.contains(operation)
        ) && consumed
            .iter()
            .all(|mark| current.identity.markings.contains(mark)),
        "FORBIDDEN"
    );
    let binding = service
        .bind_evaluation_in(client, spec, operations, &consumed, session, &metadata)
        .await?;
    Ok(Some((binding, consumed)))
}
async fn source(
    client: &Client,
    session: &VerifiedSession,
    resource: Uuid,
) -> Result<(Uuid, String, Value, Vec<ConsumedMarking>)> {
    let row=client.query_opt("SELECT b.entry_id,b.digest,b.reference,a.resources FROM moria.fs_entries e JOIN moria.fs_resource_bindings b ON b.entry_id=e.id AND b.active JOIN moria.fs_roles r ON r.entry_id=b.root_id AND NOT r.retired JOIN moria.fs_active_releases a ON a.root_id=b.root_id AND a.environment=b.environment AND a.digest=b.digest WHERE e.resource_id=$1 AND e.environment_id=$2 AND NOT e.deleted",&[&resource,&session.environment()]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
    let resources: Value = row.get(3);
    let reference: String = row.get(2);
    let declaration = resources
        .as_array()
        .and_then(|resources| {
            resources.iter().find(|definition| {
                format!(
                    "{}/{}/{}",
                    definition["metadata"]["namespace"].as_str().unwrap_or(""),
                    definition["kind"].as_str().unwrap_or(""),
                    definition["metadata"]["name"].as_str().unwrap_or("")
                ) == reference
            })
        })
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("EVALUATION_SOURCE_INVALID"))?;
    let markings =
        Authority::resource_access_for_operation(client, session, &resource.to_string(), "run")
            .await?
            .into_iter()
            .map(|marking| ConsumedMarking {
                environment: session.environment().into(),
                marking,
            })
            .collect();
    Ok((row.get(0), row.get(1), declaration, markings))
}
async fn reference(
    client: &Client,
    session: &VerifiedSession,
    reference: &str,
    kind: &str,
) -> Result<Uuid> {
    ensure!(
        reference.split('/').nth(1) == Some(kind),
        "EVALUATION_REFERENCE_INVALID"
    );
    Ok(client.query_opt("SELECT e.resource_id FROM moria.fs_resource_bindings b JOIN moria.fs_entries e ON e.id=b.entry_id AND NOT e.deleted JOIN moria.fs_roles r ON r.entry_id=b.root_id AND NOT r.retired WHERE b.environment=$1 AND b.reference=$2 AND b.active",&[&session.environment(),&reference]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?.get(0))
}

#[derive(Clone)]
pub struct NativeEvaluations {
    db: Database,
    agents: NativeAgents,
}
impl NativeEvaluations {
    pub fn new(db: Database) -> Result<Self> {
        Ok(Self {
            agents: NativeAgents::new(db.clone())?,
            db,
        })
    }
    async fn header(client: &Client, session: &VerifiedSession, id: Uuid) -> Result<Row> {
        client.query_opt("SELECT e.*,r.entry_id,r.release,r.definition,r.agent_resource,r.agent_pin,r.fixture_resource,r.fixture_pin,r.maximum_cases,r.maximum_parallel,r.repeats,r.loaded,r.case_count,r.markings,r.service_profile,r.service_profile_generation,(e.finished_at IS NOT NULL AND e.finished_at<=clock_timestamp()-interval '30 days') AS expired FROM moria.runtime_executions e JOIN moria.runtime_evaluations r ON r.execution=e.id WHERE e.id=$1 AND e.environment=$2 AND e.principal=$3 AND e.application=$4",&[&id,&session.environment(),&session.principal(),&session.application()]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))
    }
    async fn current(
        client: &Client,
        session: &VerifiedSession,
        row: &Row,
    ) -> Result<Vec<ConsumedMarking>> {
        ensure!(!row.get::<_, bool>("expired"), "RESULT_EXPIRED");
        if let Some(profile) = row.get::<_, Option<Uuid>>("service_profile") {
            ensure!(client.query_opt("SELECT 1 FROM moria.runtime_evaluation_service_profiles WHERE id=$1 AND generation=$2 AND enabled FOR SHARE",&[&profile,&row.get::<_,Option<i64>>("service_profile_generation")]).await?.is_some(),"EVALUATION_SERVICE_REVOKED");
            client
                .query_one(
                    "SELECT moria.runtime_service_binding_guard($1)",
                    &[&row.get::<_, Uuid>("binding")],
                )
                .await?;
        }
        ensure!(client.query_opt("SELECT 1 FROM moria.runtime_evaluation_environments WHERE environment=$1 AND application=$2 AND enabled FOR SHARE",&[&session.environment(),&session.application()]).await?.is_some(),"EVALUATION_ENVIRONMENT_REQUIRED");
        let mut groups = vec![labels(&row.get::<_, Value>("markings"))?];
        groups.push(crate::automations::retained_in(client, session, row.get("id")).await?);
        for (resource, operation) in [
            (row.get::<_, Uuid>("resource"), "run"),
            (row.get("fixture_resource"), "read"),
        ] {
            groups.push(
                Authority::resource_access_for_operation(
                    client,
                    session,
                    &resource.to_string(),
                    operation,
                )
                .await?
                .into_iter()
                .map(|marking| ConsumedMarking {
                    environment: session.environment().into(),
                    marking,
                })
                .collect(),
            );
        }
        source(client, session, row.get("resource")).await?;
        let marks = union(&groups, session.environment())?;
        let state = Authority::current(client, session).await?;
        execution::invocation_permission(
            &state.identity,
            row.get("resource"),
            "evaluations",
            "run",
            &marks
                .iter()
                .map(|label| label.marking.clone())
                .collect::<Vec<_>>(),
        )?;
        Ok(marks)
    }
    async fn public(client: &Client, row: &Row, marks: Vec<ConsumedMarking>) -> Result<Value> {
        let id: Uuid = row.get("id");
        let status: String = row.get("status");
        let summary = client
            .query_opt(
                "SELECT summary FROM moria.runtime_evaluation_receipts WHERE execution=$1",
                &[&id],
            )
            .await?
            .map(|row| row.get::<_, Value>(0));
        Ok(
            json!({"valueVersion":1,"id":id,"resourceId":row.get::<_,Uuid>("resource"),"release":row.get::<_,String>("release"),"status":match status.as_str(){"succeeded"=>"completed","preparing"=>"running",other=>other},"loaded":row.get::<_,bool>("loaded"),"caseCount":row.get::<_,Option<i32>>("case_count"),"markings":marks,"summary":summary,"error":row.get::<_,Option<String>>("error")}),
        )
    }
    async fn evaluator_purpose_in(
        client: &Client,
        session: &VerifiedSession,
        execution: Uuid,
    ) -> Result<()> {
        let row=client.query_opt("SELECT e.binding,EXISTS(SELECT 1 FROM moria.runtime_execution_services s WHERE s.binding=e.binding) AS service_origin FROM moria.runtime_executions e JOIN moria.runtime_evaluator_executions v ON v.execution=e.id WHERE e.id=$1 AND e.environment=$2 AND e.principal=$3 AND e.application=$4",&[&execution,&session.environment(),&session.principal(),&session.application()]).await?.ok_or_else(||anyhow::anyhow!("EVALUATION_CASE_AUTHORITY"))?;
        if row.get::<_, bool>("service_origin") {
            client
                .query_one(
                    "SELECT moria.runtime_service_binding_guard($1)",
                    &[&row.get::<_, Uuid>("binding")],
                )
                .await?;
        }
        Ok(())
    }
    async fn retained(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        session: &VerifiedSession,
        row: &Row,
    ) -> Result<Vec<ConsumedMarking>> {
        let mut marks = Self::current(tx.client(), session, row).await?;
        marks.extend(
            crate::sandbox::released_source_markings_in(
                tx.client(),
                session,
                row.get("resource"),
                "run",
                &row.get::<_, String>("release"),
            )
            .await?,
        );
        let agents = tx
            .query(
                "SELECT agent_execution FROM moria.runtime_evaluation_cases WHERE evaluation=$1 AND agent_execution IS NOT NULL ORDER BY case_id,repeat LIMIT 1001",
                &[&row.get::<_, Uuid>("id")],
            )
            .await?;
        ensure!(agents.len() <= 1000, "EVALUATION_CASE_LIMIT");
        for agent in agents {
            marks.extend(self.agents.retained_in(tx, session, agent.get(0)).await?);
        }
        let evaluators=tx.query("SELECT execution FROM moria.runtime_evaluator_executions WHERE evaluation=$1 LIMIT 1001",&[&row.get::<_,Uuid>("id")]).await?;
        ensure!(evaluators.len() <= 1000, "EVALUATION_CASE_LIMIT");
        for evaluator in evaluators {
            Self::evaluator_purpose_in(tx.client(), session, evaluator.get(0)).await?;
            crate::sandbox::require_clean_observations_in(tx.client(), evaluator.get(0)).await?;
            marks.extend(
                crate::sandbox::retained_observations_in(tx.client(), evaluator.get(0)).await?,
            );
        }
        let marks = union(&[marks], session.environment())?;
        execution::invocation_permission(
            &Authority::current(tx, session).await?.identity,
            row.get("resource"),
            "evaluations",
            "run",
            &marks
                .iter()
                .map(|label| label.marking.clone())
                .collect::<Vec<_>>(),
        )?;
        Ok(marks)
    }
    pub async fn invoke(
        &self,
        session: &VerifiedSession,
        resource: Uuid,
        key: &str,
        payload: Value,
    ) -> Result<Value> {
        ensure!(
            !key.is_empty()
                && key.len() <= 128
                && key
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.:/-".contains(&b)),
            "EVALUATION_KEY_INVALID"
        );
        ensure!(
            payload
                .as_object()
                .is_some_and(|payload| payload.keys().all(|key| key == "wait"))
                && payload.get("wait").is_none_or(Value::is_boolean),
            "EVALUATION_REQUEST_INVALID"
        );
        let lock = format!(
            "moria/evaluation/request/{}",
            digest(&json!([
                session.environment(),
                resource,
                session.principal(),
                session.application(),
                key
            ]))?
        );
        let mut admission =
            Admission::begin(&self.db, session.environment(), false, Some(&lock)).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        let state = Authority::current(&*tx, session).await?;
        let limits=tx.query_opt("SELECT maximum_cases,maximum_repeats,maximum_parallel FROM moria.runtime_evaluation_environments WHERE environment=$1 AND application=$2 AND enabled FOR SHARE",&[&session.environment(),&session.application()]).await?.ok_or_else(||anyhow::anyhow!("EVALUATION_ENVIRONMENT_REQUIRED"))?;
        if let Some(row)=tx.query_opt("SELECT id FROM moria.runtime_executions WHERE environment=$1 AND resource=$2 AND principal=$3 AND application=$4 AND request_key=$5",&[&session.environment(),&resource,&session.principal(),&session.application(),&key]).await?{
            let id = row.get(0);
            tx.commit().await?;
            admission.release().await?;
            return self.get(session, id).await;
        }
        let (entry, release, definition, source_marks) =
            source(tx.client(), session, resource).await?;
        ensure!(
            definition["kind"] == "EvaluationSuite",
            "EVALUATION_SOURCE_INVALID"
        );
        let spec = &definition["spec"];
        let repeats = spec["repeats"]
            .as_i64()
            .filter(|n| *n > 0 && *n <= i64::from(limits.get::<_, i32>(1)))
            .ok_or_else(|| anyhow::anyhow!("EVALUATION_LIMIT_INVALID"))?
            as i32;
        let agent = reference(
            tx.client(),
            session,
            spec["agent"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("EVALUATION_SOURCE_INVALID"))?,
            "Agent",
        )
        .await?;
        let fixture = reference(
            tx.client(),
            session,
            spec["fixtures"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("EVALUATION_SOURCE_INVALID"))?,
            "Dataset",
        )
        .await?;
        let agent_pin = NativeAgents::pin_in(tx.client(), session, agent).await?;
        let fixture_pin =
            catalog::resolve_dataset_pin_in(&tx, session, &fixture.to_string()).await?;
        let marks = union(
            &[
                source_marks,
                labels(&agent_pin["markings"])?,
                fixture_pin
                    .markings
                    .iter()
                    .map(|marking| ConsumedMarking {
                        environment: session.environment().into(),
                        marking: marking.clone(),
                    })
                    .collect(),
            ],
            session.environment(),
        )?;
        execution::invocation_permission(
            &state.identity,
            resource,
            "evaluations",
            "run",
            &marks
                .iter()
                .map(|label| label.marking.clone())
                .collect::<Vec<_>>(),
        )?;
        let spec_json = json!({"version":1,"domain":"evaluation","release":release.strip_prefix("sha256:").ok_or_else(||anyhow::anyhow!("EVALUATION_SOURCE_INVALID"))?,"resourceClass":"evaluation-control","timeoutSeconds":3600,"retryPolicy":"idempotent","inputSchema":{"type":"object","properties":{}},"pinnedInputs":[fixture_pin.execution_input()],"expectedHeads":{}});
        let context = json!({"environment":session.environment(),"principal":session.principal(),"application":session.application(),"sessionHash":session.session_hash()});
        let binding = Self::binding(
            tx.client(),
            session,
            resource,
            &spec_json,
            &context,
            &marks,
            &agent_model_operations(&agent_pin)?,
        )
        .await?;
        let bound_marks: Vec<String> = tx
            .query_one(
                "SELECT markings FROM moria.runtime_execution_bindings WHERE id=$1",
                &[&binding],
            )
            .await?
            .get(0);
        let marks = union(
            &[
                marks,
                bound_marks
                    .into_iter()
                    .map(|marking| ConsumedMarking {
                        environment: session.environment().into(),
                        marking,
                    })
                    .collect(),
            ],
            session.environment(),
        )?;
        let id: Uuid = tx
            .query_one(
                "SELECT moria.runtime_execution_request($1,$2,'{}',$3,$4)",
                &[
                    &binding,
                    &key,
                    &empty_hash(&spec_json)?,
                    &json!([{"key":"fixtures","partition":null}]),
                ],
            )
            .await?
            .get(0);
        let pin_json = serde_json::to_value(&fixture_pin)?;
        tx.execute("INSERT INTO moria.runtime_evaluations(execution,entry_id,release,definition,agent_resource,agent_pin,fixture_resource,fixture_pin,maximum_cases,maximum_parallel,repeats,markings) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",&[&id,&entry,&release,&definition,&agent,&agent_pin,&fixture,&pin_json,&limits.get::<_,i32>(0),&limits.get::<_,i32>(2),&repeats,&json!(marks)]).await?;
        let header = Self::header(tx.client(), session, id).await?;
        let result = Self::public(tx.client(), &header, marks).await?;
        Authority::finish(&*tx, session, &state).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(result)
    }
    async fn binding(
        client: &Client,
        session: &VerifiedSession,
        resource: Uuid,
        spec: &Value,
        context: &Value,
        marks: &[ConsumedMarking],
        extra_operations: &[String],
    ) -> Result<Uuid> {
        let state = Authority::current(client, session).await?;
        let operation = execution::invocation_permission(
            &state.identity,
            resource,
            "evaluations",
            "run",
            &marks
                .iter()
                .map(|label| label.marking.clone())
                .collect::<Vec<_>>(),
        )?;
        let mut operations = vec![operation];
        operations.extend_from_slice(extra_operations);
        if spec["domain"] == "evaluator"
            && let Some((binding, _)) = child_binding_in(
                client,
                session,
                resource,
                spec,
                &operations,
                &marks
                    .iter()
                    .map(|label| label.marking.clone())
                    .collect::<Vec<_>>(),
            )
            .await?
        {
            return Ok(binding.id);
        }
        if let Some((binding, _)) = crate::automations::dispatch_binding_in(
            client,
            session,
            resource,
            spec,
            &operations,
            &marks
                .iter()
                .map(|label| label.marking.clone())
                .collect::<Vec<_>>(),
        )
        .await?
        {
            return Ok(binding.id);
        }
        Ok(client.query_one("INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$8,$5,$6,$7,incarnation FROM moria.indexing_instance RETURNING id",&[&session.environment(),&resource,&spec,&context,&marks.iter().map(|label|label.marking.clone()).collect::<Vec<_>>(),&state.security_epoch.parse::<i64>()?,&state.instance_epoch.parse::<i64>()?,&operations]).await?.get(0))
    }
    pub async fn get(&self, session: &VerifiedSession, id: Uuid) -> Result<Value> {
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.read_transaction().await?;
        let row = Self::header(tx.client(), session, id).await?;
        let marks = self.retained(&tx, session, &row).await?;
        let result = Self::public(tx.client(), &row, marks).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(result)
    }
    pub async fn cancel(&self, session: &VerifiedSession, id: Uuid) -> Result<Value> {
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        let row=tx.query_opt("SELECT e.binding,e.resource,r.release,r.service_profile,r.service_profile_generation FROM moria.runtime_executions e JOIN moria.runtime_evaluations r ON r.execution=e.id WHERE e.id=$1 AND e.environment=$2 AND e.principal=$3 AND e.application=$4 FOR UPDATE OF e",&[&id,&session.environment(),&session.principal(),&session.application()]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        ensure!(tx.query_opt("SELECT 1 FROM moria.runtime_evaluation_environments WHERE environment=$1 AND application=$2 AND enabled FOR SHARE",&[&session.environment(),&session.application()]).await?.is_some(),"EVALUATION_ENVIRONMENT_REQUIRED");
        if let Some(profile) = row.get::<_, Option<Uuid>>("service_profile") {
            ensure!(tx.query_opt("SELECT 1 FROM moria.runtime_evaluation_service_profiles WHERE id=$1 AND generation=$2 AND enabled FOR SHARE",&[&profile,&row.get::<_,Option<i64>>("service_profile_generation")]).await?.is_some(),"EVALUATION_SERVICE_REVOKED");
        }
        execution::cancellation_authority_in(tx.client(), session, &row, "run").await?;
        execution::cancel_authorized_in(tx.client(), session, id).await?;
        tx.commit().await?;
        admission.release().await?;
        ExecutionAuthority {
            db: self.db.clone(),
        }
        .reap_cancellations(32)
        .await?;
        Ok(json!({"id":id,"cancelRequested":true}))
    }
    pub async fn cases(
        &self,
        session: &VerifiedSession,
        id: Uuid,
        after: Option<&str>,
    ) -> Result<Value> {
        let position: Option<(String, i32)> = after
            .map(serde_json::from_str)
            .transpose()
            .map_err(|_| anyhow::anyhow!("EVALUATION_CURSOR_INVALID"))?;
        ensure!(
            position
                .as_ref()
                .is_none_or(|(name, repeat)| !name.is_empty()
                    && name.len() <= 180
                    && (0..16).contains(repeat)),
            "EVALUATION_CURSOR_INVALID"
        );
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.read_transaction().await?;
        let header = Self::header(tx.client(), session, id).await?;
        let base_marks = Self::current(tx.client(), session, &header).await?;
        let rows=tx.query("SELECT * FROM moria.runtime_evaluation_cases WHERE evaluation=$1 AND ($2::text IS NULL OR (case_id COLLATE \"C\",repeat)>($2 COLLATE \"C\",$3)) ORDER BY case_id COLLATE \"C\",repeat LIMIT 51",&[&id,&position.as_ref().map(|value|value.0.as_str()),&position.as_ref().map(|value|value.1)]).await?;
        let mut cases = Vec::new();
        for row in rows.iter().take(50) {
            let mut marks = union(
                &[
                    base_marks.clone(),
                    labels(&row.get::<_, Value>("markings"))?,
                ],
                session.environment(),
            )?;
            if let Some(agent) = row.get::<_, Option<Uuid>>("agent_execution") {
                marks = union(
                    &[marks, self.agents.retained_in(&tx, session, agent).await?],
                    session.environment(),
                )?;
            }
            if let Some(evaluator) = row.get::<_, Option<Uuid>>("evaluator_execution") {
                Self::evaluator_purpose_in(tx.client(), session, evaluator).await?;
                crate::sandbox::require_clean_observations_in(tx.client(), evaluator).await?;
                marks = union(
                    &[
                        marks,
                        crate::sandbox::retained_observations_in(tx.client(), evaluator).await?,
                    ],
                    session.environment(),
                )?;
            }
            let state = Authority::current(&*tx, session).await?;
            execution::invocation_permission(
                &state.identity,
                header.get("resource"),
                "evaluations",
                "run",
                &marks
                    .iter()
                    .map(|label| label.marking.clone())
                    .collect::<Vec<_>>(),
            )?;
            cases.push(json!({"caseId":row.get::<_,String>("case_id"),"repeat":row.get::<_,i32>("repeat"),"status":row.get::<_,String>("outcome"),"agentExecution":row.get::<_,Option<Uuid>>("agent_execution"),"evaluatorExecution":row.get::<_,Option<Uuid>>("evaluator_execution"),"score":row.get::<_,Option<f64>>("score"),"message":row.get::<_,Option<String>>("detail"),"error":row.get::<_,Option<String>>("error"),"durationMillis":row.get::<_,Option<i64>>("duration_millis").map(|value|value.to_string()),"markings":marks}));
        }
        let next = if rows.len() > 50 {
            let row = &rows[49];
            Some(canonical::canonical_json(&json!([
                row.get::<_, String>("case_id"),
                row.get::<_, i32>("repeat")
            ]))?)
        } else {
            None
        };
        tx.commit().await?;
        admission.release().await?;
        Ok(json!({"cases":cases,"after":next}))
    }
    pub async fn events(&self, session: &VerifiedSession, id: Uuid, after: &str) -> Result<Value> {
        let sequence = after
            .parse::<i64>()
            .ok()
            .filter(|value| *value >= 0 && value.to_string() == after)
            .ok_or_else(|| anyhow::anyhow!("EVENT_CURSOR_INVALID"))?;
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.read_transaction().await?;
        let row = Self::header(tx.client(), session, id).await?;
        let marks = self.retained(&tx, session, &row).await?;
        let events=tx.query("SELECT e.sequence::text,e.kind,e.payload FROM moria.runtime_execution_events e WHERE e.execution=$1 AND e.sequence>$2 ORDER BY e.sequence LIMIT 128",&[&id,&sequence]).await?;
        let body = json!({"events":events.iter().map(|row|json!({"sequence":row.get::<_,String>(0),"type":row.get::<_,String>(1),"payload":row.get::<_,Value>(2),"markings":marks})).collect::<Vec<_>>(),"after":events.last().map(|row|row.get::<_,String>(0)).unwrap_or_else(||after.into())});
        tx.commit().await?;
        admission.release().await?;
        Ok(body)
    }
    pub async fn compare(
        &self,
        session: &VerifiedSession,
        left: Uuid,
        right: Uuid,
    ) -> Result<Value> {
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.read_transaction().await?;
        let a = Self::header(tx.client(), session, left).await?;
        let b = Self::header(tx.client(), session, right).await?;
        let marks = union(
            &[
                self.retained(&tx, session, &a).await?,
                self.retained(&tx, session, &b).await?,
            ],
            session.environment(),
        )?;
        ensure!(
            a.get::<_, String>("status") == "succeeded"
                && b.get::<_, String>("status") == "succeeded",
            "EVALUATION_NOT_READY"
        );
        let definition_a: Value = a.get("definition");
        let definition_b: Value = b.get("definition");
        let incompatible:bool=tx.query_one("SELECT EXISTS((SELECT case_id,repeat,input_hash,expected FROM moria.runtime_evaluation_cases WHERE evaluation=$1 EXCEPT SELECT case_id,repeat,input_hash,expected FROM moria.runtime_evaluation_cases WHERE evaluation=$2) UNION ALL (SELECT case_id,repeat,input_hash,expected FROM moria.runtime_evaluation_cases WHERE evaluation=$2 EXCEPT SELECT case_id,repeat,input_hash,expected FROM moria.runtime_evaluation_cases WHERE evaluation=$1))",&[&left,&right]).await?.get(0);
        let comparable =
            !incompatible && definition_a["spec"]["evaluator"] == definition_b["spec"]["evaluator"];
        let summaries = if comparable {
            Some(
                json!({"left":Self::public(tx.client(),&a,labels(&a.get::<_,Value>("markings"))?).await?["summary"],"right":Self::public(tx.client(),&b,labels(&b.get::<_,Value>("markings"))?).await?["summary"]}),
            )
        } else {
            None
        };
        tx.commit().await?;
        admission.release().await?;
        Ok(
            json!({"comparable":comparable,"reason":if comparable{None}else{Some("CASE_OR_EVALUATOR_MISMATCH")},"summaries":summaries,"markings":marks}),
        )
    }
}

impl NativeEvaluations {
    async fn live(&self, lease: &Lease, host: Uuid) -> Result<VerifiedSession> {
        execution::job_session(&self.db, lease, host).await
    }
    async fn owned<T, F: std::future::Future<Output = Result<T>>>(
        &self,
        lease: &Lease,
        host: Uuid,
        mut cancel: watch::Receiver<bool>,
        work: F,
    ) -> Result<T> {
        let remaining:i64=self.db.get().await?.query_one("SELECT GREATEST(0,floor(extract(epoch FROM (deadline-clock_timestamp()))*1000))::bigint FROM moria.runtime_executions WHERE id=$1",&[&lease.execution]).await?.get(0);
        ensure!(remaining > 0, "DEADLINE_EXCEEDED");
        let session = self.live(lease, host).await?;
        let authority = ExecutionAuthority {
            db: self.db.clone(),
        };
        let guard = async {
            let mut tick = tokio::time::interval(Duration::from_millis(500));
            let mut renewed = Instant::now();
            loop {
                tick.tick().await;
                let current = async {
                    let client = self.db.get().await?;
                    Authority::current(&**client, &session).await?;
                    Ok::<(), anyhow::Error>(())
                };
                if let Err(error) = current.await {
                    break Err::<(), anyhow::Error>(error);
                }
                if renewed.elapsed() >= Duration::from_secs(8) {
                    if let Err(error) = authority.renew(lease).await {
                        break Err(error);
                    }
                    renewed = Instant::now();
                }
            }
        };
        let cancelled = async {
            loop {
                if cancel.changed().await.is_err() || *cancel.borrow() {
                    break Err::<(), anyhow::Error>(anyhow::anyhow!("CANCELLED"));
                }
            }
        };
        let run = async {
            tokio::select! {result=work=>result,result=guard=>{result?;bail!("JOB_LEASE_LOST")},result=cancelled=>{result?;bail!("CANCELLED")}}
        };
        tokio::time::timeout(Duration::from_millis(remaining as u64), run)
            .await
            .map_err(|_| anyhow::anyhow!("DEADLINE_EXCEEDED"))?
    }
    async fn load(&self, lease: &Lease, host: Uuid) -> Result<Value> {
        let session = self.live(lease, host).await?;
        let client = self.db.get().await?;
        let header = Self::header(&client, &session, lease.execution).await?;
        Self::current(&client, &session, &header).await?;
        if header.get::<_, bool>("loaded") {
            return Ok(json!({"loaded":header.get::<_,Option<i32>>("case_count")}));
        }
        let fixture: Uuid = header.get("fixture_resource");
        let maximum = header.get::<_, i32>("maximum_cases") as usize;
        let repeats = header.get::<_, i32>("repeats");
        let pin: Value = header.get("agent_pin");
        drop(client);
        let loaded = catalog::read_pinned_dataset(
            self.db.clone(),
            session.clone(),
            lease.clone(),
            &fixture.to_string(),
            Cancellation::default(),
        )
        .await?;
        let mut batches = loaded.snapshot.scan(1000).await?;
        let mut cases = Vec::new();
        let mut ids = BTreeSet::new();
        while let Some(batch) = batches.try_next().await? {
            let string_column = |name: &str| -> Result<&StringArray> {
                let index = batch
                    .schema()
                    .index_of(name)
                    .map_err(|_| anyhow::anyhow!("EVALUATION_FIXTURE_SCHEMA"))?;
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow::anyhow!("EVALUATION_FIXTURE_SCHEMA"))
            };
            let names = string_column("caseId")?;
            let inputs = string_column("inputJson")?;
            let expected = string_column("expectedJson")?;
            for index in 0..batch.num_rows() {
                ensure!(
                    !names.is_null(index) && !inputs.is_null(index) && !expected.is_null(index),
                    "EVALUATION_FIXTURE_SCHEMA"
                );
                let name = names.value(index);
                ensure!(
                    !name.is_empty()
                        && name.len() <= 180
                        && !name.contains('\0')
                        && ids.insert(name.to_owned()),
                    "EVALUATION_CASE_ID"
                );
                ensure!(
                    inputs.value(index).len() <= 8192
                        && expected.value(index).len() <= 8192
                        && (cases.len() + 1) * repeats as usize <= maximum,
                    "EVALUATION_CASE_LIMIT"
                );
                let input: Value = serde_json::from_str(inputs.value(index))
                    .map_err(|_| anyhow::anyhow!("EVALUATION_FIXTURE_JSON"))?;
                let expected: Value = serde_json::from_str(expected.value(index))
                    .map_err(|_| anyhow::anyhow!("EVALUATION_FIXTURE_JSON"))?;
                let (input, input_hash) = NativeAgents::normalize_pinned(&pin, &input)?;
                cases.push((name.to_owned(), input, input_hash, expected));
            }
        }
        ensure!(!cases.is_empty(), "EVALUATION_CASE_LIMIT");
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        tx.query_one(
            "SELECT moria.runtime_task_check($1,$2,$3)",
            &[&lease.task, &lease.owner, &lease.fence.parse::<i64>()?],
        )
        .await?;
        let header = Self::header(tx.client(), &session, lease.execution).await?;
        let marks = Self::current(tx.client(), &session, &header).await?;
        if !header.get::<_, bool>("loaded") {
            let source_spec: Value = header.get("spec");
            let deadline: String = tx
                .query_one(
                    "SELECT deadline::text FROM moria.runtime_executions WHERE id=$1",
                    &[&lease.execution],
                )
                .await?
                .get(0);
            for (name, input, input_hash, expected) in &cases {
                for repeat in 0..repeats {
                    let mut spec = source_spec.clone();
                    spec["pinnedInputs"] = json!([]);
                    let context = json!({"environment":session.environment(),"principal":session.principal(),"application":session.application(),"sessionHash":session.session_hash(),"parentExecution":lease.execution,"parentDeadline":deadline});
                    let binding = Self::binding(
                        tx.client(),
                        &session,
                        header.get("resource"),
                        &spec,
                        &context,
                        &marks,
                        &agent_model_operations(&header.get::<_, Value>("agent_pin"))?,
                    )
                    .await?;
                    let key = format!(
                        "evaluation/{}/{}",
                        lease.execution,
                        digest(&json!([name, repeat]))?
                    );
                    let id: Uuid = tx
                        .query_one(
                            "SELECT moria.runtime_execution_request($1,$2,'{}',$3,$4)",
                            &[
                                &binding,
                                &key,
                                &empty_hash(&spec)?,
                                &json!([{"key":"case","partition":null}]),
                            ],
                        )
                        .await?
                        .get(0);
                    tx.execute("INSERT INTO moria.runtime_evaluation_cases(evaluation,case_id,repeat,input,expected,input_hash,execution,markings) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",&[&lease.execution,&name,&repeat,&input,&expected,&input_hash,&id,&json!(marks)]).await?;
                }
            }
            let total = (cases.len() * repeats as usize) as i32;
            tx.execute(
                "UPDATE moria.runtime_evaluations SET loaded=true,case_count=$2 WHERE execution=$1",
                &[&lease.execution, &total],
            )
            .await?;
            tx.query_one("SELECT moria.runtime_execution_event($1,'fixturesLoaded',$2)",&[&lease.execution,&json!({"cases":total,"snapshot":loaded.pin.snapshot,"revision":loaded.pin.revision})]).await?;
        }
        tx.query_one(
            "SELECT moria.runtime_task_check($1,$2,$3)",
            &[&lease.task, &lease.owner, &lease.fence.parse::<i64>()?],
        )
        .await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(json!({"loaded":cases.len()*repeats as usize}))
    }
    async fn case(&self, lease: &Lease, host: Uuid) -> Result<Value> {
        for retry in 0..4 {
            let result = self.case_once(lease, host).await;
            if result.as_ref().is_err_and(execution::transaction_conflict) && retry < 3 {
                tokio::time::sleep(Duration::from_millis(10 << retry)).await;
            } else {
                return result;
            }
        }
        unreachable!()
    }
    async fn case_once(&self, lease: &Lease, host: Uuid) -> Result<Value> {
        let session = self.live(lease, host).await?;
        let agent_id = self
            .agents
            .invoke_evaluation_case(&session, lease.execution)
            .await?;
        {
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            tx.query_one(
                "SELECT moria.runtime_task_check($1,$2,$3)",
                &[&lease.task, &lease.owner, &lease.fence.parse::<i64>()?],
            )
            .await?;
            tx.execute("UPDATE moria.runtime_evaluation_cases SET agent_execution=$2,outcome='running' WHERE execution=$1 AND outcome IN ('pending','running')",&[&lease.execution,&agent_id]).await?;
            tx.commit().await?;
        }
        let agent = loop {
            if let Some(result) = self
                .agents
                .evaluation_progress(&session, lease.execution, agent_id)
                .await?
            {
                break result;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let client = self.db.get().await?;
        let row=client.query_one("SELECT c.*,r.definition,r.release,e.resource,e.deadline::text FROM moria.runtime_evaluation_cases c JOIN moria.runtime_evaluations r ON r.execution=c.evaluation JOIN moria.runtime_executions e ON e.id=c.execution WHERE c.execution=$1",&[&lease.execution]).await?;
        let marks = union(
            &[
                labels(&row.get::<_, Value>("markings"))?,
                labels(&agent["markings"])?,
            ],
            session.environment(),
        )?;
        if agent["status"] != "succeeded" {
            return Ok(json!({"outcome":"error","error":"AGENT_FAILED","markings":marks}));
        }
        let input = json!({"inputJson":canonical::canonical_json(&row.get::<_,Value>("input"))?,"expectedJson":canonical::canonical_json(&row.get::<_,Value>("expected"))?,"actualJson":canonical::canonical_json(&agent["result"])?});
        ensure!(
            canonical::canonical_json(&input)?.len() <= 32768,
            "EVALUATION_ENVELOPE_LIMIT"
        );
        let definition: Value = row.get("definition");
        let artifact = &definition["spec"]["evaluator"];
        let release: String = row.get("release");
        let root: Uuid = row.get("evaluation");
        let name: String = row.get("case_id");
        let repeat: i32 = row.get("repeat");
        let resource: Uuid = row.get("resource");
        let deadline: String = row.get("deadline");
        drop(client);
        let evaluator = if let Some(evaluator) = row.get::<_, Option<Uuid>>("evaluator_execution") {
            ensure!(self.db.get().await?.query_opt("SELECT 1 FROM moria.runtime_evaluator_executions v JOIN moria.runtime_executions e ON e.id=v.execution WHERE v.execution=$1 AND v.evaluation=$2 AND v.case_id=$3 AND v.repeat=$4 AND e.resource=$5 AND e.environment=$6 AND e.principal=$7 AND e.application=$8",&[&evaluator,&root,&name,&repeat,&resource,&session.environment(),&session.principal(),&session.application()]).await?.is_some(),"EVALUATION_PIN_INVALID");
            evaluator
        } else {
            let mut admission =
                Admission::begin(&self.db, session.environment(), false, None).await?;
            let tx = admission.client.as_mut().unwrap().transaction().await?;
            tx.query_one(
                "SELECT moria.runtime_task_check($1,$2,$3)",
                &[&lease.task, &lease.owner, &lease.fence.parse::<i64>()?],
            )
            .await?;
            let root_header = Self::header(tx.client(), &session, root).await?;
            Self::current(tx.client(), &session, &root_header).await?;
            let fields = json!({"inputJson":{"type":"string","nullable":false},"expectedJson":{"type":"string","nullable":false},"actualJson":{"type":"string","nullable":false}});
            let spec = json!({"version":1,"domain":"evaluator","release":release.strip_prefix("sha256:").unwrap(),"resourceClass":"standard","timeoutSeconds":30,"retryPolicy":"never","inputSchema":{"type":"object","properties":fields},"pinnedInputs":[],"expectedHeads":{}});
            let context = json!({"environment":session.environment(),"principal":session.principal(),"application":session.application(),"sessionHash":session.session_hash(),"parentExecution":lease.execution,"parentDeadline":deadline});
            let binding = Self::binding(
                tx.client(),
                &session,
                resource,
                &spec,
                &context,
                &marks,
                &[],
            )
            .await?;
            let outputs = json!({"passed":{"type":"boolean","nullable":false},"score":{"type":"float64","nullable":false},"message":{"type":"string","nullable":false}});
            tx.execute("INSERT INTO moria.runtime_sandbox_bindings(binding,domain,artifact_uri,artifact_digest,outputs,effect,memory_mib,cpu_millis,timeout_seconds) VALUES($1,'evaluator',$2,$3,$4,'readOnly',128,10000,30)",&[&binding,&artifact["uri"].as_str().ok_or_else(||anyhow::anyhow!("EVALUATOR_ARTIFACT_INVALID"))?,&artifact["digest"].as_str().ok_or_else(||anyhow::anyhow!("EVALUATOR_ARTIFACT_INVALID"))?,&outputs]).await?;
            let parsed: execution::ExecutionSpec = serde_json::from_value(spec)?;
            let (normalized, hash) = execution::normalized(&parsed, &input)?;
            let id: Uuid = tx
                .query_one(
                    "SELECT moria.runtime_execution_request($1,$2,$3,$4,$5)",
                    &[
                        &binding,
                        &format!("evaluation/{}/evaluator", lease.execution),
                        &normalized,
                        &hash,
                        &json!([{"key":"evaluator","partition":null}]),
                    ],
                )
                .await?
                .get(0);
            tx.execute("INSERT INTO moria.runtime_evaluator_executions(execution,evaluation,case_id,repeat,markings) VALUES($1,$2,$3,$4,$5) ON CONFLICT(execution) DO NOTHING",&[&id,&root,&name,&repeat,&json!(marks)]).await?;
            tx.execute("UPDATE moria.runtime_evaluation_cases SET evaluator_execution=$2 WHERE execution=$1",&[&lease.execution,&id]).await?;
            tx.query_one(
                "SELECT moria.runtime_task_check($1,$2,$3)",
                &[&lease.task, &lease.owner, &lease.fence.parse::<i64>()?],
            )
            .await?;
            tx.commit().await?;
            admission.release().await?;
            id
        };
        loop {
            let client = self.db.get().await?;
            let row=client.query_one("SELECT e.status,r.result FROM moria.runtime_executions e LEFT JOIN moria.runtime_evaluator_receipts r ON r.execution=e.id WHERE e.id=$1",&[&evaluator]).await?;
            let status: String = row.get(0);
            let released_marks = if ["succeeded", "failed", "cancelled"].contains(&status.as_str())
            {
                Self::evaluator_purpose_in(&client, &session, evaluator).await?;
                Some(union(
                    &[
                        marks.clone(),
                        crate::sandbox::retained_observations_in(&client, evaluator).await?,
                    ],
                    session.environment(),
                )?)
            } else {
                None
            };
            if status == "succeeded" {
                let result: Value = row.get(1);
                return Ok(
                    json!({"outcome":if result["passed"]==true{"pass"}else{"fail"},"score":result["score"],"detail":result["message"],"markings":released_marks.unwrap()}),
                );
            }
            if ["failed", "cancelled"].contains(&status.as_str()) {
                return Ok(
                    json!({"outcome":"error","error":"EVALUATOR_FAILED","markings":released_marks.unwrap()}),
                );
            }
            drop(client);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    async fn publish_control(&self, lease: &Lease, host: Uuid) -> Result<()> {
        let session = self.live(lease, host).await?;
        let root:Uuid=self.db.get().await?.query_one("SELECT COALESCE((SELECT evaluation FROM moria.runtime_evaluation_cases WHERE execution=$1),$1)",&[&lease.execution]).await?.get(0);
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        self.live_in(tx.client(), lease, host).await?;
        let header = Self::header(tx.client(), &session, root).await?;
        let mut marks = Self::current(tx.client(), &session, &header).await?;
        let children = if root == lease.execution {
            tx.query("SELECT agent_execution FROM moria.runtime_evaluation_cases WHERE evaluation=$1 AND agent_execution IS NOT NULL ORDER BY case_id,repeat LIMIT 1001",&[&root]).await?
        } else {
            tx.query("SELECT agent_execution FROM moria.runtime_evaluation_cases WHERE evaluation=$1 AND execution=$2 AND agent_execution IS NOT NULL",&[&root,&lease.execution]).await?
        };
        ensure!(children.len() <= 1000, "EVALUATION_CASE_LIMIT");
        for child in children {
            marks = union(
                &[
                    marks,
                    self.agents.retained_in(&tx, &session, child.get(0)).await?,
                ],
                session.environment(),
            )?;
        }
        self.live_in(tx.client(), lease, host).await?;
        let completion = if root == lease.execution {
            ensure!(header.get::<_, bool>("loaded"), "EVALUATION_NOT_READY");
            tx.execute("UPDATE moria.runtime_evaluation_cases c SET outcome='error',error='CASE_WORKER_LOST',finished_at=clock_timestamp() FROM moria.runtime_executions e WHERE c.evaluation=$1 AND e.id=c.execution AND e.status IN ('failed','cancelled') AND c.outcome IN ('pending','running')",&[&root]).await?;
            let counts=tx.query_one("SELECT count(*)::text,count(*) FILTER(WHERE outcome='pass')::text,count(*) FILTER(WHERE outcome='fail')::text,count(*) FILTER(WHERE outcome='error')::text,avg(score)::float8,stddev_samp(score)::float8,count(*) FILTER(WHERE outcome IN ('pending','running')) FROM moria.runtime_evaluation_cases WHERE evaluation=$1",&[&root]).await?;
            ensure!(counts.get::<_, i64>(6) == 0, "EVALUATION_NOT_READY");
            let summary = json!({"total":counts.get::<_,String>(0),"passed":counts.get::<_,String>(1),"failed":counts.get::<_,String>(2),"errors":counts.get::<_,String>(3),"meanScore":counts.get::<_,Option<f64>>(4),"scoreStandardDeviation":counts.get::<_,Option<f64>>(5)});
            tx.execute(
                "UPDATE moria.runtime_evaluations SET markings=$2 WHERE execution=$1",
                &[&root, &json!(marks)],
            )
            .await?;
            tx.execute("INSERT INTO moria.runtime_evaluation_receipts(execution,summary,summary_hash,markings) VALUES($1,$2,$3,$4)",&[&root,&summary,&digest(&summary)?,&json!(marks)]).await?;
            summary
        } else {
            let result:Value=tx.query_one("SELECT result FROM moria.runtime_execution_tasks WHERE execution=$1 AND status='sealed'",&[&lease.execution]).await?.get(0);
            marks = union(
                &[marks, labels(&result["markings"])?],
                session.environment(),
            )?;
            let outcome = result["outcome"]
                .as_str()
                .filter(|value| ["pass", "fail", "error"].contains(value))
                .ok_or_else(|| anyhow::anyhow!("EVALUATION_RESULT_INVALID"))?;
            tx.execute("UPDATE moria.runtime_evaluation_cases SET outcome=$2,score=$3,detail=$4,error=$5,markings=$6,duration_millis=(SELECT GREATEST(0,floor(extract(epoch FROM (clock_timestamp()-created_at))*1000))::bigint FROM moria.runtime_executions WHERE id=$1),finished_at=clock_timestamp() WHERE execution=$1 AND outcome IN ('pending','running')",&[&lease.execution,&outcome,&result["score"].as_f64(),&result["detail"].as_str(),&result["error"].as_str(),&json!(marks)]).await?;
            result
        };
        self.live_in(tx.client(), lease, host).await?;
        tx.query_one(
            "SELECT moria.runtime_execution_complete($1,$2,$3,$4)",
            &[
                &lease.execution,
                &lease.owner,
                &lease.fence.parse::<i64>()?,
                &completion,
            ],
        )
        .await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(())
    }
    async fn live_in(&self, client: &Client, lease: &Lease, host: Uuid) -> Result<VerifiedSession> {
        execution::job_session_in(client, lease, host).await
    }
    async fn fail_control(&self, lease: &Lease, error: &anyhow::Error) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let changed = if let Some(task) = lease.task {
            tx.execute("UPDATE moria.runtime_executions e SET status='failed',error='EVALUATION_FAILED',finished_at=clock_timestamp(),lease_until=NULL FROM moria.runtime_execution_tasks t WHERE t.id=$1 AND t.execution=e.id AND t.status='running' AND t.owner=$2 AND t.fence=$3 AND t.lease_until>clock_timestamp() AND e.status='running' AND EXISTS(SELECT 1 FROM moria.indexing_instance WHERE incarnation=$4)",&[&task,&lease.owner,&lease.fence.parse::<i64>()?,&lease.incarnation]).await?
        } else {
            tx.execute("UPDATE moria.runtime_executions SET status='failed',error='EVALUATION_FAILED',finished_at=clock_timestamp(),lease_until=NULL WHERE id=$1 AND status='preparing' AND owner=$2 AND fence=$3 AND lease_until>clock_timestamp() AND EXISTS(SELECT 1 FROM moria.indexing_instance WHERE incarnation=$4)",&[&lease.execution,&lease.owner,&lease.fence.parse::<i64>()?,&lease.incarnation]).await?
        };
        if changed > 0 {
            tx.execute("UPDATE moria.runtime_execution_tasks SET status='cancelled',lease_until=NULL WHERE execution=$1 AND status IN ('pending','running')",&[&lease.execution]).await?;
            let sqlstate = error.chain().find_map(|cause| {
                cause
                    .downcast_ref::<tokio_postgres::Error>()
                    .and_then(tokio_postgres::Error::as_db_error)
                    .map(|error| error.code().code())
            });
            tx.query_one("SELECT moria.runtime_execution_event($1,'failed',$2)",&[&lease.execution,&json!({"code":"EVALUATION_FAILED","cause":crate::database::safe_error(error),"sqlstate":sqlstate})]).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn worker_once(
        &self,
        owner: Uuid,
        host: Uuid,
        cancel: watch::Receiver<bool>,
    ) -> Result<bool> {
        let authority = ExecutionAuthority {
            db: self.db.clone(),
        };
        if let Some(claim) = authority
            .claim_registered(owner, host, &[Domain::Evaluation], true, None)
            .await?
        {
            let lease = claim.lease;
            let work = self
                .owned(&lease, host, cancel, self.publish_control(&lease, host))
                .await;
            if let Err(error) = work {
                self.fail_control(&lease, &error).await?;
            }
            return Ok(true);
        }
        let Some(claim) = authority
            .claim_registered(owner, host, &[Domain::Evaluation], false, None)
            .await?
        else {
            return Ok(false);
        };
        let lease = claim.lease;
        let is_root: bool = self
            .db
            .get()
            .await?
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM moria.runtime_evaluations WHERE execution=$1)",
                &[&lease.execution],
            )
            .await?
            .get(0);
        let result = if is_root {
            self.owned(&lease, host, cancel, self.load(&lease, host))
                .await
        } else {
            self.owned(&lease, host, cancel, self.case(&lease, host))
                .await
        };
        match result {
            Ok(result) => {
                authority
                    .seal(&lease, result.clone(), &digest(&result)?)
                    .await?;
            }
            Err(error) => self.fail_control(&lease, &error).await?,
        }
        Ok(true)
    }
    pub async fn evaluator_once(
        &self,
        owner: Uuid,
        host: Uuid,
        cancel: watch::Receiver<bool>,
    ) -> Result<bool> {
        let supervisor = SandboxSupervisor::new(
            self.db.clone(),
            host,
            Arc::new(EvaluatorBroker {
                db: self.db.clone(),
            }),
        )?;
        let authority = ExecutionAuthority {
            db: self.db.clone(),
        };
        if let Some(claim) = authority.claim_evaluator(owner, host, true, None).await? {
            let lease = claim.lease;
            let work = async {
                let context = supervisor.publication_context(lease.clone()).await?;
                let mut admission =
                    Admission::begin(&self.db, context.environment(), false, None).await?;
                let tx = admission.client.as_mut().unwrap().transaction().await?;
                self.live_in(tx.client(), &lease, host).await?;
                context.check_in(tx.client()).await?;
                let result:Value=tx.query_one("SELECT result FROM moria.runtime_execution_tasks WHERE execution=$1 AND status='sealed'",&[&lease.execution]).await?.get(0);
                tx.execute("INSERT INTO moria.runtime_evaluator_receipts(execution,result,result_hash,markings) VALUES($1,$2,$3,$4)",&[&lease.execution,&result,&digest(&result)?,&json!(context.consumed_markings())]).await?;
                context.check_in(tx.client()).await?;
                tx.query_one(
                    "SELECT moria.runtime_execution_complete($1,$2,$3,$4)",
                    &[
                        &lease.execution,
                        &lease.owner,
                        &lease.fence.parse::<i64>()?,
                        &result,
                    ],
                )
                .await?;
                tx.commit().await?;
                admission.release().await?;
                Ok(())
            };
            if let Err(error) = self.owned(&lease, host, cancel, work).await {
                self.fail_control(&lease, &error).await?;
            }
            return Ok(true);
        }
        let Some(claim) = authority.claim_evaluator(owner, host, false, None).await? else {
            return Ok(false);
        };
        let lease = claim.lease;
        if let Err(error) = supervisor.execute(lease.clone(), cancel).await {
            self.fail_control(&lease, &error).await?;
        }
        Ok(true)
    }
}

pub(crate) async fn check_context(client: &Client, context: &SandboxContext) -> Result<()> {
    ensure!(context.domain() == "evaluator", "EVALUATOR_CONTEXT_INVALID");
    let session = Authority::stored_session_in(
        client,
        context
            .session_hash()
            .ok_or_else(|| anyhow::anyhow!("UNAUTHENTICATED"))?,
        context.environment(),
        context.principal(),
        context.application(),
    )
    .await?;
    let root: Uuid = client
        .query_one(
            "SELECT evaluation FROM moria.runtime_evaluator_executions WHERE execution=$1",
            &[&context.lease().execution],
        )
        .await?
        .get(0);
    let header = NativeEvaluations::header(client, &session, root).await?;
    let marks = NativeEvaluations::current(client, &session, &header).await?;
    context.record_consumed_markings(&marks)?;
    context.record_consumed_markings(
        &crate::sandbox::released_source_markings_in(
            client,
            &session,
            header.get("resource"),
            "run",
            context
                .source_release()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_FILE_PROOF_REQUIRED"))?,
        )
        .await?,
    )?;
    let state = Authority::current(client, &session).await?;
    execution::invocation_permission(
        &state.identity,
        header.get("resource"),
        "evaluations",
        "run",
        &context
            .consumed_markings()
            .iter()
            .map(|label| label.marking.clone())
            .collect::<Vec<_>>(),
    )?;
    Ok(())
}

struct EvaluatorBroker {
    db: Database,
}

/// Retained literal input authority follows actual accepted case ancestry, never guest references.
pub(crate) async fn retained_input_in(
    client: &Client,
    session: &VerifiedSession,
    execution: Uuid,
) -> Result<Vec<ConsumedMarking>> {
    let roots=client.query("WITH RECURSIVE ancestors AS(SELECT e.id,0 AS depth FROM moria.runtime_executions e WHERE e.id=$1 AND e.environment=$2 AND e.principal=$3 AND e.application=$4 UNION ALL SELECT d.parent_execution,a.depth+1 FROM ancestors a JOIN moria.runtime_job_delegations d ON d.execution=a.id WHERE d.parent_execution IS NOT NULL AND a.depth<8) SELECT DISTINCT c.evaluation FROM ancestors a JOIN moria.runtime_evaluation_cases c ON c.execution=a.id",&[&execution,&session.environment(),&session.principal(),&session.application()]).await?;
    ensure!(roots.len() <= 8, "EVALUATION_PIN_INVALID");
    let mut groups = vec![];
    for root in roots {
        let header = NativeEvaluations::header(client, session, root.get(0)).await?;
        groups.push(NativeEvaluations::current(client, session, &header).await?);
        groups.push(
            crate::sandbox::released_source_markings_in(
                client,
                session,
                header.get("resource"),
                "run",
                &header.get::<_, String>("release"),
            )
            .await?,
        );
    }
    union(&groups, session.environment())
}
#[async_trait]
impl PlatformBroker for EvaluatorBroker {
    async fn start(&self, context: &SandboxContext) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        context.complete_current_observation_in(tx.client()).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn http(&self, context: &SandboxContext, call: HttpCall) -> Result<HttpResponse> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        context.check_in(tx.client()).await?;
        tx.query_one("SELECT moria.runtime_execution_event($1,'brokerDenied',$2)",&[&context.lease().execution,&json!({"code":"EVALUATOR_OPERATION_DENIED","markings":context.consumed_markings()})]).await?;
        context
            .complete_observation_in(tx.client(), &call.id)
            .await?;
        tx.commit().await?;
        bail!("EVALUATOR_OPERATION_DENIED")
    }
    async fn log(
        &self,
        context: &SandboxContext,
        message: &str,
        marks: &[ConsumedMarking],
    ) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        context.record_consumed_markings(marks)?;
        context.check_in(tx.client()).await?;
        tx.query_one(
            "SELECT moria.runtime_execution_event($1,'log',$2)",
            &[
                &context.lease().execution,
                &json!({"message":message,"markings":context.consumed_markings()}),
            ],
        )
        .await?;
        context.complete_current_observation_in(tx.client()).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn finish(&self, context: &SandboxContext, result: &Value) -> Result<()> {
        context.check().await?;
        ensure!(
            result["passed"].is_boolean()
                && result["score"]
                    .as_f64()
                    .is_some_and(|score| (0.0..=1.0).contains(&score))
                && result["message"]
                    .as_str()
                    .is_some_and(|message| message.len() <= 4096),
            "EVALUATOR_RESULT_INVALID"
        );
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        context.complete_current_observation_in(tx.client()).await?;
        tx.commit().await?;
        Ok(())
    }
}

pub async fn cli(db: Database, arguments: Vec<String>) -> Result<()> {
    let operation = arguments.first().map(String::as_str).unwrap_or("");
    if operation == "install-service" {
        use tokio::io::AsyncReadExt;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Registration {
            environment: String,
            resource: Uuid,
            configuration: Value,
        }
        let mut bytes = vec![];
        tokio::io::stdin()
            .take(65537)
            .read_to_end(&mut bytes)
            .await?;
        ensure!(bytes.len() <= 65536, "EVALUATION_SERVICE_PROFILE_INVALID");
        let registration: Registration = serde_json::from_slice(&bytes)?;
        let mut admission = Admission::begin(&db, &registration.environment, true, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        install_service_in(
            tx.client(),
            &registration.environment,
            registration.resource,
            &registration.configuration,
        )
        .await?;
        tx.commit().await?;
        admission.release().await?;
        println!("{}", json!({"configured":true}));
        return Ok(());
    }
    let service = NativeEvaluations::new(db.clone())?;
    if ["worker", "once", "evaluator-once", "cleanup"].contains(&operation) {
        let owner: Uuid = env::var("MORIA_EVALUATION_WORKER_OWNER")?.parse()?;
        let host: Uuid = env::var("MORIA_EVALUATION_HOST_ID")?.parse()?;
        db.get()
            .await?
            .query_one(
                "SELECT moria.runtime_worker_check($1,$2,NULL,'evaluation')",
                &[&owner, &host],
            )
            .await?;
        db.get()
            .await?
            .query_one(
                "SELECT moria.runtime_worker_check($1,$2,NULL,'evaluator')",
                &[&owner, &host],
            )
            .await?;
        let (cancel, cancelled) = watch::channel(false);
        if operation == "cleanup" {
            let supervisor =
                SandboxSupervisor::new(db.clone(), host, Arc::new(EvaluatorBroker { db }))?;
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
        if operation == "evaluator-once" {
            println!(
                "{}",
                json!({"worked":service.evaluator_once(owner,host,cancelled).await?})
            );
            return Ok(());
        }
        let concurrency = env::var("MORIA_EVALUATION_CONCURRENCY")
            .unwrap_or_else(|_| "4".into())
            .parse::<usize>()?;
        ensure!(
            (2..=8).contains(&concurrency),
            "EVALUATION_WORKER_CONFIG_INVALID"
        );
        let mut control = tokio::task::JoinSet::new();
        let mut evaluators = tokio::task::JoinSet::new();
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        println!(
            "{}",
            json!({"event":"evaluation-worker","owner":owner,"host":host,"controlConcurrency":concurrency,"evaluatorConcurrency":concurrency})
        );
        loop {
            while control.len() < concurrency {
                let service = service.clone();
                let cancelled = cancelled.clone();
                control.spawn(async move {
                    let result = service.worker_once(owner, host, cancelled).await;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    result
                });
            }
            while evaluators.len() < concurrency {
                let service = service.clone();
                let cancelled = cancelled.clone();
                evaluators.spawn(async move {
                    let result = service.evaluator_once(owner, host, cancelled).await;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    result
                });
            }
            tokio::select! {_=terminate.recv()=>break,_=tokio::signal::ctrl_c()=>break,result=control.join_next()=>{if let Some(result)=result{result??;}},result=evaluators.join_next()=>{if let Some(result)=result{result??;}}}
        }
        cancel.send(true)?;
        while let Some(result) = control.join_next().await {
            result??;
        }
        while let Some(result) = evaluators.join_next().await {
            result??;
        }
        return Ok(());
    }
    use tokio::io::{AsyncReadExt, stdin};
    let mut bytes = Vec::new();
    stdin().take(65537).read_to_end(&mut bytes).await?;
    ensure!(bytes.len() <= 65536, "EVALUATION_REQUEST_INVALID");
    let value: Value = serde_json::from_slice(&bytes)?;
    let session = Authority { db }
        .authenticate(
            value["token"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("UNAUTHENTICATED"))?,
        )
        .await?;
    let id = || -> Result<Uuid> {
        Ok(value["id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("EVALUATION_REQUEST_INVALID"))?
            .parse()?)
    };
    let output = match operation {
        "invoke" => {
            service
                .invoke(
                    &session,
                    value["resource"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("EVALUATION_REQUEST_INVALID"))?
                        .parse()?,
                    value["key"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("EVALUATION_REQUEST_INVALID"))?,
                    json!({}),
                )
                .await?
        }
        "get" => service.get(&session, id()?).await?,
        "cases" => {
            service
                .cases(&session, id()?, value["after"].as_str())
                .await?
        }
        "events" => {
            service
                .events(&session, id()?, value["after"].as_str().unwrap_or("0"))
                .await?
        }
        "cancel" => service.cancel(&session, id()?).await?,
        "compare" => {
            service
                .compare(
                    &session,
                    id()?,
                    value["other"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("EVALUATION_REQUEST_INVALID"))?
                        .parse()?,
                )
                .await?
        }
        _ => bail!("EVALUATION_OPERATION_INVALID"),
    };
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}
