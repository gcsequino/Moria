//! Native bounded Sync/Transform orchestration. Python is confined to the engine guest.
use crate::{
    authority::{Authority, VerifiedSession},
    authorization::authorize,
    canonical,
    connector_discovery::Capability,
    database::Database,
    execution::{ExecutionAuthority, ExecutionSpec, Lease},
    functions::Admission,
    jobs::RegisteredService,
    sandbox::{
        ConsumedMarking, HttpCall, HttpResponse, PlatformBroker, SandboxContext, SandboxSupervisor,
    },
    storage::IndexNamespace,
};
use crate::{
    engine_io::{EngineIoLimits, EngineOutput, NativeEngineIo},
    storage::{Cancellation, SealedObject},
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, watch};
use tokio_postgres::Client;
use uuid::Uuid;

pub(crate) mod discovery;

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
        "FORBIDDEN",
        "UNAUTHENTICATED",
        "IDENTITY_STALE",
        "BINDING_CHANGED",
        "AUTHORIZATION_CHANGED",
        "PIPELINE_PROFILE_INVALID",
        "PIPELINE_SOURCE_INVALID",
        "PIPELINE_CAPABILITY_UNSUPPORTED",
        "PIPELINE_PROFILE_CONFLICT",
        "PIPELINE_WORKER_UNAVAILABLE",
        "PIPELINE_SCHEMA_UNSUPPORTED",
        "PIPELINE_DISCOVERY_INVALID",
        "PIPELINE_REFERENCE_INVALID",
        "PIPELINE_INPUT_KIND",
        "PIPELINE_MARKINGS_INVALID",
        "PIPELINE_KEY_INVALID",
        "PIPELINE_PROFILE_REQUIRED",
        "PIPELINE_LIMIT_INVALID",
        "PIPELINE_CONFIGURATION_INVALID",
        "PIPELINE_OUTPUT_MISSING",
        "PIPELINE_BINDING_INVALID",
        "PIPELINE_CONTEXT_INVALID",
        "PIPELINE_FRAME_LIMIT",
        "PIPELINE_REQUEST_INVALID",
        "PIPELINE_OPERATION_DENIED",
        "PIPELINE_NOT_STARTED",
        "PIPELINE_ALREADY_STARTED",
        "PIPELINE_LOG_LIMIT",
        "PIPELINE_OUTPUT_INVALID",
        "PIPELINE_CHECKPOINT_REQUIRED",
        "PIPELINE_WORKER_CONFIG_INVALID",
        "PIPELINE_PROFILE_REVOKED",
        "PIPELINE_DESTINATION_REVOKED",
        "PIPELINE_DESTINATION_INVALID",
        "PIPELINE_HTTP_FAILED",
        "PIPELINE_RESPONSE_INVALID",
        "PIPELINE_FAILED",
        "PIPELINE_PUBLICATION_FAILED",
    ]
    .into_iter()
    .find(|code| message == *code)
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EngineLimits {
    timeout_seconds: u32,
    #[serde(rename = "memoryMiB")]
    memory_mib: u32,
    cpu_millis: u32,
    input_bytes: u64,
    output_bytes: u64,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Output {
    namespace: IndexNamespace,
    schema: Value,
    #[serde(
        default,
        rename = "mutationBudget",
        skip_serializing_if = "Option::is_none"
    )]
    mutation_budget: Option<crate::dataset_writer::MutationBudget>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Profile {
    service_authority: Uuid,
    service_binding_revision: String,
    resource_class: String,
    hard_spill: bool,
    adapter: Value,
    outputs: BTreeMap<String, Output>,
    limits: EngineLimits,
    connector: Option<Uuid>,
}
fn profile(value: &Value) -> Result<Profile> {
    let result: Profile = serde_json::from_value(value.clone())?;
    ensure!(
        result.hard_spill
            && crate::execution::token(&result.resource_class, 64)
            && !result.service_binding_revision.is_empty()
            && result.service_binding_revision.len() <= 128
            && (1..=86400).contains(&result.limits.timeout_seconds)
            && (256..=4096).contains(&result.limits.memory_mib)
            && (1..=86_400_000).contains(&result.limits.cpu_millis)
            && (1_048_576..=1_099_511_627_776).contains(&result.limits.input_bytes)
            && (1_048_576..=1_099_511_627_776).contains(&result.limits.output_bytes)
            && (1..=64).contains(&result.outputs.len()),
        "PIPELINE_PROFILE_INVALID"
    );
    crate::sandbox::artifact_image(&result.adapter)?;
    for output in result.outputs.values() {
        if let Some(budget) = &output.mutation_budget {
            budget.validate()?;
            ensure!(
                budget.max_input_bytes <= result.limits.output_bytes
                    && budget.max_changed_logical_bytes <= result.limits.output_bytes
                    && budget.max_compaction_bytes <= result.limits.input_bytes,
                "DATASET_MUTATION_BUDGET_INVALID"
            );
        }
        let _: iceberg::spec::Schema = serde_json::from_value(output.schema.clone())?;
        let columns = crate::snapshot::columns(&output.schema)?;
        let scalars = [
            "boolean",
            "int64",
            "float64",
            "string",
            "date",
            "timestamp",
            "decimal",
            "uid",
        ];
        ensure!(
            !columns.is_empty()
                && columns.values().all(|value| scalars
                    .contains(&value["type"].as_str().unwrap_or(""))
                    || (value["type"] == "array"
                        && value["items"]["nullable"] == false
                        && scalars.contains(&value["items"]["type"].as_str().unwrap_or("")))),
            "PIPELINE_SCHEMA_UNSUPPORTED"
        );
    }
    Ok(result)
}

/// Called only by trusted release composition in its existing metadata transaction.
pub(crate) async fn install_in(
    client: &Client,
    environment: &str,
    resource: Uuid,
    revision: &str,
    value: &Value,
) -> Result<()> {
    let profile = profile(value)?;
    let (_, release, declaration) = source_in(client, environment, resource).await?;
    let domain = match declaration["kind"].as_str() {
        Some("Transform") => "transform",
        Some("Connection") => "sync",
        _ => anyhow::bail!("PIPELINE_SOURCE_INVALID"),
    };
    ensure!(
        domain == "sync" || profile.connector.is_none(),
        "PIPELINE_PROFILE_INVALID"
    );
    program(client, environment, &release, &declaration, &profile, None).await?;
    let service = RegisteredService::load_current_in(
        client,
        environment,
        resource,
        domain,
        &profile.service_binding_revision,
    )
    .await?;
    ensure!(
        service.id() == profile.service_authority
            && service.resource_class() == profile.resource_class,
        "PIPELINE_PROFILE_INVALID"
    );
    if let Some(id) = profile.connector {
        let destination = destination_in(client, environment, resource, id).await?;
        connector_requests_in(client, resource, &release, &declaration, Some(&destination)).await?;
    }
    for (reference, output) in &profile.outputs {
        let id = reference_in(client, &release, reference).await?;
        let (_, _, dataset) = source_in(client, environment, id).await?;
        ensure!(
            dataset["kind"] == "Dataset"
                && normalized_columns(&dataset["spec"]["columns"])?
                    == serde_json::to_value(crate::snapshot::columns(&output.schema)?)?,
            "PIPELINE_SCHEMA_UNSUPPORTED"
        );
        service
            .resource_labels_in(client, &id.to_string(), "read")
            .await?;
        service
            .resource_labels_in(client, &id.to_string(), "write")
            .await?;
        client.execute("INSERT INTO moria.dataset_heads(environment,dataset,revision) VALUES($1,$2,0) ON CONFLICT DO NOTHING",&[&environment,&id.to_string()]).await?;
        ensure!(client.query_opt("SELECT 1 FROM moria.native_storage_namespaces WHERE environment=$1 AND resource=$2 AND warehouse_id=$3 AND table_id=$4 AND revision=$5 AND enabled AND purpose='dataset-source'",&[&environment,&id.to_string(),&output.namespace.warehouse_id,&output.namespace.table_id,&output.namespace.revision]).await?.is_some(),"STORAGE_NAMESPACE_REVOKED");
    }
    if let Some(existing)=client.query_opt("SELECT id,profile,enabled FROM moria.runtime_pipeline_profiles WHERE environment=$1 AND resource=$2 AND revision=$3",&[&environment,&resource,&revision]).await? {
        ensure!(existing.get::<_,Value>("profile")==*value,"PIPELINE_PROFILE_CONFLICT");
        if !existing.get::<_,bool>("enabled") {
            client.execute("UPDATE moria.runtime_pipeline_profiles SET enabled=false WHERE environment=$1 AND resource=$2 AND enabled",&[&environment,&resource]).await?;
            client.execute("UPDATE moria.runtime_pipeline_profiles SET enabled=true WHERE id=$1",&[&existing.get::<_,Uuid>("id")]).await?;
        }
        return Ok(());
    }
    client.execute("UPDATE moria.runtime_pipeline_profiles SET enabled=false WHERE environment=$1 AND resource=$2 AND enabled",&[&environment,&resource]).await?;
    client.execute("INSERT INTO moria.runtime_pipeline_profiles(environment,resource,revision,domain,service_authority,service_revision,resource_class,profile) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",&[&environment,&resource,&revision,&domain,&profile.service_authority,&profile.service_binding_revision,&profile.resource_class,&value]).await?;
    Ok(())
}

/// Metadata availability only. Artifact presence, IO and physical capacity are
/// independently verified on the actual worker before a claim starts code.
pub(crate) async fn source_readiness_in(
    client: &Client,
    environment: &str,
    resource: Uuid,
) -> Result<Value> {
    let Some(row)=client.query_opt("SELECT profile,domain FROM moria.runtime_pipeline_profiles WHERE environment=$1 AND resource=$2 AND enabled FOR SHARE",&[&environment,&resource]).await? else{return Ok(json!({"configured":false,"purposeReady":false,"workerReady":false}))};
    let profile = profile(&row.get::<_, Value>("profile"))?;
    let domain: String = row.get("domain");
    let service = RegisteredService::load_current_in(
        client,
        environment,
        resource,
        &domain,
        &profile.service_binding_revision,
    )
    .await?;
    ensure!(
        service.id() == profile.service_authority
            && service.resource_class() == profile.resource_class,
        "PIPELINE_PROFILE_INVALID"
    );
    let (_, release, definition) = source_in(client, environment, resource).await?;
    let program = program(client, environment, &release, &definition, &profile, None).await?;
    service
        .resource_labels_in(
            client,
            &resource.to_string(),
            if domain == "transform" {
                "build"
            } else {
                "sync"
            },
        )
        .await?;
    if let Some(id) = profile.connector {
        let destination = destination_in(client, environment, resource, id).await?;
        for request in
            connector_requests_in(client, resource, &release, &definition, Some(&destination))
                .await?
        {
            crate::secrets::grant_registered_in(client, &service, &request).await?;
        }
    }
    for (name, output) in &profile.outputs {
        let id = reference_in(client, &release, name).await?;
        let (_, _, dataset) = source_in(client, environment, id).await?;
        ensure!(
            dataset["kind"] == "Dataset"
                && normalized_columns(&dataset["spec"]["columns"])?
                    == serde_json::to_value(crate::snapshot::columns(&output.schema)?)?,
            "PIPELINE_SCHEMA_UNSUPPORTED"
        );
        service
            .resource_labels_in(client, &id.to_string(), "read")
            .await?;
        service
            .resource_labels_in(client, &id.to_string(), "write")
            .await?;
        ensure!(client.query_opt("SELECT 1 FROM moria.native_storage_namespaces WHERE environment=$1 AND resource=$2 AND warehouse_id=$3 AND table_id=$4 AND revision=$5 AND enabled AND purpose='dataset-source'",&[&environment,&id.to_string(),&output.namespace.warehouse_id,&output.namespace.table_id,&output.namespace.revision]).await?.is_some(),"STORAGE_NAMESPACE_REVOKED");
    }
    let worker:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_workers w JOIN moria.indexing_instance i ON i.incarnation=w.incarnation JOIN moria.runtime_resource_classes c ON c.name=$1 AND c.enabled WHERE w.enabled AND w.expires_at>clock_timestamp() AND $1=ANY(w.resource_classes) AND $2=ANY(w.domains) AND EXISTS(SELECT 1 FROM moria.runtime_engine_scratch_slots s WHERE s.host_id=w.host_id AND s.resource_class=$1 AND s.incarnation=i.incarnation AND s.enabled))",&[&profile.resource_class,&domain]).await?.get(0);
    Ok(
        json!({"configured":true,"purposeReady":true,"workerReady":worker,"resourceClass":profile.resource_class,"artifacts":[profile.adapter,program.artifact]}),
    )
}

fn markings(environment: &str, values: &BTreeSet<String>) -> Vec<ConsumedMarking> {
    values
        .iter()
        .map(|marking| ConsumedMarking {
            environment: environment.into(),
            marking: marking.clone(),
        })
        .collect()
}

pub(crate) async fn check_context(client: &Client, context: &SandboxContext) -> Result<()> {
    client
        .query_one(
            "SELECT moria.runtime_pipeline_current($1)",
            &[&context.lease().execution],
        )
        .await?;
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
    let row=client.query_one("SELECT e.resource,p.inputs,p.outputs,p.secret_approvals FROM moria.runtime_executions e JOIN moria.runtime_pipeline_executions p ON p.execution=e.id WHERE e.id=$1",&[&context.lease().execution]).await?;
    let mut labels =
        accepted_secrets_in(client, &session, &row.get::<_, Value>("secret_approvals")).await?;
    labels.extend(
        Authority::resource_access(client, &session, &row.get::<_, Uuid>(0).to_string(), "read")
            .await?,
    );
    for (field, operation) in [("inputs", "read"), ("outputs", "write")] {
        let resources: Value = row.get(field);
        for resource in resources
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))?
            .values()
        {
            labels.extend(
                Authority::resource_access(
                    client,
                    &session,
                    resource["resource"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))?,
                    operation,
                )
                .await?,
            );
        }
    }
    context.record_consumed_markings(&markings(context.environment(), &labels))?;
    Ok(())
}

async fn source_in(
    client: &Client,
    environment: &str,
    resource: Uuid,
) -> Result<(Uuid, String, Value)> {
    crate::filesystem::NativeSources::active_definition_in(client, environment, resource)
        .await?
        .ok_or_else(|| anyhow::anyhow!("PIPELINE_SOURCE_INVALID"))
}

async fn reference_in(client: &Client, release: &str, reference: &str) -> Result<Uuid> {
    Ok(
        crate::filesystem::NativeSources::resource_in(client, release, reference)
            .await?
            .parse()?,
    )
}
fn normalized_columns(value: &Value) -> Result<Value> {
    let mut result = serde_json::Map::new();
    for (name, value) in value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("PIPELINE_SCHEMA_UNSUPPORTED"))?
    {
        result.insert(
            name.clone(),
            crate::values::descriptor(value, &Value::Null, 0)?,
        );
    }
    Ok(Value::Object(result))
}
struct Program {
    artifact: Value,
    inputs: Vec<String>,
    outputs: Vec<String>,
    configuration: Value,
    timeout: i32,
    memory: i32,
    cpu: i32,
    stream: Option<String>,
    dependency: Option<Uuid>,
}

// Cursor identity describes source interpretation and the physical sink. Lease,
// approval and release generations fence a window without resetting its cursor.
fn checkpoint_revision(
    program: &Program,
    definition: &Value,
    adapter: &Value,
    outputs: &Value,
    destination: Option<&DestinationDefinition>,
    materials: &[Value],
) -> Result<String> {
    let destination = destination
        .map(|destination| {
            Ok::<_, anyhow::Error>(json!({
                "endpoint": destination.endpoint()?.as_str(),
                "methods": destination.methods.iter().collect::<BTreeSet<_>>(),
                "paths": destination.paths.iter().collect::<BTreeSet<_>>(),
                "legacyMaterialRef": if materials.is_empty() { Some(&destination.secret_ref) } else { None },
            }))
        })
        .transpose()?;
    canonical::fingerprint(&json!({
        "version": 1,
        "abi": "connector/v1",
        "implementation": program.dependency,
        "artifact": program.artifact,
        "adapter": adapter,
        "configuration": program.configuration,
        "stream": program.stream,
        "mode": definition["spec"]["mode"],
        "coverage": definition["spec"]["coverage"],
        "outputs": outputs,
        "destination": destination,
        "materials": materials,
    }))
}
async fn program(
    client: &Client,
    environment: &str,
    release: &str,
    definition: &Value,
    profile: &Profile,
    capability: Option<Capability>,
) -> Result<Program> {
    let spec = &definition["spec"];
    let transform = definition["kind"] == "Transform";
    let (artifact, inputs, outputs, configuration, limits, stream, dependency) = if transform {
        ensure!(
            spec["runtime"] == "python-duckdb-v1"
                && profile.connector.is_none()
                && capability != Some(Capability::SchemaDiscovery),
            "PIPELINE_CAPABILITY_UNSUPPORTED"
        );
        (
            spec["artifact"].clone(),
            serde_json::from_value::<Vec<String>>(spec["inputs"].clone())?,
            serde_json::from_value::<Vec<String>>(spec["outputs"].clone())?,
            json!({}),
            spec["limits"].clone(),
            None,
            None,
        )
    } else {
        ensure!(
            definition["kind"] == "Connection"
                && spec["secrets"]
                    .as_array()
                    .is_some_and(|values| values.len() <= 8)
                && profile.connector.is_some(),
            "PIPELINE_CAPABILITY_UNSUPPORTED"
        );
        let sparse_approved = !profile.outputs.is_empty()
            && profile
                .outputs
                .values()
                .all(|output| output.mutation_budget.is_some());
        if capability == Some(Capability::BatchRead) {
            let (mode, _) = crate::dataset_writer::accepted_semantics(Some(definition))?;
            ensure!(
                mode == "replace" || sparse_approved,
                "DATASET_MUTATION_BUDGET_REQUIRED"
            );
        }
        let implementation_id = reference_in(
            client,
            release,
            spec["implementation"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("PIPELINE_SOURCE_INVALID"))?,
        )
        .await?;
        let (_, _, implementation) = source_in(client, environment, implementation_id).await?;
        ensure!(
            implementation["kind"] == "ConnectorImplementation"
                && implementation["spec"]["abiVersion"] == "connector/v1"
                && crate::connector_discovery::capability_supported(
                    &implementation["spec"]["capabilities"],
                    &spec["mode"],
                    &spec["coverage"],
                    capability,
                    sparse_approved,
                ),
            "PIPELINE_CAPABILITY_UNSUPPORTED"
        );
        crate::filesystem::validate_configuration(
            &implementation["spec"]["configurationSchema"],
            &spec["configuration"],
        )
        .map_err(|_| anyhow::anyhow!("PIPELINE_CONFIGURATION_INVALID"))?;
        (
            implementation["spec"]["artifact"].clone(),
            Vec::new(),
            vec![
                spec["output"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("PIPELINE_SOURCE_INVALID"))?
                    .into(),
            ],
            spec["configuration"].clone(),
            json!({"timeoutSeconds":profile.limits.timeout_seconds,"memoryMiB":profile.limits.memory_mib,"cpuMillis":profile.limits.cpu_millis}),
            Some(
                spec["stream"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("PIPELINE_SOURCE_INVALID"))?
                    .to_owned(),
            ),
            Some(implementation_id),
        )
    };
    let timeout = limits["timeoutSeconds"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("PIPELINE_LIMIT_INVALID"))?;
    let memory = limits["memoryMiB"].as_u64().unwrap_or(512);
    let cpu = limits["cpuMillis"].as_u64().unwrap_or(60000);
    ensure!(
        timeout > 0
            && timeout <= u64::from(profile.limits.timeout_seconds)
            && memory >= 256
            && memory <= u64::from(profile.limits.memory_mib)
            && cpu > 0
            && cpu <= u64::from(profile.limits.cpu_millis),
        "PIPELINE_LIMIT_INVALID"
    );
    ensure!(
        inputs.len() <= 64
            && !outputs.is_empty()
            && outputs.len() <= 64
            && outputs.iter().cloned().collect::<BTreeSet<_>>()
                == profile.outputs.keys().cloned().collect(),
        "PIPELINE_PROFILE_INVALID"
    );
    crate::sandbox::artifact_image(&artifact)?;
    Ok(Program {
        artifact,
        inputs,
        outputs: if capability == Some(Capability::SchemaDiscovery) {
            Vec::new()
        } else {
            outputs
        },
        configuration,
        timeout: timeout as i32,
        memory: memory as i32,
        cpu: cpu as i32,
        stream,
        dependency,
    })
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcceptedSecret {
    request: crate::secrets::SecretRequest,
    approval: crate::secrets::SecretApprovalRef,
    material_revision: String,
}

async fn connector_requests_in(
    client: &Client,
    resource: Uuid,
    release: &str,
    definition: &Value,
    destination: Option<&DestinationDefinition>,
) -> Result<Vec<crate::secrets::SecretRequest>> {
    if definition["kind"] != "Connection" {
        return Ok(Vec::new());
    }
    let values = definition["spec"]["secrets"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("PIPELINE_SOURCE_INVALID"))?;
    ensure!(values.len() <= 8, "PIPELINE_CAPABILITY_UNSUPPORTED");
    let mut secrets = BTreeSet::new();
    for value in values {
        let secret = reference_in(
            client,
            release,
            value
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("PIPELINE_SOURCE_INVALID"))?,
        )
        .await?;
        ensure!(secrets.insert(secret), "SECRET_BINDING_INVALID");
    }
    let destination = destination.ok_or_else(|| anyhow::anyhow!("PIPELINE_DESTINATION_INVALID"))?;
    let credentials = destination.credentials_for(&secrets)?;
    let fingerprint = destination.fingerprint()?;
    Ok(credentials
        .into_iter()
        .map(|credential| crate::secrets::SecretRequest {
            release: release.into(),
            consumer: resource,
            secret: credential.secret,
            owner_kind: crate::secrets::OwnerKind::Connector,
            owner_profile: destination.id,
            owner_revision: destination.revision.clone(),
            destination_fingerprint: fingerprint.clone(),
            material_ref: credential.material_ref,
        })
        .collect())
}

pub(crate) async fn inspect_connector_in(
    client: &Client,
    environment: &str,
    resource: Uuid,
) -> Result<Value> {
    let (_, release, definition) = source_in(client, environment, resource).await?;
    ensure!(
        definition["kind"] == "Connection",
        "PIPELINE_SOURCE_INVALID"
    );
    let profile: Value = client.query_opt("SELECT profile FROM moria.runtime_pipeline_profiles WHERE environment=$1 AND resource=$2 AND domain='sync' AND enabled", &[&environment,&resource]).await?
        .ok_or_else(|| anyhow::anyhow!("PIPELINE_PROFILE_REQUIRED"))?.get(0);
    let profile = self::profile(&profile)?;
    let destination = destination_in(
        client,
        environment,
        resource,
        profile
            .connector
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_DESTINATION_INVALID"))?,
    )
    .await?;
    Ok(
        json!({"bindings":connector_requests_in(client, resource, &release, &definition, Some(&destination)).await?}),
    )
}

async fn accepted_secrets_in(
    client: &Client,
    session: &VerifiedSession,
    values: &Value,
) -> Result<BTreeSet<String>> {
    let accepted: Vec<AcceptedSecret> = serde_json::from_value(values.clone())?;
    ensure!(accepted.len() <= 8, "SECRET_BINDING_INVALID");
    let mut labels = BTreeSet::new();
    for accepted in accepted {
        let grant = crate::secrets::grant_in(client, session, &accepted.request).await?;
        ensure!(
            grant.snapshot() == accepted.approval
                && grant.material_revision() == accepted.material_revision,
            "SECRET_APPROVAL_REVOKED"
        );
        labels.extend(grant.markings().iter().cloned());
    }
    Ok(labels)
}

#[derive(Clone)]
pub struct NativePipelines {
    db: Database,
}
impl NativePipelines {
    pub fn new(db: Database) -> Self {
        Self { db }
    }
    pub async fn invoke(
        &self,
        session: &VerifiedSession,
        resource: Uuid,
        key: &str,
    ) -> Result<Value> {
        self.invoke_inner(session, resource, key, Capability::BatchRead)
            .await
    }
    pub async fn discover(
        &self,
        session: &VerifiedSession,
        resource: Uuid,
        key: &str,
    ) -> Result<Value> {
        self.invoke_inner(session, resource, key, Capability::SchemaDiscovery)
            .await
    }
    async fn invoke_inner(
        &self,
        session: &VerifiedSession,
        resource: Uuid,
        key: &str,
        capability: Capability,
    ) -> Result<Value> {
        ensure!(
            !key.is_empty()
                && key.len() <= 128
                && key
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.:/-".contains(&b)),
            "PIPELINE_KEY_INVALID"
        );
        let request_key = canonical::fingerprint(&json!([
            session.environment(),
            session.principal(),
            session.application(),
            resource,
            key
        ]))?;
        let request_key = if capability == Capability::SchemaDiscovery {
            canonical::fingerprint(&json!([request_key, capability.as_str()]))?
        } else {
            request_key
        };
        let lock = format!("moria/pipeline/request/{request_key}");
        // The provenance prepass mutates authority. Take exclusive admission before BEGIN.
        let mut admission =
            Admission::begin(&self.db, session.environment(), true, Some(&lock)).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        let state = Authority::current(&*tx, session).await?;
        let (entry, release, definition) =
            source_in(tx.client(), session.environment(), resource).await?;
        let domain = match definition["kind"].as_str() {
            Some("Transform") => "transform",
            Some("Connection") => "sync",
            _ => anyhow::bail!("PIPELINE_SOURCE_INVALID"),
        };
        ensure!(
            capability != Capability::SchemaDiscovery || domain == "sync",
            "PIPELINE_CAPABILITY_UNSUPPORTED"
        );
        let operation = if domain == "transform" {
            "build"
        } else {
            "sync"
        };
        let caller_marks = Authority::resource_access_for_operation(
            tx.client(),
            session,
            &resource.to_string(),
            operation,
        )
        .await?;
        authorize(
            &state.identity,
            &resource.to_string(),
            operation,
            &caller_marks,
        )?;
        if let Some(row)=tx.query_opt("SELECT e.id FROM moria.runtime_executions e JOIN moria.runtime_pipeline_executions p ON p.execution=e.id WHERE e.environment=$1 AND e.resource=$2 AND e.request_key=$3 AND p.requester->>'principal'=$4 AND p.requester->>'application'=$5",&[&session.environment(),&resource,&request_key,&session.principal(),&session.application()]).await? {
            let result=self.public_in(tx.client(),session,row.get(0)).await?;
            tx.commit().await?;admission.release().await?;return Ok(result);
        }
        let registered=tx.query_opt("SELECT id,profile FROM moria.runtime_pipeline_profiles WHERE environment=$1 AND resource=$2 AND enabled AND domain=$3 FOR SHARE",&[&session.environment(),&resource,&domain]).await?.ok_or_else(||anyhow::anyhow!("PIPELINE_PROFILE_REQUIRED"))?;
        let profile = profile(&registered.get::<_, Value>("profile"))?;
        let service = RegisteredService::load_current_in(
            tx.client(),
            session.environment(),
            resource,
            domain,
            &profile.service_binding_revision,
        )
        .await?;
        ensure!(
            service.id() == profile.service_authority
                && service.resource_class() == profile.resource_class,
            "PIPELINE_PROFILE_INVALID"
        );
        let program = program(
            tx.client(),
            session.environment(),
            &release,
            &definition,
            &profile,
            Some(capability),
        )
        .await?;
        let destination = if let Some(id) = profile.connector {
            Some(destination_in(tx.client(), session.environment(), resource, id).await?)
        } else {
            None
        };
        let destination_fingerprint = destination
            .as_ref()
            .map(DestinationDefinition::fingerprint)
            .transpose()?;
        let mut input_ids = BTreeMap::new();
        let mut output_ids = BTreeMap::new();
        let mut labels = BTreeSet::new();
        let mut accepted_secrets = Vec::new();
        let mut material_semantics = Vec::new();
        for request in connector_requests_in(
            tx.client(),
            resource,
            &release,
            &definition,
            destination.as_ref(),
        )
        .await?
        {
            let caller = crate::secrets::grant_in(tx.client(), session, &request).await?;
            let service_grant =
                crate::secrets::grant_registered_in(tx.client(), &service, &request).await?;
            ensure!(
                caller.snapshot() == service_grant.snapshot(),
                "SECRET_APPROVAL_REVOKED"
            );
            labels.extend(caller.markings().iter().cloned());
            labels.extend(service_grant.markings().iter().cloned());
            let destination = destination.as_ref().unwrap();
            let credential = destination
                .credentials
                .iter()
                .find(|slot| slot.secret == request.secret)
                .cloned()
                .unwrap_or_else(|| Credential {
                    secret: request.secret,
                    material_ref: destination.secret_ref.clone(),
                    header: "authorization".into(),
                    scheme: CredentialScheme::Bearer,
                });
            material_semantics.push(json!({"secret":request.secret,"materialRevision":service_grant.material_revision(),"header":credential.header,"scheme":credential.scheme}));
            accepted_secrets.push(AcceptedSecret {
                request,
                approval: service_grant.snapshot(),
                material_revision: service_grant.material_revision().into(),
            });
        }
        if let Some(dependency) = program.dependency {
            labels.extend(
                service
                    .resource_labels_in(tx.client(), &dependency.to_string(), "read")
                    .await?,
            );
        }
        if let Some(destination) = &destination {
            labels.extend(destination.markings.clone());
        }
        labels.extend(
            service
                .resource_labels_in(tx.client(), &resource.to_string(), "read")
                .await?,
        );
        labels.extend(
            service
                .resource_labels_in(tx.client(), &resource.to_string(), operation)
                .await?,
        );
        for (reference, ids, operations) in [
            (&program.inputs, &mut input_ids, vec!["read"]),
            (&program.outputs, &mut output_ids, vec!["read", "write"]),
        ] {
            for name in reference {
                let id = reference_in(tx.client(), &release, name).await?;
                let (_, _, dataset) = source_in(tx.client(), session.environment(), id).await?;
                ensure!(dataset["kind"] == "Dataset", "PIPELINE_INPUT_KIND");
                for operation in &operations {
                    labels.extend(
                        service
                            .resource_labels_in(tx.client(), &id.to_string(), operation)
                            .await?,
                    );
                }
                ids.insert(name.clone(), id);
            }
        }
        ensure!(
            input_ids.len() == program.inputs.len() && output_ids.len() == program.outputs.len(),
            "PIPELINE_REFERENCE_INVALID"
        );
        for output in output_ids.values() {
            tx.execute("INSERT INTO moria.dataset_heads(environment,dataset,revision) VALUES($1,$2,0) ON CONFLICT DO NOTHING",&[&session.environment(),&output.to_string()]).await?;
            let admitted: bool = tx
                .query_one(
                    "SELECT $3::text[]<@moria.runtime_data_labels($1,$2)",
                    &[
                        &session.environment(),
                        &output.to_string(),
                        &labels.iter().cloned().collect::<Vec<_>>(),
                    ],
                )
                .await?
                .get(0);
            ensure!(admitted, "MARKING_DOWNGRADE");
        }
        if !input_ids.is_empty() {
            tx.query_one(
                "SELECT moria.runtime_service_dataset_edges($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                &[
                    &service.id(),
                    &session.environment(),
                    &resource,
                    &domain,
                    &profile.service_binding_revision,
                    &profile.resource_class,
                    &input_ids.values().copied().collect::<Vec<_>>(),
                    &output_ids.values().copied().collect::<Vec<_>>(),
                    &labels.iter().cloned().collect::<Vec<_>>(),
                ],
            )
            .await?;
        }
        let mut inputs = serde_json::Map::new();
        let mut outputs = serde_json::Map::new();
        let mut checkpoint_outputs = serde_json::Map::new();
        let mut pins = Vec::new();
        let mut heads = BTreeMap::new();
        for (name, id) in input_ids {
            let pin = crate::catalog::resolve_dataset_pin_registered_in(
                tx.client(),
                &service,
                &id.to_string(),
            )
            .await?;
            labels.extend(pin.markings.clone());
            pins.push(pin.execution_input());
            inputs.insert(name, json!({"resource":id,"pin":pin}));
        }
        let (write_mode, _) = crate::dataset_writer::accepted_semantics(Some(&definition))?;
        for (name, id) in output_ids {
            let output = &profile.outputs[&name];
            let (_, _, dataset) = source_in(tx.client(), session.environment(), id).await?;
            ensure!(
                dataset["spec"]["writeModes"]
                    .as_array()
                    .is_some_and(|modes| modes.contains(&json!(write_mode)))
                    && (write_mode != "replace"
                        || normalized_columns(&dataset["spec"]["columns"])?
                            == serde_json::to_value(crate::snapshot::columns(&output.schema)?)?),
                "PIPELINE_SCHEMA_UNSUPPORTED"
            );
            ensure!(tx.query_opt("SELECT 1 FROM moria.native_storage_namespaces WHERE environment=$1 AND resource=$2 AND warehouse_id=$3 AND table_id=$4 AND revision=$5 AND enabled AND purpose='dataset-source'",&[&session.environment(),&id.to_string(),&output.namespace.warehouse_id,&output.namespace.table_id,&output.namespace.revision]).await?.is_some(),"STORAGE_NAMESPACE_REVOKED");
            let physical: Value = tx.query_one("SELECT jsonb_build_object('resource',b.resource,'physicalBindingFingerprint',b.physical_binding_fingerprint,'namespace',jsonb_build_object('warehouseId',b.warehouse_id,'tableId',b.table_id,'revision',b.namespace_revision),'icebergUuid',n.iceberg_uuid,'columns',b.columns,'icebergSchema',b.iceberg_schema,'primaryKey',to_jsonb(b.primary_key),'keySchemaHash',b.key_schema_hash,'writeModes',(SELECT jsonb_agg(mode ORDER BY mode COLLATE \"C\") FROM unnest(b.write_modes) mode)) FROM moria.native_dataset_bindings b JOIN moria.native_storage_namespaces n ON n.environment=b.environment AND n.resource=b.resource::text AND n.warehouse_id=b.warehouse_id AND n.table_id=b.table_id AND n.revision=b.namespace_revision WHERE b.environment=$1 AND b.resource=$2 AND b.enabled AND n.enabled FOR SHARE OF b,n", &[&session.environment(),&id]).await?.get(0);
            checkpoint_outputs.insert(
                id.to_string(),
                json!({"binding":&physical,"writerSchema":output.schema}),
            );
            tx.execute("INSERT INTO moria.dataset_heads(environment,dataset,revision) VALUES($1,$2,0) ON CONFLICT DO NOTHING",&[&session.environment(),&id.to_string()]).await?;
            if write_mode == "replace" {
                heads.insert(id.to_string(),tx.query_one("SELECT revision::text FROM moria.dataset_heads WHERE environment=$1 AND dataset=$2 FOR SHARE",&[&session.environment(),&id.to_string()]).await?.get::<_,String>(0));
                outputs.insert(
                    name,
                    json!({"resource":id,"namespace":output.namespace,"schema":output.schema}),
                );
            } else {
                let budget = output
                    .mutation_budget
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("DATASET_MUTATION_BUDGET_REQUIRED"))?;
                let target: iceberg::spec::Schema =
                    serde_json::from_value(physical["icebergSchema"].clone())?;
                let keys: Vec<String> = serde_json::from_value(physical["primaryKey"].clone())?;
                ensure!(
                    physical["writeModes"]
                        .as_array()
                        .is_some_and(|modes| modes.contains(&json!(write_mode)))
                        && (write_mode == "append" || !keys.is_empty())
                        && normalized_columns(&dataset["spec"]["columns"])?
                            == serde_json::to_value(crate::snapshot::columns(
                                &physical["icebergSchema"]
                            )?)?,
                    "PIPELINE_SCHEMA_UNSUPPORTED"
                );
                let input = if write_mode == "delete" {
                    crate::dataset_writer::delete_input_schema(&target, &keys)?
                } else {
                    target.clone()
                };
                ensure!(
                    serde_json::from_value::<iceberg::spec::Schema>(output.schema.clone())?
                        == input,
                    "PIPELINE_SCHEMA_UNSUPPORTED"
                );
                let head=tx.query_one(
                    "SELECT revision,snapshot_id,metadata_location FROM moria.dataset_heads WHERE environment=$1 AND dataset=$2 FOR SHARE",
                    &[&session.environment(),&id.to_string()],
                ).await?;
                let revision: i64 = head.get(0);
                let snapshot: Option<String> = head.get(1);
                let location: Option<String> = head.get(2);
                if revision == 0 {
                    ensure!(
                        snapshot.is_none() && location.is_none(),
                        "DATASET_WRITE_CAPABILITY_REQUIRED"
                    );
                } else {
                    let location = location
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("DATASET_WRITE_CAPABILITY_REQUIRED"))?;
                    let snapshot = snapshot
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("DATASET_WRITE_CAPABILITY_REQUIRED"))?;
                    let fingerprint = physical["physicalBindingFingerprint"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("STORAGE_NAMESPACE_REVOKED"))?;
                    ensure!(
                        crate::catalog::NativeSnapshotRef::decode(location)?.version==3
                        && tx.query_opt(
                            "SELECT 1 FROM moria.runtime_dataset_roots r
                              JOIN moria.runtime_dataset_root_locations l ON l.root=r.key
                              JOIN moria.runtime_dataset_graph_roots g ON g.root=r.key AND g.physical_fingerprint=l.physical_fingerprint AND g.incarnation=l.incarnation
                             WHERE r.key=moria.runtime_dataset_root_key($3) AND r.environment=$1 AND r.dataset=$2
                               AND r.metadata_location=$3 AND r.snapshot_id=$4 AND r.sealed AND l.sealed
                               AND r.reader_profile='iceberg-v2-native-mor-bounded-v3'
                               AND l.physical_fingerprint=$5 AND l.incarnation=(SELECT incarnation FROM moria.indexing_instance)
                               AND l.namespace->>'warehouseId'=$6 AND l.namespace->>'tableId'=$7
                               AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_expired_roots x WHERE x.root=r.key)",
                            &[&session.environment(),&id.to_string(),&location,&snapshot,&fingerprint,
                              &output.namespace.warehouse_id,&output.namespace.table_id],
                        ).await?.is_some(),
                        "DATASET_WRITE_CAPABILITY_REQUIRED"
                    );
                }
                heads.insert(id.to_string(), revision.to_string());
                outputs.insert(
                    name,
                    json!({
                        "resource":id,"namespace":output.namespace,"schema":target,
                        "inputSchema":input,"mutationBudget":budget,
                    }),
                );
            }
            Authority::resource_access_for_operation(tx.client(), session, &id.to_string(), "read")
                .await?;
        }
        ensure!(labels.len() <= 256, "PIPELINE_MARKINGS_INVALID");
        authorize(
            &Authority::current(&*tx, session).await?.identity,
            &resource.to_string(),
            operation,
            &labels.iter().cloned().collect::<Vec<_>>(),
        )?;
        let spec: ExecutionSpec = serde_json::from_value(
            json!({"version":1,"domain":domain,"release":release.strip_prefix("sha256:").ok_or_else(||anyhow::anyhow!("PIPELINE_SOURCE_INVALID"))?,"resourceClass":profile.resource_class,"timeoutSeconds":program.timeout,"retryPolicy":"never","inputSchema":{"type":"object","properties":{}},"pinnedInputs":pins,"expectedHeads":heads}),
        )?;
        spec.validate()?;
        let (_, hash) = crate::execution::normalized(&spec, &json!({}))?;
        let operations = [
            format!("{resource}:read"),
            format!("{resource}:{operation}"),
        ]
        .into_iter()
        .chain(
            program
                .dependency
                .map(|dependency| format!("{dependency}:read")),
        )
        .chain(
            inputs
                .values()
                .map(|value| format!("{}:read", value["resource"].as_str().unwrap())),
        )
        .chain(outputs.values().flat_map(|value| {
            [
                format!("{}:read", value["resource"].as_str().unwrap()),
                format!("{}:write", value["resource"].as_str().unwrap()),
            ]
        }))
        .chain(
            accepted_secrets
                .iter()
                .map(|secret| format!("{}:use", secret.request.secret)),
        )
        .collect::<Vec<_>>();
        let binding = if let Some((binding, consumed)) = crate::automations::dispatch_binding_in(
            tx.client(),
            session,
            resource,
            &serde_json::to_value(&spec)?,
            &operations,
            &labels.iter().cloned().collect::<Vec<_>>(),
        )
        .await?
        {
            labels.extend(consumed);
            binding
        } else {
            service
                .bind_in(
                    tx.client(),
                    &serde_json::to_value(&spec)?,
                    &operations,
                    &labels.iter().cloned().collect::<Vec<_>>(),
                    session,
                )
                .await?
        };
        let effect = if capability == Capability::SchemaDiscovery {
            "readOnly"
        } else {
            "readWrite"
        };
        tx.execute("INSERT INTO moria.runtime_sandbox_bindings(binding,domain,artifact_uri,artifact_digest,outputs,effect,memory_mib,cpu_millis,timeout_seconds) VALUES($1,$2,$3,$4,$5,$9,$6,$7,$8)",&[&binding.id,&domain,&program.artifact["uri"].as_str().unwrap(),&program.artifact["digest"].as_str().unwrap(),&json!({"complete":{"type":"boolean","nullable":false}}),&program.memory,&program.cpu,&program.timeout,&effect]).await?;
        let checkpoint = if let Some(stream) = program
            .stream
            .as_ref()
            .filter(|_| capability == Capability::BatchRead)
        {
            let config_revision = checkpoint_revision(
                &program,
                &definition,
                &profile.adapter,
                &Value::Object(checkpoint_outputs),
                destination.as_ref(),
                &material_semantics,
            )?;
            let previous=tx.query_opt("SELECT version::text,value FROM moria.checkpoints WHERE environment=$1 AND connection=$2 AND stream=$3 AND config_revision=$4",&[&session.environment(),&resource.to_string(),&stream,&config_revision]).await?;
            tx.execute("INSERT INTO moria.runtime_snapshot_checkpoint_bindings(binding,connection,stream,config_revision) VALUES($1,$2,$3,$4)",&[&binding.id,&resource.to_string(),&stream,&config_revision]).await?;
            Some(
                json!({"connection":resource,"stream":stream,"configRevision":config_revision,"expectedVersion":previous.as_ref().map(|row|row.get::<_,String>(0)).unwrap_or_else(||"0".into()),"value":previous.map(|row|row.get::<_,Value>(1)).unwrap_or(Value::Null)}),
            )
        } else {
            None
        };
        let execution: Uuid = tx
            .query_one(
                "SELECT moria.runtime_execution_request($1,$2,'{}',$3,$4)",
                &[
                    &binding.id,
                    &request_key,
                    &hash,
                    &json!([{"key":"engine","partition":null}]),
                ],
            )
            .await?
            .get(0);
        tx.execute("INSERT INTO moria.runtime_pipeline_executions(execution,profile,entry_id,release,definition,inputs,outputs,configuration,checkpoint,destination_fingerprint,requester,markings,code_resource,secret_approvals,capability) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",&[&execution,&registered.get::<_,Uuid>("id"),&entry,&release,&definition,&Value::Object(inputs),&Value::Object(outputs),&program.configuration,&checkpoint,&destination_fingerprint,&json!({"kind":"user","principal":session.principal(),"application":session.application(),"key":key}),&json!(markings(session.environment(),&labels)),&program.dependency,&serde_json::to_value(&accepted_secrets)?,&capability.as_str()]).await?;
        let result = self.public_in(tx.client(), session, execution).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(result)
    }
    async fn public_in(
        &self,
        client: &Client,
        session: &VerifiedSession,
        id: Uuid,
    ) -> Result<Value> {
        let row=client.query_opt("SELECT e.*,p.release,p.requester,p.inputs,p.outputs,p.code_resource,p.markings,p.secret_approvals,(e.finished_at IS NOT NULL AND e.finished_at<=clock_timestamp()-interval '30 days') AS expired FROM moria.runtime_executions e JOIN moria.runtime_pipeline_executions p ON p.execution=e.id WHERE e.id=$1 AND e.environment=$2 AND p.requester->>'principal'=$3 AND p.requester->>'application'=$4",&[&id,&session.environment(),&session.principal(),&session.application()]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        ensure!(!row.get::<_, bool>("expired"), "RESULT_EXPIRED");
        let state = Authority::current(client, session).await?;
        let labels: Value = row.get("markings");
        let mut labels: Vec<String> = labels
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_MARKINGS_INVALID"))?
            .iter()
            .map(|label| {
                label["marking"]
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow::anyhow!("PIPELINE_MARKINGS_INVALID"))
            })
            .collect::<Result<_>>()?;
        labels.extend(
            accepted_secrets_in(client, session, &row.get::<_, Value>("secret_approvals")).await?,
        );
        labels.extend(
            crate::sandbox::retained_observations_in(client, id)
                .await?
                .into_iter()
                .map(|label| label.marking),
        );
        labels.extend(
            crate::automations::retained_in(client, session, id)
                .await?
                .into_iter()
                .map(|marking| marking.marking),
        );
        let resource: Uuid = row.get("resource");
        Authority::resource_access_for_operation(
            client,
            session,
            &resource.to_string(),
            if row.get::<_, Value>("spec")["domain"] == "transform" {
                "build"
            } else {
                "sync"
            },
        )
        .await?;
        authorize(
            &state.identity,
            &resource.to_string(),
            if row.get::<_, Value>("spec")["domain"] == "transform" {
                "build"
            } else {
                "sync"
            },
            &labels,
        )?;
        for output in row
            .get::<_, Value>("outputs")
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))?
            .values()
        {
            Authority::resource_access_for_operation(
                client,
                session,
                output["resource"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))?,
                "read",
            )
            .await?;
        }
        for input in row
            .get::<_, Value>("inputs")
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))?
            .values()
        {
            Authority::resource_access_for_operation(
                client,
                session,
                input["resource"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))?,
                "read",
            )
            .await?;
            let namespace: IndexNamespace =
                serde_json::from_value(input["pin"]["namespace"].clone())?;
            ensure!(client.query_opt("SELECT 1 FROM moria.native_storage_namespaces WHERE environment=$1 AND resource=$2 AND warehouse_id=$3 AND table_id=$4 AND revision=$5 AND enabled",&[&session.environment(),&input["resource"].as_str().unwrap(),&namespace.warehouse_id,&namespace.table_id,&namespace.revision]).await?.is_some(),"STORAGE_NAMESPACE_REVOKED");
        }
        Authority::finish(client, session, &state).await?;
        crate::sandbox::require_clean_observations_in(client, id).await?;
        Ok(
            json!({"valueVersion":1,"id":id,"resourceId":resource,"release":row.get::<_,String>("release"),"status":row.get::<_,String>("status"),"result":row.get::<_,Option<Value>>("result"),"error":row.get::<_,Option<String>>("error"),"markings":markings(session.environment(),&labels.into_iter().collect())}),
        )
    }
    pub async fn get(&self, session: &VerifiedSession, id: Uuid) -> Result<Value> {
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.read_transaction().await?;
        let value = self.public_in(tx.client(), session, id).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(value)
    }
    pub async fn cancel(&self, session: &VerifiedSession, id: Uuid) -> Result<Value> {
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        let row=tx.query_opt("SELECT e.binding,e.resource,e.spec->>'domain' AS domain,p.release FROM moria.runtime_executions e JOIN moria.runtime_pipeline_executions p ON p.execution=e.id JOIN moria.runtime_pipeline_profiles profile ON profile.id=p.profile AND profile.enabled WHERE e.id=$1 AND e.environment=$2 AND p.requester->>'principal'=$3 AND p.requester->>'application'=$4 FOR UPDATE OF e",&[&id,&session.environment(),&session.principal(),&session.application()]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        crate::execution::cancellation_authority_in(
            tx.client(),
            session,
            &row,
            if row.get::<_, String>("domain") == "transform" {
                "build"
            } else {
                "sync"
            },
        )
        .await?;
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
    pub async fn events(&self, session: &VerifiedSession, id: Uuid, after: &str) -> Result<Value> {
        let sequence = after.parse::<i64>()?;
        ensure!(
            sequence >= 0 && sequence.to_string() == after,
            "EVENT_CURSOR_INVALID"
        );
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.read_transaction().await?;
        let protected = self.public_in(tx.client(), session, id).await?;
        let rows=tx.query("SELECT e.sequence::text,e.kind,e.payload FROM moria.runtime_execution_events e WHERE e.execution=$1 AND e.sequence>$2 ORDER BY e.sequence LIMIT 128",&[&id,&sequence]).await?;
        let result = json!({"events":rows.iter().map(|row|json!({"sequence":row.get::<_,String>(0),"type":row.get::<_,String>(1),"payload":row.get::<_,Value>(2),"markings":protected["markings"]})).collect::<Vec<_>>(),"after":rows.last().map(|row|row.get::<_,String>(0)).unwrap_or_else(||after.into())});
        tx.commit().await?;
        admission.release().await?;
        Ok(result)
    }
    async fn fail(&self, lease: &Lease, error: &anyhow::Error) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let changed = if let Some(task) = lease.task {
            tx.execute("UPDATE moria.runtime_executions SET status='failed',error='PIPELINE_FAILED',finished_at=clock_timestamp() WHERE id=$1 AND status='running' AND EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.id=$2 AND t.execution=$1 AND t.status='running' AND t.owner=$3 AND t.fence=$4 AND t.lease_until>clock_timestamp()) AND EXISTS(SELECT 1 FROM moria.indexing_instance WHERE incarnation=$5)",&[&lease.execution,&task,&lease.owner,&lease.fence.parse::<i64>()?,&lease.incarnation]).await?
        } else {
            tx.execute("UPDATE moria.runtime_executions SET status='failed',error='PIPELINE_PUBLICATION_FAILED',finished_at=clock_timestamp(),lease_until=NULL WHERE id=$1 AND status='preparing' AND owner=$2 AND fence=$3 AND lease_until>clock_timestamp() AND NOT EXISTS(SELECT 1 FROM moria.publications WHERE execution=$1::text) AND EXISTS(SELECT 1 FROM moria.indexing_instance WHERE incarnation=$4)",&[&lease.execution,&lease.owner,&lease.fence.parse::<i64>()?,&lease.incarnation]).await?
        };
        if changed > 0 {
            tx.query_one(
                "SELECT moria.runtime_execution_event($1,'failed',$2)",
                &[
                    &lease.execution,
                    &json!({"error":"PIPELINE_FAILED","cause":crate::database::safe_error(error),"sandboxCode":crate::sandbox::error_code(error),"stages":error.chain().filter_map(|cause|["SANDBOX_PREPARE","SANDBOX_BINDING","SANDBOX_RESERVATION","SANDBOX_JOB_SESSION","SCRATCH_ADMISSION","ENGINE_IO_ADMISSION"].into_iter().find(|stage|cause.to_string()==*stage)).collect::<Vec<_>>(),"sqlState":error.chain().find_map(|cause|cause.downcast_ref::<tokio_postgres::Error>().and_then(|e|e.code()).map(|state|state.code()))}),
                ],
            )
            .await?;
            if let Some(task) = lease.task {
                tx.execute("UPDATE moria.runtime_execution_tasks SET status='cancelled',lease_until=NULL WHERE id=$1 AND owner=$2 AND fence=$3 AND status='running'",&[&task,&lease.owner,&lease.fence.parse::<i64>()?]).await?;
                tx.execute("UPDATE moria.runtime_execution_attempts SET status='failed',finished_at=clock_timestamp() WHERE task=$1 AND fence=$2 AND status='running'",&[&task,&lease.fence.parse::<i64>()?]).await?;
                if tx.query_opt("SELECT 1 FROM moria.runtime_sandbox_attempts WHERE execution=$1 AND creation_state>0",&[&lease.execution]).await?.is_none(){tx.query_one("SELECT moria.runtime_execution_event($1,'noCodeStart','{}')",&[&lease.execution]).await?;}
            }
        }
        tx.commit().await?;
        Ok(())
    }
    async fn publish(
        &self,
        _supervisor: &SandboxSupervisor,
        context: &SandboxContext,
    ) -> Result<()> {
        let session = Authority {
            db: self.db.clone(),
        }
        .sandbox_session(context)
        .await?;
        let client = self.db.get().await?;
        let row=client.query_one("SELECT r.outputs,r.checkpoint,r.capability,p.profile FROM moria.runtime_pipeline_executions r JOIN moria.runtime_pipeline_profiles p ON p.id=r.profile WHERE r.execution=$1",&[&context.lease().execution]).await?;
        if row.get::<_, String>("capability") == "schemaDiscovery" {
            drop(client);
            return discovery::complete(&self.db, context).await;
        }
        let profile = profile(&row.get::<_, Value>("profile"))?;
        let sealed=client.query("SELECT id,owner,fence,result FROM moria.runtime_execution_tasks WHERE execution=$1 AND status='sealed' LIMIT 2",&[&context.lease().execution]).await?;
        ensure!(sealed.len() == 1, "PIPELINE_OUTPUT_INVALID");
        let task = &sealed[0];
        let result: Value = task.get("result");
        let mut checkpoint: Option<Value> = row.get("checkpoint");
        if let Some(checkpoint) = &mut checkpoint {
            let value:Value=client.query_opt("SELECT value FROM moria.runtime_pipeline_checkpoints WHERE execution=$1 AND task=$2 AND owner=$3 AND fence=$4",&[&context.lease().execution,&task.get::<_,Uuid>("id"),&task.get::<_,Option<Uuid>>("owner"),&task.get::<_,i64>("fence")]).await?.ok_or_else(||anyhow::anyhow!("PIPELINE_CHECKPOINT_REQUIRED"))?.get(0);
            checkpoint["value"] = value;
        }
        let outputs: Value = row.get("outputs");
        drop(client);
        let root = std::env::var("MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT")
            .map_err(|_| anyhow::anyhow!("PIPELINE_WORKER_CONFIG_INVALID"))?;
        let limits: crate::storage::ClosureLimits = std::env::var("MORIA_NATIVE_CLOSURE_LIMITS")
            .ok()
            .map(|value| serde_json::from_str(&value))
            .transpose()?
            .unwrap_or(crate::storage::ClosureLimits {
                max_entries: 1_000_000,
                max_referenced_bytes: profile.limits.output_bytes.saturating_mul(4),
                max_spool_bytes: 1024 * 1024 * 1024,
            });
        let mut candidates = Vec::new();
        for output in outputs
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))?
            .values()
        {
            context.check().await?;
            let namespace: IndexNamespace = serde_json::from_value(output["namespace"].clone())?;
            let resource = output["resource"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))?;
            let partset: SealedObject =
                serde_json::from_value(result["datasetOutputs"][resource].clone())?;
            let grant = Arc::new(
                crate::catalog::DatasetIoGrant::acquire(
                    self.db.clone(),
                    session.clone(),
                    context.lease().clone(),
                    namespace.clone(),
                )
                .await?,
            );
            let (store, _) = crate::storage::configured_store_with_fingerprint(&namespace)?;
            let scratch = crate::storage::PosixStore::provision(
                std::path::Path::new(&root),
                &namespace.warehouse_id,
                &namespace.table_id,
                crate::storage::StorageLimits {
                    max_object_bytes: 128 * 1024 * 1024,
                    chunk_bytes: 1024 * 1024,
                },
            )?;
            candidates.push(
                crate::dataset_writer::prepare_output(
                    store,
                    &scratch,
                    grant,
                    serde_json::from_value(output["schema"].clone())?,
                    &[partset],
                    limits,
                    Cancellation::default(),
                )
                .await?,
            );
        }
        let mut admission = Admission::begin(&self.db, context.environment(), true, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        context.check_in(tx.client()).await?;
        check_context(tx.client(), context).await?;
        tx.query_one(
            "SELECT moria.runtime_snapshot_publish($1,$2,$3,$4,$5,$6,$7)",
            &[
                &session.session_hash(),
                &context.lease().execution,
                &context.lease().owner,
                &context.lease().fence.parse::<i64>()?,
                &context.lease().incarnation,
                &candidates,
                &checkpoint,
            ],
        )
        .await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(())
    }
    pub async fn worker_once(
        &self,
        owner: Uuid,
        host: Uuid,
        mut cancel: watch::Receiver<bool>,
    ) -> Result<bool> {
        crate::scratch::verify_cleanup_privilege()?;
        let broker = Arc::new(NativePipelineBroker {
            db: self.db.clone(),
            state: Mutex::new(None),
            discovery: Mutex::new(None),
            cancel: Cancellation::default(),
        });
        let supervisor = SandboxSupervisor::new(self.db.clone(), host, broker)?;
        supervisor.cleanup_abandoned().await?;
        let authority = ExecutionAuthority {
            db: self.db.clone(),
        };
        if let Some(claim) = authority.claim_engine(owner, host, true, None).await? {
            let context = supervisor.publication_context(claim.lease.clone()).await?;
            let remaining:i64=self.db.get().await?.query_one("SELECT GREATEST(0,floor(extract(epoch FROM(deadline-clock_timestamp()))*1000))::bigint FROM moria.runtime_executions WHERE id=$1",&[&claim.lease.execution]).await?.get(0);
            let guard = async {
                let mut renewed = tokio::time::Instant::now();
                loop {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    context.check().await?;
                    if renewed.elapsed() >= Duration::from_secs(8) {
                        authority.renew(&claim.lease).await?;
                        renewed = tokio::time::Instant::now();
                    }
                }
                #[allow(unreachable_code)]
                Ok::<(), anyhow::Error>(())
            };
            let result = tokio::select! {result=self.publish(&supervisor,&context)=>result,result=guard=>result,_=tokio::time::sleep(Duration::from_millis(remaining.max(0) as u64))=>Err(anyhow::anyhow!("DEADLINE_EXCEEDED")),_=cancel.changed()=>Err(anyhow::anyhow!("CANCELLED"))};
            if let Err(error) = result {
                if *cancel.borrow() {
                    return Ok(true);
                }
                self.fail(&claim.lease, &error).await?;
            }
            return Ok(true);
        }
        let Some(claim) = authority.claim_engine(owner, host, false, None).await? else {
            return Ok(false);
        };
        if let Err(error) = supervisor.execute(claim.lease.clone(), cancel).await {
            self.fail(&claim.lease, &error).await?;
        }
        Ok(true)
    }
}

pub async fn cli(db: Database, args: Vec<String>) -> Result<()> {
    let operation = args.first().map(String::as_str).unwrap_or("");
    let service = NativePipelines::new(db.clone());
    if ["worker", "once"].contains(&operation) {
        crate::scratch::verify_cleanup_privilege()?;
        let owner: Uuid = std::env::var("MORIA_PIPELINE_WORKER_OWNER")?.parse()?;
        let host: Uuid = std::env::var("MORIA_PIPELINE_HOST_ID")?.parse()?;
        ensure!(
            std::env::var("MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT").is_ok(),
            "PIPELINE_WORKER_CONFIG_INVALID"
        );
        ensure!(db.get().await?.query_opt("SELECT 1 FROM moria.runtime_workers w JOIN moria.indexing_instance i ON i.incarnation=w.incarnation WHERE w.owner=$1 AND w.host_id=$2 AND w.enabled AND w.expires_at>clock_timestamp() AND w.domains && ARRAY['transform','sync']",&[&owner,&host]).await?.is_some(),"FUNCTION_WORKER_NOT_AUTHORIZED");
        let (cancel, cancelled) = watch::channel(false);
        if operation == "once" {
            println!(
                "{}",
                json!({"worked":service.worker_once(owner,host,cancelled).await?})
            );
            return Ok(());
        }
        let concurrency = std::env::var("MORIA_PIPELINE_CONCURRENCY")
            .unwrap_or_else(|_| "2".into())
            .parse::<usize>()?;
        ensure!(
            (1..=8).contains(&concurrency),
            "PIPELINE_WORKER_CONFIG_INVALID"
        );
        let mut jobs = tokio::task::JoinSet::new();
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        println!(
            "{}",
            json!({"event":"pipeline-worker","owner":owner,"host":host,"concurrency":concurrency,"processId":std::process::id().to_string()})
        );
        loop {
            while jobs.len() < concurrency {
                let service = service.clone();
                let cancelled = cancelled.clone();
                jobs.spawn(async move {
                    let result = service.worker_once(owner, host, cancelled).await;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    result
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
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    tokio::io::stdin()
        .take(65537)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 65536, "PIPELINE_REQUEST_INVALID");
    let value: Value = serde_json::from_slice(&bytes)?;
    let session = Authority { db: db.clone() }
        .authenticate(
            value["token"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("UNAUTHENTICATED"))?,
        )
        .await?;
    let id = || {
        value["id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_REQUEST_INVALID"))?
            .parse::<Uuid>()
            .map_err(anyhow::Error::from)
    };
    let result = match operation {
        "invoke" => {
            service
                .invoke(
                    &session,
                    value["resource"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("PIPELINE_REQUEST_INVALID"))?
                        .parse()?,
                    value["key"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("PIPELINE_REQUEST_INVALID"))?,
                )
                .await?
        }
        "discover" => {
            service
                .discover(
                    &session,
                    value["resource"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("PIPELINE_REQUEST_INVALID"))?
                        .parse()?,
                    value["key"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("PIPELINE_REQUEST_INVALID"))?,
                )
                .await?
        }
        "get" => service.get(&session, id()?).await?,
        "cancel" => service.cancel(&session, id()?).await?,
        "events" => {
            service
                .events(&session, id()?, value["after"].as_str().unwrap_or("0"))
                .await?
        }
        _ => anyhow::bail!("PIPELINE_REQUEST_INVALID"),
    };
    println!("{result}");
    Ok(())
}

struct IoState {
    execution: Uuid,
    io: NativeEngineIo,
    metadata: Value,
    destination: Option<Destination>,
    calls: u64,
    method_counts: BTreeMap<&'static str, u64>,
    method_millis: BTreeMap<&'static str, u64>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Credential {
    secret: Uuid,
    material_ref: String,
    header: String,
    scheme: CredentialScheme,
}
#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum CredentialScheme {
    Raw,
    Bearer,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DestinationDefinition {
    id: Uuid,
    revision: String,
    endpoint_ref: String,
    secret_ref: String,
    #[serde(default)]
    credentials: Vec<Credential>,
    methods: Vec<String>,
    paths: Vec<String>,
    markings: Vec<String>,
    allowed_markings: Vec<String>,
}
impl DestinationDefinition {
    fn credentials_for(&self, secrets: &BTreeSet<Uuid>) -> Result<Vec<Credential>> {
        if self.credentials.is_empty() {
            ensure!(secrets.len() <= 1, "SECRET_BINDING_INVALID");
            return Ok(secrets
                .iter()
                .map(|secret| Credential {
                    secret: *secret,
                    material_ref: self.secret_ref.clone(),
                    header: "authorization".into(),
                    scheme: CredentialScheme::Bearer,
                })
                .collect());
        }
        validate_credentials(&self.credentials)?;
        ensure!(
            self.credentials
                .iter()
                .map(|slot| slot.secret)
                .collect::<BTreeSet<_>>()
                == *secrets,
            "SECRET_BINDING_INVALID"
        );
        Ok(self.credentials.clone())
    }

    fn endpoint(&self) -> Result<reqwest::Url> {
        let url = reqwest::Url::parse(
            &std::env::var(&self.endpoint_ref)
                .map_err(|_| anyhow::anyhow!("PIPELINE_WORKER_CONFIG_INVALID"))?,
        )?;
        ensure!(
            ["http", "https"].contains(&url.scheme())
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && url.host_str().is_some(),
            "PIPELINE_DESTINATION_INVALID"
        );
        Ok(url)
    }

    fn fingerprint(&self) -> Result<String> {
        canonical::fingerprint(&json!({"binding":self,"endpoint":self.endpoint()?.as_str()}))
    }
}
async fn destination_in(
    client: &Client,
    environment: &str,
    resource: Uuid,
    id: Uuid,
) -> Result<DestinationDefinition> {
    let value:Value=client.query_opt("SELECT jsonb_build_object('id',id,'revision',revision,'endpointRef',endpoint_ref,'secretRef',secret_ref,'credentials',credentials,'methods',methods,'paths',paths,'markings',markings,'allowedMarkings',allowed_markings) FROM moria.runtime_pipeline_destinations WHERE id=$1 AND environment=$2 AND resource=$3 AND enabled FOR SHARE",&[&id,&environment,&resource]).await?.ok_or_else(||anyhow::anyhow!("PIPELINE_DESTINATION_REVOKED"))?.get(0);
    let result: DestinationDefinition = serde_json::from_value(value)?;
    validate_credentials(&result.credentials)?;
    ensure!(
        !result.methods.is_empty()
            && result
                .methods
                .iter()
                .all(|method| method == "GET" || method == "POST")
            && !result.paths.is_empty()
            && result.paths.iter().all(|path| path.starts_with('/')
                && !path.starts_with("//")
                && path.len() <= 1024
                && !path.contains(['?', '#', '\\'])
                && !path.split('/').any(|part| part == "." || part == "..")),
        "PIPELINE_DESTINATION_INVALID"
    );
    Ok(result)
}
fn validate_credentials(credentials: &[Credential]) -> Result<()> {
    let mut secrets = BTreeSet::new();
    let mut headers = BTreeSet::new();
    ensure!(credentials.len() <= 8, "PIPELINE_DESTINATION_INVALID");
    for credential in credentials {
        let header = reqwest::header::HeaderName::from_bytes(credential.header.as_bytes())
            .map_err(|_| anyhow::anyhow!("PIPELINE_DESTINATION_INVALID"))?;
        ensure!(
            credential.header == header.as_str()
                && ![
                    "host",
                    "content-length",
                    "transfer-encoding",
                    "connection",
                    "accept",
                    "content-type",
                    "cookie",
                    "set-cookie",
                    "proxy-authorization"
                ]
                .contains(&header.as_str())
                && (header == reqwest::header::AUTHORIZATION || header.as_str().starts_with("x-"))
                && secrets.insert(credential.secret)
                && headers.insert(credential.header.clone())
                && !credential.material_ref.is_empty()
                && credential.material_ref.len() <= 128
                && credential
                    .material_ref
                    .bytes()
                    .enumerate()
                    .all(|(i, b)| b.is_ascii_alphabetic()
                        || b == b'_'
                        || (i > 0 && b.is_ascii_digit())),
            "PIPELINE_DESTINATION_INVALID"
        );
    }
    Ok(())
}

fn response_contains_material(value: &Value, material: &str) -> bool {
    match value {
        Value::String(value) => value.contains(material),
        Value::Array(values) => values
            .iter()
            .any(|value| response_contains_material(value, material)),
        Value::Object(values) => values.iter().any(|(key, value)| {
            key.contains(material) || response_contains_material(value, material)
        }),
        _ => value.to_string().contains(material),
    }
}

struct Destination {
    definition: DestinationDefinition,
    url: reqwest::Url,
    headers: reqwest::header::HeaderMap,
    materials: Vec<String>,
    client: reqwest::Client,
    bytes: u64,
    limit: u64,
}
impl Destination {
    fn new(definition: DestinationDefinition, limit: u64) -> Result<Self> {
        let url = definition.endpoint()?;
        validate_credentials(&definition.credentials)?;
        let mut headers = reqwest::header::HeaderMap::new();
        let mut materials = Vec::new();
        let slots = if definition.credentials.is_empty() {
            vec![Credential {
                secret: Uuid::nil(),
                material_ref: definition.secret_ref.clone(),
                header: "authorization".into(),
                scheme: CredentialScheme::Bearer,
            }]
        } else {
            definition.credentials.clone()
        };
        for credential in slots {
            let material = std::env::var(&credential.material_ref)
                .map_err(|_| anyhow::anyhow!("PIPELINE_WORKER_CONFIG_INVALID"))?;
            ensure!(
                !material.is_empty() && material.len() <= 8192 && !material.contains(['\r', '\n']),
                "PIPELINE_WORKER_CONFIG_INVALID"
            );
            let value = match credential.scheme {
                CredentialScheme::Raw => material.clone(),
                CredentialScheme::Bearer => format!("Bearer {material}"),
            };
            let mut value = reqwest::header::HeaderValue::from_str(&value)
                .map_err(|_| anyhow::anyhow!("PIPELINE_WORKER_CONFIG_INVALID"))?;
            value.set_sensitive(true);
            headers.insert(
                reqwest::header::HeaderName::from_bytes(credential.header.as_bytes())
                    .map_err(|_| anyhow::anyhow!("PIPELINE_DESTINATION_INVALID"))?,
                value,
            );
            materials.push(material);
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self {
            definition,
            url,
            headers,
            materials,
            client,
            bytes: 0,
            limit,
        })
    }
    async fn request(&mut self, context: &SandboxContext, bytes: &[u8]) -> Result<Value> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            method: String,
            path: String,
            body: Option<Value>,
        }
        let request: Request = serde_json::from_slice(bytes)?;
        ensure!(
            self.definition.methods.contains(&request.method)
                && request.path.starts_with('/')
                && !request.path.contains(['\\', '\0', '\r', '\n']),
            "PIPELINE_OPERATION_DENIED"
        );
        let url = self.url.join(&request.path)?;
        ensure!(
            url.origin() == self.url.origin()
                && self.definition.paths.iter().any(|path| url.path() == path
                    || (path.ends_with('/') && url.path().starts_with(path))),
            "PIPELINE_OPERATION_DENIED"
        );
        self.bytes = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_FRAME_LIMIT"))?;
        ensure!(self.bytes <= self.limit, "PIPELINE_FRAME_LIMIT");
        context.record_consumed_markings(
            &self
                .definition
                .markings
                .iter()
                .map(|marking| ConsumedMarking {
                    environment: context.environment().into(),
                    marking: marking.clone(),
                })
                .collect::<Vec<_>>(),
        )?;
        let incoming = context
            .consumed_markings()
            .into_iter()
            .map(|label| label.marking)
            .collect::<BTreeSet<_>>();
        ensure!(
            incoming
                .iter()
                .all(|label| self.definition.allowed_markings.contains(label)),
            "MARKING_FLOW"
        );
        context.check().await?;
        let mut call = self
            .client
            .request(reqwest::Method::from_bytes(request.method.as_bytes())?, url)
            .headers(self.headers.clone())
            .header("accept", "application/json");
        if let Some(body) = request.body {
            ensure!(request.method == "POST", "PIPELINE_REQUEST_INVALID");
            call = call.json(&body);
        }
        let received = call
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("PIPELINE_HTTP_FAILED"))?;
        let status = received.status().as_u16();
        ensure!(
            !received.status().is_redirection(),
            "PIPELINE_OPERATION_DENIED"
        );
        let mut stream = received.bytes_stream();
        let mut body = Vec::new();
        while let Some(chunk) = stream.next().await {
            context.check().await?;
            let chunk = chunk.map_err(|_| anyhow::anyhow!("PIPELINE_HTTP_FAILED"))?;
            ensure!(
                body.len() + chunk.len() <= 768 * 1024,
                "PIPELINE_FRAME_LIMIT"
            );
            self.bytes = self
                .bytes
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| anyhow::anyhow!("PIPELINE_FRAME_LIMIT"))?;
            ensure!(self.bytes <= self.limit, "PIPELINE_FRAME_LIMIT");
            body.extend(chunk);
        }
        context.check().await?;
        ensure!(
            !self.materials.iter().any(|material| body
                .windows(material.len())
                .any(|window| window == material.as_bytes())),
            "PIPELINE_RESPONSE_INVALID"
        );
        let body = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice::<Value>(&body)
                .map_err(|_| anyhow::anyhow!("PIPELINE_REQUEST_INVALID"))?
        };
        // JSON escapes must not hide reflected material from the guest boundary.
        ensure!(
            !self
                .materials
                .iter()
                .any(|material| response_contains_material(&body, material)),
            "PIPELINE_RESPONSE_INVALID"
        );
        Ok(json!({"status":status,"body":body}))
    }
}
struct NativePipelineBroker {
    db: Database,
    state: Mutex<Option<IoState>>,
    discovery: Mutex<Option<discovery::State>>,
    cancel: Cancellation,
}
impl NativePipelineBroker {
    async fn metric(&self, context: &SandboxContext, kind: &str, payload: Value) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        context.check_in(tx.client()).await?;
        tx.query_one(
            "SELECT moria.runtime_execution_event($1,$2,$3)",
            &[&context.lease().execution, &kind, &payload],
        )
        .await?;
        context.check_in(tx.client()).await?;
        tx.commit().await?;
        Ok(())
    }
}
impl Drop for NativePipelineBroker {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
fn response(status: u16, body: Vec<u8>, media: &str) -> Result<HttpResponse> {
    ensure!(body.len() <= 1024 * 1024, "PIPELINE_FRAME_LIMIT");
    Ok(HttpResponse {
        status,
        headers: BTreeMap::from([("content-type".into(), media.into())]),
        body: STANDARD.encode(body),
    })
}
fn json_response(value: Value) -> Result<HttpResponse> {
    response(200, serde_json::to_vec(&value)?, "application/json")
}
fn name(segment: &str) -> Result<String> {
    let url = reqwest::Url::parse(&format!("http://engine.invalid/?name={segment}"))?;
    let pairs = url.query_pairs().collect::<Vec<_>>();
    ensure!(
        pairs.len() == 1 && pairs[0].0 == "name",
        "PIPELINE_OPERATION_DENIED"
    );
    Ok(pairs[0].1.to_string())
}
#[async_trait]
impl PlatformBroker for NativePipelineBroker {
    async fn start(&self, context: &SandboxContext) -> Result<()> {
        ensure!(
            ["transform", "sync"].contains(&context.domain()),
            "PIPELINE_CONTEXT_INVALID"
        );
        let client = self.db.get().await?;
        context.check_in(&client).await?;
        check_context(&client, context).await?;
        let session = Authority::sandbox_session_in(&client, context).await?;
        let row=client.query_one("SELECT r.inputs,r.outputs,r.configuration,r.checkpoint,r.destination_fingerprint,r.capability,r.definition,p.profile,e.resource FROM moria.runtime_pipeline_executions r JOIN moria.runtime_pipeline_profiles p ON p.id=r.profile JOIN moria.runtime_executions e ON e.id=r.execution WHERE r.execution=$1",&[&context.lease().execution]).await?;
        let profile = profile(&row.get::<_, Value>("profile"))?;
        let destination = if let Some(id) = profile.connector {
            let definition =
                destination_in(&client, context.environment(), row.get("resource"), id).await?;
            ensure!(
                Some(definition.fingerprint()?)
                    == row.get::<_, Option<String>>("destination_fingerprint"),
                "PIPELINE_DESTINATION_REVOKED"
            );
            context.record_consumed_markings(
                &definition
                    .markings
                    .iter()
                    .map(|marking| ConsumedMarking {
                        environment: context.environment().into(),
                        marking: marking.clone(),
                    })
                    .collect::<Vec<_>>(),
            )?;
            Some(Destination::new(definition, profile.limits.input_bytes)?)
        } else {
            None
        };
        if row.get::<_, String>("capability") == "schemaDiscovery" {
            ensure!(context.domain() == "sync", "PIPELINE_CONTEXT_INVALID");
            let definition: Value = row.get("definition");
            let stream = definition["spec"]["stream"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))?
                .to_owned();
            let destination =
                destination.ok_or_else(|| anyhow::anyhow!("PIPELINE_DESTINATION_INVALID"))?;
            let mut state = self.discovery.lock().await;
            ensure!(state.is_none(), "PIPELINE_ALREADY_STARTED");
            *state = Some(discovery::State::new(
                context.lease().execution,
                stream,
                destination,
                row.get("configuration"),
            ));
            drop(state);
            drop(client);
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            context.complete_current_observation_in(tx.client()).await?;
            tx.commit().await?;
            return Ok(());
        }
        let input_json: Value = row.get("inputs");
        let output_json: Value = row.get("outputs");
        let inputs = input_json
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))?
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.clone(),
                    value["resource"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))?
                        .to_owned(),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let outputs = output_json
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))?
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.clone(),
                    EngineOutput {
                        resource: value["resource"]
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))?
                            .into(),
                        namespace: serde_json::from_value(value["namespace"].clone())?,
                        schema: serde_json::from_value(value["schema"].clone())?,
                        input_schema: value
                            .get("inputSchema")
                            .cloned()
                            .map(serde_json::from_value)
                            .transpose()?,
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let limits=client.query_one("SELECT b.memory_mib,s.capacity_bytes FROM moria.runtime_executions e JOIN moria.runtime_sandbox_bindings b ON b.binding=e.binding JOIN moria.runtime_engine_scratch_reservations r ON r.execution=e.id AND r.task=$2 AND r.owner=$3 AND r.fence=$4 AND r.incarnation=$5 AND r.cleaned_at IS NULL JOIN moria.runtime_engine_scratch_slots s ON s.id=r.slot AND s.enabled WHERE e.id=$1",&[&context.lease().execution,&context.lease().task,&context.lease().owner,&context.lease().fence.parse::<i64>()?,&context.lease().incarnation]).await?;
        let memory = u64::try_from(limits.get::<_, i32>(0))? * 1024 * 1024;
        let spill = u64::try_from(limits.get::<_, i64>(1))?;
        let metadata = json!({"version":1,"domain":context.domain(),"configuration":row.get::<_,Value>("configuration"),"checkpoint":row.get::<_,Option<Value>>("checkpoint").map(|value|value["value"].clone()),"inputs":inputs.keys().collect::<Vec<_>>(),"outputs":outputs.keys().collect::<Vec<_>>(),"limits":{"memoryBytes":memory.to_string(),"spillBytes":spill.to_string()}});
        drop(client);
        let io = NativeEngineIo::new(
            self.db.clone(),
            session,
            context.lease().clone(),
            inputs,
            outputs,
            EngineIoLimits {
                input_bytes: profile.limits.input_bytes,
                output_bytes: profile.limits.output_bytes,
            },
            self.cancel.clone(),
        )
        .await
        .context("ENGINE_IO_ADMISSION")?;
        context.record_consumed_markings(
            &io.consumed_markings()
                .into_iter()
                .map(|marking| ConsumedMarking {
                    environment: context.environment().into(),
                    marking,
                })
                .collect::<Vec<_>>(),
        )?;
        let mut state = self.state.lock().await;
        ensure!(state.is_none(), "PIPELINE_ALREADY_STARTED");
        *state = Some(IoState {
            execution: context.lease().execution,
            io,
            metadata,
            destination,
            calls: 0,
            method_counts: BTreeMap::new(),
            method_millis: BTreeMap::new(),
        });
        drop(state);
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        context.complete_current_observation_in(tx.client()).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn http(&self, context: &SandboxContext, call: HttpCall) -> Result<HttpResponse> {
        context.check().await?;
        let body = call
            .body
            .as_ref()
            .map(|body| STANDARD.decode(body))
            .transpose()?
            .unwrap_or_default();
        ensure!(
            body.len() <= 1024 * 1024 && !call.path.contains('?'),
            "PIPELINE_FRAME_LIMIT"
        );
        let parts = call.path.split('/').collect::<Vec<_>>();
        ensure!(
            parts.len() >= 5 && parts[..4] == ["", "api", "v1", "engine"],
            "PIPELINE_OPERATION_DENIED"
        );
        {
            let mut discovery = self.discovery.lock().await;
            if let Some(state) = discovery.as_mut() {
                let result = state
                    .http(&self.db, context, &call, &parts[4..], &body)
                    .await;
                drop(discovery);
                let mut client = self.db.get().await?;
                let tx = client.transaction().await?;
                context
                    .complete_observation_in(tx.client(), &call.id)
                    .await?;
                tx.commit().await?;
                return result;
            }
        }
        let mut owned = self.state.lock().await;
        let state = owned
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_NOT_STARTED"))?;
        ensure!(
            state.execution == context.lease().execution,
            "PIPELINE_CONTEXT_INVALID"
        );
        state.calls = state
            .calls
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_FRAME_LIMIT"))?;
        let operation = match (call.method.as_str(), &parts[4..]) {
            ("GET", ["metadata"]) => "metadata",
            ("POST", ["inputs", _, "open"]) => "inputOpen",
            ("GET", ["inputs", _, "batches", _]) => "inputBatch",
            ("POST", ["outputs", _, "open"]) => "outputOpen",
            ("POST", ["outputs", _, "append"]) => "outputAppend",
            ("POST", ["outputs", _, "finish"]) => "outputFinish",
            ("POST", ["checkpoint"]) => "checkpoint",
            ("POST", ["connector", "request"]) => "connector",
            _ => "denied",
        };
        *state.method_counts.entry(operation).or_default() += 1;
        let phase = ["inputOpen", "outputFinish"].contains(&operation)
            || (operation == "connector" && state.method_counts[operation] == 1);
        let started = tokio::time::Instant::now();
        if phase {
            self.metric(context,"enginePhase",json!({"phase":operation,"state":"started","calls":state.calls.to_string(),"metrics":state.io.metrics()})).await?;
        }
        let result = match (call.method.as_str(), &parts[4..]) {
            ("GET", ["metadata"]) => json_response(state.metadata.clone()),
            ("POST", ["inputs", input, "open"]) => {
                ensure!(
                    serde_json::from_slice::<Value>(&body)? == json!({}),
                    "PIPELINE_REQUEST_INVALID"
                );
                let opened = state.io.open_input(&name(input)?).await?;
                json_response(json!({"scan":opened.scan,"schema":STANDARD.encode(opened.schema)}))
            }
            ("GET", ["inputs", input, "batches", scan]) => match state
                .io
                .next_input(&name(input)?, Uuid::parse_str(scan)?)
                .await?
            {
                Some(bytes) => response(200, bytes, "application/vnd.apache.arrow.stream"),
                None => response(204, Vec::new(), "application/vnd.apache.arrow.stream"),
            },
            ("POST", ["outputs", output, "open"]) => {
                ensure!(
                    serde_json::from_slice::<Value>(&body)? == json!({}),
                    "PIPELINE_REQUEST_INVALID"
                );
                let schema = state.io.open_output(&name(output)?).await?;
                json_response(json!({"schema":STANDARD.encode(schema)}))
            }
            ("POST", ["outputs", output, "append"]) => {
                ensure!(
                    call.headers.get("content-type").map(String::as_str)
                        == Some("application/vnd.apache.arrow.stream"),
                    "PIPELINE_REQUEST_INVALID"
                );
                state.io.append_output(&name(output)?, &body).await?;
                json_response(json!({"accepted":true}))
            }
            ("POST", ["outputs", output, "finish"]) => {
                ensure!(
                    serde_json::from_slice::<Value>(&body)? == json!({}),
                    "PIPELINE_REQUEST_INVALID"
                );
                let output = name(output)?;
                let root = state.io.finish_output(&output).await?;
                let mut client = self.db.get().await?;
                let tx = client.transaction().await?;
                context.check_in(tx.client()).await?;
                let resource:Uuid=tx.query_one("SELECT (outputs->$2->>'resource')::uuid FROM moria.runtime_pipeline_executions WHERE execution=$1",&[&context.lease().execution,&output]).await?.get(0);
                tx.execute("INSERT INTO moria.runtime_pipeline_outputs(execution,task,owner,fence,resource,root) VALUES($1,$2,$3,$4,$5,$6)",&[&context.lease().execution,&context.lease().task,&context.lease().owner,&context.lease().fence.parse::<i64>()?,&resource,&serde_json::to_value(root)?]).await?;
                context.check_in(tx.client()).await?;
                tx.commit().await?;
                json_response(json!({"sealed":true}))
            }
            ("POST", ["checkpoint"]) => {
                ensure!(context.domain() == "sync", "PIPELINE_OPERATION_DENIED");
                let value: Value = serde_json::from_slice(&body)?;
                ensure!(
                    value
                        .as_object()
                        .is_some_and(|v| v.len() == 1 && v.contains_key("value"))
                        && body.len() <= 65536,
                    "PIPELINE_REQUEST_INVALID"
                );
                let mut client = self.db.get().await?;
                let tx = client.transaction().await?;
                context.check_in(tx.client()).await?;
                tx.execute("INSERT INTO moria.runtime_pipeline_checkpoints(execution,task,owner,fence,value) VALUES($1,$2,$3,$4,$5) ON CONFLICT(execution) DO UPDATE SET value=EXCLUDED.value WHERE runtime_pipeline_checkpoints.task=EXCLUDED.task AND runtime_pipeline_checkpoints.owner=EXCLUDED.owner AND runtime_pipeline_checkpoints.fence=EXCLUDED.fence",&[&context.lease().execution,&context.lease().task,&context.lease().owner,&context.lease().fence.parse::<i64>()?,&value["value"]]).await?;
                context.check_in(tx.client()).await?;
                tx.commit().await?;
                json_response(json!({"staged":true}))
            }
            ("POST", ["connector", "request"]) => {
                ensure!(context.domain() == "sync", "PIPELINE_OPERATION_DENIED");
                json_response(
                    state
                        .destination
                        .as_mut()
                        .ok_or_else(|| anyhow::anyhow!("PIPELINE_OPERATION_DENIED"))?
                        .request(context, &body)
                        .await?,
                )
            }
            _ => Err(anyhow::anyhow!("PIPELINE_OPERATION_DENIED")),
        };
        let elapsed = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        let duration = state.method_millis.entry(operation).or_default();
        *duration = duration.saturating_add(elapsed);
        let progress = (state.calls >= 64 && state.calls.is_power_of_two())
            .then(|| json!({"calls":state.calls.to_string(),"methods":state.method_counts.iter().map(|(name,count)|(*name,count.to_string())).collect::<BTreeMap<_,_>>(),"methodMillis":state.method_millis.iter().map(|(name,count)|(*name,count.to_string())).collect::<BTreeMap<_,_>>(),"metrics":state.io.metrics(),"authority":context.authority_metrics()}));
        let finished=phase.then(||json!({"phase":operation,"state":"finished","calls":state.calls.to_string(),"elapsedMillis":elapsed.to_string(),"metrics":state.io.metrics()}));
        drop(owned);
        if let Some(finished) = finished {
            self.metric(context, "enginePhase", finished).await?;
        }
        if let Some(progress) = progress {
            self.metric(context, "engineProgress", progress).await?;
        }
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        context
            .complete_observation_in(tx.client(), &call.id)
            .await?;
        tx.commit().await?;
        result
    }
    async fn log(
        &self,
        context: &SandboxContext,
        message: &str,
        consumed: &[ConsumedMarking],
    ) -> Result<()> {
        ensure!(message.len() <= 4096, "PIPELINE_LOG_LIMIT");
        context.record_consumed_markings(consumed)?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
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
        ensure!(
            *result == json!({"complete":true}),
            "PIPELINE_OUTPUT_INVALID"
        );
        {
            let discovery = self.discovery.lock().await;
            if let Some(state) = discovery.as_ref() {
                return state.finish(&self.db, context).await;
            }
        }
        let owned = self.state.lock().await;
        let state = owned
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_NOT_STARTED"))?;
        ensure!(
            state.execution == context.lease().execution,
            "PIPELINE_CONTEXT_INVALID"
        );
        let outputs = state.io.sealed_outputs()?;
        let calls = state.calls;
        let metrics = state.io.metrics();
        let methods = state
            .method_counts
            .iter()
            .map(|(name, count)| (*name, count.to_string()))
            .collect::<BTreeMap<_, _>>();
        let method_millis = state
            .method_millis
            .iter()
            .map(|(name, count)| (*name, count.to_string()))
            .collect::<BTreeMap<_, _>>();
        drop(owned);
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        context.check_in(tx.client()).await?;
        tx.query_one(
            "SELECT moria.runtime_execution_event($1,'engineIo',$2)",
            &[
                &context.lease().execution,
                &json!({"calls":calls.to_string(),"outputs":outputs.len(),"methods":methods,"methodMillis":method_millis,"metrics":metrics,"authority":context.authority_metrics()}),
            ],
        )
        .await?;
        context.complete_current_observation_in(tx.client()).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reflected_material_is_rejected_after_json_decoding_in_values_and_keys() {
        for body in [
            r#"{"value":"credential\u002dvalue"}"#,
            r#"{"credential\u002dvalue":false}"#,
            r#"[{"nested":["credential-value"]}]"#,
        ] {
            let value: Value = serde_json::from_str(body).unwrap();
            assert!(response_contains_material(&value, "credential-value"));
        }
        assert!(response_contains_material(
            &json!({"value":"quoted\"credential"}),
            "quoted\"credential"
        ));
        assert!(response_contains_material(
            &serde_json::from_str::<Value>("1e3").unwrap(),
            "1000"
        ));
        assert!(!response_contains_material(
            &json!({"value":"credential", "other":[1,null,true]}),
            "credential-value"
        ));
    }

    fn credential(secret: Uuid, header: &str) -> Credential {
        Credential {
            secret,
            material_ref: "CONNECTOR_MATERIAL".into(),
            header: header.into(),
            scheme: CredentialScheme::Raw,
        }
    }

    #[test]
    fn credential_slots_require_exact_unique_bounded_secret_and_header_mappings() {
        let secrets = (0..8).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
        let mut credentials = secrets
            .iter()
            .enumerate()
            .map(|(index, secret)| credential(*secret, &format!("x-key-{index}")))
            .collect::<Vec<_>>();
        assert!(validate_credentials(&credentials).is_ok());
        credentials.push(credential(Uuid::new_v4(), "x-ninth"));
        assert!(validate_credentials(&credentials).is_err());
        credentials.pop();
        credentials[1].secret = credentials[0].secret;
        assert!(validate_credentials(&credentials).is_err());
        credentials[1].secret = secrets[1];
        credentials[1].header = credentials[0].header.clone();
        assert!(validate_credentials(&credentials).is_err());
        for header in [
            "host",
            "connection",
            "content-length",
            "cookie",
            "set-cookie",
            "proxy-authorization",
            "authorization\r\nx-inject",
            "X-Key",
            "accept",
        ] {
            assert!(
                validate_credentials(&[credential(secrets[0], header)]).is_err(),
                "{header}"
            );
        }
        assert!(validate_credentials(&[credential(secrets[0], "authorization")]).is_ok());
        let mut invalid = credential(secrets[0], "x-key");
        invalid.material_ref = "PUBLIC_SOURCE_SUPPLIED_PATH/secret".into();
        assert!(validate_credentials(&[invalid]).is_err());
    }

    #[test]
    fn credential_slots_cannot_select_a_subset_or_undeclared_secret() {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let mut destination = DestinationDefinition {
            id: Uuid::new_v4(),
            revision: "v1".into(),
            endpoint_ref: "CONNECTOR_ENDPOINT".into(),
            secret_ref: "CONNECTOR_MATERIAL".into(),
            credentials: vec![
                credential(first, "authorization"),
                credential(second, "x-key"),
            ],
            methods: vec!["GET".into()],
            paths: vec!["/rows".into()],
            markings: vec![],
            allowed_markings: vec![],
        };
        assert!(
            destination
                .credentials_for(&BTreeSet::from([first, second]))
                .is_ok()
        );
        assert!(
            destination
                .credentials_for(&BTreeSet::from([first]))
                .is_err()
        );
        assert!(destination.credentials_for(&BTreeSet::new()).is_err());
        assert!(
            destination
                .credentials_for(&BTreeSet::from([first, Uuid::new_v4()]))
                .is_err()
        );
        destination.credentials.clear();
        let legacy = destination
            .credentials_for(&BTreeSet::from([first]))
            .unwrap();
        assert_eq!(legacy.len(), 1);
        assert_eq!(legacy[0].material_ref, "CONNECTOR_MATERIAL");
        assert_eq!(legacy[0].header, "authorization");
        assert!(
            destination
                .credentials_for(&BTreeSet::from([first, second]))
                .is_err()
        );
    }

    #[test]
    fn checkpoint_identity_tracks_interpretation_without_window_or_authority_generations() {
        let mut program = Program {
            artifact: json!({"uri":"oci://fixture/connector","digest":format!("sha256:{}", "1".repeat(64))}),
            inputs: vec![],
            outputs: vec!["source/Dataset/Rows".into()],
            configuration: json!({"pageSize":64}),
            timeout: 120,
            memory: 512,
            cpu: 60000,
            stream: Some("rows".into()),
            dependency: Some(Uuid::nil()),
        };
        let mut definition = json!({"kind":"Connection","release":"old","spec":{"mode":"replace","coverage":"full"}});
        let adapter = json!({"digest":"locked-adapter"});
        let mut outputs = json!({"dataset":{"binding":{"physicalBindingFingerprint":"physical-v1","primaryKey":["id"],"columns":{"id":{"type":"int64"}}}}});
        let mut materials = vec![json!({"secret":Uuid::nil(),"materialRevision":"semantic-v1"})];
        let revision =
            |program: &Program, definition: &Value, outputs: &Value, materials: &[Value]| {
                checkpoint_revision(program, definition, &adapter, outputs, None, materials)
                    .unwrap()
            };
        let original = revision(&program, &definition, &outputs, &materials);
        definition["release"] = json!("unrelated-new-release");
        definition["authorityGeneration"] = json!("99");
        definition["expectedHead"] = json!("24");
        assert_eq!(
            original,
            revision(&program, &definition, &outputs, &materials)
        );
        program.configuration["pageSize"] = json!("64");
        assert_ne!(
            original,
            revision(&program, &definition, &outputs, &materials)
        );
        program.configuration["pageSize"] = json!(64);
        materials[0]["materialRevision"] = json!("semantic-v2");
        assert_ne!(
            original,
            revision(&program, &definition, &outputs, &materials)
        );
        materials[0]["materialRevision"] = json!("semantic-v1");
        outputs["dataset"]["binding"]["primaryKey"] = json!([]);
        assert_ne!(
            original,
            revision(&program, &definition, &outputs, &materials)
        );
        outputs["dataset"]["binding"]["primaryKey"] = json!(["id"]);
        outputs["dataset"]["binding"]["physicalBindingFingerprint"] = json!("physical-v2");
        assert_ne!(
            original,
            revision(&program, &definition, &outputs, &materials)
        );
    }
}
