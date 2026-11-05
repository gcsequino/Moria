use crate::{
    authority::{Authority, VerifiedSession},
    authorization::authorize,
    canonical::fingerprint,
    storage::{IndexNamespace, configured_binding_fingerprint, configured_physical_binding},
    values,
};
use anyhow::{Result, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use tokio_postgres::Client;
use uuid::Uuid;
#[path = "source_actions.rs"]
mod actions;
#[path = "source_bindings.rs"]
mod bindings;
#[path = "source_links.rs"]
mod links;
#[path = "source_security.rs"]
mod security;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Profile {
    environment: String,
    project_id: Uuid,
    reference: String,
    configuration: Value,
}
fn profiles() -> Result<Vec<Profile>> {
    let text = std::env::var("MORIA_NATIVE_SOURCE_PROFILES").unwrap_or_else(|_| "[]".into());
    ensure!(text.len() <= 1024 * 1024, "SOURCE_RUNTIME_PROFILE_INVALID");
    let profiles: Vec<Profile> = serde_json::from_str(&text)
        .map_err(|_| anyhow::anyhow!("SOURCE_RUNTIME_PROFILE_INVALID"))?;
    ensure!(profiles.len() <= 2000, "RESOURCE_LIMIT");
    let mut seen = BTreeSet::new();
    for profile in &profiles {
        ensure!(
            !profile.environment.is_empty()
                && profile.environment.len() <= 128
                && profile.reference.len() <= 4096
                && profile.configuration.is_object()
                && seen.insert((
                    profile.environment.clone(),
                    profile.project_id,
                    profile.reference.clone()
                )),
            "SOURCE_RUNTIME_PROFILE_INVALID"
        );
    }
    Ok(profiles)
}
fn passive(kind: &str) -> bool {
    matches!(
        kind,
        "Namespace"
            | "Policy"
            | "Type"
            | "ObjectSet"
            | "RestrictedView"
            | "Marking"
            | "SecretReference"
            | "ModelProvider"
            | "ConnectorImplementation"
            | "AgentAdapter"
    )
}
fn requires_profile(kind: &str) -> bool {
    matches!(
        kind,
        "Dataset"
            | "ObjectType"
            | "LinkType"
            | "Model"
            | "Application"
            | "Grant"
            | "RetrievalCollection"
            | "Transform"
            | "Connection"
            | "Automation"
            | "Function"
            | "Agent"
            | "EvaluationSuite"
    )
}
fn sandbox_profile(resource: &Value, all: &BTreeMap<String, &Value>) -> Result<Value> {
    match resource["kind"].as_str() {
        Some("Function") => crate::functions::source_profile(resource),
        Some("Agent") => {
            let adapter = all
                .get(
                    resource["spec"]["adapter"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
                )
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
            crate::agents::source_profile(resource, adapter)
        }
        Some("EvaluationSuite") => crate::evaluations::source_profile(resource),
        _ => anyhow::bail!("SOURCE_RUNTIME_PROFILE_INVALID"),
    }
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SandboxProfile {
    resource_class: String,
    #[serde(default)]
    service: Option<Value>,
}

/// Public plan data contains requirements and hashes, never operator endpoints, secret references or identities.
pub(crate) async fn plan_in(
    client: &Client,
    session: &VerifiedSession,
    release: &Value,
    resources: &[Value],
) -> Result<Value> {
    let configured = profiles()?;
    let project: Uuid = release["snapshot"]["role"]["projectId"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?
        .parse()?;
    let relevant = configured
        .iter()
        .filter(|profile| {
            profile.environment == session.environment() && profile.project_id == project
        })
        .map(|profile| (profile.reference.clone(), profile.configuration.clone()))
        .collect::<BTreeMap<_, _>>();
    let state = Authority::current(client, session).await?;
    let admin = authorize(&state.identity, "instance", "manage", &[]).is_ok();
    let mut components = Vec::new();
    for resource in resources {
        let kind = resource["kind"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
        let reference = format!(
            "{}/{}/{}",
            resource["metadata"]["namespace"].as_str().unwrap(),
            kind,
            resource["metadata"]["name"].as_str().unwrap()
        );
        let mut reasons = Vec::new();
        let mut readiness_reasons = Vec::new();
        if requires_profile(kind) && !relevant.contains_key(&reference) {
            reasons.push("MISSING_OPERATOR_PROFILE");
        }
        if !passive(kind) && !admin {
            reasons.push("INSTANCE_MANAGE_REQUIRED");
        }
        if kind == "Dataset" && dataset_schema(&resource["spec"]["columns"]).is_err() {
            reasons.push("DATASET_TYPE_PROFILE_PENDING");
        }
        if kind == "Automation" {
            if resource["spec"]["operation"] == "evaluate" {
                reasons.push("DOMAIN_REGISTRATION_PENDING");
            }
            readiness_reasons.push("AUTOMATION_WORKER_PROOF_REQUIRED");
        }
        if matches!(kind, "Application" | "Grant")
            && !security::validate_in(client, session, release, resource).await?
        {
            reasons.push("SECURITY_ADMINISTRATION_REQUIRED");
        }
        if admin && matches!(kind, "Function" | "Agent" | "EvaluationSuite") {
            let domain = match kind {
                "Function" => "function",
                "Agent" => "agent",
                _ => "evaluation",
            };
            let class = if kind == "EvaluationSuite" {
                "evaluation-control"
            } else {
                "standard"
            };
            let live:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_workers w JOIN moria.runtime_resource_classes c ON c.name=$2 AND c.enabled JOIN moria.indexing_instance i ON i.incarnation=w.incarnation WHERE w.enabled AND w.expires_at>clock_timestamp() AND $2=ANY(w.resource_classes) AND $1=ANY(w.domains))", &[&domain,&class]).await?.get(0);
            if !live {
                readiness_reasons.push("NATIVE_WORKER_UNAVAILABLE");
            }
            if kind == "EvaluationSuite" {
                let registered:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_evaluation_environments WHERE environment=$1 AND enabled)", &[&session.environment()]).await?.get(0);
                if !registered {
                    reasons.push("EVALUATION_ENVIRONMENT_REQUIRED");
                }
            }
        }
        components.push(json!({"reference":reference,"kind":kind,"status":if reasons.is_empty(){"configured"}else{"pending"},"reasons":reasons,"readinessReasons":readiness_reasons,"requiredOperations":if passive(kind){vec!["filesystem:publish"]}else{vec!["filesystem:publish","instance:manage"]}}));
    }
    let configured_all = components.iter().all(|component| {
        !component["reasons"]
            .as_array()
            .unwrap()
            .contains(&json!("MISSING_OPERATOR_PROFILE"))
    }) && !relevant.is_empty();
    Ok(
        json!({"version":1,"ready":false,"configured":false,"configurationReady":configured_all && components.iter().all(|component|component["status"]=="configured"),"attemptRuntime":configured_all,"profileFingerprint":fingerprint(&json!(relevant))?,"components":components}),
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModelProfile {
    binding: Value,
}

pub(crate) async fn compose_in(
    client: &Client,
    session: &VerifiedSession,
    release: &Value,
    resources: &[Value],
    planned: &Value,
) -> Result<Value> {
    let mut current = plan_in(client, session, release, resources).await?;
    ensure!(
        current["profileFingerprint"] == planned["profileFingerprint"],
        "SOURCE_RUNTIME_CHANGED"
    );
    if current["attemptRuntime"] != true {
        let root: Uuid = release["rootId"].as_str().unwrap().parse()?;
        disable_in(client, session, root).await?;
        return Ok(current);
    }
    ensure!(
        current["configurationReady"] == true,
        "SOURCE_RUNTIME_NOT_READY"
    );
    client
        .query_one(
            "SELECT moria.runtime_environment_managed_check($1)",
            &[&session.environment()],
        )
        .await?;
    let root: Uuid = release["rootId"].as_str().unwrap().parse()?;
    let project: Uuid = release["snapshot"]["role"]["projectId"]
        .as_str()
        .unwrap()
        .parse()?;
    let profiles = profiles()?
        .into_iter()
        .filter(|profile| {
            profile.environment == session.environment() && profile.project_id == project
        })
        .map(|profile| (profile.reference, profile.configuration))
        .collect::<BTreeMap<_, _>>();
    let all = resources
        .iter()
        .chain(
            release["compiled"]["manifest"]["resources"]
                .as_array()
                .unwrap()
                .iter(),
        )
        .map(|resource| {
            (
                format!(
                    "{}/{}/{}",
                    resource["metadata"]["namespace"].as_str().unwrap(),
                    resource["kind"].as_str().unwrap(),
                    resource["metadata"]["name"].as_str().unwrap()
                ),
                resource,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let desired = resources
        .iter()
        .map(bindings::resource_reference)
        .collect::<Vec<_>>();
    disable_removed_in(client, session, root, &desired).await?;
    let mut ready = current["components"]
        .as_array()
        .unwrap()
        .iter()
        .all(|component| component["readinessReasons"].as_array().unwrap().is_empty());
    let mut ordered = resources.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|resource| match resource["kind"].as_str() {
        Some("Dataset") => 0,
        Some("Model") => 1,
        _ => 2,
    });
    for resource in ordered {
        let kind = resource["kind"].as_str().unwrap();
        let reference = format!(
            "{}/{}/{}",
            resource["metadata"]["namespace"].as_str().unwrap(),
            kind,
            resource["metadata"]["name"].as_str().unwrap()
        );
        if passive(kind) {
            continue;
        }
        let id:Uuid=client.query_one("SELECT resource_id FROM moria.fs_resource_identities WHERE root_id=$1 AND reference=$2", &[&root,&reference]).await?.get(0);
        match kind {
            "Dataset" => {
                let profile: DatasetProfile = serde_json::from_value(profiles[&reference].clone())
                    .map_err(|_| anyhow::anyhow!("SOURCE_RUNTIME_PROFILE_INVALID"))?;
                install_dataset_in(
                    client,
                    session,
                    id,
                    &reference,
                    release["digest"].as_str().unwrap(),
                    resource,
                    &profile,
                )
                .await?;
            }
            "Model" => {
                let profile: ModelProfile = serde_json::from_value(profiles[&reference].clone())
                    .map_err(|_| anyhow::anyhow!("SOURCE_RUNTIME_PROFILE_INVALID"))?;
                let provider = all
                    .get(resource["spec"]["provider"].as_str().unwrap())
                    .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
                let policy = all
                    .get(provider["spec"]["destinationPolicy"].as_str().unwrap())
                    .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
                ensure!(
                    profile.binding["destination"] == provider["spec"]["destination"]
                        && profile.binding["destinationPolicy"]["allowedDestinations"]
                            == policy["spec"]["allowedDestinations"]
                        && profile.binding["destinationPolicy"]["allowedMarkings"]
                            == policy["spec"]["allowedMarkings"],
                    "SOURCE_RUNTIME_PROFILE_INVALID"
                );
                let endpoint_ref = profile.binding["endpointRef"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("SOURCE_RUNTIME_PROFILE_INVALID"))?;
                let endpoint = std::env::var(endpoint_ref)
                    .map_err(|_| anyhow::anyhow!("MODEL_CONFIGURATION_UNAVAILABLE"))?;
                ensure!(
                    reqwest::Url::parse(&endpoint)?
                        == reqwest::Url::parse(provider["spec"]["endpoint"].as_str().unwrap())?,
                    "SOURCE_RUNTIME_PROFILE_INVALID"
                );
                let revision = crate::models::NativeModels::validated_registration_in(
                    client,
                    session,
                    &id.to_string(),
                    resource,
                    &profile.binding,
                )
                .await?;
                client.execute("INSERT INTO moria.native_model_bindings(environment,resource,revision,definition) VALUES($1,$2,$3,$4) ON CONFLICT(environment,resource) DO UPDATE SET revision=EXCLUDED.revision,definition=EXCLUDED.definition,enabled=true", &[&session.environment(),&id.to_string(),&revision,&profile.binding]).await?;
                let status = crate::models::NativeModels::registration_readiness_in(
                    client,
                    session,
                    &id.to_string(),
                    &profile.binding,
                )
                .await?;
                if status["ready"] != true {
                    ready = false;
                }
                if let Some(component) = current["components"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|component| component["reference"] == reference)
                {
                    component["readiness"] = status;
                }
            }
            "Function" | "Agent" | "EvaluationSuite" => {
                let operator: SandboxProfile = serde_json::from_value(profiles[&reference].clone())
                    .map_err(|_| anyhow::anyhow!("SOURCE_RUNTIME_PROFILE_INVALID"))?;
                ensure!(
                    operator.resource_class == "standard",
                    "SOURCE_RUNTIME_PROFILE_INVALID"
                );
                ensure!(
                    kind == "EvaluationSuite" || operator.service.is_none(),
                    "SOURCE_RUNTIME_PROFILE_INVALID"
                );
                if let Some(service) = operator.service {
                    crate::evaluations::install_service_in(
                        client,
                        session.environment(),
                        id,
                        &service,
                    )
                    .await?;
                } else if kind == "EvaluationSuite" {
                    client.execute("UPDATE moria.runtime_evaluation_service_profiles SET enabled=false WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&id]).await?;
                }
                sandbox_profile(resource, &all)?;
                // Configuration never contacts the worker's container runtime. Availability
                // is checked separately against the immutable configured artifact.
                ready = false;
                if let Some(component) = current["components"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|component| component["reference"] == reference)
                {
                    component["readiness"] =
                        json!({"ready":false,"reasons":["ARTIFACT_VERIFICATION_REQUIRED"]});
                }
            }
            "ObjectType" | "Action" | "LinkType" | "RetrievalCollection" => {}
            "Application" | "Grant" => {
                ensure!(
                    profiles[&reference]
                        .as_object()
                        .is_some_and(|profile| profile.is_empty()),
                    "SOURCE_RUNTIME_PROFILE_INVALID"
                );
                ensure!(
                    security::validate_in(client, session, release, resource).await?,
                    "SOURCE_RUNTIME_NOT_READY"
                );
            }
            "Automation" => {
                let profile = profiles
                    .get(&reference)
                    .ok_or_else(|| anyhow::anyhow!("SOURCE_RUNTIME_PROFILE_INVALID"))?;
                crate::automations::install_in(
                    client,
                    session.environment(),
                    id,
                    &fingerprint(profile)?,
                    profile,
                )
                .await?;
            }
            "Transform" | "Connection" => {
                let profile = profiles
                    .get(&reference)
                    .ok_or_else(|| anyhow::anyhow!("SOURCE_RUNTIME_PROFILE_INVALID"))?;
                crate::pipelines::install_in(
                    client,
                    session.environment(),
                    id,
                    &fingerprint(profile)?,
                    profile,
                )
                .await?;
            }
            _ => anyhow::bail!("SOURCE_RUNTIME_NOT_READY"),
        }
    }
    let pending =
        bindings::install_objects_in(client, session, release, resources, &profiles, &all).await?;
    for resource in resources
        .iter()
        .filter(|resource| resource["kind"] == "RetrievalCollection")
    {
        let reference = bindings::resource_reference(resource);
        let spec = &resource["spec"];
        let profile: crate::search::SearchRuntimeProfile =
            serde_json::from_value(profiles[&reference].clone())
                .map_err(|_| anyhow::anyhow!("SOURCE_RUNTIME_PROFILE_INVALID"))?;
        let id = bindings::id_in(client, session.environment(), root, &reference).await?;
        let source = bindings::id_in(
            client,
            session.environment(),
            root,
            spec["source"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
        )
        .await?;
        let model = bindings::id_in(
            client,
            session.environment(),
            root,
            spec["embeddingModel"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
        )
        .await?;
        let definition = json!({"source":source,"embeddingModel":model,"textProperty":spec["textProperty"],"chunkSize":spec["chunkSize"],"chunkOverlap":spec["chunkOverlap"],"metric":spec["metric"],"searchMode":spec["searchMode"],"markings":profile.markings});
        crate::search::NativeSearch::install_in(client, session, id, definition, &profile).await?;
        ready = false;
    }
    bindings::finish_objects_in(client, session, pending).await?;
    for component in current["components"].as_array_mut().unwrap() {
        if component["kind"] == "ObjectType"
            || component["reference"]
                .as_str()
                .is_some_and(|value| value.split('/').nth(1) == Some("ObjectType"))
        {
            ready = false;
            component["readinessReasons"]
                .as_array_mut()
                .unwrap()
                .push(json!("SOURCE_GENERATION_REQUIRED"));
        }
        let reference = component["reference"].as_str().unwrap();
        if all[reference]["kind"] == "Application"
            && all[reference]["spec"].get("frontend").is_some()
        {
            ready = false;
            component["readinessReasons"]
                .as_array_mut()
                .unwrap()
                .push(json!("FRONTEND_ARTIFACT_REQUIRED"));
        }
    }
    let binding_fingerprint = bindings_fingerprint(client, session.environment(), root).await?;
    client.execute("INSERT INTO moria.native_source_runtime_configurations(root,environment,release,profile_fingerprint,binding_fingerprint) VALUES($1,$2,$3,$4,$5) ON CONFLICT(root,environment) DO UPDATE SET release=EXCLUDED.release,profile_fingerprint=EXCLUDED.profile_fingerprint,binding_fingerprint=EXCLUDED.binding_fingerprint", &[&root,&session.environment(),&release["digest"].as_str().unwrap(),&current["profileFingerprint"].as_str().unwrap(),&binding_fingerprint]).await?;
    current["configured"] = true.into();
    current["ready"] = ready.into();
    Ok(current)
}
async fn disable_in(client: &Client, session: &VerifiedSession, root: Uuid) -> Result<()> {
    // Losing readiness can revoke this source's configured capabilities; it cannot grant a new one.
    client.execute("UPDATE moria.native_search_projector_bindings b SET enabled=false FROM moria.fs_resource_identities i WHERE i.root_id=$1 AND b.environment=$2 AND b.collection=i.resource_id::text AND b.enabled", &[&root,&session.environment()]).await?;
    for table in [
        "native_serving_bindings",
        "native_serving_actions",
        "native_serving_links",
        "native_search_collections",
        "native_model_bindings",
        "runtime_projection_bindings",
    ] {
        client.execute(&format!("UPDATE moria.{table} b SET enabled=false FROM moria.fs_resource_identities i WHERE i.root_id=$1 AND b.environment=$2 AND b.resource=i.resource_id::text AND b.enabled"), &[&root,&session.environment()]).await?;
    }
    client.execute("UPDATE moria.native_dataset_bindings b SET enabled=false,revision=$3 FROM moria.fs_resource_identities i WHERE i.root_id=$1 AND b.environment=$2 AND b.resource=i.resource_id AND b.enabled", &[&root,&session.environment(),&fingerprint(&json!([root,Uuid::new_v4(),"disabled"]))?]).await?;
    client.execute("UPDATE moria.indexing_bindings b SET enabled=false FROM moria.fs_resource_identities i WHERE i.root_id=$1 AND b.environment=$2 AND b.target=i.resource_id::text AND b.enabled", &[&root,&session.environment()]).await?;
    client.execute("UPDATE moria.runtime_pipeline_profiles b SET enabled=false FROM moria.fs_resource_identities i WHERE i.root_id=$1 AND b.environment=$2 AND b.resource=i.resource_id AND b.enabled", &[&root,&session.environment()]).await?;
    client.execute("UPDATE moria.runtime_source_automation_profiles b SET enabled=false FROM moria.fs_resource_identities i WHERE i.root_id=$1 AND b.environment=$2 AND b.resource=i.resource_id AND b.enabled", &[&root,&session.environment()]).await?;
    client.execute("UPDATE moria.runtime_evaluation_service_profiles b SET enabled=false FROM moria.fs_resource_identities i WHERE i.root_id=$1 AND b.environment=$2 AND b.resource=i.resource_id AND b.enabled", &[&root,&session.environment()]).await?;

    Ok(())
}
async fn disable_removed_in(
    client: &Client,
    session: &VerifiedSession,
    root: Uuid,
    desired: &[String],
) -> Result<()> {
    for table in [
        "native_serving_bindings",
        "native_serving_actions",
        "native_serving_links",
        "native_search_collections",
        "native_model_bindings",
        "runtime_projection_bindings",
    ] {
        client.execute(&format!("UPDATE moria.{table} b SET enabled=false FROM moria.fs_resource_identities i WHERE i.root_id=$1 AND NOT(i.reference=ANY($3::text[])) AND b.environment=$2 AND b.resource=i.resource_id::text AND b.enabled"), &[&root,&session.environment(),&desired]).await?;
    }
    client.execute("UPDATE moria.native_dataset_bindings b SET enabled=false,revision=$4 FROM moria.fs_resource_identities i WHERE i.root_id=$1 AND NOT(i.reference=ANY($3::text[])) AND b.environment=$2 AND b.resource=i.resource_id AND b.enabled", &[&root,&session.environment(),&desired,&fingerprint(&json!([root,Uuid::new_v4(),"removed"]))?]).await?;
    for (table, column) in [
        ("indexing_bindings", "target"),
        ("native_search_projector_bindings", "collection"),
    ] {
        client.execute(&format!("UPDATE moria.{table} b SET enabled=false FROM moria.fs_resource_identities i WHERE i.root_id=$1 AND NOT(i.reference=ANY($3::text[])) AND b.environment=$2 AND b.{column}=i.resource_id::text AND b.enabled"), &[&root,&session.environment(),&desired]).await?;
    }
    client.execute("UPDATE moria.runtime_pipeline_profiles b SET enabled=false FROM moria.fs_resource_identities i WHERE i.root_id=$1 AND NOT(i.reference=ANY($3::text[])) AND b.environment=$2 AND b.resource=i.resource_id AND b.enabled", &[&root,&session.environment(),&desired]).await?;
    client.execute("UPDATE moria.runtime_source_automation_profiles b SET enabled=false FROM moria.fs_resource_identities i WHERE i.root_id=$1 AND NOT(i.reference=ANY($3::text[])) AND b.environment=$2 AND b.resource=i.resource_id AND b.enabled", &[&root,&session.environment(),&desired]).await?;
    client.execute("UPDATE moria.runtime_evaluation_service_profiles b SET enabled=false FROM moria.fs_resource_identities i WHERE i.root_id=$1 AND NOT(i.reference=ANY($3::text[])) AND b.environment=$2 AND b.resource=i.resource_id AND b.enabled", &[&root,&session.environment(),&desired]).await?;

    Ok(())
}

async fn bindings_fingerprint(client: &Client, environment: &str, root: Uuid) -> Result<String> {
    let mut bindings = BTreeMap::new();
    for (table, resource, fields) in [
        (
            "native_dataset_bindings",
            "resource::text",
            "revision,enabled",
        ),
        (
            "native_serving_bindings",
            "resource",
            "revision,definition,enabled,authority_generation",
        ),
        (
            "native_serving_actions",
            "resource",
            "revision,definition,enabled,authority_generation",
        ),
        (
            "native_serving_links",
            "resource",
            "revision,definition,enabled,authority_generation",
        ),
        (
            "native_model_bindings",
            "resource",
            "revision,definition,enabled",
        ),
        (
            "runtime_projection_bindings",
            "resource",
            "revision,definition,enabled",
        ),
        ("indexing_bindings", "target", "definition,enabled"),
        (
            "native_search_collections",
            "resource",
            "revision,definition,projector_binding,enabled",
        ),
        (
            "native_search_projector_bindings",
            "collection",
            "revision,service_id,service_binding_revision,enabled",
        ),
        (
            "runtime_pipeline_profiles",
            "resource::text",
            "revision,profile,enabled",
        ),
        (
            "runtime_source_automation_profiles",
            "resource::text",
            "revision,enabled",
        ),
        (
            "runtime_evaluation_service_profiles",
            "resource::text",
            "id,authority,suite_revision,agent_revision,evaluator_revision,enabled,generation,suite_revision||'/'||authority::text AS revision",
        ),
    ] {
        let sql = format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(q) ORDER BY resource,COALESCE(to_jsonb(q)->>'revision','')),'[]') FROM (SELECT {resource} AS resource,{fields} FROM moria.{table} WHERE environment=$1 AND {resource} IN(SELECT resource_id::text FROM moria.fs_resource_identities WHERE root_id=$2)) q"
        );
        let value: Value = client.query_one(&sql, &[&environment, &root]).await?.get(0);
        bindings.insert(table, value);
    }
    fingerprint(&json!(bindings))
}

pub(crate) struct RuntimeStatus {
    pub value: Value,
    pub artifacts: Vec<(String, Value, Option<Value>)>,
    pub frontends: Vec<(String, Uuid, Value)>,
    pub anchor: String,
}
pub(crate) async fn status_in(
    db: &crate::database::Database,
    client: &Client,
    session: &VerifiedSession,
    release: &Value,
    resources: &[Value],
) -> Result<RuntimeStatus> {
    let root: Uuid = release["rootId"].as_str().unwrap().parse()?;
    let current = Authority::current(client, session).await?;
    let admin = authorize(&current.identity, "instance", "manage", &[]).is_ok();
    let mut value = plan_in(client, session, release, resources).await?;
    if !admin {
        value["withheld"] = true.into();
        value["reason"] = "INSTANCE_MANAGE_REQUIRED".into();
        value["authority"] = "caller".into();
        for component in value["components"].as_array_mut().unwrap() {
            component["readiness"] =
                json!({"ready":false,"withheld":true,"reason":"INSTANCE_MANAGE_REQUIRED"});
        }
        let anchor = fingerprint(&json!([release["digest"], value]))?;
        return Ok(RuntimeStatus {
            value,
            artifacts: vec![],
            frontends: vec![],
            anchor,
        });
    }
    let actual = bindings_fingerprint(client, session.environment(), root).await?;
    let row=client.query_opt("SELECT release,profile_fingerprint,binding_fingerprint FROM moria.native_source_runtime_configurations WHERE root=$1 AND environment=$2", &[&root,&session.environment()]).await?;
    let configured = row.is_some_and(|row| {
        row.get::<_, String>(0) == release["digest"].as_str().unwrap()
            && row.get::<_, String>(1) == value["profileFingerprint"].as_str().unwrap()
            && row.get::<_, String>(2) == actual
    });
    value["configured"] = configured.into();
    let managed:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_placements WHERE environment=$1) AND moria.runtime_environment_cell_available($1)", &[&session.environment()]).await?.get(0);
    let mut ready = configured && managed && value["configurationReady"] == true;
    let all = resources
        .iter()
        .chain(
            release["compiled"]["manifest"]["resources"]
                .as_array()
                .unwrap()
                .iter(),
        )
        .map(|resource| (bindings::resource_reference(resource), resource))
        .collect::<BTreeMap<_, _>>();
    let mut artifacts = vec![];
    let mut frontends = vec![];
    value["authority"] = "caller".into();
    for component in value["components"].as_array_mut().unwrap() {
        let reference = component["reference"].as_str().unwrap().to_owned();
        let resource = all[&reference];
        component["readinessReasons"]
            .as_array_mut()
            .unwrap()
            .retain(|value| value != "SOURCE_GENERATION_REQUIRED");
        if !component["readinessReasons"].as_array().unwrap().is_empty() {
            ready = false;
        }
        match resource["kind"].as_str().unwrap() {
            "Application" if resource["spec"].get("frontend").is_some() => {
                let id = bindings::id_in(client, session.environment(), root, &reference).await?;
                frontends.push((reference, id, resource["spec"]["frontend"].clone()));
            }
            "LinkType" if resource["spec"]["mode"] == "mapped" => {
                let source = bindings::id_in(
                    client,
                    session.environment(),
                    root,
                    resource["spec"]["sourceType"].as_str().unwrap(),
                )
                .await?;
                let target = bindings::id_in(
                    client,
                    session.environment(),
                    root,
                    resource["spec"]["targetType"].as_str().unwrap(),
                )
                .await?;
                let available: bool = client.query_one(
                    "SELECT moria.runtime_link_constraint_ready($1,$2,revision) AND moria.runtime_link_constraint_ready($1,$3,revision) FROM moria.environments WHERE id=$1",
                    &[&session.environment(), &source.to_string(), &target.to_string()],
                ).await?.get(0);
                ready &= available;
                component["readiness"] = json!({"ready":available,"reason":if available {Value::Null} else {json!("LINK_CONSTRAINT_NOT_READY")}});
            }
            "ObjectType" => {
                let id = bindings::id_in(client, session.environment(), root, &reference).await?;
                let row = client.query_one(
                    "SELECT EXISTS(SELECT 1 FROM moria.native_serving_bindings o JOIN moria.indexing_bindings ib ON ib.environment=o.environment AND ib.target=o.resource AND ib.enabled JOIN LATERAL(SELECT * FROM moria.source_generations WHERE environment=o.environment AND object_type=o.resource ORDER BY revision DESC LIMIT 1) g ON true JOIN moria.indexing_builds b ON b.id=g.stage AND b.status='active' JOIN moria.dataset_heads h ON h.environment=ib.environment AND h.dataset=ib.source JOIN moria.runtime_projection_bindings pb ON pb.environment=o.environment AND pb.resource=o.resource AND pb.enabled JOIN moria.runtime_projections p ON p.environment=o.environment AND p.resource=o.resource AND p.stage=g.stage AND p.status='active' AND p.binding_revision=pb.revision WHERE o.environment=$1 AND o.resource=$2 AND o.enabled AND b.source_revision=h.revision AND b.definition->>'servingBindingRevision'=o.revision AND b.definition->>'servingBindingGeneration'=o.authority_generation::text)",
                    &[&session.environment(), &id.to_string()],
                ).await?;
                let generation_available: bool = row.get(0);
                client
                    .batch_execute("SAVEPOINT native_indexing_readiness")
                    .await?;
                let purpose=async {
                    let binding=client.query_opt("SELECT context->>'executionBinding' FROM moria.indexing_bindings WHERE environment=$1 AND target=$2 AND enabled",&[&session.environment(),&id.to_string()]).await?.ok_or_else(||anyhow::anyhow!("SERVICE_PURPOSE_REVOKED"))?;
                    let metadata=binding.get::<_,Option<String>>(0).ok_or_else(||anyhow::anyhow!("SERVICE_PURPOSE_REVOKED"))?.parse::<Uuid>()?;
                    client.query_one("SELECT moria.runtime_service_binding_guard($1)",&[&metadata]).await?;
                    Ok::<(),anyhow::Error>(())
                }.await;
                if purpose.is_err() {
                    client
                        .batch_execute("ROLLBACK TO SAVEPOINT native_indexing_readiness")
                        .await?;
                }
                client
                    .batch_execute("RELEASE SAVEPOINT native_indexing_readiness")
                    .await?;
                let available = generation_available && purpose.is_ok();
                ready &= available;
                component["readiness"] = if let Err(error) = purpose {
                    json!({"ready":false,"reason":crate::serving::error_code(&error)})
                } else if generation_available {
                    json!({"ready":true})
                } else {
                    json!({"ready":false,"reason":"SOURCE_GENERATION_REQUIRED"})
                };
            }
            "Transform" | "Connection" => {
                let id = bindings::id_in(client, session.environment(), root, &reference).await?;
                client
                    .batch_execute("SAVEPOINT native_pipeline_readiness")
                    .await?;
                let status =
                    crate::pipelines::source_readiness_in(client, session.environment(), id).await;
                if status.is_err() {
                    client
                        .batch_execute("ROLLBACK TO SAVEPOINT native_pipeline_readiness")
                        .await?;
                }
                client
                    .batch_execute("RELEASE SAVEPOINT native_pipeline_readiness")
                    .await?;
                match status {
                    Ok(status) => {
                        ready &= status["configured"] == true
                            && status["purposeReady"] == true
                            && status["workerReady"] == true;
                        if let Some(images) = status["artifacts"].as_array() {
                            ensure!(images.len() == 2, "SOURCE_RUNTIME_PROFILE_INVALID");
                            artifacts.push((
                                reference.clone(),
                                images[1].clone(),
                                Some(images[0].clone()),
                            ));
                        }
                        component["readiness"] = status;
                    }
                    Err(error) => {
                        ready = false;
                        component["readiness"] =
                            json!({"ready":false,"reason":crate::serving::error_code(&error)});
                    }
                }
            }
            "Function" | "Agent" | "EvaluationSuite" => {
                if resource["kind"] == "EvaluationSuite" {
                    let id =
                        bindings::id_in(client, session.environment(), root, &reference).await?;
                    client
                        .batch_execute("SAVEPOINT native_evaluation_readiness")
                        .await?;
                    let status =
                        crate::evaluations::service_readiness_in(client, session.environment(), id)
                            .await;
                    if status.is_err() {
                        client
                            .batch_execute("ROLLBACK TO SAVEPOINT native_evaluation_readiness")
                            .await?;
                    }
                    client
                        .batch_execute("RELEASE SAVEPOINT native_evaluation_readiness")
                        .await?;
                    match status {
                        Ok(status) => {
                            if status["configured"] == true {
                                ready &=
                                    status["purposeReady"] == true && status["workerReady"] == true;
                            }
                            component["serviceReadiness"] = status;
                        }
                        Err(error) => {
                            ready = false;
                            component["serviceReadiness"] =
                                json!({"ready":false,"reason":crate::serving::error_code(&error)});
                        }
                    }
                }
                let sandbox = sandbox_profile(resource, &all)?;
                artifacts.push((
                    reference,
                    sandbox["artifact"].clone(),
                    sandbox.get("adapter").cloned(),
                ));
            }
            "Model" => {
                let id = bindings::id_in(client, session.environment(), root, &reference).await?;
                let binding=client.query_opt("SELECT definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled", &[&session.environment(),&id.to_string()]).await?;
                if let Some(binding) = binding {
                    let status = crate::models::NativeModels::registration_readiness_in(
                        client,
                        session,
                        &id.to_string(),
                        &binding.get(0),
                    )
                    .await?;
                    if status["ready"] != true {
                        ready = false;
                    }
                    component["readiness"] = status;
                } else {
                    ready = false;
                }
            }
            "RetrievalCollection" => {
                let id = bindings::id_in(client, session.environment(), root, &reference).await?;
                let search = crate::search::NativeSearch {
                    db: db.clone(),
                    models: crate::models::NativeModels::new(db.clone())?,
                };
                client
                    .batch_execute("SAVEPOINT native_runtime_readiness")
                    .await?;
                let available=async {
                    let row=client.query_opt("SELECT p.service_binding_revision,c.resource_class FROM moria.native_search_collections c JOIN moria.native_search_projector_bindings p ON p.id=c.projector_binding AND p.enabled WHERE c.environment=$1 AND c.resource=$2 AND c.enabled", &[&session.environment(),&id.to_string()]).await?.ok_or_else(||anyhow::anyhow!("SEARCH_PROJECTOR_UNAVAILABLE"))?;
                    crate::jobs::RegisteredService::load_current_in(client,session.environment(),id,"search",&row.get::<_,String>(0)).await?;
                    ensure!(client.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_workers w JOIN moria.runtime_resource_classes c ON c.name=$1 AND c.enabled JOIN moria.indexing_instance i ON i.incarnation=w.incarnation WHERE w.enabled AND w.expires_at>clock_timestamp() AND $1=ANY(w.resource_classes) AND 'search'=ANY(w.domains))", &[&row.get::<_,String>(1)]).await?.get::<_,bool>(0),"SEARCH_PROJECTOR_UNAVAILABLE");
                    search.visibility_in(client,session,&id.to_string()).await
                }.await;
                if available.is_err() {
                    client
                        .batch_execute("ROLLBACK TO SAVEPOINT native_runtime_readiness")
                        .await?;
                }
                client
                    .batch_execute("RELEASE SAVEPOINT native_runtime_readiness")
                    .await?;
                if available.is_err() {
                    ready = false;
                }
                component["readiness"] = match available {
                    Ok(value) => json!({"ready":true,"visibility":value}),
                    Err(error) => {
                        json!({"ready":false,"reason":crate::serving::error_code(&error)})
                    }
                };
            }
            _ => {}
        }
    }
    value["ready"] = ready.into();
    value["managedCellReady"] = managed.into();
    let anchor = fingerprint(&json!([release["digest"], actual, value]))?;
    Ok(RuntimeStatus {
        value,
        artifacts,
        frontends,
        anchor,
    })
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DatasetProfile {
    pub namespace: IndexNamespace,
    pub iceberg_uuid: Uuid,
    pub markings: Vec<String>,
}

/// Immutable scalar schema IDs are assigned in canonical UTF-16 field-name order.
/// Evolution requires a separately proved protocol; existing columns cannot be reinterpreted.
pub(crate) fn dataset_schema(columns: &Value) -> Result<(Value, Value)> {
    let descriptor = values::descriptor(
        &json!({"type":"object","properties":columns}),
        &Value::Null,
        0,
    )?;
    let columns = descriptor["properties"]
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("DATASET_BINDING_INVALID"))?;
    ensure!(
        !columns.is_empty() && columns.len() <= 10000,
        "DATASET_BINDING_INVALID"
    );
    let mut ordered = columns.iter().collect::<Vec<_>>();
    ordered.sort_by(|a, b| a.0.encode_utf16().cmp(b.0.encode_utf16()));
    fn scalar(ty: &Value) -> Result<Value> {
        Ok(json!(match ty["type"].as_str().unwrap_or_default() {
            "string" => "string".to_owned(),
            "boolean" => "boolean".to_owned(),
            "int64" => "long".to_owned(),
            "float64" => "double".to_owned(),
            "date" => "date".to_owned(),
            "timestamp" => "timestamptz".to_owned(),
            "uid" => "uuid".to_owned(),
            "decimal" => format!("decimal({}, {})", ty["precision"], ty["scale"]),
            _ => anyhow::bail!("DATASET_TYPE_PROFILE_PENDING"),
        }))
    }
    let mut next_id = 0;
    let fields = ordered.into_iter().map(|(name,ty)| {
        next_id+=1;let id=next_id;
        let primitive=if ty["type"]=="array" {next_id+=1;ensure!(ty["items"]["nullable"]==false,"DATASET_BINDING_INVALID");json!({"type":"list","element-id":next_id,"element-required":true,"element":scalar(&ty["items"])?})}else{scalar(ty)?};
        Ok(json!({"id":id,"name":name,"required":ty["nullable"]==false,"type":primitive}))
    }).collect::<Result<Vec<_>>>()?;
    let schema = json!({"type":"struct","schema-id":0,"identifier-field-ids":[],"fields":fields});
    let _: iceberg::spec::Schema = serde_json::from_value(schema.clone())?;
    Ok((json!(columns), schema))
}

pub(super) async fn retain_classification_in(
    client: &Client,
    session: &VerifiedSession,
    resource: Uuid,
    labels: &[String],
) -> Result<()> {
    let state = Authority::current(client, session).await?;
    authorize(&state.identity, "instance", "manage", &[])?;
    let floor =
        Authority::resource_access(client, session, &resource.to_string(), "publish").await?;
    ensure!(
        labels.len() <= 256
            && floor.iter().all(|label| labels.contains(label))
            && labels.iter().all(|label| !label.is_empty()
                && label.len() <= 180
                && state.identity.markings.contains(label)),
        "MARKING_DOWNGRADE"
    );
    client.execute("INSERT INTO moria.resource_data_labels(environment,resource,marking) SELECT $1,$2,unnest($3::text[]) ON CONFLICT DO NOTHING", &[&session.environment(),&resource.to_string(),&labels]).await?;
    Ok(())
}
pub(crate) fn validate_dataset_primary_key(columns: &Value, primary_key: &[String]) -> Result<()> {
    ensure!(
        primary_key.len() <= 256
            && primary_key
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == primary_key.len()
            && primary_key.iter().all(|key| columns
                .get(key)
                .is_some_and(|descriptor| descriptor["nullable"] == false)),
        "DATASET_PRIMARY_KEY_INVALID"
    );
    Ok(())
}

/// Configuration authority creates metadata only. Actual Dataset IO still requires an exact live lease.
pub(crate) async fn install_dataset_in(
    client: &Client,
    session: &VerifiedSession,
    resource: Uuid,
    reference: &str,
    release: &str,
    declaration: &Value,
    profile: &DatasetProfile,
) -> Result<String> {
    ensure!(
        declaration["kind"] == "Dataset" && profile.markings.len() <= 256,
        "DATASET_BINDING_INVALID"
    );
    for value in [
        &profile.namespace.warehouse_id,
        &profile.namespace.table_id,
        &profile.namespace.revision,
    ] {
        ensure!(
            !value.is_empty()
                && value.len() <= 128
                && value.as_bytes()[0].is_ascii_alphanumeric()
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'),
            "DATASET_BINDING_INVALID"
        );
    }
    let state = Authority::current(client, session).await?;
    authorize(&state.identity, "instance", "manage", &[])?;
    let labels =
        Authority::resource_access(client, session, &resource.to_string(), "publish").await?;
    ensure!(
        labels.iter().all(|label| profile.markings.contains(label)),
        "MARKING_DOWNGRADE"
    );
    ensure!(
        profile
            .markings
            .iter()
            .all(|label| state.identity.markings.contains(label)),
        "FORBIDDEN"
    );
    let (columns, schema) = dataset_schema(&declaration["spec"]["columns"])?;
    let primary_key: Vec<String> = declaration["spec"]
        .get("primaryKey")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or_default();
    let write_modes: Vec<String> =
        serde_json::from_value(declaration["spec"]["writeModes"].clone())?;
    validate_dataset_primary_key(&columns, &primary_key)?;
    ensure!(
        !write_modes.is_empty()
            && write_modes.len() <= 4
            && write_modes
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == write_modes.len()
            && write_modes
                .iter()
                .all(|mode| ["replace", "append", "upsert", "delete"].contains(&mode.as_str())),
        "DATASET_WRITE_MODE_INVALID"
    );
    ensure!(
        !write_modes
            .iter()
            .any(|mode| mode == "upsert" || mode == "delete")
            || !primary_key.is_empty(),
        "DATASET_PRIMARY_KEY_MISSING"
    );
    let key_schema_hash = fingerprint(
        &json!({"version":1,"keys":primary_key.iter().map(|key|json!({"name":key,"descriptor":columns[key]})).collect::<Vec<_>>()}),
    )?;
    let physical = configured_binding_fingerprint(&profile.namespace)?;
    // The provider factory fingerprint includes IO limits. Persist the stable physical identity as well.
    let physical_identity = fingerprint(&configured_physical_binding(&profile.namespace)?)?;
    let revision = fingerprint(
        &json!({"resource":resource,"reference":reference,"release":release,"columns":columns,"schema":schema,"primaryKey":primary_key,"writeModes":write_modes,"namespace":profile.namespace,"icebergUuid":profile.iceberg_uuid,"markings":profile.markings,"provider":physical}),
    )?;
    if let Some(row) = client.query_opt("SELECT environment,resource,purpose,iceberg_uuid,markings,revision FROM moria.native_storage_namespaces WHERE warehouse_id=$1 AND table_id=$2 FOR UPDATE", &[&profile.namespace.warehouse_id,&profile.namespace.table_id]).await? {
        ensure!(row.get::<_,String>(0)==session.environment() && row.get::<_,String>(1)==resource.to_string() && row.get::<_,String>(2)=="dataset-source" && row.get::<_,Option<Uuid>>(3)==Some(profile.iceberg_uuid),"DATASET_BINDING_INVALID");
        ensure!(row.get::<_,Vec<String>>(4).iter().all(|label|profile.markings.contains(label)),"MARKING_DOWNGRADE");
    }
    client.execute("INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,iceberg_uuid,markings) VALUES($1,$2,$3,$4,$5,'dataset-source',$6,$7) ON CONFLICT(warehouse_id,table_id) DO UPDATE SET revision=EXCLUDED.revision,markings=EXCLUDED.markings,enabled=true", &[&session.environment(),&profile.namespace.warehouse_id,&profile.namespace.table_id,&resource.to_string(),&profile.namespace.revision,&profile.iceberg_uuid,&profile.markings]).await?;
    client.execute("INSERT INTO moria.native_dataset_bindings(environment,resource,reference,revision,columns,iceberg_schema,warehouse_id,table_id,namespace_revision,physical_binding_fingerprint,source_digest,primary_key,write_modes,key_schema_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT(environment,resource) DO UPDATE SET reference=EXCLUDED.reference,revision=EXCLUDED.revision,columns=EXCLUDED.columns,iceberg_schema=EXCLUDED.iceberg_schema,primary_key=EXCLUDED.primary_key,write_modes=EXCLUDED.write_modes,key_schema_hash=EXCLUDED.key_schema_hash,warehouse_id=EXCLUDED.warehouse_id,table_id=EXCLUDED.table_id,namespace_revision=EXCLUDED.namespace_revision,physical_binding_fingerprint=EXCLUDED.physical_binding_fingerprint,source_digest=EXCLUDED.source_digest,enabled=true", &[&session.environment(),&resource,&reference,&revision,&columns,&schema,&profile.namespace.warehouse_id,&profile.namespace.table_id,&profile.namespace.revision,&physical_identity,&release,&primary_key,&write_modes,&key_schema_hash]).await?;
    client.execute("INSERT INTO moria.dataset_heads(environment,dataset) VALUES($1,$2) ON CONFLICT DO NOTHING", &[&session.environment(),&resource.to_string()]).await?;
    Ok(revision)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dataset_schema_is_stable_exact_and_bounded() {
        let columns = json!({"z":{"type":"decimal","nullable":true,"precision":20,"scale":4},"a":{"type":"int64","nullable":false},"time":{"type":"timestamp","nullable":false}});
        let (_, schema) = dataset_schema(&columns).unwrap();
        assert_eq!(
            schema["fields"][0],
            json!({"id":1,"name":"a","required":true,"type":"long"})
        );
        assert_eq!(schema["fields"][1]["type"], "timestamptz");
        assert_eq!(schema["fields"][2]["type"], "decimal(20, 4)");
        let (_,schema)=dataset_schema(&json!({"id":{"type":"uid","nullable":false},"labels":{"type":"array","nullable":true,"items":{"type":"string","nullable":false}}})).unwrap();
        assert_eq!(schema["fields"][0]["type"], "uuid");
        assert_eq!(
            schema["fields"][1]["type"],
            json!({"type":"list","element-id":3,"element-required":true,"element":"string"})
        );
    }
}
