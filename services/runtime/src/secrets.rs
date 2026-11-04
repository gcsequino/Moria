//! A declaration constrains use; only an explicit operator approval selects host material.
use crate::{
    authority::{Authority, VerifiedSession},
    authorization::authorize,
    canonical::fingerprint,
    filesystem::NativeSources,
};
use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use tokio_postgres::Client;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OwnerKind {
    Model,
    Connector,
}
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SecretRequest {
    pub release: String,
    pub consumer: Uuid,
    pub secret: Uuid,
    pub owner_kind: OwnerKind,
    pub owner_profile: Uuid,
    pub owner_revision: String,
    pub destination_fingerprint: String,
    pub material_ref: String,
}
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SecretApprovalRef {
    pub id: Uuid,
    pub generation: String,
    pub incarnation: Uuid,
}
pub(crate) struct SecretGrant {
    request: SecretRequest,
    reference: SecretApprovalRef,
    labels: BTreeSet<String>,
    material_revision: String,
}
impl SecretGrant {
    pub(crate) fn snapshot(&self) -> SecretApprovalRef {
        self.reference.clone()
    }
    pub(crate) fn operation(&self) -> String {
        format!("{}:use", self.request.secret)
    }
    pub(crate) fn markings(&self) -> &BTreeSet<String> {
        &self.labels
    }
    pub(crate) fn material_ref(&self) -> &str {
        &self.request.material_ref
    }
    pub(crate) fn material_revision(&self) -> &str {
        &self.material_revision
    }
    pub(crate) async fn check_in(
        &self,
        client: &Client,
        session: &VerifiedSession,
    ) -> Result<BTreeSet<String>> {
        let current = grant_in(client, session, &self.request).await?;
        ensure!(
            current.reference == self.reference,
            "SECRET_APPROVAL_REVOKED"
        );
        let labels = self
            .labels
            .union(&current.labels)
            .cloned()
            .collect::<BTreeSet<_>>();
        let state = Authority::current(client, session).await?;
        authorize(
            &state.identity,
            &self.request.secret.to_string(),
            "use",
            &labels.iter().cloned().collect::<Vec<_>>(),
        )?;
        Authority::finish(client, session, &state).await?;
        Ok(labels)
    }
    pub(crate) async fn check_registered_in(
        &self,
        client: &Client,
        service: &crate::jobs::RegisteredService,
        accepted_binding: Uuid,
    ) -> Result<BTreeSet<String>> {
        let row = client.query_opt("SELECT s.authority,s.target_revision,b.resource FROM moria.runtime_execution_services s JOIN moria.runtime_execution_bindings b ON b.id=s.binding WHERE b.id=$1 AND b.environment=$2 AND b.enabled", &[&accepted_binding,&service.environment()]).await?.ok_or_else(|| anyhow::anyhow!("SERVICE_PURPOSE_REVOKED"))?;
        ensure!(
            row.get::<_, Uuid>(0) == service.id()
                && row.get::<_, String>(1) == service.binding_revision()
                && row.get::<_, Uuid>(2) == service.target(),
            "SERVICE_PURPOSE_REVOKED"
        );
        client
            .query_one(
                "SELECT moria.runtime_service_binding_guard($1)",
                &[&accepted_binding],
            )
            .await?;
        let current = grant_registered_in(client, service, &self.request).await?;
        ensure!(
            current.reference == self.reference,
            "SECRET_APPROVAL_REVOKED"
        );
        let labels = self
            .labels
            .union(&current.labels)
            .cloned()
            .collect::<BTreeSet<_>>();
        let state = service.state_in(client).await?;
        authorize(
            &state.identity,
            &self.request.secret.to_string(),
            "use",
            &labels.iter().cloned().collect::<Vec<_>>(),
        )?;
        Ok(labels)
    }
}
fn field<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value[name]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("SECRET_BINDING_INVALID"))
}
fn reference(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .enumerate()
                .all(|(i, b)| b.is_ascii_alphabetic()
                    || b == b'_'
                    || (i > 0 && b.is_ascii_digit())),
        "SECRET_BINDING_INVALID"
    );
    Ok(())
}
fn endpoint(binding: &Value) -> Result<reqwest::Url> {
    let name = field(binding, "endpointRef")?;
    reference(name)?;
    let url = reqwest::Url::parse(
        &std::env::var(name).map_err(|_| anyhow::anyhow!("SECRET_CONFIGURATION_UNAVAILABLE"))?,
    )?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none(),
        "SECRET_BINDING_INVALID"
    );
    Ok(url)
}
pub(crate) async fn owner_metadata_in(
    client: &Client,
    environment: &str,
    kind: OwnerKind,
    profile: Uuid,
    consumer: Uuid,
) -> Result<(String, String, String)> {
    let (revision, binding) = match kind {
        OwnerKind::Model => {
            let row = client.query_opt("SELECT revision,definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled", &[&environment,&profile.to_string()]).await?.ok_or_else(|| anyhow::anyhow!("SECRET_APPROVAL_REVOKED"))?;
            (row.get::<_, String>(0), row.get::<_, Value>(1))
        }
        OwnerKind::Connector => {
            let row = client.query_opt("SELECT revision,jsonb_build_object('id',id,'revision',revision,'endpointRef',endpoint_ref,'secretRef',secret_ref,'methods',methods,'paths',paths,'markings',markings,'allowedMarkings',allowed_markings) FROM moria.runtime_pipeline_destinations WHERE id=$1 AND environment=$2 AND resource=$3 AND enabled", &[&profile,&environment,&consumer]).await?.ok_or_else(|| anyhow::anyhow!("SECRET_APPROVAL_REVOKED"))?;
            (row.get::<_, String>(0), row.get::<_, Value>(1))
        }
    };
    let material = field(&binding, "secretRef")?.to_owned();
    reference(&material)?;
    let destination =
        fingerprint(&json!({"binding":binding,"endpoint":endpoint(&binding)?.as_str()}))?;
    Ok((revision, destination, material))
}
async fn active(
    client: &Client,
    environment: &str,
    resource: Uuid,
    kind: &str,
) -> Result<(String, Value)> {
    let (_, release, value) = NativeSources::active_definition_in(client, environment, resource)
        .await?
        .ok_or_else(|| anyhow::anyhow!("SECRET_SOURCE_CHANGED"))?;
    ensure!(value["kind"] == kind, "SECRET_SOURCE_CHANGED");
    Ok((release, value))
}
async fn resolve(client: &Client, release: &str, value: &str) -> Result<Uuid> {
    Ok(NativeSources::resource_in(client, release, value)
        .await?
        .parse()?)
}
async fn normalized_secret_in(
    client: &Client,
    release: &str,
    definition: &Value,
) -> Result<(String, BTreeSet<Uuid>)> {
    ensure!(
        definition["kind"] == "SecretReference",
        "SECRET_SOURCE_CHANGED"
    );
    let binding = field(&definition["spec"], "binding")?;
    ensure!(
        !binding.is_empty() && binding.len() <= 1024,
        "SECRET_BINDING_INVALID"
    );
    let values = definition["spec"]["consumers"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("SECRET_BINDING_INVALID"))?;
    ensure!(
        !values.is_empty() && values.len() <= 256,
        "SECRET_BINDING_INVALID"
    );
    let mut consumers = BTreeSet::new();
    for value in values {
        consumers.insert(
            resolve(
                client,
                release,
                value
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("SECRET_BINDING_INVALID"))?,
            )
            .await?,
        );
    }
    Ok((
        fingerprint(&json!({"binding":binding,"consumers":consumers}))?,
        consumers,
    ))
}
async fn definition_hash_in(
    client: &Client,
    environment: &str,
    request: &SecretRequest,
) -> Result<String> {
    let (release, definition) =
        active(client, environment, request.secret, "SecretReference").await?;
    let (definition_hash, consumers) = normalized_secret_in(client, &release, &definition).await?;
    let (pinned_release, pinned) =
        NativeSources::definition_in(client, &request.release, request.secret).await?;
    ensure!(
        definition_hash
            == normalized_secret_in(client, &pinned_release, &pinned)
                .await?
                .0,
        "SECRET_SOURCE_CHANGED"
    );
    ensure!(
        consumers.contains(&request.consumer),
        "SECRET_CONSUMER_DENIED"
    );
    match request.owner_kind {
        OwnerKind::Model => {
            let (model_release, model) =
                active(client, environment, request.owner_profile, "Model").await?;
            ensure!(
                resolve(client, &model_release, field(&model["spec"], "provider")?).await?
                    == request.consumer,
                "SECRET_CONSUMER_DENIED"
            );
            let (provider_release, provider) =
                active(client, environment, request.consumer, "ModelProvider").await?;
            ensure!(
                resolve(
                    client,
                    &provider_release,
                    field(&provider["spec"], "secret")?
                )
                .await?
                    == request.secret,
                "SECRET_CONSUMER_DENIED"
            );
        }
        OwnerKind::Connector => {
            let (connection_release, connection) =
                active(client, environment, request.consumer, "Connection").await?;
            let values = connection["spec"]["secrets"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("SECRET_BINDING_INVALID"))?;
            ensure!(values.len() <= 256, "SECRET_BINDING_INVALID");
            let mut declared = BTreeSet::new();
            for value in values {
                declared.insert(
                    resolve(
                        client,
                        &connection_release,
                        value
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("SECRET_BINDING_INVALID"))?,
                    )
                    .await?,
                );
            }
            ensure!(declared.contains(&request.secret), "SECRET_CONSUMER_DENIED");
        }
    }
    let actual = owner_metadata_in(
        client,
        environment,
        request.owner_kind,
        request.owner_profile,
        request.consumer,
    )
    .await?;
    ensure!(
        actual
            == (
                request.owner_revision.clone(),
                request.destination_fingerprint.clone(),
                request.material_ref.clone()
            ),
        "SECRET_APPROVAL_REVOKED"
    );
    Ok(definition_hash)
}
async fn approval_in(
    client: &Client,
    environment: &str,
    request: &SecretRequest,
) -> Result<(SecretApprovalRef, BTreeSet<String>, String)> {
    let definition = definition_hash_in(client, environment, request).await?;
    let kind = match request.owner_kind {
        OwnerKind::Model => "model",
        OwnerKind::Connector => "connector",
    };
    let row = client.query_opt("SELECT a.id,a.generation::text,a.incarnation,a.markings,a.material_revision FROM moria.native_secret_approvals a JOIN moria.indexing_instance i ON i.incarnation=a.incarnation WHERE a.environment=$1 AND a.secret=$2 AND a.consumer=$3 AND a.owner_kind=$4 AND a.owner_profile=$5 AND a.owner_revision=$6 AND a.destination_fingerprint=$7 AND a.material_ref=$8 AND a.secret_definition_hash=$9 AND a.enabled", &[&environment,&request.secret,&request.consumer,&kind,&request.owner_profile,&request.owner_revision,&request.destination_fingerprint,&request.material_ref,&definition]).await?.ok_or_else(|| anyhow::anyhow!("SECRET_APPROVAL_REQUIRED"))?;
    Ok((
        SecretApprovalRef {
            id: row.get(0),
            generation: row.get(1),
            incarnation: row.get(2),
        },
        row.get::<_, Vec<String>>(3).into_iter().collect(),
        row.get(4),
    ))
}
pub(crate) async fn grant_in(
    client: &Client,
    session: &VerifiedSession,
    request: &SecretRequest,
) -> Result<SecretGrant> {
    let state = Authority::current(client, session).await?;
    let (secret_release, _) = active(
        client,
        session.environment(),
        request.secret,
        "SecretReference",
    )
    .await?;
    let mut labels = Authority::resource_release_access(
        client,
        session,
        &request.secret.to_string(),
        "use",
        &secret_release,
    )
    .await?
    .into_iter()
    .collect::<BTreeSet<_>>();
    let (pinned_release, _) =
        NativeSources::definition_in(client, &request.release, request.secret).await?;
    if pinned_release != secret_release {
        labels.extend(
            NativeSources::declaration_labels_in(client, session, request.secret, &pinned_release)
                .await?,
        );
    }
    let (reference, material_labels, material_revision) =
        approval_in(client, session.environment(), request).await?;
    labels.extend(material_labels);
    authorize(
        &state.identity,
        &request.secret.to_string(),
        "use",
        &labels.iter().cloned().collect::<Vec<_>>(),
    )?;
    Authority::finish(client, session, &state).await?;
    Ok(SecretGrant {
        request: request.clone(),
        reference,
        labels,
        material_revision,
    })
}
/// Metadata admission only; this neither creates a session nor reads material.
pub(crate) async fn grant_registered_in(
    client: &Client,
    service: &crate::jobs::RegisteredService,
    request: &SecretRequest,
) -> Result<SecretGrant> {
    let state = service.state_in(client).await?;
    let mut labels = service
        .resource_labels_in(client, &request.secret.to_string(), "use")
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let (secret_release, _) = active(
        client,
        service.environment(),
        request.secret,
        "SecretReference",
    )
    .await?;
    labels.extend(
        NativeSources::registered_declaration_labels_in(
            client,
            service,
            request.secret,
            &secret_release,
        )
        .await?,
    );
    let (pinned_release, _) =
        NativeSources::definition_in(client, &request.release, request.secret).await?;
    if pinned_release != secret_release {
        labels.extend(
            NativeSources::registered_declaration_labels_in(
                client,
                service,
                request.secret,
                &pinned_release,
            )
            .await?,
        );
    }
    let (reference, material_labels, material_revision) =
        approval_in(client, service.environment(), request).await?;
    labels.extend(material_labels);
    authorize(
        &state.identity,
        &request.secret.to_string(),
        "use",
        &labels.iter().cloned().collect::<Vec<_>>(),
    )?;
    let current = service.state_in(client).await?;
    ensure!(
        state.security_epoch == current.security_epoch
            && state.instance_epoch == current.instance_epoch,
        "AUTHORIZATION_CHANGED"
    );
    Ok(SecretGrant {
        request: request.clone(),
        reference,
        labels,
        material_revision,
    })
}
/// Discover Source constraints; a missing managed declaration never falls back to an operator-only route.
pub(crate) struct ModelSource {
    pub request: Option<SecretRequest>,
    pub declarations: Vec<(Uuid, String)>,
}
pub(crate) async fn model_source_in(
    client: &Client,
    environment: &str,
    resource: &str,
    binding: &Value,
) -> Result<Option<ModelSource>> {
    let Ok(model_id) = Uuid::parse_str(resource) else {
        return Ok(None);
    };
    let source = NativeSources::active_definition_in(client, environment, model_id).await?;
    let Some((_, release, model)) = source else {
        ensure!(!client.query_one("SELECT EXISTS(SELECT 1 FROM moria.fs_resource_identities WHERE resource_id=$1)", &[&model_id]).await?.get::<_,bool>(0), "SECRET_SOURCE_CHANGED");
        return Ok(None);
    };
    ensure!(model["kind"] == "Model", "SECRET_SOURCE_CHANGED");
    crate::models::NativeModels::validate_declared_route(&model, binding)?;
    let consumer = resolve(client, &release, field(&model["spec"], "provider")?).await?;
    let (provider_release, provider) =
        active(client, environment, consumer, "ModelProvider").await?;
    let (pinned_provider_release, pinned_provider) =
        NativeSources::definition_in(client, &release, consumer).await?;
    ensure!(
        provider["spec"] == pinned_provider["spec"],
        "SECRET_SOURCE_CHANGED"
    );
    ensure!(
        endpoint(binding)? == reqwest::Url::parse(field(&provider["spec"], "endpoint")?)?
            && binding["destination"] == provider["spec"]["destination"],
        "SECRET_SOURCE_CHANGED"
    );
    let policy_id = resolve(
        client,
        &provider_release,
        field(&provider["spec"], "destinationPolicy")?,
    )
    .await?;
    let (policy_release, policy) = active(client, environment, policy_id, "Policy").await?;
    let (pinned_policy_release, pinned_policy) =
        NativeSources::definition_in(client, &pinned_provider_release, policy_id).await?;
    ensure!(
        pinned_policy["kind"] == "Policy"
            && pinned_policy["spec"]["type"] == "modelDestination"
            && policy["spec"]["type"] == "modelDestination",
        "SECRET_SOURCE_CHANGED"
    );
    let trusted: crate::authorization::DestinationPolicy =
        serde_json::from_value(binding["destinationPolicy"].clone())?;
    ensure!(
        trusted.allowed_destinations
            == serde_json::from_value::<BTreeSet<String>>(
                policy["spec"]["allowedDestinations"].clone()
            )?
            && trusted.allowed_markings
                == serde_json::from_value::<BTreeSet<String>>(
                    policy["spec"]["allowedMarkings"].clone()
                )?,
        "SECRET_SOURCE_CHANGED"
    );
    ensure!(
        trusted.allowed_destinations
            == serde_json::from_value::<BTreeSet<String>>(
                pinned_policy["spec"]["allowedDestinations"].clone()
            )?
            && trusted.allowed_markings
                == serde_json::from_value::<BTreeSet<String>>(
                    pinned_policy["spec"]["allowedMarkings"].clone()
                )?,
        "SECRET_SOURCE_CHANGED"
    );
    let declarations = BTreeSet::from([
        (model_id, release.clone()),
        (consumer, provider_release.clone()),
        (consumer, pinned_provider_release.clone()),
        (policy_id, policy_release),
        (policy_id, pinned_policy_release),
    ])
    .into_iter()
    .collect();
    let request = if let Some(secret) = provider["spec"]["secret"].as_str() {
        let secret = resolve(client, &provider_release, secret).await?;
        let (owner_revision, destination_fingerprint, material_ref) =
            owner_metadata_in(client, environment, OwnerKind::Model, model_id, consumer).await?;
        Some(SecretRequest {
            release,
            consumer,
            secret,
            owner_kind: OwnerKind::Model,
            owner_profile: model_id,
            owner_revision,
            destination_fingerprint,
            material_ref,
        })
    } else {
        None
    };
    Ok(Some(ModelSource {
        request,
        declarations,
    }))
}

pub(crate) async fn model_request_in(
    client: &Client,
    environment: &str,
    resource: &str,
    binding: &Value,
) -> Result<Option<SecretRequest>> {
    Ok(model_source_in(client, environment, resource, binding)
        .await?
        .and_then(|source| source.request))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ApprovalInput {
    request_key: String,
    environment: String,
    binding: SecretRequest,
    material_revision: String,
    markings: Vec<String>,
}
async fn approve_in(client: &Client, input: ApprovalInput) -> Result<Value> {
    client
        .query_one("SELECT moria.runtime_recovery_operator()", &[])
        .await?;
    ensure!(
        !input.request_key.is_empty()
            && input.request_key.len() <= 256
            && !input.environment.is_empty()
            && input.environment.len() <= 1024
            && !input.material_revision.is_empty()
            && input.material_revision.len() <= 1024
            && input.markings.len() <= 256
            && input
                .markings
                .iter()
                .all(|v| !v.is_empty() && v.len() <= 256),
        "SECRET_APPROVAL_INVALID"
    );
    let mut markings = input.markings;
    markings.sort();
    markings.dedup();
    let request = input.binding;
    let definition = definition_hash_in(client, &input.environment, &request).await?;
    let kind = match request.owner_kind {
        OwnerKind::Model => "model",
        OwnerKind::Connector => "connector",
    };
    let scope = fingerprint(&json!([
        input.environment,
        request.secret,
        request.consumer,
        kind,
        request.owner_profile
    ]))?;
    client
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&scope],
        )
        .await?;
    let incarnation: Uuid = client
        .query_one("SELECT incarnation FROM moria.indexing_instance", &[])
        .await?
        .get(0);
    let existing = client.query_opt("SELECT id,generation::text,incarnation,enabled,environment,secret,consumer,source_release,secret_definition_hash,owner_kind,owner_profile,owner_revision,destination_fingerprint,material_ref,material_revision,markings FROM moria.native_secret_approvals WHERE request_key=$1", &[&input.request_key]).await?;
    if let Some(row) = existing {
        ensure!(
            row.get::<_, String>("environment") == input.environment
                && row.get::<_, Uuid>("secret") == request.secret
                && row.get::<_, Uuid>("consumer") == request.consumer
                && row.get::<_, String>("source_release") == request.release
                && row.get::<_, String>("secret_definition_hash") == definition
                && row.get::<_, String>("owner_kind") == kind
                && row.get::<_, Uuid>("owner_profile") == request.owner_profile
                && row.get::<_, String>("owner_revision") == request.owner_revision
                && row.get::<_, String>("destination_fingerprint")
                    == request.destination_fingerprint
                && row.get::<_, String>("material_ref") == request.material_ref
                && row.get::<_, String>("material_revision") == input.material_revision
                && row.get::<_, Vec<String>>("markings") == markings
                && row.get::<_, Uuid>("incarnation") == incarnation,
            "SECRET_APPROVAL_CONFLICT"
        );
        return Ok(
            json!({"id":row.get::<_,Uuid>("id"),"generation":row.get::<_,String>("generation"),"incarnation":incarnation,"enabled":row.get::<_,bool>("enabled")}),
        );
    }
    client.execute("UPDATE moria.native_secret_approvals SET enabled=false WHERE environment=$1 AND secret=$2 AND consumer=$3 AND owner_kind=$4 AND owner_profile=$5 AND enabled", &[&input.environment,&request.secret,&request.consumer,&kind,&request.owner_profile]).await?;
    let row = client.query_one("INSERT INTO moria.native_secret_approvals(request_key,environment,secret,consumer,source_release,secret_definition_hash,owner_kind,owner_profile,owner_revision,destination_fingerprint,material_ref,material_revision,markings,incarnation,approved_role) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,session_user) RETURNING id,generation::text", &[&input.request_key,&input.environment,&request.secret,&request.consumer,&request.release,&definition,&kind,&request.owner_profile,&request.owner_revision,&request.destination_fingerprint,&request.material_ref,&input.material_revision,&markings,&incarnation]).await?;
    Ok(
        json!({"id":row.get::<_,Uuid>(0),"generation":row.get::<_,String>(1),"incarnation":incarnation,"enabled":true}),
    )
}

pub(crate) async fn cli(db: crate::database::Database, args: Vec<String>) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    tokio::io::stdin()
        .take(65537)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 65536, "INPUT_INVALID");
    let input: Value = serde_json::from_slice(&bytes)?;
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    tx.query_one("SELECT moria.runtime_recovery_operator()", &[])
        .await?;
    let result = match args.first().map(String::as_str) {
        Some("approve") => approve_in(tx.client(), serde_json::from_value(input)?).await?,
        Some("disable") => {
            let id: Uuid = field(&input, "id")?.parse()?;
            let row = tx.query_opt("UPDATE moria.native_secret_approvals SET enabled=false WHERE id=$1 RETURNING id,generation::text,incarnation", &[&id]).await?.ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?;
            json!({"id":row.get::<_,Uuid>(0),"generation":row.get::<_,String>(1),"incarnation":row.get::<_,Uuid>(2),"enabled":false})
        }
        Some("inspect-model") => {
            let environment = field(&input, "environment")?;
            let model = field(&input, "model")?;
            let binding:Value = tx.query_opt("SELECT definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled", &[&environment,&model]).await?.ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?.get(0);
            json!({"binding":model_request_in(tx.client(), environment, model, &binding).await?})
        }
        _ => bail!("INPUT_INVALID"),
    };
    tx.commit().await?;
    println!("{result}");
    Ok(())
}

/// Operator readiness metadata only, without material IO or an actor capability.
pub(crate) async fn material_revision_in(
    client: &Client,
    environment: &str,
    request: &SecretRequest,
) -> Result<Option<String>> {
    let definition = definition_hash_in(client, environment, request).await?;
    let kind = match request.owner_kind {
        OwnerKind::Model => "model",
        OwnerKind::Connector => "connector",
    };
    Ok(client.query_opt("SELECT a.material_revision FROM moria.native_secret_approvals a JOIN moria.indexing_instance i ON i.incarnation=a.incarnation WHERE a.environment=$1 AND a.secret=$2 AND a.consumer=$3 AND a.owner_kind=$4 AND a.owner_profile=$5 AND a.owner_revision=$6 AND a.destination_fingerprint=$7 AND a.material_ref=$8 AND a.secret_definition_hash=$9 AND a.enabled", &[&environment,&request.secret,&request.consumer,&kind,&request.owner_profile,&request.owner_revision,&request.destination_fingerprint,&request.material_ref,&definition]).await?.map(|row| row.get(0)))
}
