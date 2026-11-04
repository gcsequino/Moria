use crate::{
    authority::{Authority, SecuritySnapshot, VerifiedSession},
    authorization::{DestinationPolicy, authorize, authorize_destination},
    canonical::fingerprint,
    database::Database,
    serving::Admission,
};
use anyhow::{Result, bail, ensure};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    env,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ModelAdmission {
    pub markings: BTreeSet<String>,
    pub secret_approval: Option<crate::secrets::SecretApprovalRef>,
    pub secret_operation: Option<String>,
}
impl ModelAdmission {
    fn from_grant(markings: BTreeSet<String>, secret: Option<crate::secrets::SecretGrant>) -> Self {
        Self {
            markings,
            secret_approval: secret.as_ref().map(crate::secrets::SecretGrant::snapshot),
            secret_operation: secret.as_ref().map(crate::secrets::SecretGrant::operation),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Route {
    endpoint_ref: String,
    secret_ref: Option<String>,
    model: String,
    model_revision: String,
    resource_class: String,
    destination: String,
    markings: Vec<String>,
    destination_policy: DestinationPolicy,
    capabilities: BTreeSet<String>,
    dimensions: Option<usize>,
    #[serde(default = "openai_embedding_protocol")]
    embedding_protocol: String,
    #[serde(default = "model_timeout_seconds")]
    timeout_seconds: u64,
}
fn model_timeout_seconds() -> u64 {
    120
}
fn openai_embedding_protocol() -> String {
    "openai-compatible-v1".into()
}

enum RequestPurpose<'a> {
    Invoke(&'a str),
    Probe(&'a str),
}

#[derive(Clone)]
pub struct NativeModels {
    pub db: Database,
    admission: Arc<Semaphore>,
    http: reqwest::Client,
}
struct Call {
    id: Uuid,
    owner: Uuid,
    caller_execution: Option<Uuid>,
    secret: Option<crate::secrets::SecretGrant>,
    session: VerifiedSession,
    resource: String,
    route: Route,
    definition: Value,
    endpoint: reqwest::Url,
    snapshot: SecuritySnapshot,
    labels: BTreeSet<String>,
    sandbox: Option<crate::sandbox::SandboxContext>,
    projector: Option<Arc<crate::full_object_source::FullObjectSourceGrant>>,
    deadline: tokio::time::Instant,
    provider_started: AtomicBool,
    provider_finished: AtomicBool,
    _permit: OwnedSemaphorePermit,
}

impl NativeModels {
    fn binding_fingerprint(
        definition: &Value,
        endpoint: &reqwest::Url,
        material_revision: Option<&str>,
    ) -> Result<String> {
        let mut binding = json!({"binding":definition,"endpoint":endpoint.as_str()});
        if let Some(revision) = material_revision {
            binding["materialRevision"] = json!(revision);
        }
        fingerprint(&binding)
    }
    fn classify_result(value: &mut Value, labels: &BTreeSet<String>) -> Result<()> {
        ensure!(value.is_object(), "MODEL_PROTOCOL");
        value["markings"] = json!(labels);
        ensure!(
            serde_json::to_vec(value)?.len() <= 1024 * 1024,
            "MODEL_PROTOCOL"
        );
        Ok(())
    }

    /// Current configuration and actual capability evidence; this never invokes a provider.
    pub(crate) async fn registration_readiness_in(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        resource: &str,
        definition: &Value,
    ) -> Result<Value> {
        let state = Authority::current(client, session).await?;
        authorize(&state.identity, "instance", "manage", &[])?;
        Authority::resource_access(client, session, resource, "read").await?;
        let route: Route = serde_json::from_value(definition.clone())?;
        let endpoint = Self::endpoint(&route, "")?;

        let current:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled AND definition=$3)", &[&session.environment(),&resource,&definition]).await?.get(0);
        let class:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_resource_classes WHERE name=$1 AND enabled)", &[&route.resource_class]).await?.get(0);
        let secret_request =
            crate::secrets::model_request_in(client, session.environment(), resource, definition)
                .await?;
        let material_revision = if let Some(request) = &secret_request {
            crate::secrets::material_revision_in(client, session.environment(), request).await?
        } else {
            None
        };
        let secret_ready = secret_request.is_none() || material_revision.is_some();
        let hash = Self::binding_fingerprint(definition, &endpoint, material_revision.as_deref())?;
        let rows=client.query("SELECT capability FROM moria.native_model_probes p JOIN moria.native_model_calls c ON c.id=p.call_id WHERE p.environment=$1 AND p.resource=$2 AND p.definition_hash=$3 AND p.expires_at>clock_timestamp() AND c.state='succeeded'", &[&session.environment(),&resource,&hash]).await?;
        let proved = rows
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<BTreeSet<_>>();
        let mut required = route.capabilities;
        if required.contains("stream") && required.contains("tools") {
            required.insert("streamTools".into());
        }
        if required.contains("stream") && required.contains("structured") {
            required.insert("streamStructured".into());
        }
        let missing = required.difference(&proved).cloned().collect::<Vec<_>>();
        Authority::finish(client, session, &state).await?;
        Ok(
            json!({"configured":current,"ready":current&&class&&secret_ready&&missing.is_empty(),"secretReady":secret_ready,"resourceClassReady":class,"missingCapabilities":missing,"modelBindingFingerprint":hash}),
        )
    }

    pub(crate) fn validate_declared_route(declaration: &Value, definition: &Value) -> Result<()> {
        let route: Route = serde_json::from_value(definition.clone())?;
        ensure!(declaration["kind"] == "Model", "MODEL_BINDING_INVALID");
        let spec = &declaration["spec"];
        ensure!(
            spec["upstreamName"].as_str() == Some(route.model.as_str())
                && spec["versionLabel"]
                    .as_str()
                    .is_none_or(|version| version == route.model_revision)
                && !route.model.is_empty()
                && route.model.len() <= 256
                && !route.model_revision.is_empty()
                && route.model_revision.len() <= 256
                && (1..=120).contains(&route.timeout_seconds)
                && !route.destination.is_empty()
                && route.destination.len() <= 256
                && route.markings.len() <= 256,
            "MODEL_BINDING_INVALID"
        );
        let declared: BTreeSet<&str> = spec["capabilities"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("MODEL_BINDING_INVALID"))?
            .iter()
            .map(|capability| match capability.as_str() {
                Some("chat") => Ok("chat"),
                Some("tools") => Ok("tools"),
                Some("structuredOutput") => Ok("structured"),
                Some("streaming") => Ok("stream"),
                Some("embeddings") => Ok("embed"),
                _ => bail!("MODEL_BINDING_INVALID"),
            })
            .collect::<Result<_>>()?;
        ensure!(
            route
                .capabilities
                .iter()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                == declared,
            "MODEL_BINDING_INVALID"
        );
        ensure!(
            route.dimensions
                == spec["embeddingDimensions"]
                    .as_u64()
                    .map(|value| value as usize)
                && (!declared.contains("embed")
                    || route
                        .dimensions
                        .is_some_and(|value| (1..=4096).contains(&value))),
            "MODEL_BINDING_INVALID"
        );
        Ok(())
    }

    /// Validates operator configuration without granting inference or contacting its provider.
    pub(crate) async fn validated_registration_in(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        resource: &str,
        declaration: &Value,
        definition: &Value,
    ) -> Result<String> {
        let snapshot = Authority::current(client, session).await?;
        authorize(&snapshot.identity, "instance", "manage", &[])?;
        let labels = Authority::resource_access(client, session, resource, "publish").await?;
        let route: Route = serde_json::from_value(definition.clone())?;
        Self::validate_declared_route(declaration, definition)?;
        ensure!(
            labels.iter().all(|label| route.markings.contains(label)),
            "MODEL_BINDING_INVALID"
        );
        ensure!(
            client
                .query_opt(
                    "SELECT 1 FROM moria.runtime_resource_classes WHERE name=$1 AND enabled",
                    &[&route.resource_class]
                )
                .await?
                .is_some(),
            "MODEL_BINDING_INVALID"
        );
        if let Some(secret) = &route.secret_ref {
            ensure!(
                !secret.is_empty()
                    && secret.len() <= 128
                    && secret.bytes().all(|byte| byte.is_ascii_uppercase()
                        || byte.is_ascii_digit()
                        || byte == b'_')
                    && env::var(secret)
                        .is_ok_and(|value| !value.is_empty() && value.len() <= 16384),
                "MODEL_CONFIGURATION_UNAVAILABLE"
            );
        }
        authorize_destination(
            &snapshot.identity,
            &route.markings.iter().cloned().collect(),
            Some(&route.destination_policy),
            &route.destination,
        )?;
        let endpoint = Self::endpoint(&route, "")?;
        Authority::finish(client, session, &snapshot).await?;
        fingerprint(&json!({"binding":definition,"endpoint":endpoint.as_str()}))
    }

    pub fn new(db: Database) -> Result<Self> {
        Ok(Self {
            db,
            admission: Arc::new(Semaphore::new(8)),
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(120))
                .build()?,
        })
    }

    async fn authorized(
        &self,
        session: &VerifiedSession,
        resource: &str,
        route: &Route,
        labels: &BTreeSet<String>,
        expected: Option<&SecuritySnapshot>,
    ) -> Result<(
        SecuritySnapshot,
        BTreeSet<String>,
        Option<crate::secrets::SecretGrant>,
    )> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let result = Self::authorized_in(
            admission.client(),
            session,
            resource,
            route,
            labels,
            expected,
        )
        .await;
        admission.complete(result).await
    }

    async fn authorized_in(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        resource: &str,
        route: &Route,
        labels: &BTreeSet<String>,
        expected: Option<&SecuritySnapshot>,
    ) -> Result<(
        SecuritySnapshot,
        BTreeSet<String>,
        Option<crate::secrets::SecretGrant>,
    )> {
        let snapshot = Authority::current(client, session).await?;
        if let Some(expected) = expected {
            ensure!(
                snapshot.security_epoch == expected.security_epoch
                    && snapshot.instance_epoch == expected.instance_epoch,
                "AUTHORIZATION_CHANGED"
            );
        }
        let current=client.query_opt("SELECT definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&resource]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        let current_definition: Value = current.get(0);
        let current: Route = serde_json::from_value(current_definition.clone())?;
        let mut effective = labels.clone();
        let source = crate::secrets::model_source_in(
            client,
            session.environment(),
            resource,
            &current_definition,
        )
        .await?;
        if let Some(source) = &source {
            effective.extend(
                crate::filesystem::NativeSources::declaration_labels_batch_in(
                    client,
                    session,
                    &source.declarations,
                )
                .await?,
            );
        }
        let secret = if let Some(request) = source.and_then(|source| source.request) {
            let grant = crate::secrets::grant_in(client, session, &request).await?;
            effective.extend(grant.markings().iter().cloned());
            Some(grant)
        } else {
            None
        };
        effective.extend(route.markings.iter().cloned());
        effective.extend(current.markings.iter().cloned());
        effective.extend(Authority::resource_access(client, session, resource, "read").await?);
        authorize(
            &snapshot.identity,
            resource,
            "invoke",
            &effective.iter().cloned().collect::<Vec<_>>(),
        )?;
        authorize_destination(
            &snapshot.identity,
            &effective,
            Some(&route.destination_policy),
            &route.destination,
        )?;
        authorize_destination(
            &snapshot.identity,
            &effective,
            Some(&current.destination_policy),
            &route.destination,
        )?;
        Authority::finish(client, session, &snapshot).await?;
        Ok((snapshot, effective, secret))
    }

    pub(crate) async fn admission_in(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        resource: &str,
    ) -> Result<ModelAdmission> {
        let definition: Value = client.query_opt("SELECT definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled", &[&session.environment(), &resource]).await?.ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?.get(0);
        let route: Route = serde_json::from_value(definition)?;
        let (state, labels, secret) =
            Self::authorized_in(client, session, resource, &route, &BTreeSet::new(), None).await?;
        Authority::finish(client, session, &state).await?;
        Ok(ModelAdmission::from_grant(labels, secret))
    }

    async fn base_access(&self, session: &VerifiedSession, resource: &str) -> Result<()> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let result = async {
            let state = Authority::current(admission.client(), session).await?;
            let labels =
                Authority::resource_access(admission.client(), session, resource, "read").await?;
            authorize(&state.identity, resource, "invoke", &labels)?;
            Ok(())
        }
        .await;
        admission.complete(result).await
    }

    async fn begin(
        &self,
        session: &VerifiedSession,
        resource: &str,
        key: &str,
        purpose: RequestPurpose<'_>,
        input: &Value,
        labels: BTreeSet<String>,
    ) -> Result<(Option<Call>, Option<Value>)> {
        self.base_access(session, resource).await?;
        let (kind, probe) = match purpose {
            RequestPurpose::Invoke(kind) => (kind, false),
            RequestPurpose::Probe(kind) => (kind, true),
        };
        ensure!(
            !key.is_empty() && key.len() <= 256 && key.bytes().all(|b| b.is_ascii_graphic()),
            "MODEL_CALL_KEY_INVALID"
        );
        ensure!(
            serde_json::to_vec(input)?.len() <= 256 * 1024,
            "MODEL_REQUEST_INVALID"
        );
        let request_hash = fingerprint(&json!({"kind":kind,"input":input,"markings":labels}))?;
        let admission = Admission::begin(&self.db, session, false).await?;
        let existing=admission.client().query_opt("SELECT id,binding,markings,request_hash,state,result,caller_execution FROM moria.native_model_calls WHERE environment=$1 AND application=$2 AND principal=$3 AND resource=$4 AND call_key=$5",&[&session.environment(),&session.application(),&session.principal(),&resource,&key]).await?;
        if let Some(existing) = existing {
            let result = async {
                let route: Route = serde_json::from_value(existing.get(1))?;
                let mut retained: BTreeSet<String> =
                    existing.get::<_, Vec<String>>(2).into_iter().collect();
                retained.extend(
                    self.receipt_sources_in(admission.client(), session, existing.get(6))
                        .await?,
                );
                let (_, retained, _) = Self::authorized_in(
                    admission.client(),
                    session,
                    resource,
                    &route,
                    &retained,
                    None,
                )
                .await?;
                ensure!(
                    existing.get::<_, String>(3) == request_hash,
                    "IDEMPOTENCY_CONFLICT"
                );
                ensure!(
                    existing.get::<_, String>(4) == "succeeded",
                    "MODEL_CALL_UNAVAILABLE"
                );
                let mut value = existing
                    .get::<_, Option<Value>>(5)
                    .ok_or_else(|| anyhow::anyhow!("RESULT_EXPIRED"))?;
                Self::classify_result(&mut value, &retained)?;
                Ok((None, Some(value)))
            }
            .await;
            return admission.complete(result).await;
        }
        admission.complete(Ok(())).await?;
        let client = self.db.get().await?;
        let definition:Value=client.query_one("SELECT definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&resource]).await?.get(0);
        drop(client);
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        let route: Route = serde_json::from_value(definition.clone())?;
        ensure!(
            route.capabilities.contains(kind)
                || (probe
                    && match kind {
                        "streamTools" =>
                            route.capabilities.contains("stream")
                                && route.capabilities.contains("tools"),
                        "streamStructured" =>
                            route.capabilities.contains("stream")
                                && route.capabilities.contains("structured"),
                        _ => false,
                    }),
            "UNSUPPORTED_CAPABILITY"
        );
        ensure!(
            (1..=120).contains(&route.timeout_seconds),
            "MODEL_BINDING_INVALID"
        );
        let endpoint = Self::endpoint(&route, "")?;
        ensure!(
            !route.model.is_empty() && route.model.len() <= 256 && !route.model_revision.is_empty(),
            "MODEL_BINDING_INVALID"
        );
        if kind == "embed" {
            ensure!(
                route.dimensions.is_some_and(|d| (1..=4096).contains(&d)),
                "MODEL_BINDING_INVALID"
            );
        }
        let (snapshot, effective, secret) = self
            .authorized(session, resource, &route, &labels, None)
            .await?;
        if !probe {
            self.verified(
                session,
                resource,
                &definition,
                &endpoint,
                kind,
                secret
                    .as_ref()
                    .map(crate::secrets::SecretGrant::material_revision),
            )
            .await?;
        }
        let id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let effective: Vec<String> = effective.into_iter().collect();
        let admission_started = tokio::time::Instant::now();
        let admitted:Value=self.db.get().await?.query_one("SELECT moria.native_model_admit($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::text::bigint,$11::text::bigint, $12,$13,$14)",&[&id,&session.environment(),&session.application(),&session.principal(),&resource,&key,&request_hash,&definition,&effective,&snapshot.security_epoch,&snapshot.instance_epoch,&owner,&route.resource_class,&session.session_hash()]).await?.get(0);
        ensure!(admitted["accepted"] == true, "MODEL_CALL_IN_PROGRESS");
        let remaining = admitted["remainingMillis"]
            .as_str()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0 && *value <= 120000)
            .ok_or_else(|| anyhow::anyhow!("MODEL_CALL_UNAVAILABLE"))?;
        let deadline = admission_started + Duration::from_millis(remaining);
        Ok((
            Some(Call {
                id,
                owner,
                caller_execution: admitted["callerExecution"]
                    .as_str()
                    .map(Uuid::parse_str)
                    .transpose()?,
                secret,
                session: session.clone(),
                resource: resource.into(),
                route,
                definition,
                endpoint,
                snapshot,
                labels: effective.into_iter().collect(),
                sandbox: None,
                projector: None,
                deadline,
                provider_started: AtomicBool::new(false),
                provider_finished: AtomicBool::new(false),
                _permit: permit,
            }),
            None,
        ))
    }

    fn endpoint(route: &Route, path: &str) -> Result<reqwest::Url> {
        fn reference(name: &str) -> Result<String> {
            ensure!(
                !name.is_empty()
                    && name.len() <= 128
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'),
                "MODEL_BINDING_INVALID"
            );
            env::var(name).map_err(|_| anyhow::anyhow!("MODEL_CONFIGURATION_UNAVAILABLE"))
        }
        ensure!(
            ["openai-compatible-v1", "ollama-v1"].contains(&route.embedding_protocol.as_str()),
            "MODEL_BINDING_INVALID"
        );
        let text = reference(&route.endpoint_ref)?;
        let mut endpoint =
            reqwest::Url::parse(&text).map_err(|_| anyhow::anyhow!("MODEL_BINDING_INVALID"))?;
        ensure!(
            matches!(endpoint.scheme(), "http" | "https")
                && endpoint.username().is_empty()
                && endpoint.password().is_none()
                && endpoint.query().is_none()
                && endpoint.fragment().is_none(),
            "MODEL_BINDING_INVALID"
        );
        ensure!(
            endpoint.path().ends_with('/')
                && (route.embedding_protocol != "ollama-v1" || endpoint.path().ends_with("/v1/")),
            "MODEL_BINDING_INVALID"
        );
        endpoint = endpoint.join(path)?;
        Ok(endpoint)
    }

    async fn verified(
        &self,
        session: &VerifiedSession,
        resource: &str,
        definition: &Value,
        endpoint: &reqwest::Url,
        capability: &str,
        material_revision: Option<&str>,
    ) -> Result<()> {
        let digest = Self::binding_fingerprint(definition, endpoint, material_revision)?;
        ensure!(self.db.get().await?.query_one("SELECT EXISTS(SELECT 1 FROM moria.native_model_probes p JOIN moria.native_model_calls c ON c.id=p.call_id WHERE p.environment=$1 AND p.resource=$2 AND p.definition_hash=$3 AND p.capability=$4 AND p.expires_at>clock_timestamp() AND c.state='succeeded')",&[&session.environment(),&resource,&digest,&capability]).await?.get::<_,bool>(0),"UNSUPPORTED_CAPABILITY");
        Ok(())
    }

    async fn check_plan(
        &self,
        call: &Call,
        plan: &crate::model_protocol::ChatPlan,
        stream: bool,
    ) -> Result<()> {
        for (capability, required) in [
            ("tools", !plan.tools.is_empty()),
            ("structured", plan.output.is_some()),
            ("streamTools", stream && !plan.tools.is_empty()),
            ("streamStructured", stream && plan.output.is_some()),
        ] {
            if required {
                ensure!(
                    capability.starts_with("stream")
                        || call.route.capabilities.contains(capability),
                    "UNSUPPORTED_CAPABILITY"
                );
                self.verified(
                    &call.session,
                    &call.resource,
                    &call.definition,
                    &call.endpoint,
                    capability,
                    call.secret
                        .as_ref()
                        .map(crate::secrets::SecretGrant::material_revision),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn current(&self, call: &Call) -> Result<()> {
        if let Some(context) = &call.sandbox {
            context.check().await?;
        }
        if let Some(grant) = &call.projector {
            grant.check().await?;
        }
        ensure!(
            Self::endpoint(&call.route, "")? == call.endpoint,
            "MODEL_BINDING_CHANGED"
        );
        self.authorized(
            &call.session,
            &call.resource,
            &call.route,
            &call.labels,
            Some(&call.snapshot),
        )
        .await?;
        if call.caller_execution.is_some() {
            let admission = Admission::begin(&self.db, &call.session, false).await?;
            let checked = async {
                let mut labels = call.labels.clone();
                labels.extend(
                    self.receipt_sources_in(
                        admission.client(),
                        &call.session,
                        call.caller_execution,
                    )
                    .await?,
                );
                Self::authorized_in(
                    admission.client(),
                    &call.session,
                    &call.resource,
                    &call.route,
                    &labels,
                    Some(&call.snapshot),
                )
                .await?;
                Ok(())
            }
            .await;
            admission.complete(checked).await?;
        }
        let client = self.db.get().await?;
        if let Some(secret) = &call.secret {
            secret.check_in(&client, &call.session).await?;
        }
        let row=client.query_opt("SELECT c.state,c.error,c.deadline>clock_timestamp() AND b.enabled AND b.definition=$3 FROM moria.native_model_calls c JOIN moria.native_model_bindings b ON b.environment=c.environment AND b.resource=c.resource WHERE c.id=$1 AND c.owner=$2",&[&call.id,&call.owner,&call.definition]).await?.ok_or_else(||anyhow::anyhow!("MODEL_CALL_UNAVAILABLE"))?;
        ensure!(
            row.get::<_, Option<String>>(1).as_deref() != Some("MODEL_CANCELLED"),
            "MODEL_CANCELLED"
        );
        ensure!(
            row.get::<_, String>(0) == "running" && row.get::<_, bool>(2),
            "MODEL_CALL_UNAVAILABLE"
        );
        Ok(())
    }

    async fn guarded<T, F: std::future::Future<Output = Result<T>>>(
        &self,
        call: &Call,
        future: F,
    ) -> Result<T> {
        let operation = async {
            let guard = async {
                let mut timer = tokio::time::interval(Duration::from_millis(500));
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                timer.tick().await;
                loop {
                    timer.tick().await;
                    if let Err(error) = self.current(call).await {
                        break error;
                    }
                }
            };
            // Poll the request while its independent guard waits on locks the request may hold.
            tokio::select! {
                result = future => result,
                error = guard => Err(error),
            }
        };
        tokio::time::timeout_at(call.deadline, operation)
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("MODEL_INCOMPLETE")))
    }

    pub async fn capabilities(&self, session: &VerifiedSession, resource: &str) -> Result<Value> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let result = self
            .capabilities_in(admission.client(), session, resource)
            .await;
        admission.complete(result).await
    }

    pub(crate) async fn capabilities_in(
        &self,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        resource: &str,
    ) -> Result<Value> {
        let snapshot = Authority::current(client, session).await?;
        let labels = Authority::resource_access(client, session, resource, "read").await?;
        authorize(&snapshot.identity, resource, "invoke", &labels)?;
        let definition:Value=client.query_opt("SELECT definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&resource]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?.get(0);
        let route: Route = serde_json::from_value(definition.clone())?;
        let (_, markings, secret) = Self::authorized_in(
            client,
            session,
            resource,
            &route,
            &BTreeSet::new(),
            Some(&snapshot),
        )
        .await?;
        let profile = Self::profile_in(
            client,
            session.environment(),
            resource,
            &definition,
            &route,
            markings,
            secret
                .as_ref()
                .map(crate::secrets::SecretGrant::material_revision),
        )
        .await?;
        Authority::finish(client, session, &snapshot).await?;
        Ok(profile)
    }

    async fn profile_in(
        client: &tokio_postgres::Client,
        environment: &str,
        resource: &str,
        definition: &Value,
        route: &Route,
        markings: BTreeSet<String>,
        material_revision: Option<&str>,
    ) -> Result<Value> {
        let endpoint = Self::endpoint(route, "")?;
        let digest = Self::binding_fingerprint(definition, &endpoint, material_revision)?;
        let rows=client.query("SELECT capability FROM moria.native_model_probes p JOIN moria.native_model_calls c ON c.id=p.call_id WHERE p.environment=$1 AND p.resource=$2 AND p.definition_hash=$3 AND p.expires_at>clock_timestamp() AND c.state='succeeded' ORDER BY capability",&[&environment,&resource,&digest]).await?;
        let capabilities: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
        Ok(
            json!({"model":route.model,"modelRevision":route.model_revision,"dimensions":route.dimensions,"modelBindingFingerprint":digest,"markings":markings,"capabilities":capabilities,"schemaProfile":{"references":false,"regex":"linear","nodes":256}}),
        )
    }

    async fn registered_authorized_in(
        client: &tokio_postgres::Client,
        service: &crate::jobs::RegisteredService,
        resource: &str,
    ) -> Result<(
        SecuritySnapshot,
        Value,
        Route,
        BTreeSet<String>,
        Option<crate::secrets::SecretGrant>,
    )> {
        let state = service.state_in(client).await?;
        let mut markings: BTreeSet<String> = service
            .resource_labels_in(client, resource, "invoke")
            .await?
            .into_iter()
            .collect();
        authorize(
            &state.identity,
            resource,
            "invoke",
            &markings.iter().cloned().collect::<Vec<_>>(),
        )?;
        let definition:Value=client.query_opt("SELECT definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&service.environment(),&resource]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?.get(0);
        let route: Route = serde_json::from_value(definition.clone())?;
        let source =
            crate::secrets::model_source_in(client, service.environment(), resource, &definition)
                .await?;
        if let Some(source) = &source {
            markings.extend(
                crate::filesystem::NativeSources::registered_declaration_labels_batch_in(
                    client,
                    service,
                    &source.declarations,
                )
                .await?,
            );
        }
        let secret = if let Some(request) = source.and_then(|source| source.request) {
            let grant = crate::secrets::grant_registered_in(client, service, &request).await?;
            markings.extend(grant.markings().iter().cloned());
            Some(grant)
        } else {
            None
        };
        markings.extend(route.markings.iter().cloned());
        authorize(
            &state.identity,
            resource,
            "invoke",
            &markings.iter().cloned().collect::<Vec<_>>(),
        )?;
        authorize_destination(
            &state.identity,
            &markings,
            Some(&route.destination_policy),
            &route.destination,
        )?;
        Ok((state, definition, route, markings, secret))
    }

    pub(crate) async fn registered_admission_in(
        client: &tokio_postgres::Client,
        service: &crate::jobs::RegisteredService,
        resource: &str,
    ) -> Result<ModelAdmission> {
        let (state, _, _, labels, secret) =
            Self::registered_authorized_in(client, service, resource).await?;
        let current = service.state_in(client).await?;
        ensure!(
            state.security_epoch == current.security_epoch
                && state.instance_epoch == current.instance_epoch,
            "AUTHORIZATION_CHANGED"
        );
        Ok(ModelAdmission::from_grant(labels, secret))
    }

    pub(crate) async fn registered_embedding_profile_in(
        &self,
        client: &tokio_postgres::Client,
        service: &crate::jobs::RegisteredService,
        resource: &str,
    ) -> Result<Value> {
        let (state, definition, route, markings, secret) =
            Self::registered_authorized_in(client, service, resource).await?;
        let mut profile = Self::profile_in(
            client,
            service.environment(),
            resource,
            &definition,
            &route,
            markings,
            secret
                .as_ref()
                .map(crate::secrets::SecretGrant::material_revision),
        )
        .await?;
        ensure!(
            profile["capabilities"]
                .as_array()
                .is_some_and(|values| values.iter().any(|value| value == "embed")),
            "UNSUPPORTED_CAPABILITY"
        );
        let current = service.state_in(client).await?;
        ensure!(
            state.security_epoch == current.security_epoch
                && state.instance_epoch == current.instance_epoch,
            "AUTHORIZATION_CHANGED"
        );
        profile["destination"] = json!(route.destination);
        profile["secretApproval"] =
            json!(secret.as_ref().map(crate::secrets::SecretGrant::snapshot));
        profile["secretOperation"] =
            json!(secret.as_ref().map(crate::secrets::SecretGrant::operation));
        Ok(profile)
    }

    fn receipt_sources_in<'a>(
        &'a self,
        client: &'a tokio_postgres::Client,
        session: &'a VerifiedSession,
        execution: Option<Uuid>,
    ) -> futures::future::BoxFuture<'a, Result<BTreeSet<String>>> {
        Box::pin(async move {
            let mut labels = BTreeSet::new();
            if let Some(execution) = execution {
                let domain: String = client
                    .query_one(
                        "SELECT spec->>'domain' FROM moria.runtime_executions WHERE id=$1",
                        &[&execution],
                    )
                    .await?
                    .get(0);
                if domain == "agent" {
                    labels.extend(
                        Box::pin(crate::platform::NativePlatformBroker::retained_sources_in(
                            &self.db, client, session, execution,
                        ))
                        .await?
                        .into_iter()
                        .map(|mark| mark.marking),
                    );
                } else {
                    // Search embedding receipts are usable only inside a live, matching native projector purpose.
                    ensure!(domain == "search", "MODEL_CALL_UNAVAILABLE");
                    let origin: Option<Value> = client
                        .query_one(
                            "SELECT moria.runtime_job_session_origin($1)",
                            &[&session.session_hash()],
                        )
                        .await?
                        .get(0);
                    let current = origin
                        .as_ref()
                        .and_then(|v| v["execution"].as_str())
                        .map(Uuid::parse_str)
                        .transpose()?
                        .ok_or_else(|| anyhow::anyhow!("MODEL_CALL_UNAVAILABLE"))?;
                    let source = client.query_opt("SELECT p.source_resource,p.source_dataset FROM moria.runtime_executions old JOIN moria.native_search_execution_sources os ON os.execution=old.id JOIN moria.native_search_rebuilds r ON r.generation=os.generation JOIN moria.native_search_projector_bindings p ON p.id=r.projector_binding JOIN moria.runtime_executions current ON current.id=$2 JOIN moria.native_search_execution_sources cs ON cs.execution=current.id JOIN moria.native_search_rebuilds cr ON cr.generation=cs.generation WHERE old.id=$1 AND old.environment=$3 AND old.principal=$4 AND old.application=$5 AND ROW(current.environment,current.principal,current.application)=ROW(old.environment,old.principal,old.application) AND current.spec->>'domain'='search' AND cr.projector_binding=p.id AND cr.resource=r.resource AND p.enabled AND p.purpose='full-object-projection' AND p.domain='search' AND (old.finished_at IS NULL OR old.finished_at>clock_timestamp()-interval '30 days')", &[&execution,&current,&session.environment(),&session.principal(),&session.application()]).await?.ok_or_else(||anyhow::anyhow!("MODEL_CALL_UNAVAILABLE"))?;
                    let state = Authority::current(client, session).await?;
                    for column in ["source_resource", "source_dataset"] {
                        let resource: String = source.get(column);
                        let markings =
                            Authority::resource_access(client, session, &resource, "read").await?;
                        authorize(&state.identity, &resource, "read", &markings)?;
                        labels.extend(markings);
                    }
                    Authority::finish(client, session, &state).await?;
                }
            }
            Ok(labels)
        })
    }

    pub(crate) async fn validate_retained_in(
        &self,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        resource: &str,
        key: &str,
    ) -> Result<Vec<crate::sandbox::ConsumedMarking>> {
        ensure!(
            !key.is_empty() && key.len() <= 256 && key.bytes().all(|byte| byte.is_ascii_graphic()),
            "MODEL_CALL_KEY_INVALID"
        );
        let snapshot = Authority::current(client, session).await?;
        let current = Authority::resource_access(client, session, resource, "read").await?;
        authorize(&snapshot.identity, resource, "invoke", &current)?;
        let row=client.query_opt("SELECT binding,markings,state,result IS NOT NULL,caller_execution FROM moria.native_model_calls WHERE environment=$1 AND application=$2 AND principal=$3 AND resource=$4 AND call_key=$5",&[&session.environment(),&session.application(),&session.principal(),&resource,&key]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        let mut retained = self.receipt_sources_in(client, session, row.get(4)).await?;
        let route: Route = serde_json::from_value(row.get(0))?;
        retained.extend(row.get::<_, Vec<String>>(1));
        let (_, labels, _) = Self::authorized_in(
            client,
            session,
            resource,
            &route,
            &retained,
            Some(&snapshot),
        )
        .await?;
        ensure!(
            row.get::<_, String>(2) != "succeeded" || row.get::<_, bool>(3),
            "RESULT_EXPIRED"
        );
        Authority::finish(client, session, &snapshot).await?;
        Ok(labels
            .into_iter()
            .map(|marking| crate::sandbox::ConsumedMarking {
                environment: session.environment().into(),
                marking,
            })
            .collect())
    }

    pub async fn inspect(
        &self,
        session: &VerifiedSession,
        resource: &str,
        key: &str,
        cancel: bool,
    ) -> Result<Value> {
        self.base_access(session, resource).await?;
        ensure!(
            !key.is_empty() && key.len() <= 256 && key.bytes().all(|b| b.is_ascii_graphic()),
            "MODEL_CALL_KEY_INVALID"
        );
        for attempt in 0..2 {
            let admission = Admission::begin(&self.db, session, false).await?;
            let result=async {
                let row=admission.client().query_opt("SELECT id,binding,markings,caller_execution FROM moria.native_model_calls WHERE environment=$1 AND application=$2 AND principal=$3 AND resource=$4 AND call_key=$5",&[&session.environment(),&session.application(),&session.principal(),&resource,&key]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
                let id:Uuid=row.get(0);let route:Route=serde_json::from_value(row.get(1))?;let mut labels:BTreeSet<String>=row.get::<_,Vec<String>>(2).into_iter().collect();
                labels.extend(self.receipt_sources_in(admission.client(),session,row.get(3)).await?);
                let (_,labels,_)=Self::authorized_in(admission.client(),session,resource,&route,&labels,None).await?;
                if cancel {admission.client().execute("UPDATE moria.native_model_calls c SET state='failed',error='MODEL_CANCELLED',uncertain=EXISTS(SELECT 1 FROM moria.native_model_attempts a WHERE a.call_id=c.id AND a.status NOT IN ('connectFailure','failed')),finished_at=clock_timestamp() WHERE c.id=$1 AND c.state='running'",&[&id]).await?;}
                let state=admission.client().query_one("SELECT CASE WHEN state='running' AND deadline<=clock_timestamp() THEN 'failed' ELSE state END,result,CASE WHEN state='running' AND deadline<=clock_timestamp() THEN 'MODEL_INCOMPLETE' ELSE error END,uncertain,deadline>clock_timestamp() FROM moria.native_model_calls WHERE id=$1",&[&id]).await?;
                let status:String=state.get(0);let mut result:Option<Value>=state.get(1);
                if let Some(value)=&mut result {Self::classify_result(value,&labels)?;}let error:Option<String>=state.get(2);
                ensure!(status!="succeeded"||result.is_some(),"RESULT_EXPIRED");
                Ok(json!({"id":id,"state":status,"error":error,"result":result,"uncertain":state.get::<_,bool>(3)&&state.get::<_,bool>(4),"cancelled":error.as_deref()==Some("MODEL_CANCELLED")}))
            }.await;
            let result = admission.complete(result).await;
            if attempt == 0
                && result.as_ref().err().is_some_and(|e| {
                    e.chain().any(|cause| {
                        cause
                            .downcast_ref::<tokio_postgres::Error>()
                            .is_some_and(|db| db.code().is_some_and(|code| code.code() == "40001"))
                    })
                })
            {
                continue;
            }
            return result;
        }
        bail!("MODEL_CALL_UNAVAILABLE")
    }

    async fn response(
        &self,
        call: &Call,
        path: &str,
        payload: &Value,
    ) -> Result<reqwest::Response> {
        let endpoint = call.endpoint.join(path)?;
        for ordinal in 0..2i32 {
            self.current(call).await?;
            self.db.get().await?.execute("INSERT INTO moria.native_model_attempts(call_id,ordinal,status) VALUES($1,$2,'started')",&[&call.id,&ordinal]).await?;
            let mut request = self
                .http
                .post(endpoint.clone())
                .json(payload)
                .header("x-request-id", call.id.to_string());
            if let Some(reference) = &call.route.secret_ref {
                if let Some(secret) = &call.secret {
                    ensure!(
                        secret.material_ref() == reference,
                        "SECRET_APPROVAL_REVOKED"
                    );
                }
                ensure!(
                    !reference.is_empty()
                        && reference.len() <= 128
                        && reference
                            .bytes()
                            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'),
                    "MODEL_BINDING_INVALID"
                );
                request = request.bearer_auth(
                    env::var(reference)
                        .map_err(|_| anyhow::anyhow!("MODEL_CONFIGURATION_UNAVAILABLE"))?,
                );
            }
            call.provider_started.store(true, Ordering::Release);
            match request.send().await {
                Ok(response) => {
                    let code = i32::from(response.status().as_u16());
                    self.db.get().await?.execute("UPDATE moria.native_model_attempts SET status='response',http_status=$3,finished_at=clock_timestamp() WHERE call_id=$1 AND ordinal=$2",&[&call.id,&ordinal,&code]).await?;
                    if !response.status().is_success() {
                        let status = response.status();
                        let body = Self::body(call, response).await?;
                        if call.route.embedding_protocol == "ollama-v1"
                            && path == "../api/embed"
                            && status == reqwest::StatusCode::BAD_REQUEST
                            && body["error"].as_str().is_some_and(|message| {
                                [
                                    "the input length exceeds the context length",
                                    "input length exceeds maximum context length",
                                ]
                                .contains(&message)
                            })
                        {
                            bail!("MODEL_CONTEXT_LIMIT");
                        }
                        bail!("MODEL_UNAVAILABLE");
                    }
                    return Ok(response);
                }
                Err(error) => {
                    if error.is_connect() || error.is_builder() {
                        call.provider_started.store(false, Ordering::Release);
                    }
                    let status = if error.is_connect() {
                        "connectFailure"
                    } else {
                        "uncertain"
                    };
                    self.db.get().await?.execute("UPDATE moria.native_model_attempts SET status=$3,finished_at=clock_timestamp() WHERE call_id=$1 AND ordinal=$2",&[&call.id,&ordinal,&status]).await?;
                    if !error.is_connect() || ordinal == 1 {
                        bail!("MODEL_UNAVAILABLE");
                    }
                }
            }
        }
        bail!("MODEL_UNAVAILABLE")
    }

    async fn body(call: &Call, response: reqwest::Response) -> Result<Value> {
        ensure!(
            response.content_length().is_none_or(|n| n <= 1024 * 1024),
            "MODEL_PROTOCOL"
        );
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(part) = stream.next().await {
            let part = part.map_err(|_| anyhow::anyhow!("MODEL_INCOMPLETE"))?;
            ensure!(bytes.len() + part.len() <= 1024 * 1024, "MODEL_PROTOCOL");
            bytes.extend_from_slice(&part);
        }
        call.provider_finished.store(true, Ordering::Release);
        serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("MODEL_PROTOCOL"))
    }

    async fn complete(&self, call: &Call, result: Result<Value>) -> Result<Value> {
        let deadline = call
            .deadline
            .min(tokio::time::Instant::now() + Duration::from_secs(30));
        match tokio::time::timeout_at(deadline, self.complete_once(call, result)).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => self.reconcile_completion(call, error).await,
            Err(_) => {
                self.reconcile_completion(call, anyhow::anyhow!("MODEL_INCOMPLETE"))
                    .await
            }
        }
    }

    async fn reconcile_completion(&self, call: &Call, fallback: anyhow::Error) -> Result<Value> {
        let client = tokio::time::timeout(Duration::from_secs(5), self.db.get())
            .await
            .map_err(|_| anyhow::anyhow!("MODEL_INCOMPLETE"))??;
        let operation = async {
            let row = client
                .query_opt(
                    "SELECT state,result,error FROM moria.native_model_calls WHERE id=$1 AND owner=$2",
                    &[&call.id, &call.owner],
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("MODEL_CALL_UNAVAILABLE"))?;
            if row.get::<_, String>(0) == "succeeded" {
                let mut value = row
                    .get::<_, Option<Value>>(1)
                    .ok_or_else(|| anyhow::anyhow!("RESULT_EXPIRED"))?;
                let admission = Admission::begin(&self.db, &call.session, false).await?;
                let checked = async {
                    if let Some(context) = &call.sandbox {
                        context.check_in(admission.client()).await?;
                    }
                    let mut labels = call.labels.clone();
                    labels.extend(
                        self.receipt_sources_in(
                            admission.client(),
                            &call.session,
                            call.caller_execution,
                        )
                        .await?,
                    );
                    let (_, labels, _) = Self::authorized_in(
                        admission.client(),
                        &call.session,
                        &call.resource,
                        &call.route,
                        &labels,
                        None,
                    )
                    .await?;
                    Self::classify_result(&mut value, &labels)?;
                    if let Some(grant) = &call.projector {
                        grant.check_in(admission.client()).await?;
                    }
                    Ok(())
                }
                .await;
                admission.complete(checked).await?;
                return Ok(value);
            }
            if row.get::<_, Option<String>>(2).as_deref() == Some("MODEL_CANCELLED") {
                return Err(anyhow::anyhow!("MODEL_CANCELLED"));
            }
            let uncertain = call.provider_started.load(Ordering::Acquire)
                && !call.provider_finished.load(Ordering::Acquire);
            client.execute("UPDATE moria.native_model_calls SET state='failed',uncertain=$3,error=CASE WHEN error='MODEL_CANCELLED' THEN error ELSE 'MODEL_INCOMPLETE' END,finished_at=clock_timestamp() WHERE id=$1 AND owner=$2 AND state='running'",&[&call.id,&call.owner,&uncertain]).await?;
            Err(fallback)
        };
        match tokio::time::timeout(Duration::from_secs(5), operation).await {
            Ok(result) => result,
            Err(_) => {
                drop(deadpool_postgres::Object::take(client));
                Err(anyhow::anyhow!("MODEL_INCOMPLETE"))
            }
        }
    }

    async fn complete_once(&self, call: &Call, result: Result<Value>) -> Result<Value> {
        let result = async {
            let mut value = result?;
            value["markings"]=json!(call.labels);
            value["modelBindingFingerprint"] = json!(Self::binding_fingerprint(&call.definition, &call.endpoint, call.secret.as_ref().map(crate::secrets::SecretGrant::material_revision))?);
            self.current(call).await?;
            ensure!(serde_json::to_vec(&value)?.len() <= 1024 * 1024, "MODEL_PROTOCOL");
                let admission = Admission::begin(&self.db, &call.session, false).await?;
                // Fence the task before private-session readers acquire execution-row share locks.
                if let Some(context) = &call.sandbox { context.check_in(admission.client()).await?; }
                if let Some(grant) = &call.projector { grant.check_in(admission.client()).await?; }
                let current = Authority::current(admission.client(), &call.session).await?;
                let finish=async{
                    Authority::finish(admission.client(),&call.session,&call.snapshot).await?;
                    ensure!(current.security_epoch==call.snapshot.security_epoch,"AUTHORIZATION_CHANGED");
                    ensure!(admission.client().query_one("SELECT EXISTS(SELECT 1 FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled AND definition=$3)",&[&call.session.environment(),&call.resource,&call.definition]).await?.get::<_,bool>(0),"MODEL_BINDING_CHANGED");
                    let mut labels=call.labels.clone();
                    if let Some(secret)=&call.secret { labels.extend(secret.check_in(admission.client(),&call.session).await?); }
                    labels.extend(self.receipt_sources_in(admission.client(),&call.session,call.caller_execution).await?);
                    labels.extend(Authority::resource_access(admission.client(),&call.session,&call.resource,"read").await?);
                    authorize(&current.identity,&call.resource,"invoke",&labels.iter().cloned().collect::<Vec<_>>())?;
                    authorize_destination(&current.identity,&labels,Some(&call.route.destination_policy),&call.route.destination)?;

                    Self::classify_result(&mut value,&labels)?;
                    let changed=admission.client().execute("UPDATE moria.native_model_calls SET state='succeeded',result=$3,uncertain=false,finished_at=clock_timestamp() WHERE id=$1 AND owner=$2 AND state='running' AND deadline>clock_timestamp()",&[&call.id,&call.owner,&value]).await?;
                    ensure!(changed==1,"MODEL_CALL_UNAVAILABLE");
                    admission.client().execute("UPDATE moria.native_model_attempts SET usage=$2 WHERE call_id=$1 AND ordinal=(SELECT max(ordinal) FROM moria.native_model_attempts WHERE call_id=$1)",&[&call.id,&value["usage"]]).await?;
                    Ok(value)
                }.await;
                admission.complete(finish).await
        }.await;
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                let uncertain = call.provider_started.load(Ordering::Acquire)
                    && !call.provider_finished.load(Ordering::Acquire);
                let code = crate::model_http::error_code(&error);
                self.db.get().await?.execute("UPDATE moria.native_model_calls SET state='failed',uncertain=$3,error=CASE WHEN error='MODEL_CANCELLED' THEN error ELSE $4 END,finished_at=clock_timestamp() WHERE id=$1 AND owner=$2 AND (state='running' OR (state='failed' AND error='MODEL_CANCELLED'))",&[&call.id,&call.owner,&uncertain,&code]).await?;
                Err(error)
            }
        }
    }

    pub async fn chat(
        &self,
        session: &VerifiedSession,
        resource: &str,
        key: &str,
        input: &Value,
    ) -> Result<Value> {
        self.chat_labeled(session, resource, key, input, BTreeSet::new())
            .await
    }

    pub async fn probe(&self, session: &VerifiedSession, resource: &str) -> Result<Value> {
        self.base_access(session, resource).await?;
        let definition:Value=self.db.get().await?.query_one("SELECT definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&resource]).await?.get(0);
        let route: Route = serde_json::from_value(definition)?;
        self.authorized(session, resource, &route, &BTreeSet::new(), None)
            .await?;
        ensure!(
            route
                .capabilities
                .iter()
                .all(|c| ["chat", "embed", "tools", "structured", "stream"].contains(&c.as_str())),
            "MODEL_BINDING_INVALID"
        );
        let mut passed = Vec::new();
        let mut failed = serde_json::Map::new();
        let mut requested = route.capabilities.clone();
        if requested.contains("stream") {
            if requested.contains("tools") {
                requested.insert("streamTools".into());
            }
            if requested.contains("structured") {
                requested.insert("streamStructured".into());
            }
        }
        for capability in &requested {
            let mut input = json!({"messages":[{"role":"user","content":"/no_think\nReply with the text Moria."}],"maxTokens":1024});
            if capability == "tools" || capability == "streamTools" {
                input["messages"][0]["content"] = json!(
                    "/no_think\nCall moria_probe with the integer value 7. Do not answer directly."
                );
                input["tools"] = json!([{"name":"moria_probe","parameters":{"type":"object","properties":{"value":{"type":"integer","const":7}},"required":["value"],"additionalProperties":false}}]);
            }
            if capability == "structured" || capability == "streamStructured" {
                input["messages"][0]["content"] =
                    json!("/no_think\nReturn exactly the JSON object {\"answer\":7}.");
                input["responseFormat"] = json!({"name":"moria_probe","schema":{"type":"object","properties":{"answer":{"type":"integer","const":7}},"required":["answer"],"additionalProperties":false}});
            }
            if capability == "embed" {
                input = json!({"input":["Moria embedding capability probe"]});
            }
            let (call, _) = self
                .begin(
                    session,
                    resource,
                    &format!("probe/{}", Uuid::new_v4()),
                    RequestPurpose::Probe(capability),
                    &input,
                    BTreeSet::new(),
                )
                .await?;
            let call = call.unwrap();
            let result = self
                .guarded(&call, async {
                    if capability == "embed" {
                        return self.embeddings(&call, &input["input"], 1).await;
                    }
                    let mut plan = crate::model_protocol::ChatPlan::parse(&input)?;
                    plan.payload["model"] = json!(call.route.model);
                    let body = if capability.starts_with("stream") {
                        plan.payload["stream"] = json!(true);
                        plan.payload["stream_options"] = json!({"include_usage":true});
                        let response = self
                            .response(&call, "chat/completions", &plan.payload)
                            .await?;
                        ensure!(
                            response
                                .headers()
                                .get(reqwest::header::CONTENT_TYPE)
                                .and_then(|h| h.to_str().ok())
                                .is_some_and(|s| s.split(';').next() == Some("text/event-stream")),
                            "MODEL_PROTOCOL"
                        );
                        let mut stream = response.bytes_stream();
                        let mut decoder = crate::model_protocol::ChatStream::default();
                        while let Some(part) = stream.next().await {
                            self.current(&call).await?;
                            decoder
                                .feed(&part.map_err(|_| anyhow::anyhow!("MODEL_INCOMPLETE"))?)?;
                        }
                        call.provider_finished.store(true, Ordering::Release);
                        decoder.finish()?
                    } else {
                        Self::body(
                            &call,
                            self.response(&call, "chat/completions", &plan.payload)
                                .await?,
                        )
                        .await?
                    };
                    let normalized = plan.normalize(
                        body,
                        call.id,
                        &call.route.model,
                        &call.route.model_revision,
                    )?;
                    if capability == "tools" || capability == "streamTools" {
                        ensure!(
                            normalized["finishReason"] == "tool_calls",
                            "UNSUPPORTED_CAPABILITY"
                        );
                    } else {
                        ensure!(
                            normalized["content"]
                                .as_str()
                                .is_some_and(|s| !s.is_empty()),
                            "MODEL_PROTOCOL"
                        );
                    }
                    Ok(normalized)
                })
                .await;
            match self.complete(&call, result).await {
                Ok(_) => {
                    let digest = Self::binding_fingerprint(
                        &call.definition,
                        &call.endpoint,
                        call.secret
                            .as_ref()
                            .map(crate::secrets::SecretGrant::material_revision),
                    )?;
                    let admission = Admission::begin(&self.db, session, false).await?;
                    let write=async {
                        Authority::finish(admission.client(),session,&call.snapshot).await?;
                        admission.client().query_one("SELECT resource FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled AND definition=$3 FOR SHARE",&[&session.environment(),&resource,&call.definition]).await?;
                        admission.client().execute("INSERT INTO moria.native_model_probes(environment,resource,definition_hash,capability,call_id,expires_at) VALUES($1,$2,$3,$4,$5,clock_timestamp()+interval '24 hours') ON CONFLICT(environment,resource,definition_hash,capability) DO UPDATE SET call_id=EXCLUDED.call_id,tested_at=clock_timestamp(),expires_at=EXCLUDED.expires_at",&[&session.environment(),&resource,&digest,&capability,&call.id]).await?;Ok(())
                    }.await;
                    admission.complete(write).await?;
                    passed.push(capability.clone());
                }
                Err(_) => {
                    failed.insert(capability.clone(), json!("PROBE_FAILED"));
                }
            }
        }
        Ok(
            json!({"model":route.model,"modelRevision":route.model_revision,"capabilities":passed,"unsupported":failed,"schemaProfile":{"references":false,"regex":"linear","nodes":256},"validitySeconds":86400}),
        )
    }

    async fn embeddings(&self, call: &Call, texts: &Value, count: usize) -> Result<Value> {
        let ollama = call.route.embedding_protocol == "ollama-v1";
        let path = if ollama { "../api/embed" } else { "embeddings" };
        let payload = if ollama {
            json!({"model":call.route.model,"input":texts,"truncate":false})
        } else {
            json!({"model":call.route.model,"input":texts,"encoding_format":"float"})
        };
        let mut body = Self::body(call, self.response(call, path, &payload).await?).await?;
        if ollama {
            let fields = body
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
            let embeddings = fields
                .remove("embeddings")
                .and_then(|value| value.as_array().cloned())
                .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
            let usage = match fields.get("prompt_eval_count") {
                Some(value) => {
                    let value = value
                        .as_u64()
                        .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
                    json!({"prompt_tokens":value,"total_tokens":value})
                }
                None => Value::Null,
            };
            fields.insert(
                "data".into(),
                json!(
                    embeddings
                        .into_iter()
                        .enumerate()
                        .map(|(index, embedding)| json!({"index":index,"embedding":embedding}))
                        .collect::<Vec<_>>()
                ),
            );
            fields.insert("usage".into(), usage);
            fields.insert("providerProtocol".into(), json!("ollama-v1"));
        }
        Self::embedding_result(call, body, count)
    }

    fn embedding_result(call: &Call, body: Value, count: usize) -> Result<Value> {
        let data = body["data"]
            .as_array()
            .filter(|d| d.len() == count)
            .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
        let mut vectors = vec![None; data.len()];
        for item in data {
            let index = item["index"]
                .as_u64()
                .and_then(|i| usize::try_from(i).ok())
                .filter(|i| *i < data.len())
                .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
            ensure!(vectors[index].is_none(), "MODEL_PROTOCOL");
            let vector = item["embedding"]
                .as_array()
                .filter(|v| Some(v.len()) == call.route.dimensions)
                .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
            let values = vector
                .iter()
                .map(|v| {
                    v.as_f64()
                        .filter(|v| v.is_finite() && v.abs() <= f32::MAX as f64)
                        .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))
                })
                .collect::<Result<Vec<_>>>()?;
            ensure!(
                values.iter().any(|v| (*v as f32) != 0.0),
                "MODEL_ZERO_VECTOR"
            );
            vectors[index] = Some(values);
        }
        Ok(
            json!({"id":call.id,"model":call.route.model,"modelRevision":call.route.model_revision,"dimensions":call.route.dimensions,"vectors":vectors,"usage":crate::model_protocol::usage(&body["usage"] )?,"metadata":{"provider":crate::model_protocol::provider_metadata(body,false)}}),
        )
    }
    pub(crate) async fn chat_labeled(
        &self,
        session: &VerifiedSession,
        resource: &str,
        key: &str,
        input: &Value,
        labels: BTreeSet<String>,
    ) -> Result<Value> {
        self.chat_owned(session, resource, key, input, labels, None)
            .await
    }

    pub(crate) async fn chat_sandbox(
        &self,
        context: &crate::sandbox::SandboxContext,
        resource: &str,
        key: &str,
        input: &Value,
        labels: BTreeSet<String>,
    ) -> Result<Value> {
        let session = Authority {
            db: self.db.clone(),
        }
        .sandbox_session(context)
        .await?;
        self.chat_owned(&session, resource, key, input, labels, Some(context))
            .await
    }

    async fn chat_owned(
        &self,
        session: &VerifiedSession,
        resource: &str,
        key: &str,
        input: &Value,
        labels: BTreeSet<String>,
        sandbox: Option<&crate::sandbox::SandboxContext>,
    ) -> Result<Value> {
        if let Some(context) = sandbox {
            context.check().await?;
        }
        let mut plan = crate::model_protocol::ChatPlan::parse(input)?;
        let (call, replay) = self
            .begin(
                session,
                resource,
                key,
                RequestPurpose::Invoke("chat"),
                input,
                labels,
            )
            .await?;
        if let Some(replay) = replay {
            if let Some(context) = sandbox {
                context.check().await?;
            }
            return Ok(replay);
        }
        let mut call = call.unwrap();
        call.sandbox = sandbox.cloned();
        let result = self
            .guarded(&call, async {
                self.check_plan(&call, &plan, false).await?;
                plan.payload["model"] = json!(call.route.model);
                let body = Self::body(
                    &call,
                    self.response(&call, "chat/completions", &plan.payload)
                        .await?,
                )
                .await?;
                plan.normalize(body, call.id, &call.route.model, &call.route.model_revision)
            })
            .await;
        self.complete(&call, result).await
    }

    pub async fn embed(
        &self,
        session: &VerifiedSession,
        resource: &str,
        key: &str,
        input: &Value,
    ) -> Result<Value> {
        self.embed_labeled(session, resource, key, input, BTreeSet::new())
            .await
    }

    pub(crate) async fn embed_labeled(
        &self,
        session: &VerifiedSession,
        resource: &str,
        key: &str,
        input: &Value,
        labels: BTreeSet<String>,
    ) -> Result<Value> {
        self.embed_owned(session, resource, key, input, labels, None)
            .await
    }

    pub(crate) async fn embed_projector(
        &self,
        grant: Arc<crate::full_object_source::FullObjectSourceGrant>,
        key: &str,
        input: &Value,
    ) -> Result<Value> {
        grant.check().await?;
        let session = grant.session().clone();
        let resource = grant.model_resource().to_owned();
        let labels = grant.markings().iter().cloned().collect();
        self.embed_owned(&session, &resource, key, input, labels, Some(grant))
            .await
    }

    async fn embed_owned(
        &self,
        session: &VerifiedSession,
        resource: &str,
        key: &str,
        input: &Value,
        labels: BTreeSet<String>,
        projector: Option<Arc<crate::full_object_source::FullObjectSourceGrant>>,
    ) -> Result<Value> {
        let texts = input["input"]
            .as_array()
            .filter(|v| !v.is_empty() && v.len() <= 64 && v.iter().all(Value::is_string))
            .ok_or_else(|| anyhow::anyhow!("MODEL_REQUEST_INVALID"))?;
        ensure!(
            input.as_object().is_some_and(|m| m.len() == 1),
            "MODEL_REQUEST_INVALID"
        );
        let (call, replay) = self
            .begin(
                session,
                resource,
                key,
                RequestPurpose::Invoke("embed"),
                input,
                labels,
            )
            .await?;
        if let Some(replay) = replay {
            if let Some(grant) = &projector {
                grant.check().await?;
            }
            return Ok(replay);
        }
        let mut call = call.unwrap();
        call.projector = projector;
        let result = self
            .guarded(&call, async {
                self.embeddings(&call, &json!(texts), texts.len()).await
            })
            .await;
        self.complete(&call, result).await
    }

    pub async fn stream(
        &self,
        session: &VerifiedSession,
        resource: &str,
        key: &str,
        input: &Value,
    ) -> Result<tokio::sync::mpsc::Receiver<Result<Value>>> {
        let mut plan = crate::model_protocol::ChatPlan::parse(input)?;
        let (call, replay) = self
            .begin(
                session,
                resource,
                key,
                RequestPurpose::Invoke("stream"),
                input,
                BTreeSet::new(),
            )
            .await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        if let Some(replay) = replay {
            sender
                .send(Ok(json!({"kind":"result","result":replay})))
                .await
                .map_err(|_| anyhow::anyhow!("MODEL_CANCELLED"))?;
            return Ok(receiver);
        }
        let call = call.unwrap();
        let service = self.clone();
        tokio::spawn(async move {
            let result = service
                .guarded(&call, async {
                    service.check_plan(&call, &plan, true).await?;
                    plan.payload["model"] = json!(call.route.model);
                    plan.payload["stream"] = json!(true);
                    plan.payload["stream_options"] = json!({"include_usage":true});
                    let response = service
                        .response(&call, "chat/completions", &plan.payload)
                        .await?;
                    ensure!(
                        response
                            .headers()
                            .get(reqwest::header::CONTENT_TYPE)
                            .and_then(|v| v.to_str().ok())
                            .is_some_and(|v| v.split(';').next() == Some("text/event-stream")),
                        "MODEL_PROTOCOL"
                    );
                    let mut body = response.bytes_stream();
                    let mut decoder = crate::model_protocol::ChatStream::default();
                    while let Some(part) = body.next().await {
                        let part = part.map_err(|_| anyhow::anyhow!("MODEL_INCOMPLETE"))?;
                        let events = decoder.feed(&part)?;
                        if decoder.terminal() {
                            call.provider_finished.store(true, Ordering::Release);
                        }
                        for event in events {
                            service.current(&call).await?;
                            sender
                                .send(Ok(event))
                                .await
                                .map_err(|_| anyhow::anyhow!("MODEL_CANCELLED"))?;
                        }
                    }
                    call.provider_finished.store(true, Ordering::Release);
                    plan.normalize(
                        decoder.finish()?,
                        call.id,
                        &call.route.model,
                        &call.route.model_revision,
                    )
                })
                .await;
            let result = service
                .complete(&call, result)
                .await
                .map(|result| json!({"kind":"result","result":result}));
            let _ = tokio::time::timeout(Duration::from_secs(3), sender.send(result)).await;
        });
        Ok(receiver)
    }
}

pub async fn cli(db: Database, arguments: Vec<String>) -> Result<()> {
    if arguments.first().map(String::as_str) == Some("serve") {
        return crate::model_http::serve(
            db,
            env::var("MORIA_NATIVE_MODEL_LISTEN")
                .unwrap_or_else(|_| "127.0.0.1:0".into())
                .parse()?,
        )
        .await;
    }
    use tokio::io::{AsyncReadExt, stdin};
    let mut bytes = Vec::new();
    stdin().take(256 * 1024 + 1).read_to_end(&mut bytes).await?;
    ensure!(bytes.len() <= 256 * 1024, "MODEL_REQUEST_INVALID");
    let value: Value = serde_json::from_slice(&bytes)?;
    let text = |key: &str| {
        value[key]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("MODEL_REQUEST_INVALID"))
    };
    let session = Authority { db: db.clone() }
        .authenticate(text("token")?)
        .await?;
    let gateway = NativeModels::new(db)?;
    if arguments.first().map(String::as_str) == Some("capabilities") {
        println!(
            "{}",
            gateway.capabilities(&session, text("resource")?).await?
        );
        return Ok(());
    }
    if arguments
        .first()
        .map(String::as_str)
        .is_some_and(|a| a == "status" || a == "cancel")
    {
        println!(
            "{}",
            gateway
                .inspect(
                    &session,
                    text("resource")?,
                    text("key")?,
                    arguments[0] == "cancel"
                )
                .await?
        );
        return Ok(());
    }
    if arguments.first().map(String::as_str) == Some("probe") {
        println!("{}", gateway.probe(&session, text("resource")?).await?);
        return Ok(());
    }
    if arguments.first().map(String::as_str) == Some("stream") {
        let mut receiver = gateway
            .stream(&session, text("resource")?, text("key")?, &value["input"])
            .await?;
        let mut complete = false;
        while let Some(event) = receiver.recv().await {
            let event = event?;
            complete |= event["kind"] == "result";
            println!("{event}");
        }
        ensure!(complete, "MODEL_INCOMPLETE");
        return Ok(());
    }
    let result = match arguments.first().map(String::as_str) {
        Some("chat") => {
            gateway
                .chat(&session, text("resource")?, text("key")?, &value["input"])
                .await?
        }
        Some("embed") => {
            gateway
                .embed(&session, text("resource")?, text("key")?, &value["input"])
                .await?
        }
        _ => bail!("UNSUPPORTED_CAPABILITY"),
    };
    println!("{result}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_receipt_classification_preserves_the_result_limit() {
        let mut value = json!({"id":"original","content":"x".repeat(1024 * 1024 - 100)});
        NativeModels::classify_result(&mut value, &BTreeSet::new()).unwrap();
        let labels = (0..256)
            .map(|n| format!("{n:03}-{}", "m".repeat(120)))
            .collect();
        assert!(NativeModels::classify_result(&mut value, &labels).is_err());
        assert_eq!(value["id"], "original");
        assert!(NativeModels::classify_result(&mut json!([]), &BTreeSet::new()).is_err());
    }
}
