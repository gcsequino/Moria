use crate::{
    canonical,
    database::Database,
    execution::{ExecutionAuthority, Lease},
    values,
};
use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{
        UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::watch,
};
use uuid::Uuid;

const OUTPUT_BYTES: usize = 262144;
const FRAME_BYTES: usize = 65536;
const DOCKER_API: &str = "v1.51";

#[derive(Default)]
struct ObservedResources {
    cpu: AtomicU64,
    memory: AtomicU64,
}
#[derive(Default)]
struct AuthorityMetrics {
    calls: AtomicU64,
    micros: AtomicU64,
}
struct AuthorityObservation<'a> {
    metrics: &'a AuthorityMetrics,
    started: std::time::Instant,
}
impl Drop for AuthorityObservation<'_> {
    fn drop(&mut self) {
        self.metrics.calls.fetch_add(1, Ordering::Relaxed);
        self.metrics.micros.fetch_add(
            self.started.elapsed().as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
}

impl ObservedResources {
    fn record(&self, cpu: u64, memory: u64) {
        self.cpu.fetch_max(cpu, Ordering::SeqCst);
        self.memory.fetch_max(memory, Ordering::SeqCst);
    }
}

pub(crate) fn artifact_image(artifact: &Value) -> Result<String> {
    let uri = artifact["uri"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("FUNCTION_ARTIFACT_INVALID"))?;
    let digest = artifact["digest"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("FUNCTION_ARTIFACT_INVALID"))?;
    ensure!(
        uri.starts_with("oci://")
            && uri.len() <= 256
            && uri[6..]
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._:/-".contains(&b))
            && uri[6..]
                .split('/')
                .all(|part| !part.is_empty() && part != "." && part != "..")
            && digest.starts_with("sha256:")
            && digest.len() == 71
            && digest[7..]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "FUNCTION_ARTIFACT_INVALID"
    );
    Ok(format!("{}@{digest}", &uri[6..]))
}

/// Metadata-only release readiness: inspect a preloaded immutable image, never pull or run it.
pub(crate) async fn verify_preloaded_artifact(
    artifact: &Value,
    adapter: Option<&Value>,
) -> Result<()> {
    let docker = Docker::new()?;
    let image = artifact_image(artifact)?;
    docker.image(&image).await?;
    if let Some(adapter) = adapter {
        let adapter = artifact_image(adapter)?;
        docker.image(&adapter).await?;
        docker.derived_image(&image, &adapter).await?;
    }
    Ok(())
}

/// Read-only readiness: inspect exact local digests and layer ancestry; never pull, build or run.
pub(crate) async fn preloaded_artifact_available(
    artifact: &Value,
    adapter: Option<&Value>,
) -> Result<bool> {
    artifact_image(artifact)?;
    if let Some(adapter) = adapter {
        artifact_image(adapter)?;
    }
    Ok(verify_preloaded_artifact(artifact, adapter).await.is_ok())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SandboxDomain {
    Function,
    Agent,
    Evaluator,
    Transform,
    Sync,
}
impl SandboxDomain {
    fn name(self) -> &'static str {
        match self {
            Self::Function => "function",
            Self::Agent => "agent",
            Self::Evaluator => "evaluator",
            Self::Transform => "transform",
            Self::Sync => "sync",
        }
    }
    fn engine(self) -> bool {
        matches!(self, Self::Transform | Self::Sync)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct ConsumedMarking {
    pub environment: String,
    pub marking: String,
}
pub(crate) async fn released_source_markings_in(
    client: &tokio_postgres::Client,
    session: &crate::authority::VerifiedSession,
    resource: Uuid,
    operation: &str,
    release: &str,
) -> Result<Vec<ConsumedMarking>> {
    Ok(crate::authority::Authority::resource_release_access(
        client,
        session,
        &resource.to_string(),
        operation,
        release,
    )
    .await?
    .into_iter()
    .map(|marking| ConsumedMarking {
        environment: session.environment().into(),
        marking,
    })
    .collect())
}

/// A public artifact cannot describe an incomplete or uncertain callback.
pub(crate) async fn require_clean_observations_in(
    client: &tokio_postgres::Client,
    execution: Uuid,
) -> Result<()> {
    let clean: bool = client
        .query_one(
            "SELECT NOT EXISTS(SELECT 1 FROM moria.runtime_sandbox_attempts WHERE execution=$1 AND (observation_pending IS NOT NULL OR observation_blocked))",
            &[&execution],
        )
        .await?
        .get(0);
    ensure!(clean, "FUNCTION_OBSERVATION_INCOMPLETE");
    Ok(())
}

pub(crate) async fn retained_observations_in(
    client: &tokio_postgres::Client,
    execution: Uuid,
) -> Result<Vec<ConsumedMarking>> {
    let Some(row)=client.query_opt("SELECT s.consumed_markings,s.observation_pending,s.observation_blocked,e.environment,e.status FROM moria.runtime_sandbox_attempts s JOIN moria.runtime_executions e ON e.id=s.execution WHERE s.execution=$1",&[&execution]).await? else {return Ok(Vec::new())};
    let pending: Option<String> = row.get(1);
    let status: String = row.get(4);
    ensure!(
        !row.get::<_, bool>(2)
            && (pending.is_none() || ["queued", "running", "preparing"].contains(&status.as_str())),
        "FUNCTION_OBSERVATION_INCOMPLETE"
    );
    let environment: String = row.get(3);
    let value: Value = row.get(0);
    let labels = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?;
    ensure!(labels.len() <= 256, "FUNCTION_MARKINGS_INVALID");
    labels
        .iter()
        .map(|label| {
            ensure!(
                label["environment"] == environment && label["marking"].is_string(),
                "FUNCTION_MARKINGS_INVALID"
            );
            Ok(ConsumedMarking {
                environment: environment.clone(),
                marking: label["marking"].as_str().unwrap().into(),
            })
        })
        .collect()
}

pub struct MutationAnchor {
    execution: Uuid,
    owner: Uuid,
    fence: String,
    security: i64,
    instance: i64,
}

/// Constructed only from a live function lease and immutable trusted function binding.
#[derive(Clone)]
pub struct SandboxContext {
    db: Database,
    host_id: Uuid,
    lease: Lease,
    environment: String,
    principal: String,
    application: String,
    session_hash: Option<String>,
    original_session_hash: Option<String>,
    effect: String,
    domain: SandboxDomain,
    source_release: Option<String>,
    release_blocked: Arc<AtomicBool>,
    observation: Arc<Mutex<Option<String>>>,
    authority_metrics: Arc<AuthorityMetrics>,
    container_name: String,
    outputs: Value,
    prepare_only: bool,
    point_reads_only: bool,
    attempt: Uuid,
    correlation: Uuid,
    markings: Arc<Mutex<BTreeSet<ConsumedMarking>>>,
}
impl SandboxContext {
    pub fn source_release(&self) -> Option<&str> {
        self.source_release.as_deref()
    }
    pub(crate) fn block_release(&self) {
        self.release_blocked.store(true, Ordering::Release);
    }
    pub(crate) fn authority_metrics(&self) -> Value {
        json!({"checks":self.authority_metrics.calls.load(Ordering::Relaxed).to_string(),"elapsedMicros":self.authority_metrics.micros.load(Ordering::Relaxed).to_string()})
    }
    pub async fn mutation_anchor_in(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<MutationAnchor> {
        ensure!(
            self.effect == "readWrite" && self.lease.task.is_some(),
            "FUNCTION_EFFECT"
        );
        self.check_in(client).await?;
        let row=client.query_one("SELECT security_epoch,instance_epoch FROM moria.runtime_function_epochs WHERE execution=$1 AND owner=$2 AND fence=$3", &[&self.lease.execution,&self.lease.owner,&self.lease.fence.parse::<i64>()?]).await?;
        Ok(MutationAnchor {
            execution: self.lease.execution,
            owner: self.lease.owner,
            fence: self.lease.fence.clone(),
            security: row.get(0),
            instance: row.get(1),
        })
    }
    pub async fn acknowledge_own_mutation_in(
        &self,
        client: &tokio_postgres::Client,
        anchor: &MutationAnchor,
    ) -> Result<()> {
        ensure!(
            anchor.execution == self.lease.execution
                && anchor.owner == self.lease.owner
                && anchor.fence == self.lease.fence
                && self.effect == "readWrite",
            "FUNCTION_LEASE_INVALID"
        );
        client
            .query_one(
                "SELECT moria.runtime_function_acknowledge($1,$2,$3,$4,$5,$6)",
                &[
                    &self.lease.execution,
                    &self.lease.owner,
                    &self.lease.fence.parse::<i64>()?,
                    &anchor.security,
                    &anchor.instance,
                    &serde_json::to_value(self.consumed_markings())?,
                ],
            )
            .await?;
        self.check_in(client).await
    }
    pub fn environment(&self) -> &str {
        &self.environment
    }
    pub fn principal(&self) -> &str {
        &self.principal
    }
    pub fn application(&self) -> &str {
        &self.application
    }
    pub fn session_hash(&self) -> Option<&str> {
        self.session_hash.as_deref()
    }
    pub fn original_session_hash(&self) -> Option<&str> {
        self.original_session_hash.as_deref()
    }
    pub fn effect(&self) -> &str {
        &self.effect
    }
    pub fn domain(&self) -> &str {
        self.domain.name()
    }
    fn container_name(&self) -> String {
        self.container_name.clone()
    }
    pub fn output_fields(&self) -> &Value {
        &self.outputs
    }
    pub fn prepare_only(&self) -> bool {
        self.prepare_only
    }
    pub fn point_reads_only(&self) -> bool {
        self.point_reads_only
    }
    pub fn lease(&self) -> &Lease {
        &self.lease
    }
    pub fn attempt_id(&self) -> Uuid {
        self.attempt
    }
    pub fn correlation_id(&self) -> Uuid {
        self.correlation
    }
    pub fn consumed_markings(&self) -> Vec<ConsumedMarking> {
        self.markings
            .lock()
            .expect("marking lock poisoned")
            .iter()
            .cloned()
            .collect()
    }
    pub fn record_consumed_markings(&self, markings: &[ConsumedMarking]) -> Result<()> {
        ensure!(
            markings.len() <= 256
                && markings.iter().all(|m| m.environment == self.environment
                    && !m.marking.is_empty()
                    && m.marking.len() <= 180
                    && !m.marking.contains('\0')),
            "FUNCTION_MARKINGS_INVALID"
        );
        let mut current = self
            .markings
            .lock()
            .map_err(|_| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?;
        let mut combined = current.clone();
        combined.extend(markings.iter().cloned());
        ensure!(combined.len() <= 256, "FUNCTION_MARKINGS_INVALID");
        *current = combined;
        Ok(())
    }
    async fn begin_observation(&self, id: &str) -> Result<()> {
        ensure!(
            !id.is_empty() && id.len() <= 160,
            "FUNCTION_OBSERVATION_INCOMPLETE"
        );
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        self.check_in(tx.client()).await?;
        let changed=tx.execute("UPDATE moria.runtime_sandbox_attempts SET observation_pending=$6,consumed_markings=moria.runtime_sandbox_markings_union(consumed_markings,$7) WHERE execution=$1 AND task=$2 AND owner=$3 AND fence=$4 AND attempt_id=$5 AND observation_pending IS NULL AND NOT observation_blocked AND state IN('starting','running')",&[&self.lease.execution,&self.lease.task,&self.lease.owner,&self.lease.fence.parse::<i64>()?,&self.attempt,&id,&json!(self.consumed_markings())]).await?;
        ensure!(changed == 1, "FUNCTION_OBSERVATION_INCOMPLETE");
        tx.commit().await?;
        *self
            .observation
            .lock()
            .map_err(|_| anyhow::anyhow!("FUNCTION_OBSERVATION_INCOMPLETE"))? = Some(id.into());
        Ok(())
    }
    /// Complete only in the transaction that durably records this callback's evidence.
    pub async fn complete_observation_in(
        &self,
        client: &tokio_postgres::Client,
        id: &str,
    ) -> Result<()> {
        self.check_in(client).await?;
        let changed=client.execute("UPDATE moria.runtime_sandbox_attempts SET observation_pending=NULL,consumed_markings=moria.runtime_sandbox_markings_union(consumed_markings,$7) WHERE execution=$1 AND task=$2 AND owner=$3 AND fence=$4 AND attempt_id=$5 AND observation_pending=$6 AND NOT observation_blocked AND state IN('starting','running')",&[&self.lease.execution,&self.lease.task,&self.lease.owner,&self.lease.fence.parse::<i64>()?,&self.attempt,&id,&json!(self.consumed_markings())]).await?;
        ensure!(changed == 1, "FUNCTION_OBSERVATION_INCOMPLETE");
        Ok(())
    }
    pub async fn complete_current_observation_in(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<()> {
        let id = self
            .observation
            .lock()
            .map_err(|_| anyhow::anyhow!("FUNCTION_OBSERVATION_INCOMPLETE"))?
            .clone()
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_OBSERVATION_INCOMPLETE"))?;
        self.complete_observation_in(client, &id).await
    }
    async fn observation_completed(&self) -> Result<()> {
        let client = self.db.get().await?;
        self.check_in(&client).await?;
        ensure!(client.query_opt("SELECT 1 FROM moria.runtime_sandbox_attempts WHERE execution=$1 AND task=$2 AND owner=$3 AND fence=$4 AND attempt_id=$5 AND observation_pending IS NULL AND NOT observation_blocked",&[&self.lease.execution,&self.lease.task,&self.lease.owner,&self.lease.fence.parse::<i64>()?,&self.attempt]).await?.is_some(),"FUNCTION_OBSERVATION_INCOMPLETE");
        *self
            .observation
            .lock()
            .map_err(|_| anyhow::anyhow!("FUNCTION_OBSERVATION_INCOMPLETE"))? = None;
        Ok(())
    }
    pub async fn check(&self) -> Result<()> {
        let _observation = AuthorityObservation {
            metrics: &self.authority_metrics,
            started: std::time::Instant::now(),
        };
        let client = self.db.get().await?;
        self.check_inner(&client).await
    }
    pub async fn check_in(&self, client: &tokio_postgres::Client) -> Result<()> {
        let _observation = AuthorityObservation {
            metrics: &self.authority_metrics,
            started: std::time::Instant::now(),
        };
        self.check_inner(client).await
    }
    async fn check_inner(&self, client: &tokio_postgres::Client) -> Result<()> {
        ensure!(
            !self.release_blocked.load(Ordering::Acquire),
            "FUNCTION_OBSERVATION_INCOMPLETE"
        );
        let fence: i64 = self.lease.fence.parse()?;
        ensure!(
            fence > 0 && fence.to_string() == self.lease.fence,
            "FUNCTION_LEASE_INVALID"
        );
        client.query_one("SELECT moria.runtime_function_worker_check($1,$2,(SELECT resource_class FROM moria.runtime_executions WHERE id=$3),$4)", &[&self.lease.owner,&self.host_id,&self.lease.execution,&self.domain.name()]).await?;
        ensure!(
            client
                .query_opt(
                    "SELECT 1 FROM moria.indexing_instance WHERE incarnation=$1",
                    &[&self.lease.incarnation]
                )
                .await?
                .is_some(),
            "INCARNATION_CHANGED"
        );
        if let Some(task) = self.lease.task {
            ensure!(
                client
                    .query_opt(
                        "SELECT 1 FROM moria.runtime_execution_tasks WHERE id=$1 AND execution=$2",
                        &[&task, &self.lease.execution]
                    )
                    .await?
                    .is_some(),
                "FUNCTION_LEASE_INVALID"
            );
            client
                .query_one(
                    "SELECT moria.runtime_task_check($1,$2,$3)",
                    &[&task, &self.lease.owner, &fence],
                )
                .await?;
        } else {
            client
                .query_one(
                    "SELECT moria.runtime_execution_check($1)",
                    &[&self.lease.execution],
                )
                .await?;
            ensure!(client.query_opt("SELECT 1 FROM moria.runtime_executions WHERE id=$1 AND owner=$2 AND fence=$3 AND status='preparing' AND lease_until>clock_timestamp()", &[&self.lease.execution,&self.lease.owner,&fence]).await?.is_some(),"FUNCTION_LEASE_INVALID");
        }
        if self.domain == SandboxDomain::Agent {
            crate::agents::check_context(client, self).await?;
            return Ok(());
        }
        if self.domain == SandboxDomain::Evaluator {
            crate::evaluations::check_context(client, self).await?;
            return Ok(());
        }
        if self.domain.engine() {
            client
                .query_one(
                    "SELECT moria.runtime_pipeline_current($1)",
                    &[&self.lease.execution],
                )
                .await?;
            let session = crate::authority::Authority::stored_session_in(
                client,
                self.session_hash
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("UNAUTHENTICATED"))?,
                &self.environment,
                &self.principal,
                &self.application,
            )
            .await?;
            let resource: String = client
                .query_one(
                    "SELECT resource::text FROM moria.runtime_executions WHERE id=$1",
                    &[&self.lease.execution],
                )
                .await?
                .get(0);
            let labels =
                crate::authority::Authority::resource_access(client, &session, &resource, "read")
                    .await?;
            self.record_consumed_markings(
                &labels
                    .into_iter()
                    .map(|marking| ConsumedMarking {
                        environment: self.environment.clone(),
                        marking,
                    })
                    .collect::<Vec<_>>(),
            )?;
            return Ok(());
        }
        if let Some(hash) = self.session_hash.as_deref()
            && client
                .query_opt(
                    "SELECT 1 FROM moria.runtime_function_invocations WHERE execution=$1",
                    &[&self.lease.execution],
                )
                .await?
                .is_some()
        {
            let state: Value = client
                .query_one("SELECT moria.native_serving_session($1)", &[&hash])
                .await?
                .get(0);
            let snapshot: crate::authority::SecuritySnapshot = serde_json::from_value(state)?;
            ensure!(
                snapshot.environment == self.environment
                    && snapshot.principal == self.principal
                    && snapshot.application == self.application,
                "FUNCTION_ACTOR_MISMATCH"
            );
            let resource:String=client.query_opt("SELECT e.resource::text FROM moria.runtime_executions e JOIN moria.runtime_function_invocations f ON f.execution=e.id JOIN moria.fs_entries entry ON entry.id=f.entry_id AND entry.resource_id=e.resource AND NOT entry.deleted JOIN moria.fs_resource_bindings b ON b.entry_id=entry.id AND b.active JOIN moria.fs_roles r ON r.entry_id=b.root_id AND NOT r.retired WHERE e.id=$1", &[&self.lease.execution]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?.get(0);
            let release = self
                .source_release
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_FILE_PROOF_REQUIRED"))?;
            let inherited: Value = client
                .query_one(
                    "SELECT moria.native_resource_release_access($1,$2,'invoke',$3)",
                    &[&hash, &resource, &release],
                )
                .await?
                .get(0);
            let inherited: Vec<String> = serde_json::from_value(inherited)?;
            self.record_consumed_markings(
                &inherited
                    .into_iter()
                    .map(|marking| ConsumedMarking {
                        environment: self.environment.clone(),
                        marking,
                    })
                    .collect::<Vec<_>>(),
            )?;
            crate::execution::invocation_permission(
                &snapshot.identity,
                resource.parse()?,
                "functions",
                "invoke",
                &self
                    .consumed_markings()
                    .iter()
                    .map(|m| m.marking.clone())
                    .collect::<Vec<_>>(),
            )?;
            let stopped:bool=client.query_one("WITH RECURSIVE ancestors AS(SELECT parent_id FROM moria.runtime_function_invocations WHERE execution=$1 UNION ALL SELECT f.parent_id FROM ancestors a JOIN moria.runtime_function_invocations f ON f.execution=a.parent_id) SELECT EXISTS(SELECT 1 FROM ancestors a JOIN moria.runtime_executions e ON e.id=a.parent_id WHERE e.status IN ('failed','cancelled') OR e.deadline<=clock_timestamp())", &[&self.lease.execution]).await?.get(0);
            ensure!(!stopped, "CANCELLED");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpCall {
    pub id: String,
    pub method: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub body: Option<String>,
}
#[derive(Clone, Debug, Serialize)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: String,
}
#[async_trait]
pub trait PlatformBroker: Send + Sync {
    async fn start(&self, context: &SandboxContext) -> Result<()>;
    async fn http(&self, context: &SandboxContext, call: HttpCall) -> Result<HttpResponse>;
    async fn log(
        &self,
        context: &SandboxContext,
        message: &str,
        consumed: &[ConsumedMarking],
    ) -> Result<()>;
    async fn finish(&self, context: &SandboxContext, result: &Value) -> Result<()>;
}

#[derive(Clone, Debug)]
struct FunctionBinding {
    image: String,
    outputs: Value,
    memory: i64,
    cpu: i64,
    adapter: Option<String>,
    scratch: Option<Arc<crate::scratch::VerifiedScratch>>,
    protocol_bytes: usize,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxResult {
    pub result: Value,
    pub consumed_markings: Vec<ConsumedMarking>,
    pub attempt_id: Uuid,
    pub correlation_id: Uuid,
}

#[derive(Clone)]
pub(crate) struct Docker {
    pub(crate) client: reqwest::Client,
}
impl Docker {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .unix_socket("/var/run/docker.sock")
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(5))
                .build()?,
        })
    }
    pub(crate) fn url(segments: &[&str]) -> Result<reqwest::Url> {
        let mut url = reqwest::Url::parse(&format!("http://localhost/{DOCKER_API}"))?;
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("FUNCTION_WORKER_UNAVAILABLE"))?;
            for segment in segments {
                path.push(segment);
            }
        }
        Ok(url)
    }
    pub(crate) async fn json_response(response: reqwest::Response) -> Result<Value> {
        ensure!(
            response.status().is_success(),
            "FUNCTION_WORKER_UNAVAILABLE"
        );
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.try_next().await? {
            ensure!(
                bytes.len() + chunk.len() <= 1048576,
                "FUNCTION_WORKER_PROTOCOL"
            );
            bytes.extend_from_slice(&chunk);
        }
        Ok(if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)?
        })
    }
    pub(crate) async fn inspect(&self, name: &str) -> Result<Option<Value>> {
        let response = self
            .client
            .get(Self::url(&["containers", name, "json"])?)
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::json_response(response).await?))
    }
    async fn image(&self, image: &str) -> Result<()> {
        let metadata = Self::json_response(
            self.client
                .get(Self::url(&["images", image, "json"])?)
                .send()
                .await?,
        )
        .await?;
        ensure!(
            metadata["Os"] == "linux"
                && metadata["RepoDigests"]
                    .as_array()
                    .is_some_and(|digests| digests.iter().any(|d| d == image))
                && metadata
                    .pointer("/Config/Volumes")
                    .is_none_or(|v| v.is_null() || v.as_object().is_some_and(|o| o.is_empty())),
            "FUNCTION_ARTIFACT_INVALID"
        );
        Ok(())
    }
    async fn derived_image(&self, image: &str, adapter: &str) -> Result<()> {
        self.image(adapter).await?;
        let child = Self::json_response(
            self.client
                .get(Self::url(&["images", image, "json"])?)
                .send()
                .await?,
        )
        .await?;
        let parent = Self::json_response(
            self.client
                .get(Self::url(&["images", adapter, "json"])?)
                .send()
                .await?,
        )
        .await?;
        let parent = parent
            .pointer("/RootFS/Layers")
            .and_then(Value::as_array)
            .filter(|layers| !layers.is_empty())
            .ok_or_else(|| anyhow::anyhow!("AGENT_ADAPTER_INVALID"))?;
        let child = child
            .pointer("/RootFS/Layers")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("AGENT_ADAPTER_INVALID"))?;
        ensure!(child.starts_with(parent), "AGENT_ADAPTER_INVALID");
        Ok(())
    }
    async fn create(
        &self,
        name: &str,
        context: &SandboxContext,
        binding: &FunctionBinding,
    ) -> Result<()> {
        let memory = binding.memory * 1024 * 1024;
        let config = json!({"Image":binding.image,"User":"65532:65532","AttachStdin":true,"AttachStdout":true,"AttachStderr":true,"OpenStdin":true,"StdinOnce":true,"Tty":false,
          "Healthcheck":{"Test":["NONE"]},"Labels":{"moria.component":format!("native-{}",context.domain.name()),"moria.execution":context.lease.execution.to_string()},
          "HostConfig":{"NetworkMode":"none","ReadonlyRootfs":true,"CapDrop":["ALL"],"SecurityOpt":["no-new-privileges:true"],"PidsLimit":64,"Memory":memory,"MemorySwap":memory,"NanoCpus":1000000000,
            "Ulimits":[{"Name":"cpu","Soft":(binding.cpu+999)/1000,"Hard":(binding.cpu+999)/1000}],"IpcMode":"none","Tmpfs":{"/tmp":"rw,noexec,nosuid,nodev,size=16777216,mode=1777"},"LogConfig":{"Type":"none","Config":{}}}});
        if let Some(scratch) = &binding.scratch {
            ensure!(name == scratch.container_name(), "SCRATCH_UNAVAILABLE");
            scratch.create_container(config).await?;
            return Ok(());
        }
        Self::json_response(
            self.client
                .post(Self::url(&["containers", "create"])?)
                .query(&[("name", name)])
                .header("content-type", "application/json")
                .body(serde_json::to_vec(&config)?)
                .send()
                .await?,
        )
        .await?;
        Ok(())
    }
    async fn attach(&self, name: &str) -> Result<(BufReader<OwnedReadHalf>, OwnedWriteHalf)> {
        let stream = UnixStream::connect("/var/run/docker.sock").await?;
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        writer.write_all(format!("POST /{DOCKER_API}/containers/{name}/attach?stream=1&stdin=1&stdout=1&stderr=1 HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: tcp\r\nContent-Length: 0\r\n\r\n").as_bytes()).await?;
        let mut header = Vec::new();
        loop {
            let mut line = Vec::new();
            let count = reader.read_until(b'\n', &mut line).await?;
            ensure!(
                count > 0 && header.len() + count <= 8192,
                "FUNCTION_WORKER_PROTOCOL"
            );
            let finished = line == b"\r\n";
            header.extend(line);
            if finished {
                break;
            }
        }
        let text = std::str::from_utf8(&header)?;
        ensure!(
            text.starts_with("HTTP/1.1 101 ")
                || (text.starts_with("HTTP/1.1 200 ")
                    && text
                        .to_ascii_lowercase()
                        .contains("application/vnd.docker.raw-stream")),
            "FUNCTION_WORKER_PROTOCOL"
        );
        Ok((reader, writer))
    }
    async fn start(&self, name: &str) -> Result<()> {
        Self::json_response(
            self.client
                .post(Self::url(&["containers", name, "start"])?)
                .send()
                .await?,
        )
        .await?;
        Ok(())
    }
    async fn resources(&self, name: &str) -> Result<(u64, u64)> {
        let data = Self::json_response(
            self.client
                .get(Self::url(&["containers", name, "stats"])?)
                .query(&[("stream", "false"), ("one-shot", "true")])
                .timeout(Duration::from_secs(1))
                .send()
                .await?,
        )
        .await?;
        let cpu = data
            .pointer("/cpu_stats/cpu_usage/total_usage")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_WORKER_ACCOUNTING"))?;
        let memory = data
            .pointer("/memory_stats/usage")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_WORKER_ACCOUNTING"))?;
        Ok((cpu, memory))
    }
    async fn wait(&self, name: &str) -> Result<i64> {
        let data = Self::json_response(
            self.client
                .post(Self::url(&["containers", name, "wait"])?)
                .query(&[("condition", "not-running")])
                .send()
                .await?,
        )
        .await?;
        data["StatusCode"]
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_WORKER_UNAVAILABLE"))
    }
    async fn stop_scoped(&self, name: &str, execution: Uuid) -> Result<Option<String>> {
        let Some(metadata) = self.inspect(name).await? else {
            return Ok(None);
        };
        ensure!(
            metadata
                .pointer("/Config/Labels/moria.component")
                .and_then(Value::as_str)
                .is_some_and(|component| [
                    "native-function",
                    "native-agent",
                    "native-evaluator",
                    "native-transform",
                    "native-sync"
                ]
                .contains(&component))
                && metadata.pointer("/Config/Labels/moria.execution")
                    == Some(&json!(execution.to_string())),
            "FUNCTION_CLEANUP_SCOPE"
        );
        let id = metadata["Id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_WORKER_UNAVAILABLE"))?;
        if metadata.pointer("/State/Running") == Some(&Value::Bool(true)) {
            let response = self
                .client
                .post(Self::url(&["containers", id, "stop"])?)
                .query(&[("t", "1")])
                .timeout(Duration::from_secs(3))
                .send()
                .await?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            ensure!(
                response.status().is_success()
                    || response.status() == reqwest::StatusCode::NOT_MODIFIED,
                "FUNCTION_CLEANUP_FAILED"
            );
        }
        Ok(Some(id.into()))
    }
    async fn remove(&self, name: &str, execution: Uuid, creation: u8) -> Result<bool> {
        let Some(id) = self.stop_scoped(name, execution).await? else {
            return Ok(creation != 1);
        };
        let response = self
            .client
            .delete(Self::url(&["containers", &id])?)
            .query(&[("force", "true"), ("v", "true")])
            .timeout(Duration::from_secs(3))
            .send()
            .await?;
        ensure!(
            response.status().is_success() || response.status() == reqwest::StatusCode::NOT_FOUND,
            "FUNCTION_CLEANUP_FAILED"
        );
        Ok(true)
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum GuestMessage {
    #[serde(rename = "result")]
    Result { version: u32, value: Value },
    #[serde(rename = "error")]
    Error { version: u32 },
    #[serde(rename = "log")]
    Log { version: u32, message: String },
    #[serde(rename = "http")]
    Http {
        version: u32,
        id: String,
        method: String,
        path: String,
        headers: BTreeMap<String, String>,
        body: Option<String>,
    },
    #[serde(rename = "osdk")]
    Osdk {
        version: u32,
        id: String,
        method: String,
        path: String,
        input: Value,
    },
}
async fn frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    total: &mut usize,
    maximum: usize,
) -> Result<Option<(u8, Vec<u8>)>> {
    let mut header = [0_u8; 8];
    if reader.read(&mut header[..1]).await? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut header[1..]).await?;
    let length = u32::from_be_bytes(header[4..8].try_into()?) as usize;
    ensure!(
        [1, 2].contains(&header[0])
            && header[1..4] == [0, 0, 0]
            && length
                <= if maximum == OUTPUT_BYTES {
                    OUTPUT_BYTES
                } else {
                    2 * 1024 * 1024
                }
            && total.saturating_add(length + 8) <= maximum,
        "FUNCTION_PROTOCOL"
    );
    *total += length + 8;
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes).await?;
    Ok(Some((header[0], bytes)))
}
async fn write<W: AsyncWrite + Unpin>(writer: &mut W, value: Value) -> Result<()> {
    write_limited(writer, value, FRAME_BYTES).await
}
async fn write_limited<W: AsyncWrite + Unpin>(
    writer: &mut W,
    value: Value,
    maximum: usize,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(&value)?;
    ensure!(bytes.len() <= maximum, "FUNCTION_PROTOCOL");
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}
fn call(
    id: String,
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Option<String>,
) -> Result<HttpCall> {
    call_profile(id, method, path, headers, body, false)
}
fn call_profile(
    id: String,
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Option<String>,
    engine: bool,
) -> Result<HttpCall> {
    ensure!(
        !id.is_empty()
            && id.len() <= if engine { 20 } else { 3 }
            && id.bytes().all(|b| b.is_ascii_digit())
            && ["GET", "POST", "PUT", "PATCH", "DELETE"].contains(&method.as_str()),
        "FUNCTION_PROTOCOL"
    );
    ensure!(
        path.starts_with("/api/")
            && path.len() <= 2048
            && !path.bytes().any(|b| b.is_ascii_control())
            && headers.len() <= 16,
        "FUNCTION_REQUEST_INVALID"
    );
    for (key, value) in &headers {
        ensure!(
            [
                "content-type",
                "idempotency-key",
                "accept",
                "x-moria-value-version",
                "x-moria-size",
                "x-moria-digest"
            ]
            .contains(&key.to_ascii_lowercase().as_str())
                && value.len() <= 1024
                && !value.contains(['\r', '\n']),
            "FUNCTION_REQUEST_INVALID"
        );
    }
    if let Some(body) = &body {
        let maximum = if engine { 1024 * 1024 } else { 32768 };
        ensure!(
            body.len() <= maximum * 4 / 3 + 4 && STANDARD.decode(body)?.len() <= maximum,
            "FUNCTION_PROTOCOL"
        );
    }
    Ok(HttpCall {
        id,
        method,
        path,
        headers,
        body,
    })
}

pub struct SandboxSupervisor {
    db: Database,
    host_id: Uuid,
    broker: Arc<dyn PlatformBroker>,
    docker: Docker,
}
impl SandboxSupervisor {
    async fn cleanup_attempt(&self, name: &str, execution: Uuid, creation: u8) -> Result<bool> {
        // Stop known owned code even when PostgreSQL is unavailable. Durable removal
        // and scratch reuse still require the serialized database cleanup proof.
        self.docker.stop_scoped(name, execution).await?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let locked: bool = tx
            .query_one(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))",
                &[&format!("moria/sandbox-cleanup/{execution}")],
            )
            .await?
            .get(0);
        if !locked {
            return Ok(false);
        }
        let result = self.cleanup_attempt_locked(name, execution, creation).await;
        tx.commit().await?;
        result
    }
    async fn cleanup_attempt_locked(
        &self,
        name: &str,
        execution: Uuid,
        creation: u8,
    ) -> Result<bool> {
        if name.starts_with("moria-engine-scratch-") {
            let reservation:Uuid=self.db.get().await?.query_one("SELECT r.id FROM moria.runtime_sandbox_attempts s JOIN moria.runtime_engine_scratch_reservations r ON r.execution=s.execution AND r.task=s.task AND r.owner=s.owner AND r.fence=s.fence AND r.host_id=s.host_id WHERE s.execution=$1 AND s.host_id=$2 AND s.container_name=$3 AND s.domain IN ('transform','sync')",&[&execution,&self.host_id,&name]).await?.get(0);
            self.docker.stop_scoped(name, execution).await?;
            crate::scratch::cleanup_reservation(&self.db, reservation, self.host_id).await?;
            Ok(true)
        } else {
            self.docker.remove(name, execution, creation).await
        }
    }
    async fn engine_sealed_result(&self, context: &SandboxContext, value: &Value) -> Result<Value> {
        ensure!(
            *value == json!({"complete":true}),
            "PIPELINE_OUTPUT_INVALID"
        );
        let client = self.db.get().await?;
        context.check_in(&client).await?;
        let outputs: Value = client
            .query_one(
                "SELECT outputs FROM moria.runtime_pipeline_executions WHERE execution=$1",
                &[&context.lease.execution],
            )
            .await?
            .get(0);
        let expected = outputs
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))?
            .values()
            .map(|value| {
                value["resource"]
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow::anyhow!("PIPELINE_BINDING_INVALID"))
            })
            .collect::<Result<BTreeSet<_>>>()?;
        let rows=client.query("SELECT resource::text,root FROM moria.runtime_pipeline_outputs WHERE execution=$1 AND task=$2 AND owner=$3 AND fence=$4",&[&context.lease.execution,&context.lease.task,&context.lease.owner,&context.lease.fence.parse::<i64>()?]).await?;
        let result: BTreeMap<String, Value> = rows
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        ensure!(
            !result.is_empty() && result.keys().cloned().collect::<BTreeSet<_>>() == expected,
            "PIPELINE_OUTPUT_MISSING"
        );
        context.check_in(&client).await?;
        Ok(json!({"datasetOutputs":result}))
    }
    async fn point_profile(
        client: &tokio_postgres::Client,
        execution: Uuid,
        effect: &str,
    ) -> Result<bool> {
        if effect == "editProposal" {
            return Ok(true);
        }
        Ok(client.query_one("WITH RECURSIVE ancestors AS(SELECT execution,parent_execution,1 AS depth FROM moria.runtime_job_delegations WHERE execution=$1 UNION ALL SELECT d.execution,d.parent_execution,a.depth+1 FROM ancestors a JOIN moria.runtime_job_delegations d ON d.execution=a.parent_execution WHERE a.depth<8) SELECT COALESCE(bool_or(f.prepare_only OR f.definition->'spec'->>'effect'='editProposal'),false) FROM ancestors a LEFT JOIN moria.runtime_function_invocations f ON f.execution=a.execution", &[&execution]).await?.get(0))
    }
    pub fn new(db: Database, host_id: Uuid, broker: Arc<dyn PlatformBroker>) -> Result<Self> {
        Ok(Self {
            db,
            host_id,
            broker,
            docker: Docker::new()?,
        })
    }
    pub async fn claim(&self, owner: Uuid) -> Result<Option<crate::execution::Claim>> {
        self.claim_for(owner, None).await
    }
    pub(crate) async fn claim_for(
        &self,
        owner: Uuid,
        execution: Option<Uuid>,
    ) -> Result<Option<crate::execution::Claim>> {
        self.db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_function_worker_check($1,$2)",
                &[&owner, &self.host_id],
            )
            .await?;
        ExecutionAuthority {
            db: self.db.clone(),
        }
        .claim_function(owner, false, execution, self.host_id)
        .await
    }

    pub(crate) async fn claim_publication(
        &self,
        owner: Uuid,
        execution: Option<Uuid>,
    ) -> Result<Option<crate::execution::Claim>> {
        self.db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_function_worker_check($1,$2)",
                &[&owner, &self.host_id],
            )
            .await?;
        ExecutionAuthority {
            db: self.db.clone(),
        }
        .claim_function(owner, true, execution, self.host_id)
        .await
    }

    pub(crate) async fn claim_agent(
        &self,
        owner: Uuid,
        publication: bool,
        execution: Option<Uuid>,
    ) -> Result<Option<crate::execution::Claim>> {
        ExecutionAuthority {
            db: self.db.clone(),
        }
        .claim_agent(owner, publication, execution, self.host_id)
        .await
    }

    pub(crate) async fn publication_context(&self, lease: Lease) -> Result<SandboxContext> {
        ensure!(lease.task.is_none(), "FUNCTION_LEASE_INVALID");
        let client = self.db.get().await?;
        let row=client.query_one("SELECT e.context,e.environment,f.effect,f.outputs,f.domain,s.attempt_id,s.correlation_id,s.consumed_markings,s.container_name,i.prepare_only,i.release FROM moria.runtime_sandbox_attempts s JOIN moria.runtime_executions e ON e.id=s.execution JOIN moria.runtime_sandbox_bindings f ON f.binding=e.binding AND f.domain=e.spec->>'domain' JOIN moria.runtime_sandbox_metadata i ON i.execution=e.id AND i.domain=f.domain WHERE e.id=$1 AND s.state='sealed' AND s.cleaned", &[&lease.execution]).await?;
        let actor: Value = row.get("context");
        let environment: String = row.get("environment");
        let labels: Value = row.get("consumed_markings");
        let markings = labels
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
            .iter()
            .map(|label| {
                Ok(ConsumedMarking {
                    environment: label["environment"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
                        .into(),
                    marking: label["marking"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
                        .into(),
                })
            })
            .collect::<Result<BTreeSet<_>>>()?;
        let effect: String = row.get("effect");
        let point_reads_only = Self::point_profile(&client, lease.execution, &effect).await?;
        let original_session_hash = actor["sessionHash"].as_str().map(str::to_owned);
        let session_hash = if client
            .query_opt(
                "SELECT 1 FROM moria.runtime_job_delegations WHERE execution=$1",
                &[&lease.execution],
            )
            .await?
            .is_some()
        {
            Some(
                crate::execution::job_session_in(&client, &lease, self.host_id)
                    .await?
                    .session_hash()
                    .to_owned(),
            )
        } else {
            original_session_hash.clone()
        };
        let context = SandboxContext {
            db: self.db.clone(),
            host_id: self.host_id,
            lease,
            environment,
            principal: actor["principal"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_BINDING_INVALID"))?
                .into(),
            application: actor["application"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_BINDING_INVALID"))?
                .into(),
            session_hash,
            original_session_hash,
            effect,
            source_release: Some(row.get("release")),
            release_blocked: Arc::new(AtomicBool::new(false)),
            observation: Arc::new(Mutex::new(None)),
            authority_metrics: Arc::new(AuthorityMetrics::default()),
            domain: match row.get::<_, String>("domain").as_str() {
                "function" => SandboxDomain::Function,
                "agent" => SandboxDomain::Agent,
                "evaluator" => SandboxDomain::Evaluator,
                "transform" => SandboxDomain::Transform,
                "sync" => SandboxDomain::Sync,
                _ => bail!("SANDBOX_DOMAIN_INVALID"),
            },
            container_name: row.get("container_name"),
            outputs: row.get("outputs"),
            prepare_only: row.get("prepare_only"),
            point_reads_only,
            attempt: row.get("attempt_id"),
            correlation: row.get("correlation_id"),
            markings: Arc::new(Mutex::new(markings)),
        };
        context.check().await?;
        Ok(context)
    }
    async fn prepare(&self, lease: Lease) -> Result<(SandboxContext, FunctionBinding, Value, u64)> {
        let task = lease
            .task
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_LEASE_INVALID"))?;
        let fence: i64 = lease.fence.parse()?;
        ensure!(
            fence > 0 && fence.to_string() == lease.fence,
            "FUNCTION_LEASE_INVALID"
        );
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one("SELECT moria.runtime_function_worker_check($1,$2,e.resource_class,e.spec->>'domain') FROM moria.runtime_executions e WHERE e.id=$3", &[&lease.owner,&self.host_id,&lease.execution]).await?;
        ensure!(
            tx.query_opt(
                "SELECT 1 FROM moria.indexing_instance WHERE incarnation=$1",
                &[&lease.incarnation]
            )
            .await?
            .is_some(),
            "INCARNATION_CHANGED"
        );
        tx.query_one(
            "SELECT moria.runtime_task_check($1,$2,$3)",
            &[&task, &lease.owner, &fence],
        )
        .await?;
        let row=tx.query_one("SELECT e.environment,e.context,e.spec,e.input,t.attempts,f.*,floor(extract(epoch FROM((CASE WHEN f.domain IN ('agent','transform','sync') THEN e.deadline ELSE LEAST(e.deadline,t.lease_until) END)-clock_timestamp()))*1000)::bigint AS remaining_ms FROM moria.runtime_executions e JOIN moria.runtime_execution_tasks t ON t.execution=e.id AND t.id=$2 JOIN moria.runtime_sandbox_bindings f ON f.binding=e.binding AND f.domain=e.spec->>'domain' WHERE e.id=$1", &[&lease.execution,&task]).await.context("SANDBOX_BINDING")?;
        let spec: Value = row.get("spec");
        let actor: Value = row.get("context");
        let environment: String = row.get("environment");
        ensure!(
            spec["version"] == 1
                && (spec["domain"] == "function"
                    || spec["domain"] == "agent"
                    || spec["domain"] == "evaluator"
                    || spec["domain"] == "transform"
                    || spec["domain"] == "sync")
                && spec["retryPolicy"] == "never"
                && row.get::<_, i32>("attempts") == 1
                && row.get::<_, i32>("value_version") == 1,
            "FUNCTION_BINDING_INVALID"
        );
        let uri: String = row.get("artifact_uri");
        let digest: String = row.get("artifact_digest");
        let image = artifact_image(&json!({"uri":uri,"digest":digest}))?;
        let memory: i32 = row.get("memory_mib");
        let cpu: i32 = row.get("cpu_millis");
        let timeout: i32 = row.get("timeout_seconds");
        let effect: String = row.get("effect");
        let domain = match spec["domain"].as_str() {
            Some("agent") => SandboxDomain::Agent,
            Some("evaluator") => SandboxDomain::Evaluator,
            Some("function") => SandboxDomain::Function,
            Some("transform") => SandboxDomain::Transform,
            Some("sync") => SandboxDomain::Sync,
            _ => bail!("SANDBOX_DOMAIN_INVALID"),
        };
        let maximum = if domain.engine() {
            86400
        } else if domain == SandboxDomain::Agent {
            600
        } else {
            30
        };
        ensure!(
            (32..=if domain.engine() { 4096 } else { 1024 }).contains(&memory)
                && (1..=maximum * 1000).contains(&cpu)
                && (1..=maximum).contains(&timeout)
                && ["readOnly", "readWrite", "editProposal"].contains(&effect.as_str()),
            "FUNCTION_LIMIT_INVALID"
        );
        let input_schema = values::descriptor(&spec["inputSchema"], &json!({}), 0)?;
        ensure!(
            input_schema["type"] == "object"
                && input_schema
                    .get("optional")
                    .is_none_or(|v| v.as_array().is_some_and(|a| a.is_empty())),
            "FUNCTION_BINDING_INVALID"
        );
        let input = values::normalize(&row.get::<_, Value>("input"), &input_schema)?;
        let outputs: Value = row.get("outputs");
        ensure!(outputs.is_object(), "FUNCTION_BINDING_INVALID");
        values::descriptor(
            &json!({"type":"object","properties":outputs}),
            &json!({}),
            0,
        )?;
        let mut attempt = Uuid::new_v4();
        let mut correlation = Uuid::new_v4();
        let name = if domain.engine() {
            let reservation:Uuid=tx.query_one("SELECT id FROM moria.runtime_engine_scratch_reservations WHERE execution=$1 AND task=$2 AND owner=$3 AND fence=$4 AND host_id=$5 AND incarnation=$6 AND cleaned_at IS NULL",&[&lease.execution,&task,&lease.owner,&fence,&self.host_id,&lease.incarnation]).await.context("SANDBOX_RESERVATION")?.get(0);
            format!("moria-engine-scratch-{reservation}")
        } else {
            format!("moria-native-{}-{}", domain.name(), lease.execution)
        };
        let principal = actor["principal"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_BINDING_INVALID"))?
            .to_owned();
        let application = actor["application"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_BINDING_INVALID"))?
            .to_owned();
        ensure!(
            actor["environment"].as_str() == Some(environment.as_str()),
            "FUNCTION_BINDING_INVALID"
        );
        let mut markings: Vec<String> = tx
            .query_one(
                "SELECT markings FROM moria.runtime_execution_bindings WHERE id=$1",
                &[&row.get::<_, Uuid>("binding")],
            )
            .await?
            .get(0);
        let mut prepare_only = false;
        let mut source_release = None;
        if let Some(row) = tx
            .query_opt(
                "SELECT markings,attempt_id,correlation_id,prepare_only,release FROM moria.runtime_sandbox_metadata WHERE execution=$1",
                &[&lease.execution],
            )
            .await?
        {
            attempt=row.get("attempt_id");correlation=row.get("correlation_id");
            prepare_only = row.get("prepare_only");
            source_release=Some(row.get("release"));
            let labels: Value = row.get(0);
            for label in labels
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
            {
                ensure!(
                    label["environment"].as_str() == Some(environment.as_str()),
                    "FUNCTION_MARKINGS_INVALID"
                );
                markings.push(
                    label["marking"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("FUNCTION_MARKINGS_INVALID"))?
                        .into(),
                );
            }
            markings.sort();
            markings.dedup();
        }
        ensure!(
            markings.len() <= 256
                && markings.iter().all(|marking| !marking.is_empty()
                    && marking.len() <= 180
                    && !marking.contains('\0')),
            "FUNCTION_MARKINGS_INVALID"
        );
        let point_reads_only = Self::point_profile(tx.client(), lease.execution, &effect).await?;
        let original_session_hash = actor["sessionHash"].as_str().map(str::to_owned);
        let session_hash = if tx
            .query_opt(
                "SELECT 1 FROM moria.runtime_job_delegations WHERE execution=$1",
                &[&lease.execution],
            )
            .await?
            .is_some()
        {
            Some(
                crate::execution::job_session_in(tx.client(), &lease, self.host_id)
                    .await
                    .context("SANDBOX_JOB_SESSION")?
                    .session_hash()
                    .to_owned(),
            )
        } else {
            original_session_hash.clone()
        };
        let context = SandboxContext {
            db: self.db.clone(),
            host_id: self.host_id,
            lease: lease.clone(),
            environment: environment.clone(),
            principal,
            application,
            session_hash,
            original_session_hash,
            effect,
            domain,
            source_release,
            release_blocked: Arc::new(AtomicBool::new(false)),
            observation: Arc::new(Mutex::new(None)),
            authority_metrics: Arc::new(AuthorityMetrics::default()),
            container_name: name.clone(),
            outputs: outputs.clone(),
            prepare_only,
            point_reads_only,
            attempt,
            correlation,
            markings: Arc::new(Mutex::new(
                markings
                    .into_iter()
                    .map(|marking| ConsumedMarking {
                        environment: environment.clone(),
                        marking,
                    })
                    .collect(),
            )),
        };
        let remaining: i64 = row.get("remaining_ms");
        ensure!(remaining > 0, "FUNCTION_TIMEOUT");
        let created=tx.query_opt("INSERT INTO moria.runtime_sandbox_attempts(execution,task,binding,owner,fence,host_id,attempt_id,correlation_id,container_name,state,domain) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'starting',$10) ON CONFLICT DO NOTHING RETURNING execution", &[&lease.execution,&task,&row.get::<_,Uuid>("binding"),&lease.owner,&fence,&self.host_id,&attempt,&correlation,&name,&domain.name()]).await?;
        ensure!(created.is_some(), "FUNCTION_ALREADY_EXECUTED");
        let start = json!({"version":1,"valueVersion":1,"type":"start","invocationId":lease.execution,"attemptId":attempt,"input":input,"inputs":input_schema["properties"],"outputs":outputs,"effect":context.effect});
        ensure!(
            serde_json::to_vec(&start)?.len() <= FRAME_BYTES,
            "FUNCTION_INPUT_INVALID"
        );
        let mut protocol_bytes = OUTPUT_BYTES;
        let adapter = if domain.engine() {
            let profile:Value=tx.query_one("SELECT p.profile FROM moria.runtime_pipeline_executions r JOIN moria.runtime_pipeline_profiles p ON p.id=r.profile AND p.enabled WHERE r.execution=$1",&[&lease.execution]).await?.get(0);
            let bytes = profile["limits"]["inputBytes"]
                .as_u64()
                .and_then(|a| {
                    profile["limits"]["outputBytes"]
                        .as_u64()
                        .and_then(|b| a.checked_add(b))
                })
                .and_then(|v| v.checked_mul(8))
                .and_then(|v| v.checked_add(64 * 1024 * 1024))
                .ok_or_else(|| anyhow::anyhow!("PIPELINE_PROFILE_INVALID"))?;
            protocol_bytes = usize::try_from(bytes)?;
            Some(artifact_image(&profile["adapter"])?)
        } else if domain == SandboxDomain::Agent {
            Some(crate::agents::adapter_image(tx.client(), lease.execution).await?)
        } else {
            None
        };
        tx.commit().await?;
        let scratch = if domain.engine() {
            Some(Arc::new(
                crate::scratch::VerifiedScratch::for_lease(self.db.clone(), &lease, self.host_id)
                    .await
                    .context("SCRATCH_ADMISSION")?,
            ))
        } else {
            None
        };
        Ok((
            context,
            FunctionBinding {
                image,
                outputs,
                memory: i64::from(memory),
                cpu: i64::from(cpu),
                adapter,
                scratch,
                protocol_bytes,
            },
            start,
            (remaining as u64).min(timeout as u64 * 1000),
        ))
    }

    async fn run(
        &self,
        context: &SandboxContext,
        binding: &FunctionBinding,
        start: Value,
        creation: Arc<AtomicU8>,
        observed: Arc<ObservedResources>,
    ) -> Result<Value> {
        let name = context.container_name();
        self.docker.image(&binding.image).await?;
        if let Some(adapter) = &binding.adapter {
            self.docker.derived_image(&binding.image, adapter).await?;
        }
        context.check().await?;
        self.db
            .get()
            .await?
            .execute(
                "UPDATE moria.runtime_sandbox_attempts SET creation_state=1 WHERE execution=$1",
                &[&context.lease.execution],
            )
            .await?;
        creation.store(1, Ordering::SeqCst);
        self.docker.create(&name, context, binding).await?;
        creation.store(2, Ordering::SeqCst);
        self.db
            .get()
            .await?
            .execute(
                "UPDATE moria.runtime_sandbox_attempts SET creation_state=2 WHERE execution=$1",
                &[&context.lease.execution],
            )
            .await?;
        let (mut reader, mut writer) =
            tokio::time::timeout(Duration::from_secs(5), self.docker.attach(&name)).await??;
        context.check().await?;
        self.docker.start(&name).await?;
        self.db.get().await?.execute("UPDATE moria.runtime_sandbox_attempts SET state='running' WHERE execution=$1 AND owner=$2 AND fence=$3", &[&context.lease.execution,&context.lease.owner,&context.lease.fence.parse::<i64>()?]).await?;
        write(&mut writer, start).await?;
        let completed = Arc::new(AtomicBool::new(false));
        let completed_monitor = completed.clone();
        let monitor = async {
            loop {
                tokio::time::sleep(Duration::from_millis(200)).await;
                match self.docker.resources(&name).await {
                    Ok((cpu, memory)) => {
                        observed.record(cpu, memory);
                        ensure!(cpu <= binding.cpu as u64 * 1000000, "FUNCTION_CPU_LIMIT");
                    }
                    Err(_) => {
                        let stopped = self.docker.inspect(&name).await?.is_some_and(|m| {
                            m.pointer("/State/Running") == Some(&Value::Bool(false))
                        });
                        ensure!(
                            stopped && completed_monitor.load(Ordering::SeqCst),
                            "FUNCTION_WORKER_ACCOUNTING"
                        );
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        };
        let protocol = async {
            let engine = context.domain.engine();
            let frame_bytes = if engine { 2 * 1024 * 1024 } else { FRAME_BYTES };
            let mut total = 0;
            let mut buffer = Vec::new();
            let mut result = None;
            let mut calls = BTreeSet::new();
            let mut sequence = 0u64;
            let mut logs = 0;
            while let Some((channel, bytes)) =
                frame(&mut reader, &mut total, binding.protocol_bytes).await?
            {
                if channel == 2 {
                    continue;
                }
                buffer.extend(bytes);
                while let Some(end) = buffer.iter().position(|b| *b == b'\n') {
                    ensure!(end <= frame_bytes && result.is_none(), "FUNCTION_PROTOCOL");
                    let line: Vec<u8> = buffer.drain(..=end).collect();
                    let message: GuestMessage = serde_json::from_slice(&line[..end])
                        .map_err(|_| anyhow::anyhow!("FUNCTION_PROTOCOL"))?;
                    match message {
                        GuestMessage::Result { version, value } => {
                            ensure!(version == 1, "FUNCTION_PROTOCOL");
                            let descriptor = values::descriptor(
                                &json!({"type":"object","properties":binding.outputs}),
                                &json!({}),
                                0,
                            )?;
                            let value = values::normalize(&value, &descriptor)
                                .map_err(|_| anyhow::anyhow!("FUNCTION_OUTPUT_INVALID"))?;
                            ensure!(
                                canonical::canonical_json(&value)?.len() <= 32768,
                                "FUNCTION_OUTPUT_INVALID"
                            );
                            let (cpu, memory) = self.docker.resources(&name).await?;
                            observed.record(cpu, memory);
                            ensure!(cpu <= binding.cpu as u64 * 1000000, "FUNCTION_CPU_LIMIT");
                            ensure!(
                                self.docker.inspect(&name).await?.is_some_and(|m| m
                                    .pointer("/State/Running")
                                    == Some(&Value::Bool(true))),
                                "FUNCTION_WORKER_ACCOUNTING"
                            );
                            context.check().await?;
                            write_limited(
                                &mut writer,
                                json!({"version":1,"type":"complete"}),
                                frame_bytes,
                            )
                            .await?;
                            completed.store(true, Ordering::SeqCst);
                            result = Some(value);
                        }
                        GuestMessage::Error { version } => {
                            ensure!(version == 1, "FUNCTION_PROTOCOL");
                            bail!("FUNCTION_FAILED")
                        }
                        GuestMessage::Log { version, message } => {
                            ensure!(
                                version == 1 && message.len() <= 4096 && logs < 124,
                                "FUNCTION_PROTOCOL"
                            );
                            logs += 1;
                            context
                                .begin_observation(&format!("_system/log/{}", Uuid::new_v4()))
                                .await?;
                            self.broker
                                .log(context, &message, &context.consumed_markings())
                                .await?;
                            context.observation_completed().await?;
                        }
                        GuestMessage::Http {
                            version,
                            id,
                            method,
                            path,
                            headers,
                            body,
                        } => {
                            ensure!(version == 1, "FUNCTION_PROTOCOL");
                            if engine {
                                sequence = sequence
                                    .checked_add(1)
                                    .ok_or_else(|| anyhow::anyhow!("FUNCTION_PROTOCOL"))?;
                                ensure!(id == sequence.to_string(), "FUNCTION_PROTOCOL");
                            } else {
                                ensure!(
                                    calls.len() < 32 && calls.insert(id.clone()),
                                    "FUNCTION_PROTOCOL"
                                );
                            }
                            let request =
                                call_profile(id.clone(), method, path, headers, body, engine)?;
                            context.begin_observation(&id).await?;
                            match self.broker.http(context, request).await {
                                Ok(response) => {
                                    context.observation_completed().await?;
                                    validate_response_profile(&response, engine)?;
                                    write_limited(&mut writer,json!({"version":1,"type":"httpResult","id":id,"value":response}),frame_bytes).await?;
                                }
                                Err(error) => {
                                    self.broker_failure(context, &id, &error).await?;
                                    context.observation_completed().await?;
                                    write_limited(&mut writer,json!({"version":1,"type":"httpError","id":id,"code":"FUNCTION_BROKER_FAILED"}),frame_bytes).await?;
                                }
                            }
                        }
                        GuestMessage::Osdk {
                            version,
                            id,
                            method,
                            path,
                            input,
                        } => {
                            ensure!(
                                !engine
                                    && version == 1
                                    && calls.len() < 32
                                    && calls.insert(id.clone()),
                                "FUNCTION_PROTOCOL"
                            );
                            let body = if input.is_null() {
                                None
                            } else {
                                Some(STANDARD.encode(serde_json::to_vec(&input)?))
                            };
                            let request = call(
                                id.clone(),
                                method,
                                path,
                                BTreeMap::from([(
                                    "content-type".into(),
                                    "application/json".into(),
                                )]),
                                body,
                            )?;
                            context.begin_observation(&id).await?;
                            match self.broker.http(context, request).await {
                                Ok(response) => {
                                    context.observation_completed().await?;
                                    validate_response(&response)?;
                                    if (200..300).contains(&response.status) {
                                        let value: Value = serde_json::from_slice(
                                            &STANDARD.decode(&response.body)?,
                                        )?;
                                        write(&mut writer,json!({"version":1,"type":"osdkResult","id":id,"value":value})).await?;
                                    } else {
                                        write(&mut writer,json!({"version":1,"type":"osdkError","id":id,"code":"FUNCTION_BROKER_DENIED"})).await?;
                                    }
                                }
                                Err(error) => {
                                    self.broker_failure(context, &id, &error).await?;
                                    context.observation_completed().await?;
                                    write(&mut writer,json!({"version":1,"type":"osdkError","id":id,"code":"FUNCTION_BROKER_FAILED"})).await?;
                                }
                            }
                        }
                    }
                }
                ensure!(buffer.len() <= frame_bytes, "FUNCTION_PROTOCOL");
            }
            ensure!(buffer.is_empty(), "FUNCTION_PROTOCOL");
            ensure!(self.docker.wait(&name).await? == 0, "FUNCTION_FAILED");
            let result = result.ok_or_else(|| anyhow::anyhow!("FUNCTION_OUTPUT_MISSING"))?;
            context.begin_observation("_system/finish").await?;
            self.broker.finish(context, &result).await?;
            context.observation_completed().await?;
            Ok::<_, anyhow::Error>(result)
        };
        tokio::select! {result=protocol=>result,result=monitor=>{result?;bail!("FUNCTION_WORKER_ACCOUNTING")}}
    }

    pub async fn execute(
        &self,
        lease: Lease,
        mut cancelled: watch::Receiver<bool>,
    ) -> Result<SandboxResult> {
        let (context, binding, start, remaining) =
            self.prepare(lease).await.context("SANDBOX_PREPARE")?;
        let name = context.container_name();
        let creation = Arc::new(AtomicU8::new(0));
        let observed = Arc::new(ObservedResources::default());
        let guard = async {
            let mut checks = 0_u32;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                context.check().await?;
                checks += 1;
                if (context.domain == SandboxDomain::Agent || context.domain.engine())
                    && checks.is_multiple_of(8)
                {
                    ExecutionAuthority {
                        db: self.db.clone(),
                    }
                    .renew(&context.lease)
                    .await?;
                    context.check().await?;
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        };
        let cancel = async {
            if *cancelled.borrow() {
                return;
            }
            loop {
                if cancelled.changed().await.is_err() || *cancelled.borrow() {
                    return;
                }
            }
        };
        let result = tokio::select! {
            result=async {context.begin_observation("_system/start").await?;self.broker.start(&context).await?;context.observation_completed().await?; self.run(&context,&binding,start,creation.clone(),observed.clone()).await}=>result,
            _=tokio::time::sleep(Duration::from_millis(remaining))=>Err(anyhow::anyhow!("FUNCTION_TIMEOUT")),
            _=cancel=>Err(anyhow::anyhow!("CANCELLED")),
            result=guard=>{result.and_then(|_|Err(anyhow::anyhow!("FUNCTION_WORKER_LOST")))}
        };
        let cleaned = tokio::time::timeout(
            Duration::from_secs(10),
            self.cleanup_attempt(
                &name,
                context.lease.execution,
                creation.load(Ordering::SeqCst),
            ),
        )
        .await
        .ok()
        .and_then(std::result::Result::ok)
        .unwrap_or(false);
        let result = match result {
            Ok(value) if cleaned => Ok(value),
            Ok(_) => Err(anyhow::anyhow!("FUNCTION_CLEANUP_FAILED")),
            Err(error) => Err(error),
        };
        let labels = serde_json::to_value(context.consumed_markings())?;
        self.db.get().await?.execute("UPDATE moria.runtime_sandbox_attempts SET cleaned=$2,cpu_observed_ns=$3,consumed_markings=moria.runtime_sandbox_markings_union(consumed_markings,$4),memory_observed_bytes=$7,observation_blocked=observation_blocked OR $8 OR observation_pending IS NOT NULL WHERE execution=$1 AND owner=$5 AND fence=$6", &[&context.lease.execution,&cleaned,&(observed.cpu.load(Ordering::SeqCst).min(i64::MAX as u64) as i64),&labels,&context.lease.owner,&context.lease.fence.parse::<i64>()?,&(observed.memory.load(Ordering::SeqCst).min(i64::MAX as u64) as i64),&context.release_blocked.load(Ordering::Acquire)]).await?;
        let result = match result {
            Ok(value) => {
                async {
                    context.check().await?;
                    let value = if context.domain.engine() {
                        self.engine_sealed_result(&context, &value).await?
                    } else {
                        value
                    };
                    let digest = if context.domain.engine() {
                        canonical::fingerprint(&value)?
                    } else {
                        canonical::value_fingerprint(
                            &value,
                            &json!({"type":"object","properties":binding.outputs}),
                            &json!({}),
                        )?
                    };
                    context.check().await?;
                    ExecutionAuthority {
                        db: self.db.clone(),
                    }
                    .seal_function(
                        &context.lease,
                        value.clone(),
                        digest
                            .strip_prefix("sha256:")
                            .ok_or_else(|| anyhow::anyhow!("FUNCTION_OUTPUT_INVALID"))?,
                        self.host_id,
                    )
                    .await?;
                    Ok::<_, anyhow::Error>(value)
                }
                .await
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(value) => {
                self.db.get().await?.execute("UPDATE moria.runtime_sandbox_attempts SET state='sealed',finished_at=clock_timestamp() WHERE execution=$1", &[&context.lease.execution]).await?;
                Ok(SandboxResult {
                    result: value,
                    consumed_markings: context.consumed_markings(),
                    attempt_id: context.attempt,
                    correlation_id: context.correlation,
                })
            }
            Err(error) => {
                let code = error_code(&error);
                let mut client = self.db.get().await?;
                let tx = client.transaction().await?;
                tx.execute("UPDATE moria.runtime_sandbox_attempts SET state='failed',error=$2,finished_at=clock_timestamp() WHERE execution=$1", &[&context.lease.execution,&code]).await?;
                let changed=tx.execute("UPDATE moria.runtime_executions SET status=CASE WHEN $4='CANCELLED' THEN 'cancelled' ELSE 'failed' END,error=$4,finished_at=clock_timestamp() WHERE id=$1 AND status='running' AND EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.execution=$1 AND t.id=$2 AND t.owner=$3 AND t.fence=$5 AND t.status='running')", &[&context.lease.execution,&context.lease.task,&context.lease.owner,&code,&context.lease.fence.parse::<i64>()?]).await?;
                if changed > 0 {
                    if creation.load(Ordering::SeqCst) == 0 {
                        tx.query_one("SELECT moria.runtime_execution_event($1,'noCodeStart',jsonb_build_object('error',$2::text))",&[&context.lease.execution,&code]).await?;
                    }
                    tx.execute("UPDATE moria.runtime_execution_tasks SET status='cancelled',lease_until=NULL WHERE id=$1 AND owner=$2 AND fence=$3", &[&context.lease.task,&context.lease.owner,&context.lease.fence.parse::<i64>()?]).await?;
                    tx.execute("UPDATE moria.runtime_execution_attempts SET status='failed',finished_at=clock_timestamp() WHERE task=$1 AND fence=$2 AND status='running'", &[&context.lease.task,&context.lease.fence.parse::<i64>()?]).await?;
                    tx.query_one("SELECT moria.runtime_execution_event($1,CASE WHEN $2::text='CANCELLED' THEN 'cancelled' ELSE 'failed' END,jsonb_build_object('error',$2::text))", &[&context.lease.execution,&code]).await?;
                }
                tx.commit().await?;
                bail!(code)
            }
        }
    }

    async fn broker_failure(
        &self,
        context: &SandboxContext,
        call: &str,
        error: &anyhow::Error,
    ) -> Result<()> {
        let database = error
            .chain()
            .find_map(|cause| cause.downcast_ref::<tokio_postgres::Error>())
            .and_then(|error| error.as_db_error());
        let root = error.root_cause().to_string();
        let reported = database.map(|error| error.message()).unwrap_or(&root);
        let code = if [
            "JOB_CAPABILITY_MISMATCH",
            "JOB_ORIGIN_MISMATCH",
            "JOB_DELEGATION_DEPTH",
            "JOB_ACCEPTANCE_REQUIRED",
            "JOB_ACTOR_MISMATCH",
            "JOB_DELEGATION_INACTIVE",
            "JOB_PARENT_INACTIVE",
            "JOB_LEASE_LOST",
            "JOB_DELEGATION_REQUIRED",
            "AUTHORIZATION_CHANGED",
            "UNAUTHENTICATED",
            "FORBIDDEN",
            "NOT_FOUND",
            "FUNCTION_RECURSION",
            "FUNCTION_EFFECT",
            "FUNCTION_INPUT_CHANGED",
            "ENGINE_FRAME_LIMIT",
            "ENGINE_INPUT_ALREADY_OPEN",
            "ENGINE_INPUT_LIMIT",
            "ENGINE_IO_INVALID",
            "ENGINE_IPC_COMPLEXITY",
            "ENGINE_IPC_INCOMPLETE",
            "ENGINE_IPC_INVALID",
            "ENGINE_IPC_UNSUPPORTED",
            "ENGINE_OUTPUT_ALREADY_OPEN",
            "ENGINE_OUTPUT_INCOMPLETE",
            "ENGINE_OUTPUT_LIMIT",
            "ENGINE_OUTPUT_NOT_OPEN",
            "ENGINE_SCAN_INVALID",
            "DECODED_BATCH_LIMIT",
            "SCHEMA_CHANGED",
            "UNDECLARED_DATASET",
            "WRITER_FAILED",
        ]
        .contains(&reported)
        {
            reported
        } else {
            "FUNCTION_BROKER_FAILED"
        };
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        if tx.query_opt("SELECT 1 FROM moria.runtime_executions e JOIN moria.runtime_execution_tasks t ON t.execution=e.id WHERE e.id=$1 AND t.id=$2 AND e.status='running' AND t.status='running' AND t.owner=$3 AND t.fence=$4 AND t.lease_until>clock_timestamp() FOR UPDATE OF e,t",&[&context.lease.execution,&context.lease.task,&context.lease.owner,&context.lease.fence.parse::<i64>()?]).await?.is_some() {
            tx.query_one("SELECT moria.runtime_execution_event($1,'brokerFailed',$2)",&[&context.lease.execution,&json!({"call":call,"code":code,"sqlState":database.map(|error|error.code().code())})]).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn cleanup_abandoned(&self) -> Result<usize> {
        let scratch_privilege = crate::scratch::verify_cleanup_privilege().is_ok();
        let rows=self.db.get().await?.query("SELECT s.execution,s.container_name,s.creation_state FROM moria.runtime_sandbox_attempts s JOIN moria.runtime_executions e ON e.id=s.execution WHERE s.host_id=$1 AND (s.domain NOT IN ('transform','sync') OR $2) AND NOT s.cleaned AND (e.status IN ('failed','cancelled','succeeded') OR NOT EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.id=s.task AND t.owner=s.owner AND t.fence=s.fence AND t.status='running' AND t.lease_until>clock_timestamp())) ORDER BY COALESCE(s.cleanup_checked_at,s.created_at),s.execution LIMIT 16", &[&self.host_id,&scratch_privilege]).await?;
        let mut cleaned = 0;
        for row in rows {
            let id: Uuid = row.get(0);
            let name: String = row.get(1);
            let creation: i32 = row.get(2);
            self.db.get().await?.execute("UPDATE moria.runtime_sandbox_attempts SET cleanup_checked_at=clock_timestamp() WHERE execution=$1 AND host_id=$2 AND NOT cleaned",&[&id,&self.host_id]).await?;
            let complete = match self.cleanup_attempt(&name, id, creation as u8).await {
                Ok(complete) => complete,
                Err(error) if cleanup_quarantined(&error) => false,
                Err(error) => return Err(error),
            };
            if complete {
                self.db.get().await?.execute("UPDATE moria.runtime_sandbox_attempts SET cleaned=true WHERE execution=$1 AND host_id=$2", &[&id,&self.host_id]).await?;
                cleaned += 1;
            }
        }
        let reservations = if scratch_privilege {
            self.db.get().await?.query("SELECT r.id FROM moria.runtime_engine_scratch_reservations r JOIN moria.runtime_executions e ON e.id=r.execution JOIN moria.runtime_execution_tasks t ON t.id=r.task WHERE r.host_id=$1 AND r.cleaned_at IS NULL AND (e.status IN ('failed','cancelled','succeeded') OR t.status<>'running' OR t.owner IS DISTINCT FROM r.owner OR t.fence<>r.fence OR t.lease_until<=clock_timestamp() OR e.deadline<=clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM moria.runtime_sandbox_attempts s WHERE s.execution=r.execution) ORDER BY r.id LIMIT 16",&[&self.host_id]).await?
        } else {
            Vec::new()
        };
        for row in reservations {
            match crate::scratch::cleanup_reservation(&self.db, row.get(0), self.host_id).await {
                Ok(()) => cleaned += 1,
                Err(error) if cleanup_quarantined(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(cleaned)
    }
}
fn cleanup_quarantined(error: &anyhow::Error) -> bool {
    [
        "SCRATCH_CREATION_UNCERTAIN",
        "SCRATCH_CLEANUP_UNCERTAIN",
        "SCRATCH_CONTAINER_NOT_TERMINAL",
    ]
    .contains(&error.root_cause().to_string().as_str())
}
fn validate_response(response: &HttpResponse) -> Result<()> {
    validate_response_profile(response, false)
}
fn validate_response_profile(response: &HttpResponse, engine: bool) -> Result<()> {
    ensure!(
        (200..600).contains(&response.status)
            && response.headers.len() <= 16
            && response.body.len()
                <= if engine {
                    1024 * 1024 * 4 / 3 + 4
                } else {
                    32768 * 4 / 3 + 4
                }
            && STANDARD.decode(&response.body)?.len() <= if engine { 1024 * 1024 } else { 32768 },
        "FUNCTION_BROKER_PROTOCOL"
    );
    for (key, value) in &response.headers {
        ensure!(
            ["content-type", "content-length", "etag"].contains(&key.to_ascii_lowercase().as_str())
                && value.len() <= 1024
                && !value.contains(['\r', '\n']),
            "FUNCTION_BROKER_PROTOCOL"
        );
    }
    Ok(())
}
pub fn error_code(error: &anyhow::Error) -> &'static str {
    match error.root_cause().to_string().as_str() {
        "FUNCTION_PROTOCOL" => "FUNCTION_PROTOCOL",
        "FUNCTION_INPUT_INVALID" => "FUNCTION_INPUT_INVALID",
        "FUNCTION_OUTPUT_INVALID" => "FUNCTION_OUTPUT_INVALID",
        "FUNCTION_OUTPUT_MISSING" => "FUNCTION_OUTPUT_MISSING",
        "FUNCTION_CPU_LIMIT" => "FUNCTION_CPU_LIMIT",
        "FUNCTION_TIMEOUT" => "FUNCTION_TIMEOUT",
        "CANCELLED" => "CANCELLED",
        "FUNCTION_CLEANUP_FAILED" => "FUNCTION_CLEANUP_FAILED",
        "FUNCTION_ALREADY_EXECUTED" => "FUNCTION_ALREADY_EXECUTED",
        "FUNCTION_ARTIFACT_INVALID" => "FUNCTION_ARTIFACT_INVALID",
        "FUNCTION_LIMIT_INVALID" => "FUNCTION_LIMIT_INVALID",
        "FUNCTION_BINDING_INVALID" => "FUNCTION_BINDING_INVALID",
        "FUNCTION_OBSERVATION_INCOMPLETE" => "FUNCTION_OBSERVATION_INCOMPLETE",
        "FUNCTION_WORKER_ACCOUNTING" => "FUNCTION_WORKER_ACCOUNTING",
        _ => "FUNCTION_FAILED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn broker_calls_cannot_override_actor_or_select_external_urls() {
        assert!(
            call(
                "1".into(),
                "GET".into(),
                "/api/v1/objects/records/id".into(),
                BTreeMap::new(),
                None
            )
            .is_ok()
        );
        assert!(
            call(
                "1".into(),
                "GET".into(),
                "https://external.invalid".into(),
                BTreeMap::new(),
                None
            )
            .is_err()
        );
        assert!(
            call(
                "1".into(),
                "GET".into(),
                "/api/v1/objects/records/id".into(),
                BTreeMap::from([("authorization".into(), "Bearer forged".into())]),
                None
            )
            .is_err()
        );
    }
}
