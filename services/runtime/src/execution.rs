use crate::{
    authority::{Authority, VerifiedSession},
    database::Database,
    values,
};
use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::{Row, Transaction};
use uuid::Uuid;

/// Select one complete current capability; accepted bindings retain this exact operation.
pub(crate) fn invocation_permission(
    identity: &crate::authorization::Identity,
    resource: Uuid,
    legacy: &str,
    operation: &str,
    markings: &[String],
) -> Result<String> {
    let resource = resource.to_string();
    if crate::authorization::authorize(identity, &resource, operation, markings).is_ok() {
        return Ok(format!("{resource}:{operation}"));
    }
    crate::authorization::authorize(identity, legacy, operation, markings)?;
    Ok(format!("{legacy}:{operation}"))
}

pub(crate) fn transaction_conflict(error: &anyhow::Error) -> bool {
    error
        .chain()
        .filter_map(|cause| cause.downcast_ref::<tokio_postgres::Error>())
        .any(|error| {
            error
                .as_db_error()
                .is_some_and(|db| ["40001", "40P01"].contains(&db.code().code()))
        })
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Domain {
    Transform,
    Sync,
    Function,
    Agent,
    Evaluator,
    Evaluation,
    Delivery,
    Indexing,
    Search,
    Maintenance,
}

/// Internal capability issuance from an exact live, registered native worker lease.
pub(crate) async fn job_session_in(
    client: &tokio_postgres::Client,
    lease: &Lease,
    host: Uuid,
) -> Result<VerifiedSession> {
    let proposed = format!(
        "{:x}",
        Sha256::digest([Uuid::new_v4().into_bytes(), Uuid::new_v4().into_bytes()].concat())
    );
    let hash: String = client
        .query_one(
            "SELECT moria.runtime_job_session_issue($1,$2,$3,$4,$5,$6,$7)",
            &[
                &proposed,
                &lease.execution,
                &lease.task,
                &lease.owner,
                &lease.fence()?,
                &host,
                &lease.incarnation,
            ],
        )
        .await?
        .get(0);
    let row = client
        .query_one(
            "SELECT environment,principal,application FROM moria.runtime_executions WHERE id=$1",
            &[&lease.execution],
        )
        .await?;
    Authority::stored_session_in(
        client,
        &hash,
        &row.get::<_, String>(0),
        &row.get::<_, String>(1),
        &row.get::<_, String>(2),
    )
    .await
}
pub async fn job_session(db: &Database, lease: &Lease, host: Uuid) -> Result<VerifiedSession> {
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    let session = job_session_in(tx.client(), lease, host).await?;
    tx.commit().await?;
    Ok(session)
}
impl Domain {
    fn name(self) -> &'static str {
        match self {
            Self::Transform => "transform",
            Self::Sync => "sync",
            Self::Function => "function",
            Self::Agent => "agent",
            Self::Evaluator => "evaluator",
            Self::Evaluation => "evaluation",
            Self::Delivery => "delivery",
            Self::Indexing => "indexing",
            Self::Search => "search",
            Self::Maintenance => "maintenance",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PinnedInput {
    pub resource: Uuid,
    pub revision: String,
    pub snapshot: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExecutionSpec {
    pub version: u32,
    pub domain: Domain,
    pub release: String,
    pub resource_class: String,
    pub timeout_seconds: u32,
    pub retry_policy: String,
    pub input_schema: Value,
    pub pinned_inputs: Vec<PinnedInput>,
    pub expected_heads: std::collections::BTreeMap<Uuid, String>,
}
impl ExecutionSpec {
    pub(crate) fn validate(&self) -> Result<()> {
        let cap = match self.domain {
            Domain::Function => 30,
            Domain::Agent => 600,
            Domain::Evaluator => 30,
            Domain::Evaluation => 3600,
            Domain::Transform | Domain::Sync => 86400,
            _ => 1800,
        };
        ensure!(
            self.version == 1 && self.timeout_seconds > 0 && self.timeout_seconds <= cap,
            "Invalid execution version or deadline"
        );
        ensure!(
            self.release.len() == 64
                && self
                    .release
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
            "Invalid immutable release"
        );
        ensure!(
            token(&self.resource_class, 64)
                && ["never", "idempotent"].contains(&self.retry_policy.as_str()),
            "Invalid resource class or retry policy"
        );
        ensure!(
            !matches!(
                self.domain,
                Domain::Function | Domain::Agent | Domain::Evaluator
            ) || self.retry_policy == "never",
            "User code cannot be automatically retried"
        );
        ensure!(
            self.pinned_inputs.len() <= 64 && self.expected_heads.len() <= 64,
            "Too many pinned references"
        );
        for input in &self.pinned_inputs {
            revision(&input.revision)?;
            ensure!(token(&input.snapshot, 180), "Invalid pinned snapshot");
        }
        for head in self.expected_heads.values() {
            revision(head)?;
        }
        let descriptor = values::descriptor(&self.input_schema, &json!({}), 0)?;
        ensure!(
            descriptor["type"] == "object",
            "Execution input must be a typed envelope"
        );
        Ok(())
    }
}

fn revision(value: &str) -> Result<()> {
    let number = value.parse::<i64>()?;
    ensure!(
        number >= 0 && number.to_string() == value,
        "Invalid revision"
    );
    Ok(())
}
pub(crate) fn token(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.:/".contains(&c))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TaskPlan {
    pub key: String,
    pub partition: Option<u32>,
}
pub(crate) fn plans(spec: &ExecutionSpec, tasks: &[TaskPlan]) -> Result<Value> {
    ensure!(
        !tasks.is_empty() && tasks.len() <= 256,
        "Invalid task count"
    );
    ensure!(
        !matches!(
            spec.domain,
            Domain::Function | Domain::Agent | Domain::Evaluator
        ) || tasks.len() == 1,
        "User code has one attempt task"
    );
    let mut seen = std::collections::HashSet::new();
    for task in tasks {
        ensure!(
            token(&task.key, 128)
                && seen.insert(&task.key)
                && task.partition.is_none_or(|p| p < 4096),
            "Invalid task plan"
        );
    }
    Ok(serde_json::to_value(tasks)?)
}

pub(crate) fn normalized(spec: &ExecutionSpec, input: &Value) -> Result<(Value, String)> {
    spec.validate()?;
    let descriptor = values::descriptor(&spec.input_schema, &json!({}), 0)?;
    let input = values::normalize(input, &descriptor)?;
    let bytes = serde_json::to_vec(&input)?;
    ensure!(bytes.len() <= 65536, "Execution input exceeds bounds");
    let mut hash = Sha256::new();
    hash.update(b"moria/runtime-input/v1\0");
    for bytes in [serde_json::to_vec(&descriptor)?, bytes] {
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    Ok((input, format!("{:x}", hash.finalize())))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Lease {
    pub incarnation: Uuid,
    pub execution: Uuid,
    pub task: Option<Uuid>,
    pub owner: Uuid,
    pub fence: String,
    pub lease_until: String,
}
impl Lease {
    fn fence(&self) -> Result<i64> {
        let fence: i64 = self.fence.parse()?;
        ensure!(
            fence > 0 && fence.to_string() == self.fence,
            "Invalid fence"
        );
        Ok(fence)
    }
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Claim {
    pub lease: Lease,
    pub spec: Value,
    pub input: Value,
    pub plan: Option<Value>,
}

pub struct ExecutionAuthority {
    pub db: Database,
}
impl ExecutionAuthority {
    async fn binding(&self, binding: Uuid) -> Result<(ExecutionSpec, Row)> {
        let row = self
            .db
            .get()
            .await?
            .query_one(
                "SELECT * FROM moria.runtime_execution_bindings WHERE id=$1",
                &[&binding],
            )
            .await?;
        let spec = serde_json::from_value(row.get("spec"))?;
        Ok((spec, row))
    }

    pub async fn request(
        &self,
        binding: Uuid,
        key: &str,
        input: Value,
        tasks: Vec<TaskPlan>,
    ) -> Result<Uuid> {
        ensure!(token(key, 180), "Invalid logical request key");
        let (mut spec, row) = self.binding(binding).await?;
        let context: Value = row.get("context");
        if let Some(old) = self.db.get().await?.query_opt("SELECT spec FROM moria.runtime_executions WHERE environment=$1 AND resource=$2 AND principal=$3 AND application=$4 AND request_key=$5", &[&row.get::<_,String>("environment"), &row.get::<_,Uuid>("resource"), &context["principal"].as_str(), &context["application"].as_str(), &key]).await? {
            spec = serde_json::from_value(old.get(0))?;
        }
        let (input, hash) = normalized(&spec, &input)?;
        let tasks = plans(&spec, &tasks)?;
        Ok(self
            .db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_execution_request($1,$2,$3,$4,$5)",
                &[&binding, &key, &input, &hash, &tasks],
            )
            .await?
            .get(0))
    }

    async fn read(&self, id: Uuid, after: Option<i64>) -> Result<Value> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let row = tx
            .query_one(
                "SELECT environment,binding FROM moria.runtime_executions WHERE id=$1",
                &[&id],
            )
            .await?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))",
            &[&format!("moria/environment/{}", row.get::<_, String>(0))],
        )
        .await?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended('moria/filesystem',0))",
            &[],
        )
        .await?;
        tx.query_one(
            "SELECT moria.runtime_binding_check($1)",
            &[&row.get::<_, Uuid>(1)],
        )
        .await?;
        let result = if let Some(after) = after {
            Value::Array(tx.query("SELECT jsonb_build_object('sequence',sequence::text,'kind',kind,'payload',payload) FROM moria.runtime_execution_events WHERE execution=$1 AND sequence>$2 ORDER BY sequence LIMIT 256", &[&id,&after]).await?.into_iter().map(|r| r.get(0)).collect())
        } else {
            tx.query_one("SELECT jsonb_build_object('id',id,'status',status,'fence',fence::text,'deadline',deadline::text,'result',result,'error',error,'eventSequence',event_sequence::text,'input',input,'spec',spec) FROM moria.runtime_executions WHERE id=$1", &[&id]).await?.get(0)
        };
        tx.query_one(
            "SELECT moria.runtime_binding_check($1)",
            &[&row.get::<_, Uuid>(1)],
        )
        .await?;
        tx.commit().await?;
        Ok(result)
    }
    pub async fn status(&self, id: Uuid) -> Result<Value> {
        self.read(id, None).await
    }

    async fn authorize_claim(tx: &Transaction<'_>, id: Uuid, sandbox: bool) -> Result<bool> {
        tx.batch_execute("SAVEPOINT execution_admission").await?;
        let checked = async {
            if sandbox {
                tx.query_one("SELECT moria.runtime_sandbox_claim_authority($1)", &[&id])
                    .await?;
            }
            tx.query_one("SELECT moria.runtime_execution_check($1)", &[&id])
                .await
        }
        .await;
        match checked {
            Ok(_) => {
                tx.batch_execute("RELEASE SAVEPOINT execution_admission")
                    .await?;
                Ok(true)
            }
            Err(error) => {
                let code = error.as_db_error().map(|e| e.message()).unwrap_or("");
                if ![
                    "BINDING_CHANGED",
                    "AUTHORIZATION_CHANGED",
                    "UNAUTHENTICATED",
                    "EXECUTION_INACTIVE",
                    "FORBIDDEN",
                    "IDENTITY_STALE",
                    "JOB_DELEGATION_INACTIVE",
                    "JOB_PARENT_INACTIVE",
                    "JOB_LEASE_LOST",
                ]
                .contains(&code)
                {
                    return Err(error.into());
                }
                tx.batch_execute("ROLLBACK TO SAVEPOINT execution_admission")
                    .await?;
                tx.execute("UPDATE moria.runtime_executions SET status='failed',error=$2,finished_at=clock_timestamp() WHERE id=$1 AND status IN ('queued','running','preparing')", &[&id,&code]).await?;
                tx.execute("UPDATE moria.runtime_execution_tasks SET status='cancelled',lease_until=NULL WHERE execution=$1 AND status IN ('pending','running')", &[&id]).await?;
                tx.execute("UPDATE moria.runtime_execution_attempts SET status='interrupted',finished_at=clock_timestamp() WHERE task IN (SELECT id FROM moria.runtime_execution_tasks WHERE execution=$1) AND status='running'", &[&id]).await?;
                tx.query_one("SELECT moria.runtime_execution_event($1,'failed',jsonb_build_object('error',$2::text))", &[&id,&code]).await?;
                if tx.query_opt("SELECT 1 FROM moria.runtime_executions e JOIN moria.runtime_sandbox_bindings b ON b.binding=e.binding WHERE e.id=$1 AND b.domain IN ('transform','sync') AND NOT EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.execution=e.id AND t.attempts<>0)",&[&id]).await?.is_some(){
                    tx.query_one("SELECT moria.runtime_execution_event($1,'noCodeStart',jsonb_build_object('error',$2::text))",&[&id,&code]).await?;
                }
                Ok(false)
            }
        }
    }

    pub async fn claim(
        &self,
        owner: Uuid,
        domains: &[Domain],
        publication: bool,
    ) -> Result<Option<Claim>> {
        ensure!(
            !domains.iter().any(|domain| matches!(
                domain,
                Domain::Function | Domain::Agent | Domain::Evaluator
            )),
            "FUNCTION_SANDBOX_REQUIRED"
        );
        self.claim_inner(owner, domains, publication, None, None, false)
            .await
    }

    pub(crate) async fn claim_function(
        &self,
        owner: Uuid,
        publication: bool,
        execution: Option<Uuid>,
        host: Uuid,
    ) -> Result<Option<Claim>> {
        self.db
            .get()
            .await?
            .query_one("SELECT moria.runtime_function_worker_check($1)", &[&owner])
            .await?;
        self.claim_inner(
            owner,
            &[Domain::Function],
            publication,
            execution,
            Some(host),
            false,
        )
        .await
    }

    pub(crate) async fn claim_agent(
        &self,
        owner: Uuid,
        publication: bool,
        execution: Option<Uuid>,
        host: Uuid,
    ) -> Result<Option<Claim>> {
        self.db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_function_worker_check($1,$2,NULL,'agent')",
                &[&owner, &host],
            )
            .await?;
        self.claim_inner(
            owner,
            &[Domain::Agent],
            publication,
            execution,
            Some(host),
            false,
        )
        .await
    }

    pub async fn claim_registered(
        &self,
        owner: Uuid,
        host: Uuid,
        domains: &[Domain],
        publication: bool,
        target: Option<Uuid>,
    ) -> Result<Option<Claim>> {
        ensure!(
            !domains.iter().any(|domain| matches!(
                domain,
                Domain::Function | Domain::Agent | Domain::Evaluator
            )),
            "FUNCTION_SANDBOX_REQUIRED"
        );
        self.claim_inner(owner, domains, publication, target, Some(host), false)
            .await
    }

    pub(crate) async fn claim_evaluator(
        &self,
        owner: Uuid,
        host: Uuid,
        publication: bool,
        target: Option<Uuid>,
    ) -> Result<Option<Claim>> {
        self.db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_worker_check($1,$2,NULL,'evaluator')",
                &[&owner, &host],
            )
            .await?;
        self.claim_inner(
            owner,
            &[Domain::Evaluator],
            publication,
            target,
            Some(host),
            false,
        )
        .await
    }

    pub(crate) async fn claim_engine(
        &self,
        owner: Uuid,
        host: Uuid,
        publication: bool,
        target: Option<Uuid>,
    ) -> Result<Option<Claim>> {
        crate::scratch::verify_cleanup_privilege()?;
        self.claim_inner(
            owner,
            &[Domain::Transform, Domain::Sync],
            publication,
            target,
            Some(host),
            true,
        )
        .await
    }

    async fn claim_inner(
        &self,
        owner: Uuid,
        domains: &[Domain],
        publication: bool,
        target: Option<Uuid>,
        host: Option<Uuid>,
        engine: bool,
    ) -> Result<Option<Claim>> {
        self.reap_cancellations(16).await?;
        let function = host.is_some();
        ensure!(
            !domains.is_empty() && domains.len() <= 8,
            "Worker capabilities required"
        );
        let mut domains: Vec<&str> = domains.iter().map(|d| d.name()).collect();
        if let Some(host) = host {
            let allowed:Vec<String>=self.db.get().await?.query_opt("SELECT w.domains FROM moria.runtime_workers w JOIN moria.indexing_instance i ON i.incarnation=w.incarnation WHERE w.owner=$1 AND w.host_id=$2 AND w.enabled AND w.expires_at>clock_timestamp()",&[&owner,&host]).await?.ok_or_else(||anyhow::anyhow!("FUNCTION_WORKER_NOT_AUTHORIZED"))?.get(0);
            domains.retain(|domain| allowed.iter().any(|value| value == domain));
            ensure!(!domains.is_empty(), "FUNCTION_WORKER_NOT_AUTHORIZED");
        }
        self.db
            .get()
            .await?
            .query_one("SELECT moria.runtime_execution_reap()", &[])
            .await?;
        let classes = if let Some(host) = host {
            self.db.get().await?.query("SELECT c.name FROM moria.runtime_resource_classes c JOIN moria.runtime_workers w ON c.name=ANY(w.resource_classes) JOIN moria.indexing_instance i ON i.incarnation=w.incarnation WHERE c.enabled AND w.owner=$1 AND w.host_id=$2 AND w.enabled AND w.expires_at>clock_timestamp() ORDER BY c.name LIMIT 64",&[&owner,&host]).await?
        } else {
            self.db.get().await?.query("SELECT name FROM moria.runtime_resource_classes WHERE enabled ORDER BY name LIMIT 64", &[]).await?
        };
        for class in classes {
            let name: String = class.get(0);
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            let Some(class) = tx.query_opt("SELECT * FROM moria.runtime_resource_classes WHERE name=$1 AND enabled FOR UPDATE SKIP LOCKED", &[&name]).await? else { continue };
            let active: i64 = tx
                .query_one("SELECT moria.runtime_resource_in_use($1)", &[&name])
                .await?
                .get(0);
            if active >= i64::from(class.get::<_, i32>("maximum_tasks")) {
                continue;
            }
            let query = if publication {
                "SELECT e.* FROM moria.runtime_executions e WHERE e.resource_class=$1 AND moria.runtime_environment_cell_available(e.environment) AND e.status='running' AND e.deadline>clock_timestamp() AND e.spec->>'domain'=ANY($2::text[]) AND ($4::uuid IS NULL OR e.id=$4) AND EXISTS(SELECT 1 FROM moria.runtime_sandbox_bindings s WHERE s.binding=e.binding AND s.domain IN ('transform','sync'))=$5 AND NOT EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.execution=e.id AND t.status<>'sealed') AND moria.runtime_resource_in_use($1,e.environment)<$3 ORDER BY e.created_at FOR UPDATE OF e SKIP LOCKED LIMIT 1"
            } else {
                "SELECT e.* FROM moria.runtime_executions e WHERE e.resource_class=$1 AND moria.runtime_environment_cell_available(e.environment) AND e.status IN ('queued','running') AND e.deadline>clock_timestamp() AND e.spec->>'domain'=ANY($2::text[]) AND ($4::uuid IS NULL OR e.id=$4) AND EXISTS(SELECT 1 FROM moria.runtime_sandbox_bindings s WHERE s.binding=e.binding AND s.domain IN ('transform','sync'))=$5 AND EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.execution=e.id AND t.status='pending') AND moria.runtime_resource_in_use($1,e.environment)<$3 ORDER BY e.created_at FOR UPDATE OF e SKIP LOCKED LIMIT 1"
            };
            let Some(execution) = tx
                .query_opt(
                    query,
                    &[
                        &name,
                        &domains,
                        &i64::from(class.get::<_, i32>("maximum_environment_tasks")),
                        &target,
                        &engine,
                    ],
                )
                .await?
            else {
                continue;
            };
            let id: Uuid = execution.get("id");
            if engine {
                tx.batch_execute("SAVEPOINT engine_admission").await?;
                if let Err(error) = tx
                    .query_one("SELECT moria.runtime_pipeline_current($1)", &[&id])
                    .await
                {
                    tx.batch_execute(
                        "ROLLBACK TO SAVEPOINT engine_admission;RELEASE SAVEPOINT engine_admission",
                    )
                    .await?;
                    if error.as_db_error().is_some_and(|error| {
                        error.code() == &tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE
                            || error.code()
                                == &tokio_postgres::error::SqlState::T_R_DEADLOCK_DETECTED
                    }) {
                        return Err(error.into());
                    }
                    let error: anyhow::Error = error.into();
                    let Some(code) = crate::pipelines::domain_error_code(&error)
                        .or_else(|| crate::database::authority_error_code(&error))
                    else {
                        return Err(error);
                    };
                    tx.execute("UPDATE moria.runtime_executions SET status='failed',error=$2,finished_at=clock_timestamp(),lease_until=NULL WHERE id=$1",&[&id,&code]).await?;
                    tx.execute("UPDATE moria.runtime_execution_tasks SET status='cancelled',lease_until=NULL WHERE execution=$1 AND status='pending'",&[&id]).await?;
                    tx.query_one("SELECT moria.runtime_execution_event($1,$2,jsonb_build_object('error',$3::text))",&[&id,&if publication{"failed"}else{"noCodeStart"},&code]).await?;
                    tx.commit().await?;
                    continue;
                }
                tx.batch_execute("RELEASE SAVEPOINT engine_admission")
                    .await?;
            }
            if execution.get::<_, Value>("spec")["domain"] == "evaluation"
                && !tx
                    .query_one("SELECT moria.runtime_evaluation_case_admission($1)", &[&id])
                    .await?
                    .get::<_, bool>(0)
            {
                tx.commit().await?;
                continue;
            }
            if function {
                tx.query_one(
                    "SELECT moria.runtime_function_worker_check($1,$2,$3,$4)",
                    &[
                        &owner,
                        &host,
                        &name,
                        &execution.get::<_, Value>("spec")["domain"]
                            .as_str()
                            .unwrap(),
                    ],
                )
                .await?;
            }
            if !Self::authorize_claim(
                &tx,
                id,
                function
                    && ["function", "agent"].contains(
                        &execution.get::<_, Value>("spec")["domain"]
                            .as_str()
                            .unwrap(),
                    ),
            )
            .await?
            {
                tx.commit().await?;
                continue;
            }
            let task = if publication {
                None
            } else {
                Some(tx.query_one("SELECT id FROM moria.runtime_execution_tasks WHERE execution=$1 AND status='pending' ORDER BY ordinal FOR UPDATE SKIP LOCKED LIMIT 1", &[&id]).await?.get::<_,Uuid>(0))
            };
            let worker_generation: Option<i64> = if host.is_some() {
                Some(
                    tx.query_one(
                        "SELECT generation FROM moria.runtime_workers WHERE owner=$1 FOR SHARE",
                        &[&owner],
                    )
                    .await?
                    .get(0),
                )
            } else {
                None
            };
            let row = if let Some(task) = task {
                tx.execute(
                    "UPDATE moria.runtime_executions SET status='running' WHERE id=$1",
                    &[&id],
                )
                .await?;
                let row = tx.query_one("UPDATE moria.runtime_execution_tasks SET status='running',owner=$2,fence=fence+1,attempts=attempts+1,worker_generation=$4,lease_until=LEAST((SELECT deadline FROM moria.runtime_executions WHERE id=$3),clock_timestamp()+interval '30 seconds') WHERE id=$1 RETURNING fence,lease_until::text,plan", &[&task,&owner,&id,&worker_generation]).await?;
                tx.execute("INSERT INTO moria.runtime_execution_attempts(task,fence,owner) VALUES($1,$2,$3)", &[&task,&row.get::<_,i64>(0),&owner]).await?;
                row
            } else {
                tx.query_one("UPDATE moria.runtime_executions SET status='preparing',owner=$2,fence=fence+1,worker_generation=$3,lease_until=LEAST(deadline,clock_timestamp()+interval '30 seconds') WHERE id=$1 RETURNING fence,lease_until::text,NULL::jsonb AS plan", &[&id,&owner,&worker_generation]).await?
            };
            let fence: i64 = row.get(0);
            if function
                && task.is_some()
                && ["function", "agent"].contains(
                    &execution.get::<_, Value>("spec")["domain"]
                        .as_str()
                        .unwrap(),
                )
            {
                tx.execute(
                    if execution.get::<_,Value>("spec")["domain"]=="agent" {"UPDATE moria.runtime_agent_epochs SET owner=$2,fence=$3 WHERE execution=$1"} else {"UPDATE moria.runtime_function_epochs SET owner=$2,fence=$3 WHERE execution=$1"},
                    &[&id, &owner, &fence],
                )
                .await?;
            }
            if task.is_some() && execution.get::<_, Value>("spec")["domain"] == "maintenance" {
                tx.execute("UPDATE moria.runtime_source_automation_epochs SET owner=$2,fence=$3 WHERE execution=$1",&[&id,&owner,&fence]).await?;
            }
            tx.query_one("SELECT moria.runtime_execution_event($1,$2,jsonb_build_object('task',$3::uuid,'owner',$4::uuid,'fence',$5::text))", &[&id,&if publication {"preparing"} else {"claimed"},&task,&owner,&fence.to_string()]).await?;
            let incarnation = tx
                .query_one("SELECT incarnation FROM moria.indexing_instance", &[])
                .await?
                .get(0);
            if engine && let Some(task) = task {
                let reservation: Option<Uuid> = tx
                    .query_one(
                        "SELECT moria.runtime_engine_scratch_reserve($1,$2,$3,$4,$5,$6,$7)",
                        &[&id, &task, &owner, &fence, &host, &incarnation, &name],
                    )
                    .await?
                    .get(0);
                if reservation.is_none() {
                    tx.rollback().await?;
                    continue;
                }
            }
            let claim = Claim {
                lease: Lease {
                    incarnation,
                    execution: id,
                    task,
                    owner,
                    fence: fence.to_string(),
                    lease_until: row.get(1),
                },
                spec: execution.get("spec"),
                input: execution.get("input"),
                plan: row.get(2),
            };
            tx.commit().await?;
            return Ok(Some(claim));
        }
        Ok(None)
    }

    async fn check(tx: &Transaction<'_>, lease: &Lease) -> Result<()> {
        ensure!(
            tx.query_opt(
                "SELECT 1 FROM moria.indexing_instance WHERE incarnation=$1",
                &[&lease.incarnation]
            )
            .await?
            .is_some(),
            "Incarnation changed"
        );
        if let Some(task) = lease.task {
            ensure!(
                tx.query_opt(
                    "SELECT 1 FROM moria.runtime_execution_tasks WHERE id=$1 AND execution=$2",
                    &[&task, &lease.execution]
                )
                .await?
                .is_some(),
                "Wrong execution lease"
            );
            tx.query_one(
                "SELECT moria.runtime_task_check($1,$2,$3)",
                &[&task, &lease.owner, &lease.fence()?],
            )
            .await?;
        } else {
            tx.query_one("SELECT 1 FROM moria.runtime_executions WHERE id=$1 AND owner=$2 AND fence=$3 AND status='preparing' AND lease_until>clock_timestamp() FOR UPDATE", &[&lease.execution,&lease.owner,&lease.fence()?]).await?;
            tx.query_one(
                "SELECT moria.runtime_execution_check($1)",
                &[&lease.execution],
            )
            .await?;
        }
        Ok(())
    }

    pub async fn renew(&self, lease: &Lease) -> Result<()> {
        for retry in 0..4 {
            let result = self.renew_once(lease).await;
            let locked = result.as_ref().is_err_and(|error| {
                error.chain().any(|cause| {
                    cause
                        .downcast_ref::<tokio_postgres::Error>()
                        .and_then(tokio_postgres::Error::as_db_error)
                        .is_some_and(|error| {
                            error.code() == &tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE
                        })
                })
            });
            if locked && retry < 3 {
                tokio::time::sleep(std::time::Duration::from_millis(10 << retry)).await;
            } else {
                return result;
            }
        }
        unreachable!()
    }

    async fn renew_once(&self, lease: &Lease) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        Self::check(&tx, lease).await?;
        if let Some(task) = lease.task {
            tx.execute("UPDATE moria.runtime_execution_tasks SET lease_until=LEAST((SELECT deadline FROM moria.runtime_executions WHERE id=$2),clock_timestamp()+interval '30 seconds') WHERE id=$1", &[&task,&lease.execution]).await?;
        } else {
            tx.execute("UPDATE moria.runtime_executions SET lease_until=LEAST(deadline,clock_timestamp()+interval '30 seconds') WHERE id=$1", &[&lease.execution]).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn seal(&self, lease: &Lease, result: Value, digest: &str) -> Result<()> {
        self.seal_inner(lease, result, digest, None).await
    }

    pub(crate) async fn seal_function(
        &self,
        lease: &Lease,
        result: Value,
        digest: &str,
        host: Uuid,
    ) -> Result<()> {
        self.seal_inner(lease, result, digest, Some(host)).await
    }

    async fn seal_inner(
        &self,
        lease: &Lease,
        result: Value,
        digest: &str,
        host: Option<Uuid>,
    ) -> Result<()> {
        let task = lease
            .task
            .ok_or_else(|| anyhow::anyhow!("Task lease required"))?;
        ensure!(
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                && serde_json::to_vec(&result)?.len() <= 65536,
            "Invalid sealed result"
        );
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let is_function = tx
            .query_one(
                "SELECT spec->>'domain' IN ('function','agent','evaluator') OR EXISTS(SELECT 1 FROM moria.runtime_sandbox_bindings s WHERE s.binding=e.binding AND s.domain IN ('transform','sync')) FROM moria.runtime_executions e WHERE id=$1",
                &[&lease.execution],
            )
            .await?
            .get::<_, bool>(0);
        ensure!(is_function == host.is_some(), "FUNCTION_SANDBOX_REQUIRED");
        if host.is_some() {
            tx.query_one(
                "SELECT moria.runtime_function_worker_check($1,$2,e.resource_class,e.spec->>'domain') FROM moria.runtime_executions e WHERE e.id=$3",
                &[&lease.owner,&host,&lease.execution],
            )
            .await?;
        }
        ensure!(
            tx.query_opt(
                "SELECT 1 FROM moria.indexing_instance WHERE incarnation=$1",
                &[&lease.incarnation]
            )
            .await?
            .is_some(),
            "Incarnation changed"
        );
        tx.query_one(
            "SELECT 1 FROM moria.runtime_executions WHERE id=$1 FOR UPDATE",
            &[&lease.execution],
        )
        .await?;
        tx.query_one(
            "SELECT moria.runtime_execution_check($1)",
            &[&lease.execution],
        )
        .await?;
        let old = tx.query_one("SELECT * FROM moria.runtime_execution_tasks WHERE id=$1 AND execution=$2 FOR UPDATE", &[&task,&lease.execution]).await?;
        if let Some(host) = host {
            ensure!(tx.query_opt("SELECT 1 FROM moria.runtime_sandbox_attempts WHERE execution=$1 AND task=$2 AND owner=$3 AND fence=$4 AND host_id=$5 AND cleaned AND state IN('running','sealed') AND observation_pending IS NULL AND NOT observation_blocked FOR SHARE",&[&lease.execution,&task,&lease.owner,&lease.fence()?,&host]).await?.is_some(),"FUNCTION_OBSERVATION_INCOMPLETE");
        }
        if old.get::<_, String>("status") == "sealed" {
            ensure!(
                old.get::<_, Option<Uuid>>("owner") == Some(lease.owner)
                    && old.get::<_, i64>("fence") == lease.fence()?
                    && old.get::<_, Option<Value>>("result") == Some(result)
                    && old.get::<_, Option<String>>("digest").as_deref() == Some(digest),
                "Sealed result conflict"
            );
        } else {
            Self::check(&tx, lease).await?;
            tx.execute("UPDATE moria.runtime_execution_tasks SET status='sealed',result=$2,digest=$3,lease_until=NULL WHERE id=$1", &[&task,&result,&digest]).await?;
            tx.execute("UPDATE moria.runtime_execution_attempts SET status='sealed',finished_at=clock_timestamp() WHERE task=$1 AND fence=$2", &[&task,&lease.fence()?]).await?;
            tx.query_one("SELECT moria.runtime_execution_event($1,'sealed',jsonb_build_object('task',$2::uuid))", &[&lease.execution,&task]).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn cancel(&self, id: Uuid) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let row = tx
            .query_one(
                "SELECT binding,status FROM moria.runtime_executions WHERE id=$1 FOR UPDATE",
                &[&id],
            )
            .await?;
        tx.query_one(
            "SELECT moria.runtime_binding_check($1)",
            &[&row.get::<_, Uuid>(0)],
        )
        .await?;
        if ["queued", "running", "preparing"].contains(&row.get::<_, String>(1).as_str()) {
            tx.execute("UPDATE moria.runtime_executions SET status='cancelled',finished_at=clock_timestamp(),lease_until=NULL WHERE id=$1", &[&id]).await?;
            tx.execute("UPDATE moria.runtime_execution_tasks SET status='cancelled',lease_until=NULL WHERE execution=$1 AND status IN ('pending','running')", &[&id]).await?;
            tx.execute("UPDATE moria.runtime_execution_attempts SET status='cancelled',finished_at=clock_timestamp() WHERE task IN (SELECT id FROM moria.runtime_execution_tasks WHERE execution=$1) AND status='running'", &[&id]).await?;
            tx.query_one(
                "SELECT moria.runtime_execution_event($1,'cancelled','{}'::jsonb)",
                &[&id],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn event(&self, lease: &Lease, payload: Value) -> Result<String> {
        ensure!(
            serde_json::to_vec(&payload)?.len() <= 8192,
            "Event payload exceeds bounds"
        );
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        Self::check(&tx, lease).await?;
        tx.query_one("UPDATE moria.runtime_executions SET custom_events=custom_events+1 WHERE id=$1 AND custom_events<256 RETURNING id", &[&lease.execution]).await?;
        let sequence: i64 = tx
            .query_one(
                "SELECT moria.runtime_execution_event($1,'progress',$2)",
                &[&lease.execution, &payload],
            )
            .await?
            .get(0);
        tx.commit().await?;
        Ok(sequence.to_string())
    }

    pub async fn events(&self, id: Uuid, after: &str) -> Result<Vec<Value>> {
        revision(after)?;
        Ok(serde_json::from_value(
            self.read(id, Some(after.parse()?)).await?,
        )?)
    }

    pub async fn schedule(
        &self,
        id: Uuid,
        binding: Uuid,
        interval: i32,
        enabled: bool,
        input: Value,
        tasks: Vec<TaskPlan>,
    ) -> Result<Uuid> {
        ensure!(
            (60..=604800).contains(&interval),
            "Invalid schedule interval"
        );
        let (spec, _) = self.binding(binding).await?;
        let (input, hash) = normalized(&spec, &input)?;
        let tasks = plans(&spec, &tasks)?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one("SELECT moria.runtime_binding_check($1)", &[&binding])
            .await?;
        let row=tx.query_opt("INSERT INTO moria.runtime_automations(id,binding,enabled,interval_seconds,next_run,input,input_hash,tasks) VALUES($1,$2,$3,$4,clock_timestamp(),$5,$6,$7) ON CONFLICT(id) DO UPDATE SET enabled=EXCLUDED.enabled,interval_seconds=EXCLUDED.interval_seconds WHERE moria.runtime_automations.binding=EXCLUDED.binding AND moria.runtime_automations.input=EXCLUDED.input AND moria.runtime_automations.input_hash=EXCLUDED.input_hash AND moria.runtime_automations.tasks=EXCLUDED.tasks RETURNING id", &[&id,&binding,&enabled,&interval,&input,&hash,&tasks]).await?;
        ensure!(row.is_some(), "Automation identity conflict");
        tx.commit().await?;
        Ok(id)
    }
    pub async fn tick(&self) -> Result<i32> {
        self.reap_cancellations(64).await?;
        let client = self.db.get().await?;
        client
            .query_one("SELECT moria.runtime_execution_reap()", &[])
            .await?;
        let scheduled: i32 = client
            .query_one("SELECT moria.runtime_schedule_tick()", &[])
            .await?
            .get(0);
        drop(client);
        Ok(scheduled
            + crate::automations::NativeAutomations::new(self.db.clone())
                .tick()
                .await? as i32)
    }
}

/// The domain must prove exact root command ownership and current Source/purpose
/// in this transaction. This records control intent without reading result data.
pub(crate) async fn cancellation_authority_in(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    row: &Row,
    operation: &str,
) -> Result<()> {
    let state = Authority::current(client, session).await?;
    let resource: Uuid = row.get("resource");
    Authority::resource_release_access(
        client,
        session,
        &resource.to_string(),
        operation,
        &row.get::<_, String>("release"),
    )
    .await?;
    let binding: Uuid = row.get("binding");
    if client
        .query_opt(
            "SELECT 1 FROM moria.runtime_execution_services WHERE binding=$1",
            &[&binding],
        )
        .await?
        .is_some()
    {
        client
            .query_one(
                "SELECT moria.runtime_service_binding_guard($1)",
                &[&binding],
            )
            .await?;
    }
    Authority::finish(client, session, &state).await
}

pub(crate) async fn cancel_authorized_in(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    id: Uuid,
) -> Result<()> {
    ensure!(
        client
            .query_opt(
                "SELECT 1 FROM moria.runtime_executions WHERE id=$1 AND environment=$2 FOR UPDATE",
                &[&id, &session.environment()]
            )
            .await?
            .is_some(),
        "NOT_FOUND"
    );
    cancel_node_in(client,id,id,&json!({"environment":session.environment(),"principal":session.principal(),"application":session.application()}),None).await
}

async fn cancel_node_in(
    client: &tokio_postgres::Client,
    id: Uuid,
    root: Uuid,
    requester: &Value,
    requested_at: Option<&str>,
) -> Result<()> {
    let inserted=client.execute("INSERT INTO moria.runtime_execution_cancellations(execution,root,requested_by,requested_at) VALUES($1,$2,$3,COALESCE($4::text::timestamptz,clock_timestamp())) ON CONFLICT DO NOTHING",&[&id,&root,&requester,&requested_at]).await?;
    if inserted > 0 {
        // Touch the authority row even after success so an old RR parent proof
        // cannot overlook a newly committed cancellation signal.
        client
            .query_one(
                "SELECT moria.runtime_execution_event($1,'cancellationRequested','{}')",
                &[&id],
            )
            .await?;
    }
    let changed=client.execute("UPDATE moria.runtime_executions SET status='cancelled',error='CANCELLED',finished_at=clock_timestamp(),lease_until=NULL WHERE id=$1 AND status IN('queued','running','preparing')",&[&id]).await?;
    if changed > 0 {
        client.execute("UPDATE moria.runtime_execution_tasks SET status='cancelled',lease_until=NULL WHERE execution=$1 AND status IN('pending','running')",&[&id]).await?;
        client.execute("UPDATE moria.runtime_execution_attempts SET status='cancelled',finished_at=clock_timestamp() WHERE task IN(SELECT id FROM moria.runtime_execution_tasks WHERE execution=$1) AND status='running'",&[&id]).await?;
        client
            .query_one(
                "SELECT moria.runtime_execution_event($1,'cancelled','{}')",
                &[&id],
            )
            .await?;
    }
    Ok(())
}

impl ExecutionAuthority {
    /// Structural bookkeeping is bounded; the root signal already fences every
    /// accepted descendant through the indexed current-parent authority proof.
    pub(crate) async fn reap_cancellations(&self, maximum: usize) -> Result<()> {
        ensure!(maximum <= 64, "EXECUTION_INVALID");
        for _ in 0..maximum {
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            let Some(signal)=tx.query_opt("SELECT * FROM moria.runtime_execution_cancellations WHERE expanded_at IS NULL ORDER BY requested_at,execution FOR UPDATE SKIP LOCKED LIMIT 1",&[]).await? else {return Ok(())};
            let parent: Uuid = signal.get("execution");
            let child=tx.query_opt("SELECT e.id,e.environment,e.principal,e.application,p.environment AS parent_environment,p.principal AS parent_principal,p.application AS parent_application FROM moria.runtime_job_delegations d JOIN moria.runtime_executions e ON e.id=d.execution JOIN moria.runtime_executions p ON p.id=d.parent_execution WHERE d.parent_execution=$1 AND NOT EXISTS(SELECT 1 FROM moria.runtime_execution_cancellations c WHERE c.execution=d.execution) ORDER BY e.id FOR UPDATE OF e SKIP LOCKED LIMIT 1",&[&parent]).await?;
            if let Some(child) = child {
                ensure!(
                    child.get::<_, String>("environment")
                        == child.get::<_, String>("parent_environment")
                        && child.get::<_, String>("principal")
                            == child.get::<_, String>("parent_principal")
                        && child.get::<_, String>("application")
                            == child.get::<_, String>("parent_application"),
                    "JOB_ACTOR_MISMATCH"
                );
                let at:String=tx.query_one("SELECT requested_at::text FROM moria.runtime_execution_cancellations WHERE execution=$1",&[&parent]).await?.get(0);
                cancel_node_in(
                    tx.client(),
                    child.get("id"),
                    signal.get("root"),
                    &signal.get::<_, Value>("requested_by"),
                    Some(&at),
                )
                .await?;
            }
            tx.execute("UPDATE moria.runtime_execution_cancellations s SET expanded_at=clock_timestamp() WHERE execution=$1 AND NOT EXISTS(SELECT 1 FROM moria.runtime_job_delegations d WHERE d.parent_execution=s.execution AND NOT EXISTS(SELECT 1 FROM moria.runtime_execution_cancellations c WHERE c.execution=d.execution))",&[&parent]).await?;
            tx.commit().await?;
        }
        Ok(())
    }
}

/// Operator/control composition only; no request-selected program or business-completion command.
pub async fn cli(db: Database, args: Vec<String>) -> Result<()> {
    let command = args
        .first()
        .map(String::as_str)
        .ok_or_else(|| anyhow::anyhow!("Execution command required"))?;
    let authority = ExecutionAuthority { db };
    if command == "scheduler" {
        ensure!(args.len() == 1, "Unexpected scheduler arguments");
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let shutdown = async {
            tokio::select! { result=tokio::signal::ctrl_c()=>result,_=terminate.recv()=>Ok(()) }
        };
        tokio::pin!(shutdown);
        loop {
            let scheduled =
                tokio::select! { _=&mut shutdown=>break,result=authority.tick()=>result? };
            if scheduled > 0 {
                tokio::io::stdout()
                    .write_all(
                        format!("{}\n", json!({"event":"scheduled","count":scheduled})).as_bytes(),
                    )
                    .await?;
            }
            tokio::select! { _=&mut shutdown=>break,_=tokio::time::sleep(std::time::Duration::from_secs(1))=>{} }
        }
        return Ok(());
    }
    let mut bytes = Vec::new();
    tokio::io::stdin()
        .take(131073)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 131072, "Execution command exceeds bounds");
    let value: Value = if bytes.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&bytes)?
    };
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct Request {
        binding: Uuid,
        request_key: String,
        input: Value,
        tasks: Vec<TaskPlan>,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Id {
        id: Uuid,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ClaimRequest {
        owner: Uuid,
        domains: Vec<Domain>,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Seal {
        lease: Lease,
        result: Value,
        digest: String,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Event {
        lease: Lease,
        payload: Value,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Events {
        id: Uuid,
        after: String,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Schedule {
        id: Uuid,
        binding: Uuid,
        interval: i32,
        enabled: bool,
        input: Value,
        tasks: Vec<TaskPlan>,
    }
    let result = match command {
        "request" => {
            let r: Request = serde_json::from_value(value)?;
            json!({"id":authority.request(r.binding,&r.request_key,r.input,r.tasks).await?})
        }
        "status" => {
            authority
                .status(serde_json::from_value::<Id>(value)?.id)
                .await?
        }
        "claim" | "prepare" => {
            let r: ClaimRequest = serde_json::from_value(value)?;
            serde_json::to_value(
                authority
                    .claim(r.owner, &r.domains, command == "prepare")
                    .await?,
            )?
        }
        "worker-claim" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct WorkerClaim {
                domains: Vec<Domain>,
                #[serde(default)]
                publication: bool,
                target: Option<Uuid>,
            }
            let request: WorkerClaim = serde_json::from_value(value)?;
            let owner = std::env::var("MORIA_EXECUTION_WORKER_OWNER")?.parse()?;
            let host = std::env::var("MORIA_EXECUTION_HOST_ID")?.parse()?;
            serde_json::to_value(
                authority
                    .claim_registered(
                        owner,
                        host,
                        &request.domains,
                        request.publication,
                        request.target,
                    )
                    .await?,
            )?
        }
        "renew" => {
            authority.renew(&serde_json::from_value(value)?).await?;
            json!({"renewed":true})
        }
        "seal" => {
            let r: Seal = serde_json::from_value(value)?;
            authority.seal(&r.lease, r.result, &r.digest).await?;
            json!({"sealed":true})
        }
        "cancel" => {
            authority
                .cancel(serde_json::from_value::<Id>(value)?.id)
                .await?;
            json!({"cancelled":true})
        }
        "event" => {
            let r: Event = serde_json::from_value(value)?;
            json!({"sequence":authority.event(&r.lease,r.payload).await?})
        }
        "events" => {
            let r: Events = serde_json::from_value(value)?;
            json!(authority.events(r.id, &r.after).await?)
        }
        "schedule" => {
            let r: Schedule = serde_json::from_value(value)?;
            json!({"id":authority.schedule(r.id,r.binding,r.interval,r.enabled,r.input,r.tasks).await?})
        }
        "tick" => {
            ensure!(value == json!({}), "Unexpected tick input");
            json!({"scheduled":authority.tick().await?})
        }
        _ => bail!("Unknown execution command"),
    };
    tokio::io::stdout()
        .write_all(format!("{result}\n").as_bytes())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_input_normalization_and_single_attempt_validation() {
        let mut spec = ExecutionSpec {
            version: 1,
            domain: Domain::Function,
            release: "a".repeat(64),
            resource_class: "standard".into(),
            timeout_seconds: 30,
            retry_policy: "never".into(),
            input_schema: json!({"type":"object","properties":{"amount":{"type":"decimal","precision":8,"scale":2,"nullable":false}}}),
            pinned_inputs: vec![],
            expected_heads: Default::default(),
        };
        assert_eq!(
            normalized(&spec, &json!({"amount":"1.2"})).unwrap(),
            normalized(&spec, &json!({"amount":"1.20"})).unwrap()
        );
        spec.retry_policy = "idempotent".into();
        assert!(spec.validate().is_err());
        spec.retry_policy = "never".into();
        spec.timeout_seconds = 31;
        assert!(spec.validate().is_err());
    }
}
