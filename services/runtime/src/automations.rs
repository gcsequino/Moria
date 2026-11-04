use crate::{
    authority::{Authority, VerifiedSession},
    canonical,
    database::Database,
    execution::{Domain, ExecutionAuthority, ExecutionSpec, Lease, TaskPlan},
    functions::Admission,
    jobs::{RegisteredService, ServiceBinding},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, env, time::Duration};
use tokio_postgres::Client;
use uuid::Uuid;

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Profile {
    service_authority: Uuid,
    service_binding_revision: String,
    target_service_binding_revision: String,
    resource_class: String,
    control_timeout_seconds: u32,
}
fn profile(value: &Value) -> Result<Profile> {
    let profile: Profile = serde_json::from_value(value.clone())
        .map_err(|_| anyhow::anyhow!("AUTOMATION_PROFILE_INVALID"))?;
    ensure!(
        profile.resource_class == "automation-control"
            && (10..=600).contains(&profile.control_timeout_seconds)
            && [
                &profile.service_binding_revision,
                &profile.target_service_binding_revision
            ]
            .iter()
            .all(|v| !v.is_empty() && v.len() <= 128),
        "AUTOMATION_PROFILE_INVALID"
    );
    Ok(profile)
}
fn domain(operation: &str) -> Result<(&'static str, &'static str)> {
    Ok(match operation {
        "build" => ("transform", "build"),
        "sync" => ("sync", "sync"),
        "invokeAgent" => ("agent", "invoke"),
        "evaluate" => ("evaluation", "run"),
        _ => anyhow::bail!("AUTOMATION_SOURCE_INVALID"),
    })
}
async fn source(client: &Client, environment: &str, resource: Uuid) -> Result<(String, Value)> {
    let row=client.query_opt("SELECT b.digest,b.reference,a.resources FROM moria.fs_resource_bindings b JOIN moria.fs_entries e ON e.id=b.entry_id AND e.resource_id=$1 AND e.environment_id=$2 AND NOT e.deleted JOIN moria.fs_roles r ON r.entry_id=b.root_id AND NOT r.retired JOIN moria.fs_active_releases a ON a.root_id=b.root_id AND a.environment=b.environment AND a.digest=b.digest WHERE b.active",&[&resource,&environment]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
    let reference: String = row.get(1);
    let resources: Value = row.get(2);
    let declaration = resources
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
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("AUTOMATION_SOURCE_INVALID"))?;
    Ok((row.get(0), declaration))
}
pub(crate) async fn install_in(
    client: &Client,
    environment: &str,
    resource: Uuid,
    revision: &str,
    configuration: &Value,
) -> Result<()> {
    let profile = profile(configuration)?;
    let (release, declaration) = source(client, environment, resource).await?;
    ensure!(
        declaration["kind"] == "Automation" && revision == canonical::fingerprint(configuration)?,
        "AUTOMATION_SOURCE_INVALID"
    );
    let spec = &declaration["spec"];
    ensure!(
        spec["overlap"] == "skip"
            && spec["missed"] == "coalesce"
            && spec["input"].is_object()
            && canonical::canonical_json(&spec["input"])?.len() <= 32768,
        "AUTOMATION_SOURCE_INVALID"
    );
    let (target_domain, _) = domain(
        spec["operation"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("AUTOMATION_SOURCE_INVALID"))?,
    )?;
    ensure!(
        target_domain == "agent"
            || spec["input"]
                .as_object()
                .is_some_and(|input| input.is_empty()),
        "AUTOMATION_INPUT_UNSUPPORTED"
    );
    let target = crate::filesystem::NativeSources::resource_in(
        client,
        &release,
        spec["target"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("AUTOMATION_SOURCE_INVALID"))?,
    )
    .await?
    .parse::<Uuid>()?;
    let application = crate::filesystem::NativeSources::resource_in(
        client,
        &release,
        spec["application"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("AUTOMATION_SOURCE_INVALID"))?,
    )
    .await?
    .parse::<Uuid>()?;
    let service = RegisteredService::load_current_in(
        client,
        environment,
        resource,
        "maintenance",
        &profile.service_binding_revision,
    )
    .await?;
    ensure!(
        service.id() == profile.service_authority
            && service.application() == application.to_string()
            && service.resource_class() == profile.resource_class,
        "AUTOMATION_PROFILE_INVALID"
    );
    service
        .resource_labels_in(client, &resource.to_string(), "run")
        .await?;
    let selected = RegisteredService::load_current_in(
        client,
        environment,
        target,
        target_domain,
        &profile.target_service_binding_revision,
    )
    .await?;
    ensure!(
        selected.id() == service.id()
            && selected.application() == service.application()
            && selected.principal() == service.principal(),
        "AUTOMATION_PROFILE_INVALID"
    );
    let interval = spec["intervalSeconds"]
        .as_i64()
        .filter(|v| (1..=31536000).contains(v))
        .ok_or_else(|| anyhow::anyhow!("AUTOMATION_SOURCE_INVALID"))? as i32;
    let id:Uuid=client.query_one("INSERT INTO moria.runtime_source_automation_profiles(environment,resource,revision,authority,service_revision,target_service_revision,resource_class,timeout_seconds) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(environment,resource,revision) DO UPDATE SET enabled=true RETURNING id",&[&environment,&resource,&revision,&service.id(),&profile.service_binding_revision,&profile.target_service_binding_revision,&profile.resource_class,&(profile.control_timeout_seconds as i32)]).await?.get(0);
    client.execute("UPDATE moria.runtime_source_automation_profiles SET enabled=false WHERE environment=$1 AND resource=$2 AND id<>$3 AND enabled",&[&environment,&resource,&id]).await?;
    client.execute("UPDATE moria.runtime_source_automation_schedules SET enabled=false WHERE environment=$1 AND resource=$2 AND enabled AND (release<>$3 OR profile<>$4)",&[&environment,&resource,&release,&id]).await?;
    client.execute("INSERT INTO moria.runtime_source_automation_schedules(profile,environment,resource,release,definition,target,target_domain,application,input,interval_seconds) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(profile,release) DO UPDATE SET enabled=true",&[&id,&environment,&resource,&release,&declaration,&target,&target_domain,&application,&spec["input"],&interval]).await?;
    client.execute("INSERT INTO moria.runtime_source_automation_status(environment,resource) VALUES($1,$2) ON CONFLICT DO NOTHING",&[&environment,&resource]).await?;
    Ok(())
}

/// Only an actual leased automation control execution can select this private composition path.
pub(crate) async fn dispatch_binding_in(
    client: &Client,
    session: &VerifiedSession,
    target: Uuid,
    spec: &Value,
    operations: &[String],
    markings: &[String],
) -> Result<Option<(ServiceBinding, Vec<String>)>> {
    let control=client.query_opt("SELECT p.authority,p.target_service_revision FROM moria.runtime_job_sessions j JOIN moria.runtime_source_automation_controls c ON c.execution=j.execution JOIN moria.runtime_source_automation_firings f ON f.id=c.firing JOIN moria.runtime_source_automation_schedules s ON s.id=f.schedule JOIN moria.runtime_source_automation_profiles p ON p.id=s.profile WHERE j.token_hash=$1",&[&session.session_hash()]).await?;
    let Some(control) = control else {
        return Ok(None);
    };
    let authority: Uuid = control.get(0);
    let revision: String = control.get(1);
    let domain = spec["domain"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("AUTOMATION_PROFILE_INVALID"))?;
    let service = RegisteredService::load_current_in(
        client,
        session.environment(),
        target,
        domain,
        &revision,
    )
    .await?;
    ensure!(service.id() == authority, "AUTOMATION_DISPATCH_REVOKED");
    let metadata: Value = client
        .query_one(
            "SELECT moria.runtime_automation_dispatch_metadata($1,$2,$3,$4,$5,$6)",
            &[
                &session.session_hash(),
                &target,
                &domain,
                &authority,
                &revision,
                &service.resource_class(),
            ],
        )
        .await?
        .get(0);
    ensure!(!metadata.is_null(), "AUTOMATION_DISPATCH_REVOKED");
    let current = Authority::current(client, session).await?;
    let mut operations: BTreeSet<String> = operations.iter().cloned().collect();
    let automation:Uuid=client.query_one("SELECT s.resource FROM moria.runtime_source_automation_schedules s JOIN moria.runtime_source_automation_firings f ON f.schedule=s.id WHERE f.id=$1",&[&metadata["firing"].as_str().unwrap().parse::<Uuid>()?]).await?.get(0);
    operations.insert(format!("{automation}:run"));
    let mut markings: BTreeSet<String> = markings.iter().cloned().collect();
    for marking in metadata["markings"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("AUTOMATION_PROFILE_INVALID"))?
    {
        markings.insert(
            marking
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("AUTOMATION_PROFILE_INVALID"))?
                .into(),
        );
    }
    ensure!(
        operations.is_subset(&current.identity.granted_operations)
            && operations.is_subset(&current.identity.application_scopes)
            && markings.is_subset(&current.identity.markings),
        "FORBIDDEN"
    );
    let markings: Vec<String> = markings.into_iter().collect();
    Ok(Some((
        service
            .bind_dispatch_in(
                client,
                spec,
                &operations.into_iter().collect::<Vec<_>>(),
                &markings,
                session,
                &metadata,
            )
            .await?,
        markings,
    )))
}

pub(crate) async fn retained_in(
    client: &Client,
    session: &VerifiedSession,
    execution: Uuid,
) -> Result<Vec<crate::sandbox::ConsumedMarking>> {
    let rows=client.query("WITH RECURSIVE ancestors AS(SELECT e.id,e.binding,e.context,0 AS depth FROM moria.runtime_executions e WHERE e.id=$1 AND e.environment=$2 AND e.principal=$3 AND e.application=$4 UNION ALL SELECT e.id,e.binding,e.context,a.depth+1 FROM ancestors a JOIN moria.runtime_job_delegations d ON d.execution=a.id JOIN moria.runtime_executions e ON e.id=d.parent_execution WHERE a.depth<8) SELECT DISTINCT e.binding,s.resource,s.release,f.markings FROM ancestors e JOIN moria.runtime_source_automation_firings f ON f.id=(e.context->>'automationFiring')::uuid JOIN moria.runtime_source_automation_schedules s ON s.id=f.schedule",&[&execution,&session.environment(),&session.principal(),&session.application()]).await?;
    ensure!(rows.len() <= 8, "AUTOMATION_DISPATCH_REVOKED");
    let mut labels = BTreeSet::new();
    for row in rows {
        client
            .query_one(
                "SELECT moria.runtime_automation_binding_guard($1)",
                &[&row.get::<_, Uuid>("binding")],
            )
            .await?;
        labels.extend(row.get::<_, Vec<String>>("markings"));
        labels.extend(
            Authority::resource_release_access(
                client,
                session,
                &row.get::<_, Uuid>("resource").to_string(),
                "run",
                &row.get::<_, String>("release"),
            )
            .await?,
        );
    }
    Ok(labels
        .into_iter()
        .map(|marking| crate::sandbox::ConsumedMarking {
            environment: session.environment().into(),
            marking,
        })
        .collect())
}

pub struct NativeAutomations {
    db: Database,
}
impl NativeAutomations {
    pub fn new(db: Database) -> Self {
        Self { db }
    }
    pub async fn firings(
        &self,
        session: &VerifiedSession,
        resource: Uuid,
        after: Option<&str>,
    ) -> Result<Value> {
        let position: Option<(String, Uuid)> = after
            .map(|after| {
                ensure!(after.len() <= 256, "AUTOMATION_CURSOR_INVALID");
                let position: (String, Uuid) = serde_json::from_str(after)
                    .map_err(|_| anyhow::anyhow!("AUTOMATION_CURSOR_INVALID"))?;
                chrono::DateTime::parse_from_rfc3339(&position.0)
                    .map_err(|_| anyhow::anyhow!("AUTOMATION_CURSOR_INVALID"))?;
                Ok::<_, anyhow::Error>(position)
            })
            .transpose()?;
        let mut admission = Admission::begin(&self.db, session.environment(), false, None).await?;
        let tx = admission.read_transaction().await?;
        let state = Authority::current(tx.client(), session).await?;
        let (_, declaration) = source(tx.client(), session.environment(), resource).await?;
        ensure!(declaration["kind"] == "Automation", "NOT_FOUND");
        let current: BTreeSet<String> = Authority::resource_access_for_operation(
            tx.client(),
            session,
            &resource.to_string(),
            "run",
        )
        .await?
        .into_iter()
        .collect();
        let bound = if position.is_some() {
            "AND (f.scheduled_at,f.id)<($3::text::timestamptz,$4::uuid)"
        } else {
            ""
        };
        let query = format!(
            "SELECT f.id,to_char(f.scheduled_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS scheduled,f.outcome,f.control,f.markings,s.release FROM moria.runtime_source_automation_firings f JOIN moria.runtime_source_automation_schedules s ON s.id=f.schedule WHERE f.environment=$1 AND f.resource=$2 {bound} ORDER BY f.scheduled_at DESC,f.id DESC LIMIT 51"
        );
        let rows = if let Some((instant, id)) = &position {
            tx.query(&query, &[&session.environment(), &resource, &instant, &id])
                .await?
        } else {
            tx.query(&query, &[&session.environment(), &resource])
                .await?
        };
        let mut more = rows.len() > 50;
        let mut releases = std::collections::BTreeMap::new();
        let mut firings = Vec::new();
        let mut next = None;
        let mut response_bytes = 1024;
        for row in rows.into_iter().take(50) {
            let release: String = row.get("release");
            if !releases.contains_key(&release) {
                releases.insert(
                    release.clone(),
                    Authority::resource_release_access(
                        tx.client(),
                        session,
                        &resource.to_string(),
                        "run",
                        &release,
                    )
                    .await?,
                );
            }
            let mut marks = current.clone();
            marks.extend(row.get::<_, Vec<String>>("markings"));
            marks.extend(releases.get(&release).unwrap().iter().cloned());
            ensure!(
                marks.len() <= 256 && marks.is_subset(&state.identity.markings),
                "FORBIDDEN"
            );
            let id: Uuid = row.get("id");
            let scheduled: String = row.get("scheduled");
            let cursor = serde_json::to_string(&(scheduled.clone(), id))?;
            let firing = json!({"id":id,"scheduledAt":scheduled,"outcome":row.get::<_,String>("outcome"),"controlExecution":row.get::<_,Option<Uuid>>("control"),"childView":"withheld","markings":marks.into_iter().map(|marking|json!({"environment":session.environment(),"marking":marking})).collect::<Vec<_>>()});
            let bytes = serde_json::to_vec(&firing)?.len() + 1;
            if response_bytes + bytes > 262144 {
                ensure!(!firings.is_empty(), "RESOURCE_LIMIT");
                more = true;
                break;
            }
            response_bytes += bytes;
            next = Some(cursor);
            firings.push(firing);
        }
        let result = json!({"firings":firings,"nextAfter":if more{next}else{None}});
        ensure!(
            serde_json::to_vec(&result)?.len() <= 262144,
            "RESOURCE_LIMIT"
        );
        Authority::finish(tx.client(), session, &state).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(result)
    }
    pub async fn tick(&self) -> Result<usize> {
        // Hints avoid replicas selecting the same page; authority locks remain after environment admission.
        let discovery = Admission {
            client: Some(self.db.get().await?),
        };
        let due=discovery.client.as_ref().unwrap().query("WITH current_clock AS MATERIALIZED(SELECT clock_timestamp() AS value),due AS MATERIALIZED(SELECT s.id,s.environment,s.next_run FROM moria.runtime_source_automation_schedules s JOIN moria.runtime_source_automation_profiles p ON p.id=s.profile AND p.enabled WHERE s.enabled AND s.next_run<=(SELECT value FROM current_clock) AND moria.runtime_environment_cell_available(s.environment) ORDER BY s.next_run,s.id LIMIT 256) SELECT id,environment FROM due WHERE pg_try_advisory_lock(hashtextextended('moria/automation/discovery/'||id::text,0)) LIMIT 16",&[]).await?;
        let mut count = 0;
        for item in due {
            let environment: String = item.get(1);
            let mut admission = Admission::begin(&self.db, &environment, false, None).await?;
            let tx = admission.client.as_mut().unwrap().transaction().await?;
            let schedule=tx.query_opt("SELECT s.*,s.next_run::text AS scheduled,p.authority,p.service_revision,p.resource_class,p.timeout_seconds,p.generation AS profile_generation FROM moria.runtime_source_automation_schedules s JOIN moria.runtime_source_automation_profiles p ON p.id=s.profile AND p.enabled WHERE s.id=$1 AND s.enabled AND s.next_run<=clock_timestamp() FOR UPDATE OF s,p SKIP LOCKED",&[&item.get::<_,Uuid>(0)]).await?;
            let Some(schedule) = schedule else { continue };
            tx.query_one(
                "SELECT moria.runtime_environment_cell_check($1)",
                &[&environment],
            )
            .await?;
            let schedule_id: Uuid = schedule.get("id");
            let resource: Uuid = schedule.get("resource");
            let release: String = schedule.get("release");
            let labels: Vec<String> = tx
                .query_one(
                    "SELECT markings FROM moria.runtime_service_authorities WHERE id=$1",
                    &[&schedule.get::<_, Uuid>("authority")],
                )
                .await?
                .get(0);
            tx.batch_execute("SAVEPOINT factory_dispatch").await?;
            let outcome=async {
                let (current,definition)=source(tx.client(),&environment,resource).await?;
                ensure!(current==release && definition==schedule.get::<_,Value>("definition"),"AUTOMATION_SOURCE_CHANGED");
                let service=RegisteredService::load_current_in(tx.client(),&environment,resource,"maintenance",&schedule.get::<_,String>("service_revision")).await?;
                ensure!(service.id()==schedule.get::<_,Uuid>("authority") && service.resource_class()==schedule.get::<_,String>("resource_class") && service.application()==schedule.get::<_,Uuid>("application").to_string(),"AUTOMATION_PROFILE_INVALID");
                let mut consumed:BTreeSet<String>=service.resource_labels_in(tx.client(),&resource.to_string(),"run").await?.into_iter().collect();
                let (_,operation)=domain(definition["spec"]["operation"].as_str().unwrap())?;
                ensure!(schedule.get::<_,String>("target_domain")=="agent" || schedule.get::<_,Value>("input").as_object().is_some_and(|input|input.is_empty()),"AUTOMATION_INPUT_UNSUPPORTED");
                let target:Uuid=schedule.get("target");
                consumed.extend(service.resource_labels_in(tx.client(),&target.to_string(),operation).await?);
                let active:Option<Uuid>=tx.query_one("SELECT active_firing FROM moria.runtime_source_automation_status WHERE environment=$1 AND resource=$2 FOR UPDATE",&[&environment,&resource]).await?.get(0);
                let overlap:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_source_automation_firings f LEFT JOIN moria.runtime_executions child ON child.id=f.child LEFT JOIN moria.runtime_executions control ON control.id=f.control WHERE f.id=$1 AND (child.status IN('queued','running','preparing') OR control.status IN('queued','running','preparing')))",&[&active]).await?.get(0);
                let firing:Uuid=tx.query_one("INSERT INTO moria.runtime_source_automation_firings(schedule,scheduled_at,outcome,input,markings,profile_generation) VALUES($1,$2::text::timestamptz,$3,$4,$5,$6) RETURNING id",&[&schedule_id,&schedule.get::<_,String>("scheduled"),&if overlap{"overlap"}else{"enqueued"},&schedule.get::<_,Value>("input"),&consumed.iter().cloned().collect::<Vec<_>>(),&schedule.get::<_,i64>("profile_generation")]).await?.get(0);
                if !overlap {
                    tx.execute("UPDATE moria.runtime_source_automation_status SET active_firing=$3 WHERE environment=$1 AND resource=$2",&[&environment,&resource,&firing]).await?;
                    let spec=ExecutionSpec{version:1,domain:Domain::Maintenance,release:release.strip_prefix("sha256:").ok_or_else(||anyhow::anyhow!("AUTOMATION_SOURCE_INVALID"))?.into(),resource_class:service.resource_class().into(),timeout_seconds:schedule.get::<_,i32>("timeout_seconds") as u32,retry_policy:"idempotent".into(),input_schema:json!({"type":"object","properties":{}}),pinned_inputs:vec![],expected_heads:Default::default()};
                    let operations=vec![format!("{resource}:run"),format!("{target}:{operation}")];
                    let binding=service.bind_background_in(tx.client(),&serde_json::to_value(&spec)?,&operations,&consumed.into_iter().collect::<Vec<_>>()).await?;
                    let (input,hash)=crate::execution::normalized(&spec,&json!({}))?;
                    let plans=crate::execution::plans(&spec,&[TaskPlan{key:"dispatch".into(),partition:None}])?;
                    let control:Uuid=tx.query_one("SELECT moria.runtime_execution_request($1,$2,$3,$4,$5)",&[&binding.id,&format!("automation/{firing}"),&input,&hash,&plans]).await?.get(0);
                    tx.execute("INSERT INTO moria.runtime_source_automation_controls(execution,firing) VALUES($1,$2)",&[&control,&firing]).await?;
                    tx.execute("UPDATE moria.runtime_source_automation_firings SET control=$2 WHERE id=$1",&[&firing,&control]).await?;
                }
                Ok::<_,anyhow::Error>(())
            }.await;
            if let Err(error) = outcome {
                if crate::execution::transaction_conflict(&error) {
                    return Err(error);
                }
                let code = crate::database::safe_error(&error);
                if code == "INDEXING_FAILED" && domain_error_code(&error).is_none() {
                    return Err(error);
                }
                tx.batch_execute(
                    "ROLLBACK TO SAVEPOINT factory_dispatch; RELEASE SAVEPOINT factory_dispatch",
                )
                .await?;
                tx.execute("INSERT INTO moria.runtime_source_automation_firings(schedule,scheduled_at,outcome,input,markings,profile_generation) VALUES($1,$2::text::timestamptz,'denied',$3,$4,$5)",&[&schedule_id,&schedule.get::<_,String>("scheduled"),&schedule.get::<_,Value>("input"),&labels,&schedule.get::<_,i64>("profile_generation")]).await?;
            } else {
                tx.batch_execute("RELEASE SAVEPOINT factory_dispatch")
                    .await?;
            }
            tx.execute("UPDATE moria.runtime_source_automation_schedules SET next_run=clock_timestamp()+interval_seconds*interval '1 second' WHERE id=$1",&[&schedule_id]).await?;
            tx.commit().await?;
            admission.release().await?;
            count += 1;
        }
        discovery.release().await?;
        Ok(count)
    }
    async fn child(&self, lease: &Lease) -> Result<Option<Uuid>> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT moria.runtime_task_check($1,$2,$3)",
            &[&lease.task, &lease.owner, &lease.fence.parse::<i64>()?],
        )
        .await?;
        let child=tx.query_one("SELECT f.child FROM moria.runtime_source_automation_firings f JOIN moria.runtime_source_automation_controls c ON c.firing=f.id WHERE c.execution=$1",&[&lease.execution]).await?.get(0);
        tx.commit().await?;
        Ok(child)
    }
    async fn dispatch(&self, lease: &Lease, host: Uuid) -> Result<Uuid> {
        if let Some(child) = self.child(lease).await? {
            return Ok(child);
        }
        let session = crate::execution::job_session(&self.db, lease, host).await?;
        let client = self.db.get().await?;
        let row=client.query_one("SELECT f.id,f.input,s.target,s.target_domain FROM moria.runtime_source_automation_controls c JOIN moria.runtime_source_automation_firings f ON f.id=c.firing JOIN moria.runtime_source_automation_schedules s ON s.id=f.schedule WHERE c.execution=$1",&[&lease.execution]).await?;
        let target: Uuid = row.get("target");
        let key = format!("automation/{}", row.get::<_, Uuid>("id"));
        drop(client);
        match row.get::<_, String>("target_domain").as_str() {
            "transform" | "sync" => {
                crate::pipelines::NativePipelines::new(self.db.clone())
                    .invoke(&session, target, &key)
                    .await?;
            }
            "agent" => {
                crate::agents::NativeAgents::new(self.db.clone())?
                    .invoke(
                        &session,
                        target,
                        &key,
                        json!({"input":row.get::<_,Value>("input"),"wait":false}),
                    )
                    .await?;
            }
            "evaluation" => {
                crate::evaluations::NativeEvaluations::new(self.db.clone())?
                    .invoke(&session, target, &key, json!({"wait":false}))
                    .await?;
            }
            _ => anyhow::bail!("AUTOMATION_SOURCE_INVALID"),
        }
        self.child(lease)
            .await?
            .ok_or_else(|| anyhow::anyhow!("AUTOMATION_CHILD_RECEIPT_REQUIRED"))
    }
    async fn publish(&self, lease: &Lease) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT moria.runtime_execution_check($1)",
            &[&lease.execution],
        )
        .await?;
        let row=tx.query_one("SELECT f.id,f.child FROM moria.runtime_source_automation_controls c JOIN moria.runtime_source_automation_firings f ON f.id=c.firing WHERE c.execution=$1 AND f.child IS NOT NULL",&[&lease.execution]).await?;
        let receipt = json!({"firing":row.get::<_,Uuid>(0),"child":row.get::<_,Uuid>(1)});
        let result:Value=tx.query_one("SELECT result FROM moria.runtime_execution_tasks WHERE execution=$1 AND status='sealed'",&[&lease.execution]).await?.get(0);
        ensure!(result == receipt, "AUTOMATION_CHILD_RECEIPT_REQUIRED");
        tx.query_one(
            "SELECT moria.runtime_execution_complete($1,$2,$3,$4)",
            &[
                &lease.execution,
                &lease.owner,
                &lease.fence.parse::<i64>()?,
                &receipt,
            ],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn worker_once(&self, owner: Uuid, host: Uuid) -> Result<bool> {
        let authority = ExecutionAuthority {
            db: self.db.clone(),
        };
        self.db
            .get()
            .await?
            .query_one("SELECT moria.runtime_execution_reap()", &[])
            .await?;
        let candidates=self.db.get().await?.query("SELECT e.id,e.environment FROM moria.runtime_executions e JOIN moria.runtime_source_automation_controls c ON c.execution=e.id WHERE e.status IN('queued','running') AND e.deadline>clock_timestamp() AND moria.runtime_environment_cell_available(e.environment) AND EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.execution=e.id AND t.status IN('pending','sealed')) ORDER BY e.created_at LIMIT 16",&[]).await?;
        for row in candidates {
            let id: Uuid = row.get(0);
            let environment: String = row.get(1);
            let mut admission = Admission::begin(&self.db, &environment, false, None).await?;
            let tx = admission.client.as_mut().unwrap().transaction().await?;
            let checked = tx
                .query_one(
                    "SELECT moria.runtime_automation_claim_authority($1,$2,$3)",
                    &[&id, &owner, &host],
                )
                .await;
            let checked = match checked {
                Ok(_) => tx.commit().await.map_err(anyhow::Error::from),
                Err(error) => {
                    tx.rollback().await?;
                    Err(error.into())
                }
            };
            admission.release().await?;
            if let Err(error) = checked {
                let code = crate::serving::error_code(&error);
                if ["RESOURCE_BUSY", "EXECUTION_INACTIVE"].contains(&code) {
                    continue;
                }
                if [
                    "AUTOMATION_DISPATCH_REVOKED",
                    "AUTHORIZATION_CHANGED",
                    "SERVICE_PURPOSE_REVOKED",
                    "FORBIDDEN",
                    "SOURCE_FILE_PROOF_REQUIRED",
                    "NOT_FOUND",
                ]
                .contains(&code)
                {
                    let mut client = self.db.get().await?;
                    let tx = client.transaction().await?;
                    let class:String=tx.query_one("SELECT resource_class FROM moria.runtime_executions WHERE id=$1 FOR UPDATE",&[&id]).await?.get(0);
                    tx.query_one(
                        "SELECT moria.runtime_worker_check($1,$2,$3,'maintenance')",
                        &[&owner, &host, &class],
                    )
                    .await?;
                    let changed=tx.execute("UPDATE moria.runtime_executions e SET status='failed',error='AUTOMATION_DISPATCH_REVOKED',finished_at=clock_timestamp(),lease_until=NULL WHERE id=$1 AND status IN('queued','running') AND NOT EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.execution=e.id AND t.status='running')",&[&id]).await?;
                    if changed > 0 {
                        tx.execute("UPDATE moria.runtime_execution_tasks SET status='cancelled',lease_until=NULL WHERE execution=$1 AND status IN('pending','sealed')",&[&id]).await?;
                        tx.query_one(
                            "SELECT moria.runtime_execution_event($1,'failed',$2)",
                            &[&id, &json!({"error":code})],
                        )
                        .await?;
                    }
                    tx.commit().await?;
                    return Ok(true);
                }
                return Err(error);
            }
            if let Some(claim) = authority
                .claim_registered(owner, host, &[Domain::Maintenance], true, Some(id))
                .await?
            {
                self.publish(&claim.lease).await?;
                return Ok(true);
            }
            let Some(claim) = authority
                .claim_registered(owner, host, &[Domain::Maintenance], false, Some(id))
                .await?
            else {
                continue;
            };
            let lease = claim.lease;
            let remaining:i64=self.db.get().await?.query_one("SELECT greatest(0,floor(extract(epoch FROM(deadline-clock_timestamp()))*1000))::bigint FROM moria.runtime_executions WHERE id=$1",&[&id]).await?.get(0);
            let result = {
                let dispatch = self.dispatch(&lease, host);
                let renew = async {
                    let mut heartbeat = tokio::time::interval(Duration::from_secs(5));
                    loop {
                        heartbeat.tick().await;
                        authority.renew(&lease).await?;
                    }
                    #[allow(unreachable_code)]
                    Ok::<(), anyhow::Error>(())
                };
                tokio::pin!(dispatch, renew);
                tokio::select! {result=&mut dispatch=>result,result=&mut renew=>Err(result.unwrap_err()),_=tokio::time::sleep(Duration::from_millis(remaining as u64))=>Err(anyhow::anyhow!("DEADLINE_EXCEEDED"))}
            };
            let child = match result {
                Ok(child) => child,
                Err(error) => {
                    if let Some(child) = self.child(&lease).await? {
                        child
                    } else {
                        let mut client = self.db.get().await?;
                        let tx = client.transaction().await?;
                        tx.query_one(
                            "SELECT moria.runtime_task_check($1,$2,$3)",
                            &[&lease.task, &lease.owner, &lease.fence.parse::<i64>()?],
                        )
                        .await?;
                        tx.execute("UPDATE moria.runtime_execution_tasks SET status='cancelled',lease_until=NULL WHERE id=$1",&[&lease.task]).await?;
                        tx.execute("UPDATE moria.runtime_execution_attempts SET status='failed',finished_at=clock_timestamp() WHERE task=$1 AND fence=$2 AND owner=$3 AND status='running'",&[&lease.task,&lease.fence.parse::<i64>()?,&lease.owner]).await?;
                        tx.execute("UPDATE moria.runtime_executions SET status='failed',error='AUTOMATION_DISPATCH_FAILED',finished_at=clock_timestamp(),lease_until=NULL WHERE id=$1",&[&id]).await?;
                        let sql_state = error.chain().find_map(|cause| {
                            cause
                                .downcast_ref::<tokio_postgres::Error>()
                                .and_then(tokio_postgres::Error::as_db_error)
                                .map(|database| database.code().code().to_owned())
                        });
                        let sql_function = error
                            .chain()
                            .find_map(|cause| {
                                cause
                                    .downcast_ref::<tokio_postgres::Error>()
                                    .and_then(tokio_postgres::Error::as_db_error)
                                    .and_then(|database| database.where_())
                            })
                            .and_then(|context| {
                                context.lines().find_map(|line| {
                                    line.strip_prefix("PL/pgSQL function ")
                                        .and_then(|function| function.split_once('('))
                                        .map(|(name, _)| name)
                                })
                            })
                            .filter(|name| {
                                !name.is_empty()
                                    && name.len() <= 128
                                    && name.bytes().all(|byte| {
                                        byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'.'
                                    })
                            })
                            .map(str::to_owned);
                        tx.query_one(
                            "SELECT moria.runtime_execution_event($1,'failed',$2)",
                            &[&id, &json!({"error":crate::database::safe_error(&error),"phase":"factoryDispatch","sqlState":sql_state,"sqlFunction":sql_function})],
                        )
                        .await?;
                        tx.commit().await?;
                        return Ok(true);
                    }
                }
            };
            let client = self.db.get().await?;
            let firing:Uuid=client.query_one("SELECT firing FROM moria.runtime_source_automation_controls WHERE execution=$1",&[&id]).await?.get(0);
            drop(client);
            let receipt = json!({"firing":firing,"child":child});
            let digest = canonical::fingerprint(&receipt)?;
            authority
                .seal(
                    &lease,
                    receipt,
                    digest.strip_prefix("sha256:").unwrap_or(&digest),
                )
                .await?;
            if let Some(claim) = authority
                .claim_registered(owner, host, &[Domain::Maintenance], true, Some(id))
                .await?
            {
                self.publish(&claim.lease).await?;
            }
            return Ok(true);
        }
        Ok(false)
    }
}

pub(crate) fn domain_error_code(error: &anyhow::Error) -> Option<&'static str> {
    for cause in error.chain() {
        let message = cause
            .downcast_ref::<tokio_postgres::Error>()
            .and_then(|error| error.as_db_error())
            .map(|db| db.message().to_owned())
            .unwrap_or_else(|| cause.to_string());
        let code = match message.as_str() {
            "AUTOMATION_PROFILE_INVALID" => "AUTOMATION_PROFILE_INVALID",
            "AUTOMATION_CURSOR_INVALID" => "AUTOMATION_CURSOR_INVALID",
            "AUTOMATION_SOURCE_INVALID" => "AUTOMATION_SOURCE_INVALID",
            "AUTOMATION_INPUT_UNSUPPORTED" => "AUTOMATION_INPUT_UNSUPPORTED",
            "AUTOMATION_SOURCE_CHANGED" => "AUTOMATION_SOURCE_CHANGED",
            "AUTOMATION_DISPATCH_REVOKED" => "AUTOMATION_DISPATCH_REVOKED",
            "AUTOMATION_CHILD_RECEIPT_REQUIRED" => "AUTOMATION_CHILD_RECEIPT_REQUIRED",
            "AUTOMATION_ALREADY_DISPATCHED" => "AUTOMATION_ALREADY_DISPATCHED",
            _ => continue,
        };
        return Some(code);
    }
    None
}
pub async fn cli(db: Database, arguments: Vec<String>) -> Result<()> {
    ensure!(arguments.len() == 1, "AUTOMATION_PROFILE_INVALID");
    if arguments[0] == "install" {
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
        ensure!(bytes.len() <= 65536, "AUTOMATION_PROFILE_INVALID");
        let registration: Registration = serde_json::from_slice(&bytes)?;
        let mut admission = Admission::begin(&db, &registration.environment, true, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        install_in(
            tx.client(),
            &registration.environment,
            registration.resource,
            &canonical::fingerprint(&registration.configuration)?,
            &registration.configuration,
        )
        .await?;
        tx.commit().await?;
        admission.release().await?;
        println!("{}", json!({"configured":true}));
        return Ok(());
    }
    let service = NativeAutomations::new(db.clone());
    if arguments[0] == "tick" {
        println!("{}", json!({"scheduled":service.tick().await?}));
        return Ok(());
    }
    ensure!(
        ["once", "worker"].contains(&arguments[0].as_str()),
        "AUTOMATION_PROFILE_INVALID"
    );
    let owner: Uuid = env::var("MORIA_AUTOMATION_WORKER_OWNER")?.parse()?;
    let host: Uuid = env::var("MORIA_AUTOMATION_HOST_ID")?.parse()?;
    db.get()
        .await?
        .query_one(
            "SELECT moria.runtime_worker_check($1,$2,NULL,'maintenance')",
            &[&owner, &host],
        )
        .await?;
    if arguments[0] == "once" {
        println!(
            "{}",
            json!({"worked":service.worker_once(owner,host).await?})
        );
        return Ok(());
    }
    println!("{}", json!({"event":"automation-worker"}));
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        db.get()
            .await?
            .query_one("SELECT moria.runtime_execution_reap()", &[])
            .await?;
        let worked = service.worker_once(owner, host).await?;
        if !worked {
            tokio::select! {_=tokio::time::sleep(Duration::from_millis(500))=>{},_=terminate.recv()=>return Ok(()),_=tokio::signal::ctrl_c()=>return Ok(())}
        }
    }
}
