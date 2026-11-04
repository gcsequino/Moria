use super::{Definition, Generation, NativeSearch};
use crate::{
    authority::{Authority, VerifiedSession},
    canonical::fingerprint,
    execution::{Domain, ExecutionAuthority, Lease, job_session},
    full_object_source::{
        FullObjectPageBudget, FullObjectSourceGrant, FullObjectSourcePin,
        resolve_full_object_source_pin_in,
    },
    jobs::RegisteredService,
    serving::Admission,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

pub(super) struct SearchWorker {
    pub search: NativeSearch,
    pub owner: Uuid,
    pub host: Uuid,
    pub schedule_after: Option<(String, String)>,
}
struct Heartbeat(tokio::task::JoinHandle<()>);
impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl SearchWorker {
    async fn context(
        &self,
        lease: &Lease,
        session: &VerifiedSession,
        at_revision: Option<&str>,
    ) -> Result<(Generation, FullObjectSourcePin)> {
        let client = self.search.db.get().await?;
        let row=client.query_one("SELECT g.*,x.model_secret_approval,r.projector_binding,r.source_stage,r.source_revision::text AS source_pin_revision,r.markings,c.id AS collection_id,p.service_binding_revision,p.service_id FROM moria.native_search_execution_sources x JOIN moria.native_search_rebuilds r ON r.generation=x.generation JOIN moria.native_search_generations g ON g.id=r.generation JOIN moria.native_search_collections c ON c.environment=g.environment AND c.resource=g.resource AND c.revision=g.collection_revision AND c.definition=g.definition AND c.enabled JOIN moria.native_search_projector_bindings p ON p.id=r.projector_binding AND p.id=c.projector_binding AND p.enabled WHERE x.execution=$1",&[&lease.execution]).await?;
        let environment: String = row.get("environment");
        let mut admission =
            crate::functions::Admission::begin(&self.search.db, &environment, false, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        let authority = Authority::current(tx.client(), session).await?;
        let service = RegisteredService::load_in(
            &self.search.db,
            tx.client(),
            &environment,
            row.get("collection_id"),
            "search",
            &row.get::<_, String>("service_binding_revision"),
        )
        .await?;
        ensure!(
            service.id() == row.get::<_, Uuid>("service_id"),
            "SERVICE_PURPOSE_REVOKED"
        );
        let base = row.get::<_, i64>("base_revision").to_string();
        let pin = resolve_full_object_source_pin_in(
            tx.client(),
            &service,
            row.get("projector_binding"),
            at_revision.unwrap_or(&base),
        )
        .await?;
        ensure!(
            pin.source_stage == row.get::<_, Uuid>("source_stage")
                && pin.source_revision == row.get::<_, String>("source_pin_revision")
                && pin.object_binding_revision == row.get::<_, String>("object_binding_revision"),
            "SEARCH_SOURCE_CHANGED"
        );
        let profile = self
            .search
            .models
            .registered_embedding_profile_in(tx.client(), &service, &pin.model_resource)
            .await?;
        ensure!(
            profile["modelBindingFingerprint"] == row.get::<_, String>("model_fingerprint")
                && profile["modelRevision"] == row.get::<_, String>("model_revision"),
            "SEARCH_MODEL_CHANGED"
        );
        let captured: Option<Value> = row.get("model_secret_approval");
        ensure!(
            profile
                .get("secretApproval")
                .filter(|value| !value.is_null())
                .cloned()
                == captured,
            "SECRET_APPROVAL_REVOKED"
        );
        let definition: Definition = serde_json::from_value(row.get("definition"))?;
        ensure!(
            definition.source == pin.object_resource
                && definition.text_property == pin.text_property
                && definition.embedding_model == pin.model_resource,
            "SEARCH_PROJECTOR_UNAVAILABLE"
        );
        let generation = Generation {
            id: row.get("id"),
            definition,
            collection_revision: row.get("collection_revision"),
            object_binding_revision: row.get("object_binding_revision"),
            model_fingerprint: row.get("model_fingerprint"),
            model_revision: row.get("model_revision"),
            dimensions: row.get::<_, i32>("dimensions") as usize,
            incarnation: row.get("incarnation"),
            covered_revision: row.get::<_, i64>("covered_revision").to_string(),
            markings: row.get("markings"),
        };
        ensure!(
            pin.markings
                .iter()
                .all(|label| generation.markings.contains(label)),
            "AUTHORIZATION_CHANGED"
        );
        Authority::finish(tx.client(), session, &authority).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok((generation, pin))
    }
    async fn task(&self, lease: &Lease, session: &VerifiedSession) -> Result<()> {
        let task = lease.task.ok_or_else(|| anyhow::anyhow!("LEASE_LOST"))?;
        let plan: Value = self
            .search
            .db
            .get()
            .await?
            .query_one(
                "SELECT plan FROM moria.runtime_execution_tasks WHERE id=$1",
                &[&task],
            )
            .await?
            .get(0);
        if plan["key"] == "catchup" {
            return self.catchup(lease, session).await;
        }
        let (generation, pin) = self
            .context(lease, session, None)
            .await
            .context("SEARCH_CONTEXT")?;
        let grant = Arc::new(
            FullObjectSourceGrant::acquire(
                self.search.db.clone(),
                session.clone(),
                lease.clone(),
                pin,
            )
            .await
            .context("SEARCH_FULL_GRANT")?,
        );
        loop {
            grant.check().await?;
            let progress=self.search.db.get().await?.query_one("SELECT lower_uid,upper_uid,after_uid,rows_processed,digest,complete FROM moria.native_search_task_progress WHERE task=$1 AND generation=$2",&[&task,&generation.id]).await?;
            if progress.get::<_, bool>(5) {
                let count = progress.get::<_, i64>(3);
                let digest: Vec<u8> = progress.get(4);
                let digest = digest
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                ExecutionAuthority {
                    db: self.search.db.clone(),
                }
                .seal(
                    lease,
                    json!({"generation":generation.id,"rows":count.to_string()}),
                    &digest,
                )
                .await?;
                return Ok(());
            }
            let after: Option<Uuid> = progress.get(2);
            let rows = grant
                .page(
                    progress.get(0),
                    progress.get(1),
                    after,
                    FullObjectPageBudget {
                        rows: 64,
                        bytes: 8 * 1024 * 1024,
                        time_ms: 30000,
                    },
                )
                .await
                .context("SEARCH_SOURCE_PAGE")?;
            let prepared = self
                .search
                .prepare_documents(
                    &grant,
                    session,
                    &generation,
                    generation.covered_revision.parse()?,
                    &rows,
                )
                .await
                .context("SEARCH_EMBED_BATCH")?;
            let mut digest: Vec<u8> = progress.get(4);
            for row in &rows {
                let mut hash = Sha256::new();
                hash.update(&digest);
                hash.update(row.uid.as_bytes());
                hash.update(
                    fingerprint(&json!({"text":row.text,"deleted":row.deleted}))?.as_bytes(),
                );
                digest = hash.finalize().to_vec();
            }
            grant.check().await?;
            let admission = Admission::begin(&self.search.db, session, false).await?;
            let result=async {
    let client=admission.client();let authority=Authority::current(client,session).await?;
    client.query_one("SELECT moria.runtime_task_check($1,$2,$3)",&[&task,&lease.owner,&lease.fence.parse::<i64>()?]).await?;
    let old=client.query_one("SELECT after_uid,digest FROM moria.native_search_task_progress WHERE task=$1 FOR UPDATE",&[&task]).await?;
    ensure!(old.get::<_,Option<Uuid>>(0)==after&&old.get::<_,Vec<u8>>(1)==progress.get::<_,Vec<u8>>(4),"LEASE_LOST");
    let ids:Vec<Uuid>=prepared.iter().map(|(uid,_)|*uid).collect();let documents:Vec<Option<Uuid>>=prepared.iter().map(|(_,document)|*document).collect();
    client.execute("INSERT INTO moria.native_search_heads(generation,uid,document) SELECT $1,* FROM unnest($2::uuid[],$3::uuid[]) rows(uid,document) WHERE document IS NOT NULL ON CONFLICT(generation,uid) DO UPDATE SET document=EXCLUDED.document",&[&generation.id,&ids,&documents]).await?;
    client.execute("UPDATE moria.native_search_task_progress SET after_uid=$2,rows_processed=rows_processed+$3,digest=$4,complete=$5 WHERE task=$1",&[&task,&rows.last().map(|row|row.uid).or(after),&(rows.len() as i64),&digest,&rows.is_empty()]).await?;
    Authority::finish(client,session,&authority).await?;Ok(())
   }.await;
            admission.complete(result).await?;
        }
    }
    async fn catchup(&self, lease: &Lease, session: &VerifiedSession) -> Result<()> {
        let task = lease.task.ok_or_else(|| anyhow::anyhow!("LEASE_LOST"))?;
        let (generation, initial_pin) = self
            .context(lease, session, None)
            .await
            .context("SEARCH_CONTEXT")?;
        let target:i64=self.search.db.get().await?.query_one("SELECT CASE WHEN x.kind='replay' THEN x.target_revision ELSE env.revision END FROM moria.native_search_execution_sources x JOIN moria.runtime_executions e ON e.id=x.execution JOIN moria.environments env ON env.id=e.environment WHERE e.id=$1",&[&lease.execution]).await?.get(0);
        loop {
            let client = self.search.db.get().await?;
            let covered: i64 = client
                .query_one(
                    "SELECT covered_revision FROM moria.native_search_generations WHERE id=$1",
                    &[&generation.id],
                )
                .await?
                .get(0);
            let frame=client.query_opt("SELECT id,commit_revision,item_count FROM moria.runtime_change_transactions WHERE environment=$1 AND incarnation=$2 AND complete AND commit_revision>$3 AND commit_revision<=$4 ORDER BY commit_revision LIMIT 1",&[&session.environment(),&lease.incarnation,&covered,&target]).await?;
            let Some(frame) = frame else {
                let admission = Admission::begin(&self.search.db, session, true).await?;
                let result=async {
     let client=admission.client();let authority=Authority::current(client,session).await?;
     client.query_one("SELECT moria.runtime_task_check($1,$2,$3)",&[&task,&lease.owner,&lease.fence.parse::<i64>()?]).await?;
     client.execute("UPDATE moria.native_search_generations SET covered_revision=$2 WHERE id=$1 AND covered_revision<=$2",&[&generation.id,&target]).await?;
     client.execute("UPDATE moria.native_search_replay_progress SET complete=true WHERE generation=$1",&[&generation.id]).await?;
     client.execute("UPDATE moria.runtime_change_pins SET after_revision=COALESCE((SELECT max(commit_revision) FROM moria.runtime_change_transactions WHERE environment=$2 AND incarnation=$3 AND complete AND commit_revision<=$4),0) WHERE id=$1 AND owner=$5",&[&lease.execution,&session.environment(),&lease.incarnation,&target,&lease.execution]).await?;
     Authority::finish(client,session,&authority).await?;Ok(())
    }.await;
                admission.complete(result).await?;
                let digest = format!(
                    "{:x}",
                    Sha256::digest(format!("{}/{target}", generation.id))
                );
                return (ExecutionAuthority {
                    db: self.search.db.clone(),
                })
                .seal(
                    lease,
                    json!({"generation":generation.id,"coveredRevision":target.to_string()}),
                    &digest,
                )
                .await;
            };
            let frame_id: Uuid = frame.get(0);
            let revision: i64 = frame.get(1);
            let count: i64 = client
                .query_one(
                    "SELECT count(*) FROM moria.runtime_change_items WHERE transaction_id=$1",
                    &[&frame_id],
                )
                .await?
                .get(0);
            ensure!(
                count == i64::from(frame.get::<_, i32>(2)),
                "SEARCH_FRAME_INCOMPLETE"
            );
            ensure!(!client.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_change_items WHERE transaction_id=$1 AND object_type=$2 AND kind='rebuild')",&[&frame_id,&initial_pin.object_resource]).await?.get::<_,bool>(0),"SEARCH_SOURCE_CHANGED");
            drop(client);
            let (generation, pin) = self
                .context(lease, session, Some(&revision.to_string()))
                .await?;
            let grant = Arc::new(
                FullObjectSourceGrant::acquire(
                    self.search.db.clone(),
                    session.clone(),
                    lease.clone(),
                    pin,
                )
                .await?,
            );
            loop {
                grant.check().await?;
                let client = self.search.db.get().await?;
                let progress=client.query_one("SELECT frame,after_uid FROM moria.native_search_replay_progress WHERE generation=$1",&[&generation.id]).await?;
                ensure!(
                    progress
                        .get::<_, Option<Uuid>>(0)
                        .is_none_or(|old| old == frame_id),
                    "SEARCH_FRAME_INCOMPLETE"
                );
                let after: Option<Uuid> = progress.get(1);
                let ids:Vec<Uuid>=client.query("SELECT DISTINCT uid FROM moria.runtime_change_items WHERE transaction_id=$1 AND object_type=$2 AND kind='object' AND ($3::uuid IS NULL OR uid>$3) ORDER BY uid LIMIT 64",&[&frame_id,&initial_pin.object_resource,&after]).await?.iter().map(|row|row.get(0)).collect();
                drop(client);
                if ids.is_empty() {
                    break;
                }
                let mut groups = vec![ids.clone()];
                let mut prepared = Vec::new();
                while let Some(group) = groups.pop() {
                    match grant.read_uids(&group).await {
                        Ok(rows) => prepared.extend(
                            self.search
                                .prepare_documents(&grant, session, &generation, revision, &rows)
                                .await?,
                        ),
                        Err(error)
                            if crate::database::safe_error(&error) == "RESOURCE_LIMIT"
                                && group.len() > 1 =>
                        {
                            let half = group.len() / 2;
                            groups.push(group[half..].to_vec());
                            groups.push(group[..half].to_vec());
                        }
                        Err(error) => return Err(error),
                    }
                }
                grant.check().await?;
                let admission = Admission::begin(&self.search.db, session, false).await?;
                let result=async {
     let client=admission.client();let authority=Authority::current(client,session).await?;
     client.query_one("SELECT moria.runtime_task_check($1,$2,$3)",&[&task,&lease.owner,&lease.fence.parse::<i64>()?]).await?;
     let old=client.query_one("SELECT frame,after_uid FROM moria.native_search_replay_progress WHERE generation=$1 FOR UPDATE",&[&generation.id]).await?;
     ensure!(old.get::<_,Option<Uuid>>(0).is_none_or(|old|old==frame_id)&&old.get::<_,Option<Uuid>>(1)==after,"LEASE_LOST");
     let uids:Vec<Uuid>=prepared.iter().map(|(uid,_)|*uid).collect();let docs:Vec<Option<Uuid>>=prepared.iter().map(|(_,doc)|*doc).collect();
     client.execute("INSERT INTO moria.native_search_pending_heads(generation,frame,uid,document) SELECT $1,$2,* FROM unnest($3::uuid[],$4::uuid[]) ON CONFLICT(generation,frame,uid) DO UPDATE SET document=EXCLUDED.document",&[&generation.id,&frame_id,&uids,&docs]).await?;
     client.execute("UPDATE moria.native_search_replay_progress SET frame=$2,after_uid=$3 WHERE generation=$1",&[&generation.id,&frame_id,&ids.last()]).await?;
     Authority::finish(client,session,&authority).await?;Ok(())
    }.await;
                admission.complete(result).await?;
            }
            grant.check().await?;
            let admission = Admission::begin(&self.search.db, session, true).await?;
            let result=async {
    let client=admission.client();let authority=Authority::current(client,session).await?;
    client.query_one("SELECT moria.runtime_task_check($1,$2,$3)",&[&task,&lease.owner,&lease.fence.parse::<i64>()?]).await?;
    let exact:bool=client.query_one("SELECT NOT EXISTS(SELECT uid FROM moria.runtime_change_items WHERE transaction_id=$2 AND object_type=$3 AND kind='object' EXCEPT SELECT uid FROM moria.native_search_pending_heads WHERE generation=$1 AND frame=$2) AND NOT EXISTS(SELECT uid FROM moria.native_search_pending_heads WHERE generation=$1 AND frame=$2 EXCEPT SELECT uid FROM moria.runtime_change_items WHERE transaction_id=$2 AND object_type=$3 AND kind='object')",&[&generation.id,&frame_id,&initial_pin.object_resource]).await?.get(0);
    ensure!(exact,"SEARCH_FRAME_INCOMPLETE");
    client.execute("DELETE FROM moria.native_search_heads h USING moria.native_search_pending_heads p WHERE p.generation=$1 AND p.frame=$2 AND p.document IS NULL AND h.generation=p.generation AND h.uid=p.uid",&[&generation.id,&frame_id]).await?;
    client.execute("INSERT INTO moria.native_search_heads(generation,uid,document) SELECT generation,uid,document FROM moria.native_search_pending_heads WHERE generation=$1 AND frame=$2 AND document IS NOT NULL ON CONFLICT(generation,uid) DO UPDATE SET document=EXCLUDED.document",&[&generation.id,&frame_id]).await?;
    client.execute("UPDATE moria.native_search_generations SET covered_revision=$2 WHERE id=$1 AND covered_revision<$2",&[&generation.id,&revision]).await?;
    client.execute("UPDATE moria.runtime_change_pins SET after_revision=$2 WHERE id=$1 AND owner=$3",&[&lease.execution,&revision,&lease.execution]).await?;
    client.execute("UPDATE moria.runtime_change_pins SET after_revision=$2 WHERE id=$1 AND owner=$1",&[&generation.id,&revision]).await?;
    client.execute("DELETE FROM moria.native_search_pending_heads WHERE generation=$1 AND frame=$2",&[&generation.id,&frame_id]).await?;
    client.execute("UPDATE moria.native_search_replay_progress SET frame=NULL,after_uid=NULL WHERE generation=$1",&[&generation.id]).await?;
    Authority::finish(client,session,&authority).await?;Ok(())
   }.await;
            admission.complete(result).await?;
        }
    }
    async fn publish(&self, lease: &Lease, session: &VerifiedSession) -> Result<()> {
        let (generation, pin) = self
            .context(lease, session, None)
            .await
            .context("SEARCH_CONTEXT")?;
        let grant = FullObjectSourceGrant::acquire(
            self.search.db.clone(),
            session.clone(),
            lease.clone(),
            pin.clone(),
        )
        .await?;
        grant.check().await?;
        let client = self.search.db.get().await?;
        let verified: bool = client
            .query_one(
                "SELECT base_verified FROM moria.native_search_rebuilds WHERE generation=$1",
                &[&generation.id],
            )
            .await?
            .get(0);
        if !verified {
            let counts=client.query_one("SELECT (SELECT count(*) FROM moria.runtime_object_base_stage($1,$3,$4,$5::text::bigint)),(SELECT COALESCE(sum(p.rows_processed),0)::bigint FROM moria.native_search_task_progress p JOIN moria.runtime_execution_tasks t ON t.id=p.task WHERE p.generation=$2 AND t.plan->>'key' LIKE 'partition-%' AND p.complete AND t.status='sealed'),(SELECT count(*) FROM moria.native_search_task_progress p JOIN moria.runtime_execution_tasks t ON t.id=p.task WHERE p.generation=$2 AND t.plan->>'key' LIKE 'partition-%' AND p.complete AND t.status='sealed'),r.partitions FROM moria.native_search_rebuilds r WHERE r.generation=$2",&[&pin.source_stage,&generation.id,&session.environment(),&pin.object_resource,&pin.environment_revision]).await?;
            ensure!(
                counts.get::<_, i64>(0) == counts.get::<_, i64>(1)
                    && counts.get::<_, i64>(2) == i64::from(counts.get::<_, i32>(3)),
                "SEARCH_GENERATION_INCOMPLETE"
            );
        }
        drop(client);
        grant.check().await?;
        let admission = Admission::begin(&self.search.db, session, true).await?;
        let result=async {
   let client=admission.client();let authority=Authority::current(client,session).await?;
   let owned:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_executions WHERE id=$1 AND owner=$2 AND fence=$3 AND status='preparing' AND lease_until>clock_timestamp())",&[&lease.execution,&lease.owner,&lease.fence.parse::<i64>()?]).await?.get(0);ensure!(owned,"LEASE_LOST");
   client.query_one("SELECT moria.runtime_execution_check($1)",&[&lease.execution]).await?;
   client.execute("UPDATE moria.native_search_rebuilds SET base_verified=true WHERE generation=$1",&[&generation.id]).await?;
   let state=client.query_one("SELECT p.complete,g.covered_revision,e.revision,EXISTS(SELECT 1 FROM moria.runtime_change_transactions f WHERE f.environment=g.environment AND f.incarnation=g.incarnation AND f.complete AND f.commit_revision>g.covered_revision) FROM moria.native_search_replay_progress p JOIN moria.native_search_generations g ON g.id=p.generation JOIN moria.environments e ON e.id=g.environment WHERE g.id=$1",&[&generation.id]).await?;
   let kind:String=client.query_one("SELECT kind FROM moria.native_search_execution_sources WHERE execution=$1",&[&lease.execution]).await?.get(0);
   if !state.get::<_,bool>(0)||(kind=="rebuild"&&state.get::<_,bool>(3)){
    let ordinal:i32=client.query_one("SELECT COALESCE(max(ordinal),-1)+1 FROM moria.runtime_execution_tasks WHERE execution=$1",&[&lease.execution]).await?.get(0);
    let existing=client.query_opt("SELECT id FROM moria.runtime_execution_tasks WHERE execution=$1 AND plan->>'key'='catchup'",&[&lease.execution]).await?;
    let task=if let Some(row)=existing {let id:Uuid=row.get(0);client.execute("UPDATE moria.runtime_execution_tasks SET status='pending',owner=NULL,lease_until=NULL,result=NULL,digest=NULL WHERE id=$1 AND status='sealed'",&[&id]).await?;id}else{client.query_one("INSERT INTO moria.runtime_execution_tasks(execution,ordinal,plan) VALUES($1,$2,'{\"key\":\"catchup\",\"partition\":null}') RETURNING id",&[&lease.execution,&ordinal]).await?.get(0)};
    client.execute("INSERT INTO moria.native_search_task_progress(task,generation,lower_uid) VALUES($1,$2,$3) ON CONFLICT(task) DO NOTHING",&[&task,&generation.id,&Uuid::nil()]).await?;
    client.execute("UPDATE moria.native_search_replay_progress SET complete=false WHERE generation=$1",&[&generation.id]).await?;
    Authority::finish(client,session,&authority).await?;
    client.execute("UPDATE moria.runtime_executions SET status='running',owner=NULL,lease_until=NULL WHERE id=$1",&[&lease.execution]).await?;
    return Ok(());
   }
   let current:i64=if kind=="replay" {state.get(1)}else{state.get(2)};
   ensure!(client.query_one("SELECT stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1",&[&session.environment(),&pin.object_resource]).await?.get::<_,Option<Uuid>>(0)==Some(pin.source_stage),"SEARCH_SOURCE_CHANGED");
   let profile=self.search.models.capabilities_in(client,session,&generation.definition.embedding_model).await?;
   ensure!(profile["modelBindingFingerprint"]==generation.model_fingerprint,"SEARCH_MODEL_CHANGED");
   client.execute("UPDATE moria.native_search_generations SET status='retired' WHERE environment=$1 AND resource=$2 AND status='active' AND id<>$3",&[&session.environment(),&pin.collection,&generation.id]).await?;
   client.execute("UPDATE moria.native_search_generations SET status='active',covered_revision=$2,error=NULL WHERE id=$1 AND status IN ('building','active')",&[&generation.id,&current]).await?;
   let switched=client.execute("UPDATE moria.native_search_collections SET active_generation=$3 WHERE environment=$1 AND resource=$2 AND revision=$4 AND enabled AND projector_binding=$5",&[&session.environment(),&pin.collection,&generation.id,&generation.collection_revision,&pin.projector_binding]).await?;ensure!(switched==1,"SEARCH_PROJECTOR_UNAVAILABLE");
   client.execute("DELETE FROM moria.runtime_change_pins WHERE id=$1 AND owner=$1",&[&generation.id]).await?;
   client.execute("INSERT INTO moria.runtime_change_pins(id,environment,incarnation,owner,after_revision,expires_at) SELECT $1,$2,$3,$1,COALESCE((SELECT max(commit_revision) FROM moria.runtime_change_transactions WHERE environment=$2 AND incarnation=$3 AND complete AND commit_revision<=$4),0),'infinity'::timestamptz",&[&generation.id,&session.environment(),&lease.incarnation,&current]).await?;
   client.execute("DELETE FROM moria.runtime_change_pins p USING moria.native_search_generations g WHERE g.environment=$1 AND g.resource=$2 AND g.status='retired' AND p.id=g.id AND p.owner=g.id",&[&session.environment(),&pin.collection]).await?;
   client.execute("DELETE FROM moria.runtime_change_pins WHERE id=$1 AND owner=$1",&[&lease.execution]).await?;
   Authority::finish(client,session,&authority).await?;
   client.query_one("SELECT moria.runtime_execution_complete($1,$2,$3,$4)",&[&lease.execution,&lease.owner,&lease.fence.parse::<i64>()?,&json!({"generation":generation.id,"visibility":super::VisibilityToken{environment:session.environment().into(),collection:pin.collection.clone(),generation:generation.id,incarnation:generation.incarnation,revision:current.to_string()}})]).await?;
   Ok(())
  }.await;
        admission.complete(result).await
    }

    pub(super) async fn once(&mut self) -> Result<bool> {
        let scheduler = ExecutionAuthority {
            db: self.search.db.clone(),
        };
        let claim = if let Some(claim) = scheduler
            .claim_registered(self.owner, self.host, &[Domain::Search], false, None)
            .await?
        {
            Some(claim)
        } else {
            scheduler
                .claim_registered(self.owner, self.host, &[Domain::Search], true, None)
                .await?
        };
        let Some(claim) = claim else {
            return match self.search.schedule_next(&mut self.schedule_after).await {
                Ok(worked) => Ok(worked),
                Err(error)
                    if matches!(
                        super::error_code(&error),
                        "UNSUPPORTED_CAPABILITY"
                            | "SECRET_APPROVAL_REQUIRED"
                            | "SECRET_APPROVAL_REVOKED"
                            | "SECRET_SOURCE_CHANGED"
                            | "MODEL_BINDING_CHANGED"
                            | "MODEL_CONFIGURATION_UNAVAILABLE"
                            | "SERVICE_PURPOSE_REVOKED"
                            | "FULL_SOURCE_PURPOSE_REVOKED"
                            | "AUTHORIZATION_CHANGED"
                            | "FORBIDDEN"
                            | "SEARCH_SOURCE_CHANGED"
                            | "MARKING_DOWNGRADE"
                    ) =>
                {
                    eprintln!(
                        "{}",
                        json!({"event":"search_schedule_blocked","error":super::error_code(&error),"collection":self.schedule_after})
                    );
                    Ok(true)
                }
                Err(error) => Err(error),
            };
        };
        let lease = claim.lease;
        let session = job_session(&self.search.db, &lease, self.host).await?;
        let heartbeat_db = self.search.db.clone();
        let heartbeat_lease = lease.clone();
        let _heartbeat = Heartbeat(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                if (ExecutionAuthority {
                    db: heartbeat_db.clone(),
                })
                .renew(&heartbeat_lease)
                .await
                .is_err()
                {
                    break;
                }
            }
        }));
        let result = if lease.task.is_some() {
            self.task(&lease, &session).await
        } else {
            self.publish(&lease, &session).await
        };
        if let Err(error) = result {
            self.fail(&lease, &error).await?;
        }
        Ok(true)
    }
    async fn fail(&self, lease: &Lease, error: &anyhow::Error) -> Result<()> {
        let mut client = self.search.db.get().await?;
        let tx = client.transaction().await?;
        let code = crate::database::safe_error(error);
        let owned = if let Some(task) = lease.task {
            tx.execute("UPDATE moria.runtime_execution_tasks SET status='cancelled',lease_until=NULL WHERE id=$1 AND owner=$2 AND fence=$3 AND status='running' AND lease_until>clock_timestamp()",&[&task,&lease.owner,&lease.fence.parse::<i64>()?]).await?
        } else {
            tx.execute("UPDATE moria.runtime_executions SET status='failed',error=$4,finished_at=clock_timestamp(),lease_until=NULL WHERE id=$1 AND owner=$2 AND fence=$3 AND status='preparing' AND lease_until>clock_timestamp()",&[&lease.execution,&lease.owner,&lease.fence.parse::<i64>()?,&code]).await?
        };
        if owned == 1 {
            tx.execute("UPDATE moria.runtime_executions SET status='failed',error=$2,finished_at=clock_timestamp(),lease_until=NULL WHERE id=$1 AND status IN ('queued','running','preparing')",&[&lease.execution,&code]).await?;
            tx.execute("UPDATE moria.native_search_generations g SET status='failed',error=$2 FROM moria.native_search_execution_sources r WHERE r.execution=$1 AND g.id=r.generation AND g.status IN ('building','ready')",&[&lease.execution,&code]).await?;
            tx.execute("UPDATE moria.native_search_generations g SET error=$2 FROM moria.native_search_execution_sources x WHERE x.execution=$1 AND g.id=x.generation AND g.status='active'",&[&lease.execution,&code]).await?;
            tx.execute(
                "DELETE FROM moria.runtime_change_pins WHERE id=$1 AND owner=$1",
                &[&lease.execution],
            )
            .await?;
            tx.query_one("SELECT moria.runtime_execution_event($1,'failed',jsonb_build_object('error',$2::text))",&[&lease.execution,&code]).await?;
        }
        tx.commit().await?;
        eprintln!(
            "{}",
            json!({"event":"search_failed","execution":lease.execution,"error":code,"phase":error.chain().map(|cause|cause.to_string()).find(|value|["SEARCH_CONTEXT","SEARCH_FULL_GRANT","SEARCH_SOURCE_PAGE","SEARCH_EMBED_BATCH"].contains(&value.as_str())),"sqlState":error.chain().find_map(|cause|cause.downcast_ref::<tokio_postgres::Error>().and_then(|error|error.code()).map(|code|code.code()))})
        );
        Ok(())
    }
}
