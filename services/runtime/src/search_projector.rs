use super::{Definition, NativeSearch};
use crate::{
    authority::{Authority, VerifiedSession},
    authorization::authorize,
    canonical::fingerprint,
    execution::{Domain, ExecutionSpec, TaskPlan},
    full_object_source::resolve_full_object_source_pin_in,
    jobs::RegisteredService,
    retrieval::CHUNKER_REVISION,
    serving::Admission,
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use tokio_postgres::Client;
use uuid::Uuid;
impl NativeSearch {
    pub async fn inspect_rebuild(
        &self,
        session: &VerifiedSession,
        resource: &str,
        execution: Uuid,
    ) -> Result<Value> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let result=async{
            let client=admission.client();let authority=Authority::current(client,session).await?;
            let marks=Authority::resource_access(client,session,resource,"read").await?;
            authorize(&authority.identity,resource,"project",&marks)?;
            let row=client.query_opt("SELECT e.status,e.error,e.result,x.generation,r.markings FROM moria.native_search_execution_sources x JOIN moria.runtime_executions e ON e.id=x.execution JOIN moria.native_search_rebuilds r ON r.generation=x.generation WHERE e.id=$1 AND e.environment=$2 AND r.resource=$3",&[&execution,&session.environment(),&resource]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
            authorize(&authority.identity,resource,"project",&row.get::<_,Vec<String>>(4))?;
            Authority::finish(client,session,&authority).await?;
            Ok(json!({"execution":execution,"generation":row.get::<_,Uuid>(3),"state":{"status":row.get::<_,String>(0),"error":row.get::<_,Option<String>>(1),"result":row.get::<_,Option<Value>>(2)}}))
        }.await;
        admission.complete(result).await
    }
    pub async fn request_rebuild(
        &self,
        session: &VerifiedSession,
        resource: &str,
        key: &str,
    ) -> Result<Value> {
        ensure!(
            !key.is_empty() && key.len() <= 256 && key.bytes().all(|v| v.is_ascii_graphic()),
            "IDEMPOTENCY_KEY_INVALID"
        );
        let admission = Admission::begin(&self.db, session, true).await?;
        let result = self
            .accept_rebuild(
                admission.client(),
                Some(session),
                session.environment(),
                resource,
                key,
            )
            .await;
        admission.complete(result).await
    }
    pub(super) async fn accept_rebuild(
        &self,
        client: &Client,
        requester: Option<&VerifiedSession>,
        environment: &str,
        resource: &str,
        key: &str,
    ) -> Result<Value> {
        if let Some(session) = requester {
            let authority = Authority::current(client, session).await?;
            let marks = Authority::resource_access(client, session, resource, "write").await?;
            authorize(&authority.identity, resource, "project", &marks)?;
            if let Some(row)=client.query_opt("SELECT r.execution,r.generation,r.markings,e.status,e.result,e.error FROM moria.native_search_rebuilds r JOIN moria.runtime_executions e ON e.id=r.execution WHERE r.environment=$1 AND r.resource=$2 AND r.principal=$3 AND r.application=$4 AND r.request_key=$5",&[&environment,&resource,&session.principal(),&session.application(),&key]).await?{
    authorize(&authority.identity,resource,"project",&row.get::<_,Vec<String>>(2))?;
    Authority::finish(client,session,&authority).await?;
    return Ok(json!({"execution":row.get::<_,Uuid>(0),"generation":row.get::<_,Uuid>(1),"state":{"status":row.get::<_,String>(3),"result":row.get::<_,Option<Value>>(4),"error":row.get::<_,Option<String>>(5)}}));
   }
        }
        let collection=client.query_opt("SELECT c.id,c.revision,c.definition,c.resource_class,c.partitions,c.timeout_seconds,p.id AS projector,p.revision AS projector_revision,p.service_id,p.service_binding_revision FROM moria.native_search_collections c JOIN moria.native_search_projector_bindings p ON p.id=c.projector_binding AND p.environment=c.environment AND p.collection=c.resource AND p.collection_id=c.id AND p.enabled WHERE c.environment=$1 AND c.resource=$2 AND c.enabled FOR UPDATE OF c",&[&environment,&resource]).await?.ok_or_else(||anyhow::anyhow!("SEARCH_PROJECTOR_UNAVAILABLE"))?;
        let collection_id: Uuid = collection.get("id");
        let collection_revision: String = collection.get("revision");
        let definition_json: Value = collection.get("definition");
        let definition: Definition = serde_json::from_value(definition_json.clone())?;
        ensure!(
            definition.chunk_size == 1000
                && definition.chunk_overlap == 100
                && definition.metric == "cosine"
                && definition.search_mode == "exact",
            "UNSUPPORTED_CAPABILITY"
        );
        let service = RegisteredService::load_in(
            &self.db,
            client,
            environment,
            collection_id,
            "search",
            &collection.get::<_, String>("service_binding_revision"),
        )
        .await?;
        ensure!(
            service.id() == collection.get::<_, Uuid>("service_id")
                && service.resource_class() == collection.get::<_, String>("resource_class"),
            "SERVICE_PURPOSE_REVOKED"
        );
        let base: String = client
            .query_one(
                "SELECT revision::text FROM moria.environments WHERE id=$1 FOR SHARE",
                &[&environment],
            )
            .await?
            .get(0);
        let pin =
            resolve_full_object_source_pin_in(client, &service, collection.get("projector"), &base)
                .await?;
        ensure!(
            pin.object_resource == definition.source
                && pin.text_property == definition.text_property
                && pin.model_resource == definition.embedding_model,
            "SEARCH_PROJECTOR_UNAVAILABLE"
        );
        let profile = self
            .models
            .registered_embedding_profile_in(client, &service, &definition.embedding_model)
            .await?;
        ensure!(
            profile["destination"] == pin.model_destination
                && profile["capabilities"]
                    .as_array()
                    .is_some_and(|v| v.iter().any(|v| v == "embed")),
            "UNSUPPORTED_CAPABILITY"
        );
        ensure!(
            serde_json::from_value::<Vec<String>>(profile["markings"].clone())?
                .iter()
                .all(|label| pin.markings.contains(label)),
            "MARKING_DOWNGRADE"
        );
        let model_fingerprint = profile["modelBindingFingerprint"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
        let model_revision = profile["modelRevision"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
        let dimensions = profile["dimensions"]
            .as_i64()
            .filter(|v| (1..=4096).contains(v))
            .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))? as i32;
        let labels: Vec<String> = pin
            .markings
            .iter()
            .cloned()
            .chain(definition.markings.iter().cloned())
            .chain(serde_json::from_value::<Vec<String>>(
                profile["markings"].clone(),
            )?)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut requester_authority = None;
        if let Some(session) = requester {
            // Declared provenance may advance the epoch; authenticate again before acceptance.
            let authority = Authority::current(client, session).await?;
            let mut marks = Authority::resource_access(client, session, resource, "write").await?;
            marks.extend(labels.iter().cloned());
            authorize(&authority.identity, resource, "project", &marks)?;
            requester_authority = Some(authority);
        }
        let partitions = collection.get::<_, i32>("partitions") as u32;
        let generation = Uuid::new_v4();
        let incarnation: Uuid = client
            .query_one("SELECT incarnation FROM moria.indexing_instance", &[])
            .await?
            .get(0);
        let spec=ExecutionSpec{version:1,domain:Domain::Search,release:fingerprint(&json!({"runtime":env!("MORIA_INDEXING_SOURCE_SHA256"),"collection":collection_revision,"definition":definition_json,"projector":collection.get::<_,String>("projector_revision"),"model":model_fingerprint,"chunker":CHUNKER_REVISION}))?.trim_start_matches("sha256:").into(),resource_class:service.resource_class().into(),timeout_seconds:collection.get::<_,i32>("timeout_seconds") as u32,retry_policy:"idempotent".into(),input_schema:json!({"type":"object","properties":{}}),pinned_inputs:vec![pin.dataset.execution_input()],expected_heads:Default::default()};
        let mut operations = vec![
            format!("{resource}:project"),
            format!("{}:read", pin.object_resource),
            format!("{}:read", pin.dataset.resource),
            format!("{}:invoke", definition.embedding_model),
            "filesystem:read".into(),
        ];
        if let Some(operation) = profile["secretOperation"].as_str() {
            operations.push(operation.to_owned());
        }
        let model_secret_approval = profile
            .get("secretApproval")
            .filter(|value| !value.is_null())
            .cloned();
        let binding = if let Some(requester) = requester {
            service
                .bind_in(client, &json!(spec), &operations, &labels, requester)
                .await?
        } else {
            service
                .bind_background_in(client, &json!(spec), &operations, &labels)
                .await?
        };
        let plans: Vec<TaskPlan> = (0..partitions)
            .map(|partition| TaskPlan {
                key: format!("partition-{partition}"),
                partition: Some(partition),
            })
            .collect();
        ensure!(
            binding.security_epoch == pin.dataset.security_epoch
                && binding.instance_epoch == pin.dataset.instance_epoch,
            "AUTHORIZATION_CHANGED"
        );
        let (input, input_hash) = crate::execution::normalized(&spec, &json!({}))?;
        let tasks = crate::execution::plans(&spec, &plans)?;
        let execution: Uuid = client
            .query_one(
                "SELECT moria.runtime_execution_request($1,$2,$3,$4,$5)",
                &[
                    &binding.id,
                    &format!(
                        "search/{}",
                        fingerprint(&json!({"generation":generation,"key":key}))?
                            .trim_start_matches("sha256:")
                    ),
                    &input,
                    &input_hash,
                    &tasks,
                ],
            )
            .await?
            .get(0);
        client
            .query_one(
                "SELECT moria.runtime_job_delegation_accept($1)",
                &[&execution],
            )
            .await?;
        let base_revision = base.parse::<i64>()?;
        client.execute("INSERT INTO moria.native_search_generations(id,environment,resource,collection_revision,definition,object_binding_revision,model_fingerprint,model_revision,dimensions,chunker_revision,incarnation,base_revision,covered_revision,status) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$12,'building')",&[&generation,&environment,&resource,&collection_revision,&definition_json,&pin.object_binding_revision,&model_fingerprint,&model_revision,&dimensions,&CHUNKER_REVISION,&incarnation,&base_revision]).await?;
        client.execute("UPDATE moria.native_search_generations SET source_signal=(SELECT nonce FROM moria.native_search_binding_signals WHERE environment=$2 AND resource=$3),model_signal=(SELECT nonce FROM moria.native_search_binding_signals WHERE environment=$2 AND resource=$4) WHERE id=$1",&[&generation,&environment,&pin.object_resource,&pin.model_resource]).await?;
        let principal = requester.map_or(service.principal(), VerifiedSession::principal);
        let application = requester.map_or(service.application(), VerifiedSession::application);
        let schema:Value=client.query_one("SELECT object_schema FROM moria.source_generations WHERE environment=$1 AND object_type=$2 AND revision=$3::text::bigint AND stage=$4",&[&environment,&pin.object_resource,&pin.source_revision,&pin.source_stage]).await?.get(0);
        client.execute("INSERT INTO moria.native_search_rebuilds(generation,execution,environment,resource,principal,application,request_key,source_stage,source_revision,source_schema,partitions,markings,projector_binding) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::text::bigint,$10,$11,$12,$13)",&[&generation,&execution,&environment,&resource,&principal,&application,&key,&pin.source_stage,&pin.source_revision,&schema,&(partitions as i32),&labels,&pin.projector_binding]).await?;
        client.execute("INSERT INTO moria.native_search_execution_sources(execution,generation,kind,target_revision,model_secret_approval) VALUES($1,$2,'rebuild',$3,$4)",&[&execution,&generation,&base_revision,&model_secret_approval]).await?;
        for row in client.query("SELECT id,ordinal FROM moria.runtime_execution_tasks WHERE execution=$1 ORDER BY ordinal",&[&execution]).await?{
   let ordinal=row.get::<_,i32>(1) as u128;let lower=Uuid::from_u128((ordinal*256/partitions as u128)<<120);
   let upper=if ordinal+1==partitions as u128{None}else{Some(Uuid::from_u128(((ordinal+1)*256/partitions as u128)<<120))};
   client.execute("INSERT INTO moria.native_search_task_progress(task,generation,lower_uid,upper_uid) VALUES($1,$2,$3,$4)",&[&row.get::<_,Uuid>(0),&generation,&lower,&upper]).await?;
  }
        client
            .execute(
                "INSERT INTO moria.native_search_replay_progress(generation) VALUES($1)",
                &[&generation],
            )
            .await?;
        client.execute("INSERT INTO moria.runtime_change_pins(id,environment,incarnation,owner,after_revision,expires_at) SELECT $1,$2,$3,$4,COALESCE((SELECT max(commit_revision) FROM moria.runtime_change_transactions WHERE environment=$2 AND incarnation=$3 AND complete AND commit_revision<=$5),0),e.deadline FROM moria.runtime_executions e WHERE e.id=$4",&[&execution,&environment,&incarnation,&execution,&base_revision]).await?;
        if let (Some(session), Some(authority)) = (requester, requester_authority.as_ref()) {
            Authority::finish(client, session, authority).await?;
        }
        Ok(json!({"execution":execution,"generation":generation,"state":{"status":"queued"}}))
    }
}
