use super::{Definition, NativeSearch};
use crate::{
    canonical::fingerprint,
    execution::{ExecutionSpec, TaskPlan},
    full_object_source::resolve_full_object_source_pin_in,
    jobs::RegisteredService,
};
use anyhow::{Result, ensure};
use serde_json::json;
use uuid::Uuid;
impl NativeSearch {
    pub(super) async fn schedule_next(&self, after: &mut Option<(String, String)>) -> Result<bool> {
        let client = self.db.get().await?;
        let candidate=client.query_opt("SELECT c.environment,c.resource FROM moria.native_search_collections c JOIN moria.native_search_projector_bindings p ON p.id=c.projector_binding AND p.enabled JOIN moria.runtime_service_authorities s ON s.id=p.service_id AND s.enabled JOIN moria.environments env ON env.id=c.environment LEFT JOIN moria.native_search_generations g ON g.id=c.active_generation LEFT JOIN moria.native_search_rebuilds r ON r.generation=g.id LEFT JOIN moria.native_search_binding_signals source_signal ON source_signal.environment=c.environment AND source_signal.resource=p.source_resource LEFT JOIN moria.native_search_binding_signals model_signal ON model_signal.environment=c.environment AND model_signal.resource=p.model_resource WHERE c.enabled AND ($1::text IS NULL OR (c.environment,c.resource)>($1,$2)) AND moria.runtime_environment_cell_available(c.environment) AND (g.id IS NULL OR (g.error IS NULL AND (g.covered_revision<env.revision OR g.collection_revision<>c.revision OR r.projector_binding IS DISTINCT FROM c.projector_binding OR source_signal.nonce IS DISTINCT FROM g.source_signal OR model_signal.nonce IS DISTINCT FROM g.model_signal))) AND NOT EXISTS(SELECT 1 FROM moria.runtime_executions e WHERE e.environment=c.environment AND e.resource=c.id AND e.spec->>'domain'='search' AND e.status IN ('queued','running','preparing')) ORDER BY c.environment,c.resource LIMIT 1",&[&after.as_ref().map(|key|key.0.as_str()),&after.as_ref().map(|key|key.1.as_str())]).await?;
        let Some(candidate) = candidate else {
            *after = None;
            return Ok(false);
        };
        let environment: String = candidate.get(0);
        let resource: String = candidate.get(1);
        *after = Some((environment.clone(), resource.clone()));
        drop(client);
        let mut admission =
            crate::functions::Admission::begin(&self.db, &environment, true, None).await?;
        let tx = admission.client.as_mut().unwrap().transaction().await?;
        let collection=tx.query_one("SELECT c.*,env.revision AS target_revision FROM moria.native_search_collections c JOIN moria.environments env ON env.id=c.environment WHERE c.environment=$1 AND c.resource=$2 AND c.enabled FOR UPDATE OF c",&[&environment,&resource]).await?;
        let collection_id: Uuid = collection.get("id");
        let target: i64 = collection.get("target_revision");
        if tx.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_executions WHERE environment=$1 AND resource=$2 AND spec->>'domain'='search' AND status IN ('queued','running','preparing'))",&[&environment,&collection_id]).await?.get::<_,bool>(0){tx.commit().await?;admission.release().await?;return Ok(false);}
        let active: Option<Uuid> = collection.get("active_generation");
        let Some(active) = active else {
            self.accept_rebuild(
                tx.client(),
                None,
                &environment,
                &resource,
                &format!("initial/{target}"),
            )
            .await?;
            tx.commit().await?;
            admission.release().await?;
            return Ok(true);
        };
        let root=tx.query_one("SELECT g.*,r.execution,r.projector_binding,r.source_stage,r.source_revision::text AS source_pin_revision,r.markings,e.spec FROM moria.native_search_generations g JOIN moria.native_search_rebuilds r ON r.generation=g.id JOIN moria.runtime_executions e ON e.id=r.execution WHERE g.id=$1 AND g.status='active' FOR UPDATE OF g",&[&active]).await?;
        let covered: i64 = root.get("covered_revision");
        let projector: Uuid = collection.get("projector_binding");
        let binding=tx.query_one("SELECT service_id,service_binding_revision FROM moria.native_search_projector_bindings WHERE id=$1 AND enabled",&[&projector]).await?;
        let service = RegisteredService::load_in(
            &self.db,
            tx.client(),
            &environment,
            collection_id,
            "search",
            &binding.get::<_, String>(1),
        )
        .await?;
        ensure!(
            service.id() == binding.get::<_, Uuid>(0),
            "SERVICE_PURPOSE_REVOKED"
        );
        let pin = resolve_full_object_source_pin_in(
            tx.client(),
            &service,
            projector,
            &target.to_string(),
        )
        .await?;
        let profile = self
            .models
            .registered_embedding_profile_in(tx.client(), &service, &pin.model_resource)
            .await?;
        ensure!(
            serde_json::from_value::<Vec<String>>(profile["markings"].clone())?
                .iter()
                .all(|label| pin.markings.contains(label)),
            "MARKING_DOWNGRADE"
        );
        let definition: Definition = serde_json::from_value(collection.get("definition"))?;
        let changed = projector != root.get::<_, Uuid>("projector_binding")
            || pin.source_stage != root.get::<_, Uuid>("source_stage")
            || pin.source_revision != root.get::<_, String>("source_pin_revision")
            || pin.object_binding_revision != root.get::<_, String>("object_binding_revision")
            || profile["modelBindingFingerprint"] != root.get::<_, String>("model_fingerprint")
            || root.get::<_, String>("collection_revision")
                != collection.get::<_, String>("revision");
        if changed {
            self.accept_rebuild(
                tx.client(),
                None,
                &environment,
                &resource,
                &format!("replacement/{active}/{target}"),
            )
            .await?;
            tx.commit().await?;
            admission.release().await?;
            return Ok(true);
        }
        if covered >= target {
            tx.execute("UPDATE moria.native_search_generations SET source_signal=(SELECT nonce FROM moria.native_search_binding_signals WHERE environment=$2 AND resource=$3),model_signal=(SELECT nonce FROM moria.native_search_binding_signals WHERE environment=$2 AND resource=$4) WHERE id=$1",&[&active,&environment,&pin.object_resource,&pin.model_resource]).await?;
            tx.commit().await?;
            admission.release().await?;
            return Ok(false);
        }
        let labels: Vec<String> = root.get("markings");
        ensure!(
            pin.markings.iter().all(|label| labels.contains(label)),
            "AUTHORIZATION_CHANGED"
        );
        let mut spec: ExecutionSpec = serde_json::from_value(root.get("spec"))?;
        spec.release=fingerprint(&json!({"runtime":env!("MORIA_INDEXING_SOURCE_SHA256"),"generation":active,"model":profile["modelBindingFingerprint"],"kind":"search-replay/v1"}))?.trim_start_matches("sha256:").into();
        spec.pinned_inputs = vec![pin.dataset.execution_input()];
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
        let binding = service
            .bind_background_in(tx.client(), &json!(spec), &operations, &labels)
            .await?;
        ensure!(
            binding.security_epoch == pin.dataset.security_epoch
                && binding.instance_epoch == pin.dataset.instance_epoch,
            "AUTHORIZATION_CHANGED"
        );
        let (input, hash) = crate::execution::normalized(&spec, &json!({}))?;
        let tasks = crate::execution::plans(
            &spec,
            &[TaskPlan {
                key: "catchup".into(),
                partition: None,
            }],
        )?;
        let key = format!("search-replay/{active}/{covered}/{target}");
        let execution: Uuid = tx
            .query_one(
                "SELECT moria.runtime_execution_request($1,$2,$3,$4,$5)",
                &[&binding.id, &key, &input, &hash, &tasks],
            )
            .await?
            .get(0);
        tx.query_one(
            "SELECT moria.runtime_job_delegation_accept($1)",
            &[&execution],
        )
        .await?;
        tx.execute("INSERT INTO moria.native_search_execution_sources(execution,generation,kind,target_revision,model_secret_approval) VALUES($1,$2,'replay',$3,$4)",&[&execution,&active,&target,&model_secret_approval]).await?;
        let task: Uuid = tx
            .query_one(
                "SELECT id FROM moria.runtime_execution_tasks WHERE execution=$1",
                &[&execution],
            )
            .await?
            .get(0);
        tx.execute("INSERT INTO moria.native_search_task_progress(task,generation,lower_uid) VALUES($1,$2,$3)",&[&task,&active,&Uuid::nil()]).await?;
        tx.execute(
            "UPDATE moria.native_search_replay_progress SET complete=false WHERE generation=$1",
            &[&active],
        )
        .await?;
        tx.execute("INSERT INTO moria.runtime_change_pins(id,environment,incarnation,owner,after_revision,expires_at) SELECT $1,$2,incarnation,$1,COALESCE((SELECT max(commit_revision) FROM moria.runtime_change_transactions WHERE environment=$2 AND incarnation=i.incarnation AND complete AND commit_revision<=$3),0),e.deadline FROM moria.indexing_instance i,moria.runtime_executions e WHERE e.id=$1",&[&execution,&environment,&covered]).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(true)
    }
}
