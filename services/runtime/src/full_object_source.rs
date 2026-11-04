use crate::{
    authority::{Authority, VerifiedSession},
    authorization::{DestinationPolicy, authorize, authorize_destination},
    canonical::fingerprint,
    catalog::{DatasetIoGrant, DatasetPin, dataset_pin_registered_row},
    database::Database,
    execution::Lease,
    jobs::RegisteredService,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};
use tokio_postgres::Client;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FullObjectSourcePin {
    pub projector_binding: Uuid,
    pub projector_revision: String,
    pub service_id: Uuid,
    pub service_binding_revision: String,
    pub collection: String,
    pub collection_id: Uuid,
    pub object_resource: String,
    pub object_id: Uuid,
    pub object_binding_revision: String,
    pub text_property: String,
    pub source_stage: Uuid,
    pub source_revision: String,
    pub source_schema_hash: String,
    pub environment_revision: String,
    pub dataset: DatasetPin,
    pub model_resource: String,
    pub model_destination: String,
    pub model_binding_revision: String,
    pub markings: Vec<String>,
}
#[derive(Clone, Copy)]
pub(crate) struct FullObjectPageBudget {
    pub rows: u32,
    pub bytes: u32,
    pub time_ms: u32,
}
pub(crate) struct FullObjectTextRow {
    pub uid: Uuid,
    pub text: Option<String>,
    pub deleted: bool,
}

fn revision(text: &str) -> Result<i64> {
    let value: i64 = text.parse()?;
    ensure!(value >= 0 && value.to_string() == text, "REVISION_INVALID");
    Ok(value)
}
fn marks(value: Option<&Value>) -> Result<Vec<String>> {
    Ok(serde_json::from_value(value.cloned().unwrap_or(json!([])))?)
}

/// Admission adds only the operator-declared dependencies, then captures current metadata authority.
pub(crate) async fn resolve_full_object_source_pin_in(
    client: &Client,
    service: &RegisteredService,
    projector_binding: Uuid,
    at_revision: &str,
) -> Result<FullObjectSourcePin> {
    client
        .query_one(
            "SELECT moria.runtime_search_projector_admit($1,$2)",
            &[&projector_binding, &service.id()],
        )
        .await?;
    pin_state_in(client, service, projector_binding, at_revision).await
}

async fn pin_state_in(
    client: &Client,
    service: &RegisteredService,
    projector_binding: Uuid,
    at_revision: &str,
) -> Result<FullObjectSourcePin> {
    let at = revision(at_revision)?;
    let state = service.state_in(client).await?;
    let current: i64 = client
        .query_one(
            "SELECT revision FROM moria.environments WHERE id=$1",
            &[&service.environment()],
        )
        .await?
        .get(0);
    ensure!(at <= current, "REVISION_INVALID");
    let row=client.query_opt("SELECT p.*,c.definition AS collection_definition,o.object_type_id,o.revision AS object_binding_revision,o.authority_generation::text AS object_binding_generation,o.definition AS object_definition,g.stage AS source_stage,g.revision::text AS source_revision,g.object_schema,g.required_markings,build.definition AS indexing_definition,i.source_revision::text AS dataset_revision,i.metadata_location,i.snapshot_id,m.revision AS model_binding_revision,m.definition AS model_definition FROM moria.native_search_projector_bindings p JOIN moria.native_search_collections c ON c.environment=p.environment AND c.resource=p.collection AND c.id=p.collection_id AND c.projector_binding=p.id AND c.enabled JOIN moria.native_serving_bindings o ON o.environment=p.environment AND o.resource=p.source_resource AND o.enabled JOIN LATERAL(SELECT * FROM moria.source_generations g WHERE g.environment=p.environment AND g.object_type=p.source_resource AND g.revision<=$3 ORDER BY revision DESC LIMIT 1)g ON true JOIN moria.index_provenance i ON i.stage=g.stage AND i.environment=p.environment AND i.source_dataset=p.source_dataset JOIN moria.indexing_builds build ON build.id=g.stage AND build.source=i.source_dataset AND build.source_revision=i.source_revision AND build.metadata_location=i.metadata_location AND build.snapshot_id=i.snapshot_id AND build.binding_revision=i.contract JOIN moria.native_model_bindings m ON m.environment=p.environment AND m.resource=p.model_resource AND m.enabled WHERE p.id=$1 AND p.environment=$2 AND p.enabled AND p.purpose='full-object-projection' AND p.domain='search' FOR SHARE OF p,c,o,m",&[&projector_binding,&service.environment(),&at]).await?.ok_or_else(||anyhow::anyhow!("FULL_SOURCE_PURPOSE_REVOKED"))?;
    let service_id: Uuid = row.get("service_id");
    let collection_id: Uuid = row.get("collection_id");
    let target_revision: String = row.get("service_binding_revision");
    ensure!(
        service_id == service.id()
            && collection_id == service.target()
            && target_revision == service.binding_revision(),
        "SERVICE_PURPOSE_REVOKED"
    );
    let object_resource: String = row.get("source_resource");
    let collection: String = row.get("collection");
    let model_resource: String = row.get("model_resource");
    let source_dataset: String = row.get("source_dataset");
    let dataset_resource: Uuid = source_dataset
        .parse()
        .map_err(|_| anyhow::anyhow!("UNSUPPORTED_DATASET_BINDING"))?;
    let text_property: String = row.get("text_property");
    let index_definition: Value = row.get("indexing_definition");
    if let Some(generation) = index_definition.get("servingBindingGeneration") {
        ensure!(
            generation == &Value::String(row.get("object_binding_generation"))
                && index_definition["servingBindingRevision"]
                    == Value::String(row.get("object_binding_revision"))
                && index_definition["servingDefinition"]
                    == row.get::<_, Value>("object_definition"),
            "SEARCH_SOURCE_CHANGED"
        );
    } else {
        let object_id: Option<Uuid> = row.get("object_type_id");
        ensure!(!client.query_one("SELECT EXISTS(SELECT 1 FROM moria.fs_resource_identities WHERE resource_id=$1)",&[&object_id]).await?.get::<_,bool>(0),"SEARCH_SOURCE_CHANGED");
    }
    let schema: Value = row
        .get::<_, Option<Value>>("object_schema")
        .ok_or_else(|| anyhow::anyhow!("SCHEMA_CHANGED"))?;
    ensure!(
        !text_property.is_empty()
            && text_property.len() <= 128
            && schema["properties"][&text_property]["type"] == "string",
        "UNSUPPORTED_SOURCE_FIELD"
    );
    let mut labels = BTreeSet::new();
    for (resource, operation) in [
        (&collection, "project"),
        (&object_resource, "read"),
        (&source_dataset, "read"),
        (&model_resource, "invoke"),
    ] {
        labels.extend(
            service
                .resource_labels_in(client, resource, operation)
                .await?,
        );
    }
    labels.extend(row.get::<_, Vec<String>>("required_markings"));
    let object_definition: Value = row.get("object_definition");
    let collection_definition: Value = row.get("collection_definition");
    let model_definition: Value = row.get("model_definition");
    labels.extend(marks(
        object_definition
            .get("policies")
            .and_then(|p| p.get("markings")),
    )?);
    labels.extend(marks(collection_definition.get("markings"))?);
    labels.extend(marks(model_definition.get("markings"))?);
    let output: Vec<String> = row.get("markings");
    ensure!(
        labels.iter().all(|label| output.contains(label)),
        "MARKING_DOWNGRADE"
    );
    labels.extend(output);
    authorize(
        &state.identity,
        &collection,
        "project",
        &labels.iter().cloned().collect::<Vec<_>>(),
    )?;
    let model_destination: String = row.get("model_destination");
    ensure!(
        model_definition["destination"].as_str() == Some(model_destination.as_str()),
        "DESTINATION_FORBIDDEN"
    );
    let policy: DestinationPolicy =
        serde_json::from_value(model_definition["destinationPolicy"].clone())?;
    authorize_destination(&state.identity, &labels, Some(&policy), &model_destination)?;
    client
        .query_one(
            "SELECT moria.runtime_dataset_kind($1,$2)",
            &[&service.environment(), &source_dataset],
        )
        .await?;
    let history=client.query_one("SELECT revision::text AS revision,snapshot_id,metadata_location FROM moria.snapshot_history WHERE environment=$1 AND dataset=$2 AND revision::text=$3 AND snapshot_id=$4 AND metadata_location=$5",&[&service.environment(),&source_dataset,&row.get::<_,String>("dataset_revision"),&row.get::<_,String>("snapshot_id"),&row.get::<_,String>("metadata_location")]).await?;
    let dataset = dataset_pin_registered_row(
        client,
        service,
        dataset_resource,
        &history,
        service
            .resource_labels_in(client, &source_dataset, "read")
            .await?,
    )
    .await?;
    let current = service.state_in(client).await?;
    ensure!(
        state.security_epoch == current.security_epoch
            && state.instance_epoch == current.instance_epoch,
        "AUTHORIZATION_CHANGED"
    );
    Ok(FullObjectSourcePin {
        projector_binding,
        projector_revision: row.get("revision"),
        service_id,
        service_binding_revision: target_revision,
        collection,
        collection_id,
        object_resource,
        object_id: row
            .get::<_, Option<Uuid>>("object_type_id")
            .ok_or_else(|| anyhow::anyhow!("BINDING_INVALID"))?,
        object_binding_revision: crate::serving::binding_revision(
            &row.get::<_, String>("object_binding_revision"),
            &row.get::<_, String>("object_binding_generation"),
        )?,
        text_property,
        source_stage: row
            .get::<_, Option<Uuid>>("source_stage")
            .ok_or_else(|| anyhow::anyhow!("SEARCH_SOURCE_MISSING"))?,
        source_revision: row.get("source_revision"),
        source_schema_hash: fingerprint(&json!({
            "objectSchema":schema,
            "sourceColumns":index_definition.get("sourceColumns"),
            "sourceMapping":index_definition.get("sourceMapping"),
            "datasetMetadata":dataset.metadata_location,
        }))?,
        environment_revision: at_revision.into(),
        dataset,
        model_resource,
        model_destination,
        model_binding_revision: row.get("model_binding_revision"),
        markings: labels.into_iter().collect(),
    })
}

/// Full-data access exists only inside the registered projector's actual accepted execution lease.
pub(crate) struct FullObjectSourceGrant {
    db: Database,
    session: VerifiedSession,
    lease: Lease,
    pin: FullObjectSourcePin,
    dataset: Arc<DatasetIoGrant>,
}
impl FullObjectSourceGrant {
    pub(crate) async fn acquire(
        db: Database,
        session: VerifiedSession,
        lease: Lease,
        pin: FullObjectSourcePin,
    ) -> Result<Self> {
        let dataset = Arc::new(
            DatasetIoGrant::acquire(
                db.clone(),
                session.clone(),
                lease.clone(),
                pin.dataset.namespace.clone(),
            )
            .await?,
        );
        let grant = Self {
            db,
            session,
            lease,
            pin,
            dataset,
        };
        grant.check().await?;
        Ok(grant)
    }
    pub(crate) fn markings(&self) -> &[String] {
        &self.pin.markings
    }
    pub(crate) fn session(&self) -> &VerifiedSession {
        &self.session
    }
    pub(crate) fn model_resource(&self) -> &str {
        &self.pin.model_resource
    }
    pub(crate) async fn check(&self) -> Result<()> {
        let client = self.db.get().await?;
        self.check_in(&client).await
    }
    pub(crate) async fn check_in(&self, client: &Client) -> Result<()> {
        let purpose: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM moria.native_search_projector_bindings p WHERE p.id=$2 AND p.environment=$3 AND p.collection_id=$4 AND p.revision=$5 AND p.service_id=$6 AND p.service_binding_revision=$7 AND p.collection=$8 AND p.enabled AND p.purpose='full-object-projection' AND p.domain='search') FROM moria.runtime_executions e WHERE e.id=$1 FOR UPDATE OF e",
                &[&self.lease.execution, &self.pin.projector_binding, &self.session.environment(), &self.pin.collection_id, &self.pin.projector_revision, &self.pin.service_id, &self.pin.service_binding_revision, &self.pin.collection],
            )
            .await?
            .get(0);
        ensure!(purpose, "FULL_SOURCE_PURPOSE_REVOKED");
        self.dataset.check_current_in(client).await?;
        let at = revision(&self.pin.environment_revision)?;
        let row=client.query_opt("SELECT r.source_stage,r.source_revision::text,g.base_revision,b.spec->>'resourceClass' AS resource_class FROM moria.runtime_executions e JOIN moria.runtime_execution_bindings b ON b.id=e.binding JOIN moria.runtime_execution_services s ON s.binding=b.id JOIN moria.runtime_job_delegations d ON d.execution=e.id AND d.service_authority=s.authority JOIN moria.native_search_execution_sources map ON map.execution=e.id JOIN moria.native_search_rebuilds r ON r.generation=map.generation JOIN moria.native_search_generations g ON g.id=r.generation JOIN moria.environments env ON env.id=e.environment WHERE e.id=$1 AND e.environment=$2 AND e.resource=$3 AND e.spec->>'domain'='search' AND e.status IN ('running','preparing') AND s.authority=$4 AND s.target_revision=$5 AND r.projector_binding=$6 AND g.base_revision<=$7 AND $7<=env.revision AND moria.runtime_dataset_session_matches($8,$1,$9,$10,$11,$12)",&[&self.lease.execution,&self.session.environment(),&self.pin.collection_id,&self.pin.service_id,&self.pin.service_binding_revision,&self.pin.projector_binding,&at,&self.session.session_hash(),&self.lease.task,&self.lease.owner,&self.lease.fence.parse::<i64>()?,&self.lease.incarnation]).await?.ok_or_else(||anyhow::anyhow!("FULL_SOURCE_PURPOSE_REVOKED"))?;
        ensure!(
            row.get::<_, Option<Uuid>>("source_stage") == Some(self.pin.source_stage)
                && row.get::<_, String>("source_revision") == self.pin.source_revision,
            "SEARCH_SOURCE_CHANGED"
        );
        let service = RegisteredService::load_in(
            &self.db,
            client,
            self.session.environment(),
            self.pin.collection_id,
            "search",
            &self.pin.service_binding_revision,
        )
        .await?;
        let model = crate::models::NativeModels::registered_admission_in(
            client,
            &service,
            &self.pin.model_resource,
        )
        .await?;
        let accepted: Option<Value> = client.query_one("SELECT model_secret_approval FROM moria.native_search_execution_sources WHERE execution=$1", &[&self.lease.execution]).await?.get(0);
        ensure!(
            model
                .secret_approval
                .as_ref()
                .map(serde_json::to_value)
                .transpose()?
                == accepted,
            "SECRET_APPROVAL_REVOKED"
        );
        ensure!(
            model
                .markings
                .iter()
                .all(|label| self.pin.markings.contains(label)),
            "MARKING_DOWNGRADE"
        );
        let fresh = pin_state_in(
            client,
            &service,
            self.pin.projector_binding,
            &self.pin.environment_revision,
        )
        .await?;
        ensure!(
            fresh.source_stage == self.pin.source_stage
                && fresh.source_revision == self.pin.source_revision,
            "SEARCH_SOURCE_CHANGED"
        );
        ensure!(
            serde_json::to_value(&fresh)? == serde_json::to_value(&self.pin)?,
            "AUTHORIZATION_CHANGED"
        );
        Authority::current(client, &self.session).await?;
        Ok(())
    }
    pub(crate) async fn page(
        &self,
        lower: Uuid,
        upper: Option<Uuid>,
        after: Option<Uuid>,
        budget: FullObjectPageBudget,
    ) -> Result<Vec<FullObjectTextRow>> {
        ensure!(
            self.lease.task.is_some()
                && (1..=1000).contains(&budget.rows)
                && (1..=8 * 1024 * 1024).contains(&budget.bytes)
                && (1..=30000).contains(&budget.time_ms),
            "RESOURCE_LIMIT"
        );
        ensure!(
            upper.is_none_or(|value| lower < value)
                && after.is_none_or(|value| value >= lower && upper.is_none_or(|end| value < end)),
            "SOURCE_RANGE_INVALID"
        );
        self.check().await?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT set_config('statement_timeout',$1,true)",
            &[&format!("{}ms", budget.time_ms)],
        )
        .await?;
        tx.batch_execute("SET LOCAL work_mem='16MB'").await?;
        let range=tx.query_opt("SELECT 1 FROM moria.native_search_task_progress WHERE task=$1 AND lower_uid=$2 AND upper_uid IS NOT DISTINCT FROM $3",&[&self.lease.task.unwrap(),&lower,&upper]).await?;
        ensure!(range.is_some(), "SOURCE_RANGE_INVALID");
        let at = revision(&self.pin.environment_revision)?;
        let rows = i64::from(budget.rows);
        let bytes = i64::from(budget.bytes);
        let sql = "WITH keys AS MATERIALIZED(SELECT r.uid,64+COALESCE(octet_length(to_jsonb(CASE WHEN COALESCE(o.tombstone,false) THEN NULL::text WHEN o.overrides?$5 THEN o.overrides->>$5 ELSE r.value->>$5 END)::text),0)::bigint AS bytes FROM moria.runtime_object_base_stage($1,$2,$3,$4) r LEFT JOIN LATERAL(SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$2 AND object_type=$3 AND uid=r.uid AND revision<=$4 ORDER BY revision DESC LIMIT 1)o ON true WHERE r.uid>=$6 AND ($7::uuid IS NULL OR r.uid<$7) AND ($8::uuid IS NULL OR r.uid>$8) ORDER BY r.uid LIMIT $9),sized AS MATERIALIZED(SELECT uid,bytes,sum(bytes)OVER(ORDER BY uid) AS total FROM keys),chosen AS MATERIALIZED(SELECT uid FROM sized WHERE total<=$10::bigint),guard AS MATERIALIZED(SELECT moria.native_serving_check_budget(COALESCE((SELECT bytes FROM sized ORDER BY uid LIMIT 1),0),$10::bigint) AS ok) SELECT p.uid,p.text,p.deleted FROM guard LEFT JOIN LATERAL(SELECT r.uid,CASE WHEN COALESCE(o.tombstone,false) THEN NULL::text WHEN o.overrides?$5 THEN o.overrides->>$5 ELSE r.value->>$5 END AS text,COALESCE(o.tombstone,false) AS deleted FROM chosen k JOIN moria.runtime_object_base_stage($1,$2,$3,$4) r ON r.uid=k.uid LEFT JOIN LATERAL(SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$2 AND object_type=$3 AND uid=r.uid AND revision<=$4 ORDER BY revision DESC LIMIT 1)o ON true ORDER BY r.uid)p ON guard.ok ORDER BY p.uid";
        let result = tx
            .query(
                sql,
                &[
                    &self.pin.source_stage,
                    &self.session.environment(),
                    &self.pin.object_resource,
                    &at,
                    &self.pin.text_property,
                    &lower,
                    &upper,
                    &after,
                    &rows,
                    &bytes,
                ],
            )
            .await?;
        let mut output = Vec::new();
        let mut observed = 0usize;
        for row in result {
            if let Some(uid) = row.get::<_, Option<Uuid>>(0) {
                let text: Option<String> = row.get(1);
                observed = observed
                    .checked_add(text.as_ref().map_or(0, |text| text.len()) + 64)
                    .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
                ensure!(observed <= budget.bytes as usize, "RESOURCE_LIMIT");
                output.push(FullObjectTextRow {
                    uid,
                    text,
                    deleted: row.get(2),
                });
            }
        }
        tx.commit().await?;
        drop(client);
        self.check().await?;
        Ok(output)
    }
    pub(crate) async fn read_uids(&self, uids: &[Uuid]) -> Result<Vec<FullObjectTextRow>> {
        ensure!(
            self.lease.task.is_some()
                && uids.len() <= 128
                && uids.iter().copied().collect::<BTreeSet<_>>().len() == uids.len(),
            "SOURCE_RANGE_INVALID"
        );
        self.check().await?;
        if uids.is_empty() {
            return Ok(Vec::new());
        }
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.batch_execute("SET LOCAL statement_timeout='5s'; SET LOCAL work_mem='16MB'")
            .await?;
        let range = tx
            .query_one(
                "SELECT lower_uid,upper_uid FROM moria.native_search_task_progress WHERE task=$1",
                &[&self.lease.task.unwrap()],
            )
            .await?;
        let lower: Uuid = range.get(0);
        let upper: Option<Uuid> = range.get(1);
        ensure!(
            uids.iter()
                .all(|uid| *uid >= lower && upper.is_none_or(|upper| *uid < upper)),
            "SOURCE_RANGE_INVALID"
        );
        let at = revision(&self.pin.environment_revision)?;
        let sql = "WITH sizes AS MATERIALIZED(SELECT requested.uid,64+COALESCE(octet_length(to_jsonb(CASE WHEN r.uid IS NULL OR COALESCE(o.tombstone,false) THEN NULL::text WHEN o.overrides?$5 THEN o.overrides->>$5 ELSE r.value->>$5 END)::text),0)::bigint AS bytes FROM unnest($6::uuid[])requested(uid) LEFT JOIN moria.runtime_object_base_stage($1,$2,$3,$4) r ON r.uid=requested.uid LEFT JOIN LATERAL(SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$2 AND object_type=$3 AND uid=requested.uid AND revision<=$4 ORDER BY revision DESC LIMIT 1)o ON true),guard AS MATERIALIZED(SELECT moria.native_serving_check_budget(COALESCE(sum(bytes),0)::bigint,8388608) AS ok FROM sizes) SELECT payload.uid,payload.text,payload.deleted FROM guard LEFT JOIN LATERAL(SELECT requested.uid,CASE WHEN r.uid IS NULL OR COALESCE(o.tombstone,false) THEN NULL::text WHEN o.overrides?$5 THEN o.overrides->>$5 ELSE r.value->>$5 END AS text,(r.uid IS NULL OR COALESCE(o.tombstone,false)) AS deleted FROM unnest($6::uuid[])requested(uid) LEFT JOIN moria.runtime_object_base_stage($1,$2,$3,$4) r ON r.uid=requested.uid LEFT JOIN LATERAL(SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$2 AND object_type=$3 AND uid=requested.uid AND revision<=$4 ORDER BY revision DESC LIMIT 1)o ON true ORDER BY requested.uid)payload ON guard.ok ORDER BY payload.uid";
        let result = tx
            .query(
                sql,
                &[
                    &self.pin.source_stage,
                    &self.session.environment(),
                    &self.pin.object_resource,
                    &at,
                    &self.pin.text_property,
                    &uids,
                ],
            )
            .await?;
        let mut output = Vec::new();
        let mut bytes = 0usize;
        for row in result {
            if let Some(uid) = row.get::<_, Option<Uuid>>(0) {
                let text: Option<String> = row.get(1);
                bytes = bytes
                    .checked_add(text.as_ref().map_or(0, String::len) + 64)
                    .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
                ensure!(bytes <= 8 * 1024 * 1024, "RESOURCE_LIMIT");
                output.push(FullObjectTextRow {
                    uid,
                    text,
                    deleted: row.get(2),
                });
            }
        }
        ensure!(output.len() == uids.len(), "SOURCE_RANGE_INVALID");
        tx.commit().await?;
        drop(client);
        self.check().await?;
        Ok(output)
    }
}
