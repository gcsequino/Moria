use super::{Definition, NativeSearch};
use crate::{
    authority::{Authority, VerifiedSession},
    authorization::authorize,
    canonical::fingerprint,
    jobs::RegisteredService,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use tokio_postgres::Client;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SearchRuntimeProfile {
    pub service_binding_revision: String,
    pub resource_class: String,
    pub partitions: u32,
    pub timeout_seconds: u32,
    pub markings: Vec<String>,
}
impl NativeSearch {
    /// Caller owns the Source activation transaction; this registers metadata and performs no provider I/O.
    pub(crate) async fn install_in(
        client: &Client,
        session: &VerifiedSession,
        resource: Uuid,
        definition: Value,
        profile: &SearchRuntimeProfile,
    ) -> Result<Value> {
        let mut shape: Definition = serde_json::from_value(definition)?;
        ensure!(
            shape.chunk_size == 1000
                && shape.chunk_overlap == 100
                && shape.metric == "cosine"
                && shape.search_mode == "exact"
                && Uuid::parse_str(&shape.source).is_ok()
                && Uuid::parse_str(&shape.embedding_model).is_ok()
                && !shape.text_property.is_empty()
                && shape.text_property.len() <= 100
                && (1..=256).contains(&profile.partitions)
                && profile.partitions.is_power_of_two()
                && (1..=1800).contains(&profile.timeout_seconds)
                && !profile.service_binding_revision.is_empty()
                && profile.service_binding_revision.len() <= 128,
            "UNSUPPORTED_CAPABILITY"
        );
        let labels = profile.markings.iter().cloned().collect::<BTreeSet<_>>();
        ensure!(
            labels.len() == profile.markings.len()
                && labels.len() <= 256
                && labels
                    .iter()
                    .all(|label| !label.is_empty() && label.len() <= 1024),
            "SEARCH_PROJECTOR_UNAVAILABLE"
        );
        shape.markings = labels.iter().cloned().collect();
        let definition = serde_json::to_value(&shape)?;
        let object=client.query_opt("SELECT revision,definition,authority_generation::text FROM moria.native_serving_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&shape.source]).await?.ok_or_else(||anyhow::anyhow!("SEARCH_PROJECTOR_UNAVAILABLE"))?;
        let object_definition: Value = object.get(1);
        let field = &object_definition["schema"]["properties"][&shape.text_property];
        ensure!(field["type"] == "string", "VALUE_INVALID");
        let dataset = object_definition["sourceResource"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("UNSUPPORTED_DATASET_BINDING"))?;
        Uuid::parse_str(dataset)?;
        client
            .query_one(
                "SELECT moria.runtime_dataset_kind($1,$2)",
                &[&session.environment(), &dataset],
            )
            .await?;
        let model=client.query_opt("SELECT revision,definition FROM moria.native_model_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&shape.embedding_model]).await?.ok_or_else(||anyhow::anyhow!("SEARCH_PROJECTOR_UNAVAILABLE"))?;
        let model_definition: Value = model.get(1);
        let destination = model_definition["destination"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("DESTINATION_FORBIDDEN"))?;
        ensure!(
            model_definition["capabilities"]
                .as_array()
                .is_some_and(|values| values.iter().any(|value| value == "embed"))
                && model_definition["dimensions"]
                    .as_u64()
                    .is_some_and(|n| (1..=4096).contains(&n)),
            "UNSUPPORTED_CAPABILITY"
        );
        ensure!(
            model_definition["destinationPolicy"]["allowedDestinations"]
                .as_array()
                .is_some_and(|values| values.iter().any(|value| value == destination))
                && labels.iter().all(|label| {
                    model_definition["destinationPolicy"]["allowedMarkings"]
                        .as_array()
                        .is_some_and(|values| values.iter().any(|value| value == label))
                }),
            "DESTINATION_FORBIDDEN"
        );
        let service = RegisteredService::load_current_in(
            client,
            session.environment(),
            resource,
            "search",
            &profile.service_binding_revision,
        )
        .await?;
        ensure!(
            service.resource_class() == profile.resource_class,
            "SERVICE_PURPOSE_REVOKED"
        );
        let resource_text = resource.to_string();
        let requirements = [
            (resource_text.as_str(), "project"),
            (shape.source.as_str(), "read"),
            (dataset, "read"),
            (shape.embedding_model.as_str(), "invoke"),
        ];
        let mut incoming = BTreeSet::new();
        let caller = Authority::current(client, session).await?;
        for (reference, operation) in requirements {
            let required = Authority::resource_access(
                client,
                session,
                reference,
                if operation == "project" {
                    "write"
                } else {
                    "read"
                },
            )
            .await?;
            authorize(&caller.identity, reference, operation, &required)?;
            incoming.extend(required);
            incoming.extend(
                service
                    .resource_labels_in(client, reference, operation)
                    .await?,
            );
        }
        for value in [
            &object_definition["policies"]["markings"],
            &model_definition["markings"],
        ] {
            if !value.is_null() {
                incoming.extend(serde_json::from_value::<Vec<String>>(value.clone())?);
            }
        }
        ensure!(
            incoming.iter().all(|label| labels.contains(label)),
            "MARKING_DOWNGRADE"
        );
        authorize(&caller.identity, &resource_text, "project", &shape.markings)?;
        let revision = fingerprint(&json!([definition, profile]))?;
        let projector_revision = fingerprint(&json!([
            revision,
            service.id(),
            object.get::<_, String>(0),
            object.get::<_, String>(2),
            model.get::<_, String>(0),
            model_definition
        ]))?;
        if let Some(row)=client.query_opt("SELECT c.id,c.revision,c.definition,p.revision AS projector_revision FROM moria.native_search_collections c LEFT JOIN moria.native_search_projector_bindings p ON p.id=c.projector_binding AND p.enabled WHERE c.environment=$1 AND c.resource=$2 AND c.enabled",&[&session.environment(),&resource_text]).await? {
            ensure!(row.get::<_,Uuid>(0)==resource,"BINDING_CHANGED");
            if row.get::<_,String>(1)==revision&&row.get::<_,Value>(2)==definition&&row.get::<_,Option<String>>(3).as_deref()==Some(&projector_revision){Authority::finish(client,session,&caller).await?;return Ok(json!({"resource":resource,"revision":revision,"configured":true,"readiness":"SOURCE_GENERATION_AND_CAPABILITY_PROBE_REQUIRED"}))}
        }
        client.execute("UPDATE moria.native_search_projector_bindings SET enabled=false WHERE environment=$1 AND collection=$2 AND enabled",&[&session.environment(),&resource_text]).await?;
        client.execute("INSERT INTO moria.native_search_collections(id,environment,resource,revision,definition,resource_class,partitions,timeout_seconds) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(environment,resource) DO UPDATE SET revision=EXCLUDED.revision,definition=EXCLUDED.definition,resource_class=EXCLUDED.resource_class,partitions=EXCLUDED.partitions,timeout_seconds=EXCLUDED.timeout_seconds,enabled=true WHERE native_search_collections.id=EXCLUDED.id",&[&resource,&session.environment(),&resource_text,&revision,&definition,&profile.resource_class,&(profile.partitions as i32),&(profile.timeout_seconds as i32)]).await?;
        let projector:Uuid=client.query_one("INSERT INTO moria.native_search_projector_bindings(revision,environment,collection,collection_id,service_id,service_binding_revision,source_resource,text_property,source_dataset,model_resource,model_destination,markings) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT(environment,collection,revision) DO UPDATE SET enabled=true RETURNING id",&[&projector_revision,&session.environment(),&resource_text,&resource,&service.id(),&profile.service_binding_revision,&shape.source,&shape.text_property,&dataset,&shape.embedding_model,&destination,&shape.markings]).await?.get(0);
        client.execute("UPDATE moria.native_search_collections SET projector_binding=$3 WHERE environment=$1 AND resource=$2",&[&session.environment(),&resource_text,&projector]).await?;
        client
            .query_one(
                "SELECT moria.runtime_search_projector_admit($1,$2)",
                &[&projector, &service.id()],
            )
            .await?;
        let current = Authority::current(client, session).await?;
        for (reference, operation) in requirements {
            let mut required = Authority::resource_access(
                client,
                session,
                reference,
                if operation == "project" {
                    "write"
                } else {
                    "read"
                },
            )
            .await?;
            required.extend(shape.markings.iter().cloned());
            authorize(&current.identity, reference, operation, &required)?;
        }
        Authority::finish(client, session, &current).await?;
        Ok(
            json!({"resource":resource,"revision":revision,"projector":projector,"configured":true,"readiness":"SOURCE_GENERATION_AND_CAPABILITY_PROBE_REQUIRED"}),
        )
    }
}
