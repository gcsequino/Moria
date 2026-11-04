//! Permanent indexing registration is metadata authority; bytes use real indexing attempt leases.
use crate::{
    canonical::{canonical_json, fingerprint},
    database::Database,
    jobs::RegisteredService,
    serving::ObjectSchema,
    storage::{IndexNamespace, configured_binding_fingerprint},
    values,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tokio_postgres::Client;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct IndexDefinitionInput {
    pub source: Uuid,
    pub schema: ObjectSchema,
    pub source_columns: BTreeMap<String, Value>,
    pub source_mapping: BTreeMap<String, String>,
    pub markings: Vec<String>,
    pub partitions: u32,
    pub timeout_seconds: u32,
    pub expected_revision: Option<String>,
    pub artifact_namespace: Option<IndexNamespace>,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct IndexingRegistration {
    pub revision: String,
    pub metadata_binding: Uuid,
    pub build: Option<Uuid>,
}
fn normalized_schema(schema: &ObjectSchema) -> Result<Value> {
    ensure!(
        !schema.properties.is_empty() && schema.properties.len() <= 256,
        "SCHEMA_INVALID"
    );
    let mut properties = BTreeMap::new();
    for (name, descriptor) in &schema.properties {
        ensure!(!name.is_empty() && name.len() <= 256, "SCHEMA_INVALID");
        properties.insert(
            name.clone(),
            values::descriptor(descriptor, &Value::Null, 0)?,
        );
    }
    let primary = properties
        .get(&schema.primary_key)
        .ok_or_else(|| anyhow::anyhow!("SCHEMA_INVALID"))?;
    ensure!(
        primary["type"] == "uid" && primary["nullable"] == false,
        "SCHEMA_INVALID"
    );
    Ok(json!({"primaryKey":schema.primary_key,"properties":properties}))
}

/// The caller owns environment/filesystem admission and rolls back on failure.
/// All declarations come from the accepted compiler, never a public request's physical references.
pub(crate) async fn install_in(
    client: &Client,
    environment: &str,
    target: Uuid,
    service_revision: &str,
    input: IndexDefinitionInput,
) -> Result<IndexingRegistration> {
    let definition = definition_in(client, environment, target, &input).await?;
    register_definition_in(
        client,
        environment,
        target,
        input.source,
        service_revision,
        definition,
        input.expected_revision.as_deref(),
    )
    .await
}
/// Pre-admit every installation before capturing any permanent binding in a release batch.
pub(crate) async fn admit_in(
    client: &Client,
    environment: &str,
    target: Uuid,
    service_revision: &str,
    input: &IndexDefinitionInput,
) -> Result<i64> {
    let definition = definition_in(client, environment, target, input).await?;
    check_previous_in(
        client,
        environment,
        target,
        input.source,
        &definition,
        input.expected_revision.as_deref(),
    )
    .await?;
    let service = RegisteredService::load_current_in(
        client,
        environment,
        target,
        "indexing",
        service_revision,
    )
    .await?;
    let markings: Vec<String> = serde_json::from_value(definition["markings"].clone())?;
    Ok(client
        .query_one(
            "SELECT moria.native_indexing_service_admit($1,$2,$3,$4,$5,$6,$7)",
            &[
                &environment,
                &target,
                &input.source,
                &service.id(),
                &service.binding_revision(),
                &service.resource_class(),
                &markings,
            ],
        )
        .await?
        .get(0))
}
async fn definition_in(
    client: &Client,
    environment: &str,
    target: Uuid,
    input: &IndexDefinitionInput,
) -> Result<Value> {
    ensure!(
        input.partitions > 0
            && input.partitions <= 4096
            && input.partitions.is_power_of_two()
            && input.timeout_seconds > 0
            && input.timeout_seconds <= 86400,
        "INDEXING_INVALID"
    );
    let schema = normalized_schema(&input.schema)?;
    ensure!(
        !input.source_columns.is_empty()
            && input.source_columns.len() <= 256
            && input.source_mapping.len() == input.schema.properties.len(),
        "SCHEMA_INVALID"
    );
    let source_columns = input
        .source_columns
        .iter()
        .map(|(name, column)| Ok((name.clone(), values::descriptor(column, &Value::Null, 0)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    for (target_property, descriptor) in schema["properties"].as_object().unwrap() {
        let source = input
            .source_mapping
            .get(target_property)
            .ok_or_else(|| anyhow::anyhow!("SCHEMA_INVALID"))?;
        ensure!(
            source_columns.get(source) == Some(descriptor),
            "SCHEMA_CHANGED"
        );
    }
    let declared_source=client.query_opt("SELECT columns FROM moria.native_dataset_bindings WHERE environment=$1 AND resource=$2 AND enabled FOR SHARE",&[&environment,&input.source]).await?.ok_or_else(||anyhow::anyhow!("DATASET_BINDING_INVALID"))?;
    let accepted_columns: BTreeMap<String, Value> = serde_json::from_value(declared_source.get(0))?;
    let accepted_columns = accepted_columns
        .into_iter()
        .map(|(name, column)| Ok((name, values::descriptor(&column, &Value::Null, 0)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    ensure!(source_columns == accepted_columns, "SCHEMA_CHANGED");
    let serving=client.query_opt("SELECT definition->'schema' AS schema,definition,revision,authority_generation::text FROM moria.native_serving_bindings WHERE environment=$1 AND resource=$2 AND object_type_id=$3 AND enabled FOR SHARE",
        &[&environment,&target.to_string(),&target]).await?.ok_or_else(||anyhow::anyhow!("BINDING_INVALID"))?;
    let declared: ObjectSchema = serde_json::from_value(serving.get("schema"))?;
    ensure!(normalized_schema(&declared)? == schema, "SCHEMA_CHANGED");
    if let Some(previous)=client.query_opt("SELECT object_schema FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1",&[&environment,&target.to_string()]).await? {
        let previous:Value=previous.get(0);
        if !previous.is_null() {
            ensure!(previous["primaryKey"]==schema["primaryKey"],"MIGRATION_REQUIRED");
            let old=previous["properties"].as_object().ok_or_else(||anyhow::anyhow!("SCHEMA_INVALID"))?;
            let new=schema["properties"].as_object().unwrap();
            ensure!(old.iter().all(|(name,value)|new.get(name)==Some(value))
                && new.iter().all(|(name,value)|old.contains_key(name)||value["nullable"]==true),"MIGRATION_REQUIRED");
        }
    }
    let id:Uuid=client.query_one("INSERT INTO moria.indexing_environments(environment) VALUES($1) ON CONFLICT(environment) DO UPDATE SET environment=EXCLUDED.environment RETURNING id",&[&environment]).await?.get(0);
    let incarnation: Uuid = client
        .query_one("SELECT incarnation FROM moria.indexing_instance", &[])
        .await?
        .get(0);
    let mut markings = input.markings.clone();
    markings.sort();
    markings.dedup();
    ensure!(markings.len() <= 256, "MARKING_DOWNGRADE");
    let mut definition = json!({"version":1,"valueVersion":1,"environmentId":id,"incarnation":incarnation,"targetId":target,
        "partitions":input.partitions,"timeoutSeconds":input.timeout_seconds,"schema":schema,"markings":markings,
        "sourceColumns":source_columns,"sourceMapping":input.source_mapping,
        "servingBindingRevision":serving.get::<_,String>("revision"),"servingBindingGeneration":serving.get::<_,String>("authority_generation"),"servingDefinition":serving.get::<_,Value>("definition")});
    definition["linkConstraints"] = client
        .query_one(
            "SELECT moria.runtime_link_constraints($1,$2)",
            &[&environment, &target.to_string()],
        )
        .await?
        .get(0);
    if let Some(layout)=client.query_opt("SELECT revision FROM moria.runtime_projection_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&environment,&target.to_string()]).await? {
        definition["projectionRevision"]=json!(layout.get::<_,String>(0));
    }
    if let Some(namespace) = &input.artifact_namespace {
        let namespace_row=client.query_opt("SELECT markings FROM moria.native_storage_namespaces WHERE environment=$1 AND resource=$2 AND purpose='index-artifact' AND enabled AND warehouse_id=$3 AND table_id=$4 AND revision=$5 FOR SHARE",
            &[&environment,&target.to_string(),&namespace.warehouse_id,&namespace.table_id,&namespace.revision]).await?.ok_or_else(||anyhow::anyhow!("STORAGE_NAMESPACE_REVOKED"))?;
        let mut actual: Vec<String> = namespace_row.get(0);
        actual.sort();
        actual.dedup();
        ensure!(actual == markings, "MARKING_DOWNGRADE");
        let binding = configured_binding_fingerprint(namespace)?;
        definition["artifactNamespace"] = serde_json::to_value(namespace)?;
        definition["artifactBindingFingerprint"] = json!(binding);
    }
    Ok(definition)
}
async fn check_previous_in(
    client: &Client,
    environment: &str,
    target: Uuid,
    source: Uuid,
    definition: &Value,
    expected: Option<&str>,
) -> Result<Option<tokio_postgres::Row>> {
    let previous=client.query_opt("SELECT revision,definition,context,security_epoch,instance_epoch,enabled,source,target_id FROM moria.indexing_bindings WHERE environment=$1 AND target=$2 FOR UPDATE",
        &[&environment,&target.to_string()]).await?;
    if let Some(row) = &previous {
        ensure!(
            row.get::<_, Uuid>("target_id") == target,
            "BINDING_CONFLICT"
        );
        if let Some(expected) = expected {
            ensure!(
                row.get::<_, String>("revision") == expected,
                "BINDING_CHANGED"
            );
        } else {
            ensure!(
                &row.get::<_, Value>("definition") == definition
                    && row.get::<_, String>("source") == source.to_string(),
                "BINDING_CONFLICT"
            );
        }
    } else {
        ensure!(expected.is_none(), "BINDING_CHANGED");
    }
    Ok(previous)
}
async fn register_definition_in(
    client: &Client,
    environment: &str,
    target: Uuid,
    source: Uuid,
    service_revision: &str,
    definition: Value,
    expected: Option<&str>,
) -> Result<IndexingRegistration> {
    let previous =
        check_previous_in(client, environment, target, source, &definition, expected).await?;
    let service = RegisteredService::load_current_in(
        client,
        environment,
        target,
        "indexing",
        service_revision,
    )
    .await?;
    let markings: Vec<String> = serde_json::from_value(definition["markings"].clone())?;
    ensure!(
        markings.len() <= 256 && definition["targetId"] == target.to_string(),
        "INDEXING_INVALID"
    );
    client
        .query_one(
            "SELECT moria.native_indexing_service_admit($1,$2,$3,$4,$5,$6,$7)",
            &[
                &environment,
                &target,
                &source,
                &service.id(),
                &service.binding_revision(),
                &service.resource_class(),
                &markings,
            ],
        )
        .await?;
    let state = service.state_in(client).await?;
    if let Some(previous) = &previous {
        let context: Value = previous.get("context");
        if previous.get::<_, bool>("enabled")
            && previous.get::<_, Value>("definition") == definition
            && previous.get::<_, String>("source") == source.to_string()
            && previous.get::<_, i64>("security_epoch").to_string() == state.security_epoch
            && previous.get::<_, i64>("instance_epoch").to_string() == state.instance_epoch
            && context["serviceAuthority"] == service.id().to_string()
            && context["serviceTargetRevision"] == service_revision
            && let Some(binding) = context["executionBinding"]
                .as_str()
                .and_then(|v| v.parse::<Uuid>().ok())
        {
            let valid=client.query_opt("SELECT 1 FROM moria.native_indexing_service_definitions WHERE binding=$1 AND environment=$2 AND target=$3 AND source=$4 AND definition=$5",
                    &[&binding,&environment,&target.to_string(),&source.to_string(),&definition]).await?;
            if valid.is_some() {
                let build = enqueue(client, environment, target, source).await?;
                return Ok(IndexingRegistration {
                    revision: previous.get("revision"),
                    metadata_binding: binding,
                    build,
                });
            }
        }
    }
    let payload = json!({"version":1,"environment":environment,"target":target,"targetId":target,"source":source,"definition":definition});
    let canonical = canonical_json(&payload)?;
    ensure!(canonical.len() <= 1024 * 1024, "RESOURCE_LIMIT");
    let hash = format!("{:x}", Sha256::digest(canonical.as_bytes()));
    let spec = json!({"version":1,"domain":"indexing","release":hash,"resourceClass":service.resource_class(),"timeoutSeconds":1800,
        "retryPolicy":"idempotent","inputSchema":{"type":"object","properties":{}},"pinnedInputs":[],"expectedHeads":{}});
    let mut operations = vec![format!("{source}:read"), format!("{target}:index")];
    if state
        .identity
        .granted_operations
        .iter()
        .any(|v| v == "filesystem:read")
        && state
            .identity
            .application_scopes
            .iter()
            .any(|v| v == "filesystem:read")
    {
        operations.push("filesystem:read".into());
    }
    operations.sort();
    let metadata = service
        .bind_background_in(client, &spec, &operations, &markings)
        .await?;
    client.execute("INSERT INTO moria.native_indexing_service_definitions(binding,environment,target,target_id,source,definition,canonical_definition,definition_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
        &[&metadata.id,&environment,&target.to_string(),&target,&source.to_string(),&definition,&canonical,&hash]).await?;
    let context = json!({"environment":environment,"principal":service.principal(),"application":service.application(),
        "executionBinding":metadata.id,"serviceAuthority":service.id(),"serviceTargetRevision":service.binding_revision()});
    let revision = service_binding_revision(
        &hash,
        metadata.id,
        &metadata.security_epoch,
        &metadata.instance_epoch,
    )?;
    client.execute("INSERT INTO moria.indexing_bindings(environment,target,target_id,source,revision,definition,context,security_epoch,instance_epoch) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(environment,target) DO UPDATE SET source=EXCLUDED.source,revision=EXCLUDED.revision,definition=EXCLUDED.definition,context=EXCLUDED.context,security_epoch=EXCLUDED.security_epoch,instance_epoch=EXCLUDED.instance_epoch,enabled=true WHERE moria.indexing_bindings.target_id=EXCLUDED.target_id",
        &[&environment,&target.to_string(),&target,&source.to_string(),&revision,&definition,&context,&metadata.security_epoch.parse::<i64>()?,&metadata.instance_epoch.parse::<i64>()?]).await?;
    client.execute("UPDATE moria.indexing_builds SET status='superseded' WHERE environment=$1 AND target=$2 AND binding_revision<>$3 AND status IN ('pending','planning','running','preparing','ready')",
        &[&environment,&target.to_string(),&revision]).await?;
    let build = enqueue(client, environment, target, source).await?;
    Ok(IndexingRegistration {
        revision,
        metadata_binding: metadata.id,
        build,
    })
}
fn service_binding_revision(
    hash: &str,
    metadata: Uuid,
    epoch: &str,
    instance_epoch: &str,
) -> Result<String> {
    ensure!(
        hash.len() == 64
            && hash
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            && [epoch, instance_epoch].iter().all(|v| !v.is_empty()
                && v.bytes().all(|b| b.is_ascii_digit())
                && (*v == "0" || !v.starts_with('0'))),
        "INDEXING_INVALID"
    );
    fingerprint(
        &json!({"definitionHash":hash,"metadataBinding":metadata,"securityEpoch":epoch,"instanceEpoch":instance_epoch}),
    )
}
async fn enqueue(
    client: &Client,
    environment: &str,
    target: Uuid,
    source: Uuid,
) -> Result<Option<Uuid>> {
    if let Some(head)=client.query_opt("SELECT h.revision,e.revision AS environment_revision FROM moria.dataset_heads h JOIN moria.environments e ON e.id=h.environment WHERE h.environment=$1 AND h.dataset=$2 AND h.metadata_location IS NOT NULL",
        &[&environment,&source.to_string()]).await? {
        client.query_one("SELECT moria.indexing_enqueue($1,$2,$3,$4)",&[&environment,&source.to_string(),&head.get::<_,i64>(0),&head.get::<_,i64>(1)]).await?;
        return Ok(client.query_opt("SELECT b.id FROM moria.indexing_builds b JOIN moria.indexing_bindings x ON x.environment=b.environment AND x.target=b.target AND x.revision=b.binding_revision WHERE b.environment=$1 AND b.target=$2 AND b.source_revision=$3",
            &[&environment,&target.to_string(),&head.get::<_,i64>(0)]).await?.map(|row|row.get(0)));
    }
    Ok(None)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ExistingRegistration {
    environment: String,
    target: Uuid,
    expected_revision: String,
    service_target_revision: String,
}
/// Trusted operator CLI accepts identifiers/revision CAS only, never source snapshots or paths.
pub async fn cli(db: Database, args: Vec<String>) -> Result<()> {
    ensure!(
        args.len() == 2 && args[0] == "register-existing",
        "INDEXING_INVALID"
    );
    ensure!(args[1].len() <= 4096, "RESOURCE_LIMIT");
    let request: ExistingRegistration = serde_json::from_str(&args[1])?;
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&format!("moria/environment/{}", request.environment)],
    )
    .await?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock_shared(hashtextextended('moria/filesystem',0))",
        &[],
    )
    .await?;
    tx.query_one(
        "SELECT 1 FROM moria.environments WHERE id=$1 FOR UPDATE",
        &[&request.environment],
    )
    .await?;
    let row=tx.query_opt("SELECT source,definition FROM moria.indexing_bindings WHERE environment=$1 AND target=$2 AND revision=$3 AND target_id=$4 AND enabled FOR UPDATE",
        &[&request.environment,&request.target.to_string(),&request.expected_revision,&request.target]).await?.ok_or_else(||anyhow::anyhow!("BINDING_CHANGED"))?;
    let source: Uuid = row
        .get::<_, String>(0)
        .parse()
        .map_err(|_| anyhow::anyhow!("UNSUPPORTED_DATASET_BINDING"))?;
    let old: Value = row.get(1);
    let columns:Value=tx.query_one("SELECT columns FROM moria.native_dataset_bindings WHERE environment=$1 AND resource=$2 AND enabled FOR SHARE",&[&request.environment,&source]).await?.get(0);
    let schema: ObjectSchema = serde_json::from_value(old["schema"].clone())?;
    let source_mapping = if old.get("sourceMapping").is_some() {
        serde_json::from_value(old["sourceMapping"].clone())?
    } else {
        schema
            .properties
            .keys()
            .map(|name| (name.clone(), name.clone()))
            .collect()
    };
    let input = IndexDefinitionInput {
        source,
        schema,
        source_columns: serde_json::from_value(columns)?,
        source_mapping,
        markings: serde_json::from_value(old["markings"].clone())?,
        partitions: serde_json::from_value(old["partitions"].clone())?,
        timeout_seconds: serde_json::from_value(old["timeoutSeconds"].clone())?,
        expected_revision: Some(request.expected_revision.clone()),
        artifact_namespace: old
            .get("artifactNamespace")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?,
    };
    let result = install_in(
        tx.client(),
        &request.environment,
        request.target,
        &request.service_target_revision,
        input,
    )
    .await?;
    tx.commit().await?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn service_revision_is_exact_ascii_canonical_and_binds_all_authority_parts() {
        let metadata = Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap();
        let hash = "a".repeat(64);
        let bytes = format!(
            "{{\"definitionHash\":\"{hash}\",\"instanceEpoch\":\"9007199254740993\",\"metadataBinding\":\"{metadata}\",\"securityEpoch\":\"42\"}}"
        );
        let expected = format!("sha256:{:x}", Sha256::digest(bytes));
        assert_eq!(
            service_binding_revision(&hash, metadata, "42", "9007199254740993").unwrap(),
            expected
        );
        assert_ne!(
            service_binding_revision(&hash, metadata, "43", "9007199254740993").unwrap(),
            expected
        );
        assert!(service_binding_revision(&hash, metadata, "042", "1").is_err());
        assert!(service_binding_revision(&hash.to_uppercase(), metadata, "42", "1").is_err());
    }
}
