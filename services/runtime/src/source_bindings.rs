use super::*;
use crate::{
    authorization::{ObjectPolicies, ObjectReader, ValueMap},
    indexing_service::{IndexDefinitionInput, admit_in, install_in},
    projection,
    serving::ObjectSchema,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ObjectProfile {
    projection: projection::Definition,
    artifact_namespace: IndexNamespace,
    markings: Vec<String>,
    partitions: u32,
    timeout_seconds: u32,
    service_binding_revision: String,
    identity_domain: Option<String>,
}
pub(super) async fn id_in(
    client: &Client,
    environment: &str,
    root: Uuid,
    reference: &str,
) -> Result<Uuid> {
    if let Some(row)=client.query_opt("SELECT resource_id FROM moria.fs_resource_identities WHERE root_id=$1 AND reference=$2", &[&root,&reference]).await? {return Ok(row.get(0));}
    let rows=client.query("SELECT e.resource_id FROM moria.fs_resource_bindings b JOIN moria.fs_entries e ON e.id=b.entry_id AND NOT e.deleted WHERE b.environment=$1 AND b.reference=$2 AND b.active LIMIT 2", &[&environment,&reference]).await?;
    ensure!(rows.len() == 1, "SOURCE_INVALID");
    Ok(rows[0].get(0))
}
pub(super) fn policy(all: &BTreeMap<String, &Value>, reference: &Value) -> Result<Option<Value>> {
    if reference.is_null() {
        return Ok(None);
    }
    let resource = all
        .get(
            reference
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
    ensure!(resource["kind"] == "Policy", "SOURCE_INVALID");
    Ok(Some(resource["spec"]["expression"].clone()))
}
pub(super) fn resource_reference(resource: &Value) -> String {
    format!(
        "{}/{}/{}",
        resource["metadata"]["namespace"].as_str().unwrap(),
        resource["kind"].as_str().unwrap(),
        resource["metadata"]["name"].as_str().unwrap()
    )
}

pub(super) async fn install_objects_in(
    client: &Client,
    session: &VerifiedSession,
    release: &Value,
    resources: &[Value],
    profiles: &BTreeMap<String, Value>,
    all: &BTreeMap<String, &Value>,
) -> Result<Vec<(Uuid, String, IndexDefinitionInput)>> {
    let root: Uuid = release["rootId"].as_str().unwrap().parse()?;
    let registry=client.query_opt("SELECT c.principal_attributes FROM moria.native_source_compiler_current x JOIN moria.native_source_compiler_contexts c USING(environment,fingerprint) WHERE x.environment=$1", &[&session.environment()]).await?;
    let attributes: ValueMap = serde_json::from_value(
        registry
            .ok_or_else(|| anyhow::anyhow!("SOURCE_RUNTIME_NOT_READY"))?
            .get(0),
    )?;
    let mut pending = Vec::<(Uuid, String, IndexDefinitionInput)>::new();
    for resource in resources
        .iter()
        .filter(|resource| resource["kind"] == "ObjectType")
    {
        let reference = resource_reference(resource);
        let id = id_in(client, session.environment(), root, &reference).await?;
        let profile: ObjectProfile = serde_json::from_value(
            profiles
                .get(&reference)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_RUNTIME_PROFILE_INVALID"))?,
        )
        .map_err(|_| anyhow::anyhow!("SOURCE_RUNTIME_PROFILE_INVALID"))?;
        let spec = &resource["spec"];
        let source_reference = spec["source"]["resource"].as_str().unwrap();
        let source = all
            .get(source_reference)
            .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
        let (dataset, row) = if source["kind"] == "RestrictedView" {
            (
                all.get(source["spec"]["dataset"].as_str().unwrap())
                    .copied()
                    .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
                policy(all, &source["spec"]["policy"])?,
            )
        } else {
            (*source, None)
        };
        ensure!(dataset["kind"] == "Dataset", "SOURCE_INVALID");
        let dataset_id = id_in(
            client,
            session.environment(),
            root,
            &resource_reference(dataset),
        )
        .await?;
        let source_id = id_in(client, session.environment(), root, source_reference).await?;
        let properties: ValueMap = serde_json::from_value(spec["properties"].clone())?;
        let source_columns: ValueMap = serde_json::from_value(dataset["spec"]["columns"].clone())?;
        let source_mapping: BTreeMap<String, String> =
            serde_json::from_value(spec["source"]["mapping"].clone())?;
        let schema = ObjectSchema {
            primary_key: spec["primaryKey"].as_str().unwrap().into(),
            properties: properties.clone(),
        };
        let schema_value = json!({"primaryKey":schema.primary_key,"properties":schema.properties});
        profile
            .projection
            .validate_source(&schema_value, &json!(source_columns))?;
        let mut markings = profile.markings.clone();
        markings.sort();
        markings.dedup();
        ensure!(markings.len() <= 256, "MARKING_DOWNGRADE");
        let floor = Authority::resource_access(client, session, &id.to_string(), "publish").await?;
        ensure!(
            floor.iter().all(|marking| markings.contains(marking)),
            "MARKING_DOWNGRADE"
        );
        let mut property_read = ValueMap::new();
        let mut property_write = ValueMap::new();
        if let Some(properties) = spec["propertyPolicies"].as_object() {
            for (name, value) in properties {
                if let Some(expression) = policy(all, &value["read"])? {
                    property_read.insert(name.clone(), expression);
                }
                if let Some(expression) = policy(all, &value["write"])? {
                    property_write.insert(name.clone(), expression);
                }
            }
        }
        let policies = ObjectPolicies {
            resource: id.to_string(),
            markings: markings.clone(),
            properties: properties.clone(),
            source_columns: source_columns.clone(),
            attributes: attributes.clone(),
            row,
            object_read: policy(all, &spec["readPolicy"])?,
            property_read,
        };
        ObjectReader::compile(policies.clone())?;
        if let Some(domain) = &profile.identity_domain {
            ensure!(
                !domain.is_empty() && domain.len() <= 1024,
                "SOURCE_RUNTIME_PROFILE_INVALID"
            );
            client
                .query_one(
                    "SELECT moria.runtime_environment_managed_check($1)",
                    &[&session.environment()],
                )
                .await?;
            ensure!(client.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_identity_domains d JOIN moria.runtime_placements p ON p.environment=$1 AND p.cell=d.cell JOIN moria.indexing_instance i ON i.incarnation=d.incarnation WHERE d.domain=$2)", &[&session.environment(),&domain]).await?.get::<_,bool>(0),"OWNERSHIP_LOST");
            client.query_one("SELECT pg_advisory_xact_lock(hashtextextended('moria/identity-domain/'||$1,0))", &[domain]).await?;
            let reserved=client.query_opt("SELECT target FROM moria.native_action_identity_domains WHERE domain=$1 FOR UPDATE", &[domain]).await?;
            if let Some(reserved) = reserved {
                ensure!(
                    reserved.get::<_, Uuid>(0) == id,
                    "SOURCE_RUNTIME_PROFILE_INVALID"
                );
            } else {
                ensure!(
                    !client
                        .query_one(
                            "SELECT EXISTS(SELECT 1 FROM moria.identities WHERE domain=$1)",
                            &[domain]
                        )
                        .await?
                        .get::<_, bool>(0),
                    "IDENTITY_DOMAIN_NOT_EMPTY"
                );
                client.execute("INSERT INTO moria.native_action_identity_domains(domain,target) VALUES($1,$2)", &[domain,&id]).await?;
            }
        }
        let binding = json!({"objectTypeId":id,"schema":schema_value,"policies":policies,"requiredOperations":[{"resource":source_id,"operation":"read"}],"writePolicy":policy(all,&spec["writePolicy"])?,"propertyWrite":property_write,"sourceResource":dataset_id,"sourceMapping":source_mapping,"identityDomain":profile.identity_domain});
        let revision = fingerprint(&binding)?;
        client.execute("INSERT INTO moria.native_serving_bindings(environment,resource,object_type_id,revision,definition) VALUES($1,$2,$3,$4,$5) ON CONFLICT(environment,resource) DO UPDATE SET revision=EXCLUDED.revision,definition=EXCLUDED.definition,enabled=true", &[&session.environment(),&id.to_string(),&id,&revision,&binding]).await?;
        let layout_revision = fingerprint(&json!([
            release["digest"],
            profile.projection,
            profile.artifact_namespace
        ]))?;
        client.execute("INSERT INTO moria.runtime_projection_bindings(environment,resource,revision,definition) VALUES($1,$2,$3,$4) ON CONFLICT(environment,resource) DO UPDATE SET revision=EXCLUDED.revision,definition=EXCLUDED.definition,enabled=true", &[&session.environment(),&id.to_string(),&layout_revision,&serde_json::to_value(&profile.projection)?]).await?;
        configured_binding_fingerprint(&profile.artifact_namespace)?;
        if let Some(row)=client.query_opt("SELECT environment,resource,purpose,markings FROM moria.native_storage_namespaces WHERE warehouse_id=$1 AND table_id=$2 FOR UPDATE", &[&profile.artifact_namespace.warehouse_id,&profile.artifact_namespace.table_id]).await? {
            ensure!(row.get::<_,String>(0)==session.environment()&&row.get::<_,String>(1)==id.to_string()&&row.get::<_,String>(2)=="index-artifact"&&row.get::<_,Vec<String>>(3).iter().all(|marking|markings.contains(marking)),"STORAGE_NAMESPACE_REVOKED");
        }
        client.execute("INSERT INTO moria.native_storage_namespaces(environment,warehouse_id,table_id,resource,revision,purpose,markings) VALUES($1,$2,$3,$4,$5,'index-artifact',$6) ON CONFLICT(warehouse_id,table_id) DO UPDATE SET revision=EXCLUDED.revision,markings=EXCLUDED.markings,enabled=true", &[&session.environment(),&profile.artifact_namespace.warehouse_id,&profile.artifact_namespace.table_id,&id.to_string(),&profile.artifact_namespace.revision,&markings]).await?;
        let expected=client.query_opt("SELECT revision FROM moria.indexing_bindings WHERE environment=$1 AND target=$2 FOR UPDATE", &[&session.environment(),&id.to_string()]).await?.map(|row|row.get(0));
        pending.push((
            id,
            profile.service_binding_revision,
            IndexDefinitionInput {
                source: dataset_id,
                schema,
                source_columns,
                source_mapping,
                markings,
                partitions: profile.partitions,
                timeout_seconds: profile.timeout_seconds,
                expected_revision: expected,
                artifact_namespace: Some(profile.artifact_namespace),
            },
        ));
    }
    super::links::install_in(client, session, release, resources, profiles, all).await?;
    super::actions::install_in(client, session, release, resources, all).await?;
    for (target, revision, input) in &pending {
        admit_in(client, session.environment(), *target, revision, input).await?;
    }
    Ok(pending)
}
pub(super) async fn finish_objects_in(
    client: &Client,
    session: &VerifiedSession,
    pending: Vec<(Uuid, String, IndexDefinitionInput)>,
) -> Result<()> {
    for (target, revision, input) in pending {
        install_in(client, session.environment(), target, &revision, input).await?;
    }
    Ok(())
}
