use super::*;
use crate::authorization::{CompiledExpression, ExpressionTypes, ValueMap};
pub(super) async fn install_in(
    client: &Client,
    session: &VerifiedSession,
    release: &Value,
    resources: &[Value],
    profiles: &BTreeMap<String, Value>,
    all: &BTreeMap<String, &Value>,
) -> Result<()> {
    let root: Uuid = release["rootId"].as_str().unwrap().parse()?;
    for resource in resources
        .iter()
        .filter(|resource| resource["kind"] == "LinkType")
    {
        let reference = bindings::resource_reference(resource);
        let spec = &resource["spec"];
        let id = bindings::id_in(client, session.environment(), root, &reference).await?;
        let source = bindings::id_in(
            client,
            session.environment(),
            root,
            spec["sourceType"].as_str().unwrap(),
        )
        .await?;
        let target = bindings::id_in(
            client,
            session.environment(),
            root,
            spec["targetType"].as_str().unwrap(),
        )
        .await?;
        let mut definitions = vec![];
        for id in [source, target] {
            definitions.push(client.query_opt("SELECT definition FROM moria.native_serving_bindings WHERE environment=$1 AND resource=$2 AND enabled", &[&session.environment(),&id.to_string()]).await?.ok_or_else(||anyhow::anyhow!("SOURCE_RUNTIME_NOT_READY"))?.get::<_,Value>(0));
        }
        let attributes: ValueMap =
            serde_json::from_value(definitions[0]["policies"]["attributes"].clone())?;
        let read = bindings::policy(all, &spec["readPolicy"])?;
        if let Some(read) = &read {
            CompiledExpression::compile(
                read,
                ExpressionTypes {
                    attributes: attributes.clone(),
                    ..Default::default()
                },
            )?;
        }
        let floor = Authority::resource_access(client, session, &id.to_string(), "publish").await?;
        let profile = profiles
            .get(&reference)
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow::anyhow!("SOURCE_RUNTIME_PROFILE_INVALID"))?;
        ensure!(
            profile.len() == 1 && profile.contains_key("markings"),
            "SOURCE_RUNTIME_PROFILE_INVALID"
        );
        let markings: Vec<String> = serde_json::from_value(profile["markings"].clone())
            .map_err(|_| anyhow::anyhow!("SOURCE_RUNTIME_PROFILE_INVALID"))?;
        ensure!(
            floor.iter().all(|label| markings.contains(label)),
            "MARKING_DOWNGRADE"
        );
        for definition in &definitions {
            let labels: Vec<String> =
                serde_json::from_value(definition["policies"]["markings"].clone())?;
            ensure!(
                labels.iter().all(|label| markings.contains(label)),
                "MARKING_FLOW"
            );
        }
        super::retain_classification_in(client, session, id, &markings).await?;
        client
            .query_one(
                "SELECT moria.runtime_data_admit($1,$2,$3,'write')",
                &[
                    &session.session_hash(),
                    &vec![id.to_string()],
                    &vec![source.to_string(), target.to_string()],
                ],
            )
            .await?;
        if spec["mode"] == "mapped" {
            let mut descriptors = vec![];
            for (definition, property) in definitions
                .iter()
                .zip([&spec["sourceProperty"], &spec["targetProperty"]])
            {
                let property = property
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
                let descriptor = &definition["schema"]["properties"][property];
                ensure!(
                    descriptor.is_object() && descriptor["type"] != "array",
                    "SOURCE_INVALID"
                );
                let mut descriptor = descriptor.clone();
                descriptor["nullable"] = false.into();
                descriptors.push(descriptor);
            }
            ensure!(descriptors[0] == descriptors[1], "SOURCE_INVALID");
        }
        let definition = json!({"mode":spec["mode"],"cardinality":spec["cardinality"],"sourceProperty":spec["sourceProperty"].as_str().unwrap_or(""),"targetProperty":spec["targetProperty"],"readPolicy":read,"attributes":attributes,"markings":markings});
        let revision = fingerprint(&definition)?;
        client.execute("INSERT INTO moria.native_serving_links(environment,resource,revision,source_resource,target_resource,definition) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(environment,resource) DO UPDATE SET revision=EXCLUDED.revision,source_resource=EXCLUDED.source_resource,target_resource=EXCLUDED.target_resource,definition=EXCLUDED.definition,enabled=true", &[&session.environment(),&id.to_string(),&revision,&source.to_string(),&target.to_string(),&definition]).await?;
        if spec["mode"] == "actionManaged" {
            let cardinality = spec["cardinality"].as_str().unwrap();
            for column in if cardinality == "oneToOne" {
                vec!["source_uid", "target_uid"]
            } else if cardinality == "manyToOne" {
                vec!["source_uid"]
            } else {
                vec![]
            } {
                ensure!(!client.query_one(&format!("SELECT EXISTS(SELECT 1 FROM moria.runtime_link_edges_at($1,$2,(SELECT revision FROM moria.environments WHERE id=$1)) GROUP BY {column} HAVING count(*)>1)"), &[&session.environment(),&id.to_string()]).await?.get::<_,bool>(0),"LINK_CARDINALITY");
            }
        }
    }
    Ok(())
}
