use super::*;
use crate::authorization::{
    CompiledExpression, CompiledValueExpression, ExpressionTypes, ValueMap,
};

pub(super) async fn install_in(
    client: &Client,
    session: &VerifiedSession,
    release: &Value,
    resources: &[Value],
    all: &BTreeMap<String, &Value>,
) -> Result<()> {
    let root: Uuid = release["rootId"].as_str().unwrap().parse()?;
    for resource in resources
        .iter()
        .filter(|resource| resource["kind"] == "Action")
    {
        let reference = bindings::resource_reference(resource);
        let id = bindings::id_in(client, session.environment(), root, &reference).await?;
        let spec = &resource["spec"];
        let target = bindings::id_in(
            client,
            session.environment(),
            root,
            spec["targetType"].as_str().unwrap(),
        )
        .await?;
        let target_definition:Value=client.query_opt("SELECT definition FROM moria.native_serving_bindings WHERE environment=$1 AND resource=$2 AND enabled", &[&session.environment(),&target.to_string()]).await?.ok_or_else(||anyhow::anyhow!("SOURCE_RUNTIME_NOT_READY"))?.get(0);
        let types = ExpressionTypes {
            fields: serde_json::from_value(target_definition["schema"]["properties"].clone())?,
            parameters: serde_json::from_value(spec["inputs"].clone())?,
            attributes: serde_json::from_value(
                target_definition["policies"]["attributes"].clone(),
            )?,
        };
        CompiledExpression::compile(&spec["condition"], types.clone())?;
        let markings =
            Authority::resource_access(client, session, &id.to_string(), "publish").await?;
        let mut definition = json!({"resource":id,"markings":markings,"targetParameter":spec["targetParameter"].as_str().unwrap_or(""),"inputs":spec["inputs"],"condition":spec["condition"],"edits":[]});
        if let Some(function) = spec["function"].as_str() {
            let function_resource = all
                .get(function)
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
            ensure!(
                function_resource["kind"] == "Function"
                    && function_resource["spec"]["effect"] == "editProposal",
                "SOURCE_INVALID"
            );
            definition["function"] =
                json!(bindings::id_in(client, session.environment(), root, function).await?);
        } else {
            let mut edits = Vec::new();
            for edit in spec["edits"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?
            {
                if matches!(edit["op"].as_str(), Some("link" | "unlink")) {
                    let reference = edit["linkType"].as_str().unwrap();
                    let link = all
                        .get(reference)
                        .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
                    ensure!(
                        link["kind"] == "LinkType" && link["spec"]["mode"] == "actionManaged",
                        "SOURCE_INVALID"
                    );
                    for expression in [&edit["sourceUid"], &edit["targetUid"]] {
                        CompiledValueExpression::compile(
                            expression,
                            types.clone(),
                            &json!({"type":"uid","nullable":false}),
                        )?;
                    }
                    let mut edit = edit.clone();
                    edit["linkType"] = json!(
                        bindings::id_in(client, session.environment(), root, reference).await?
                    );
                    edits.push(edit);
                    continue;
                }
                let target_id = bindings::id_in(
                    client,
                    session.environment(),
                    root,
                    edit["targetType"].as_str().unwrap(),
                )
                .await?;
                let binding:Value=client.query_opt("SELECT definition FROM moria.native_serving_bindings WHERE environment=$1 AND resource=$2 AND enabled", &[&session.environment(),&target_id.to_string()]).await?.ok_or_else(||anyhow::anyhow!("SOURCE_RUNTIME_NOT_READY"))?.get(0);
                let properties: ValueMap =
                    serde_json::from_value(binding["schema"]["properties"].clone())?;
                let mut edit = edit.clone();
                edit["targetType"] = json!(target_id);
                if edit["op"] == "create" {
                    CompiledValueExpression::compile_inferred(&edit["identityKey"], types.clone())?;
                    for (property, expression) in edit["values"].as_object().unwrap() {
                        ensure!(
                            binding["schema"]["primaryKey"] != property.as_str(),
                            "SOURCE_INVALID"
                        );
                        CompiledValueExpression::compile(
                            expression,
                            types.clone(),
                            properties
                                .get(property)
                                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
                        )?;
                    }
                    ensure!(
                        binding["identityDomain"].is_string(),
                        "ACTION_CREATION_UNAVAILABLE"
                    );
                } else {
                    CompiledValueExpression::compile(
                        &edit["uid"],
                        types.clone(),
                        &json!({"type":"uid","nullable":false}),
                    )?;
                    if let Some(property) = edit["property"].as_str() {
                        ensure!(
                            binding["schema"]["primaryKey"] != property,
                            "SOURCE_INVALID"
                        );
                        let descriptor = properties
                            .get(property)
                            .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
                        if edit["op"] == "set" {
                            CompiledValueExpression::compile(
                                &edit["value"],
                                types.clone(),
                                descriptor,
                            )?;
                        }
                    }
                }
                edits.push(edit);
            }
            definition["typedEdits"] = json!(edits);
        }
        let revision = fingerprint(&definition)?;
        definition["revision"] = revision.clone().into();
        client.execute("INSERT INTO moria.native_serving_actions(environment,resource,object_resource,revision,definition) VALUES($1,$2,$3,$4,$5) ON CONFLICT(environment,resource) DO UPDATE SET object_resource=EXCLUDED.object_resource,revision=EXCLUDED.revision,definition=EXCLUDED.definition,enabled=true", &[&session.environment(),&id.to_string(),&target.to_string(),&revision,&definition]).await?;
    }
    Ok(())
}
