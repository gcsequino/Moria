use crate::{
    authorization::{CompiledExpression, ExpressionTypes, ValueMap},
    canonical::fingerprint,
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn configuration(schema: &Value, configuration: &Value) -> Result<()> {
    let values: Value = serde_json::from_str(include_str!(
        "../../../packages/contracts/schemas/values.schema.json"
    ))?;
    let resources: Value = serde_json::from_str(include_str!(
        "../../../packages/contracts/schemas/resources.schema.json"
    ))?;
    let manifest: Value = serde_json::from_str(include_str!(
        "../../../packages/contracts/schemas/manifest.schema.json"
    ))?;
    let registry = jsonschema::Registry::new()
        .add(values["$id"].as_str().unwrap(), values.clone())?
        .add(resources["$id"].as_str().unwrap(), resources.clone())?
        .add(manifest["$id"].as_str().unwrap(), manifest.clone())?
        .prepare()?;
    let validator = jsonschema::options()
        .with_registry(&registry)
        .offline()
        .with_draft(jsonschema::Draft::Draft202012)
        .should_validate_formats(true)
        .build(schema)?;
    ensure!(validator.is_valid(configuration), "CONNECTOR_CONFIG");
    Ok(())
}

pub(super) fn reference(resource: &Value) -> Result<String> {
    Ok(format!(
        "{}/{}/{}",
        resource["metadata"]["namespace"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
        resource["kind"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
        resource["metadata"]["name"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?
    ))
}
fn fields(value: &Value) -> Result<ValueMap> {
    serde_json::from_value(value.clone()).map_err(|_| anyhow::anyhow!("SOURCE_INVALID"))
}
fn resolve<'a>(
    entries: &BTreeMap<String, &'a Value>,
    reference: &Value,
    kinds: &[&str],
) -> Result<&'a Value> {
    let resource = entries
        .get(
            reference
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
        )
        .copied()
        .ok_or_else(|| anyhow::anyhow!("UNRESOLVED_REFERENCE"))?;
    ensure!(
        kinds.is_empty() || kinds.contains(&resource["kind"].as_str().unwrap_or_default()),
        "REFERENCE_KIND"
    );
    Ok(resource)
}
fn policy(entries: &BTreeMap<String, &Value>, reference: &Value, expected: &str) -> Result<()> {
    if reference.is_null() {
        return Ok(());
    }
    ensure!(
        resolve(entries, reference, &["Policy"])?["spec"]["type"] == expected,
        "POLICY_INVALID"
    );
    Ok(())
}
fn expression(
    value: &Value,
    columns: &Value,
    parameters: &Value,
    attributes: &ValueMap,
) -> Result<()> {
    CompiledExpression::compile(
        value,
        ExpressionTypes {
            fields: fields(columns)?,
            parameters: fields(parameters)?,
            attributes: attributes.clone(),
        },
    )?;
    Ok(())
}
fn attached_policy(
    entries: &BTreeMap<String, &Value>,
    reference: &Value,
    columns: &Value,
    attributes: &ValueMap,
    attached: &mut BTreeSet<String>,
) -> Result<()> {
    if reference.is_null() {
        return Ok(());
    }
    let resource = resolve(entries, reference, &["Policy"])?;
    if let Some(expr) = resource["spec"].get("expression") {
        expression(expr, columns, &json!({}), attributes)?;
        attached.insert(reference.as_str().unwrap().to_owned());
    }
    Ok(())
}
pub(super) fn validate(manifest: &Value, attributes: &ValueMap) -> Result<()> {
    CompiledExpression::compile(
        &json!({"literal":true}),
        ExpressionTypes {
            attributes: attributes.clone(),
            ..Default::default()
        },
    )?;
    let manifest_schema: Value = serde_json::from_str(include_str!(
        "../../../packages/contracts/schemas/manifest.schema.json"
    ))?;
    let resource_schema: Value = serde_json::from_str(include_str!(
        "../../../packages/contracts/schemas/resources.schema.json"
    ))?;
    let value_schema: Value = serde_json::from_str(include_str!(
        "../../../packages/contracts/schemas/values.schema.json"
    ))?;
    let registry = jsonschema::Registry::new()
        .add(
            manifest_schema["$id"].as_str().unwrap(),
            manifest_schema.clone(),
        )?
        .add(
            resource_schema["$id"].as_str().unwrap(),
            resource_schema.clone(),
        )?
        .add(value_schema["$id"].as_str().unwrap(), value_schema.clone())?
        .prepare()?;
    let validator = jsonschema::options()
        .with_registry(&registry)
        .offline()
        .with_draft(jsonschema::Draft::Draft202012)
        .should_validate_formats(true)
        .build(&manifest_schema)?;
    ensure!(validator.is_valid(manifest), "SOURCE_INVALID");
    ensure!(
        manifest
            .get("dependencies")
            .is_none_or(|value| value.as_array().is_some_and(Vec::is_empty)),
        "DEPENDENCY_RESOLUTION_REQUIRED"
    );
    let resources = manifest["resources"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
    ensure!(resources.len() <= 10000, "RESOURCE_LIMIT");
    let mut entries = BTreeMap::new();
    for resource in resources {
        ensure!(
            entries.insert(reference(resource)?, resource).is_none(),
            "DUPLICATE_RESOURCE"
        );
    }
    let namespaces = resources
        .iter()
        .filter(|r| r["kind"] == "Namespace")
        .map(|r| r["metadata"]["name"].as_str().unwrap())
        .collect::<BTreeSet<_>>();
    let mut graph = BTreeMap::<String, Vec<String>>::new();
    let mut producers = BTreeMap::<String, String>::new();
    let mut attached = BTreeSet::new();
    for resource in resources {
        let kind = resource["kind"].as_str().unwrap();
        let spec = &resource["spec"];
        let key = reference(resource)?;
        ensure!(
            if kind == "Namespace" {
                resource["metadata"]["namespace"] == "_project"
            } else {
                namespaces.contains(resource["metadata"]["namespace"].as_str().unwrap())
            },
            "NAMESPACE_INVALID"
        );
        if let Some(markings) = resource["metadata"]["markings"].as_array() {
            for marking in markings {
                resolve(&entries, marking, &["Marking"])?;
            }
        }
        for property in ["columns", "properties", "inputs", "outputs"] {
            if let Some(map) = spec[property].as_object() {
                for descriptor in map.values() {
                    let normalized = crate::values::descriptor(descriptor, &Value::Null, 0)?;
                    if kind != "Function" {
                        let scalar = [
                            "string",
                            "boolean",
                            "int64",
                            "float64",
                            "decimal",
                            "date",
                            "timestamp",
                            "uid",
                        ];
                        ensure!(
                            (scalar.contains(&descriptor["type"].as_str().unwrap_or_default())
                                || descriptor["type"] == "array")
                                && (scalar
                                    .contains(&normalized["type"].as_str().unwrap_or_default())
                                    || (normalized["type"] == "array"
                                        && normalized["items"]["nullable"] == false
                                        && scalar.contains(
                                            &normalized["items"]["type"]
                                                .as_str()
                                                .unwrap_or_default()
                                        ))),
                            "TYPE_INVALID"
                        );
                    }
                }
            }
        }
        match kind {
            "Dataset" => {
                for name in spec["primaryKey"].as_array().into_iter().flatten() {
                    let descriptor = &spec["columns"][name.as_str().unwrap()];
                    ensure!(
                        !descriptor.is_null() && descriptor["nullable"] != true,
                        "PRIMARY_KEY_INVALID"
                    );
                }
                if spec["writeModes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|mode| mode == "upsert" || mode == "delete")
                {
                    ensure!(
                        spec["primaryKey"]
                            .as_array()
                            .is_some_and(|keys| !keys.is_empty()),
                        "PRIMARY_KEY_INVALID"
                    );
                }
            }
            "Transform" => {
                let inputs = spec["inputs"].as_array().unwrap();
                for input in inputs {
                    resolve(&entries, input, &["Dataset"])?;
                }
                for output in spec["outputs"].as_array().unwrap() {
                    resolve(&entries, output, &["Dataset"])?;
                    ensure!(
                        producers
                            .insert(output.as_str().unwrap().to_owned(), key.clone())
                            .is_none(),
                        "MULTIPLE_PRODUCERS"
                    );
                }
                graph.insert(
                    key.clone(),
                    inputs
                        .iter()
                        .map(|value| value.as_str().unwrap().to_owned())
                        .collect(),
                );
            }
            "RestrictedView" => {
                let source = resolve(&entries, &spec["dataset"], &["Dataset"])?;
                policy(&entries, &spec["policy"], "row")?;
                attached_policy(
                    &entries,
                    &spec["policy"],
                    &source["spec"]["columns"],
                    attributes,
                    &mut attached,
                )?;
            }
            "ObjectType" => {
                let pk = spec["primaryKey"].as_str().unwrap();
                ensure!(
                    spec["properties"][pk]["type"] == "uid"
                        && spec["properties"][pk]["nullable"] != true,
                    "PRIMARY_KEY_INVALID"
                );
                let mut source = resolve(
                    &entries,
                    &spec["source"]["resource"],
                    &["Dataset", "RestrictedView"],
                )?;
                if source["kind"] == "RestrictedView" {
                    source = resolve(&entries, &source["spec"]["dataset"], &["Dataset"])?;
                }
                let mapping = spec["source"]["mapping"].as_object().unwrap();
                let properties = spec["properties"].as_object().unwrap();
                ensure!(
                    mapping.keys().collect::<BTreeSet<_>>()
                        == properties.keys().collect::<BTreeSet<_>>(),
                    "MAPPING_INVALID"
                );
                for (property, column) in mapping {
                    ensure!(
                        source["spec"]["columns"].get(column.as_str().unwrap())
                            == properties.get(property),
                        "MAPPING_INVALID"
                    );
                }
                for (field, expected) in
                    [("readPolicy", "objectRead"), ("writePolicy", "objectWrite")]
                {
                    policy(&entries, &spec[field], expected)?;
                    attached_policy(
                        &entries,
                        &spec[field],
                        &spec["properties"],
                        attributes,
                        &mut attached,
                    )?;
                }
                if let Some(policies) = spec["propertyPolicies"].as_object() {
                    for (property, policies) in policies {
                        ensure!(properties.contains_key(property), "PROPERTY_UNKNOWN");
                        for (operation, value) in policies.as_object().unwrap() {
                            policy(
                                &entries,
                                value,
                                if operation == "read" {
                                    "propertyRead"
                                } else {
                                    "propertyWrite"
                                },
                            )?;
                            attached_policy(
                                &entries,
                                value,
                                &json!({}),
                                attributes,
                                &mut attached,
                            )?;
                        }
                    }
                }
            }
            "LinkType" => {
                let source = resolve(&entries, &spec["sourceType"], &["ObjectType"])?;
                let target = resolve(&entries, &spec["targetType"], &["ObjectType"])?;
                if spec["mode"] == "mapped" {
                    ensure!(spec["cardinality"] != "manyToMany", "LINK_INVALID");
                    let left =
                        &source["spec"]["properties"][spec["sourceProperty"].as_str().unwrap()];
                    let right =
                        &target["spec"]["properties"][spec["targetProperty"].as_str().unwrap()];
                    ensure!(
                        !left.is_null() && !right.is_null() && left["type"] == right["type"],
                        "LINK_INVALID"
                    );
                } else {
                    ensure!(
                        spec.get("sourceProperty").is_none()
                            && spec.get("targetProperty").is_none(),
                        "LINK_INVALID"
                    );
                }
                if !spec["readPolicy"].is_null() {
                    resolve(&entries, &spec["readPolicy"], &["Policy"])?;
                    attached_policy(
                        &entries,
                        &spec["readPolicy"],
                        &json!({}),
                        attributes,
                        &mut attached,
                    )?;
                }
            }
            "Action" => {
                let target = resolve(&entries, &spec["targetType"], &["ObjectType"])?;
                if let Some(parameter) = spec["targetParameter"].as_str() {
                    ensure!(spec["inputs"][parameter]["type"] == "uid", "ACTION_TARGET");
                } else {
                    ensure!(
                        spec.get("function").is_none()
                            && spec["edits"]
                                .as_array()
                                .into_iter()
                                .flatten()
                                .all(|edit| edit["op"] == "create"),
                        "ACTION_TARGET"
                    );
                }
                if let Some(function) = spec.get("function") {
                    let function = resolve(&entries, function, &["Function"])?;
                    ensure!(
                        function["spec"]["effect"] == "editProposal"
                            && fingerprint(&function["spec"]["inputs"])?
                                == fingerprint(&spec["inputs"])?,
                        "ACTION_FUNCTION"
                    );
                    for (property, descriptor) in function["spec"]["outputs"].as_object().unwrap() {
                        ensure!(
                            target["spec"]["primaryKey"] != property.as_str()
                                && target["spec"]["properties"].get(property) == Some(descriptor),
                            "ACTION_FUNCTION"
                        );
                    }
                }
                for edit in spec["edits"].as_array().into_iter().flatten() {
                    if !edit["linkType"].is_null() {
                        resolve(&entries, &edit["linkType"], &["LinkType"])?;
                    }
                    if !edit["targetType"].is_null() {
                        let edited = resolve(&entries, &edit["targetType"], &["ObjectType"])?;
                        if let Some(property) = edit["property"].as_str() {
                            ensure!(
                                edited["spec"]["properties"].get(property).is_some()
                                    && edited["spec"]["primaryKey"] != property,
                                "EDIT_INVALID"
                            );
                        }
                        if edit["op"] == "create" {
                            ensure!(
                                edit["values"]
                                    .get(edited["spec"]["primaryKey"].as_str().unwrap())
                                    .is_none(),
                                "EDIT_INVALID"
                            );
                        }
                    }
                }
                expression(
                    &spec["condition"],
                    &target["spec"]["properties"],
                    &spec["inputs"],
                    attributes,
                )?;
            }
            "Application" => {
                for scope in spec["scopes"].as_array().unwrap() {
                    resolve(&entries, &scope["target"], &[])?;
                }
                ensure!(
                    spec.get("frontend").is_none() || spec["mode"] == "browser",
                    "APPLICATION_MODE"
                );
            }
            "Connection" => {
                let implementation = resolve(
                    &entries,
                    &spec["implementation"],
                    &["ConnectorImplementation"],
                )?;
                let output = resolve(&entries, &spec["output"], &["Dataset"])?;
                for secret in spec["secrets"].as_array().unwrap() {
                    resolve(&entries, secret, &["SecretReference"])?;
                }
                ensure!(
                    output["spec"]["writeModes"]
                        .as_array()
                        .unwrap()
                        .contains(&spec["mode"]),
                    "WRITE_MODE"
                );
                configuration(
                    &implementation["spec"]["configurationSchema"],
                    &spec["configuration"],
                )?;
            }
            "ModelProvider" => {
                policy(&entries, &spec["destinationPolicy"], "modelDestination")?;
                if !spec["secret"].is_null() {
                    resolve(&entries, &spec["secret"], &["SecretReference"])?;
                }
            }
            "Model" => {
                resolve(&entries, &spec["provider"], &["ModelProvider"])?;
                ensure!(
                    !spec["capabilities"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|value| value == "embeddings")
                        || spec["embeddingDimensions"]
                            .as_u64()
                            .is_some_and(|value| value > 0),
                    "MODEL_INVALID"
                );
                ensure!(
                    spec["mutable"] == true
                        || spec["versionLabel"]
                            .as_str()
                            .is_some_and(|value| !value.is_empty()),
                    "MODEL_INVALID"
                );
            }
            "Agent" => {
                resolve(&entries, &spec["application"], &["Application"])?;
                resolve(&entries, &spec["adapter"], &["AgentAdapter"])?;
                resolve(&entries, &spec["model"], &["Model"])?;
            }
            "RetrievalCollection" => {
                let source = resolve(&entries, &spec["source"], &["ObjectType"])?;
                let model = resolve(&entries, &spec["embeddingModel"], &["Model"])?;
                ensure!(
                    source["spec"]["properties"][spec["textProperty"].as_str().unwrap()]["type"]
                        == "string"
                        && model["spec"]["capabilities"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|value| value == "embeddings")
                        && spec["chunkOverlap"].as_u64() < spec["chunkSize"].as_u64(),
                    "RETRIEVAL_INVALID"
                );
            }
            "EvaluationSuite" => {
                resolve(&entries, &spec["agent"], &["Agent"])?;
                resolve(&entries, &spec["fixtures"], &["Dataset"])?;
            }
            "Automation" => {
                resolve(&entries, &spec["application"], &["Application"])?;
                let expected = match spec["operation"].as_str().unwrap() {
                    "build" => "Transform",
                    "sync" => "Connection",
                    "invokeAgent" => "Agent",
                    "evaluate" => "EvaluationSuite",
                    _ => anyhow::bail!("SOURCE_INVALID"),
                };
                resolve(&entries, &spec["target"], &[expected])?;
            }
            "Grant" => {
                resolve(&entries, &spec["application"], &["Application"])?;
                resolve(&entries, &spec["target"], &[])?;
            }
            "SecretReference" => {
                for consumer in spec["consumers"].as_array().unwrap() {
                    resolve(&entries, consumer, &[])?;
                }
            }
            "ObjectSet" => {
                resolve(&entries, &spec["objectType"], &["ObjectType"])?;
            }
            "Policy" if spec["type"] == "modelDestination" => {
                for marking in spec["allowedMarkings"].as_array().unwrap() {
                    resolve(&entries, marking, &["Marking"])?;
                }
            }
            _ => {}
        }
    }
    fn visit(
        node: &str,
        graph: &BTreeMap<String, Vec<String>>,
        producers: &BTreeMap<String, String>,
        active: &mut BTreeSet<String>,
        done: &mut BTreeSet<String>,
    ) -> Result<()> {
        if done.contains(node) {
            return Ok(());
        }
        ensure!(active.len() < 64, "RESOURCE_LIMIT");
        ensure!(active.insert(node.to_owned()), "TRANSFORM_CYCLE");
        if let Some(inputs) = graph.get(node) {
            for input in inputs {
                if let Some(producer) = producers.get(input) {
                    visit(producer, graph, producers, active, done)?;
                }
            }
        }
        active.remove(node);
        done.insert(node.to_owned());
        Ok(())
    }
    let mut done = BTreeSet::new();
    for node in graph.keys() {
        visit(node, &graph, &producers, &mut BTreeSet::new(), &mut done)?;
    }
    for resource in resources {
        if resource["kind"] == "Policy"
            && resource["spec"].get("expression").is_some()
            && !attached.contains(&reference(resource)?)
        {
            ensure!(
                ["propertyRead", "propertyWrite"]
                    .contains(&resource["spec"]["type"].as_str().unwrap()),
                "POLICY_CONTEXT_REQUIRED"
            );
            expression(
                &resource["spec"]["expression"],
                &json!({}),
                &json!({}),
                attributes,
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dataset_only_scalar_arrays_match_the_shared_column_profile() {
        let mut value = manifest();
        value["resources"].as_array_mut().unwrap().pop();
        value["resources"][1]["spec"]["columns"]["tags"] =
            json!({"type":"array","nullable":false,"items":{"type":"string","nullable":false}});
        validate(&value, &ValueMap::new()).unwrap();
        value["resources"][1]["spec"]["columns"]["tags"]["items"]["nullable"] = json!(true);
        assert!(validate(&value, &ValueMap::new()).is_err());
        value["resources"][1]["spec"]["columns"]["tags"]["items"] =
            json!({"type":"array","nullable":false,"items":{"type":"string","nullable":false}});
        assert!(validate(&value, &ValueMap::new()).is_err());
    }
    #[test]
    fn object_properties_and_action_inputs_accept_scalar_arrays_too() {
        let mut value = manifest();
        let array =
            json!({"type":"array","nullable":false,"items":{"type":"string","nullable":false}});
        value["resources"][1]["spec"]["columns"]["tags"] = array.clone();
        value["resources"][2]["spec"]["properties"]["tags"] = array.clone();
        value["resources"][2]["spec"]["source"]["mapping"]["tags"] = json!("tags");
        value["resources"].as_array_mut().unwrap().push(json!({"apiVersion":"moria/v1alpha2","kind":"Action","metadata":{"namespace":"sample","name":"tags"},"spec":{"inputs":{"uid":{"type":"uid","nullable":false},"tags":array},"targetType":"sample/ObjectType/Station","targetParameter":"uid","condition":{"literal":true},"edits":[{"op":"set","targetType":"sample/ObjectType/Station","uid":{"parameter":"uid"},"property":"tags","value":{"parameter":"tags"}}]}}));
        validate(&value, &ValueMap::new()).unwrap();
        value["resources"][3]["spec"]["inputs"]["tags"]["items"]["nullable"] = json!(true);
        assert!(validate(&value, &ValueMap::new()).is_err());
    }
    #[test]
    fn compiler_acceptance_matches_all_kind_reference_and_rejections() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../packages/contracts/compiler-conformance.json"
        ))
        .unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let attributes: ValueMap = serde_json::from_value(case["attributes"].clone()).unwrap();
            let result = validate(&case["manifest"], &attributes);
            assert_eq!(
                result.is_ok(),
                case["valid"].as_bool().unwrap(),
                "{}: {:?}",
                case["name"],
                result
            );
        }
    }
    fn manifest() -> Value {
        json!({"apiVersion":"moria/v1alpha2","projectId":"22222222-2222-4222-8222-222222222222","deploymentUnitId":"33333333-3333-4333-8333-333333333333","resources":[{"apiVersion":"moria/v1alpha2","kind":"Namespace","metadata":{"namespace":"_project","name":"sample"},"spec":{"description":"Sample"}},{"apiVersion":"moria/v1alpha2","kind":"Dataset","metadata":{"namespace":"sample","name":"stations"},"spec":{"columns":{"id":{"type":"uid","nullable":false}},"writeModes":["replace"]}},{"apiVersion":"moria/v1alpha2","kind":"ObjectType","metadata":{"namespace":"sample","name":"Station"},"spec":{"primaryKey":"id","properties":{"id":{"type":"uid","nullable":false}},"source":{"resource":"sample/Dataset/stations","mapping":{"id":"id"}}}}]})
    }
    #[test]
    fn trusted_schemas_and_semantic_references_are_independently_checked() {
        let value = manifest();
        validate(&value, &ValueMap::new()).unwrap();
        let mut changed = value.clone();
        changed["resources"][2]["spec"]["source"]["resource"] = json!("sample/Dataset/missing");
        assert!(validate(&changed, &ValueMap::new()).is_err());
        let mut changed = value.clone();
        changed["resources"][2]["spec"]["properties"]["id"]["type"] = json!("string");
        assert!(validate(&changed, &ValueMap::new()).is_err());
        let mut changed = value;
        changed["resources"][0]["surprise"] = json!(true);
        assert!(validate(&changed, &ValueMap::new()).is_err());
    }
}
