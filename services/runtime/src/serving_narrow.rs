use super::*;

fn policy_fields(policy: &ObjectPolicies, source: &mut Vec<String>, effective: &mut Vec<String>) {
    if let Some(expression) = &policy.row {
        query_fields(expression, source);
    }
    if let Some(expression) = &policy.object_read {
        query_fields(expression, effective);
    }
}
fn column_json(expression: &str, schema: &Value) -> String {
    if schema["type"] == "array" {
        let item = scalar_json("cell.value", &schema["items"]);
        format!(
            "(CASE WHEN {expression} IS NULL THEN NULL ELSE COALESCE((SELECT jsonb_agg({item} ORDER BY cell.position) FROM unnest({expression}) WITH ORDINALITY AS cell(value,position)),'[]'::jsonb) END)"
        )
    } else {
        scalar_json(expression, schema)
    }
}
struct MapInputs<'a> {
    relation: &'a crate::projection::ProjectionRelation,
    alias: &'a str,
    source: bool,
    fields: &'a BTreeSet<String>,
}
fn object_map(
    input: MapInputs<'_>,
    parameters: &mut Vec<SqlParameter>,
    keys: &mut BTreeMap<String, String>,
) -> Result<String> {
    if input.fields.is_empty() {
        return Ok("'{}'::jsonb".into());
    }
    let mut parts = Vec::new();
    let fields = input.fields.iter().collect::<Vec<_>>();
    for chunk in fields.chunks(50) {
        let mut arguments = Vec::new();
        for field in chunk {
            let key = if let Some(key) = keys.get(*field) {
                key.clone()
            } else {
                let key = bind(parameters, Some((*field).clone()), "text");
                keys.insert((*field).clone(), key.clone());
                key
            };
            let columns = if input.source {
                &input.relation.source_columns
            } else {
                &input.relation.columns
            };
            let value = if let Some(column) = columns.get(*field) {
                let physical = if input.source {
                    column.source.as_ref()
                } else {
                    column.effective.as_ref()
                };
                if let Some(physical) = physical {
                    column_json(
                        &format!("{}.\"{}\"", input.alias, physical),
                        &column.descriptor,
                    )
                } else {
                    format!(
                        "{}.{}->{key}",
                        input.alias,
                        if input.source { "source" } else { "effective" }
                    )
                }
            } else {
                format!(
                    "{}.{}->{key}",
                    input.alias,
                    if input.source { "source" } else { "effective" }
                )
            };
            arguments.extend([key, value]);
        }
        parts.push(format!("jsonb_build_object({})", arguments.join(",")));
    }
    Ok(format!("({})", parts.join(" || ")))
}
pub(super) fn maps(
    binding: &Binding,
    query: &Query,
    plan: &crate::projection::ProjectionPlan,
    captured: [Option<&BindingDefinition>; 2],
    current_alias: &str,
    parameters: &mut Vec<SqlParameter>,
) -> Result<[String; 4]> {
    let mut source = Vec::new();
    let mut effective = Vec::new();
    policy_fields(&binding.definition.policies, &mut source, &mut effective);
    for definition in captured.into_iter().flatten() {
        policy_fields(&definition.policies, &mut source, &mut effective);
    }
    let source = source.into_iter().collect::<BTreeSet<_>>();
    let current_effective = effective.into_iter().collect::<BTreeSet<_>>();
    let mut selected = current_effective.clone();
    if let Some(projection) = &query.projection {
        selected.extend(projection.iter().cloned());
    }
    let mut fields = Vec::new();
    if let Some(filter) = &query.filter {
        query_fields(filter, &mut fields);
    }
    fields.extend(query.sort.iter().map(|key| key.property.clone()));
    if let Some(aggregation) = &query.aggregation {
        fields.extend(aggregation.group_by.iter().cloned());
        fields.extend(
            aggregation
                .metrics
                .values()
                .filter_map(|metric| metric.property.clone()),
        );
    }
    selected.extend(fields);
    let mut keys = BTreeMap::new();
    let pinned_source = object_map(
        MapInputs {
            relation: &plan.pinned,
            alias: "p",
            source: true,
            fields: &source,
        },
        parameters,
        &mut keys,
    )?;
    let pinned_effective = if query.projection.is_some() {
        object_map(
            MapInputs {
                relation: &plan.pinned,
                alias: "p",
                source: false,
                fields: &selected,
            },
            parameters,
            &mut keys,
        )?
    } else {
        "p.effective".into()
    };
    let current_source = object_map(
        MapInputs {
            relation: &plan.current,
            alias: current_alias,
            source: true,
            fields: &source,
        },
        parameters,
        &mut keys,
    )?;
    let current_effective = object_map(
        MapInputs {
            relation: &plan.current,
            alias: current_alias,
            source: false,
            fields: &current_effective,
        },
        parameters,
        &mut keys,
    )?;
    ensure!(parameters.len() <= 60000, "RESOURCE_LIMIT");
    Ok([
        pinned_source,
        pinned_effective,
        current_source,
        current_effective,
    ])
}
