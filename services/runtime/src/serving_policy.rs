use super::*;
use crate::authorization::PreparedProjection;

pub(super) async fn generation_definition(
    client: &tokio_postgres::Client,
    environment: &str,
    resource: &str,
    revision: &str,
) -> Result<Option<BindingDefinition>> {
    let row=client.query_opt("SELECT b.definition->'servingDefinition' FROM LATERAL(SELECT stage FROM moria.source_generations WHERE environment=$1 AND object_type=$2 AND revision<=$3::text::bigint ORDER BY revision DESC LIMIT 1) g JOIN moria.indexing_builds b ON b.id=g.stage", &[&environment,&resource,&revision]).await?;
    let value = row.and_then(|row| row.get::<_, Option<Value>>(0));
    value
        .filter(|value| !value.is_null())
        .map(|value| serde_json::from_value(value).map_err(Into::into))
        .transpose()
}
pub(super) async fn stage_definition(
    client: &tokio_postgres::Client,
    resource: &str,
    stage: Uuid,
) -> Result<Option<BindingDefinition>> {
    let row=client.query_opt("SELECT definition->'servingDefinition' FROM moria.indexing_builds WHERE id=$1 AND target=$2", &[&stage,&resource]).await?;
    let value = row.and_then(|row| row.get::<_, Option<Value>>(0));
    value
        .filter(|value| !value.is_null())
        .map(|value| serde_json::from_value(value).map_err(Into::into))
        .transpose()
}
pub(super) fn prepare_generation(
    snapshot: &SecuritySnapshot,
    binding: &Binding,
    captured: &BindingDefinition,
    selected: &[String],
    fields: &[String],
) -> Result<PreparedProjection> {
    ensure!(
        captured.policies.resource == binding.resource
            && captured.object_type_id == binding.definition.object_type_id,
        "BINDING_INVALID"
    );
    for required in &captured.required_operations {
        authorize(
            &snapshot.identity,
            &required.resource,
            &required.operation,
            &captured.policies.markings,
        )?;
    }
    ObjectReader::compile(captured.policies.clone())?
        .prepare(
            &snapshot.identity,
            snapshot.attributes.clone(),
            Some(selected.to_vec()),
            fields,
            vec![],
        )
        .map_err(Into::into)
}
pub(super) async fn authorize_generation_query(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    binding: &Binding,
    pinned: &str,
    current: &str,
    query: &Query,
) -> Result<()> {
    let snapshot = Authority::current(client, session).await?;
    let mut fields = vec![binding.definition.schema.primary_key.clone()];
    if let Some(filter) = &query.filter {
        query_fields(filter, &mut fields);
    }
    fields.extend(query.sort.iter().map(|key| key.property.clone()));
    if let Some(aggregation) = &query.aggregation {
        fields.extend(aggregation.group_by.clone());
        fields.extend(
            aggregation
                .metrics
                .values()
                .filter_map(|metric| metric.property.clone()),
        );
    }
    let projection = ObjectReader::compile(binding.definition.policies.clone())?.prepare(
        &snapshot.identity,
        snapshot.attributes.clone(),
        if query.aggregation.is_some() {
            Some(vec![])
        } else {
            query.projection.clone()
        },
        &fields,
        vec![],
    )?;
    for revision in [pinned, current].into_iter().collect::<BTreeSet<_>>() {
        if let Some(captured) =
            generation_definition(client, session.environment(), &binding.resource, revision)
                .await?
        {
            prepare_generation(
                &snapshot,
                binding,
                &captured,
                projection.selected(),
                &fields,
            )?;
        }
    }
    Authority::finish(client, session, &snapshot).await
}
pub(super) fn generation_predicates(
    captured: Option<&BindingDefinition>,
    attributes: &ValueMap,
    source: &str,
    effective: &str,
    parameters: &mut Vec<SqlParameter>,
    projection: Option<(&crate::projection::ProjectionRelation, &str)>,
) -> Result<Vec<String>> {
    let Some(captured) = captured else {
        return Ok(vec![]);
    };
    let policies = &captured.policies;
    Ok(vec![
        predicate_mapped(
            policies.row.as_ref(),
            &policies.source_columns,
            &policies.attributes,
            attributes,
            source,
            parameters,
            projection.map(|(relation, alias)| (relation, alias, true)),
        )?,
        predicate_mapped(
            policies.object_read.as_ref(),
            &policies.properties,
            &policies.attributes,
            attributes,
            effective,
            parameters,
            projection.map(|(relation, alias)| (relation, alias, false)),
        )?,
    ])
}
pub(super) fn property_write(
    captured: Option<&BindingDefinition>,
    binding: &Binding,
    property: &str,
    attributes: &ValueMap,
) -> Result<()> {
    if let Some(captured) = captured {
        check_property_write(
            &Binding {
                resource: binding.resource.clone(),
                revision: String::new(),
                definition: captured.clone(),
            },
            property,
            attributes,
        )?;
    }
    Ok(())
}
pub(super) fn write_rows(
    captured: Option<&BindingDefinition>,
    attributes: &ValueMap,
    before: &ObjectRow,
    after: &ObjectRow,
) -> Result<()> {
    if let Some(captured) = captured
        && let Some(write) = &captured.write_policy
    {
        let write = CompiledExpression::compile(
            write,
            ExpressionTypes {
                fields: captured.schema.properties.clone(),
                attributes: captured.policies.attributes.clone(),
                ..Default::default()
            },
        )?;
        for row in [before, after] {
            ensure!(
                write.allows(&ExpressionValues {
                    fields: row.effective.clone(),
                    attributes: attributes.clone(),
                    ..Default::default()
                })?,
                "FORBIDDEN"
            );
        }
    }
    Ok(())
}
