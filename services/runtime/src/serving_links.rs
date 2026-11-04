use super::*;

pub(super) struct EdgeEdit {
    pub resource: String,
    pub source_uid: Uuid,
    pub target_uid: Uuid,
    pub source_resource: String,
    pub target_resource: String,
    pub linked: bool,
    pub cardinality: String,
    pub markings: Vec<String>,
    destination: Vec<String>,
}
pub(super) async fn markings(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    snapshot: &SecuritySnapshot,
    resource: &str,
) -> Result<Vec<String>> {
    let row=client.query_opt("SELECT source_resource,target_resource,definition FROM moria.native_serving_links WHERE environment=$1 AND resource=$2 AND enabled", &[&session.environment(),&resource]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
    let definition: LinkDefinition = serde_json::from_value(row.get(2))?;
    authorize(&snapshot.identity, resource, "read", &definition.markings)?;
    let mut labels = definition.markings;
    labels.extend(Authority::resource_access(client, session, resource, "read").await?);
    for resource in [row.get::<_, String>(0), row.get::<_, String>(1)] {
        let binding = NativeServing::binding(client, session, &resource).await?;
        NativeServing::required(snapshot, &binding)?;
        ObjectReader::compile(binding.definition.policies.clone())?.prepare(
            &snapshot.identity,
            snapshot.attributes.clone(),
            Some(vec![]),
            &[],
            vec![],
        )?;
        labels.extend(binding.definition.policies.markings);
        labels.extend(Authority::resource_access(client, session, &resource, "read").await?);
    }
    Ok(labels)
}
pub(super) async fn prepare_edge(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    snapshot: &SecuritySnapshot,
    edit: &TypedActionEdit,
    types: &ExpressionTypes,
    values: &ExpressionValues,
    revision: &str,
) -> Result<EdgeEdit> {
    let (resource, source, target, linked) = match edit {
        TypedActionEdit::Link {
            link_type,
            source_uid,
            target_uid,
        } => (link_type, source_uid, target_uid, true),
        TypedActionEdit::Unlink {
            link_type,
            source_uid,
            target_uid,
        } => (link_type, source_uid, target_uid, false),
        _ => bail!("BINDING_INVALID"),
    };
    let row=client.query_opt("SELECT source_resource,target_resource,definition FROM moria.native_serving_links WHERE environment=$1 AND resource=$2 AND enabled", &[&session.environment(),&resource]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
    let definition: LinkDefinition =
        serde_json::from_value(row.get(2)).map_err(|_| anyhow::anyhow!("BINDING_INVALID"))?;
    ensure!(definition.mode == "actionManaged", "BINDING_INVALID");
    for operation in ["read", "write"] {
        authorize(
            &snapshot.identity,
            resource,
            operation,
            &definition.markings,
        )?;
    }
    let mut labels = definition.markings.clone();
    labels.extend(Authority::resource_access(client, session, resource, "read").await?);
    let mut destination = definition.markings.clone();
    destination.extend(Authority::resource_access(client, session, resource, "write").await?);
    if let Some(policy) = &definition.read_policy {
        ensure!(
            CompiledExpression::compile(
                policy,
                ExpressionTypes {
                    attributes: definition.attributes,
                    ..Default::default()
                }
            )?
            .allows(&ExpressionValues {
                attributes: snapshot.attributes.clone(),
                ..Default::default()
            })?,
            "FORBIDDEN"
        );
    }
    let mut ids = vec![];
    for (expression, resource) in [
        (source, row.get::<_, String>(0)),
        (target, row.get::<_, String>(1)),
    ] {
        let uid = CompiledValueExpression::compile(
            expression,
            types.clone(),
            &json!({"type":"uid","nullable":false}),
        )?
        .evaluate(values)?;
        let uid = canonical_uid(uid.as_str().unwrap())?;
        labels.extend(endpoint(client, session, snapshot, &resource, uid, revision).await?);
        ids.push((resource, uid));
    }
    let cardinality = definition
        .cardinality
        .ok_or_else(|| anyhow::anyhow!("BINDING_INVALID"))?;
    ensure!(
        ["manyToOne", "oneToOne", "manyToMany"].contains(&cardinality.as_str()),
        "BINDING_INVALID"
    );
    Ok(EdgeEdit {
        resource: resource.clone(),
        source_resource: ids[0].0.clone(),
        source_uid: ids[0].1,
        target_resource: ids[1].0.clone(),
        target_uid: ids[1].1,
        linked,
        cardinality,
        markings: labels,
        destination,
    })
}
async fn endpoint(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    snapshot: &SecuritySnapshot,
    resource: &str,
    uid: Uuid,
    revision: &str,
) -> Result<Vec<String>> {
    let binding = NativeServing::binding(client, session, resource).await?;
    NativeServing::required(snapshot, &binding)?;
    NativeServing::schema(client, session, &binding).await?;
    ensure!(
        client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM moria.object_homes WHERE uid=$1 AND object_type=$2)",
                &[&uid, &binding.resource]
            )
            .await?
            .get::<_, bool>(0),
        "NOT_FOUND"
    );
    let projection = ObjectReader::compile(binding.definition.policies.clone())?.prepare(
        &snapshot.identity,
        snapshot.attributes.clone(),
        Some(vec![]),
        &[],
        vec![],
    )?;
    let row = read_object(client, session, &binding, uid, revision, &[])
        .await?
        .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?;
    ensure!(
        projection.project(Some(&row), Some(&row))?.is_some(),
        "NOT_FOUND"
    );
    let captured =
        generation_definition(client, session.environment(), &binding.resource, revision).await?;
    generation_policy::write_rows(captured.as_ref(), &snapshot.attributes, &row, &row)?;
    if let Some(policy) = &binding.definition.write_policy {
        ensure!(
            CompiledExpression::compile(
                policy,
                ExpressionTypes {
                    fields: binding.definition.schema.properties.clone(),
                    attributes: binding.definition.policies.attributes,
                    ..Default::default()
                }
            )?
            .allows(&ExpressionValues {
                fields: row.effective,
                attributes: snapshot.attributes.clone(),
                ..Default::default()
            })?,
            "FORBIDDEN"
        );
    }
    let mut labels = binding.definition.policies.markings;
    labels.extend(Authority::resource_access(client, session, &binding.resource, "read").await?);
    Authority::resource_access(client, session, &binding.resource, "write").await?;
    Ok(labels)
}
pub(super) async fn commit_edges(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    edges: &[EdgeEdit],
    labels: &[String],
    revision: &str,
) -> Result<()> {
    let snapshot = Authority::current(client, session).await?;
    let mut final_edges = BTreeMap::new();
    for edge in edges {
        ensure!(
            labels.iter().all(|label| edge.destination.contains(label)),
            "MARKING_FLOW"
        );
        final_edges.insert((&edge.resource, edge.source_uid, edge.target_uid), edge);
    }
    for edge in final_edges.values() {
        endpoint(
            client,
            session,
            &snapshot,
            &edge.source_resource,
            edge.source_uid,
            revision,
        )
        .await?;
        endpoint(
            client,
            session,
            &snapshot,
            &edge.target_resource,
            edge.target_uid,
            revision,
        )
        .await?;
        client.execute("INSERT INTO moria.native_link_edge_versions(environment,link,source_uid,target_uid,revision,linked) VALUES($1,$2,$3,$4,$5::text::bigint,$6)", &[&session.environment(),&edge.resource,&edge.source_uid,&edge.target_uid,&revision,&edge.linked]).await?;
    }
    for edge in final_edges.values() {
        if edge.cardinality != "manyToMany" {
            ensure!(client.query_one("SELECT count(*)<=1 FROM moria.runtime_link_edges_at($1,$2,$3::text::bigint) WHERE source_uid=$4", &[&session.environment(),&edge.resource,&revision,&edge.source_uid]).await?.get::<_,bool>(0),"LINK_CARDINALITY");
        }
        if edge.cardinality == "oneToOne" {
            ensure!(client.query_one("SELECT count(*)<=1 FROM moria.runtime_link_edges_at($1,$2,$3::text::bigint) WHERE target_uid=$4", &[&session.environment(),&edge.resource,&revision,&edge.target_uid]).await?.get::<_,bool>(0),"LINK_CARDINALITY");
        }
    }
    Ok(())
}

pub(super) async fn traverse_in(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    reference: &str,
    request: &Traverse,
    link: &tokio_postgres::Row,
    definition: &LinkDefinition,
    context: (&SecuritySnapshot, Option<&mut Vec<ReadEvidence>>),
) -> Result<Value> {
    let (snapshot, mut evidence) = context;
    ensure!(
        ["mapped", "actionManaged"].contains(&definition.mode.as_str()),
        "UNSUPPORTED_CAPABILITY"
    );
    ensure!(
        matches!(
            definition.cardinality.as_deref(),
            Some("manyToOne" | "oneToOne" | "manyToMany")
        ),
        "BINDING_INVALID"
    );
    authorize(&snapshot.identity, reference, "read", &definition.markings)?;
    Authority::resource_access(client, session, reference, "read").await?;
    if let Some(policy) = &definition.read_policy {
        ensure!(
            CompiledExpression::compile(
                policy,
                ExpressionTypes {
                    attributes: definition.attributes.clone(),
                    ..Default::default()
                }
            )?
            .allows(&ExpressionValues {
                attributes: snapshot.attributes.clone(),
                ..Default::default()
            })?,
            "FORBIDDEN"
        );
    }
    let source = NativeServing::binding(client, session, &link.get::<_, String>(0)).await?;
    let target = NativeServing::binding(client, session, &link.get::<_, String>(1)).await?;
    if definition.mode == "actionManaged" {
        return managed_traverse(
            client,
            session,
            (snapshot, evidence),
            request,
            reference,
            &source,
            &target,
        )
        .await;
    }
    let target_property = definition
        .target_property
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("BINDING_INVALID"))?;
    let left = scalar_type(&source, &definition.source_property)?;
    let right = scalar_type(&target, target_property)?;
    let mut left_type = left.clone();
    left_type["nullable"] = false.into();
    let mut right_type = right.clone();
    right_type["nullable"] = false.into();
    ensure!(left_type == right_type, "BINDING_INVALID");
    let (anchor, anchor_key, result, result_key) = if request.direction == "out" {
        (
            &source,
            &definition.source_property,
            &target,
            target_property,
        )
    } else {
        (
            &target,
            target_property,
            &source,
            &definition.source_property,
        )
    };
    for (binding, key) in [
        (&source, &definition.source_property),
        (&target, target_property),
    ] {
        NativeServing::required(snapshot, binding)?;
        NativeServing::schema(client, session, binding).await?;
        Authority::resource_access(client, session, &binding.resource, "read").await?;
        ObjectReader::compile(binding.definition.policies.clone())?.prepare(
            &snapshot.identity,
            snapshot.attributes.clone(),
            Some(vec![]),
            &[key.clone(), binding.definition.schema.primary_key.clone()],
            vec![],
        )?;
    }
    let projection = ObjectReader::compile(anchor.definition.policies.clone())?.prepare(
        &snapshot.identity,
        snapshot.attributes.clone(),
        Some(vec![anchor_key.clone()]),
        std::slice::from_ref(anchor_key),
        vec![],
    )?;
    let revision = NativeServing::revision(client, session, snapshot).await?;
    ensure!(client.query_one(
        "SELECT moria.runtime_link_constraint_ready($1,$2,$4::text::bigint) AND moria.runtime_link_constraint_ready($1,$3,$4::text::bigint)",
        &[&session.environment(), &source.resource, &target.resource, &revision],
    ).await?.get::<_, bool>(0), "LINK_CONSTRAINT_NOT_READY");
    let row = read_object(
        client,
        session,
        anchor,
        request.uid,
        &revision,
        projection.selected(),
    )
    .await?;
    let object = projection
        .project(row.as_ref(), row.as_ref())?
        .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?;
    NativeServing::append_link_points(
        (
            anchor,
            &revision,
            projection.selected().to_vec(),
            vec![request.uid],
        ),
        evidence.as_deref_mut(),
    );
    // Authorize the destination key and requested projection even when the anchor is null.
    ObjectReader::compile(result.definition.policies.clone())?.prepare(
        &snapshot.identity,
        snapshot.attributes.clone(),
        request.projection.clone(),
        std::slice::from_ref(result_key),
        vec![],
    )?;
    let key = &object[anchor_key];
    let objects = if key.is_null() {
        json!([])
    } else {
        query_sort_limits(client).await?;
        let query = Query {
            filter: Some(json!({"op":"eq","args":[{"field":result_key},{"literal":key}]})),
            projection: request.projection.clone(),
            page_size: request.fanout,
            cursor: None,
            sort: vec![],
            scope: request.scope.clone(),
            budget: Some(request.budget.clone()),
            aggregation: None,
        };
        let result =
            NativeServing::query_recorded(client, session, &result.resource, &query, evidence)
                .await?;
        ensure!(result.get("nextCursor").is_none(), "RESOURCE_LIMIT");
        result["objects"].clone()
    };
    Authority::finish(client, session, snapshot).await?;
    let result = json!({"revision":revision,"objects":objects});
    ensure!(
        serde_json::to_vec(&result)?.len() <= request.budget.response_bytes,
        "RESPONSE_TOO_LARGE"
    );
    Ok(result)
}

async fn managed_traverse(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    context: (&SecuritySnapshot, Option<&mut Vec<ReadEvidence>>),
    request: &Traverse,
    reference: &str,
    source: &Binding,
    target: &Binding,
) -> Result<Value> {
    let (snapshot, mut evidence) = context;
    let (anchor, result) = if request.direction == "out" {
        (source, target)
    } else {
        (target, source)
    };
    for binding in [source, target] {
        NativeServing::required(snapshot, binding)?;
        NativeServing::schema(client, session, binding).await?;
        Authority::resource_access(client, session, &binding.resource, "read").await?;
    }
    let anchor_projection = ObjectReader::compile(anchor.definition.policies.clone())?.prepare(
        &snapshot.identity,
        snapshot.attributes.clone(),
        Some(vec![]),
        &[],
        vec![],
    )?;
    let projection = ObjectReader::compile(result.definition.policies.clone())?.prepare(
        &snapshot.identity,
        snapshot.attributes.clone(),
        request.projection.clone(),
        &[],
        vec![],
    )?;
    let revision = NativeServing::revision(client, session, snapshot).await?;
    let anchor_row = read_object(client, session, anchor, request.uid, &revision, &[]).await?;
    ensure!(
        anchor_projection
            .project(anchor_row.as_ref(), anchor_row.as_ref())?
            .is_some(),
        "NOT_FOUND"
    );
    NativeServing::append_link_points(
        (anchor, &revision, vec![], vec![request.uid]),
        evidence.as_deref_mut(),
    );
    let query = Query {
        filter: None,
        projection: request.projection.clone(),
        page_size: request.fanout.min(request.budget.scan_rows),
        cursor: None,
        sort: vec![],
        scope: request.scope.clone(),
        budget: None,
        aggregation: None,
    };
    authorize_generation_query(client, session, result, &revision, &revision, &query).await?;
    let pin = ReadPin {
        revision: revision.clone(),
        after: None,
        keys: vec![],
        expires: String::new(),
    };
    let (base, mut parameters) = canonical_candidates(
        client,
        session.environment(),
        result,
        &snapshot.attributes,
        &pin,
        &revision,
        &query,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?;
    let link = bind(&mut parameters, Some(reference.into()), "text");
    let anchor = bind(&mut parameters, Some(request.uid.to_string()), "uuid");
    let (anchor_column, result_column) = if request.direction == "out" {
        ("source_uid", "target_uid")
    } else {
        ("target_uid", "source_uid")
    };
    let ranked = format!(
        "SELECT c.* FROM ({base}) c JOIN moria.runtime_link_edges_at($1::text,{link},$3::text::bigint) edges ON edges.{anchor_column}={anchor} AND edges.{result_column}=c.uid"
    );
    let prefix = "WITH settings AS(SELECT $1::text,$2::text,$3::text::bigint,$4::text::bigint,$5::text::uuid,$6::text::bigint)";
    let sql = page_sql(
        prefix,
        &ranked,
        &SortPlan {
            extra: String::new(),
            order: "uid ASC".into(),
            after: "TRUE".into(),
        },
        None,
    );
    query_sort_limits(client).await?;
    let rows = collect_query_rows(client, &sql, &parameters, false, "link_edges").await?;
    ensure!(rows.len() <= query.page_size, "RESOURCE_LIMIT");
    NativeServing::append_link_points(
        (
            result,
            &revision,
            projection.selected().to_vec(),
            rows.iter().map(|row| row.get(0)).collect(),
        ),
        evidence,
    );
    let mut objects = vec![];
    for row in rows {
        let pinned = ObjectRow {
            source: serde_json::from_value(row.get(1))?,
            effective: serde_json::from_value(row.get(2))?,
        };
        let current = ObjectRow {
            source: serde_json::from_value(row.get(3))?,
            effective: serde_json::from_value(row.get(4))?,
        };
        objects.push(
            projection
                .project(Some(&pinned), Some(&current))?
                .ok_or_else(|| anyhow::anyhow!("POLICY_MISMATCH"))?,
        );
    }
    Authority::finish(client, session, snapshot).await?;
    let result = json!({"revision":revision,"objects":objects});
    ensure!(
        serde_json::to_vec(&result)?.len() <= request.budget.response_bytes,
        "RESPONSE_TOO_LARGE"
    );
    Ok(result)
}
