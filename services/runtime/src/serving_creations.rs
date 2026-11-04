use super::*;

pub(super) struct Creation {
    pub uid: Uuid,
    pub canonical: ValueMap,
    pub base: ValueMap,
    pub overrides: ValueMap,
    pub scope_hash: String,
}

/// Reserve permanent identity only through the target's operator-owned local domain.
pub(super) struct CreationInput<'a> {
    pub action: &'a ActionDefinition,
    pub key: &'a str,
    pub identity: &'a CompiledValueExpression,
    pub expressions: &'a ValueMap,
    pub types: &'a ExpressionTypes,
    pub values: &'a ExpressionValues,
}
pub(super) async fn prepare(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    binding: &Binding,
    input: CreationInput<'_>,
) -> Result<Creation> {
    let CreationInput {
        action,
        key,
        identity,
        expressions,
        types,
        values,
    } = input;
    let domain = binding
        .definition
        .identity_domain
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("ACTION_CREATION_UNAVAILABLE"))?;
    ensure!(
        binding.definition.source_resource.is_some(),
        "BINDING_INVALID"
    );
    client
        .query_one(
            "SELECT moria.runtime_environment_managed_check($1)",
            &[&session.environment()],
        )
        .await?;
    client.query_one("SELECT pg_advisory_xact_lock(hashtextextended('moria/object-population/'||$1||'/'||$2,0))", &[&session.environment(),&binding.resource]).await?;
    let ownership=client.query_opt("SELECT d.cell,d.fence::text,d.incarnation FROM moria.runtime_identity_domains d JOIN moria.native_action_identity_domains a ON a.domain=d.domain AND a.target=$2 JOIN moria.runtime_placements p ON p.environment=$3 AND p.cell=d.cell AND p.serving JOIN moria.indexing_instance i ON i.incarnation=d.incarnation WHERE d.domain=$1 AND d.cell::text=current_setting('moria.cell_id',true) FOR SHARE OF d,p", &[domain,&binding.definition.object_type_id,&session.environment()]).await?.ok_or_else(||anyhow::anyhow!("OWNERSHIP_LOST"))?;
    let identity_value = identity.evaluate(values)?;
    let typed_identity =
        crate::canonical::canonical_value(&identity_value, identity.schema(), &Value::Null)?;
    let tuple = crate::canonical::canonical_json(
        &json!({"version":1,"environment":session.environment(),"application":session.application(),"principal":session.principal(),"action":action.resource,"key":key,"objectType":binding.definition.object_type_id,"identityKey":typed_identity}),
    )?;
    ensure!(tuple.len() <= 65536, "IDENTITY_REQUEST_INVALID");
    let uid: Uuid = client
        .query_one(
            "SELECT uid FROM moria.runtime_identity_resolve($1,$2,$3,$4::text::bigint,$5)",
            &[
                domain,
                &vec![tuple.clone()],
                &ownership.get::<_, Uuid>(0),
                &ownership.get::<_, String>(1),
                &ownership.get::<_, Uuid>(2),
            ],
        )
        .await?
        .get(0);
    ensure!(
        !client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM moria.object_homes WHERE uid=$1)",
                &[&uid]
            )
            .await?
            .get::<_, bool>(0),
        "ACTION_CREATION_CONFLICT"
    );
    client
        .execute(
            "INSERT INTO moria.object_homes(uid,object_type) VALUES($1,$2)",
            &[&uid, &binding.resource],
        )
        .await?;
    let mut base = ValueMap::new();
    let mut overrides = ValueMap::new();
    for (property, schema) in &binding.definition.schema.properties {
        if property == &binding.definition.schema.primary_key {
            base.insert(property.clone(), json!(uid));
        } else if schema["nullable"] == true {
            base.insert(property.clone(), Value::Null);
        }
    }
    for (property, expression) in expressions {
        ensure!(
            property != &binding.definition.schema.primary_key,
            "BINDING_INVALID"
        );
        let schema = binding
            .definition
            .schema
            .properties
            .get(property)
            .ok_or_else(|| anyhow::anyhow!("VALUE_INVALID"))?;
        overrides.insert(
            property.clone(),
            CompiledValueExpression::compile(expression, types.clone(), schema)?
                .evaluate(values)?,
        );
    }
    let effective = validate_row(
        &json!(effective(&base, &overrides)),
        &binding.definition.schema.properties,
    )?;
    let mut canonical = ValueMap::new();
    for (property, column) in &binding.definition.source_mapping {
        let value = effective
            .get(property)
            .ok_or_else(|| anyhow::anyhow!("ACTION_CANONICAL_SOURCE_REQUIRED"))?;
        if let Some(previous) = canonical.insert(column.clone(), value.clone()) {
            ensure!(previous == *value, "ACTION_CANONICAL_SOURCE_REQUIRED");
        }
    }
    for (column, schema) in &binding.definition.policies.source_columns {
        if !canonical.contains_key(column) {
            ensure!(
                schema["nullable"] == true,
                "ACTION_CANONICAL_SOURCE_REQUIRED"
            );
            canonical.insert(column.clone(), Value::Null);
        }
    }
    let canonical = validate_row(
        &json!(canonical),
        &binding.definition.policies.source_columns,
    )?;
    Ok(Creation {
        uid,
        canonical,
        base,
        overrides,
        scope_hash: hash(&tuple),
    })
}

impl NativeServing {
    pub async fn attach_source(
        &self,
        session: &VerifiedSession,
        resource: &str,
        uid: &str,
        key: &str,
        input: &Value,
    ) -> Result<Value> {
        let uid = canonical_uid(uid)?;
        ensure!(
            !key.is_empty() && key.len() <= 256,
            "IDEMPOTENCY_KEY_INVALID"
        );
        ensure!(
            input.as_object().is_some_and(|value| value.len() == 2
                && value.contains_key("sourceDataset")
                && value.contains_key("expectedRevision")),
            "INPUT_INVALID"
        );
        let source = canonical_uid(
            input["sourceDataset"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?,
        )?;
        let expected = input["expectedRevision"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
        ensure!(
            expected
                .parse::<i64>()
                .is_ok_and(|value| value >= 0 && value.to_string() == expected),
            "INPUT_INVALID"
        );
        tokio::time::timeout(Duration::from_secs(5),async {
            let admission=Admission::begin(&self.db,session,true).await?;
            let outcome=async {
                let client=admission.client();let snapshot=Authority::current(client,session).await?;
                authorize(&snapshot.identity,"instance","manage",&[])?;
                let binding=Self::binding(client,session,resource).await?;Self::required(&snapshot,&binding)?;Self::schema(client,session,&binding).await?;
                ensure!(binding.definition.source_resource==Some(source),"ACTION_SOURCE_ATTACHMENT_REQUIRED");
                authorize(&snapshot.identity,&binding.resource,"write",&binding.definition.policies.markings)?;
                let mut incoming=Authority::resource_access(client,session,&source.to_string(),"read").await?;
                authorize(&snapshot.identity,&source.to_string(),"read",&incoming)?;
                let mut destination=binding.definition.policies.markings.clone();destination.extend(Authority::resource_access(client,session,&binding.resource,"write").await?);
                incoming.extend(Authority::resource_access(client,session,&binding.resource,"read").await?);
                let creation=client.query_opt("SELECT markings FROM moria.native_object_creations WHERE environment=$1 AND object_type=$2 AND uid=$3", &[&session.environment(),&binding.resource,&uid]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
                incoming.extend(creation.get::<_,Vec<String>>(0));ensure!(incoming.iter().all(|label|destination.contains(label)),"MARKING_FLOW");
                let current=Self::revision(client,session,&snapshot).await?;
                endpoint_write(client,session,&snapshot,&binding,uid,&current).await?;
                let digest=fingerprint(&json!([binding.resource,uid,source,expected]))?;
                if let Some(row)=client.query_opt("SELECT fingerprint,result,expires_at<=clock_timestamp() FROM moria.native_object_attachment_receipts WHERE environment=$1 AND application=$2 AND principal=$3 AND key=$4", &[&session.environment(),&session.application(),&session.principal(),&key]).await? {
                    ensure!(row.get::<_,String>(0)==digest,"IDEMPOTENCY_CONFLICT");ensure!(!row.get::<_,bool>(2),"RESULT_EXPIRED");Authority::finish(client,session,&snapshot).await?;return Ok(row.get(1));
                }
                ensure!(expected==current,"FILESYSTEM_CONFLICT");
                client.query_one("SELECT pg_advisory_xact_lock(hashtextextended('moria/object-population/'||$1||'/'||$2,0))", &[&session.environment(),&binding.resource]).await?;
                let existing=client.query_opt("SELECT source_dataset,revision::text FROM moria.native_object_source_attachments WHERE environment=$1 AND object_type=$2 AND uid=$3", &[&session.environment(),&binding.resource,&uid]).await?;
                let revision=if let Some(existing)=existing {ensure!(existing.get::<_,String>(0)==source.to_string(),"IDEMPOTENCY_CONFLICT");existing.get::<_,String>(1)}else{
                    let revision:String=client.query_one("UPDATE moria.environments SET revision=revision+1 WHERE id=$1 RETURNING revision::text", &[&session.environment()]).await?.get(0);
                    client.execute("INSERT INTO moria.native_object_source_attachments(environment,object_type,uid,source_dataset,revision,application,principal) VALUES($1,$2,$3,$4,$5::text::bigint,$6,$7)", &[&session.environment(),&binding.resource,&uid,&source.to_string(),&revision,&session.application(),&session.principal()]).await?;
                    client.execute("INSERT INTO moria.native_serving_audit(environment,revision,application,principal,action,key,targets) VALUES($1,$2::text::bigint,$3,$4,$5,$6,$7)", &[&session.environment(),&revision,&session.application(),&session.principal(),&format!("attach-source/{}",binding.resource),&key,&vec![uid]]).await?;
                    revision
                };
                let result=json!({"objectType":binding.resource,"uid":uid,"sourceDataset":source,"revision":revision});
                client.execute("INSERT INTO moria.native_object_attachment_receipts(environment,application,principal,key,fingerprint,result) VALUES($1,$2,$3,$4,$5,$6)", &[&session.environment(),&session.application(),&session.principal(),&key,&digest,&result]).await?;
                endpoint_write(client,session,&snapshot,&binding,uid,&revision).await?;Authority::resource_access(client,session,&source.to_string(),"read").await?;
                Authority::finish(client,session,&snapshot).await?;Ok(result)
            }.await;
            admission.complete(outcome).await
        }).await.unwrap_or_else(|_|Err(anyhow::anyhow!("QUERY_TIMEOUT")))
    }
}
async fn endpoint_write(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    snapshot: &SecuritySnapshot,
    binding: &Binding,
    uid: Uuid,
    revision: &str,
) -> Result<()> {
    let projection = ObjectReader::compile(binding.definition.policies.clone())?.prepare(
        &snapshot.identity,
        snapshot.attributes.clone(),
        Some(vec![]),
        &[],
        vec![],
    )?;
    let row = read_object(client, session, binding, uid, revision, &[])
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
                    attributes: binding.definition.policies.attributes.clone(),
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
    Ok(())
}
