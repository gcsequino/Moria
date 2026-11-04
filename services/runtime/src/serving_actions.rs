use super::*;

impl TypedActionEdit {
    pub(super) fn target(&self) -> Option<&str> {
        match self {
            Self::Create { target_type, .. }
            | Self::Set { target_type, .. }
            | Self::ClearOverride { target_type, .. }
            | Self::Delete { target_type, .. }
            | Self::Restore { target_type, .. } => Some(target_type),
            Self::Link { .. } | Self::Unlink { .. } => None,
        }
    }
    fn uid(&self) -> Option<&Value> {
        match self {
            Self::Create { .. } | Self::Link { .. } | Self::Unlink { .. } => None,
            Self::Set { uid, .. }
            | Self::ClearOverride { uid, .. }
            | Self::Delete { uid, .. }
            | Self::Restore { uid, .. } => Some(uid),
        }
    }
}
struct Target {
    binding: Binding,
    uid: Uuid,
    before: ObjectRow,
    overrides: ValueMap,
    tombstone: bool,
    edits: Vec<Edit>,
    captured: Option<BindingDefinition>,
    creation: Option<creations::Creation>,
}
async fn object(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    binding: &Binding,
    uid: Uuid,
    revision: &str,
    preparation: (Option<&OrdinaryAction>, &mut Option<OrdinaryAction>),
) -> Result<(ObjectRow, ValueMap, bool)> {
    let (prepared, capture) = preparation;
    if let Some(plan) = prepared {
        let row = plan.row(&binding.resource, uid)?;
        return Ok((
            ObjectRow {
                source: row.source.canonical,
                effective: effective(&row.source.object, &row.overrides),
            },
            row.overrides,
            row.tombstone,
        ));
    }
    let source = source_row(
        client,
        session.environment(),
        &binding.resource,
        uid,
        revision,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?;
    let overlay=client.query_opt("SELECT v.overrides,v.tombstone,v.revision::text FROM moria.overlay_versions v WHERE v.environment=$1 AND v.object_type=$2 AND v.uid=$3 AND v.revision<=$4::text::bigint ORDER BY v.revision DESC LIMIT 1", &[&session.environment(),&binding.resource,&uid,&revision]).await?;
    let overrides = overlay
        .as_ref()
        .map(|row| serde_json::from_value::<ValueMap>(row.get(0)))
        .transpose()?
        .unwrap_or_default();
    let deleted = overlay.as_ref().is_some_and(|row| row.get::<_, bool>(1));
    if let Some(plan) = capture {
        plan.capture_resource(client, session, &binding.resource)
            .await?;
        plan.record(
            &binding.resource,
            uid,
            OrdinaryRow {
                source: source.clone(),
                overrides: overrides.clone(),
                tombstone: deleted,
                overlay_revision: overlay.map(|row| row.get(2)),
            },
        )?;
    }
    Ok((
        ObjectRow {
            source: source.canonical,
            effective: effective(&source.object, &overrides),
        },
        overrides,
        deleted,
    ))
}
pub(super) struct ActionPreparation<'a> {
    pub snapshot: &'a SecuritySnapshot,
    pub consumed: &'a [String],
    pub ordinary: Option<&'a OrdinaryAction>,
    pub capture: Option<&'a mut Option<OrdinaryAction>>,
}
impl NativeServing {
    pub(super) async fn invoke_typed_action(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        action: &ActionDefinition,
        primary: &Binding,
        key: &str,
        input: &Value,
        preparation: ActionPreparation<'_>,
    ) -> Result<Value> {
        let ActionPreparation {
            snapshot,
            consumed,
            ordinary,
            capture,
        } = preparation;
        let preparing = capture.is_some();
        let batched = if preparing || ordinary.is_some() {
            Some(ordinary::access(client, session, action, primary, snapshot).await?)
        } else {
            None
        };
        let resource_labels = |resource: &str, operation: &str| -> Result<Vec<String>> {
            batched
                .as_ref()
                .and_then(|labels| labels.get(&(resource.to_owned(), operation.to_owned())))
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("BINDING_INVALID"))
        };
        let mut ordinary_capture = if preparing {
            Some(OrdinaryAction::new(
                Self::action_contract(client, session, &action.resource)
                    .await?
                    .0,
                action.resource.clone(),
                primary.resource.clone(),
                snapshot,
                ordinary::generation(client, session, &primary.resource).await?,
            )?)
        } else {
            None
        };
        let edits = action
            .typed_edits
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("BINDING_INVALID"))?;
        ensure!(!edits.is_empty() && edits.len() <= 1000, "ACTION_LIMIT");
        let parameters = validate_row(input, &action.inputs)?;
        ensure!(
            !action.target_parameter.is_empty()
                || edits
                    .iter()
                    .all(|edit| matches!(edit, TypedActionEdit::Create { .. })),
            "BINDING_INVALID"
        );
        let primary_uid = if action.target_parameter.is_empty() {
            None
        } else {
            Some(canonical_uid(
                parameters
                    .get(&action.target_parameter)
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("VALUE_INVALID"))?,
            )?)
        };
        let before_revision: String = client
            .query_one(
                if preparing {
                    "SELECT revision::text FROM moria.environments WHERE id=$1"
                } else {
                    "SELECT revision::text FROM moria.environments WHERE id=$1 FOR UPDATE"
                },
                &[&session.environment()],
            )
            .await?
            .get(0);
        if let Some(plan) = ordinary {
            plan.check(client, session, &action.resource, primary, snapshot)
                .await?;
        }
        let mut fields = Vec::new();
        query_fields(&action.condition, &mut fields);
        for edit in edits {
            if let TypedActionEdit::Link {
                source_uid,
                target_uid,
                ..
            }
            | TypedActionEdit::Unlink {
                source_uid,
                target_uid,
                ..
            } = edit
            {
                query_fields(source_uid, &mut fields);
                query_fields(target_uid, &mut fields);
            }
            if let Some(uid) = edit.uid() {
                query_fields(uid, &mut fields);
            }
            if let TypedActionEdit::Create {
                identity_key,
                values,
                ..
            } = edit
            {
                query_fields(identity_key, &mut fields);
                for value in values.values() {
                    query_fields(value, &mut fields);
                }
            }
            if let TypedActionEdit::Set { value, .. } = edit {
                query_fields(value, &mut fields);
            }
        }
        let primary_projection = ObjectReader::compile(primary.definition.policies.clone())?
            .prepare(
                &snapshot.identity,
                snapshot.attributes.clone(),
                None,
                &fields,
                vec![],
            )?;
        let (primary_before, primary_deleted) = if let Some(uid) = primary_uid {
            let (row, _, deleted) = object(
                client,
                session,
                primary,
                uid,
                &before_revision,
                (ordinary, &mut ordinary_capture),
            )
            .await?;
            (row, deleted)
        } else {
            ensure!(fields.is_empty(), "BINDING_INVALID");
            (
                ObjectRow {
                    source: ValueMap::new(),
                    effective: ValueMap::new(),
                },
                false,
            )
        };
        let primary_captured = generation_definition(
            client,
            session.environment(),
            &primary.resource,
            &before_revision,
        )
        .await?;
        if let Some(captured) = &primary_captured
            && primary_uid.is_some()
        {
            let projection = prepare_generation(
                snapshot,
                primary,
                captured,
                primary_projection.selected(),
                &fields,
            )?;
            ensure!(
                projection
                    .project(Some(&primary_before), Some(&primary_before))?
                    .is_some(),
                "NOT_FOUND"
            );
        }
        ensure!(
            primary_uid.is_none()
                || primary_projection
                    .project(Some(&primary_before), Some(&primary_before))?
                    .is_some(),
            "NOT_FOUND"
        );
        let expression_types = ExpressionTypes {
            fields: primary.definition.schema.properties.clone(),
            parameters: action.inputs.clone(),
            attributes: primary.definition.policies.attributes.clone(),
        };
        let values = ExpressionValues {
            fields: primary_before.effective.clone(),
            parameters: parameters.clone(),
            attributes: snapshot.attributes.clone(),
        };
        ensure!(
            CompiledExpression::compile(&action.condition, expression_types.clone())?
                .allows(&values)?,
            "ACTION_CONDITION_FAILED"
        );
        let mut labels = action.markings.clone();
        labels.extend_from_slice(consumed);
        labels.extend(primary.definition.policies.markings.clone());
        labels.extend(if batched.is_some() {
            resource_labels(&primary.resource, "read")?
        } else {
            Authority::resource_access(client, session, &primary.resource, "read").await?
        });
        labels.extend(if batched.is_some() {
            resource_labels(&action.resource, "read")?
        } else {
            Authority::resource_access(client, session, &action.resource, "read").await?
        });
        let mut targets = BTreeMap::<(String, Uuid), Target>::new();
        let mut edge_edits = Vec::new();
        for edit in edits {
            if matches!(
                edit,
                TypedActionEdit::Link { .. } | TypedActionEdit::Unlink { .. }
            ) {
                let edge = links::prepare_edge(
                    client,
                    session,
                    snapshot,
                    edit,
                    &expression_types,
                    &values,
                    &before_revision,
                )
                .await?;
                labels.extend(edge.markings.clone());
                edge_edits.push(edge);
                continue;
            }
            let resource = edit.target().unwrap().to_owned();
            if let TypedActionEdit::Create {
                identity_key,
                values: created_values,
                ..
            } = edit
            {
                ensure!(targets.len() < 100, "ACTION_LIMIT");
                let binding = Self::binding(client, session, &resource).await?;
                Self::required(snapshot, &binding)?;
                Self::schema(client, session, &binding).await?;
                ObjectReader::compile(binding.definition.policies.clone())?.prepare(
                    &snapshot.identity,
                    snapshot.attributes.clone(),
                    None,
                    &[],
                    vec![],
                )?;
                for property in created_values.keys() {
                    check_property_write(&binding, property, &snapshot.attributes)?;
                }
                let identity = CompiledValueExpression::compile_inferred(
                    identity_key,
                    expression_types.clone(),
                )?;
                let creation = creations::prepare(
                    client,
                    session,
                    &binding,
                    creations::CreationInput {
                        action,
                        key,
                        identity: &identity,
                        expressions: created_values,
                        types: &expression_types,
                        values: &values,
                    },
                )
                .await?;
                let uid = creation.uid;
                let before = ObjectRow {
                    source: creation.canonical.clone(),
                    effective: validate_row(
                        &json!(effective(&creation.base, &creation.overrides)),
                        &binding.definition.schema.properties,
                    )?,
                };
                labels.extend(binding.definition.policies.markings.clone());
                labels.extend(if batched.is_some() {
                    resource_labels(&resource, "read")?
                } else {
                    Authority::resource_access(client, session, &resource, "read").await?
                });
                let captured = generation_definition(
                    client,
                    session.environment(),
                    &resource,
                    &before_revision,
                )
                .await?;
                for property in created_values.keys() {
                    generation_policy::property_write(
                        captured.as_ref(),
                        &binding,
                        property,
                        &snapshot.attributes,
                    )?;
                }
                ensure!(
                    targets
                        .insert(
                            (resource, uid),
                            Target {
                                binding,
                                uid,
                                before,
                                overrides: creation.overrides.clone(),
                                tombstone: false,
                                edits: vec![],
                                captured,
                                creation: Some(creation)
                            }
                        )
                        .is_none(),
                    "ACTION_CREATION_CONFLICT"
                );
                continue;
            }
            let uid = CompiledValueExpression::compile(
                edit.uid().unwrap(),
                expression_types.clone(),
                &json!({"type":"uid","nullable":false}),
            )?
            .evaluate(&values)?;
            let uid = canonical_uid(uid.as_str().unwrap())?;
            if !targets.contains_key(&(resource.clone(), uid)) {
                ensure!(targets.len() < 100, "ACTION_LIMIT");
                let binding = Self::binding(client, session, &resource).await?;
                Self::required(snapshot, &binding)?;
                Self::schema(client, session, &binding).await?;
                let mut target_labels = binding.definition.policies.markings.clone();
                target_labels.extend(if batched.is_some() {
                    resource_labels(&resource, "read")?
                } else {
                    Authority::resource_access(client, session, &resource, "read").await?
                });
                let (before, overrides, tombstone) = object(
                    client,
                    session,
                    &binding,
                    uid,
                    &before_revision,
                    (ordinary, &mut ordinary_capture),
                )
                .await?;
                let captured = generation_definition(
                    client,
                    session.environment(),
                    &binding.resource,
                    &before_revision,
                )
                .await?;
                labels.extend(target_labels.clone());
                targets.insert(
                    (resource.clone(), uid),
                    Target {
                        binding,
                        uid,
                        before,
                        overrides,
                        tombstone,
                        edits: vec![],
                        captured,
                        creation: None,
                    },
                );
            }
            let target = targets.get_mut(&(resource, uid)).unwrap();
            let value = match edit {
                TypedActionEdit::Create { .. }
                | TypedActionEdit::Link { .. }
                | TypedActionEdit::Unlink { .. } => unreachable!(),
                TypedActionEdit::Set {
                    property, value, ..
                } => {
                    check_property_write(&target.binding, property, &snapshot.attributes)?;
                    generation_policy::property_write(
                        target.captured.as_ref(),
                        &target.binding,
                        property,
                        &snapshot.attributes,
                    )?;
                    let value = CompiledValueExpression::compile(
                        value,
                        expression_types.clone(),
                        &target.binding.definition.schema.properties[property],
                    )?
                    .evaluate(&values)?;
                    Edit::Set(property.clone(), value)
                }
                TypedActionEdit::ClearOverride { property, .. } => {
                    check_property_write(&target.binding, property, &snapshot.attributes)?;
                    generation_policy::property_write(
                        target.captured.as_ref(),
                        &target.binding,
                        property,
                        &snapshot.attributes,
                    )?;
                    Edit::Clear(property.clone())
                }
                TypedActionEdit::Delete { .. } => Edit::Delete,
                TypedActionEdit::Restore { .. } => Edit::Restore,
            };
            target.edits.push(value);
        }
        ensure!(
            !primary_deleted
                || targets
                    .get(&(primary.resource.clone(), primary_uid.unwrap()))
                    .is_some_and(|target| target
                        .edits
                        .iter()
                        .any(|edit| matches!(edit, Edit::Restore))),
            "NOT_FOUND"
        );
        labels.sort();
        labels.dedup();
        // Every observed source flows to every written destination, including conditions and failed-effect inputs.
        let action_destination = if batched.is_some() {
            resource_labels(&action.resource, "write")?
        } else {
            Authority::resource_access(client, session, &action.resource, "write").await?
        };
        ensure!(
            consumed
                .iter()
                .all(|label| action.markings.contains(label) || action_destination.contains(label)),
            "MARKING_FLOW"
        );
        for target in targets.values_mut() {
            let mut destination = target.binding.definition.policies.markings.clone();
            destination.extend(
                Authority::resource_access(client, session, &target.binding.resource, "write")
                    .await?,
            );
            ensure!(
                labels.iter().all(|label| destination.contains(label)),
                "MARKING_FLOW"
            );
            let projection = ObjectReader::compile(target.binding.definition.policies.clone())?
                .prepare(
                    &snapshot.identity,
                    snapshot.attributes.clone(),
                    None,
                    &[],
                    vec![],
                )?;
            let restoring = target
                .edits
                .iter()
                .any(|edit| matches!(edit, Edit::Restore));
            let historical = target
                .captured
                .as_ref()
                .map(|captured| {
                    prepare_generation(
                        snapshot,
                        &target.binding,
                        captured,
                        projection.selected(),
                        &[],
                    )
                })
                .transpose()?;
            if let Some(historical) = &historical {
                ensure!(
                    historical
                        .project(Some(&target.before), Some(&target.before))?
                        .is_some(),
                    "NOT_FOUND"
                );
            }
            ensure!(
                (!target.tombstone || restoring)
                    && projection
                        .project(Some(&target.before), Some(&target.before))?
                        .is_some(),
                "NOT_FOUND"
            );
            for edit in &target.edits {
                match edit {
                    Edit::Set(property, value) => {
                        target.overrides.insert(property.clone(), value.clone());
                    }
                    Edit::Clear(property) => {
                        target.overrides.remove(property);
                    }
                    Edit::Delete => target.tombstone = true,
                    Edit::Restore => target.tombstone = false,
                }
            }
            // clearOverride restores the mapped source, never an original Dataset field with a conflicting name.
            let source = if let Some(creation) = &target.creation {
                SourceRow {
                    canonical: creation.canonical.clone(),
                    object: creation.base.clone(),
                }
            } else if let Some(plan) = ordinary.or(ordinary_capture.as_ref()) {
                plan.row(&target.binding.resource, target.uid)?.source
            } else {
                source_row(
                    client,
                    session.environment(),
                    &target.binding.resource,
                    target.uid,
                    &before_revision,
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?
            };
            let after = ObjectRow {
                source: source.canonical,
                effective: validate_row(
                    &json!(effective(&source.object, &target.overrides)),
                    &target.binding.definition.schema.properties,
                )?,
            };
            ensure!(
                target.tombstone || projection.project(Some(&after), Some(&after))?.is_some(),
                "FORBIDDEN"
            );
            if let Some(historical) = &historical {
                ensure!(
                    target.tombstone || historical.project(Some(&after), Some(&after))?.is_some(),
                    "FORBIDDEN"
                );
            }
            generation_policy::write_rows(
                target.captured.as_ref(),
                &snapshot.attributes,
                &target.before,
                &after,
            )?;
            if let Some(write) = &target.binding.definition.write_policy {
                let write = CompiledExpression::compile(
                    write,
                    ExpressionTypes {
                        fields: target.binding.definition.schema.properties.clone(),
                        attributes: target.binding.definition.policies.attributes.clone(),
                        ..Default::default()
                    },
                )?;
                for row in [&target.before, &after] {
                    ensure!(
                        write.allows(&ExpressionValues {
                            fields: row.effective.clone(),
                            attributes: snapshot.attributes.clone(),
                            ..Default::default()
                        })?,
                        "FORBIDDEN"
                    );
                }
            }
        }
        if let Some(capture) = capture {
            Authority::finish(client, session, snapshot).await?;
            *capture = ordinary_capture;
            return Ok(Value::Null);
        }
        let next:String=client.query_one("UPDATE moria.environments SET revision=revision+1 WHERE id=$1 RETURNING revision::text", &[&session.environment()]).await?.get(0);
        for target in targets.values() {
            if let Some(creation) = &target.creation {
                client.execute("INSERT INTO moria.native_object_creations(environment,object_type,uid,created_revision,source_value,mapped_base,action_scope_hash,markings) VALUES($1,$2,$3,$4::text::bigint,$5,$6,$7,$8)", &[&session.environment(),&target.binding.resource,&target.uid,&next,&json!(creation.canonical),&json!(creation.base),&creation.scope_hash,&labels]).await?;
            }
            client.execute("INSERT INTO moria.overlay_versions(environment,object_type,uid,revision,overrides,tombstone) VALUES($1,$2,$3,$4::text::bigint,$5,$6)", &[&session.environment(),&target.binding.resource,&target.uid,&next,&json!(target.overrides),&target.tombstone]).await?;
        }
        let created=targets.values().filter(|target|target.creation.is_some()).map(|target|json!({"objectType":target.binding.resource,"uid":target.uid,"version":next})).collect::<Vec<_>>();
        let changed=targets.values().map(|target|json!({"objectType":target.binding.resource,"uid":target.uid,"version":next,"deleted":target.tombstone})).collect::<Vec<_>>();
        links::commit_edges(client, session, &edge_edits, &labels, &next).await?;
        let result = json!({"revision":next,"created":created,"changed":changed});
        let input_hash = fingerprint(&json!(parameters))?;
        client.execute("INSERT INTO moria.action_keys VALUES($1,$2,$3,$4,$5,$6,$7,$8,clock_timestamp()+interval '30 days')", &[&session.environment(),&session.application(),&session.principal(),&action.resource,&key,&input_hash,&action.revision,&result]).await?;
        let resources = targets
            .keys()
            .map(|(resource, _)| resource.clone())
            .chain(
                edge_edits
                    .iter()
                    .flat_map(|edge| [edge.source_resource.clone(), edge.target_resource.clone()]),
            )
            .chain(std::iter::once(primary.resource.clone()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        client.execute("INSERT INTO moria.native_serving_receipt_inputs(environment,application,principal,action,key,input_types,object_resource,object_resources) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", &[&session.environment(),&session.application(),&session.principal(),&action.resource,&key,&json!(action.inputs),&primary.resource,&resources]).await?;
        let uids = targets
            .keys()
            .map(|(_, uid)| *uid)
            .chain(
                edge_edits
                    .iter()
                    .flat_map(|edge| [edge.source_uid, edge.target_uid]),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        ensure!(!uids.is_empty() && uids.len() <= 100, "ACTION_LIMIT");
        client.execute("INSERT INTO moria.native_serving_audit(environment,revision,application,principal,action,key,targets) VALUES($1,$2::text::bigint,$3,$4,$5,$6,$7)", &[&session.environment(),&next,&session.application(),&session.principal(),&action.resource,&key,&uids]).await?;
        for effect in &action.effects {
            ensure!(
                !effect.key.is_empty() && effect.key.len() <= 128,
                "BINDING_INVALID"
            );
            let logical = format!("action/{}/{key}/{}", action.resource, effect.key);
            let payload = json!({"version":1,"action":action.resource,"key":key,"result":result});
            let origin = json!({"kind":"action","environment":session.environment(),"application":session.application(),"principal":session.principal(),"action":action.resource,"key":key});
            client
                .query_one(
                    "SELECT moria.runtime_delivery_enqueue($1,$2,$3,$4,$5)",
                    &[&effect.destination, &logical, &payload, &labels, &origin],
                )
                .await?;
        }
        Authority::finish(client, session, snapshot).await?;
        Ok(result)
    }
}
