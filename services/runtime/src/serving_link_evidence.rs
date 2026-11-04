use super::*;

impl NativeServing {
    pub(crate) fn link_timeout(input: &Value) -> Result<Duration> {
        Ok(Duration::from_millis(parse_traverse(input)?.budget.time_ms))
    }
    async fn link_contract(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
    ) -> Result<(String, tokio_postgres::Row)> {
        let resource = Self::resource_reference(client, session, reference).await?;
        let row = client.query_opt(
            "SELECT resource,source_resource,target_resource,definition,authority_generation::text FROM moria.native_serving_links WHERE environment=$1 AND resource=$2 AND enabled",
            &[&session.environment(), &resource],
        ).await?.ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?;
        let mut bindings = vec![];
        for resource in [row.get::<_, String>(1), row.get::<_, String>(2)] {
            let binding = Self::binding(client, session, &resource).await?;
            bindings.push(json!([binding.resource, binding.revision]));
        }
        let contract = fingerprint(&json!([
            row.get::<_, String>(0),
            row.get::<_, Value>(3),
            row.get::<_, String>(4),
            bindings
        ]))?;
        Ok((contract, row))
    }
    pub(crate) async fn link_root_for_broker(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
        input: &Value,
    ) -> Result<BrokerRead> {
        let snapshot = Authority::current(client, session).await?;
        let (contract, row) = Self::link_contract(client, session, reference).await?;
        let reference: String = row.get(0);
        let mut evidence = vec![];
        let mut markings = links::markings(client, session, &snapshot, &reference).await?;
        for resource in [row.get::<_, String>(1), row.get::<_, String>(2)] {
            let root = Self::root_for_broker(client, session, &resource).await?;
            evidence.extend(root.evidence);
            markings.extend(root.markings);
        }
        evidence[0].link = Some(LinkObservation {
            resource: reference,
            contract,
            input: input.clone(),
            result_hash: None,
            failure: None,
        });
        Authority::finish(client, session, &snapshot).await?;
        Ok(BrokerRead {
            value: Value::Null,
            evidence,
            markings,
        })
    }
    pub(crate) fn failed_link_read(root: &mut BrokerRead, code: &str) {
        root.evidence[0].link.as_mut().unwrap().failure = Some(code.to_owned());
    }
    pub(crate) async fn traverse_for_broker(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        root: &BrokerRead,
    ) -> Result<BrokerRead> {
        let observation = root.evidence[0].link.as_ref().unwrap();
        let request = parse_traverse(&observation.input)?;
        let mut evidence = root.evidence.clone();
        let value = Self::traverse_recorded(
            client,
            session,
            &observation.resource,
            &request,
            Some(&mut evidence),
        )
        .await?;
        let mut markings = root.markings.clone();
        for read in evidence.iter_mut().skip(root.evidence.len()) {
            Self::capture_read(client, session, read).await?;
            markings.extend(Self::verify_read(client, session, read, false).await?);
        }
        evidence[0].link.as_mut().unwrap().result_hash = Some(fingerprint(&value["objects"])?);
        Ok(BrokerRead {
            value,
            evidence,
            markings,
        })
    }
    pub(super) fn append_link_points(
        plan: (&Binding, &str, Vec<String>, Vec<Uuid>),
        output: Option<&mut Vec<ReadEvidence>>,
    ) {
        if let Some(output) = output {
            let (binding, revision, fields, uids) = plan;
            output.push(ReadEvidence {
                resource: binding.resource.clone(),
                binding_revision: binding.revision.clone(),
                revision: revision.to_owned(),
                fields,
                uids,
                population: false,
                fingerprints: BTreeMap::new(),
                query: None,
                policies: BTreeMap::new(),
                prior_policies: BTreeMap::new(),
                failure: None,
                link: None,
            });
        }
    }
    pub(super) async fn verify_link_observation(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        observation: &LinkObservation,
        compare: bool,
    ) -> Result<Vec<String>> {
        let snapshot = Authority::current(client, session).await?;
        let (contract, _) = Self::link_contract(client, session, &observation.resource).await?;
        ensure!(contract == observation.contract, "SCHEMA_CHANGED");
        let markings = links::markings(client, session, &snapshot, &observation.resource).await?;
        if compare && (observation.result_hash.is_some() || observation.failure.is_some()) {
            client
                .batch_execute("SAVEPOINT native_link_observation")
                .await?;
            let observed = async {
                let request = parse_traverse(&observation.input)?;
                Self::traverse_transaction(client, session, &observation.resource, &request).await
            }
            .await;
            client.batch_execute("ROLLBACK TO SAVEPOINT native_link_observation; RELEASE SAVEPOINT native_link_observation").await?;
            if let Some(expected) = &observation.result_hash {
                ensure!(
                    observed
                        .as_ref()
                        .is_ok_and(|value| fingerprint(&value["objects"])
                            .is_ok_and(|hash| &hash == expected)),
                    "FUNCTION_INPUT_CHANGED"
                );
            } else {
                ensure!(
                    observed.as_ref().is_err_and(
                        |error| Some(error_code(error)) == observation.failure.as_deref()
                    ),
                    "FUNCTION_INPUT_CHANGED"
                );
            }
        }
        Authority::finish(client, session, &snapshot).await?;
        Ok(markings)
    }
}
