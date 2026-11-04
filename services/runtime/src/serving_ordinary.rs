use super::*;

// Only this module and the ordinary Action kernel construct preparation evidence.
// It carries immutable data and version anchors; publication grants no authority from it.
pub(super) struct OrdinaryAction {
    contract: String,
    resource: String,
    security_epoch: String,
    instance_epoch: String,
    attributes: String,
    generations: BTreeMap<String, Option<(Uuid, String)>>,
    rows: BTreeMap<(String, Uuid), OrdinaryRow>,
    bytes: usize,
}
#[derive(Clone)]
pub(super) struct OrdinaryRow {
    pub source: SourceRow,
    pub overrides: ValueMap,
    pub tombstone: bool,
    pub overlay_revision: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ActionAccess {
    authority: SecuritySnapshot,
    resources: Vec<ResourceAccess>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceAccess {
    resource: String,
    operation: String,
    markings: Vec<String>,
}
pub(super) async fn access(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    action: &ActionDefinition,
    binding: &Binding,
    snapshot: &SecuritySnapshot,
) -> Result<BTreeMap<(String, String), Vec<String>>> {
    let value: Value = client
        .query_one(
            "SELECT moria.native_action_resource_access($1,$2)",
            &[&session.session_hash(), &action.resource],
        )
        .await?
        .get(0);
    let current: ActionAccess =
        serde_json::from_value(value).map_err(|_| anyhow::anyhow!("BINDING_INVALID"))?;
    ensure!(
        current.authority.environment == session.environment()
            && current.authority.application == session.application()
            && current.authority.principal == session.principal()
            && current.authority.security_epoch == snapshot.security_epoch
            && current.authority.instance_epoch == snapshot.instance_epoch
            && current.authority.attributes == snapshot.attributes,
        "AUTHORIZATION_CHANGED"
    );
    let mut resources = BTreeSet::from([binding.resource.clone()]);
    if let Some(edits) = &action.typed_edits {
        resources.extend(
            edits
                .iter()
                .filter_map(TypedActionEdit::target)
                .map(str::to_owned),
        );
    }
    resources.insert(action.resource.clone());
    let expected = resources
        .into_iter()
        .flat_map(|resource| {
            [
                (resource.clone(), "read".to_owned()),
                (resource, "write".to_owned()),
            ]
        })
        .collect::<BTreeSet<_>>();
    ensure!(
        expected.len() >= 4 && current.resources.len() == expected.len(),
        "BINDING_INVALID"
    );
    let mut labels = BTreeMap::new();
    for item in current.resources {
        let identity = (item.resource, item.operation);
        ensure!(
            expected.contains(&identity) && labels.insert(identity, item.markings).is_none(),
            "BINDING_INVALID"
        );
    }
    Ok(labels)
}
impl OrdinaryAction {
    pub(super) fn home_uids(&self) -> Vec<Uuid> {
        self.rows
            .keys()
            .map(|(_, uid)| *uid)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn new(
        contract: String,
        resource: String,
        object: String,
        snapshot: &SecuritySnapshot,
        generation: Option<(Uuid, String)>,
    ) -> Result<Self> {
        Ok(Self {
            contract,
            resource,
            security_epoch: snapshot.security_epoch.clone(),
            instance_epoch: snapshot.instance_epoch.clone(),
            attributes: fingerprint(&json!(snapshot.attributes))?,
            generations: BTreeMap::from([(object, generation)]),
            rows: BTreeMap::new(),
            bytes: 0,
        })
    }
    pub fn record(&mut self, resource: &str, uid: Uuid, row: OrdinaryRow) -> Result<()> {
        let identity = (resource.to_owned(), uid);
        if let Some(existing) = self.rows.get(&identity) {
            ensure!(
                existing.source.object == row.source.object
                    && existing.source.canonical == row.source.canonical
                    && existing.overrides == row.overrides
                    && existing.tombstone == row.tombstone
                    && existing.overlay_revision == row.overlay_revision,
                "ACTION_PREPARATION_CHANGED"
            );
            return Ok(());
        }
        self.bytes = self
            .bytes
            .checked_add(
                serde_json::to_vec(&json!([
                    row.source.object,
                    row.source.canonical,
                    row.overrides
                ]))?
                .len(),
            )
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        ensure!(
            self.bytes <= 64 * 1024 * 1024 && self.rows.len() < 101,
            "RESOURCE_LIMIT"
        );
        ensure!(self.rows.insert(identity, row).is_none(), "BINDING_INVALID");
        Ok(())
    }
    pub async fn capture_resource(
        &mut self,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        resource: &str,
    ) -> Result<()> {
        if !self.generations.contains_key(resource) {
            self.generations.insert(
                resource.to_owned(),
                generation(client, session, resource).await?,
            );
        }
        Ok(())
    }
    pub fn publication_key(&self, session: &VerifiedSession, key: &str) -> Result<String> {
        Ok(format!(
            "moria/action-publication/{}",
            fingerprint(&json!([
                session.environment(),
                session.application(),
                session.principal(),
                self.resource,
                key
            ]))?
        ))
    }
    pub fn row(&self, resource: &str, uid: Uuid) -> Result<OrdinaryRow> {
        self.rows
            .get(&(resource.to_owned(), uid))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("BINDING_INVALID"))
    }
    pub async fn check(
        &self,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
        binding: &Binding,
        snapshot: &SecuritySnapshot,
    ) -> Result<()> {
        ensure!(
            self.security_epoch == snapshot.security_epoch
                && self.instance_epoch == snapshot.instance_epoch
                && self.attributes == fingerprint(&json!(snapshot.attributes))?,
            "ACTION_PREPARATION_CHANGED"
        );
        ensure!(
            self.contract
                == NativeServing::action_contract(client, session, reference)
                    .await?
                    .0,
            "ACTION_PREPARATION_CHANGED"
        );
        ensure!(
            self.generations.contains_key(&binding.resource),
            "BINDING_INVALID"
        );
        for (resource, pinned) in &self.generations {
            ensure!(
                *pinned == generation(client, session, resource).await?,
                "ACTION_PREPARATION_CHANGED"
            );
            let uids = self
                .rows
                .keys()
                .filter_map(|(target, uid)| (target == resource).then_some(*uid))
                .collect::<Vec<_>>();
            let current=client.query("SELECT DISTINCT ON (v.uid) v.uid,v.revision::text FROM moria.overlay_versions v WHERE v.environment=$1 AND v.object_type=$2 AND v.uid=ANY($3) ORDER BY v.uid,v.revision DESC",&[&session.environment(),&resource,&uids]).await?.into_iter().map(|row|(row.get::<_,Uuid>(0),row.get::<_,String>(1))).collect::<BTreeMap<_,_>>();
            ensure!(
                self.rows
                    .iter()
                    .filter(|((target, _), _)| target == resource)
                    .all(|((_, uid), row)| current.get(uid) == row.overlay_revision.as_ref()),
                "ACTION_PREPARATION_CHANGED"
            );
        }
        Ok(())
    }
}
pub(super) async fn generation(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    resource: &str,
) -> Result<Option<(Uuid, String)>> {
    Ok(client.query_opt("SELECT g.stage,g.revision::text FROM moria.source_generations g WHERE g.environment=$1 AND g.object_type=$2 ORDER BY g.revision DESC LIMIT 1",&[&session.environment(),&resource]).await?.map(|row|(row.get(0),row.get(1))))
}
impl NativeServing {
    pub(super) async fn prepare_ordinary(
        &self,
        session: &VerifiedSession,
        reference: &str,
        key: &str,
        input: &Value,
    ) -> Result<Option<(Option<OrdinaryAction>, Value)>> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome=async {
            let client=admission.client();
            let snapshot=Authority::current(client,session).await?;
            let reference=Self::resource_reference(client,session,reference).await?;
            let definition=client.query_opt("SELECT definition FROM moria.native_serving_actions WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&reference]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
            let action:ActionDefinition=serde_json::from_value(definition.get(0))?;
            authorize(&snapshot.identity,&action.resource,"invoke",&action.markings)?;
            if action.function.is_some() || action.typed_edits.as_ref().is_some_and(|edits|edits.iter().any(|edit|matches!(edit,TypedActionEdit::Create{..}|TypedActionEdit::Link{..}|TypedActionEdit::Unlink{..}))) { return Ok(None); }
            let mut captured=None;
            let value=Self::invoke_prepared(client,session,&reference,key,input,ActionContext {db:&self.db,consumed:&[],prepared:None,ordinary:None,capture:Some(&mut captured)}).await?;
            Ok(Some((captured,value)))
        }.await;
        admission.complete(outcome).await
    }
}
