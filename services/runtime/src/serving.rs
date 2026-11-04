use crate::{
    authority::{Authority, SecuritySnapshot, VerifiedSession},
    authorization::{
        CompiledExpression, CompiledValueExpression, ExpressionTypes, ExpressionValues,
        ObjectPolicies, ObjectReader, ObjectRow, PolicyError, SqlParameter, ValueMap, authorize,
        postgres_type,
    },
    canonical::fingerprint,
    database::Database,
    values,
};
use anyhow::{Result, bail, ensure};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{ConnectInfo, State},
    http::{Method, Request, Response, StatusCode, header},
    routing::any,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::TryStreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    time::Duration,
};
use uuid::Uuid;

const RESPONSE_BYTES: usize = 8 * 1024 * 1024;
#[path = "serving_creations.rs"]
mod creations;
#[path = "serving_policy.rs"]
mod generation_policy;
#[path = "serving_link_evidence.rs"]
mod link_evidence;
#[path = "serving_links.rs"]
mod links;
#[path = "serving_narrow.rs"]
mod narrow;
#[path = "serving_ordinary.rs"]
mod ordinary;
#[path = "serving_actions.rs"]
mod typed_actions;
use generation_policy::{
    authorize_generation_query, generation_definition, generation_predicates, prepare_generation,
    stage_definition,
};
use ordinary::{OrdinaryAction, OrdinaryRow};

#[derive(Clone, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ReadEvidence {
    resource: String,
    binding_revision: String,
    revision: String,
    fields: Vec<String>,
    uids: Vec<Uuid>,
    #[serde(default)]
    population: bool,
    #[serde(default)]
    fingerprints: BTreeMap<Uuid, String>,
    #[serde(default)]
    query: Option<QueryObservation>,
    #[serde(default)]
    policies: BTreeMap<Uuid, PolicyWitness>,
    #[serde(default)]
    prior_policies: BTreeMap<Uuid, Vec<PolicyWitness>>,
    #[serde(default)]
    failure: Option<FailedRead>,
    #[serde(default)]
    link: Option<LinkObservation>,
}
#[derive(Clone, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LinkObservation {
    resource: String,
    contract: String,
    input: Value,
    result_hash: Option<String>,
    failure: Option<String>,
}
#[derive(Clone, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FailedRead {
    uid: Option<Uuid>,
    operation: String,
    input: Value,
    code: String,
}
#[derive(Clone, PartialEq, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PolicyWitness {
    source: ValueMap,
    effective: ValueMap,
}
#[derive(Clone, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueryObservation {
    filter: Option<Value>,
    projection: Vec<String>,
    sort: Vec<SortKey>,
    scope: Option<UidScope>,
    budget: Option<QueryBudget>,
    page_size: usize,
    aggregate: bool,
    after: Option<Uuid>,
    keys: Vec<Value>,
    more: bool,
    #[serde(default)]
    aggregate_spec: Option<Aggregation>,
    #[serde(default)]
    aggregate_hash: Option<String>,
}
pub(crate) struct BrokerRead {
    pub value: Value,
    pub evidence: Vec<ReadEvidence>,
    pub markings: Vec<String>,
}
pub(crate) struct PreparedAction {
    pub function: crate::functions::PreparedFunction,
    pub contract: String,
}
struct ActionContext<'a> {
    db: &'a Database,
    consumed: &'a [String],
    prepared: Option<&'a PreparedAction>,
    ordinary: Option<&'a OrdinaryAction>,
    capture: Option<&'a mut Option<OrdinaryAction>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectSchema {
    pub primary_key: String,
    pub properties: ValueMap,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequiredOperation {
    pub resource: String,
    pub operation: String,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BindingDefinition {
    object_type_id: Option<Uuid>,
    schema: ObjectSchema,
    policies: ObjectPolicies,
    #[serde(default)]
    required_operations: Vec<RequiredOperation>,
    write_policy: Option<Value>,
    #[serde(default)]
    property_write: ValueMap,
    #[serde(default)]
    source_resource: Option<Uuid>,
    #[serde(default)]
    source_mapping: BTreeMap<String, String>,
    #[serde(default)]
    identity_domain: Option<String>,
}
struct Binding {
    resource: String,
    revision: String,
    definition: BindingDefinition,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ActionDefinition {
    function: Option<String>,
    resource: String,
    revision: String,
    markings: Vec<String>,
    target_parameter: String,
    inputs: ValueMap,
    condition: Value,
    edits: Vec<ActionEdit>,
    #[serde(default)]
    effects: Vec<ActionEffect>,
    #[serde(default)]
    typed_edits: Option<Vec<TypedActionEdit>>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
enum TypedActionEdit {
    Link {
        #[serde(rename = "linkType")]
        link_type: String,
        #[serde(rename = "sourceUid")]
        source_uid: Value,
        #[serde(rename = "targetUid")]
        target_uid: Value,
    },
    Unlink {
        #[serde(rename = "linkType")]
        link_type: String,
        #[serde(rename = "sourceUid")]
        source_uid: Value,
        #[serde(rename = "targetUid")]
        target_uid: Value,
    },
    Create {
        #[serde(rename = "targetType")]
        target_type: String,
        #[serde(rename = "identityKey")]
        identity_key: Value,
        values: ValueMap,
    },
    Set {
        #[serde(rename = "targetType")]
        target_type: String,
        uid: Value,
        property: String,
        value: Value,
    },
    ClearOverride {
        #[serde(rename = "targetType")]
        target_type: String,
        uid: Value,
        property: String,
    },
    Delete {
        #[serde(rename = "targetType")]
        target_type: String,
        uid: Value,
    },
    Restore {
        #[serde(rename = "targetType")]
        target_type: String,
        uid: Value,
    },
}
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "op", deny_unknown_fields)]
enum ActionEdit {
    #[serde(rename = "set")]
    Set { property: String, value: EditValue },
    #[serde(rename = "clearOverride")]
    ClearOverride { property: String },
    #[serde(rename = "delete")]
    Delete,
    #[serde(rename = "restore")]
    Restore,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum EditValue {
    Parameter(ParameterValue),
    Literal(LiteralValue),
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ParameterValue {
    parameter: String,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiteralValue {
    literal: Value,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActionEffect {
    destination: Uuid,
    key: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LinkDefinition {
    mode: String,
    #[serde(default)]
    source_property: String,
    target_property: Option<String>,
    cardinality: Option<String>,
    read_policy: Option<Value>,
    #[serde(default)]
    attributes: ValueMap,
    markings: Vec<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Traverse {
    uid: Uuid,
    direction: String,
    projection: Option<Vec<String>>,
    scope: Option<UidScope>,
    budget: QueryBudget,
    fanout: usize,
}
fn parse_traverse(input: &Value) -> Result<Traverse> {
    canonical_uid(
        input
            .get("uid")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("QUERY_INVALID"))?,
    )?;
    validate_scope(input)?;
    let request: Traverse =
        serde_json::from_value(input.clone()).map_err(|_| anyhow::anyhow!("QUERY_INVALID"))?;
    ensure!(
        ["out", "in"].contains(&request.direction.as_str()) && (1..=1000).contains(&request.fanout),
        "QUERY_INVALID"
    );
    ensure!(
        (1..=30000).contains(&request.budget.time_ms)
            && (1..=10000).contains(&request.budget.scan_rows)
            && (1..=RESPONSE_BYTES).contains(&request.budget.response_bytes)
            && request.budget.groups.is_none(),
        "QUERY_INVALID"
    );
    if let Some(scope) = &request.scope {
        ensure!(scope.from_uid <= scope.through_uid, "QUERY_INVALID");
    }
    Ok(request)
}

pub(crate) struct Admission {
    client: Option<deadpool_postgres::Object>,
}
impl Drop for Admission {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            drop(deadpool_postgres::Object::take(client));
        }
    }
}
impl Admission {
    pub(crate) async fn global_filesystem_configuration(db: &Database) -> Result<Self> {
        let admission = Self {
            client: Some(db.get().await?),
        };
        admission
            .client()
            .query_one(
                "SELECT pg_advisory_lock(hashtextextended('moria/filesystem',0))",
                &[],
            )
            .await?;
        admission.client().batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE; SET LOCAL statement_timeout='5s'; SET LOCAL idle_in_transaction_session_timeout='5s'").await?;
        Ok(admission)
    }
    pub(crate) async fn begin(
        db: &Database,
        session: &VerifiedSession,
        write: bool,
    ) -> Result<Self> {
        Self::begin_filesystem(db, session, write, false).await
    }
    async fn begin_action_publication(
        db: &Database,
        session: &VerifiedSession,
        publication_key: &str,
        home_uids: &[Uuid],
    ) -> Result<Self> {
        let admission = Self {
            client: Some(db.get().await?),
        };
        let client = admission.client();
        // Preparation releases its shared admission before this receipt key and the
        // permanent UID homes. A Source delta can hold UID homes while preparing.
        // Wait for those rows before the environment barrier, then reauthorize fresh.
        client
            .query_one(
                "SELECT pg_advisory_lock(hashtextextended($1,0))",
                &[&publication_key],
            )
            .await?;
        client.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE; SET LOCAL statement_timeout='5s'; SET LOCAL idle_in_transaction_session_timeout='5s'").await?;
        let homes = client
            .query(
                "SELECT uid FROM moria.object_homes WHERE uid=ANY($1) ORDER BY uid FOR UPDATE",
                &[&home_uids],
            )
            .await?;
        ensure!(homes.len() == home_uids.len(), "ACTION_PREPARATION_CHANGED");
        client
            .query_one(
                "SELECT pg_advisory_lock(hashtextextended($1,0))",
                &[&format!("moria/environment/{}", session.environment())],
            )
            .await?;
        client
            .query_one(
                "SELECT pg_advisory_lock_shared(hashtextextended('moria/filesystem',0))",
                &[],
            )
            .await?;
        Ok(admission)
    }
    pub(crate) async fn begin_filesystem(
        db: &Database,
        session: &VerifiedSession,
        write: bool,
        filesystem_write: bool,
    ) -> Result<Self> {
        Self::begin_environment(db, session.environment(), write, filesystem_write).await
    }
    /// Lock admission alone establishes no actor or resource authority.
    pub(crate) async fn begin_environment(
        db: &Database,
        environment: &str,
        write: bool,
        filesystem_write: bool,
    ) -> Result<Self> {
        let admission = Self {
            client: Some(db.get().await?),
        };
        let client = admission.client();
        client
            .query_one(
                if write {
                    "SELECT pg_advisory_lock(hashtextextended($1,0))"
                } else {
                    "SELECT pg_advisory_lock_shared(hashtextextended($1,0))"
                },
                &[&format!("moria/environment/{environment}")],
            )
            .await?;
        client
            .query_one(
                if filesystem_write {
                    "SELECT pg_advisory_lock(hashtextextended('moria/filesystem',0))"
                } else {
                    "SELECT pg_advisory_lock_shared(hashtextextended('moria/filesystem',0))"
                },
                &[],
            )
            .await?;
        // Domain versions are explicitly pinned, and admitted writers need the exclusive
        // locks held above. Fresh statement snapshots let the final credential check see
        // a revocation committed while computation was blocked.
        client.batch_execute(if write {"BEGIN ISOLATION LEVEL SERIALIZABLE; SET LOCAL statement_timeout='5s'; SET LOCAL idle_in_transaction_session_timeout='5s'"}else{"BEGIN ISOLATION LEVEL READ COMMITTED; SET LOCAL statement_timeout='30s'; SET LOCAL idle_in_transaction_session_timeout='30s'"}).await?;
        Ok(admission)
    }
    pub(crate) fn client(&self) -> &tokio_postgres::Client {
        self.client.as_ref().unwrap()
    }
    pub(crate) async fn complete<T>(mut self, result: Result<T>) -> Result<T> {
        let outcome = match result {
            Ok(value) => self
                .client()
                .batch_execute("COMMIT")
                .await
                .map(|_| value)
                .map_err(Into::into),
            Err(error) => {
                self.client().batch_execute("ROLLBACK").await?;
                Err(error)
            }
        };
        self.client()
            .batch_execute("SELECT pg_advisory_unlock_all()")
            .await?;
        drop(self.client.take());
        outcome
    }
}

#[derive(Clone)]
pub struct NativeServing {
    pub db: Database,
}
pub(crate) fn binding_revision(declared: &str, generation: &str) -> Result<String> {
    fingerprint(&json!([declared, generation]))
}
pub(crate) struct ProtectedCandidates {
    /// Complete authorized relation: uid, source, effective, current_source, current_effective.
    /// Compose ranking outside this relation and append parameters after `parameters.len()`.
    pub sql: String,
    pub parameters: Vec<SqlParameter>,
    pub revision: String,
    pub resource: String,
    pub binding_revision: String,
    pub markings: Vec<String>,
    pub authority: SecuritySnapshot,
}
impl NativeServing {
    pub(crate) async fn object_contract(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
    ) -> Result<Value> {
        let snapshot = Authority::current(client, session).await?;
        let binding = Self::binding(client, session, reference).await?;
        Self::required(&snapshot, &binding)?;
        Authority::resource_access(client, session, &binding.resource, "read").await?;
        let reader = ObjectReader::compile(binding.definition.policies.clone())?.prepare(
            &snapshot.identity,
            snapshot.attributes.clone(),
            None,
            &[],
            vec![],
        )?;
        let properties = reader
            .selected()
            .iter()
            .map(|name| {
                (
                    name.clone(),
                    binding.definition.schema.properties[name].clone(),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        Authority::finish(client, session, &snapshot).await?;
        Ok(json!({"id":binding.resource,"properties":properties}))
    }
    async fn resource_reference(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
    ) -> Result<String> {
        Ok(client
            .query_one(
                "SELECT moria.runtime_data_resource($1,$2)",
                &[&session.environment(), &reference],
            )
            .await?
            .get(0))
    }
    pub(crate) async fn action_contract(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
    ) -> Result<(String, Value, String)> {
        let resolved = Self::resource_reference(client, session, reference).await?;
        let reference = resolved.as_str();
        let row=client.query_opt("SELECT definition,object_resource,authority_generation::text FROM moria.native_serving_actions WHERE environment=$1 AND resource=$2 AND enabled", &[&session.environment(),&reference]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        let definition: Value = row.get(0);
        let action: ActionDefinition = serde_json::from_value(definition.clone())?;
        let mut resources = BTreeSet::from([row.get::<_, String>(1)]);
        let mut linked = BTreeMap::new();
        if let Some(edits) = &action.typed_edits {
            ensure!(edits.len() <= 1000, "ACTION_LIMIT");
            for edit in edits {
                if let Some(resource) = edit.target() {
                    resources.insert(resource.to_owned());
                }
                if let TypedActionEdit::Link { link_type, .. }
                | TypedActionEdit::Unlink { link_type, .. } = edit
                    && !linked.contains_key(link_type)
                {
                    ensure!(linked.len() < 100, "ACTION_LIMIT");
                    let link=client.query_opt("SELECT definition,authority_generation::text,source_resource,target_resource FROM moria.native_serving_links WHERE environment=$1 AND resource=$2 AND enabled", &[&session.environment(),link_type]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
                    for resource in [link.get::<_, String>(2), link.get::<_, String>(3)] {
                        resources.insert(resource);
                    }
                    linked.insert(
                        link_type.clone(),
                        json!([link.get::<_, Value>(0), link.get::<_, String>(1)]),
                    );
                }
                ensure!(resources.len() <= 100, "ACTION_LIMIT");
            }
        }
        let mut contracts = BTreeMap::new();
        for resource in resources {
            let binding = Self::binding(client, session, &resource).await?;
            contracts.insert(resource, json!(binding.revision));
        }
        Ok((
            fingerprint(&json!([
                definition,
                row.get::<_, String>(2),
                contracts,
                linked
            ]))?,
            definition,
            row.get(1),
        ))
    }
    /// Caller holds normal environment/filesystem read admission and must call Authority::finish
    /// with the returned snapshot in the same transaction after ranking and before release.
    pub(crate) async fn protected_candidates(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
        fields: &[String],
        pinned_revision: Option<&str>,
    ) -> Result<ProtectedCandidates> {
        let snapshot = Authority::current(client, session).await?;
        let binding = Self::binding(client, session, reference).await?;
        Self::required(&snapshot, &binding)?;
        Self::schema(client, session, &binding).await?;
        let mut markings =
            Authority::resource_access(client, session, &binding.resource, "read").await?;
        markings.extend(binding.definition.policies.markings.clone());
        markings.sort();
        markings.dedup();
        let mut required = fields.to_vec();
        required.push(binding.definition.schema.primary_key.clone());
        for field in fields {
            ensure!(
                scalar_type(&binding, field)?["type"] == "string",
                "VALUE_INVALID"
            );
        }
        ObjectReader::compile(binding.definition.policies.clone())?.prepare(
            &snapshot.identity,
            snapshot.attributes.clone(),
            Some(required.clone()),
            &required,
            vec![],
        )?;
        let current = Self::revision(client, session, &snapshot).await?;
        let revision = pinned_revision.unwrap_or(&current);
        ensure!(
            revision.parse::<i64>().is_ok_and(
                |revision| revision >= 0 && revision <= current.parse::<i64>().unwrap_or(-1)
            ) && revision
                .parse::<i64>()
                .is_ok_and(|value| value.to_string() == revision),
            "INPUT_INVALID"
        );
        if pinned_revision.is_some() {
            client
                .query_one(
                    "SELECT moria.runtime_history_assert($1,$2::text::bigint)",
                    &[&session.environment(), &revision],
                )
                .await?;
        }
        let pin = ReadPin {
            revision: revision.into(),
            after: None,
            keys: vec![],
            expires: String::new(),
        };
        let query = Query {
            filter: None,
            projection: Some(required),
            page_size: 1,
            cursor: None,
            sort: vec![],
            scope: None,
            budget: None,
            aggregation: None,
        };
        authorize_generation_query(client, session, &binding, revision, &current, &query).await?;
        let (base,parameters)=canonical_candidates(client,session.environment(),&binding,&snapshot.attributes,&pin,&current,&query).await?.unwrap_or_else(||("SELECT NULL::uuid AS uid,NULL::jsonb AS source,NULL::jsonb AS effective,NULL::jsonb AS current_source,NULL::jsonb AS current_effective WHERE FALSE".into(),vec![SqlParameter::Scalar(None);6]));
        Ok(ProtectedCandidates {
            sql: format!(
                "WITH candidate_settings AS (SELECT $1::text,$2::text,$3::text::bigint,$4::text::bigint,$5::text::uuid,$6::text::bigint) {base}"
            ),
            parameters,
            revision: revision.into(),
            resource: binding.resource,
            binding_revision: binding.revision,
            markings,
            authority: snapshot,
        })
    }
    async fn binding(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
    ) -> Result<Binding> {
        let resolved: String = client
            .query_one(
                "SELECT moria.runtime_data_resource($1,$2)",
                &[&session.environment(), &reference],
            )
            .await?
            .get(0);
        let reference = resolved.as_str();
        let row=if let Some(id)=Uuid::parse_str(reference).ok().filter(|id|id.to_string()==reference) {
            client.query_opt("SELECT resource,object_type_id,revision,definition,authority_generation::text FROM moria.native_serving_bindings WHERE environment=$1 AND object_type_id=$2 AND enabled",&[&session.environment(),&id]).await?
        }else{
            client.query_opt("SELECT resource,object_type_id,revision,definition,authority_generation::text FROM moria.native_serving_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&reference]).await?
        }.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        let definition: BindingDefinition = serde_json::from_value(row.get(3))?;
        let resource: String = row.get(0);
        ensure!(
            definition.policies.resource == resource
                && definition.object_type_id == row.get::<_, Option<Uuid>>(1),
            "BINDING_INVALID"
        );
        validate_schema(&definition.schema)?;
        ObjectReader::compile(definition.policies.clone())?;
        for operation in &definition.required_operations {
            ensure!(
                !operation.resource.is_empty() && !operation.operation.is_empty(),
                "BINDING_INVALID"
            );
        }
        Ok(Binding {
            resource,
            revision: binding_revision(&row.get::<_, String>(2), &row.get::<_, String>(4))?,
            definition,
        })
    }
    async fn schema(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        binding: &Binding,
    ) -> Result<()> {
        let row=client.query_opt("SELECT object_schema,required_markings FROM moria.source_generations WHERE environment=$1 AND object_type=$2 ORDER BY revision DESC LIMIT 1",&[&session.environment(),&binding.resource]).await?;
        if let Some(row) = row {
            if let Some(schema) = row.get::<_, Option<Value>>(0) {
                ensure!(
                    normalize_schema(&binding.definition.schema)? == schema,
                    "SCHEMA_CHANGED"
                );
            }
            let labels: Vec<String> = row.get(1);
            ensure!(
                labels
                    .iter()
                    .all(|label| binding.definition.policies.markings.contains(label)),
                "MARKING_DOWNGRADE"
            );
        }
        Ok(())
    }
    fn required(snapshot: &SecuritySnapshot, binding: &Binding) -> Result<()> {
        ensure!(
            snapshot.private_purpose.as_deref() != Some("indexing"),
            "FORBIDDEN"
        );
        for required in &binding.definition.required_operations {
            authorize(
                &snapshot.identity,
                &required.resource,
                &required.operation,
                &[],
            )?;
        }
        Ok(())
    }
    async fn revision(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        snapshot: &SecuritySnapshot,
    ) -> Result<String> {
        let row=client.query_one("SELECT revision::text,security_epoch::text FROM moria.environments WHERE id=$1 FOR SHARE",&[&session.environment()]).await?;
        ensure!(
            row.get::<_, String>(1) == snapshot.security_epoch,
            "AUTHORIZATION_CHANGED"
        );
        Ok(row.get(0))
    }
    pub async fn read(
        &self,
        session: &VerifiedSession,
        reference: &str,
        uid: &str,
        projection: Option<Vec<String>>,
    ) -> Result<Value> {
        tokio::time::timeout(
            Duration::from_secs(30),
            self.read_attempt(session, reference, uid, projection),
        )
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")))
    }
    async fn read_attempt(
        &self,
        session: &VerifiedSession,
        reference: &str,
        uid: &str,
        projection: Option<Vec<String>>,
    ) -> Result<Value> {
        let uid = canonical_uid(uid)?;
        ensure!(
            projection
                .as_ref()
                .is_none_or(|fields| fields.len() <= 1000),
            "QUERY_INVALID"
        );
        let admission = Admission::begin(&self.db, session, false).await?;
        let result =
            Self::read_transaction(admission.client(), session, reference, uid, projection).await;
        admission.complete(result).await
    }
    async fn read_transaction(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
        uid: Uuid,
        projection: Option<Vec<String>>,
    ) -> Result<Value> {
        let snapshot = Authority::current(client, session).await?;
        let binding = Self::binding(client, session, reference).await?;
        Self::required(&snapshot, &binding)?;
        Authority::resource_access(client, session, &binding.resource, "read").await?;
        Self::schema(client, session, &binding).await?;
        let project = ObjectReader::compile(binding.definition.policies.clone())?.prepare(
            &snapshot.identity,
            snapshot.attributes.clone(),
            projection,
            std::slice::from_ref(&binding.definition.schema.primary_key),
            vec![],
        )?;
        let revision = Self::revision(client, session, &snapshot).await?;
        let row = read_object(
            client,
            session,
            &binding,
            uid,
            &revision,
            project.selected(),
        )
        .await?;
        let object = project
            .project(row.as_ref(), row.as_ref())?
            .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?;
        Authority::finish(client, session, &snapshot).await?;
        let result = json!({"revision":revision,"object":object});
        ensure!(
            serde_json::to_vec(&result)?.len() <= RESPONSE_BYTES,
            "RESPONSE_TOO_LARGE"
        );
        Ok(result)
    }
    pub async fn query(
        &self,
        session: &VerifiedSession,
        reference: &str,
        input: &Value,
    ) -> Result<Value> {
        let query = Query::parse(input)?;
        tokio::time::timeout(
            Duration::from_millis(query.budget.as_ref().map_or(30000, |budget| budget.time_ms)),
            self.query_attempt(session, reference, &query),
        )
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")))
    }
    async fn query_attempt(
        &self,
        session: &VerifiedSession,
        reference: &str,
        query: &Query,
    ) -> Result<Value> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let result = Self::query_transaction(admission.client(), session, reference, query).await;
        admission.complete(result).await
    }
    pub async fn aggregate(
        &self,
        session: &VerifiedSession,
        reference: &str,
        input: &Value,
    ) -> Result<Value> {
        let query = parse_aggregate(input)?;
        tokio::time::timeout(
            Duration::from_millis(query.budget.as_ref().unwrap().time_ms),
            self.query_attempt(session, reference, &query),
        )
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")))
    }
    pub async fn traverse(
        &self,
        session: &VerifiedSession,
        reference: &str,
        input: &Value,
    ) -> Result<Value> {
        let request = parse_traverse(input)?;
        tokio::time::timeout(Duration::from_millis(request.budget.time_ms), async {
            let admission = Admission::begin(&self.db, session, false).await?;
            let result =
                Self::traverse_transaction(admission.client(), session, reference, &request).await;
            admission.complete(result).await
        })
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")))
    }
    async fn traverse_transaction(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
        request: &Traverse,
    ) -> Result<Value> {
        Self::traverse_recorded(client, session, reference, request, None).await
    }
    async fn traverse_recorded(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
        request: &Traverse,
        mut evidence: Option<&mut Vec<ReadEvidence>>,
    ) -> Result<Value> {
        let resolved = Self::resource_reference(client, session, reference).await?;
        let reference = resolved.as_str();
        let snapshot = Authority::current(client, session).await?;
        let link=client.query_opt("SELECT source_resource,target_resource,definition FROM moria.native_serving_links WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&reference]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        let definition: LinkDefinition =
            serde_json::from_value(link.get(2)).map_err(|_| anyhow::anyhow!("BINDING_INVALID"))?;
        if definition.mode != "uidReference" {
            return links::traverse_in(
                client,
                session,
                reference,
                request,
                &link,
                &definition,
                (&snapshot, evidence),
            )
            .await;
        }
        ensure!(
            request.direction != "in" || request.scope.is_some(),
            "UNSUPPORTED_CAPABILITY"
        );
        authorize(&snapshot.identity, reference, "read", &definition.markings)?;
        Authority::resource_access(client, session, reference, "read").await?;
        let source = Self::binding(client, session, &link.get::<_, String>(0)).await?;
        let target = Self::binding(client, session, &link.get::<_, String>(1)).await?;
        for binding in [&source, &target] {
            Self::required(&snapshot, binding)?;
            Self::schema(client, session, binding).await?;
            Authority::resource_access(client, session, &binding.resource, "read").await?;
        }
        ensure!(
            scalar_type(&source, &definition.source_property)?["type"] == "uid",
            "BINDING_INVALID"
        );
        let source_reader = ObjectReader::compile(source.definition.policies.clone())?;
        let source_projection = source_reader.prepare(
            &snapshot.identity,
            snapshot.attributes.clone(),
            if request.direction == "out" {
                Some(vec![definition.source_property.clone()])
            } else {
                request.projection.clone()
            },
            &[
                definition.source_property.clone(),
                source.definition.schema.primary_key.clone(),
            ],
            vec![],
        )?;
        let target_reader = ObjectReader::compile(target.definition.policies.clone())?;
        let target_projection = target_reader.prepare(
            &snapshot.identity,
            snapshot.attributes.clone(),
            if request.direction == "in" {
                Some(vec![])
            } else {
                request.projection.clone()
            },
            std::slice::from_ref(&target.definition.schema.primary_key),
            vec![],
        )?;
        let revision = Self::revision(client, session, &snapshot).await?;
        let (anchor, projection) = if request.direction == "out" {
            (&source, &source_projection)
        } else {
            (&target, &target_projection)
        };
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
        Self::append_link_points(
            (
                anchor,
                &revision,
                projection.selected().to_vec(),
                vec![request.uid],
            ),
            evidence.as_deref_mut(),
        );
        let objects = if request.direction == "out" {
            if let Some(uid) = object
                .get(&definition.source_property)
                .and_then(Value::as_str)
            {
                let row = read_object(
                    client,
                    session,
                    &target,
                    canonical_uid(uid)?,
                    &revision,
                    target_projection.selected(),
                )
                .await?;
                if row.is_some() {
                    Self::append_link_points(
                        (
                            &target,
                            &revision,
                            target_projection.selected().to_vec(),
                            vec![canonical_uid(uid)?],
                        ),
                        evidence.as_deref_mut(),
                    );
                }
                target_projection
                    .project(row.as_ref(), row.as_ref())?
                    .into_iter()
                    .collect::<Vec<_>>()
            } else {
                vec![]
            }
        } else {
            let query = Query::parse(
                &json!({"filter":{"op":"eq","args":[{"field":definition.source_property},{"literal":request.uid.to_string()}]},"projection":request.projection.clone().unwrap_or_else(||source_projection.selected().to_vec()),"pageSize":request.fanout,"scope":request.scope,"budget":request.budget}),
            )?;
            let result =
                Self::query_recorded(client, session, &source.resource, &query, evidence).await?;
            ensure!(result.get("nextCursor").is_none(), "RESOURCE_LIMIT");
            serde_json::from_value(result["objects"].clone())?
        };
        Authority::finish(client, session, &snapshot).await?;
        let result = json!({"revision":revision,"objects":objects});
        ensure!(
            serde_json::to_vec(&result)?.len() <= request.budget.response_bytes,
            "RESPONSE_TOO_LARGE"
        );
        Ok(result)
    }
    async fn query_transaction(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
        query: &Query,
    ) -> Result<Value> {
        Self::query_recorded(client, session, reference, query, None).await
    }
    async fn query_recorded(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
        query: &Query,
        evidence: Option<&mut Vec<ReadEvidence>>,
    ) -> Result<Value> {
        let snapshot = Authority::current(client, session).await?;
        let binding = Self::binding(client, session, reference).await?;
        Self::required(&snapshot, &binding)?;
        Authority::resource_access(client, session, &binding.resource, "read").await?;
        Self::schema(client, session, &binding).await?;
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
        let project = ObjectReader::compile(binding.definition.policies.clone())?.prepare(
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
        for key in &query.sort {
            scalar_type(&binding, &key.property)?;
        }
        if let Some(aggregation) = &query.aggregation {
            validate_aggregation(aggregation, &binding)?;
        }
        let predicate = query
            .filter
            .as_ref()
            .map(|filter| {
                CompiledExpression::compile(
                    filter,
                    ExpressionTypes {
                        fields: binding.definition.policies.properties.clone(),
                        ..Default::default()
                    },
                )
            })
            .transpose()?;
        let current = Self::revision(client, session, &snapshot).await?;
        let digest = fingerprint(
            &json!({"environment":session.environment(),"principal":session.principal(),"application":session.application(),"delegation":[],"contract":binding.revision,"query":{"resource":binding.resource,"filter":query.filter,"projection":query.projection,"pageSize":query.page_size,"sort":query.sort,"scope":query.scope,"budget":query.budget,"aggregation":query.aggregation}}),
        )?;
        let pin = if let Some(cursor) = &query.cursor {
            let row=client.query_opt("SELECT binding_hash,security_epoch::text,revision::text,position,expires_at::text,expires_at<=clock_timestamp() AS expired FROM moria.read_cursors WHERE token_hash=$1",&[&hash(cursor)]).await?.ok_or_else(||anyhow::anyhow!("CURSOR_INVALID"))?;
            ensure!(row.get::<_, String>(0) == digest, "CURSOR_INVALID");
            ensure!(!row.get::<_, bool>(5), "CURSOR_EXPIRED");
            ensure!(
                row.get::<_, String>(1) == snapshot.security_epoch,
                "CURSOR_INVALIDATED"
            );
            let after: Value = row.get(3);
            let (uid, keys) = if query.sort.is_empty() {
                (
                    after
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("CURSOR_INVALID"))?,
                    vec![],
                )
            } else {
                let uid = after
                    .get("uid")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("CURSOR_INVALID"))?;
                let keys = after
                    .get("keys")
                    .and_then(Value::as_array)
                    .ok_or_else(|| anyhow::anyhow!("CURSOR_INVALID"))?
                    .clone();
                ensure!(keys.len() == query.sort.len(), "CURSOR_INVALID");
                for (key, value) in query.sort.iter().zip(&keys) {
                    ensure!(
                        values::normalize(value, scalar_type(&binding, &key.property)?)? == *value,
                        "CURSOR_INVALID"
                    );
                }
                (uid, keys)
            };
            ReadPin {
                revision: row.get(2),
                after: Some(canonical_uid(uid)?),
                keys,
                expires: row.get(4),
            }
        } else {
            ReadPin {
                revision: current.clone(),
                after: None,
                keys: vec![],
                expires: client
                    .query_one(
                        "SELECT (clock_timestamp()+interval '15 minutes')::text",
                        &[],
                    )
                    .await?
                    .get(0),
            }
        };
        if query.cursor.is_some() {
            client
                .query_one(
                    "SELECT moria.runtime_history_assert($1,$2::text::bigint)",
                    &[&session.environment(), &pin.revision],
                )
                .await?;
        }
        authorize_generation_query(client, session, &binding, &pin.revision, &current, query)
            .await?;
        let execution_query = if query.aggregation.is_none() && query.projection.is_none() {
            let mut request = query.clone();
            request.projection = Some(project.selected().to_vec());
            Some(request)
        } else {
            None
        };
        let rows = query_rows(
            client,
            session.environment(),
            &binding,
            &snapshot.attributes,
            &pin,
            &current,
            execution_query.as_ref().unwrap_or(query),
        )
        .await?;
        let mut observed = ReadEvidence {
            resource: binding.resource.clone(),
            binding_revision: binding.revision.clone(),
            revision: pin.revision.clone(),
            fields: fields.clone(),
            uids: vec![],
            population: true,
            fingerprints: BTreeMap::new(),
            policies: BTreeMap::new(),
            prior_policies: BTreeMap::new(),
            failure: None,
            link: None,
            query: Some(QueryObservation {
                filter: query.filter.clone(),
                projection: project.selected().to_vec(),
                sort: query.sort.clone(),
                scope: query.scope.clone(),
                budget: query.budget.clone(),
                page_size: query.page_size,
                aggregate: query.aggregation.is_some(),
                after: pin.after,
                keys: pin.keys.clone(),
                more: false,
                aggregate_spec: if query.scope.is_none() {
                    query.aggregation.clone()
                } else {
                    None
                },
                aggregate_hash: None,
            }),
        };
        observed.fields.extend(project.selected().iter().cloned());
        if let Some(aggregation) = &query.aggregation {
            let max_groups = query.budget.as_ref().unwrap().groups.unwrap();
            ensure!(rows.len() <= max_groups, "RESOURCE_LIMIT");
            let types = aggregate_types(aggregation, &binding)?;
            let mut groups = rows
                .iter()
                .map(|row| row.get::<_, Value>(0))
                .collect::<Vec<_>>();
            for group in &mut groups {
                if let Some(uids) = group.get("_readUids").and_then(Value::as_array) {
                    for uid in uids {
                        observed.uids.push(canonical_uid(
                            uid.as_str()
                                .ok_or_else(|| anyhow::anyhow!("POLICY_MISMATCH"))?,
                        )?);
                    }
                }
                group.as_object_mut().unwrap().remove("_readUids");
                for (name, schema) in &types {
                    group["values"][name] = values::normalize(&group["values"][name], schema)
                        .map_err(|_| anyhow::anyhow!("VALUE_INVALID"))?;
                }
                for property in &aggregation.group_by {
                    group["key"][property] = values::normalize(
                        &group["key"][property],
                        scalar_type(&binding, property)?,
                    )
                    .map_err(|_| anyhow::anyhow!("VALUE_INVALID"))?;
                }
            }
            let result = json!({"revision":pin.revision,"groups":groups,"types":types});
            if let Some(observation) = observed
                .query
                .as_mut()
                .filter(|query| query.aggregate_spec.is_some())
            {
                observation.aggregate_hash = Some(fingerprint(
                    &json!({"groups":result["groups"],"types":result["types"]}),
                )?);
                observed.uids.clear();
            }
            ensure!(
                serde_json::to_vec(&result)?.len() <= query.budget.as_ref().unwrap().response_bytes,
                "RESPONSE_TOO_LARGE"
            );
            Authority::finish(client, session, &snapshot).await?;
            if let Some(evidence) = evidence {
                evidence.push(observed);
            }
            return Ok(result);
        }
        let mut objects = Vec::new();
        let mut after = None;
        let mut bytes = 0usize;
        for (index, row) in rows.iter().enumerate() {
            let pinned: ObjectRow = ObjectRow {
                source: serde_json::from_value(row.get(1))?,
                effective: serde_json::from_value(row.get(2))?,
            };
            let current: ObjectRow = ObjectRow {
                source: serde_json::from_value(row.get(3))?,
                effective: serde_json::from_value(row.get(4))?,
            };
            let object = project
                .project(Some(&pinned), Some(&current))?
                .ok_or_else(|| anyhow::anyhow!("POLICY_MISMATCH"))?;
            if let Some(predicate) = &predicate {
                ensure!(
                    predicate.allows(&ExpressionValues {
                        fields: pinned.effective.clone(),
                        ..Default::default()
                    })?,
                    "POLICY_MISMATCH"
                );
            }
            if index < query.page_size {
                observed.uids.push(row.get(0));
                bytes = bytes
                    .checked_add(serde_json::to_vec(&object)?.len())
                    .ok_or_else(|| anyhow::anyhow!("RESPONSE_TOO_LARGE"))?;
                ensure!(
                    bytes
                        <= query
                            .budget
                            .as_ref()
                            .map_or(RESPONSE_BYTES, |budget| budget.response_bytes),
                    "RESPONSE_TOO_LARGE"
                );
                objects.push(object);
                after = Some(if query.sort.is_empty() {
                    json!(row.get::<_, Uuid>(0).to_string())
                } else {
                    let keys = query
                        .sort
                        .iter()
                        .map(|key| {
                            values::normalize(
                                pinned.effective.get(&key.property).unwrap_or(&Value::Null),
                                scalar_type(&binding, &key.property)?,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    json!({"uid":row.get::<_,Uuid>(0).to_string(),"keys":keys})
                });
            }
        }
        let next = if rows.len() > query.page_size {
            let token = token()?;
            let position = after.unwrap();
            let inserted=client.execute("INSERT INTO moria.read_cursors SELECT $1,$2,$3,$4::text::bigint,$5::text::bigint,$6,$7::text::timestamptz WHERE $7::text::timestamptz>clock_timestamp()",&[&hash(&token),&session.environment(),&digest,&snapshot.security_epoch,&pin.revision,&position,&pin.expires]).await?;
            ensure!(inserted == 1, "CURSOR_EXPIRED");
            Some(token)
        } else {
            None
        };
        Authority::finish(client, session, &snapshot).await?;
        let mut result = json!({"revision":pin.revision,"objects":objects});
        if let Some(next) = next {
            result["nextCursor"] = next.into();
        }
        ensure!(
            serde_json::to_vec(&result)?.len()
                <= query
                    .budget
                    .as_ref()
                    .map_or(RESPONSE_BYTES, |budget| budget.response_bytes),
            "RESPONSE_TOO_LARGE"
        );
        if let Some(evidence) = evidence {
            evidence.push(observed);
        }
        Ok(result)
    }
    pub(crate) async fn read_for_broker(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
        uid: Option<Uuid>,
        operation: &str,
        input: &Value,
    ) -> Result<BrokerRead> {
        let mut evidence = vec![];
        let value = if let Some(uid) = uid {
            let projection = input
                .get("projection")
                .map(|value| serde_json::from_value::<Vec<String>>(value.clone()))
                .transpose()
                .map_err(|_| anyhow::anyhow!("QUERY_INVALID"))?;
            let value = Self::read_transaction(client, session, reference, uid, projection).await?;
            let binding = Self::binding(client, session, reference).await?;
            let mut fields = value["object"]
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            fields.push(binding.definition.schema.primary_key.clone());
            evidence.push(ReadEvidence {
                resource: binding.resource,
                binding_revision: binding.revision,
                revision: value["revision"].as_str().unwrap().into(),
                fields,
                uids: vec![uid],
                population: false,
                fingerprints: BTreeMap::new(),
                query: None,
                policies: BTreeMap::new(),
                prior_policies: BTreeMap::new(),
                failure: None,
                link: None,
            });
            value
        } else {
            let query = if operation == "aggregate" {
                parse_aggregate(input)?
            } else {
                Query::parse(input)?
            };
            Self::query_recorded(client, session, reference, &query, Some(&mut evidence)).await?
        };
        let mut markings = BTreeSet::new();
        for read in &mut evidence {
            Self::capture_read(client, session, read).await?;
            if read
                .query
                .as_ref()
                .is_some_and(|query| query.aggregate_spec.is_some())
            {
                markings.extend(
                    Self::root_for_broker(client, session, &read.resource)
                        .await?
                        .markings,
                );
            } else {
                markings.extend(Self::verify_read(client, session, read, false).await?);
            }
        }
        Ok(BrokerRead {
            value,
            evidence,
            markings: markings.into_iter().collect(),
        })
    }
    pub(crate) async fn root_for_broker(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
    ) -> Result<BrokerRead> {
        let snapshot = Authority::current(client, session).await?;
        let binding = Self::binding(client, session, reference).await?;
        Self::required(&snapshot, &binding)?;
        ObjectReader::compile(binding.definition.policies.clone())?.prepare(
            &snapshot.identity,
            snapshot.attributes.clone(),
            Some(vec![]),
            &[],
            vec![],
        )?;
        let mut markings = binding.definition.policies.markings;
        markings
            .extend(Authority::resource_access(client, session, &binding.resource, "read").await?);
        let revision = Self::revision(client, session, &snapshot).await?;
        Authority::finish(client, session, &snapshot).await?;
        Ok(BrokerRead {
            value: Value::Null,
            markings,
            evidence: vec![ReadEvidence {
                resource: binding.resource,
                binding_revision: binding.revision,
                revision,
                fields: vec![],
                uids: vec![],
                population: false,
                fingerprints: BTreeMap::new(),
                policies: BTreeMap::new(),
                prior_policies: BTreeMap::new(),
                query: None,
                failure: None,
                link: None,
            }],
        })
    }
    pub(crate) fn failed_read(
        read: &mut ReadEvidence,
        uid: Option<Uuid>,
        operation: &str,
        input: &Value,
        code: &str,
    ) {
        read.failure = Some(FailedRead {
            uid,
            operation: operation.into(),
            input: input.clone(),
            code: code.into(),
        });
    }
    pub(crate) async fn verify_read(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        read: &ReadEvidence,
        strict: bool,
    ) -> Result<Vec<String>> {
        Self::verify_read_mode(client, session, read, strict, true).await
    }
    async fn verify_read_mode(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        read: &ReadEvidence,
        _strict: bool,
        compare: bool,
    ) -> Result<Vec<String>> {
        let snapshot = Authority::current(client, session).await?;
        let binding = Self::binding(client, session, &read.resource).await?;
        ensure!(binding.revision == read.binding_revision, "SCHEMA_CHANGED");
        Self::required(&snapshot, &binding)?;
        Self::schema(client, session, &binding).await?;
        let project = ObjectReader::compile(binding.definition.policies.clone())?.prepare(
            &snapshot.identity,
            snapshot.attributes.clone(),
            Some(vec![]),
            &read.fields,
            vec![],
        )?;
        let revision = Self::revision(client, session, &snapshot).await?;
        client
            .query_one(
                "SELECT moria.runtime_history_assert($1,$2::text::bigint)",
                &[&session.environment(), &read.revision],
            )
            .await?;
        let captured = generation_definition(
            client,
            session.environment(),
            &binding.resource,
            &read.revision,
        )
        .await?;
        let historical = captured
            .as_ref()
            .map(|captured| prepare_generation(&snapshot, &binding, captured, &[], &read.fields))
            .transpose()?;
        for witnesses in read.prior_policies.values() {
            for witness in witnesses {
                let row = ObjectRow {
                    source: witness.source.clone(),
                    effective: witness.effective.clone(),
                };
                ensure!(
                    project.project(Some(&row), Some(&row))?.is_some(),
                    "AUTHORIZATION_CHANGED"
                );
                if let Some(historical) = &historical {
                    ensure!(
                        historical.project(Some(&row), Some(&row))?.is_some(),
                        "AUTHORIZATION_CHANGED"
                    );
                }
            }
        }
        for uid in &read.uids {
            let witness = read
                .policies
                .get(uid)
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_INPUT_CHANGED"))?;
            let pinned = ObjectRow {
                source: witness.source.clone(),
                effective: witness.effective.clone(),
            };
            if let Some(historical) = &historical {
                ensure!(
                    historical.project(Some(&pinned), Some(&pinned))?.is_some(),
                    "AUTHORIZATION_CHANGED"
                );
            }
            let current =
                read_object(client, session, &binding, *uid, &revision, &read.fields).await?;
            ensure!(
                project.project(Some(&pinned), current.as_ref())?.is_some(),
                "AUTHORIZATION_CHANGED"
            );
            if compare {
                ensure!(
                    read.fingerprints.get(uid)
                        == Some(&view_fingerprint(
                            &binding,
                            &read.fields,
                            current.as_ref().unwrap()
                        )?),
                    "FUNCTION_INPUT_CHANGED"
                );
            }
        }
        if let Some(query) = read.query.as_ref().filter(|_| compare) {
            if query.aggregate_spec.is_some() {
                ensure!(
                    query.aggregate_hash.as_ref()
                        == Some(
                            &Self::aggregate_observation(
                                client,
                                session,
                                &binding,
                                &snapshot.attributes,
                                read,
                                &revision
                            )
                            .await?
                        ),
                    "FUNCTION_INPUT_CHANGED"
                );
                if read.revision != revision {
                    let mut current = read.clone();
                    current.revision = revision.clone();
                    ensure!(
                        query.aggregate_hash.as_ref()
                            == Some(
                                &Self::aggregate_observation(
                                    client,
                                    session,
                                    &binding,
                                    &snapshot.attributes,
                                    &current,
                                    &revision
                                )
                                .await?
                            ),
                        "FUNCTION_INPUT_CHANGED"
                    );
                }
            } else {
                let (rows, more) = Self::evidence_rows(
                    client,
                    session,
                    &binding,
                    &snapshot.attributes,
                    read,
                    &revision,
                    &revision,
                )
                .await?;
                let ids = rows.iter().map(|(uid, _)| *uid).collect::<Vec<_>>();
                ensure!(
                    ids == read.uids && more == query.more,
                    "FUNCTION_INPUT_CHANGED"
                );
                for (uid, row) in rows {
                    ensure!(
                        read.fingerprints.get(&uid)
                            == Some(&view_fingerprint(&binding, &read.fields, &row)?),
                        "FUNCTION_INPUT_CHANGED"
                    );
                }
            }
        }
        if let Some(failure) = read.failure.as_ref().filter(|_| compare) {
            client
                .batch_execute("SAVEPOINT native_failed_read_check")
                .await?;
            let observed = if let Some(uid) = failure.uid {
                let projection = failure
                    .input
                    .get("projection")
                    .map(|value| serde_json::from_value::<Vec<String>>(value.clone()))
                    .transpose()
                    .map_err(|_| anyhow::anyhow!("QUERY_INVALID"))?;
                Self::read_transaction(client, session, &read.resource, uid, projection).await
            } else {
                let query = if failure.operation == "aggregate" {
                    parse_aggregate(&failure.input)
                } else {
                    Query::parse(&failure.input)
                };
                match query {
                    Ok(query) => {
                        Self::query_transaction(client, session, &read.resource, &query).await
                    }
                    Err(error) => Err(error),
                }
            };
            client.batch_execute("ROLLBACK TO SAVEPOINT native_failed_read_check; RELEASE SAVEPOINT native_failed_read_check").await?;
            ensure!(
                observed
                    .as_ref()
                    .is_err_and(|error| error_code(error) == failure.code),
                "FUNCTION_INPUT_CHANGED"
            );
        }
        let mut markings = binding.definition.policies.markings;
        markings
            .extend(Authority::resource_access(client, session, &binding.resource, "read").await?);
        if let Some(observation) = &read.link {
            markings.extend(
                Self::verify_link_observation(client, session, observation, compare).await?,
            );
        }
        Authority::finish(client, session, &snapshot).await?;
        Ok(markings)
    }
    pub(crate) async fn refresh_read(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        read: &mut ReadEvidence,
    ) -> Result<Vec<String>> {
        let markings = Self::verify_read_mode(client, session, read, false, false).await?;
        let snapshot = Authority::current(client, session).await?;
        for (uid, witness) in &read.policies {
            let previous = read.prior_policies.entry(*uid).or_default();
            if !previous.contains(witness) {
                ensure!(previous.len() < 64, "FUNCTION_READ_LIMIT");
                previous.push(witness.clone());
            }
        }
        read.revision = Self::revision(client, session, &snapshot).await?;
        if let Some(query) = read
            .query
            .as_mut()
            .filter(|query| query.aggregate_spec.is_some())
        {
            query.aggregate_hash = None;
        }
        Self::capture_read(client, session, read).await?;
        Ok(markings)
    }
    async fn evidence_rows(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        binding: &Binding,
        attributes: &ValueMap,
        read: &ReadEvidence,
        pinned: &str,
        current: &str,
    ) -> Result<(Vec<(Uuid, ObjectRow)>, bool)> {
        if let Some(observation) = &read.query {
            let page = if observation.aggregate {
                observation
                    .budget
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("FUNCTION_INPUT_CHANGED"))?
                    .scan_rows
            } else {
                observation.page_size
            };
            let query = Query {
                filter: observation.filter.clone(),
                projection: Some(observation.projection.clone()),
                page_size: page,
                cursor: None,
                sort: if observation.aggregate {
                    vec![]
                } else {
                    observation.sort.clone()
                },
                scope: observation.scope.clone(),
                budget: observation.budget.clone(),
                aggregation: None,
            };
            let pin = ReadPin {
                revision: pinned.into(),
                after: observation.after,
                keys: observation.keys.clone(),
                expires: String::new(),
            };
            authorize_generation_query(client, session, binding, &pin.revision, current, &query)
                .await?;
            let rows = query_rows(
                client,
                session.environment(),
                binding,
                attributes,
                &pin,
                current,
                &query,
            )
            .await?;
            let more = rows.len() > page;
            let values = rows
                .into_iter()
                .take(page)
                .map(|row| {
                    Ok((
                        row.get(0),
                        ObjectRow {
                            source: serde_json::from_value(row.get(1))?,
                            effective: serde_json::from_value(row.get(2))?,
                        },
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((values, more))
        } else {
            let mut rows = vec![];
            for uid in &read.uids {
                let row = read_object(client, session, binding, *uid, pinned, &read.fields)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("FUNCTION_INPUT_CHANGED"))?;
                rows.push((*uid, row));
            }
            Ok((rows, false))
        }
    }
    async fn capture_read(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        read: &mut ReadEvidence,
    ) -> Result<()> {
        let snapshot = Authority::current(client, session).await?;
        let binding = Self::binding(client, session, &read.resource).await?;
        let current = Self::revision(client, session, &snapshot).await?;
        if read
            .query
            .as_ref()
            .is_some_and(|query| query.aggregate_spec.is_some())
        {
            if read.query.as_ref().unwrap().aggregate_hash.is_none() {
                let hash = Self::aggregate_observation(
                    client,
                    session,
                    &binding,
                    &snapshot.attributes,
                    read,
                    &current,
                )
                .await?;
                read.query.as_mut().unwrap().aggregate_hash = Some(hash);
            }
            read.uids.clear();
            read.fingerprints.clear();
            read.policies.clear();
            return Ok(());
        }
        let (rows, more) = Self::evidence_rows(
            client,
            session,
            &binding,
            &snapshot.attributes,
            read,
            &read.revision,
            &current,
        )
        .await?;
        read.uids = rows.iter().map(|(uid, _)| *uid).collect();
        read.fingerprints.clear();
        read.policies.clear();
        if let Some(query) = &mut read.query {
            query.more = more;
        }
        let mut source_fields = vec![];
        let mut effective_fields = vec![];
        if let Some(policy) = &binding.definition.policies.row {
            query_fields(policy, &mut source_fields);
        }
        if let Some(policy) = &binding.definition.policies.object_read {
            query_fields(policy, &mut effective_fields);
        }
        if let Some(captured) = generation_definition(
            client,
            session.environment(),
            &binding.resource,
            &read.revision,
        )
        .await?
        {
            if let Some(policy) = &captured.policies.row {
                query_fields(policy, &mut source_fields);
            }
            if let Some(policy) = &captured.policies.object_read {
                query_fields(policy, &mut effective_fields);
            }
        }
        for (uid, row) in rows {
            read.fingerprints
                .insert(uid, view_fingerprint(&binding, &read.fields, &row)?);
            read.policies.insert(
                uid,
                PolicyWitness {
                    source: source_fields
                        .iter()
                        .filter_map(|field| {
                            row.source
                                .get(field)
                                .map(|value| (field.clone(), value.clone()))
                        })
                        .collect(),
                    effective: effective_fields
                        .iter()
                        .filter_map(|field| {
                            row.effective
                                .get(field)
                                .map(|value| (field.clone(), value.clone()))
                        })
                        .collect(),
                },
            );
        }
        Ok(())
    }
    async fn aggregate_observation(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        binding: &Binding,
        attributes: &ValueMap,
        read: &ReadEvidence,
        current: &str,
    ) -> Result<String> {
        let observation = read
            .query
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_INPUT_CHANGED"))?;
        let aggregation = observation
            .aggregate_spec
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_INPUT_CHANGED"))?;
        let query = Query {
            filter: observation.filter.clone(),
            projection: Some(vec![]),
            page_size: 100,
            cursor: None,
            sort: vec![],
            scope: observation.scope.clone(),
            budget: observation.budget.clone(),
            aggregation: Some(aggregation.clone()),
        };
        validate_aggregation(aggregation, binding)?;
        let pin = ReadPin {
            revision: read.revision.clone(),
            after: None,
            keys: vec![],
            expires: String::new(),
        };
        authorize_generation_query(client, session, binding, &pin.revision, current, &query)
            .await?;
        let rows = query_rows(
            client,
            session.environment(),
            binding,
            attributes,
            &pin,
            current,
            &query,
        )
        .await?;
        let budget = query
            .budget
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("FUNCTION_INPUT_CHANGED"))?;
        ensure!(
            rows.len()
                <= budget
                    .groups
                    .ok_or_else(|| anyhow::anyhow!("FUNCTION_INPUT_CHANGED"))?,
            "RESOURCE_LIMIT"
        );
        let types = aggregate_types(aggregation, binding)?;
        let mut groups = rows
            .iter()
            .map(|row| row.get::<_, Value>(0))
            .collect::<Vec<_>>();
        for group in &mut groups {
            group
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("POLICY_MISMATCH"))?
                .remove("_readUids");
            for (name, schema) in &types {
                group["values"][name] = values::normalize(&group["values"][name], schema)
                    .map_err(|_| anyhow::anyhow!("VALUE_INVALID"))?;
            }
            for property in &aggregation.group_by {
                group["key"][property] =
                    values::normalize(&group["key"][property], scalar_type(binding, property)?)
                        .map_err(|_| anyhow::anyhow!("VALUE_INVALID"))?;
            }
        }
        fingerprint(&json!({"groups":groups,"types":types}))
    }
    pub async fn invoke(
        &self,
        session: &VerifiedSession,
        reference: &str,
        key: &str,
        input: &Value,
    ) -> Result<Value> {
        ensure!(
            !key.is_empty() && key.len() <= 256,
            "IDEMPOTENCY_KEY_INVALID"
        );
        let mut attempt = 0;
        let mut prepared = None;
        let mut deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let result = tokio::time::timeout_at(
                deadline,
                self.invoke_attempt(session, reference, key, input, prepared.as_ref()),
            )
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")));
            if result
                .as_ref()
                .is_err_and(|error| error_code(error) == "FUNCTION_PREPARE_REQUIRED")
                && prepared.is_none()
            {
                prepared = Some(self.prepare_action(session, reference, key, input).await?);
                deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                continue;
            }
            if result
                .as_ref()
                .is_err_and(|error| error.root_cause().to_string() == "ACTION_PREPARATION_CHANGED")
            {
                if attempt < 2 {
                    attempt += 1;
                    continue;
                }
                bail!("CONCURRENT_MODIFICATION");
            }
            if let Err(error) = &result
                && error
                    .chain()
                    .find_map(|error| error.downcast_ref::<tokio_postgres::Error>())
                    .is_some_and(|error| {
                        error
                            .as_db_error()
                            .is_some_and(|error| matches!(error.code().code(), "40001" | "40P01"))
                    })
            {
                if attempt < 2 {
                    attempt += 1;
                    continue;
                }
                bail!("CONCURRENT_MODIFICATION");
            }
            return result;
        }
    }
    async fn prepare_action(
        &self,
        session: &VerifiedSession,
        reference: &str,
        key: &str,
        input: &Value,
    ) -> Result<PreparedAction> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome=async {
            let client=admission.client();let snapshot=Authority::current(client,session).await?;
            let resolved=Self::resource_reference(client,session,reference).await?;let reference=resolved.as_str();
            let row=client.query_opt("SELECT a.definition,b.revision FROM moria.native_serving_actions a JOIN moria.native_serving_bindings b ON b.environment=a.environment AND b.resource=a.object_resource WHERE a.environment=$1 AND a.resource=$2 AND a.enabled AND b.enabled",&[&session.environment(),&reference]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
            let definition:Value=row.get(0);let action:ActionDefinition=serde_json::from_value(definition.clone())?;
            authorize(&snapshot.identity,&action.resource,"invoke",&action.markings)?;
            Authority::resource_access(client,session,&action.resource,"read").await?;
            let function=canonical_uid(action.function.as_deref().ok_or_else(||anyhow::anyhow!("SCHEMA_CHANGED"))?)?;
            let contract=Self::action_contract(client,session,reference).await?.0;
            let input=json!(validate_row(input,&action.inputs)?);
            Authority::finish(client,session,&snapshot).await?;
            Ok((function,contract,input))
        }.await;
        let (function, contract, input) = admission.complete(outcome).await?;
        let function = native_functions(&self.db)
            .prepare_action(
                session,
                function,
                &fingerprint(&json!([reference, key]))?,
                input,
            )
            .await?;
        Ok(PreparedAction { function, contract })
    }
    pub(crate) async fn prepare_child_action(
        &self,
        session: &VerifiedSession,
        reference: &str,
        key: &str,
        input: &Value,
        parent: &crate::sandbox::SandboxContext,
    ) -> Result<Option<PreparedAction>> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome = async {
            let client = admission.client();
            parent.check_in(client).await?;
            let (contract, definition, _) =
                Self::action_contract(client, session, reference).await?;
            let action: ActionDefinition = serde_json::from_value(definition)?;
            let Some(function) = action.function else {
                return Ok(None);
            };
            let snapshot = Authority::current(client, session).await?;
            authorize(
                &snapshot.identity,
                &action.resource,
                "invoke",
                &action.markings,
            )?;
            Authority::resource_access(client, session, &action.resource, "read").await?;
            let input = json!(validate_row(input, &action.inputs)?);
            Authority::finish(client, session, &snapshot).await?;
            Ok(Some((
                canonical_uid(&function)?,
                contract,
                input,
                action.resource,
            )))
        }
        .await;
        let Some((function, contract, input, action)) = admission.complete(outcome).await? else {
            return Ok(None);
        };
        let function = native_functions(&self.db)
            .prepare_action_child(
                session,
                function,
                &fingerprint(&json!([action, key]))?,
                input,
                parent,
            )
            .await?;
        Ok(Some(PreparedAction { function, contract }))
    }
    async fn invoke_attempt(
        &self,
        session: &VerifiedSession,
        reference: &str,
        key: &str,
        input: &Value,
        prepared: Option<&PreparedAction>,
    ) -> Result<Value> {
        let ordinary = if prepared.is_none() {
            self.prepare_ordinary(session, reference, key, input)
                .await?
        } else {
            None
        };
        let ordinary = match ordinary {
            Some((None, value)) => return Ok(value),
            Some((Some(plan), _)) => Some(plan),
            None => None,
        };
        let admission = if let Some(plan) = &ordinary {
            Admission::begin_action_publication(
                &self.db,
                session,
                &plan.publication_key(session, key)?,
                &plan.home_uids(),
            )
            .await?
        } else {
            Admission::begin(&self.db, session, true).await?
        };
        let result = Self::invoke_prepared(
            admission.client(),
            session,
            reference,
            key,
            input,
            ActionContext {
                db: &self.db,
                consumed: &[],
                prepared,
                ordinary: ordinary.as_ref(),
                capture: None,
            },
        )
        .await;
        admission.complete(result).await
    }
    pub(crate) async fn invoke_marked(
        db: &Database,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
        key: &str,
        input: &Value,
        consumed: &[String],
    ) -> Result<Value> {
        Self::invoke_prepared(
            client,
            session,
            reference,
            key,
            input,
            ActionContext {
                db,
                consumed,
                prepared: None,
                ordinary: None,
                capture: None,
            },
        )
        .await
    }
    pub(crate) async fn invoke_marked_prepared(
        db: &Database,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
        key: &str,
        input: &Value,
        context: (&[String], Option<&PreparedAction>),
    ) -> Result<Value> {
        if context.1.is_none() {
            return Self::invoke_marked(db, client, session, reference, key, input, context.0)
                .await;
        }
        Self::invoke_prepared(
            client,
            session,
            reference,
            key,
            input,
            ActionContext {
                db,
                consumed: context.0,
                prepared: context.1,
                ordinary: None,
                capture: None,
            },
        )
        .await
    }
    pub(crate) async fn action_markings(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
    ) -> Result<Vec<String>> {
        let resolved = Self::resource_reference(client, session, reference).await?;
        let reference = resolved.as_str();
        let snapshot = Authority::current(client, session).await?;
        let row=client.query_opt("SELECT object_resource,definition FROM moria.native_serving_actions WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&reference]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        let action: ActionDefinition = serde_json::from_value(row.get(1))?;
        authorize(
            &snapshot.identity,
            &action.resource,
            "invoke",
            &action.markings,
        )?;
        let binding = Self::binding(client, session, &row.get::<_, String>(0)).await?;
        Self::required(&snapshot, &binding)?;
        CompiledExpression::compile(
            &action.condition,
            ExpressionTypes {
                fields: binding.definition.schema.properties.clone(),
                parameters: action.inputs.clone(),
                attributes: binding.definition.policies.attributes.clone(),
            },
        )?;
        ObjectReader::compile(binding.definition.policies.clone())?.prepare(
            &snapshot.identity,
            snapshot.attributes.clone(),
            Some(vec![]),
            &[],
            vec![],
        )?;
        let mut labels = action.markings;
        if let Some(edits) = &action.typed_edits {
            for edit in edits {
                if let TypedActionEdit::Link { link_type, .. }
                | TypedActionEdit::Unlink { link_type, .. } = edit
                {
                    labels.extend(links::markings(client, session, &snapshot, link_type).await?);
                }
            }
            let resources = edits
                .iter()
                .filter_map(TypedActionEdit::target)
                .collect::<BTreeSet<_>>();
            for resource in resources {
                let target = Self::binding(client, session, resource).await?;
                Self::required(&snapshot, &target)?;
                ObjectReader::compile(target.definition.policies.clone())?.prepare(
                    &snapshot.identity,
                    snapshot.attributes.clone(),
                    Some(vec![]),
                    &[],
                    vec![],
                )?;
                labels.extend(target.definition.policies.markings);
                labels.extend(Authority::resource_access(client, session, resource, "read").await?);
            }
        }
        labels.extend(binding.definition.policies.markings);
        labels.extend(Authority::resource_access(client, session, &action.resource, "read").await?);
        labels
            .extend(Authority::resource_access(client, session, &binding.resource, "read").await?);
        labels.sort();
        labels.dedup();
        Authority::finish(client, session, &snapshot).await?;
        Ok(labels)
    }
    async fn invoke_prepared(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        reference: &str,
        key: &str,
        input: &Value,
        mut context: ActionContext<'_>,
    ) -> Result<Value> {
        let resolved = Self::resource_reference(client, session, reference).await?;
        let reference = resolved.as_str();
        let mut consumed = context.consumed.to_vec();
        let snapshot = Authority::current(client, session).await?;
        let row=client.query_opt("SELECT object_resource,revision,definition FROM moria.native_serving_actions WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&reference]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        let mut action: ActionDefinition = serde_json::from_value(row.get(2))?;
        ensure!(
            action.resource == reference && action.revision == row.get::<_, String>(1),
            "BINDING_INVALID"
        );
        authorize(
            &snapshot.identity,
            &action.resource,
            "invoke",
            &action.markings,
        )?;
        let previous=client.query_opt("SELECT k.input_hash,k.result,k.expires_at<=clock_timestamp() AS expired,k.action_revision,m.input_types,m.object_resource,m.object_resources FROM moria.action_keys k LEFT JOIN moria.native_serving_receipt_inputs m USING(environment,application,principal,action,key) WHERE k.environment=$1 AND k.application=$2 AND k.principal=$3 AND k.action=$4 AND k.key=$5",&[&session.environment(),&session.application(),&session.principal(),&action.resource,&key]).await?;
        let object_resource = previous
            .as_ref()
            .and_then(|previous| previous.get::<_, Option<String>>(5))
            .unwrap_or_else(|| row.get::<_, String>(0));
        let binding = Self::binding(client, session, &object_resource).await?;
        Self::required(&snapshot, &binding)?;
        if !consumed.is_empty() {
            let mut object_labels = binding.definition.policies.markings.clone();
            object_labels.extend(
                Authority::resource_access(client, session, &binding.resource, "write").await?,
            );
            let mut action_labels = action.markings.clone();
            action_labels.extend(
                Authority::resource_access(client, session, &action.resource, "write").await?,
            );
            ensure!(
                consumed
                    .iter()
                    .all(|label| object_labels.contains(label) && action_labels.contains(label)),
                "MARKING_FLOW"
            );
        }
        if let Some(previous) = previous {
            let input_hash = if let Some(types) = previous.get::<_, Option<Value>>(4) {
                let types: ValueMap = serde_json::from_value(types)?;
                fingerprint(&json!(
                    validate_row(input, &types)
                        .map_err(|_| anyhow::anyhow!("IDEMPOTENCY_CONFLICT"))?
                ))?
            } else if previous.get::<_, String>(3) == action.revision {
                fingerprint(&json!(
                    validate_row(input, &action.inputs)
                        .map_err(|_| anyhow::anyhow!("IDEMPOTENCY_CONFLICT"))?
                ))?
            } else {
                fingerprint(input)?
            };
            ensure!(
                previous.get::<_, String>(0) == input_hash,
                "IDEMPOTENCY_CONFLICT"
            );
            let result: Option<Value> = previous.get(1);
            ensure!(
                !previous.get::<_, bool>(2) && result.is_some(),
                "RESULT_EXPIRED"
            );
            ObjectReader::compile(binding.definition.policies.clone())?.prepare(
                &snapshot.identity,
                snapshot.attributes.clone(),
                None,
                &[],
                vec![],
            )?;
            Authority::resource_access(client, session, &binding.resource, "read").await?;
            for resource in previous
                .get::<_, Option<Vec<String>>>(6)
                .unwrap_or_default()
            {
                let target = Self::binding(client, session, &resource).await?;
                Self::required(&snapshot, &target)?;
                ObjectReader::compile(target.definition.policies)?.prepare(
                    &snapshot.identity,
                    snapshot.attributes.clone(),
                    Some(vec![]),
                    &[],
                    vec![],
                )?;
                Authority::resource_access(client, session, &resource, "read").await?;
            }
            if let Some(prepared)=client.query_opt("SELECT execution FROM moria.native_serving_prepared_actions WHERE environment=$1 AND application=$2 AND principal=$3 AND action=$4 AND key=$5",&[&session.environment(),&session.application(),&session.principal(),&action.resource,&key]).await? {
                let labels=native_functions(context.db).retained_prepared_in(client,session,prepared.get(0)).await?;
                let mut destination=binding.definition.policies.markings.clone();
                destination.extend(action.markings.clone());
                destination.extend(Authority::resource_access(client,session,&binding.resource,"read").await?);
                destination.extend(Authority::resource_access(client,session,&action.resource,"read").await?);
                ensure!(labels.iter().all(|label|label.environment==session.environment()&&destination.contains(&label.marking)),"MARKING_FLOW");
            }
            Authority::finish(client, session, &snapshot).await?;
            return Ok(result.unwrap());
        }
        if action.typed_edits.is_some() {
            ensure!(action.function.is_none(), "BINDING_INVALID");
            return Self::invoke_typed_action(
                client,
                session,
                &action,
                &binding,
                key,
                input,
                typed_actions::ActionPreparation {
                    snapshot: &snapshot,
                    consumed: &consumed,
                    ordinary: context.ordinary,
                    capture: context.capture.take(),
                },
            )
            .await;
        }
        if let Some(function) = &action.function {
            let prepared = context
                .prepared
                .ok_or_else(|| anyhow::anyhow!("FUNCTION_PREPARE_REQUIRED"))?;
            ensure!(
                canonical_uid(function)? == prepared.function.resource()
                    && prepared.contract
                        == Self::action_contract(client, session, reference).await?.0,
                "SCHEMA_CHANGED"
            );
            let functions = native_functions(context.db);
            let markings = functions
                .check_prepared_in(client, session, &prepared.function)
                .await?;
            ensure!(
                markings
                    .iter()
                    .all(|label| label.environment == session.environment()),
                "MARKING_FLOW"
            );
            consumed.extend(markings.into_iter().map(|label| label.marking));
            action.edits = prepared
                .function
                .values()
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("VALUE_INVALID"))?
                .iter()
                .map(|(property, value)| ActionEdit::Set {
                    property: property.clone(),
                    value: EditValue::Literal(LiteralValue {
                        literal: value.clone(),
                    }),
                })
                .collect();
            action.function = None;
            let mut object_labels = binding.definition.policies.markings.clone();
            object_labels.extend(
                Authority::resource_access(client, session, &binding.resource, "write").await?,
            );
            let mut action_labels = action.markings.clone();
            action_labels.extend(
                Authority::resource_access(client, session, &action.resource, "write").await?,
            );
            ensure!(
                consumed
                    .iter()
                    .all(|label| object_labels.contains(label) && action_labels.contains(label)),
                "MARKING_FLOW"
            );
        }
        let batched = if context.capture.is_some() || context.ordinary.is_some() {
            Some(ordinary::access(client, session, &action, &binding, &snapshot).await?)
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
        let mut labels = binding.definition.policies.markings.clone();
        let mut action_source = action.markings.clone();
        action_source.extend(if batched.is_some() {
            resource_labels(&action.resource, "read")?
        } else {
            Authority::resource_access(client, session, &action.resource, "read").await?
        });
        let mut destination = binding.definition.policies.markings.clone();
        destination.extend(if batched.is_some() {
            resource_labels(&binding.resource, "write")?
        } else {
            Authority::resource_access(client, session, &binding.resource, "write").await?
        });
        ensure!(
            action_source
                .iter()
                .all(|label| destination.contains(label)),
            "MARKING_FLOW"
        );
        if batched.is_none() {
            Authority::resource_access(client, session, &binding.resource, "read").await?;
        }
        labels.extend(action.markings.clone());
        labels.extend(destination);
        labels.extend(if batched.is_some() {
            resource_labels(&action.resource, "write")?
        } else {
            Authority::resource_access(client, session, &action.resource, "write").await?
        });
        labels.sort();
        labels.dedup();
        labels.extend_from_slice(&consumed);
        labels.sort();
        labels.dedup();
        Self::schema(client, session, &binding).await?;
        let parameters = validate_row(input, &action.inputs)?;
        let input_hash = fingerprint(&json!(parameters))?;
        let target = parameters
            .get(&action.target_parameter)
            .ok_or_else(|| anyhow::anyhow!("VALUE_INVALID"))?;
        let targets = if let Some(targets) = target.as_array() {
            targets
                .iter()
                .map(|target| {
                    canonical_uid(
                        target
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("VALUE_INVALID"))?,
                    )
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![canonical_uid(
                target
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("VALUE_INVALID"))?,
            )?]
        };
        ensure!(
            !targets.is_empty()
                && targets.len() <= 100
                && targets.iter().collect::<BTreeSet<_>>().len() == targets.len(),
            "ACTION_LIMIT"
        );
        let mut read_fields = Vec::new();
        query_fields(&action.condition, &mut read_fields);
        let project = ObjectReader::compile(binding.definition.policies.clone())?.prepare(
            &snapshot.identity,
            snapshot.attributes.clone(),
            None,
            &read_fields,
            vec![],
        )?;
        let types = ExpressionTypes {
            fields: binding.definition.schema.properties.clone(),
            parameters: action.inputs.clone(),
            attributes: binding.definition.policies.attributes.clone(),
        };
        let condition = CompiledExpression::compile(&action.condition, types.clone())?;
        let write = binding
            .definition
            .write_policy
            .as_ref()
            .map(|expression| {
                CompiledExpression::compile(
                    expression,
                    ExpressionTypes {
                        fields: binding.definition.schema.properties.clone(),
                        attributes: binding.definition.policies.attributes.clone(),
                        ..Default::default()
                    },
                )
            })
            .transpose()?;
        let mut edits = Vec::new();
        for edit in &action.edits {
            match edit {
                ActionEdit::Set { property, value } => {
                    check_property_write(&binding, property, &snapshot.attributes)?;
                    let value = match value {
                        EditValue::Parameter(parameter) => parameters
                            .get(&parameter.parameter)
                            .ok_or_else(|| anyhow::anyhow!("VALUE_INVALID"))?,
                        EditValue::Literal(literal) => &literal.literal,
                    };
                    edits.push(Edit::Set(
                        property.clone(),
                        values::normalize(value, &binding.definition.schema.properties[property])?,
                    ));
                }
                ActionEdit::ClearOverride { property } => {
                    check_property_write(&binding, property, &snapshot.attributes)?;
                    edits.push(Edit::Clear(property.clone()));
                }
                ActionEdit::Delete => edits.push(Edit::Delete),
                ActionEdit::Restore => edits.push(Edit::Restore),
            }
        }
        let preparing = context.capture.is_some();
        let mut ordinary_capture = if preparing {
            Some(OrdinaryAction::new(
                Self::action_contract(client, session, reference).await?.0,
                reference.to_owned(),
                binding.resource.clone(),
                &snapshot,
                ordinary::generation(client, session, &binding.resource).await?,
            )?)
        } else {
            None
        };
        let before_revision = client
            .query_one(
                if preparing {
                    "SELECT revision::text FROM moria.environments WHERE id=$1"
                } else {
                    "SELECT revision::text FROM moria.environments WHERE id=$1 FOR UPDATE"
                },
                &[&session.environment()],
            )
            .await?
            .get::<_, String>(0);
        if let Some(plan) = context.ordinary {
            plan.check(client, session, reference, &binding, &snapshot)
                .await?;
        }
        let captured = generation_definition(
            client,
            session.environment(),
            &binding.resource,
            &before_revision,
        )
        .await?;
        let historical = captured
            .as_ref()
            .map(|captured| {
                prepare_generation(
                    &snapshot,
                    &binding,
                    captured,
                    project.selected(),
                    &read_fields,
                )
            })
            .transpose()?;
        for edit in &action.edits {
            match edit {
                ActionEdit::Set { property, .. } | ActionEdit::ClearOverride { property } => {
                    generation_policy::property_write(
                        captured.as_ref(),
                        &binding,
                        property,
                        &snapshot.attributes,
                    )?
                }
                _ => {}
            }
        }
        let next = if preparing {
            before_revision.clone()
        } else {
            client.query_one("UPDATE moria.environments SET revision=revision+1 WHERE id=$1 RETURNING revision::text",&[&session.environment()]).await?.get::<_,String>(0)
        };
        for uid in &targets {
            let uid = *uid;
            let observed = if let Some(plan) = context.ordinary {
                plan.row(&binding.resource, uid)?
            } else {
                let source = source_row(
                    client,
                    session.environment(),
                    &binding.resource,
                    uid,
                    &before_revision,
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?;
                let previous=client.query_opt("SELECT v.overrides,v.tombstone,v.revision::text FROM moria.overlay_versions v WHERE v.environment=$1 AND v.object_type=$2 AND v.uid=$3 ORDER BY v.revision DESC LIMIT 1",&[&session.environment(),&binding.resource,&uid]).await?;
                OrdinaryRow {
                    source,
                    overrides: previous
                        .as_ref()
                        .map(|row| serde_json::from_value::<ValueMap>(row.get(0)))
                        .transpose()?
                        .unwrap_or_default(),
                    tombstone: previous.as_ref().is_some_and(|row| row.get::<_, bool>(1)),
                    overlay_revision: previous.map(|row| row.get(2)),
                }
            };
            if let Some(plan) = &mut ordinary_capture {
                plan.record(&binding.resource, uid, observed.clone())?;
            }
            let source = observed.source;
            let mut overrides = observed.overrides;
            let mut tombstone = observed.tombstone;
            let before = ObjectRow {
                source: source.canonical.clone(),
                effective: effective(&source.object, &overrides),
            };
            let restoring = edits.iter().any(|edit| matches!(edit, Edit::Restore));
            let before_visible = if tombstone && !restoring {
                None
            } else {
                Some(&before)
            };
            ensure!(
                project.project(before_visible, before_visible)?.is_some(),
                "NOT_FOUND"
            );
            if let Some(historical) = &historical {
                ensure!(
                    historical
                        .project(before_visible, before_visible)?
                        .is_some(),
                    "NOT_FOUND"
                );
            }
            for edit in &edits {
                match edit {
                    Edit::Set(property, value) => {
                        overrides.insert(property.clone(), value.clone());
                    }
                    Edit::Clear(property) => {
                        overrides.remove(property);
                    }
                    Edit::Delete => tombstone = true,
                    Edit::Restore => tombstone = false,
                }
            }
            let after = ObjectRow {
                source: source.canonical.clone(),
                effective: validate_row(
                    &json!(effective(&source.object, &overrides)),
                    &binding.definition.schema.properties,
                )?,
            };
            ensure!(
                tombstone || project.project(Some(&after), Some(&after))?.is_some(),
                "FORBIDDEN"
            );
            if let Some(historical) = &historical {
                ensure!(
                    tombstone || historical.project(Some(&after), Some(&after))?.is_some(),
                    "FORBIDDEN"
                );
            }
            generation_policy::write_rows(
                captured.as_ref(),
                &snapshot.attributes,
                &before,
                &after,
            )?;
            for state in [&before, &after] {
                if let Some(write) = &write {
                    ensure!(
                        write.allows(&ExpressionValues {
                            fields: state.effective.clone(),
                            attributes: snapshot.attributes.clone(),
                            ..Default::default()
                        })?,
                        "FORBIDDEN"
                    );
                }
            }
            ensure!(
                condition.allows(&ExpressionValues {
                    fields: before.effective,
                    parameters: parameters.clone(),
                    attributes: snapshot.attributes.clone()
                })?,
                "ACTION_CONDITION_FAILED"
            );
            if !preparing {
                client.execute("INSERT INTO moria.overlay_versions(environment,object_type,uid,revision,overrides,tombstone) VALUES($1,$2,$3,$4::text::bigint,$5,$6)",&[&session.environment(),&binding.resource,&uid,&next,&json!(overrides),&tombstone]).await?;
            }
        }
        if let Some(capture) = context.capture.as_mut() {
            Authority::finish(client, session, &snapshot).await?;
            **capture = ordinary_capture;
            return Ok(Value::Null);
        }
        let result = json!({"revision":next});
        client.execute("INSERT INTO moria.action_keys VALUES($1,$2,$3,$4,$5,$6,$7,$8,clock_timestamp()+interval '30 days')",&[&session.environment(),&session.application(),&session.principal(),&action.resource,&key,&input_hash,&action.revision,&result]).await?;
        client.execute("INSERT INTO moria.native_serving_receipt_inputs(environment,application,principal,action,key,input_types,object_resource) VALUES($1,$2,$3,$4,$5,$6,$7)",&[&session.environment(),&session.application(),&session.principal(),&action.resource,&key,&json!(action.inputs),&binding.resource]).await?;
        client.execute("INSERT INTO moria.native_serving_audit(environment,revision,application,principal,action,key,targets) VALUES($1,$2::text::bigint,$3,$4,$5,$6,$7)",&[&session.environment(),&next,&session.application(),&session.principal(),&action.resource,&key,&targets]).await?;
        if let Some(prepared) = context.prepared {
            native_functions(context.db)
                .committed_prepared_in(client, session, &prepared.function, &result)
                .await?;
            client.execute("INSERT INTO moria.native_serving_prepared_actions(environment,application,principal,action,key,execution) VALUES($1,$2,$3,$4,$5,$6)",&[&session.environment(),&session.application(),&session.principal(),&action.resource,&key,&prepared.function.id()]).await?;
        }
        for effect in &action.effects {
            ensure!(
                !effect.key.is_empty() && effect.key.len() <= 128,
                "BINDING_INVALID"
            );
            let logical_key = format!("action/{}/{key}/{}", action.resource, effect.key);
            let payload = json!({"version":1,"action":action.resource,"key":key,"result":result});
            let origin = json!({"kind":"action","environment":session.environment(),"application":session.application(),"principal":session.principal(),"action":action.resource,"key":key});
            client
                .query_one(
                    "SELECT moria.runtime_delivery_enqueue($1,$2,$3,$4,$5)",
                    &[
                        &effect.destination,
                        &logical_key,
                        &payload,
                        &labels,
                        &origin,
                    ],
                )
                .await?;
        }
        Authority::finish(client, session, &snapshot).await?;
        Ok(result)
    }
}

enum Edit {
    Set(String, Value),
    Clear(String),
    Delete,
    Restore,
}
fn native_functions(db: &Database) -> crate::functions::NativeFunctions {
    crate::functions::NativeFunctions::new(
        db.clone(),
        std::sync::Arc::new(crate::platform::NativePlatformBroker::new(db.clone())),
    )
}
fn check_property_write(binding: &Binding, name: &str, attributes: &ValueMap) -> Result<()> {
    ensure!(
        name != binding.definition.schema.primary_key
            && binding.definition.schema.properties.contains_key(name),
        "EDIT_INVALID"
    );
    if let Some(expression) = binding.definition.property_write.get(name) {
        ensure!(
            CompiledExpression::compile(
                expression,
                ExpressionTypes {
                    attributes: binding.definition.policies.attributes.clone(),
                    ..Default::default()
                }
            )?
            .allows(&ExpressionValues {
                attributes: attributes.clone(),
                ..Default::default()
            })?,
            "PROPERTY_FORBIDDEN"
        );
    }
    Ok(())
}
fn validate_schema(schema: &ObjectSchema) -> Result<()> {
    ensure!(schema.properties.len() <= 10000, "SCHEMA_INVALID");
    for value in schema.properties.values() {
        postgres_type(value)?;
    }
    let key = schema
        .properties
        .get(&schema.primary_key)
        .ok_or_else(|| anyhow::anyhow!("SCHEMA_INVALID"))?;
    ensure!(
        key["type"] == "uid" && key["nullable"] == false,
        "SCHEMA_INVALID"
    );
    Ok(())
}
fn normalize_schema(schema: &ObjectSchema) -> Result<Value> {
    let mut properties = serde_json::Map::new();
    for (name, value) in &schema.properties {
        properties.insert(name.clone(), values::descriptor(value, &Value::Null, 0)?);
    }
    Ok(json!({"primaryKey":schema.primary_key,"properties":properties}))
}
fn validate_row(input: &Value, types: &ValueMap) -> Result<ValueMap> {
    let input = input
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("ROW_INVALID"))?;
    ensure!(
        input.keys().all(|key| types.contains_key(key)),
        "ROW_INVALID"
    );
    let mut result = ValueMap::new();
    for (name, schema) in types {
        postgres_type(schema)?;
        let value = input
            .get(name)
            .or_else(|| {
                if schema["nullable"] == true {
                    Some(&Value::Null)
                } else {
                    None
                }
            })
            .ok_or_else(|| anyhow::anyhow!("ROW_INVALID"))?;
        result.insert(
            name.clone(),
            values::normalize(value, schema).map_err(|_| anyhow::anyhow!("VALUE_INVALID"))?,
        );
    }
    Ok(result)
}
fn effective(source: &ValueMap, overrides: &ValueMap) -> ValueMap {
    let mut value = source.clone();
    value.extend(overrides.clone());
    value
}
fn canonical_uid(value: &str) -> Result<Uuid> {
    let uid = Uuid::parse_str(value).map_err(|_| anyhow::anyhow!("VALUE_INVALID"))?;
    ensure!(uid.to_string() == value, "VALUE_INVALID");
    Ok(uid)
}
pub(crate) fn hash(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
pub(crate) fn token() -> Result<String> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes).map_err(|_| anyhow::anyhow!("INTERNAL"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}
#[derive(Clone)]
struct SourceRow {
    object: ValueMap,
    canonical: ValueMap,
}
async fn source_row(
    client: &tokio_postgres::Client,
    environment: &str,
    resource: &str,
    uid: Uuid,
    revision: &str,
) -> Result<Option<SourceRow>> {
    let row=client.query_opt("SELECT value,source_value FROM moria.runtime_object_base($1,$2,$4::text::bigint) WHERE uid=$3",&[&environment,&resource,&uid,&revision]).await?;
    row.map(|row| {
        Ok(SourceRow {
            object: serde_json::from_value(row.get(0))?,
            canonical: serde_json::from_value(row.get(1))?,
        })
    })
    .transpose()
}
async fn read_object(
    client: &tokio_postgres::Client,
    session: &VerifiedSession,
    binding: &Binding,
    uid: Uuid,
    revision: &str,
    selected: &[String],
) -> Result<Option<ObjectRow>> {
    let environment = session.environment();
    let resource = &binding.resource;
    let captured = generation_definition(client, environment, resource, revision).await?;
    let snapshot = Authority::current(client, session).await?;
    let historical = captured
        .as_ref()
        .map(|captured| prepare_generation(&snapshot, binding, captured, selected, selected))
        .transpose()?;
    let Some(source) = source_row(client, environment, resource, uid, revision).await? else {
        return Ok(None);
    };
    let overlay=client.query_opt("SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$1 AND object_type=$2 AND uid=$3 AND revision<=$4::text::bigint ORDER BY revision DESC LIMIT 1",&[&environment,&resource,&uid,&revision]).await?;
    if overlay.as_ref().is_some_and(|row| row.get::<_, bool>(1)) {
        return Ok(None);
    };
    let overrides = overlay
        .map(|row| serde_json::from_value::<ValueMap>(row.get(0)))
        .transpose()?
        .unwrap_or_default();
    let object = ObjectRow {
        effective: effective(&source.object, &overrides),
        source: source.canonical,
    };
    if let Some(historical) = historical
        && historical.project(Some(&object), Some(&object))?.is_none()
    {
        return Ok(None);
    }
    Ok(Some(object))
}
#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SortKey {
    property: String,
    direction: Direction,
}
#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum Direction {
    Asc,
    Desc,
}
#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UidScope {
    from_uid: Uuid,
    through_uid: Uuid,
}
#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueryBudget {
    scan_rows: usize,
    response_bytes: usize,
    time_ms: u64,
    groups: Option<usize>,
}
#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Aggregation {
    group_by: Vec<String>,
    metrics: BTreeMap<String, Metric>,
}
#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Metric {
    op: String,
    property: Option<String>,
    output_scale: Option<u64>,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Query {
    filter: Option<Value>,
    projection: Option<Vec<String>>,
    #[serde(default = "page_size")]
    page_size: usize,
    cursor: Option<String>,
    #[serde(default)]
    sort: Vec<SortKey>,
    scope: Option<UidScope>,
    budget: Option<QueryBudget>,
    #[serde(skip)]
    aggregation: Option<Aggregation>,
}
fn page_size() -> usize {
    100
}
impl Query {
    fn parse(input: &Value) -> Result<Self> {
        validate_scope(input)?;
        ensure!(
            input.is_object()
                && ["filter", "projection", "cursor", "sort", "scope", "budget"]
                    .iter()
                    .all(|key| input.get(key).is_none_or(|value| !value.is_null())),
            "QUERY_INVALID"
        );
        if let Some(filter) = input.get("filter") {
            query_expression(filter, 0, &mut 0)?;
        }
        let query: Self =
            serde_json::from_value(input.clone()).map_err(|_| anyhow::anyhow!("QUERY_INVALID"))?;
        ensure!((1..=1000).contains(&query.page_size), "QUERY_INVALID");
        ensure!(
            query.sort.len() <= 4
                && query
                    .sort
                    .iter()
                    .map(|key| &key.property)
                    .collect::<BTreeSet<_>>()
                    .len()
                    == query.sort.len(),
            "QUERY_INVALID"
        );
        if let Some(scope) = &query.scope {
            ensure!(scope.from_uid <= scope.through_uid, "QUERY_INVALID");
        }
        if let Some(budget) = &query.budget {
            ensure!(
                (1..=10000).contains(&budget.scan_rows)
                    && (1..=RESPONSE_BYTES).contains(&budget.response_bytes)
                    && (1..=30000).contains(&budget.time_ms)
                    && budget
                        .groups
                        .is_none_or(|groups| (1..=1000).contains(&groups)),
                "QUERY_INVALID"
            );
        }
        ensure!(
            query.scope.is_some() == query.budget.is_some(),
            "QUERY_INVALID"
        );
        if let Some(projection) = &query.projection {
            ensure!(
                projection.len() <= 1000
                    && projection.iter().collect::<BTreeSet<_>>().len() == projection.len(),
                "QUERY_INVALID"
            );
        }
        if let Some(cursor) = &query.cursor {
            ensure!(
                cursor.len() == 43
                    && cursor
                        .bytes()
                        .all(|ch| ch.is_ascii_alphanumeric() || ch == b'_' || ch == b'-'),
                "QUERY_INVALID"
            );
        }
        Ok(query)
    }
}
fn validate_scope(input: &Value) -> Result<()> {
    if let Some(scope) = input.get("scope") {
        for name in ["fromUid", "throughUid"] {
            canonical_uid(
                scope
                    .get(name)
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("QUERY_INVALID"))?,
            )
            .map_err(|_| anyhow::anyhow!("QUERY_INVALID"))?;
        }
    }
    Ok(())
}
fn parse_aggregate(input: &Value) -> Result<Query> {
    let mut object = input
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("QUERY_INVALID"))?;
    let mut aggregation:Aggregation=serde_json::from_value(json!({"groupBy":object.remove("groupBy").unwrap_or(json!([])),"metrics":object.remove("metrics").ok_or_else(||anyhow::anyhow!("QUERY_INVALID"))?})).map_err(|_|anyhow::anyhow!("QUERY_INVALID"))?;
    for metric in aggregation.metrics.values_mut() {
        if metric.op == "avg" {
            metric.op = "average".into();
        }
    }
    ensure!(
        !["sort", "cursor", "pageSize", "projection"]
            .iter()
            .any(|key| object.contains_key(*key)),
        "QUERY_INVALID"
    );
    let global_budget = if !object.contains_key("scope") {
        Some(
            serde_json::from_value::<QueryBudget>(
                object
                    .remove("budget")
                    .ok_or_else(|| anyhow::anyhow!("QUERY_INVALID"))?,
            )
            .map_err(|_| anyhow::anyhow!("QUERY_INVALID"))?,
        )
    } else {
        None
    };
    let mut query = Query::parse(&Value::Object(object))?;
    if let Some(budget) = global_budget {
        ensure!(
            (1..=1_000_000_000).contains(&budget.scan_rows)
                && (1..=RESPONSE_BYTES).contains(&budget.response_bytes)
                && (1..=30000).contains(&budget.time_ms)
                && budget
                    .groups
                    .is_some_and(|groups| (1..=1000).contains(&groups)),
            "QUERY_INVALID"
        );
        query.budget = Some(budget);
    }
    ensure!(
        query
            .budget
            .as_ref()
            .is_some_and(|budget| budget.groups.is_some()),
        "UNSUPPORTED_CAPABILITY"
    );
    query.aggregation = Some(aggregation);
    Ok(query)
}
fn query_expression(expression: &Value, depth: usize, count: &mut usize) -> Result<()> {
    *count += 1;
    ensure!(*count <= 256 && depth <= 16, "QUERY_INVALID");
    let object = expression
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("QUERY_INVALID"))?;
    if object.len() == 1 {
        if object.get("field").is_some_and(Value::is_string) {
            return Ok(());
        }
        if let Some(literal) = object.get("literal") {
            let scalar = |value: &Value| {
                value.is_null() || value.is_boolean() || value.is_string() || value.is_number()
            };
            ensure!(
                if let Some(items) = literal.as_array() {
                    items.len() <= 100 && items.iter().all(scalar)
                } else {
                    scalar(literal)
                },
                "QUERY_INVALID"
            );
            return Ok(());
        }
    }
    ensure!(
        object.len() == 2 && object.contains_key("op") && object.contains_key("args"),
        "QUERY_INVALID"
    );
    ensure!(
        expression["op"].as_str().is_some_and(|operator| [
            "and", "or", "not", "eq", "ne", "lt", "lte", "gt", "gte", "in", "contains", "isNull"
        ]
        .contains(&operator)),
        "QUERY_INVALID"
    );
    let args = expression["args"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("QUERY_INVALID"))?;
    for arg in args {
        query_expression(arg, depth + 1, count)?;
    }
    Ok(())
}
struct ReadPin {
    revision: String,
    after: Option<Uuid>,
    keys: Vec<Value>,
    expires: String,
}
pub(crate) fn query_fields(expression: &Value, fields: &mut Vec<String>) {
    if let Some(field) = expression.get("field").and_then(Value::as_str) {
        fields.push(field.into());
    }
    if let Some(args) = expression.get("args").and_then(Value::as_array) {
        for arg in args {
            query_fields(arg, fields);
        }
    }
}
fn bind(parameters: &mut Vec<SqlParameter>, value: Option<String>, sql_type: &str) -> String {
    parameters.push(SqlParameter::Scalar(value));
    format!("${}::text::{sql_type}", parameters.len())
}
fn predicate(
    expression: Option<&Value>,
    types: &ValueMap,
    attribute_types: &ValueMap,
    attributes: &ValueMap,
    json_expression: &str,
    parameters: &mut Vec<SqlParameter>,
) -> Result<String> {
    predicate_mapped(
        expression,
        types,
        attribute_types,
        attributes,
        json_expression,
        parameters,
        None,
    )
}
fn predicate_mapped(
    expression: Option<&Value>,
    types: &ValueMap,
    attribute_types: &ValueMap,
    attributes: &ValueMap,
    json_expression: &str,
    parameters: &mut Vec<SqlParameter>,
    projection: Option<(&crate::projection::ProjectionRelation, &str, bool)>,
) -> Result<String> {
    let Some(expression) = expression else {
        return Ok("TRUE".into());
    };
    let columns = types
        .keys()
        .enumerate()
        .map(|(index, name)| (name.clone(), format!("f{index}")))
        .collect::<BTreeMap<_, _>>();
    let compiled = CompiledExpression::compile(
        expression,
        ExpressionTypes {
            fields: types.clone(),
            attributes: attribute_types.clone(),
            ..Default::default()
        },
    )?;
    let fragment = compiled.sql(
        &ExpressionValues {
            attributes: attributes.clone(),
            ..Default::default()
        },
        &columns,
        parameters.len(),
    )?;
    parameters.extend(fragment.parameters);
    let mut text = fragment.text;
    for (name, schema) in types {
        let identifier = format!("\"{}\"", columns[name]);
        if !text.contains(&identifier) {
            continue;
        }
        if let Some((relation, alias, false)) = projection
            && relation.schema["primaryKey"] == *name
            && relation.schema["properties"][name] == *schema
            && schema["type"] == "uid"
            && schema["nullable"] == false
        {
            text = text.replace(&identifier, &format!("{alias}.uid"));
            continue;
        }
        if let Some((relation, alias, source)) = projection
            && let Some(column) = if source {
                relation.source_columns.get(name)
            } else {
                relation.columns.get(name)
            }
        {
            ensure!(column.descriptor == *schema, "SCHEMA_CHANGED");
            if let Some(column) = if source {
                &column.source
            } else {
                &column.effective
            } {
                text = text.replace(&identifier, &format!("{alias}.\"{column}\""));
                continue;
            }
        }
        let key = bind(parameters, Some(name.clone()), "text");
        let extraction = if schema["type"] == "array" {
            format!(
                "(CASE WHEN ({json_expression}->{key}) IS NULL OR ({json_expression}->{key})='null'::jsonb THEN NULL ELSE ARRAY(SELECT element::{} FROM jsonb_array_elements_text({json_expression}->{key}) AS elements(element)) END)",
                postgres_type(&schema["items"])?
            )
        } else {
            format!("(({json_expression}->>{key})::{})", postgres_type(schema)?)
        };
        text = text.replace(&identifier, &extraction);
    }
    Ok(text)
}
fn scalar_type<'a>(binding: &'a Binding, property: &str) -> Result<&'a Value> {
    let schema = binding
        .definition
        .schema
        .properties
        .get(property)
        .ok_or_else(|| anyhow::anyhow!("PROPERTY_UNKNOWN"))?;
    ensure!(schema["type"] != "array", "UNSUPPORTED_CAPABILITY");
    Ok(schema)
}
fn view_fingerprint(binding: &Binding, fields: &[String], row: &ObjectRow) -> Result<String> {
    let mut properties = ValueMap::new();
    let mut values = ValueMap::new();
    for field in fields {
        properties.insert(
            field.clone(),
            binding
                .definition
                .schema
                .properties
                .get(field)
                .ok_or_else(|| anyhow::anyhow!("PROPERTY_UNKNOWN"))?
                .clone(),
        );
        values.insert(
            field.clone(),
            row.effective.get(field).cloned().unwrap_or(Value::Null),
        );
    }
    crate::canonical::value_fingerprint(
        &json!(values),
        &json!({"type":"object","properties":properties}),
        &Value::Null,
    )
    .map_err(|_| anyhow::anyhow!("POLICY_MISMATCH"))
}
fn scalar_sql(
    binding: &Binding,
    property: &str,
    json: &str,
    parameters: &mut Vec<SqlParameter>,
) -> Result<String> {
    let schema = scalar_type(binding, property)?;
    let key = bind(parameters, Some(property.into()), "text");
    let expression = format!("(({json}->>{key})::{})", postgres_type(schema)?);
    Ok(if schema["type"] == "string" {
        format!("({expression} COLLATE \"C\")")
    } else {
        expression
    })
}
fn validate_aggregation(aggregation: &Aggregation, binding: &Binding) -> Result<()> {
    ensure!(
        aggregation.group_by.len() <= 4
            && aggregation.group_by.iter().collect::<BTreeSet<_>>().len()
                == aggregation.group_by.len()
            && (1..=16).contains(&aggregation.metrics.len()),
        "QUERY_INVALID"
    );
    for property in &aggregation.group_by {
        scalar_type(binding, property)?;
    }
    for (name, metric) in &aggregation.metrics {
        ensure!(
            !name.is_empty()
                && name.len() <= 100
                && name
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-'),
            "QUERY_INVALID"
        );
        if metric.op == "count" {
            ensure!(
                metric.property.is_none() && metric.output_scale.is_none(),
                "QUERY_INVALID"
            );
            continue;
        }
        ensure!(
            ["sum", "min", "max", "average"].contains(&metric.op.as_str()),
            "QUERY_INVALID"
        );
        let schema = scalar_type(
            binding,
            metric
                .property
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("QUERY_INVALID"))?,
        )?;
        if ["sum", "average"].contains(&metric.op.as_str()) {
            ensure!(
                ["int64", "float64", "decimal"].contains(&schema["type"].as_str().unwrap_or("")),
                "UNSUPPORTED_CAPABILITY"
            );
        }
        if metric.op == "average" && schema["type"] != "float64" {
            ensure!(
                metric.output_scale.is_some_and(|scale| scale <= 38),
                "QUERY_INVALID"
            );
        } else {
            ensure!(metric.output_scale.is_none(), "QUERY_INVALID");
        }
    }
    Ok(())
}
fn aggregate_types(aggregation: &Aggregation, binding: &Binding) -> Result<ValueMap> {
    aggregation.metrics.iter().map(|(name,metric)| {
        let schema=if metric.op=="count" {json!({"type":"int64","nullable":false})} else {
            let mut schema=scalar_type(binding,metric.property.as_deref().unwrap())?.clone();
            schema["nullable"]=true.into();
            if metric.op=="average" && schema["type"]!="float64" {schema=json!({"type":"decimal","precision":38,"scale":metric.output_scale.unwrap(),"nullable":true});}
            if metric.op=="sum" && schema["type"]=="decimal" {schema["precision"]=38.into();}
            schema
        };
        Ok((name.clone(),schema))
    }).collect()
}
fn scalar_json(expression: &str, schema: &Value) -> String {
    if schema["type"] == "timestamp" {
        format!(
            "to_jsonb(to_char({expression} AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'))"
        )
    } else if ["float64", "boolean"].contains(&schema["type"].as_str().unwrap_or("")) {
        format!("to_jsonb({expression})")
    } else {
        format!("to_jsonb(({expression})::text)")
    }
}
fn aggregate_sql(
    aggregation: &Aggregation,
    binding: &Binding,
    parameters: &mut Vec<SqlParameter>,
    max_groups: usize,
    collect_uids: bool,
) -> Result<String> {
    let mut group_expressions = Vec::new();
    let mut keys = Vec::new();
    for property in &aggregation.group_by {
        let expression = scalar_sql(binding, property, "effective", parameters)?;
        keys.push(bind(parameters, Some(property.clone()), "text"));
        keys.push(scalar_json(&expression, scalar_type(binding, property)?));
        group_expressions.push(expression);
    }
    let mut metrics = Vec::new();
    let types = aggregate_types(aggregation, binding)?;
    for (name, metric) in &aggregation.metrics {
        let expression = if metric.op == "count" {
            "count(*)".into()
        } else {
            let property = metric.property.as_deref().unwrap();
            let mut expression = scalar_sql(binding, property, "effective", parameters)?;
            let op = if metric.op == "average" {
                "avg"
            } else {
                metric.op.as_str()
            };
            let op = if types[name]["type"] == "boolean" {
                if op == "min" { "bool_and" } else { "bool_or" }
            } else {
                op
            };
            if types[name]["type"] == "uid" {
                expression = format!("({expression})::text COLLATE \"C\"");
            }
            let expression = format!("{op}({expression})");
            if metric.op == "average" && types[name]["type"] == "decimal" {
                format!(
                    "round({expression},{})::numeric(38,{})",
                    metric.output_scale.unwrap(),
                    metric.output_scale.unwrap()
                )
            } else if metric.op == "sum" {
                format!("({expression})::{}", postgres_type(&types[name])?)
            } else {
                expression
            }
        };
        metrics.push(bind(parameters, Some(name.clone()), "text"));
        metrics.push(scalar_json(&expression, &types[name]));
    }
    let group = if group_expressions.is_empty() {
        String::new()
    } else {
        format!(" GROUP BY {}", group_expressions.join(","))
    };
    Ok(format!(
        "SELECT jsonb_build_object('key',jsonb_build_object({}),'values',jsonb_build_object({}){}) AS result,row_number() OVER ({}) AS ordinal FROM admitted WHERE (SELECT ok FROM budget_guard){group} ORDER BY ordinal LIMIT {}",
        keys.join(","),
        metrics.join(","),
        if collect_uids {
            ",'_readUids',jsonb_agg(uid)"
        } else {
            ""
        },
        if group_expressions.is_empty() {
            String::new()
        } else {
            format!(
                "ORDER BY {}",
                group_expressions
                    .iter()
                    .map(|expression| format!("{expression} ASC NULLS LAST"))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        },
        max_groups + 1
    ))
}
fn aggregate_narrow(
    binding: &Binding,
    aggregation: &Aggregation,
    parameters: &mut Vec<SqlParameter>,
    relation: Option<&crate::projection::ProjectionRelation>,
) -> Result<String> {
    let fields = aggregation
        .group_by
        .iter()
        .chain(
            aggregation
                .metrics
                .values()
                .filter_map(|metric| metric.property.as_ref()),
        )
        .collect::<BTreeSet<_>>();
    let mut projection = vec![];
    for field in fields {
        let key = bind(parameters, Some(field.clone()), "text");
        let value = if let Some(relation) = relation {
            if let Some(column) = relation
                .columns
                .get(field)
                .and_then(|column| column.effective.as_ref())
            {
                scalar_json(&format!("p.\"{column}\""), scalar_type(binding, field)?)
            } else {
                format!("p.effective->{key}")
            }
        } else {
            format!("canonical.effective->{key}")
        };
        projection.extend([key, value]);
    }
    Ok(format!("jsonb_build_object({})", projection.join(",")))
}
fn aggregate_statement(
    prefix: &str,
    query: &Query,
    binding: &Binding,
    parameters: &mut Vec<SqlParameter>,
) -> Result<String> {
    let budget = query
        .budget
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("QUERY_INVALID"))?;
    let aggregation = query
        .aggregation
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("QUERY_INVALID"))?;
    let groups = budget
        .groups
        .ok_or_else(|| anyhow::anyhow!("QUERY_INVALID"))?;
    let sql = aggregate_sql(
        aggregation,
        binding,
        parameters,
        groups,
        query.scope.is_some(),
    )?;
    Ok(format!(
        "{prefix}, aggregate_rows AS MATERIALIZED ({sql}), aggregate_guard AS MATERIALIZED (SELECT moria.native_serving_check_budget(count(*),{groups}) AND moria.native_serving_check_budget(COALESCE(sum(octet_length(result::text)),0)::bigint,{}) AS ok FROM aggregate_rows) SELECT result FROM aggregate_guard CROSS JOIN aggregate_rows WHERE aggregate_guard.ok ORDER BY ordinal",
        budget.response_bytes
    ))
}

struct SortPlan {
    extra: String,
    order: String,
    after: String,
}
fn sort_plan(
    binding: &Binding,
    query: &Query,
    pin: &ReadPin,
    parameters: &mut Vec<SqlParameter>,
    relation: Option<&crate::projection::ProjectionRelation>,
) -> Result<SortPlan> {
    let mut expressions = Vec::new();
    let mut orders = Vec::new();
    let mut clauses = Vec::new();
    let mut equal: Vec<String> = Vec::new();
    for (index, key) in query.sort.iter().enumerate() {
        let schema = scalar_type(binding, &key.property)?;
        let typed = if key.property == binding.definition.schema.primary_key
            && relation.is_some_and(|relation| {
                relation.schema["primaryKey"] == key.property
                    && relation.schema["properties"][&key.property] == *schema
            }) {
            ensure!(
                schema["type"] == "uid" && schema["nullable"] == false,
                "SCHEMA_INVALID"
            );
            Some("p.uid".to_owned())
        } else {
            relation
                .and_then(|relation| relation.columns.get(&key.property))
                .map(|column| {
                    ensure!(column.descriptor == *schema, "SCHEMA_CHANGED");
                    Ok::<_, anyhow::Error>(
                        column
                            .effective
                            .as_ref()
                            .map(|name| format!("p.\"{name}\"")),
                    )
                })
                .transpose()?
                .flatten()
        };
        let expression = if let Some(typed) = typed {
            if schema["type"] == "string" {
                format!("({typed} COLLATE \"C\")")
            } else {
                typed
            }
        } else {
            scalar_sql(
                binding,
                &key.property,
                if relation.is_some() {
                    "p.effective"
                } else {
                    "effective"
                },
                parameters,
            )?
        };
        expressions.push(format!("{expression} AS sk{index}"));
        orders.push(format!(
            "sk{index} {} NULLS LAST",
            match key.direction {
                Direction::Asc => "ASC",
                Direction::Desc => "DESC",
            }
        ));
        if pin.after.is_some() {
            let value = &pin.keys[index];
            let text = if value.is_null() {
                None
            } else if let Some(text) = value.as_str() {
                Some(text.into())
            } else {
                Some(value.to_string())
            };
            let mut bound = bind(parameters, text, &postgres_type(schema)?);
            if schema["type"] == "string" {
                bound = format!("({bound} COLLATE \"C\")");
            }
            let comparison = if value.is_null() {
                "FALSE".into()
            } else {
                format!(
                    "(sk{index} {} {bound} OR sk{index} IS NULL)",
                    match key.direction {
                        Direction::Asc => ">",
                        Direction::Desc => "<",
                    }
                )
            };
            clauses.push(format!(
                "({}{comparison})",
                if equal.is_empty() {
                    String::new()
                } else {
                    format!("{} AND ", equal.join(" AND "))
                }
            ));
            equal.push(format!("sk{index} IS NOT DISTINCT FROM {bound}"));
        }
    }
    orders.push("uid ASC".into());
    let after = if pin.after.is_some() {
        clauses.push(format!(
            "({}uid>$5::text::uuid)",
            if equal.is_empty() {
                String::new()
            } else {
                format!("{} AND ", equal.join(" AND "))
            }
        ));
        clauses.join(" OR ")
    } else {
        "TRUE".into()
    };
    Ok(SortPlan {
        extra: if expressions.is_empty() {
            String::new()
        } else {
            format!(",{}", expressions.join(","))
        },
        order: orders.join(","),
        after,
    })
}
fn page_sql(
    prefix: &str,
    ranked: &str,
    order: &SortPlan,
    layout: Option<&crate::projection::Layout>,
) -> String {
    let candidates = if layout.is_some() {
        format!(
            "(SELECT * FROM ranked WHERE key_safe AND ({}) ORDER BY {} LIMIT $6::text::bigint) UNION ALL (SELECT * FROM ranked WHERE NOT key_safe AND ({}) ORDER BY {} LIMIT $6::text::bigint)",
            order.after, order.order, order.after, order.order
        )
    } else {
        format!(
            "SELECT * FROM ranked WHERE ({}) ORDER BY {} LIMIT $6::text::bigint",
            order.after, order.order
        )
    };
    // The guard consumes only the final authorized TOP K. No driver allocation of a wide JSON row
    // occurs before PostgreSQL has checked the complete page byte budget.
    format!(
        "{prefix}, ranked AS NOT MATERIALIZED ({ranked}), candidates AS NOT MATERIALIZED ({candidates}), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY {} LIMIT $6::text::bigint), byte_guard AS MATERIALIZED (SELECT moria.native_serving_check_budget(COALESCE(sum(octet_length(source::text)::bigint+octet_length(effective::text)+octet_length(current_source::text)+octet_length(current_effective::text)),0)::bigint,67108864::bigint) AS ok FROM page) SELECT uid,source,effective,current_source,current_effective FROM byte_guard CROSS JOIN page WHERE byte_guard.ok ORDER BY {}",
        order.order, order.order
    )
}
async fn query_sort_limits(client: &tokio_postgres::Client) -> Result<()> {
    let row = client
        .query_one(
            "SELECT current_setting('temp_file_limit'),current_setting('work_mem')",
            &[],
        )
        .await?;
    let limit: String = row.get(0);
    ensure!(limit != "-1", "RESOURCE_LIMIT");
    let bytes: i64 = client
        .query_one("SELECT pg_size_bytes($1::text)", &[&limit])
        .await?
        .get(0);
    ensure!(bytes > 0 && bytes <= 268435456, "RESOURCE_LIMIT");
    client.batch_execute("SET LOCAL work_mem='16MB'").await?;
    Ok(())
}
fn projection_window(
    relation: &crate::projection::ProjectionRelation,
    alias: &str,
    revision: &str,
) -> String {
    if relation.current_rows {
        format!("{alias}.to_revision IS NULL")
    } else {
        format!(
            "{alias}.from_revision<={revision} AND ({alias}.to_revision>{revision} OR {alias}.to_revision IS NULL)"
        )
    }
}
async fn projection_query_rows(
    client: &tokio_postgres::Client,
    binding: &Binding,
    attributes: &ValueMap,
    pin: &ReadPin,
    current_revision: &str,
    query: &Query,
    plan: &crate::projection::ProjectionPlan,
) -> Result<Vec<tokio_postgres::Row>> {
    let expected = normalize_schema(&binding.definition.schema)?;
    ensure!(
        plan.pinned.schema == expected && plan.current.schema == expected,
        "SCHEMA_CHANGED"
    );
    let mut parameters = vec![
        SqlParameter::Scalar(None),
        SqlParameter::Scalar(Some(binding.resource.clone())),
        SqlParameter::Scalar(Some(pin.revision.clone())),
        SqlParameter::Scalar(Some(current_revision.into())),
        SqlParameter::Scalar(pin.after.map(|uid| uid.to_string())),
        SqlParameter::Scalar(Some((query.page_size + 1).to_string())),
    ];
    let same_current = pin.revision == current_revision
        && plan.pinned.id == plan.current.id
        && plan.pinned.current_rows
        && plan.current.current_rows;
    let policies = &binding.definition.policies;
    let mut predicates = Vec::new();
    for (expression, types, attrs, values, json, relation, alias, source) in [
        (
            policies.row.as_ref(),
            &policies.source_columns,
            &policies.attributes,
            attributes,
            "p.source",
            &plan.pinned,
            "p",
            true,
        ),
        (
            policies.row.as_ref(),
            &policies.source_columns,
            &policies.attributes,
            attributes,
            "cur.source",
            &plan.current,
            "cur",
            true,
        ),
        (
            policies.object_read.as_ref(),
            &policies.properties,
            &policies.attributes,
            attributes,
            "p.effective",
            &plan.pinned,
            "p",
            false,
        ),
        (
            policies.object_read.as_ref(),
            &policies.properties,
            &policies.attributes,
            attributes,
            "cur.effective",
            &plan.current,
            "cur",
            false,
        ),
    ] {
        predicates.push(predicate_mapped(
            expression,
            types,
            attrs,
            values,
            json,
            &mut parameters,
            Some((relation, alias, source)),
        )?);
    }
    let pinned_policy = stage_definition(client, &binding.resource, plan.pinned.stage).await?;
    let current_policy = stage_definition(client, &binding.resource, plan.current.stage).await?;
    predicates.extend(generation_predicates(
        pinned_policy.as_ref(),
        attributes,
        "p.source",
        "p.effective",
        &mut parameters,
        Some((&plan.pinned, "p")),
    )?);
    predicates.extend(generation_predicates(
        current_policy.as_ref(),
        attributes,
        "cur.source",
        "cur.effective",
        &mut parameters,
        Some((&plan.current, "cur")),
    )?);
    predicates.push(predicate_mapped(
        query.filter.as_ref(),
        &policies.properties,
        &ValueMap::new(),
        &ValueMap::new(),
        "p.effective",
        &mut parameters,
        Some((&plan.pinned, "p", false)),
    )?);
    if let Some(aggregation) = query.aggregation.as_ref().filter(|_| query.scope.is_none()) {
        let budget = query
            .budget
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("QUERY_INVALID"))?;
        let narrow = aggregate_narrow(binding, aggregation, &mut parameters, Some(&plan.pinned))?;
        let join = if same_current {
            String::new()
        } else {
            format!(
                " JOIN {} cur ON cur.uid=p.uid AND {} AND NOT cur.deleted",
                plan.current.table_sql(),
                projection_window(&plan.current, "cur", "$4::text::bigint")
            )
        };
        let predicates = predicates
            .iter()
            .map(|expression| format!("({expression})"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let predicates = if same_current {
            predicates.replace("cur.", "p.")
        } else {
            predicates
        };
        let base = format!(
            "SELECT p.uid,{narrow} AS effective FROM {} p{join} WHERE {} AND NOT p.deleted AND {predicates}",
            plan.pinned.table_sql(),
            projection_window(&plan.pinned, "p", "$3::text::bigint"),
        );
        let prefix = format!(
            "WITH settings AS (SELECT $1::text,$2::text,$3::text::bigint,$4::text::bigint,$5::text::uuid,$6::text::bigint), admitted AS NOT MATERIALIZED ({base} LIMIT {}), budget_guard AS MATERIALIZED (SELECT moria.native_serving_check_budget(count(*),{}) AS ok FROM admitted)",
            budget.scan_rows + 1,
            budget.scan_rows
        );
        let sql = aggregate_statement(&prefix, query, binding, &mut parameters)?;
        return collect_query_rows(
            client,
            &sql,
            &parameters,
            true,
            "projection-engine-aggregate",
        )
        .await;
    }
    let sort = query
        .sort
        .iter()
        .map(|key| crate::projection::SortKey {
            property: key.property.clone(),
            direction: match key.direction {
                Direction::Asc => crate::projection::Direction::Asc,
                Direction::Desc => crate::projection::Direction::Desc,
            },
        })
        .collect::<Vec<_>>();
    let layout = plan.pinned.layout(&sort);
    let order = sort_plan(binding, query, pin, &mut parameters, Some(&plan.pinned))?;
    let safe = layout.map_or(String::new(), |layout| {
        format!(",p.\"{}\" AS key_safe", layout.key_safe)
    });
    let current_alias = if same_current { "p" } else { "cur" };
    let join = if same_current {
        String::new()
    } else {
        format!(
            " JOIN {} cur ON cur.uid=p.uid AND {} AND NOT cur.deleted",
            plan.current.table_sql(),
            projection_window(&plan.current, "cur", "$4::text::bigint")
        )
    };
    let predicates = predicates
        .iter()
        .map(|expression| format!("({expression})"))
        .collect::<Vec<_>>()
        .join(" AND ");
    // At the same exact revision/relation both policy contexts inspect the same immutable row version.
    // Historical cursors retain the separate current-row join and deletion check.
    let predicates = if same_current {
        predicates.replace("cur.", "p.")
    } else {
        predicates
    };
    let [source, effective, current_source, current_effective] = narrow::maps(
        binding,
        query,
        plan,
        [pinned_policy.as_ref(), current_policy.as_ref()],
        current_alias,
        &mut parameters,
    )?;
    let ranked = format!(
        "SELECT p.uid,{source} AS source,{effective} AS effective,{current_source} AS current_source,{current_effective} AS current_effective{}{safe} FROM {} p{join} WHERE {} AND NOT p.deleted AND {predicates}",
        order.extra,
        plan.pinned.table_sql(),
        projection_window(&plan.pinned, "p", "$3::text::bigint")
    );
    let sql = page_sql(
        "WITH settings AS (SELECT $1::text,$2::text,$3::text::bigint,$4::text::bigint,$5::text::uuid,$6::text::bigint)",
        &ranked,
        &order,
        layout,
    );
    ensure!(sql.len() <= 8 * 1024 * 1024, "RESOURCE_LIMIT");
    collect_query_rows(
        client,
        &sql,
        &parameters,
        false,
        if layout.is_some() {
            "projection-layout-with-overflow"
        } else {
            "projection-engine-sort"
        },
    )
    .await
}
async fn canonical_candidates(
    client: &tokio_postgres::Client,
    environment: &str,
    binding: &Binding,
    attributes: &ValueMap,
    pin: &ReadPin,
    current_revision: &str,
    query: &Query,
) -> Result<Option<(String, Vec<SqlParameter>)>> {
    let revision = pin.revision.as_str();
    let after = pin.after;
    let heads=client.query("SELECT pins.kind,h.revision::text,h.stage,h.object_schema,h.required_markings FROM (VALUES('pinned',$3::text::bigint),('current',$4::text::bigint)) pins(kind,revision) JOIN LATERAL (SELECT revision,stage,object_schema,required_markings FROM moria.source_generations WHERE environment=$1 AND object_type=$2 AND revision<=pins.revision ORDER BY revision DESC LIMIT 1) h ON TRUE",&[&environment,&binding.resource,&revision,&current_revision]).await?;
    for head in &heads {
        if let Some(schema) = head.get::<_, Option<Value>>(3) {
            ensure!(
                schema == normalize_schema(&binding.definition.schema)?,
                "SCHEMA_CHANGED"
            );
        }
        ensure!(
            head.get::<_, Vec<String>>(4).iter().all(|mark| binding
                .definition
                .policies
                .markings
                .contains(mark)),
            "MARKING_DOWNGRADE"
        );
    }
    let mut parameters = vec![
        SqlParameter::Scalar(Some(environment.into())),
        SqlParameter::Scalar(Some(binding.resource.clone())),
        SqlParameter::Scalar(Some(revision.into())),
        SqlParameter::Scalar(Some(current_revision.into())),
        SqlParameter::Scalar(after.map(|uid| uid.to_string())),
        SqlParameter::Scalar(Some((query.page_size + 1).to_string())),
    ];
    let pinned_table = "moria.runtime_object_base($1::text,$2::text,$3::text::bigint)";
    let current_table = "moria.runtime_object_base($1::text,$2::text,$4::text::bigint)";
    let pinned_constraint = "TRUE";
    let current_constraint = "TRUE";
    let policies = &binding.definition.policies;
    let effective = "(p.value||COALESCE(po.overrides,'{}'::jsonb))";
    let pinned_source = "p.source_value";
    let current_source = "r.source_value";
    let current = "(cur.value||COALESCE(co.overrides,'{}'::jsonb))";
    let mut predicates = vec![
        predicate(
            policies.row.as_ref(),
            &policies.source_columns,
            &policies.attributes,
            attributes,
            pinned_source,
            &mut parameters,
        )?,
        predicate(
            policies.row.as_ref(),
            &policies.source_columns,
            &policies.attributes,
            attributes,
            "cur.source_value",
            &mut parameters,
        )?,
        predicate(
            policies.object_read.as_ref(),
            &policies.properties,
            &policies.attributes,
            attributes,
            effective,
            &mut parameters,
        )?,
        predicate(
            policies.object_read.as_ref(),
            &policies.properties,
            &policies.attributes,
            attributes,
            current,
            &mut parameters,
        )?,
        predicate(
            query.filter.as_ref(),
            &policies.properties,
            &ValueMap::new(),
            &ValueMap::new(),
            effective,
            &mut parameters,
        )?,
    ];
    let pinned_policy =
        generation_definition(client, environment, &binding.resource, revision).await?;
    let current_policy =
        generation_definition(client, environment, &binding.resource, current_revision).await?;
    predicates.extend(generation_predicates(
        pinned_policy.as_ref(),
        attributes,
        pinned_source,
        effective,
        &mut parameters,
        None,
    )?);
    predicates.extend(generation_predicates(
        current_policy.as_ref(),
        attributes,
        "cur.source_value",
        current,
        &mut parameters,
        None,
    )?);
    let scope = if let Some(scope) = &query.scope {
        format!(
            " AND p.uid>={} AND p.uid<={}",
            bind(&mut parameters, Some(scope.from_uid.to_string()), "uuid"),
            bind(&mut parameters, Some(scope.through_uid.to_string()), "uuid")
        )
    } else {
        String::new()
    };
    let base = format!(
        "SELECT p.uid,{pinned_source} AS source,{effective} AS effective,cur.source_value AS current_source,{current} AS current_effective FROM {pinned_table} p JOIN LATERAL (SELECT r.value,{current_source} AS source_value FROM {current_table} r WHERE {current_constraint} AND r.uid=p.uid OFFSET 0) cur ON TRUE LEFT JOIN LATERAL (SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$1::text AND object_type=$2::text AND uid=p.uid AND revision<=$3::text::bigint ORDER BY revision DESC LIMIT 1) po ON TRUE LEFT JOIN LATERAL (SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$1::text AND object_type=$2::text AND uid=p.uid AND revision<=$4::text::bigint ORDER BY revision DESC LIMIT 1) co ON TRUE WHERE {pinned_constraint}{scope} AND NOT COALESCE(po.tombstone,false) AND NOT COALESCE(co.tombstone,false) AND {}",
        predicates
            .iter()
            .map(|predicate| format!("({predicate})"))
            .collect::<Vec<_>>()
            .join(" AND ")
    );
    Ok(Some((base, parameters)))
}
async fn query_rows(
    client: &tokio_postgres::Client,
    environment: &str,
    binding: &Binding,
    attributes: &ValueMap,
    pin: &ReadPin,
    current_revision: &str,
    query: &Query,
) -> Result<Vec<tokio_postgres::Row>> {
    let global_aggregate = query.aggregation.is_some() && query.scope.is_none();
    if query.budget.is_none() || global_aggregate {
        if !query.sort.is_empty() || global_aggregate {
            query_sort_limits(client).await?;
        }
        if let Some(plan) = crate::projection::resolve(
            client,
            environment,
            &binding.resource,
            &pin.revision,
            current_revision,
        )
        .await?
        {
            return projection_query_rows(
                client,
                binding,
                attributes,
                pin,
                current_revision,
                query,
                &plan,
            )
            .await;
        }
    }
    let Some((mut base, mut parameters)) = canonical_candidates(
        client,
        environment,
        binding,
        attributes,
        pin,
        current_revision,
        query,
    )
    .await?
    else {
        return empty_query_rows(client, binding, query).await;
    };
    let sql = if let Some(budget) = &query.budget {
        if global_aggregate {
            let narrow = aggregate_narrow(
                binding,
                query.aggregation.as_ref().unwrap(),
                &mut parameters,
                None,
            )?;
            base = format!("SELECT canonical.uid,{narrow} AS effective FROM ({base}) canonical");
        }
        let prefix = format!(
            "WITH settings AS (SELECT $5::text::uuid,$6::text::bigint), admitted AS {} ({base} {} LIMIT {}), budget_guard AS MATERIALIZED (SELECT moria.native_serving_check_budget(count(*),{}) AS ok FROM admitted)",
            if global_aggregate {
                "NOT MATERIALIZED"
            } else {
                "MATERIALIZED"
            },
            if global_aggregate {
                ""
            } else {
                "ORDER BY uid ASC"
            },
            budget.scan_rows + 1,
            budget.scan_rows
        );
        if query.aggregation.is_some() {
            aggregate_statement(&prefix, query, binding, &mut parameters)?
        } else {
            let order = sort_plan(binding, query, pin, &mut parameters, None)?;
            let ranked = format!(
                "SELECT admitted.*{} FROM admitted WHERE (SELECT ok FROM budget_guard)",
                order.extra
            );
            page_sql(&prefix, &ranked, &order, None)
        }
    } else {
        let order = sort_plan(binding, query, pin, &mut parameters, None)?;
        let prefix = "WITH settings AS (SELECT $1::text,$2::text,$3::text::bigint,$4::text::bigint,$5::text::uuid,$6::text::bigint)";
        let ranked = format!("SELECT canonical.*{} FROM ({base}) canonical", order.extra);
        page_sql(prefix, &ranked, &order, None)
    };
    collect_query_rows(
        client,
        &sql,
        &parameters,
        query.aggregation.is_some(),
        if global_aggregate {
            "canonical-engine-aggregate"
        } else if query.budget.is_some() {
            "bounded-scope"
        } else if query.sort.is_empty() {
            "canonical-uid"
        } else {
            "canonical-engine-sort"
        },
    )
    .await
}
fn explain_samples() -> Result<&'static std::sync::atomic::AtomicUsize> {
    static SAMPLES: std::sync::OnceLock<std::sync::atomic::AtomicUsize> =
        std::sync::OnceLock::new();
    if let Some(samples) = SAMPLES.get() {
        return Ok(samples);
    }
    let samples = match std::env::var("MORIA_NATIVE_EXPLAIN_SAMPLES") {
        Err(std::env::VarError::NotPresent) => 0,
        Ok(value) => {
            let count = value
                .parse::<usize>()
                .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?;
            ensure!((1..=16).contains(&count), "INPUT_INVALID");
            count
        }
        Err(_) => anyhow::bail!("INPUT_INVALID"),
    };
    Ok(SAMPLES.get_or_init(|| std::sync::atomic::AtomicUsize::new(samples)))
}
fn private_plan(value: &mut Value) {
    match value {
        Value::Object(object) => {
            // EXPLAIN expressions can embed bound attribute values. Operator evidence needs
            // physical nodes and counters, never the SQL or those expressions.
            if let Some(condition) = object.get("Index Cond").and_then(Value::as_str) {
                object.insert(
                    "UID Range Index Condition".into(),
                    json!(condition.contains("uid >=")),
                );
            }
            for field in [
                "Filter",
                "Index Cond",
                "Recheck Cond",
                "Hash Cond",
                "Merge Cond",
                "Join Filter",
                "Output",
                "Sort Key",
                "Presorted Key",
                "Group Key",
                "Hash Key",
                "Cache Key",
                "Function Call",
                "Remote SQL",
                "Query Text",
            ] {
                object.remove(field);
            }
            for value in object.values_mut() {
                private_plan(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                private_plan(value);
            }
        }
        _ => {}
    }
}
async fn collect_query_rows(
    client: &tokio_postgres::Client,
    sql: &str,
    parameters: &[SqlParameter],
    aggregate: bool,
    plan_class: &str,
) -> Result<Vec<tokio_postgres::Row>> {
    if explain_samples()?
        .try_update(
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
            |remaining| remaining.checked_sub(1),
        )
        .is_ok()
    {
        query_sort_limits(client).await?;
        let references = parameters.iter().map(SqlParameter::pg).collect::<Vec<_>>();
        let mut plan: Value = client
            .query_one(
                &format!("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) {sql}"),
                &references,
            )
            .await?
            .get(0);
        private_plan(&mut plan);
        let evidence = json!({"event":"serving_plan","class":plan_class,"plan":plan});
        let bytes = serde_json::to_vec(&evidence)?;
        ensure!(bytes.len() <= 262144, "RESOURCE_LIMIT");
        eprintln!("{}", std::str::from_utf8(&bytes)?);
    }
    let references = parameters.iter().map(SqlParameter::pg).collect::<Vec<_>>();
    let mut stream = Box::pin(client.query_raw(sql, references).await?);
    let mut rows = Vec::new();
    let mut size = 0usize;
    while let Some(row) = stream.try_next().await? {
        for column in if aggregate { 0..=0 } else { 1..=4 } {
            let value: Value = row.get(column);
            size = size
                .checked_add(serde_json::to_vec(&value)?.len())
                .ok_or_else(|| anyhow::anyhow!("RESPONSE_TOO_LARGE"))?;
        }
        ensure!(size <= 64 * 1024 * 1024, "RESPONSE_TOO_LARGE");
        rows.push(row);
    }
    Ok(rows)
}
async fn empty_query_rows(
    client: &tokio_postgres::Client,
    binding: &Binding,
    query: &Query,
) -> Result<Vec<tokio_postgres::Row>> {
    let Some(aggregation) = &query.aggregation else {
        return Ok(vec![]);
    };
    let mut parameters = vec![];
    let sql = format!(
        "WITH admitted AS (SELECT NULL::uuid AS uid,'{{}}'::jsonb AS effective WHERE FALSE), budget_guard AS (SELECT TRUE AS ok) {}",
        aggregate_sql(
            aggregation,
            binding,
            &mut parameters,
            query.budget.as_ref().unwrap().groups.unwrap(),
            query.scope.is_some()
        )?
    );
    let references = parameters.iter().map(SqlParameter::pg).collect::<Vec<_>>();
    Ok(client.query(&sql, &references).await?)
}

pub fn error_code(error: &anyhow::Error) -> &'static str {
    if let Some(code) = crate::automations::domain_error_code(error) {
        return code;
    }
    if let Some(code) = crate::pipelines::domain_error_code(error) {
        return code;
    }
    if let Some(code) = crate::evaluations::domain_error_code(error) {
        return code;
    }
    if let Some(code) = crate::search::domain_error_code(error) {
        return code;
    }
    if let Some(code) = crate::model_http::domain_error_code(error) {
        return code;
    }
    if let Some(code) = crate::database::authority_error_code(error) {
        return code;
    }
    for cause in error.chain() {
        if let Some(policy) = cause.downcast_ref::<PolicyError>() {
            return policy.code;
        }
        if let Some(database) = cause
            .downcast_ref::<tokio_postgres::Error>()
            .and_then(|error| error.as_db_error())
        {
            if database.code().code() == "57014" {
                return "QUERY_TIMEOUT";
            }
            if database.code().code() == "55P03" {
                return "RESOURCE_BUSY";
            }
            if matches!(database.code().code(), "53400" | "53200" | "53100") {
                return "RESOURCE_LIMIT";
            }
            if database.code().code() == "22003" {
                return "VALUE_INVALID";
            }
            if matches!(database.code().code(), "40001" | "40P01") {
                return "CONCURRENT_MODIFICATION";
            }
            if let Some(code) = known_code(database.message()) {
                return code;
            }
        }
    }
    known_code(&error.root_cause().to_string()).unwrap_or("INTERNAL")
}
fn known_code(code: &str) -> Option<&'static str> {
    Some(match code {
        "UNAUTHENTICATED" => "UNAUTHENTICATED",
        "JOB_DELEGATION_REQUIRED"
        | "JOB_DELEGATION_INACTIVE"
        | "JOB_LEASE_LOST"
        | "JOB_PARENT_INACTIVE"
        | "JOB_ACTOR_MISMATCH"
        | "JOB_DELEGATION_DEPTH" => "UNAUTHENTICATED",
        "NATIVE_DRAINING" => "NATIVE_DRAINING",
        "QUERY_TIMEOUT" => "QUERY_TIMEOUT",
        "RESOURCE_LIMIT" => "RESOURCE_LIMIT",
        "FORBIDDEN" => "FORBIDDEN",
        "AMBIGUOUS_RESOURCE" | "CLI_APPROVAL_REQUIRED" => "FORBIDDEN",
        "DESTINATION_FORBIDDEN" => "DESTINATION_FORBIDDEN",
        "NOT_FOUND" => "NOT_FOUND",
        "INPUT_INVALID" => "INPUT_INVALID",
        "QUERY_INVALID" => "QUERY_INVALID",
        "VALUE_INVALID" => "VALUE_INVALID",
        "ROW_INVALID" => "ROW_INVALID",
        "FILESYSTEM_INVALID" => "FILESYSTEM_INVALID",
        "FILESYSTEM_CONFLICT" => "FILESYSTEM_CONFLICT",
        "FILESYSTEM_LEASE_INVALID" => "FILESYSTEM_LEASE_INVALID",
        "UPLOAD_RETIRED" => "UPLOAD_RETIRED",
        "BROWSER_ORIGIN_INVALID" => "BROWSER_ORIGIN_INVALID",
        "BROWSER_ORIGIN_RESERVED" => "BROWSER_ORIGIN_RESERVED",
        "CONTENT_MISMATCH" => "CONTENT_MISMATCH",
        "RESOURCE_BUSY" => "RESOURCE_BUSY",
        "SOURCE_INVALID" => "SOURCE_INVALID",
        "SOURCE_RUNTIME_PROFILE_INVALID" => "SOURCE_RUNTIME_PROFILE_INVALID",
        "SOURCE_RUNTIME_CHANGED" => "SOURCE_RUNTIME_CHANGED",
        "SOURCE_RUNTIME_NOT_READY" => "SOURCE_RUNTIME_NOT_READY",
        "SOURCE_FILE_PROOF_REQUIRED" => "SOURCE_FILE_PROOF_REQUIRED",
        "APP_INVALID" => "APP_INVALID",
        "APP_CORRUPT" => "APP_CORRUPT",
        "APP_UNAVAILABLE" => "APP_UNAVAILABLE",
        "DATASET_BINDING_INVALID" => "DATASET_BINDING_INVALID",
        "DATASET_SCHEMA_EVOLUTION_REQUIRED" => "DATASET_SCHEMA_EVOLUTION_REQUIRED",
        "DATASET_TYPE_PROFILE_PENDING" => "DATASET_TYPE_PROFILE_PENDING",
        "FRESH_DATASET_REVISION_REQUIRED" => "FRESH_DATASET_REVISION_REQUIRED",
        "PLAN_EXPIRED" => "PLAN_EXPIRED",
        "EDIT_INVALID" => "EDIT_INVALID",
        "SCHEMA_CHANGED" => "SCHEMA_CHANGED",
        "SCHEMA_INVALID" => "SCHEMA_INVALID",
        "BINDING_INVALID" => "BINDING_INVALID",
        "IDENTITY_STALE" => "IDENTITY_STALE",
        "AUTHORIZATION_CHANGED" => "AUTHORIZATION_CHANGED",
        "PROPERTY_UNKNOWN" => "PROPERTY_UNKNOWN",
        "PROPERTY_FORBIDDEN" => "PROPERTY_FORBIDDEN",
        "MARKING_DOWNGRADE" => "MARKING_DOWNGRADE",
        "MARKING_FLOW" => "MARKING_FLOW",
        "FUNCTION_EFFECT" => "FUNCTION_EFFECT",
        "FUNCTION_PREPARE_REQUIRED" => "FUNCTION_PREPARE_REQUIRED",
        "FUNCTION_PREPARATION_FAILED" => "FUNCTION_PREPARATION_FAILED",
        "FUNCTION_PREPARATION_CHANGED" => "FUNCTION_PREPARATION_CHANGED",
        "FUNCTION_INVALID" => "FUNCTION_INVALID",
        "FUNCTION_INPUT_INVALID" => "FUNCTION_INPUT_INVALID",
        "FUNCTION_ACTOR_MISMATCH" => "FUNCTION_ACTOR_MISMATCH",
        "FUNCTION_ARTIFACT_INVALID" => "FUNCTION_ARTIFACT_INVALID",
        "FUNCTION_FAILED" => "FUNCTION_FAILED",
        "FUNCTION_LEASE_INVALID" => "FUNCTION_LEASE_INVALID",
        "FUNCTION_RECEIPT_MISSING" => "FUNCTION_RECEIPT_MISSING",
        "FUNCTION_RECURSION" => "FUNCTION_RECURSION",
        "UNSUPPORTED_VALUE_VERSION" => "UNSUPPORTED_VALUE_VERSION",
        "FUNCTION_INPUT_CHANGED" => "FUNCTION_INPUT_CHANGED",
        "FUNCTION_OBSERVATION_INCOMPLETE" => "FUNCTION_OBSERVATION_INCOMPLETE",
        "FUNCTION_READ_LIMIT" => "FUNCTION_READ_LIMIT",
        "FUNCTION_MARKINGS_INVALID" => "FUNCTION_MARKINGS_INVALID",
        "FUNCTION_HTTP" => "FUNCTION_HTTP",
        "FUNCTION_HTTP_LIMIT" => "FUNCTION_HTTP_LIMIT",
        "FUNCTION_LOG_LIMIT" => "FUNCTION_LOG_LIMIT",
        "AGENT_MARKINGS_INVALID" => "AGENT_MARKINGS_INVALID",
        "AGENT_APPLICATION_MISMATCH" => "AGENT_APPLICATION_MISMATCH",
        "AGENT_MODEL_CHANGED" => "AGENT_MODEL_CHANGED",
        "AGENT_CONTEXT_INVALID" => "AGENT_CONTEXT_INVALID",
        "AGENT_ADAPTER_INVALID" => "AGENT_ADAPTER_INVALID",
        "AGENT_INVALID" => "AGENT_INVALID",
        "AGENT_LIMIT_INVALID" => "AGENT_LIMIT_INVALID",
        "AGENT_ARTIFACT_INVALID" => "AGENT_ARTIFACT_INVALID",
        "AGENT_INPUT_INVALID" => "AGENT_INPUT_INVALID",
        "AGENT_KEY_INVALID" => "AGENT_KEY_INVALID",
        "AGENT_SOURCE_INVALID" => "AGENT_SOURCE_INVALID",
        "AGENT_REFERENCE_INVALID" => "AGENT_REFERENCE_INVALID",
        "AGENT_CALL_LIMIT" => "AGENT_CALL_LIMIT",
        "AGENT_RECEIPT_MISSING" => "AGENT_RECEIPT_MISSING",
        "AGENT_FAILED" => "AGENT_FAILED",
        "AGENT_REQUEST_INVALID" => "AGENT_REQUEST_INVALID",
        "AGENT_ADAPTER_UNSUPPORTED" => "AGENT_ADAPTER_UNSUPPORTED",
        "AGENT_MODEL_REQUEST_INVALID" => "AGENT_MODEL_REQUEST_INVALID",
        "AGENT_MODEL_KEY_REQUIRED" => "AGENT_MODEL_KEY_REQUIRED",
        "AGENT_MODEL_RESPONSE_LIMIT" => "AGENT_MODEL_RESPONSE_LIMIT",
        "AGENT_CAPABILITY_UNSUPPORTED" => "AGENT_CAPABILITY_UNSUPPORTED",
        "CURSOR_INVALID" => "CURSOR_INVALID",
        "CURSOR_EXPIRED" => "CURSOR_EXPIRED",
        "HISTORY_EXPIRED" => "HISTORY_EXPIRED",
        "CURSOR_INVALIDATED" => "CURSOR_INVALIDATED",
        "ACTION_CONDITION_FAILED" => "ACTION_CONDITION_FAILED",
        "ACTION_LIMIT" => "ACTION_LIMIT",
        "ACTION_CREATION_UNAVAILABLE" => "ACTION_CREATION_UNAVAILABLE",
        "ACTION_CREATION_CONFLICT" => "ACTION_CREATION_CONFLICT",
        "LINK_CARDINALITY" => "LINK_CARDINALITY",
        "LINK_CONSTRAINT_NOT_READY" => "LINK_CONSTRAINT_NOT_READY",
        "LINK_CONSTRAINT_CHANGED" => "LINK_CONSTRAINT_CHANGED",
        "LINK_CONSTRAINT_INVALID" => "LINK_CONSTRAINT_INVALID",
        "ACTION_CANONICAL_SOURCE_REQUIRED" => "ACTION_CANONICAL_SOURCE_REQUIRED",
        "ACTION_SOURCE_ATTACHMENT_REQUIRED" => "ACTION_SOURCE_ATTACHMENT_REQUIRED",
        "IDENTITY_REQUEST_INVALID" => "IDENTITY_REQUEST_INVALID",
        "IDENTITY_DOMAIN_NOT_EMPTY" => "IDENTITY_DOMAIN_NOT_EMPTY",
        "OWNERSHIP_LOST" => "OWNERSHIP_LOST",
        "PROJECTION_INVALID" => "PROJECTION_INVALID",
        "IDEMPOTENCY_KEY_INVALID" => "IDEMPOTENCY_KEY_INVALID",
        "IDEMPOTENCY_CONFLICT" => "IDEMPOTENCY_CONFLICT",
        "RESULT_EXPIRED" => "RESULT_EXPIRED",
        "UNSUPPORTED_CAPABILITY" => "UNSUPPORTED_CAPABILITY",
        "CONCURRENT_MODIFICATION" => "CONCURRENT_MODIFICATION",
        "BODY_TOO_LARGE" => "BODY_TOO_LARGE",
        "RESPONSE_TOO_LARGE" => "RESPONSE_TOO_LARGE",
        "CONTENT_TYPE" => "CONTENT_TYPE",
        "CONTENT_ENCODING" => "CONTENT_ENCODING",
        "POLICY_MISMATCH" => "POLICY_MISMATCH",
        _ => return None,
    })
}

#[derive(Clone)]
struct HttpState {
    serving: NativeServing,
    authentication: crate::http_auth::HttpAuthentication,
    mcp: crate::mcp::NativeMcp,
    app_host: Option<crate::app_host::NativeAppHost>,
    lifecycle: std::sync::Arc<crate::lifecycle::Lifecycle>,
    admission: std::sync::Arc<tokio::sync::Semaphore>,
    compiler_admission: std::sync::Arc<tokio::sync::Semaphore>,
}
async fn lifecycle_request(
    State(state): State<HttpState>,
    ConnectInfo(remote): ConnectInfo<crate::http_admission::Peer>,
    request: Request<Body>,
    next: axum::middleware::Next,
) -> Response<Body> {
    let observation = state.lifecycle.observe();
    let mut response = if request.uri().authority().is_some() || request.uri().scheme().is_some() {
        json_response(
            Uuid::new_v4().to_string(),
            None,
            Err(anyhow::anyhow!("INPUT_INVALID")),
        )
    } else if matches!(request.uri().path(), "/livez" | "/readyz") {
        let path = request.uri().path().to_owned();
        let method = request.method().clone();
        let headers = request.headers().clone();
        if let Err(error) = state.authentication.check_transport(remote.0, &headers) {
            json_response(Uuid::new_v4().to_string(), None, Err(error))
        } else if !matches!(method, Method::GET | Method::HEAD) {
            Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .header(header::ALLOW, "GET, HEAD")
                .body(Body::empty())
                .unwrap()
        } else {
            let ready = path == "/livez" || state.lifecycle.ready(&state.serving.db).await;
            let value = if path == "/livez" {
                json!({"live":true,"accepting":state.lifecycle.accepting()})
            } else {
                json!({"live":true,"accepting":state.lifecycle.accepting(),"ready":ready})
            };
            Response::builder()
                .status(if ready {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                })
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::CACHE_CONTROL, "no-store")
                .body(if method == Method::HEAD {
                    Body::empty()
                } else {
                    Body::from(value.to_string())
                })
                .unwrap()
        }
    } else if !state.lifecycle.accepting() {
        json_response(
            Uuid::new_v4().to_string(),
            None,
            Err(anyhow::anyhow!("NATIVE_DRAINING")),
        )
    } else {
        next.run(request).await
    };
    if state.authentication.is_public() {
        response.headers_mut().insert(
            header::STRICT_TRANSPORT_SECURITY,
            axum::http::HeaderValue::from_static("max-age=31536000"),
        );
    }
    observation.response(response)
}

async fn handle(
    State(state): State<HttpState>,
    ConnectInfo(remote): ConnectInfo<crate::http_admission::Peer>,
    request: Request<Body>,
) -> Response<Body> {
    let Ok(permit) = state.admission.clone().try_acquire_owned() else {
        return json_response(
            Uuid::new_v4().to_string(),
            None,
            Err(anyhow::anyhow!("RESOURCE_LIMIT")),
        );
    };
    let mut permits = vec![permit];
    let compiler = request
        .uri()
        .path()
        .split('/')
        .filter(|part| !part.is_empty())
        .map(decode)
        .collect::<Result<Vec<_>>>()
        .is_ok_and(|parts| {
            let start = usize::from(parts.first().is_some_and(|part| part == "api"));
            parts.len() > start + 3
                && parts[start..start + 3] == ["management", "v1", "fs"]
                && ["sources", "compiler-context", "releases", "plans"]
                    .contains(&parts[start + 3].as_str())
        });
    if compiler {
        let Ok(permit) = state.compiler_admission.clone().try_acquire_owned() else {
            return json_response(
                Uuid::new_v4().to_string(),
                None,
                Err(anyhow::anyhow!("RESOURCE_LIMIT")),
            );
        };
        permits.push(permit);
    }
    let processing = remote.processing();
    let response = handle_admitted(State(state), ConnectInfo(remote.0), request).await;
    drop(processing);
    crate::http_admission::hold_response(response, permits)
}
async fn handle_admitted(
    State(state): State<HttpState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    request: Request<Body>,
) -> Response<Body> {
    if request.uri().path().starts_with("/api/auth/") {
        return match tokio::time::timeout(
            Duration::from_secs(30),
            state.authentication.browser_handle(remote, request),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => json_response(Uuid::new_v4().to_string(), None, Err(error)),
            Err(_) => json_response(
                Uuid::new_v4().to_string(),
                None,
                Err(anyhow::anyhow!("QUERY_TIMEOUT")),
            ),
        };
    }
    if ["/mcp", "/api/mcp"].contains(&request.uri().path()) {
        return state
            .mcp
            .handle(&state.authentication, remote, request)
            .await;
    }
    if let Some(host) = &state.app_host
        && [Method::GET, Method::HEAD].contains(request.method())
        && !["/api/", "/management/", "/local/", "/v1/"]
            .iter()
            .any(|prefix| request.uri().path().starts_with(prefix))
    {
        let headers = request.headers().clone();
        let method = request.method().clone();
        let uri = request.uri().clone();
        let result = async {
            state.authentication.check_transport(remote, &headers)?;
            ensure!(
                uri.query().is_none() || !uri.path().starts_with("/releases/"),
                "INPUT_INVALID"
            );
            let session = if host.protected() {
                Some(
                    state
                        .authentication
                        .authenticate(remote, &headers, &method)
                        .await?,
                )
            } else {
                None
            };
            host.response(&state.serving.db, session.as_ref(), &method, uri.path())
                .await
        }
        .await;
        return match result {
            Ok(response) => response,
            Err(error)
                if error_code(&error) == "UNAUTHENTICATED"
                    && !uri.path().starts_with("/releases/")
                    && headers
                        .get(header::ACCEPT)
                        .and_then(|value| value.to_str().ok())
                        .is_some_and(|value| {
                            value
                                .split(',')
                                .any(|item| item.trim().split(';').next() == Some("text/html"))
                        }) =>
            {
                Response::builder()
                    .status(StatusCode::SEE_OTHER)
                    .header(header::LOCATION, "/api/auth/bootstrap")
                    .header(header::CACHE_CONTROL, "no-store")
                    .body(Body::empty())
                    .expect("fixed sign-in response")
            }
            Err(error) => json_response(Uuid::new_v4().to_string(), None, Err(error)),
        };
    }
    let request_id = Uuid::new_v4().to_string();
    let path = request.uri().path();
    let path = path.strip_prefix("/api/").map_or(path, |tail| tail);
    let segments = path
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if segments.len() == 6
        && segments[..3] == ["management", "v1", "fs"]
        && ((segments[3] == "entries" && ["content", "files"].contains(&segments[5]))
            || (segments[3] == "sources" && segments[5] == "artifacts"))
    {
        return match tokio::time::timeout(
            Duration::from_secs(1800),
            filesystem_content_dispatch(&state, remote, request),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => json_response(request_id, None, Err(error)),
            Err(_) => json_response(request_id, None, Err(anyhow::anyhow!("QUERY_TIMEOUT"))),
        };
    }
    let admission_location = if request.method() == "POST"
        && request.uri().path().contains("/functions/")
        && request.uri().path().ends_with("/invoke")
    {
        Some("/api/v1/invocations")
    } else if request.method() == "POST"
        && request.uri().path().contains("/agents/")
        && request.uri().path().ends_with("/executions")
    {
        Some("/api/v1/agent-executions")
    } else if request.method() == "POST"
        && request.uri().path().contains("/evaluations/")
        && request.uri().path().ends_with("/runs")
    {
        Some("/api/v1/evaluation-runs")
    } else if request.method() == "POST"
        && request.uri().path().contains("/pipelines/")
        && request.uri().path().ends_with("/executions")
    {
        Some("/api/v1/pipeline-executions")
    } else {
        None
    };
    let result = tokio::time::timeout(Duration::from_secs(30), dispatch(&state, remote, request))
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")));
    json_response(request_id, admission_location, result)
}
fn json_response(
    request_id: String,
    admission_location: Option<&str>,
    result: Result<Value>,
) -> Response<Body> {
    let (status, value) = match result {
        Ok(value) => (
            if admission_location.is_some()
                && ["queued", "accepted", "running", "preparing"]
                    .contains(&value["status"].as_str().unwrap_or(""))
            {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            },
            value,
        ),
        Err(error) => {
            let code = error_code(&error);
            let status = match code {
                "UNAUTHENTICATED" => StatusCode::UNAUTHORIZED,
                "NATIVE_DRAINING"
                | "RESTORE_DISPATCH_DISABLED"
                | "SECRET_CONFIGURATION_UNAVAILABLE" => StatusCode::SERVICE_UNAVAILABLE,
                "NOT_FOUND" => StatusCode::NOT_FOUND,
                "FORBIDDEN"
                | "CLI_APPROVAL_REQUIRED"
                | "DESTINATION_FORBIDDEN"
                | "EVALUATION_ENVIRONMENT_REQUIRED"
                | "EVALUATION_CASE_AUTHORITY"
                | "EVALUATOR_OPERATION_DENIED"
                | "FULL_SOURCE_PURPOSE_REVOKED"
                | "PROPERTY_FORBIDDEN"
                | "FUNCTION_EFFECT"
                | "MARKING_FLOW"
                | "SERVICE_PURPOSE_REVOKED"
                | "SERVICE_ENVIRONMENT_MISMATCH"
                | "JOB_ACCEPTANCE_REQUIRED"
                | "JOB_ACTOR_MISMATCH"
                | "JOB_CAPABILITY_MISMATCH"
                | "JOB_DELEGATION_DEPTH"
                | "JOB_DELEGATION_INACTIVE"
                | "JOB_DELEGATION_REQUIRED"
                | "JOB_ORIGIN_MISMATCH"
                | "JOB_ORIGIN_UNSUPPORTED"
                | "JOB_PARENT_INACTIVE"
                | "JOB_RECEIPT_AUTHORITY_REQUIRED"
                | "INDEXING_TASK_AUTHORITY_REQUIRED"
                | "INDEXING_ACCEPTANCE_REQUIRED"
                | "INDEXING_DELEGATION_INACTIVE"
                | "AGENT_APPLICATION_MISMATCH"
                | "AUTOMATION_DISPATCH_REVOKED"
                | "BROWSER_ORIGIN_INVALID"
                | "BROWSER_ORIGIN_RESERVED"
                | "EVALUATION_SERVICE_REVOKED"
                | "SECRET_CONSUMER_DENIED"
                | "SECRET_APPROVAL_REQUIRED"
                | "SECRET_APPROVAL_REVOKED" => StatusCode::FORBIDDEN,
                "PIPELINE_OPERATION_DENIED"
                | "PIPELINE_PROFILE_REVOKED"
                | "PIPELINE_DESTINATION_REVOKED" => StatusCode::FORBIDDEN,
                "PIPELINE_PROFILE_CONFLICT"
                | "PIPELINE_BINDING_INVALID"
                | "PIPELINE_CONTEXT_INVALID"
                | "PIPELINE_NOT_STARTED"
                | "PIPELINE_ALREADY_STARTED"
                | "PIPELINE_CHECKPOINT_REQUIRED" => StatusCode::CONFLICT,
                "PIPELINE_CAPABILITY_UNSUPPORTED"
                | "PIPELINE_SCHEMA_UNSUPPORTED"
                | "PIPELINE_INPUT_KIND" => StatusCode::UNPROCESSABLE_ENTITY,
                "PIPELINE_WORKER_UNAVAILABLE"
                | "PIPELINE_PROFILE_REQUIRED"
                | "PIPELINE_PROFILE_INVALID" => StatusCode::SERVICE_UNAVAILABLE,
                "PIPELINE_FRAME_LIMIT" | "PIPELINE_LOG_LIMIT" => StatusCode::TOO_MANY_REQUESTS,
                "PIPELINE_OUTPUT_INVALID" | "PIPELINE_FAILED" | "PIPELINE_PUBLICATION_FAILED" => {
                    StatusCode::BAD_GATEWAY
                }
                "BODY_TOO_LARGE" => StatusCode::PAYLOAD_TOO_LARGE,
                "RESOURCE_LIMIT"
                | "AGENT_CALL_LIMIT"
                | "SEARCH_DOCUMENT_LIMIT"
                | "EVALUATION_CASE_LIMIT"
                | "EVALUATION_ENVELOPE_LIMIT" => StatusCode::TOO_MANY_REQUESTS,
                "UNSUPPORTED_CAPABILITY"
                | "ACTION_CANONICAL_SOURCE_REQUIRED"
                | "ACTION_CREATION_UNAVAILABLE"
                | "RESPONSE_TOO_LARGE"
                | "AGENT_CAPABILITY_UNSUPPORTED"
                | "AGENT_ADAPTER_UNSUPPORTED"
                | "AGENT_MODEL_RESPONSE_LIMIT" => StatusCode::UNPROCESSABLE_ENTITY,
                "CURSOR_EXPIRED" | "PLAN_EXPIRED" | "HISTORY_EXPIRED" | "UPLOAD_RETIRED" => {
                    StatusCode::GONE
                }
                "EVALUATION_SERVICE_PROFILE_INVALID" | "AUTOMATION_CURSOR_INVALID" => {
                    StatusCode::BAD_REQUEST
                }
                "QUERY_TIMEOUT" => StatusCode::GATEWAY_TIMEOUT,
                "RESOURCE_BUSY" | "CONCURRENT_MODIFICATION" | "SOURCE_RUNTIME_PROFILE_INVALID" => {
                    StatusCode::SERVICE_UNAVAILABLE
                }
                "CELL_OWNERSHIP_LOST"
                | "OWNERSHIP_LOST"
                | "ACTION_CREATION_CONFLICT"
                | "LINK_CARDINALITY"
                | "LINK_CONSTRAINT_NOT_READY"
                | "LINK_CONSTRAINT_CHANGED"
                | "ACTION_SOURCE_ATTACHMENT_REQUIRED"
                | "IDENTITY_DOMAIN_NOT_EMPTY"
                | "AUTHORIZATION_CHANGED"
                | "SCHEMA_CHANGED"
                | "SOURCE_RUNTIME_CHANGED"
                | "SOURCE_RUNTIME_NOT_READY"
                | "DATASET_BINDING_INVALID"
                | "DATASET_SCHEMA_EVOLUTION_REQUIRED"
                | "DATASET_TYPE_PROFILE_PENDING"
                | "FRESH_DATASET_REVISION_REQUIRED"
                | "MARKING_DOWNGRADE"
                | "CURSOR_INVALIDATED"
                | "ACTION_CONDITION_FAILED"
                | "FUNCTION_INPUT_CHANGED"
                | "SOURCE_FILE_PROOF_REQUIRED"
                | "FUNCTION_OBSERVATION_INCOMPLETE"
                | "FUNCTION_PREPARATION_CHANGED"
                | "SEARCH_NOT_READY"
                | "SEARCH_GENERATION_CHANGED"
                | "SEARCH_NOT_VISIBLE"
                | "SEARCH_MODEL_CHANGED"
                | "SEARCH_SOURCE_CHANGED"
                | "IMMUTABLE_SEARCH_EXECUTION"
                | "MODEL_CALL_IN_PROGRESS"
                | "MODEL_CALL_UNAVAILABLE"
                | "MODEL_BINDING_CHANGED"
                | "EVALUATION_NOT_READY"
                | "EVALUATION_PIN_INVALID"
                | "EVALUATION_SOURCE_INVALID"
                | "EVALUATOR_CONTEXT_INVALID"
                | "FILESYSTEM_CONFLICT"
                | "FILESYSTEM_LEASE_INVALID"
                | "CONTENT_MISMATCH"
                | "AGENT_MODEL_CHANGED"
                | "AGENT_CONTEXT_INVALID"
                | "IDEMPOTENCY_CONFLICT"
                | "AUTOMATION_SOURCE_CHANGED"
                | "AUTOMATION_ALREADY_DISPATCHED"
                | "EVALUATION_CHILD_ALREADY_ACCEPTED"
                | "SECRET_SOURCE_CHANGED"
                | "SECRET_APPROVAL_IMMUTABLE"
                | "SECRET_APPROVAL_CONFLICT" => StatusCode::CONFLICT,
                "EXECUTION_INACTIVE"
                | "IMMUTABLE_JOB_AUTHORITY"
                | "IMMUTABLE_SERVICE_PURPOSE"
                | "JOB_CREDENTIAL_COLLISION"
                | "JOB_LEASE_LOST"
                | "JOB_TOKEN_COLLISION" => StatusCode::CONFLICT,
                "RESULT_EXPIRED" => StatusCode::GONE,
                "MODEL_CONFIGURATION_UNAVAILABLE" => StatusCode::SERVICE_UNAVAILABLE,
                "MODEL_UNAVAILABLE"
                | "MODEL_INCOMPLETE"
                | "MODEL_PROTOCOL"
                | "MODEL_ZERO_VECTOR"
                | "EVALUATOR_RESULT_INVALID"
                | "EVALUATION_RESULT_INVALID"
                | "AUTOMATION_CHILD_RECEIPT_REQUIRED" => StatusCode::BAD_GATEWAY,
                "INTERNAL" | "POLICY_MISMATCH" | "BINDING_INVALID" => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
                "SEARCH_CACHE_CONFLICT"
                | "SEARCH_DOCUMENT_CONFLICT"
                | "SEARCH_FRAME_INCOMPLETE"
                | "SEARCH_GENERATION_INCOMPLETE" => StatusCode::INTERNAL_SERVER_ERROR,
                "SEARCH_PROJECTOR_UNAVAILABLE" | "APP_CORRUPT" | "APP_UNAVAILABLE" => {
                    StatusCode::SERVICE_UNAVAILABLE
                }
                _ => StatusCode::BAD_REQUEST,
            };
            (
                status,
                json!({"error":{"code":code,"message":code,"requestId":request_id,"correlationId":request_id,"retryable":matches!(code,"RESOURCE_BUSY"|"CONCURRENT_MODIFICATION"|"QUERY_TIMEOUT"),"details":null}}),
            )
        }
    };
    let mut response = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-store")
        .header("x-content-type-options", "nosniff")
        .header("referrer-policy", "no-referrer");
    if let Some(location) = admission_location
        && status == StatusCode::ACCEPTED
        && let Some(id) = value["id"].as_str()
    {
        response = response.header(header::LOCATION, format!("{location}/{id}"));
    }
    response
        .body(Body::from(serde_json::to_vec(&value).unwrap()))
        .unwrap()
}
async fn http_session(
    state: &HttpState,
    remote: SocketAddr,
    headers: &axum::http::HeaderMap,
    uri: &axum::http::Uri,
    method: &axum::http::Method,
    max_bytes: usize,
) -> Result<VerifiedSession> {
    let path = uri.path();
    ensure!(
        path.starts_with('/')
            && !path.starts_with("//")
            && uri.authority().is_none()
            && uri.scheme().is_none(),
        "INPUT_INVALID"
    );
    ensure!(
        headers
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
            .is_none_or(|size| size <= max_bytes),
        "BODY_TOO_LARGE"
    );
    state
        .authentication
        .authenticate(remote, headers, method)
        .await
}
async fn dispatch(state: &HttpState, remote: SocketAddr, request: Request<Body>) -> Result<Value> {
    let session = http_session(
        state,
        remote,
        request.headers(),
        request.uri(),
        request.method(),
        65536,
    )
    .await?;
    let path = request.uri().path();
    let mut parts = path
        .split('/')
        .filter(|part| !part.is_empty())
        .map(decode)
        .collect::<Result<Vec<_>>>()?;
    if parts.first().is_some_and(|part| part == "api") {
        parts.remove(0);
    }
    if parts == ["management", "v1", "runtime"] && request.method() == Method::GET {
        ensure!(request.uri().query().is_none(), "INPUT_INVALID");
        let admission = Admission::begin(&state.serving.db, &session, false).await?;
        let outcome = async {
            let snapshot = Authority::current(admission.client(), &session).await?;
            crate::authorization::authorize(&snapshot.identity, "instance", "manage", &[])?;
            let mut metrics = state.lifecycle.metrics();
            metrics["availableRequestSlots"] = json!(state.admission.available_permits());
            metrics["availableCompilerSlots"] = json!(state.compiler_admission.available_permits());
            Authority::finish(admission.client(), &session, &snapshot).await?;
            Ok(metrics)
        }
        .await;
        return admission.complete(outcome).await;
    }
    if parts.len() >= 3 && parts[0] == "management" && parts[1] == "v1" && parts[2] == "fs" {
        return filesystem_dispatch(state, &session, &parts[3..], request).await;
    }
    if parts == ["v1", "contract"] && request.method() == Method::GET {
        ensure!(request.uri().query().is_none(), "INPUT_INVALID");
        return state
            .app_host
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?
            .contract(&state.serving.db, &session)
            .await;
    }
    ensure!(
        (parts.len() == 3
            || parts.len() == 4
            || (parts.len() == 5 && parts[1] == "objects" && parts[4] == "attach-source"))
            && parts[0] == "v1",
        "NOT_FOUND"
    );
    if request.method() == Method::GET
        && parts.len() == 4
        && parts[1] == "automations"
        && parts[3] == "firings"
    {
        let id = canonical_uid(&parts[2])?;
        let query = axum::extract::Query::<Vec<(String, String)>>::try_from_uri(request.uri())
            .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
            .0;
        ensure!(
            query.len() <= 1 && query.iter().all(|(key, _)| key == "after"),
            "INPUT_INVALID"
        );
        return crate::automations::NativeAutomations::new(state.serving.db.clone())
            .firings(&session, id, query.first().map(|(_, value)| value.as_str()))
            .await;
    }
    if request.method() == "GET" && parts[1] == "invocations" {
        let functions = crate::functions::NativeFunctions::new(
            state.serving.db.clone(),
            std::sync::Arc::new(crate::platform::NativePlatformBroker::new(
                state.serving.db.clone(),
            )),
        );
        let id = canonical_uid(&parts[2])?;
        if parts.len() == 3 {
            ensure!(request.uri().query().is_none(), "INPUT_INVALID");
            return functions.get(&session, id).await;
        }
        ensure!(parts[3] == "events", "NOT_FOUND");
        let query = axum::extract::Query::<Vec<(String, String)>>::try_from_uri(request.uri())
            .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
            .0;
        ensure!(
            query.len() <= 1 && query.iter().all(|(key, _)| key == "after"),
            "INPUT_INVALID"
        );
        let events = functions
            .events(
                &session,
                id,
                query.first().map_or("0", |(_, value)| value.as_str()),
            )
            .await?;
        return Ok(json!({"events":events}));
    }
    if parts[1] == "agent-executions" {
        let agents = crate::agents::NativeAgents::new(state.serving.db.clone())?;
        let id = canonical_uid(&parts[2])?;
        if request.method() == "GET" && parts.len() == 3 {
            ensure!(request.uri().query().is_none(), "INPUT_INVALID");
            return agents.get(&session, id).await;
        }
        if request.method() == "GET" && parts.len() == 4 && parts[3] == "events" {
            let query = axum::extract::Query::<Vec<(String, String)>>::try_from_uri(request.uri())
                .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
                .0;
            ensure!(
                query.len() <= 1 && query.iter().all(|(key, _)| key == "after"),
                "INPUT_INVALID"
            );
            return agents
                .events(
                    &session,
                    id,
                    query.first().map_or("0", |(_, value)| value.as_str()),
                )
                .await;
        }
    }
    if parts[1] == "pipeline-executions" && request.method() == "GET" {
        let pipelines = crate::pipelines::NativePipelines::new(state.serving.db.clone());
        let id = canonical_uid(&parts[2])?;
        if parts.len() == 3 {
            ensure!(request.uri().query().is_none(), "INPUT_INVALID");
            return pipelines.get(&session, id).await;
        }
        if parts[3] == "events" {
            let query = axum::extract::Query::<Vec<(String, String)>>::try_from_uri(request.uri())
                .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
                .0;
            ensure!(
                query.len() <= 1 && query.iter().all(|(key, _)| key == "after"),
                "INPUT_INVALID"
            );
            return pipelines
                .events(
                    &session,
                    id,
                    query.first().map_or("0", |(_, value)| value.as_str()),
                )
                .await;
        }
        anyhow::bail!("NOT_FOUND");
    }
    if parts[1] == "evaluation-runs" && request.method() == "GET" {
        let evaluations = crate::evaluations::NativeEvaluations::new(state.serving.db.clone())?;
        let id = canonical_uid(&parts[2])?;
        if parts.len() == 3 {
            ensure!(request.uri().query().is_none(), "INPUT_INVALID");
            return evaluations.get(&session, id).await;
        }
        let query = axum::extract::Query::<Vec<(String, String)>>::try_from_uri(request.uri())
            .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
            .0;
        let argument = if parts[3] == "compare" {
            "other"
        } else {
            "after"
        };
        ensure!(
            query.len() <= 1 && query.iter().all(|(key, _)| key == argument),
            "INPUT_INVALID"
        );
        return match parts[3].as_str() {
            "cases" => {
                evaluations
                    .cases(&session, id, query.first().map(|(_, value)| value.as_str()))
                    .await
            }
            "events" => {
                evaluations
                    .events(
                        &session,
                        id,
                        query.first().map_or("0", |(_, value)| value.as_str()),
                    )
                    .await
            }
            "compare" => {
                evaluations
                    .compare(
                        &session,
                        id,
                        canonical_uid(
                            query
                                .first()
                                .map(|(_, value)| value.as_str())
                                .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?,
                        )?,
                    )
                    .await
            }
            _ => Err(anyhow::anyhow!("NOT_FOUND")),
        };
    }
    ensure!(
        parts.len() == 4
            || (parts.len() == 5 && parts[1] == "objects" && parts[4] == "attach-source"),
        "NOT_FOUND"
    );
    if request.method() == "GET" && parts[1] == "objects" {
        let query = axum::extract::Query::<Vec<(String, String)>>::try_from_uri(request.uri())
            .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
            .0;
        ensure!(
            query.len() <= 1 && query.iter().all(|(name, _)| name == "properties"),
            "INPUT_INVALID"
        );
        let projection = query.first().map(|(_, value)| {
            if value.is_empty() {
                vec![]
            } else {
                value.split(',').map(String::from).collect()
            }
        });
        return state
            .serving
            .read(&session, &parts[2], &parts[3], projection)
            .await;
    }
    ensure!(request.method() == "POST", "NOT_FOUND");
    ensure!(request.uri().query().is_none(), "INPUT_INVALID");
    ensure!(
        request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value
                .split(';')
                .next()
                .unwrap_or("")
                .eq_ignore_ascii_case("application/json")),
        "CONTENT_TYPE"
    );
    ensure!(
        request
            .headers()
            .get(header::CONTENT_ENCODING)
            .and_then(|value| value.to_str().ok())
            .is_none_or(|value| value == "identity"),
        "CONTENT_ENCODING"
    );
    let key = request
        .headers()
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(String::from);
    let body = to_bytes(request.into_body(), 65536)
        .await
        .map_err(|_| anyhow::anyhow!("BODY_TOO_LARGE"))?;
    let body: Value =
        serde_json::from_slice(&body).map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?;
    if parts.len() == 5 && parts[1] == "objects" && parts[4] == "attach-source" {
        return state
            .serving
            .attach_source(
                &session,
                &parts[2],
                &parts[3],
                key.as_deref()
                    .ok_or_else(|| anyhow::anyhow!("IDEMPOTENCY_KEY_INVALID"))?,
                &body,
            )
            .await;
    }
    if parts[1] == "search" {
        let search = crate::search::NativeSearch {
            db: state.serving.db.clone(),
            models: crate::models::NativeModels::new(state.serving.db.clone())?,
        };
        if parts[3] == "visibility" {
            ensure!(
                body.as_object().is_some_and(|body| body.is_empty()),
                "INPUT_INVALID"
            );
            return search.visibility(&session, &parts[2]).await;
        }
        if parts[3] == "rebuild" {
            ensure!(
                body.as_object().is_some_and(|body| body.is_empty()),
                "INPUT_INVALID"
            );
            return search
                .request_rebuild(
                    &session,
                    &parts[2],
                    key.as_deref()
                        .ok_or_else(|| anyhow::anyhow!("IDEMPOTENCY_KEY_INVALID"))?,
                )
                .await;
        }
        if parts[3] == "status" {
            ensure!(
                body.as_object()
                    .is_some_and(|body| body.len() == 1 && body.contains_key("execution")),
                "INPUT_INVALID"
            );
            return search
                .inspect_rebuild(
                    &session,
                    &parts[2],
                    canonical_uid(
                        body["execution"]
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?,
                    )?,
                )
                .await;
        }
        if parts[3] == "query" {
            let request: crate::search::SearchRequest = serde_json::from_value(body)
                .map_err(|_| anyhow::anyhow!("SEARCH_INPUT_INVALID"))?;
            return search
                .query(
                    &session,
                    &parts[2],
                    request,
                    key.as_deref()
                        .ok_or_else(|| anyhow::anyhow!("IDEMPOTENCY_KEY_INVALID"))?,
                )
                .await;
        }
        anyhow::bail!("NOT_FOUND");
    }
    if parts[1] == "pipelines" && parts[3] == "executions" {
        ensure!(
            body.as_object()
                .is_some_and(|body| body.keys().all(|key| key == "wait")
                    && body.get("wait").is_none_or(Value::is_boolean)),
            "INPUT_INVALID"
        );
        let pipelines = crate::pipelines::NativePipelines::new(state.serving.db.clone());
        let mut value = pipelines
            .invoke(
                &session,
                canonical_uid(&parts[2])?,
                key.as_deref()
                    .ok_or_else(|| anyhow::anyhow!("IDEMPOTENCY_KEY_INVALID"))?,
            )
            .await?;
        if body["wait"] != false {
            let id = canonical_uid(
                value["id"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("INTERNAL"))?,
            )?;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
            while ["queued", "running", "preparing"]
                .contains(&value["status"].as_str().unwrap_or(""))
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
                value = pipelines.get(&session, id).await?;
            }
        }
        return Ok(value);
    }
    if parts[1] == "pipeline-executions" && parts[3] == "cancel" {
        ensure!(
            body.as_object().is_some_and(|body| body.is_empty()),
            "INPUT_INVALID"
        );
        return crate::pipelines::NativePipelines::new(state.serving.db.clone())
            .cancel(&session, canonical_uid(&parts[2])?)
            .await;
    }
    if parts[1] == "evaluations" && parts[3] == "runs" {
        let evaluations = crate::evaluations::NativeEvaluations::new(state.serving.db.clone())?;
        let wait = body.get("wait").and_then(Value::as_bool) != Some(false);
        let mut value = evaluations
            .invoke(
                &session,
                canonical_uid(&parts[2])?,
                key.as_deref()
                    .ok_or_else(|| anyhow::anyhow!("IDEMPOTENCY_KEY_INVALID"))?,
                body,
            )
            .await?;
        if wait {
            let id = canonical_uid(
                value["id"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("INTERNAL"))?,
            )?;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
            while ["queued", "running", "preparing"]
                .contains(&value["status"].as_str().unwrap_or(""))
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
                value = evaluations.get(&session, id).await?;
            }
        }
        return Ok(value);
    }
    if parts[1] == "evaluation-runs" && parts[3] == "cancel" {
        ensure!(
            body.as_object().is_some_and(|body| body.is_empty()),
            "INPUT_INVALID"
        );
        return crate::evaluations::NativeEvaluations::new(state.serving.db.clone())?
            .cancel(&session, canonical_uid(&parts[2])?)
            .await;
    }
    if parts[1] == "agents" && parts[3] == "executions" {
        let agents = crate::agents::NativeAgents::new(state.serving.db.clone())?;
        let wait = body.get("wait").and_then(Value::as_bool) != Some(false);
        let mut value = agents
            .invoke(
                &session,
                canonical_uid(&parts[2])?,
                key.as_deref()
                    .ok_or_else(|| anyhow::anyhow!("IDEMPOTENCY_KEY_INVALID"))?,
                body,
            )
            .await?;
        if wait {
            let accepted = value.clone();
            let id = canonical_uid(
                value["id"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("INTERNAL"))?,
            )?;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
            while ["queued", "accepted", "running"]
                .contains(&value["status"].as_str().unwrap_or(""))
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
                value = match agents.get(&session, id).await {
                    Ok(current) => current,
                    Err(error) if error_code(&error) == "FUNCTION_OBSERVATION_INCOMPLETE" => {
                        accepted.clone()
                    }
                    Err(error) => return Err(error),
                };
            }
        }
        return Ok(value);
    }
    if parts[1] == "agent-executions" && parts[3] == "cancel" {
        ensure!(
            body.as_object().is_some_and(|object| object.is_empty()),
            "INPUT_INVALID"
        );
        return crate::agents::NativeAgents::new(state.serving.db.clone())?
            .cancel(&session, canonical_uid(&parts[2])?)
            .await;
    }
    if parts[1] == "functions" && parts[3] == "invoke" {
        ensure!(
            body.as_object()
                .is_some_and(|object| object.contains_key("input")
                    && object
                        .keys()
                        .all(|key| ["input", "wait"].contains(&key.as_str())))
                && body.get("wait").is_none_or(Value::is_boolean),
            "INPUT_INVALID"
        );
        let functions = crate::functions::NativeFunctions::new(
            state.serving.db.clone(),
            std::sync::Arc::new(crate::platform::NativePlatformBroker::new(
                state.serving.db.clone(),
            )),
        );
        let value = functions
            .invoke(
                &session,
                canonical_uid(&parts[2])?,
                key.as_deref()
                    .ok_or_else(|| anyhow::anyhow!("IDEMPOTENCY_KEY_INVALID"))?,
                body["input"].clone(),
                None,
                false,
            )
            .await?;
        if body.get("wait").and_then(Value::as_bool) != Some(false) {
            let outcome = functions
                .wait(
                    &session,
                    canonical_uid(
                        value["id"]
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("INTERNAL"))?,
                    )?,
                    25000,
                )
                .await;
            return match outcome {
                Err(error) if error_code(&error) == "FUNCTION_OBSERVATION_INCOMPLETE" => Ok(value),
                other => other,
            };
        }
        return Ok(value);
    }
    if parts[1] == "invocations" && parts[3] == "cancel" {
        ensure!(
            body.as_object().is_some_and(|object| object.is_empty()),
            "INPUT_INVALID"
        );
        return crate::functions::NativeFunctions::new(
            state.serving.db.clone(),
            std::sync::Arc::new(crate::platform::NativePlatformBroker::new(
                state.serving.db.clone(),
            )),
        )
        .cancel(&session, canonical_uid(&parts[2])?)
        .await;
    }
    if parts[1] == "objects" && parts[3] == "query" {
        return state.serving.query(&session, &parts[2], &body).await;
    }
    if parts[1] == "objects" && parts[3] == "aggregate" {
        return state.serving.aggregate(&session, &parts[2], &body).await;
    }
    if parts[1] == "links" && parts[3] == "traverse" {
        return state.serving.traverse(&session, &parts[2], &body).await;
    }
    if parts[1] == "actions" && parts[3] == "invoke" {
        ensure!(
            body.as_object()
                .is_some_and(|body| body.len() == 1 && body.contains_key("input")),
            "INPUT_INVALID"
        );
        return state
            .serving
            .invoke(
                &session,
                &parts[2],
                key.as_deref()
                    .ok_or_else(|| anyhow::anyhow!("IDEMPOTENCY_KEY_INVALID"))?,
                &body["input"],
            )
            .await;
    }
    bail!("NOT_FOUND")
}
pub(crate) fn decode(text: &str) -> Result<String> {
    let mut result = Vec::new();
    let bytes = text.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            ensure!(index + 2 < bytes.len(), "INPUT_INVALID");
            let hex = std::str::from_utf8(&bytes[index + 1..index + 3])
                .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?;
            result.push(u8::from_str_radix(hex, 16).map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?);
            index += 3;
        } else {
            result.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(result).map_err(|_| anyhow::anyhow!("INPUT_INVALID"))
}
async fn filesystem_content_dispatch(
    state: &HttpState,
    remote: SocketAddr,
    request: Request<Body>,
) -> Result<Response<Body>> {
    use crate::filesystem::{ContentDeclaration, FileWrite, NativeFilesystem, UploadStart};
    let session = http_session(
        state,
        remote,
        request.headers(),
        request.uri(),
        request.method(),
        64 * 1024 * 1024,
    )
    .await?;
    let mut parts = request
        .uri()
        .path()
        .split('/')
        .filter(|part| !part.is_empty())
        .map(decode)
        .collect::<Result<Vec<_>>>()?;
    if parts.first().is_some_and(|part| part == "api") {
        parts.remove(0);
    }
    ensure!(
        parts.len() == 6
            && parts[..3] == ["management", "v1", "fs"]
            && ((parts[3] == "entries" && ["content", "files"].contains(&parts[5].as_str()))
                || (parts[3] == "sources" && parts[5] == "artifacts")),
        "NOT_FOUND"
    );
    let id = canonical_uid(&parts[4])?;
    let query = axum::extract::Query::<Vec<(String, String)>>::try_from_uri(request.uri())
        .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
        .0;
    let filesystem = NativeFilesystem {
        db: state.serving.db.clone(),
    };
    let compiler = parts[3] == "sources";
    if request.method() == "GET" && parts[5] == "content" {
        ensure!(
            query.len() <= 1 && query.iter().all(|(key, _)| key == "revision"),
            "INPUT_INVALID"
        );
        let read = filesystem
            .read_file(
                &session,
                id,
                query.first().map(|(_, value)| value.as_str()),
                None,
            )
            .await?;
        let content = read
            .entry
            .content
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("CONTENT_MISMATCH"))?;
        return Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                content["mediaType"]
                    .as_str()
                    .unwrap_or("application/octet-stream"),
            )
            .header(header::CONTENT_LENGTH, content["size"].to_string())
            .header("x-moria-digest", content["digest"].as_str().unwrap_or(""))
            .header("x-moria-revision", &read.entry.revision)
            .header(header::CACHE_CONTROL, "no-store")
            .header("x-content-type-options", "nosniff")
            .header(header::CONTENT_DISPOSITION, "attachment")
            .header("content-security-policy", "sandbox; default-src 'none'")
            .header("x-frame-options", "DENY")
            .body(Body::from_stream(
                read.stream
                    .map_err(|error| std::io::Error::other(error_code(&error))),
            ))
            .map_err(anyhow::Error::from);
    }
    ensure!(
        request.method() == "POST"
            && ["content", "files", "artifacts"].contains(&parts[5].as_str()),
        "NOT_FOUND"
    );
    ensure!(
        request
            .headers()
            .get(header::CONTENT_ENCODING)
            .and_then(|value| value.to_str().ok())
            .is_none_or(|value| value == "identity"),
        "CONTENT_ENCODING"
    );
    let headers = request.headers();
    let text = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))
    };
    let size = text("x-moria-size")?
        .parse::<u64>()
        .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?;
    ensure!(
        size <= (if compiler {
            16 * 1024 * 1024
        } else {
            64 * 1024 * 1024
        }) && size.to_string() == text("x-moria-size")?,
        "BODY_TOO_LARGE"
    );
    if let Some(length) = request.headers().get(header::CONTENT_LENGTH) {
        ensure!(
            length
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                == Some(size),
            "CONTENT_MISMATCH"
        );
    }
    let content = ContentDeclaration {
        digest: text("x-moria-digest")?.into(),
        size,
        media_type: text("content-type")?.into(),
    };
    let key = text("idempotency-key")?.to_owned();
    let expected = query
        .iter()
        .find(|(key, _)| key == "expectedRevision")
        .map(|(_, value)| value.clone())
        .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
    let input = if parts[5] == "files" || compiler {
        ensure!(
            query.len() == (if compiler { 4 } else { 2 })
                && query.iter().all(|(key, _)| if compiler {
                    ["name", "expectedRevision", "parentId", "sourceDigest"].contains(&key.as_str())
                } else {
                    ["name", "expectedRevision"].contains(&key.as_str())
                }),
            "INPUT_INVALID"
        );
        FileWrite::Create {
            parent_id: if compiler {
                canonical_uid(
                    query
                        .iter()
                        .find(|(key, _)| key == "parentId")
                        .map(|(_, value)| value.as_str())
                        .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?,
                )?
            } else {
                id
            },
            expected_parent_revision: expected,
            name: query
                .iter()
                .find(|(key, _)| key == "name")
                .map(|(_, value)| value.clone())
                .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?,
            content,
        }
    } else {
        ensure!(query.len() == 1, "INPUT_INVALID");
        FileWrite::Write {
            id,
            expected_revision: expected,
            content,
        }
    };
    let started = if compiler {
        crate::filesystem::NativeSources {
            db: state.serving.db.clone(),
        }
        .begin_artifact(
            &session,
            id,
            query
                .iter()
                .find(|(key, _)| key == "sourceDigest")
                .map(|(_, value)| value.as_str())
                .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?,
            &key,
            input,
        )
        .await?
    } else {
        filesystem
            .begin_file(&session, &key, input, None, None)
            .await?
    };
    match started {
        UploadStart::Receipt(receipt) => {
            Ok(json_response(Uuid::new_v4().to_string(), None, Ok(receipt)))
        }
        UploadStart::Upload(mut upload) => {
            let mut body = request.into_body().into_data_stream();
            let copied = async {
                while let Some(bytes) =
                    tokio::time::timeout(Duration::from_secs(30), body.try_next())
                        .await
                        .map_err(|_| anyhow::anyhow!("QUERY_TIMEOUT"))?
                        .map_err(|_| anyhow::anyhow!("CONTENT_MISMATCH"))?
                {
                    upload.append(&bytes).await?;
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if let Err(error) = copied {
                let _ = upload.abort().await;
                return Err(error);
            }
            Ok(json_response(
                Uuid::new_v4().to_string(),
                None,
                upload.finish().await,
            ))
        }
    }
}
async fn filesystem_dispatch(
    state: &HttpState,
    session: &VerifiedSession,
    parts: &[String],
    request: Request<Body>,
) -> Result<Value> {
    let query = axum::extract::Query::<Vec<(String, String)>>::try_from_uri(request.uri())
        .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
        .0;
    let method = request.method().clone();
    let key = request
        .headers()
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(String::from);
    let input = if method == "GET" {
        Value::Null
    } else {
        ensure!(method == "POST", "NOT_FOUND");
        ensure!(
            request
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value
                    .split(';')
                    .next()
                    .unwrap_or("")
                    .eq_ignore_ascii_case("application/json")),
            "CONTENT_TYPE"
        );
        ensure!(
            request
                .headers()
                .get(header::CONTENT_ENCODING)
                .and_then(|value| value.to_str().ok())
                .is_none_or(|value| value == "identity"),
            "CONTENT_ENCODING"
        );
        let bytes = to_bytes(request.into_body(), 65536)
            .await
            .map_err(|_| anyhow::anyhow!("BODY_TOO_LARGE"))?;
        serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
    };
    crate::filesystem::NativeFilesystem {
        db: state.serving.db.clone(),
    }
    .route(
        session,
        &crate::filesystem::FilesystemRequest {
            method: method.as_str(),
            parts,
            query: &query,
            input: &input,
            key: key.as_deref(),
            consumed: &[],
        },
    )
    .await
}
pub async fn serve(db: Database, listen: SocketAddr) -> Result<()> {
    ensure!(
        listen.ip().is_loopback(),
        "Loopback serving listener required"
    );
    let listener = tokio::net::TcpListener::bind(listen).await?;
    let address = listener.local_addr()?;
    let admission = std::sync::Arc::new(tokio::sync::Semaphore::new(64));
    let lifecycle = crate::lifecycle::Lifecycle::configured()?;
    let state = HttpState {
        lifecycle: lifecycle.clone(),
        admission: admission.clone(),
        compiler_admission: std::sync::Arc::new(tokio::sync::Semaphore::new(4)),
        serving: NativeServing { db: db.clone() },
        mcp: crate::mcp::NativeMcp::configured(db.clone())?,
        app_host: crate::app_host::NativeAppHost::configured()?,
        authentication: crate::http_auth::HttpAuthentication::configured(
            db.clone(),
            &address.to_string(),
        )?,
    };
    state.authentication.initialize_cli().await?;
    state.authentication.validate_browser_configuration()?;
    let router = Router::new()
        .fallback(any(handle))
        .with_state(state.clone())
        .merge(crate::model_http::router(
            db,
            address.to_string(),
            admission,
        )?)
        .layer(axum::middleware::from_fn_with_state(
            state,
            lifecycle_request,
        ));
    println!(
        "{}",
        json!({"event":"serving","address":address.to_string()})
    );
    let shutdown = lifecycle.clone();
    lifecycle
        .run(
            axum::serve(
                crate::http_admission::BoundedListener::new(listener),
                router.into_make_service_with_connect_info::<crate::http_admission::Peer>(),
            )
            .with_graceful_shutdown(async move { shutdown.shutdown_signal().await }),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn effective_primary_uid_predicates_seek_the_permanent_row_key() {
        let descriptor = json!({"type":"uid","nullable":false});
        let fields = ValueMap::from([("uid".into(), descriptor.clone())]);
        let column = crate::projection::TypedColumn {
            source: Some("s0".into()),
            effective: Some("e0".into()),
            descriptor: descriptor.clone(),
            source_descriptor: None,
        };
        let relation = crate::projection::ProjectionRelation {
            id: Uuid::new_v4(),
            stage: Uuid::new_v4(),
            source_revision: "1".into(),
            activation_revision: "1".into(),
            closed_revision: None,
            schema: json!({"primaryKey":"uid","properties":{"uid":descriptor}}),
            source_schema: json!({"uid":descriptor}),
            columns: BTreeMap::from([("uid".into(), column.clone())]),
            source_columns: BTreeMap::from([("uid".into(), column)]),
            layouts: vec![],
            current_rows: true,
        };
        let predicate = json!({"op":"gte","args":[{"field":"uid"},{"literal":"00000000-0000-4000-8000-000000000001"}]});
        let mut parameters = vec![];
        let effective = predicate_mapped(
            Some(&predicate),
            &fields,
            &ValueMap::new(),
            &ValueMap::new(),
            "p.effective",
            &mut parameters,
            Some((&relation, "p", false)),
        )
        .unwrap();
        assert!(effective.contains("p.uid"));
        assert!(!effective.contains("e0"));
        assert_eq!(parameters.len(), 1);
        let canonical = predicate_mapped(
            Some(&predicate),
            &fields,
            &ValueMap::new(),
            &ValueMap::new(),
            "p.source",
            &mut vec![],
            Some((&relation, "p", true)),
        )
        .unwrap();
        assert!(canonical.contains("p.\"s0\""));
        assert!(!canonical.contains("p.uid"));
    }
    #[test]
    fn consumed_views_ignore_unrelated_fields_but_preserve_old_policy_witnesses() {
        let properties = json!({"uid":{"type":"uid","nullable":false},"value":{"type":"int64","nullable":false},"hidden":{"type":"string","nullable":false},"region":{"type":"string","nullable":false}});
        let binding=Binding{resource:"objects".into(),revision:"v1".into(),definition:serde_json::from_value(json!({"schema":{"primaryKey":"uid","properties":properties},"policies":{"resource":"objects","markings":[],"properties":properties,"sourceColumns":properties,"attributes":{"region":{"type":"string","nullable":false}},"row":{"op":"eq","args":[{"field":"region"},{"principalAttribute":"region"}]},"propertyRead":{"hidden":{"literal":false}}}})).unwrap()};
        let uid = Uuid::new_v4();
        let row = ObjectRow {
            source: serde_json::from_value(json!({"region":"West"})).unwrap(),
            effective: serde_json::from_value(
                json!({"uid":uid,"value":"1","hidden":"before","region":"West"}),
            )
            .unwrap(),
        };
        let fields = vec!["uid".into(), "value".into()];
        let original = view_fingerprint(&binding, &fields, &row).unwrap();
        let mut changed = ObjectRow {
            source: row.source.clone(),
            effective: row.effective.clone(),
        };
        changed.effective.insert("hidden".into(), json!("after"));
        assert_eq!(
            original,
            view_fingerprint(&binding, &fields, &changed).unwrap()
        );
        changed.effective.insert("value".into(), json!("2"));
        assert_ne!(
            original,
            view_fingerprint(&binding, &fields, &changed).unwrap()
        );
        let identity = crate::authorization::Identity {
            environment: "env".into(),
            principal: "principal".into(),
            application: "app".into(),
            granted_operations: BTreeSet::from(["objects:read".into()]),
            application_scopes: BTreeSet::from(["objects:read".into()]),
            markings: BTreeSet::new(),
            disabled: false,
            delegated: None,
        };
        let projection = ObjectReader::compile(binding.definition.policies)
            .unwrap()
            .prepare(
                &identity,
                serde_json::from_value(json!({"region":"East"})).unwrap(),
                Some(vec![]),
                &fields,
                vec![],
            )
            .unwrap();
        changed.source.insert("region".into(), json!("East"));
        changed.effective.insert("region".into(), json!("East"));
        assert!(
            projection
                .project(Some(&row), Some(&changed))
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn request_shapes_are_strict_and_cursors_are_opaque() {
        assert!(Query::parse(&json!({"sort":["name"]})).is_err());
        assert!(Query::parse(&json!({"pageSize":0})).is_err());
        assert!(Query::parse(&json!({"projection":["name","name"]})).is_err());
        assert!(Query::parse(&json!({"cursor":"bad"})).is_err());
        assert!(Query::parse(&json!({"projection":[],"pageSize":1000})).is_ok());
        assert!(Query::parse(&json!({"filter":null})).is_err());
        let token = token().unwrap();
        assert_eq!(token.len(), 43);
        assert_ne!(token, super::token().unwrap());
        assert_eq!(
            decode("ns%2FObjectType%2Fitems").unwrap(),
            "ns/ObjectType/items"
        );
        assert!(decode("%FF").is_err());
        assert!(decode("%2").is_err());
    }
}
