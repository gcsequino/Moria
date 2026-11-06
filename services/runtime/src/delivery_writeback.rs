use crate::{
    authority::{Authority, VerifiedSession},
    authorization::authorize,
    database::Database,
    filesystem::NativeSources,
    functions::Admission,
    pipelines, secrets,
};
use anyhow::{Result, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use tokio_postgres::Client;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Registration {
    environment: String,
    action: Uuid,
    connection: Uuid,
    connector_destination: Uuid,
    delivery_binding: Uuid,
    writer_role: String,
    effect_key: String,
}

pub(crate) async fn register(db: &Database, input: Registration) -> Result<Value> {
    ensure!(
        !input.environment.is_empty()
            && (1..=63).contains(&input.writer_role.len())
            && (1..=128).contains(&input.effect_key.len())
            && input
                .effect_key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
        "DELIVERY_WRITEBACK_INVALID"
    );
    let mut admission = Admission::begin(db, &input.environment, true, None).await?;
    let tx = admission.client.as_mut().unwrap().transaction().await?;
    tx.query_one("SELECT moria.runtime_recovery_operator()", &[])
        .await?;
    let (_, action_release, definition) =
        NativeSources::active_definition_in(tx.client(), &input.environment, input.action)
            .await?
            .ok_or_else(|| anyhow::anyhow!("DELIVERY_WRITEBACK_INVALID"))?;
    ensure!(definition["kind"] == "Action", "DELIVERY_WRITEBACK_INVALID");
    let action = tx.query_opt("SELECT revision,authority_generation FROM moria.native_serving_actions WHERE environment=$1 AND resource=$2 AND enabled FOR SHARE",
        &[&input.environment, &input.action.to_string()]).await?.ok_or_else(|| anyhow::anyhow!("DELIVERY_WRITEBACK_INVALID"))?;
    let revision: String = action.get(0);
    let generation: i64 = action.get(1);
    let profile = pipelines::writeback_connector_in(
        tx.client(),
        &input.environment,
        input.connection,
        input.connector_destination,
    )
    .await?;
    let endpoint = crate::delivery::DeliveryAuthority::endpoint(profile.endpoint.as_str())?;
    let secret_ids: Vec<_> = profile
        .requests
        .iter()
        .map(|request| request.secret)
        .collect();
    let binding = tx.query_opt("SELECT 1 FROM moria.runtime_execution_bindings WHERE id=$1 AND environment=$2 AND enabled AND spec->>'domain'='delivery' AND spec->>'version'='1' AND (resource::text||':write')=ANY(operations) FOR SHARE",
        &[&input.delivery_binding,&input.environment]).await?;
    ensure!(binding.is_some(), "DELIVERY_WRITEBACK_INVALID");
    let prior=tx.query_opt("SELECT w.destination FROM moria.runtime_delivery_writebacks w JOIN moria.runtime_delivery_destinations d ON d.id=w.destination WHERE w.environment=$1 AND w.action=$2 AND w.action_revision=$3 AND w.action_generation=$4 AND w.action_release=$5 AND w.connection=$6 AND w.connection_release=$7 AND w.implementation=$8 AND w.implementation_release=$9 AND w.connector_destination=$10 AND w.destination_fingerprint=$11 AND w.writer_role::text=$12 AND w.effect_key=$13 AND d.binding=$14 AND w.secret_ids=$15",
        &[&input.environment,&input.action,&revision,&generation,&action_release,&input.connection,&profile.release,&profile.implementation,&profile.implementation_release,&input.connector_destination,&profile.fingerprint,&input.writer_role,&input.effect_key,&input.delivery_binding,&secret_ids]).await?;
    let destination: Uuid = if let Some(row) = prior {
        row.get(0)
    } else {
        let destination: Uuid = tx.query_one("INSERT INTO moria.runtime_delivery_destinations(binding,endpoint,idempotent,allowed_markings) VALUES($1,$2,false,$3) RETURNING id",
            &[&input.delivery_binding,&endpoint.as_str(),&profile.allowed_markings]).await?.get(0);
        tx.execute("INSERT INTO moria.runtime_delivery_writebacks(destination,environment,action,action_revision,action_generation,action_release,connection,connection_release,implementation,implementation_release,connector_destination,destination_fingerprint,writer_role,effect_key,secret_ids) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::text::name,$14,$15)",
            &[&destination,&input.environment,&input.action,&revision,&generation,&action_release,&input.connection,&profile.release,&profile.implementation,&profile.implementation_release,&input.connector_destination,&profile.fingerprint,&input.writer_role,&input.effect_key,&secret_ids]).await?;
        destination
    };
    tx.query_one(
        "SELECT moria.runtime_destination_check($1,$2)",
        &[&destination, &profile.markings],
    )
    .await?;
    tx.commit().await?;
    admission.release().await?;
    Ok(json!({"profile":"http-outbox-v1","destination":destination}))
}

async fn accepted_in(
    client: &Client,
    session: &VerifiedSession,
    action: &str,
    revision: &str,
    markings: &[String],
) -> Result<Option<Value>> {
    let Ok(action_id) = Uuid::parse_str(action) else {
        return Ok(None);
    };
    let Some(row) = client.query_opt("SELECT w.*,b.resource AS destination_resource,(session_user=w.writer_role) AS writer FROM moria.runtime_delivery_writebacks w JOIN moria.runtime_delivery_destinations d ON d.id=w.destination JOIN moria.runtime_execution_bindings b ON b.id=d.binding WHERE w.environment=$1 AND w.action=$2 AND w.action_revision=$3",
        &[&session.environment(),&action_id,&revision]).await? else { return Ok(None) };
    ensure!(
        row.get::<_, bool>("writer"),
        "DELIVERY_WRITEBACK_ORIGIN_INVALID"
    );
    let connection: Uuid = row.get("connection");
    let profile = pipelines::writeback_connector_in(
        client,
        session.environment(),
        connection,
        row.get("connector_destination"),
    )
    .await?;
    let (action_release, connection_release, implementation_release, fingerprint): (
        String,
        String,
        String,
        String,
    ) = (
        row.get("action_release"),
        row.get("connection_release"),
        row.get("implementation_release"),
        row.get("destination_fingerprint"),
    );
    ensure!(
        profile.release == connection_release
            && profile.implementation == row.get::<_, Uuid>("implementation")
            && profile.implementation_release == implementation_release
            && profile.fingerprint == fingerprint,
        "AUTHORIZATION_CHANGED"
    );
    let mut labels: BTreeSet<String> = markings.iter().cloned().collect();
    labels.extend(profile.markings);
    labels.extend(
        NativeSources::declaration_labels_batch_in(
            client,
            session,
            &[
                (action_id, action_release),
                (connection, connection_release),
                (profile.implementation, implementation_release),
            ],
        )
        .await?,
    );
    let mut approvals = Vec::with_capacity(profile.requests.len());
    for request in profile.requests {
        let grant = secrets::grant_in(client, session, &request).await?;
        labels.extend(grant.markings().iter().cloned());
        let approval = grant.snapshot();
        approvals.push(json!({"id":approval.id,"generation":approval.generation,"incarnation":approval.incarnation,"secret":request.secret,"materialRevision":grant.material_revision()}));
    }
    let labels: Vec<_> = labels.into_iter().collect();
    ensure!(
        labels
            .iter()
            .all(|label| profile.allowed_markings.contains(label)),
        "DELIVERY_FORBIDDEN"
    );
    let snapshot = Authority::current(client, session).await?;
    ensure!(
        snapshot.private_purpose.is_none(),
        "DELIVERY_WRITEBACK_ORIGIN_INVALID"
    );
    let destination: Uuid = row.get("destination_resource");
    authorize(
        &snapshot.identity,
        &connection.to_string(),
        "writeback",
        &labels,
    )?;
    authorize(
        &snapshot.identity,
        &destination.to_string(),
        "write",
        &labels,
    )?;
    Authority::finish(client, session, &snapshot).await?;
    Ok(Some(
        json!({"sessionHash":session.session_hash(),"securityEpoch":snapshot.security_epoch,"instanceEpoch":snapshot.instance_epoch,"approvals":approvals,"markings":labels}),
    ))
}

pub(crate) async fn record_receipt_in(
    client: &Client,
    session: &VerifiedSession,
    action: &str,
    key: &str,
    input_hash: &str,
    revision: &str,
    result: &Value,
    markings: &[String],
) -> Result<()> {
    let writeback = accepted_in(client, session, action, revision, markings).await?;
    client.execute("INSERT INTO moria.action_keys(environment,application,principal,action,key,input_hash,action_revision,result,expires_at,writeback) VALUES($1,$2,$3,$4,$5,$6,$7,$8,clock_timestamp()+interval '30 days',$9)",
        &[&session.environment(),&session.application(),&session.principal(),&action,&key,&input_hash,&revision,&result,&writeback]).await?;
    Ok(())
}
