use super::{NativeSources, fields_context, validation};
use crate::{
    authority::{Authority, VerifiedSession},
    canonical::fingerprint,
    filesystem::{ENTRY, Entry, NativeFilesystem},
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use tokio_postgres::Client;
use uuid::Uuid;

fn deletions(value: &Value) -> Result<Vec<String>> {
    let values: Vec<String> =
        serde_json::from_value(value.clone()).map_err(|_| anyhow::anyhow!("SOURCE_INVALID"))?;
    ensure!(
        values.len() <= 2000
            && values.iter().all(|value| value.len() <= 4096)
            && values.iter().collect::<BTreeSet<_>>().len() == values.len(),
        "SOURCE_INVALID"
    );
    Ok(values)
}
async fn actor(client: &Client, session: &VerifiedSession) -> Result<String> {
    let id = client
        .query_one(
            "SELECT moria.native_serving_session($1)->>'instanceId'",
            &[&session.session_hash()],
        )
        .await?
        .get::<_, Option<String>>(0)
        .ok_or_else(|| anyhow::anyhow!("FORBIDDEN"))?;
    fingerprint(&json!([[id, session.application()]]))
}
async fn origins(client: &Client, digest: &str) -> Result<BTreeMap<String, Uuid>> {
    let rows=client.query("WITH RECURSIVE releases AS(SELECT digest,snapshot,owned FROM moria.fs_source_releases WHERE digest=$1 UNION SELECT d.digest,d.snapshot,d.owned FROM releases r CROSS JOIN LATERAL jsonb_array_elements(r.snapshot->'descriptor'->'dependencies') dependency JOIN moria.fs_source_releases d ON d.digest=dependency->>'digest') SELECT (snapshot->'role'->>'projectId')::uuid,jsonb_array_elements_text(owned) FROM releases LIMIT 10001",&[&digest]).await?;
    ensure!(rows.len() <= 10000, "RESOURCE_LIMIT");
    let mut origins = BTreeMap::new();
    for row in rows {
        let project: Uuid = row.get(0);
        let reference: String = row.get(1);
        if let Some(old) = origins.insert(reference, project) {
            ensure!(old == project, "SOURCE_INVALID");
        }
    }
    Ok(origins)
}
async fn desired(
    client: &Client,
    session: &VerifiedSession,
    release: &Value,
    removed: &[String],
) -> Result<(String, Vec<Value>)> {
    origins(client, release["digest"].as_str().unwrap()).await?;
    let root = Uuid::parse_str(release["rootId"].as_str().unwrap())?;
    let active=client.query_opt("SELECT revision::text,resources FROM moria.fs_active_releases WHERE root_id=$1 AND environment=$2",&[&root,&session.environment()]).await?;
    let mut resources = Vec::<Value>::new();
    if let Some(active) = &active {
        resources = serde_json::from_value(active.get(1))?;
    }
    let owned: BTreeSet<String> = serde_json::from_value(release["owned"].clone())?;
    for resource in release["compiled"]["manifest"]["resources"]
        .as_array()
        .unwrap()
    {
        let reference = validation::reference(resource)?;
        if owned.contains(&reference) {
            if let Some(index) = resources
                .iter()
                .position(|old| validation::reference(old).ok().as_ref() == Some(&reference))
            {
                resources[index] = resource.clone();
            } else {
                resources.push(resource.clone());
            }
        }
    }
    for reference in removed {
        ensure!(
            !reference.contains("/Namespace/")
                && resources.iter().any(
                    |resource| validation::reference(resource).ok().as_ref() == Some(reference)
                ),
            "SOURCE_INVALID"
        );
        resources
            .retain(|resource| validation::reference(resource).ok().as_ref() != Some(reference));
    }
    let mut combined = resources.clone();
    combined.extend(
        release["compiled"]["manifest"]["resources"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|resource| {
                validation::reference(resource).is_ok_and(|reference| !owned.contains(&reference))
            })
            .cloned(),
    );
    let mut manifest = release["compiled"]["manifest"].clone();
    manifest["resources"] = json!(combined);
    let context = NativeSources::compiler_context_in(client, session).await?;
    validation::validate(&manifest, &fields_context(&context["principalAttributes"])?)
        .map_err(|_| anyhow::anyhow!("SOURCE_INVALID"))?;
    Ok((
        active
            .as_ref()
            .map(|row| row.get(0))
            .unwrap_or_else(|| "0".to_owned()),
        resources,
    ))
}
async fn placements(
    client: &Client,
    session: &VerifiedSession,
    root: Uuid,
    resources: &[Value],
    removed: &[String],
) -> Result<()> {
    let references = resources
        .iter()
        .map(validation::reference)
        .chain(removed.iter().cloned().map(Ok))
        .collect::<Result<Vec<_>>>()?;
    for row in client.query("SELECT entry_id,reference FROM moria.fs_resource_bindings WHERE root_id=$1 AND environment=$2 AND reference=ANY($3::text[])",&[&root,&session.environment(),&references]).await?{
        let id:Uuid=row.get(0);NativeFilesystem::access(client,session,id,"read").await?;NativeFilesystem::access(client,session,id,"publish").await?;if removed.contains(&row.get::<_,String>(1)){NativeFilesystem::access(client,session,id,"delete").await?;}
    }
    Ok(())
}
async fn create(
    client: &Client,
    session: &VerifiedSession,
    parent: Uuid,
    name: &str,
    environment: Option<&str>,
    resource: Option<Uuid>,
) -> Result<Entry> {
    if let Some(row) = client
        .query_opt(
            "SELECT id FROM moria.fs_entries WHERE parent_id=$1 AND name=$2 AND NOT deleted",
            &[&parent, &name],
        )
        .await?
    {
        let entry = NativeFilesystem::entry(client, row.get(0), false).await?;
        ensure!(
            entry.kind
                == if resource.is_some() {
                    "resource"
                } else {
                    "directory"
                }
                && environment
                    .is_none_or(|environment| entry.environment_id.as_deref() == Some(environment))
                && resource.is_none_or(|resource| entry.resource_id == Some(resource)),
            "FILESYSTEM_CONFLICT"
        );
        return Ok(entry);
    }
    NativeFilesystem::access(client, session, parent, "create").await?;
    let parent = NativeFilesystem::entry(client, parent, false).await?;
    ensure!(parent.kind == "directory", "SOURCE_INVALID");
    let environment = environment
        .map(str::to_owned)
        .or(parent.environment_id.clone());
    let kind = if resource.is_some() {
        "resource"
    } else {
        "directory"
    };
    let entry:Entry=serde_json::from_value(client.query_one(&format!("INSERT INTO moria.fs_entries(id,parent_id,name,kind,project_id,environment_id,resource_id) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING {ENTRY}"),&[&Uuid::new_v4(),&parent.id,&name,&kind,&parent.project_id,&environment,&resource]).await?.get(0))?;
    client
        .execute(
            "INSERT INTO moria.fs_entry_versions(entry_id,revision,snapshot) VALUES($1,1,$2)",
            &[&entry.id, &json!(entry)],
        )
        .await?;
    let touched: Entry = serde_json::from_value(
        client
            .query_one(
                &format!(
                    "UPDATE moria.fs_entries SET revision=revision+1 WHERE id=$1 RETURNING {ENTRY}"
                ),
                &[&parent.id],
            )
            .await?
            .get(0),
    )?;
    client.execute("INSERT INTO moria.fs_entry_versions(entry_id,revision,snapshot) VALUES($1,$2::text::bigint,$3)",&[&parent.id,&touched.revision,&json!(touched)]).await?;
    Ok(entry)
}
async fn materialize(
    client: &Client,
    session: &VerifiedSession,
    release: &Value,
    resources: &[Value],
) -> Result<()> {
    let root = Uuid::parse_str(release["rootId"].as_str().unwrap())?;
    let project = release["snapshot"]["role"]["projectId"].as_str().unwrap();
    let unit = release["snapshot"]["role"]["deploymentUnitId"]
        .as_str()
        .unwrap();
    let row=client.query_opt("WITH RECURSIVE ancestors AS(SELECT id,parent_id,project_id FROM moria.fs_entries WHERE id=$1 UNION ALL SELECT e.id,e.parent_id,e.project_id FROM moria.fs_entries e JOIN ancestors a ON e.id=a.parent_id) SELECT a.id FROM ancestors a LEFT JOIN moria.fs_entries p ON p.id=a.parent_id WHERE a.project_id=$2 AND p.project_id IS DISTINCT FROM a.project_id",&[&root,&project]).await?.ok_or_else(||anyhow::anyhow!("SOURCE_INVALID"))?;
    let environments = create(client, session, row.get(0), "environments", None, None).await?;
    let name = format!(
        "env-{}",
        &fingerprint(&json!(session.environment()))?[7..23]
    );
    let environment = create(
        client,
        session,
        environments.id,
        &name,
        Some(session.environment()),
        None,
    )
    .await?;
    let unit = create(client, session, environment.id, unit, None, None).await?;
    for entry in [&environments, &environment, &unit] {
        client.execute("INSERT INTO moria.fs_roles(entry_id,kind,configuration) VALUES($1,'system',$2) ON CONFLICT DO NOTHING",&[&entry.id,&json!({"managed":"runtime","environment":session.environment()})]).await?;
    }
    NativeFilesystem::access(client, session, unit.id, "publish").await?;
    for resource in resources {
        let reference = validation::reference(resource)?;
        let identity:Uuid=client.query_opt("SELECT resource_id FROM moria.fs_resource_identities WHERE root_id=$1 AND reference=$2",&[&root,&reference]).await?.ok_or_else(||anyhow::anyhow!("SOURCE_INVALID"))?.get(0);
        let existing=client.query_opt("SELECT entry_id,root_id FROM moria.fs_resource_bindings WHERE environment=$1 AND project_id=$2::text::uuid AND reference=$3",&[&session.environment(),&project,&reference]).await?;
        let id = if let Some(row) = existing {
            ensure!(row.get::<_, Uuid>(1) == root, "SOURCE_INVALID");
            let id: Uuid = row.get(0);
            let labels = NativeFilesystem::access(client, session, id, "read")
                .await?
                .into_iter()
                .map(|marking| json!({"environment":session.environment(),"marking":marking}))
                .collect::<Vec<_>>();
            client.execute("INSERT INTO moria.fs_policies(entry_id,retained_markings) VALUES($1,$2) ON CONFLICT(entry_id) DO UPDATE SET retained_markings=COALESCE((SELECT jsonb_agg(DISTINCT value) FROM jsonb_array_elements(fs_policies.retained_markings||EXCLUDED.retained_markings)),'[]')",&[&id,&json!(labels)]).await?;
            id
        } else {
            let directory = create(
                client,
                session,
                unit.id,
                resource["kind"].as_str().unwrap(),
                None,
                None,
            )
            .await?;
            create(
                client,
                session,
                directory.id,
                resource["metadata"]["name"].as_str().unwrap(),
                None,
                Some(identity),
            )
            .await?
            .id
        };
        client.execute("INSERT INTO moria.fs_resource_bindings(environment,reference,entry_id,root_id,digest,project_id) VALUES($1,$2,$3,$4,$5,$6::text::uuid) ON CONFLICT(environment,project_id,reference) DO UPDATE SET digest=EXCLUDED.digest,active=true",&[&session.environment(),&reference,&id,&root,&release["digest"].as_str().unwrap(),&project]).await?;
        client.execute("INSERT INTO moria.fs_resource_release_history VALUES($1,$2) ON CONFLICT DO NOTHING",&[&id,&release["digest"].as_str().unwrap()]).await?;
        let labels = resource["metadata"]["markings"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|marking| json!({"environment":session.environment(),"marking":marking}))
            .collect::<Vec<_>>();
        client.execute("INSERT INTO moria.fs_policies(entry_id,markings) VALUES($1,$2) ON CONFLICT(entry_id) DO UPDATE SET markings=COALESCE((SELECT jsonb_agg(DISTINCT value) FROM jsonb_array_elements(fs_policies.markings||EXCLUDED.markings)),'[]')",&[&id,&json!(labels)]).await?;
    }
    let references = resources
        .iter()
        .map(validation::reference)
        .collect::<Result<Vec<_>>>()?;
    client.execute("UPDATE moria.fs_resource_bindings SET active=false WHERE root_id=$1 AND environment=$2 AND NOT(reference=ANY($3::text[]))",&[&root,&session.environment(),&references]).await?;
    let origins = origins(client, release["digest"].as_str().unwrap()).await?;
    let references = release["compiled"]["manifest"]["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(validation::reference)
        .collect::<Result<BTreeSet<_>>>()?;
    for resource in resources {
        let reference = validation::reference(resource)?;
        let mut pending = vec![&resource["spec"]];
        let mut sources = BTreeSet::new();
        let mut nodes = 0;
        while let Some(value) = pending.pop() {
            nodes += 1;
            ensure!(nodes <= 100000, "RESOURCE_LIMIT");
            if let Some(value) = value.as_str() {
                if value != reference && references.contains(value) {
                    sources.insert(value.to_owned());
                }
            } else if let Some(object) = value.as_object() {
                pending.extend(object.values());
            } else if let Some(array) = value.as_array() {
                pending.extend(array);
            }
        }
        for source in sources {
            let source_project = origins
                .get(&source)
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
            client.execute("INSERT INTO moria.fs_resource_dependencies(environment,reference,source_reference,project_id,source_project_id) VALUES($1,$2,$3,$4::text::uuid,$5) ON CONFLICT DO NOTHING",&[&session.environment(),&reference,&source,&project,&source_project]).await?;
        }
        if resource["kind"] == "Transform" {
            for output in resource["spec"]["outputs"].as_array().unwrap() {
                let output = output.as_str().unwrap();
                let target_project = origins
                    .get(output)
                    .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?;
                let target:Uuid=client.query_opt("SELECT entry_id FROM moria.fs_resource_bindings WHERE environment=$1 AND project_id=$2 AND reference=$3 AND active",&[&session.environment(),&target_project,&output]).await?.ok_or_else(||anyhow::anyhow!("SOURCE_INVALID"))?.get(0);
                NativeFilesystem::access(client, session, target, "write").await?;
                client.execute("INSERT INTO moria.fs_resource_dependencies(environment,reference,source_reference,project_id,source_project_id) VALUES($1,$2,$3,$4,$5::text::uuid) ON CONFLICT DO NOTHING",&[&session.environment(),&output,&reference,&target_project,&project]).await?;
            }
        }
    }
    Ok(())
}
impl NativeSources {
    pub(crate) async fn plan_in(
        client: &Client,
        session: &VerifiedSession,
        root: Uuid,
        digest: &str,
        removed: &Value,
    ) -> Result<Value> {
        let removed = deletions(removed)?;
        let state = Authority::current(client, session).await?;
        let (release, _) = Self::release_in(client, session, digest).await?;
        ensure!(release["rootId"] == json!(root), "SOURCE_INVALID");
        NativeFilesystem::access(client, session, root, "publish").await?;
        let (revision, resources) = desired(client, session, &release, &removed).await?;
        placements(client, session, root, &resources, &removed).await?;
        let runtime = crate::source_runtime::plan_in(client, session, &release, &resources).await?;
        let id = Uuid::new_v4();
        let actor = actor(client, session).await?;
        let expires:String=client.query_one("INSERT INTO moria.fs_release_plans(id,root_id,digest,environment,actor_hash,expected_revision,environment_epoch,instance_epoch,deletions,runtime_plan) VALUES($1,$2,$3,$4,$5,$6::text::bigint,$7::text::bigint,$8::text::bigint,$9,$10) RETURNING to_char(expires_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')",&[&id,&root,&digest,&session.environment(),&actor,&revision,&state.security_epoch,&state.instance_epoch,&json!(removed),&runtime]).await?.get(0);
        Authority::finish(client, session, &state).await?;
        Ok(
            json!({"id":id,"rootId":root,"digest":digest,"environment":session.environment(),"expectedRevision":revision,"resources":resources.iter().map(validation::reference).collect::<Result<Vec<_>>>()?,"deletions":removed,"expiresAt":expires,"runtime":runtime}),
        )
    }
    pub(crate) async fn activate_in(
        client: &Client,
        session: &VerifiedSession,
        id: Uuid,
    ) -> Result<Value> {
        let mut state = Authority::current(client, session).await?;
        let row=client.query_opt("SELECT root_id,digest,environment,actor_hash,expected_revision::text,environment_epoch::text,instance_epoch::text,deletions,receipt,expires_at<=clock_timestamp(),runtime_plan FROM moria.fs_release_plans WHERE id=$1 FOR UPDATE",&[&id]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        ensure!(
            row.get::<_, String>(2) == session.environment()
                && row.get::<_, String>(3) == actor(client, session).await?,
            "NOT_FOUND"
        );
        let root: Uuid = row.get(0);
        let digest: String = row.get(1);
        let (release, _) = Self::release_in(client, session, &digest).await?;
        NativeFilesystem::access(client, session, root, "publish").await?;
        let removed = deletions(&row.get::<_, Value>(7))?;
        if let Some(receipt) = row.get::<_, Option<Value>>(8) {
            let references: Vec<String> = serde_json::from_value(receipt["resources"].clone())?;
            let resources=references.iter().map(|reference|json!({"kind":reference.split('/').nth(1).unwrap_or_default(),"metadata":{"namespace":reference.split('/').next().unwrap_or_default(),"name":reference.split('/').nth(2).unwrap_or_default()}})).collect::<Vec<_>>();
            placements(client, session, root, &resources, &removed).await?;
            Authority::finish(client, session, &state).await?;
            return Ok(receipt);
        }
        ensure!(!row.get::<_, bool>(9), "PLAN_EXPIRED");
        let (revision, resources) = desired(client, session, &release, &removed).await?;
        placements(client, session, root, &resources, &removed).await?;
        ensure!(revision == row.get::<_, String>(4), "FILESYSTEM_CONFLICT");
        ensure!(
            state.security_epoch == row.get::<_, String>(5)
                && state.instance_epoch == row.get::<_, String>(6),
            "AUTHORIZATION_CHANGED"
        );
        let revision:String=client.query_one("INSERT INTO moria.fs_active_releases(root_id,environment,revision,digest,resources) VALUES($1,$2,1,$3,$4) ON CONFLICT(root_id,environment) DO UPDATE SET revision=fs_active_releases.revision+1,digest=EXCLUDED.digest,resources=EXCLUDED.resources RETURNING revision::text",&[&root,&session.environment(),&digest,&json!(resources)]).await?.get(0);
        materialize(client, session, &release, &resources).await?;
        state.instance_epoch = crate::filesystem::governance::audit(
            client,
            session,
            "activateSource",
            &root.to_string(),
        )
        .await?;
        let current_runtime =
            crate::source_runtime::plan_in(client, session, &release, &resources).await?;
        let planned = row
            .get::<_, Option<Value>>(10)
            .unwrap_or_else(|| json!({"profileFingerprint":current_runtime["profileFingerprint"]}));
        if row.get::<_, Option<Value>>(10).is_none() {
            ensure!(
                current_runtime["attemptRuntime"] != true,
                "SOURCE_RUNTIME_CHANGED"
            );
        }
        let runtime =
            crate::source_runtime::compose_in(client, session, &release, &resources, &planned)
                .await?;
        state.security_epoch = Authority::current(client, session).await?.security_epoch;
        let receipt = json!({"rootId":root,"environment":session.environment(),"digest":digest,"revision":revision,"resources":resources.iter().map(validation::reference).collect::<Result<Vec<_>>>()?,"runtimeConfigured":runtime["configured"],"runtimeReady":runtime["ready"],"runtime":runtime});
        client
            .execute(
                "UPDATE moria.fs_release_plans SET receipt=$2 WHERE id=$1",
                &[&id, &receipt],
            )
            .await?;
        Authority::finish(client, session, &state).await?;
        Ok(receipt)
    }
}
