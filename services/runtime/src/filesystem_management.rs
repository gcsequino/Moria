use super::{ENTRY, Entry, NativeFilesystem, ROOT, identifier, name, revision};
use crate::{
    authority::{Authority, VerifiedSession},
    authorization::authorize,
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use tokio_postgres::Client;
use uuid::Uuid;

const OPERATIONS: [&str; 10] = [
    "read",
    "list",
    "create",
    "write",
    "move",
    "delete",
    "export",
    "publish",
    "register",
    "administer",
];
fn operations() -> Vec<&'static str> {
    OPERATIONS.to_vec()
}
async fn audit(client: &Client, actor: &str, operation: &str, target: &str) -> Result<String> {
    let epoch:String=client.query_one("UPDATE moria.instance_state SET security_epoch=security_epoch+1 RETURNING security_epoch::text",&[]).await?.get(0);
    client.execute("INSERT INTO moria.instance_audit(actor,operation,target,epoch) VALUES($1,$2,$3,$4::text::bigint)",&[&actor,&operation,&target,&epoch]).await?;
    Ok(epoch)
}
async fn directory(
    client: &Client,
    parent: Uuid,
    name_value: &str,
    project: Option<&str>,
) -> Result<Entry> {
    name(name_value)?;
    if let Some(row) = client
        .query_opt(
            "SELECT id FROM moria.fs_entries WHERE parent_id=$1 AND name=$2 AND NOT deleted",
            &[&parent, &name_value],
        )
        .await?
    {
        let entry = NativeFilesystem::entry(client, row.get(0), false).await?;
        ensure!(
            entry.kind == "directory"
                && project.is_none_or(|project| entry.project_id.as_deref() == Some(project)),
            "FILESYSTEM_CONFLICT"
        );
        return Ok(entry);
    }
    let parent = NativeFilesystem::entry(client, parent, false).await?;
    ensure!(parent.kind == "directory", "FILESYSTEM_INVALID");
    let project = project.map(str::to_owned).or(parent.project_id.clone());
    let entry:Entry=serde_json::from_value(client.query_one(&format!("INSERT INTO moria.fs_entries(id,parent_id,name,kind,project_id,environment_id) VALUES($1,$2,$3,'directory',$4,$5) RETURNING {ENTRY}"),&[&Uuid::new_v4(),&parent.id,&name_value,&project,&parent.environment_id]).await?.get(0))?;
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
async fn grant(client: &Client, id: Uuid, grant: Value) -> Result<()> {
    let row = client
        .query_opt(
            "SELECT grants FROM moria.fs_policies WHERE entry_id=$1",
            &[&id],
        )
        .await?;
    let mut grants: Vec<Value> = row
        .map(|row| serde_json::from_value(row.get(0)))
        .transpose()?
        .unwrap_or_default();
    if grants.iter().any(|old| {
        old["subject"] == grant["subject"]
            && old["effect"] == grant["effect"]
            && old["inherit"] == grant["inherit"]
            && grant["operations"].as_array().unwrap().iter().all(|op| {
                old["operations"]
                    .as_array()
                    .is_some_and(|ops| ops.contains(op))
            })
    }) {
        return Ok(());
    }
    grants.push(grant);
    ensure!(grants.len() <= 256, "RESOURCE_LIMIT");
    client.execute("INSERT INTO moria.fs_policies(entry_id,revision,grants) VALUES($1,1,$2) ON CONFLICT(entry_id) DO UPDATE SET revision=fs_policies.revision+1,grants=EXCLUDED.grants",&[&id,&json!(grants)]).await?;
    Ok(())
}
async fn instance_actor(client: &Client, session: &VerifiedSession) -> Result<String> {
    client
        .query_one(
            "SELECT moria.native_serving_session($1)->>'instanceId'",
            &[&session.session_hash()],
        )
        .await?
        .get::<_, Option<String>>(0)
        .ok_or_else(|| anyhow::anyhow!("FORBIDDEN"))
}
impl NativeFilesystem {
    pub(super) async fn ensure_home_in(
        client: &Client,
        session: &VerifiedSession,
    ) -> Result<Value> {
        let mut state = Authority::current(client, session).await?;
        ensure!(state.private_purpose.is_none(), "FORBIDDEN");
        authorize(&state.identity, "filesystem", "read", &[])?;
        let row=client.query_opt("SELECT p.kind,b.instance_id,i.enabled,i.home_entry_id FROM moria.principals p LEFT JOIN moria.instance_principal_bindings b USING(environment,principal) LEFT JOIN moria.instance_principals i ON i.id=b.instance_id WHERE p.environment=$1 AND p.principal=$2",&[&session.environment(),&session.principal()]).await?.ok_or_else(||anyhow::anyhow!("FORBIDDEN"))?;
        let existing: Option<Uuid> = row.get(1);
        ensure!(
            existing.is_none() || row.get::<_, Option<bool>>(2) == Some(true),
            "FORBIDDEN"
        );
        let id = existing.unwrap_or_else(Uuid::new_v4);
        if existing.is_none() {
            client
                .execute(
                    "INSERT INTO moria.instance_principals(id,kind) VALUES($1,$2)",
                    &[&id, &row.get::<_, String>(0)],
                )
                .await?;
            client.execute("INSERT INTO moria.instance_principal_bindings(environment,principal,instance_id) VALUES($1,$2,$3)",&[&session.environment(),&session.principal(),&id]).await?;
            state.instance_epoch = audit(
                client,
                session.principal(),
                "provisionPrincipal",
                &id.to_string(),
            )
            .await?;
        }
        if let Some(home) = row.get::<_, Option<Uuid>>(3) {
            Authority::finish(client, session, &state).await?;
            return Ok(json!({"principalId":id,"home":Self::entry(client,home,false).await?}));
        }
        let mut roots = std::collections::BTreeMap::new();
        for name in ["system", "groups", "users"] {
            let entry = directory(client, Uuid::parse_str(ROOT)?, name, None).await?;
            client.execute("INSERT INTO moria.fs_roles(entry_id,kind,configuration) VALUES($1,'system',$2) ON CONFLICT DO NOTHING",&[&entry.id,&json!({"name":name})]).await?;
            roots.insert(name, entry);
        }
        let allowance = |operations: Vec<&str>, inherit: bool| json!({"subject":{"kind":"principal","id":id},"operations":operations,"inherit":inherit,"effect":"allow"});
        grant(client,Uuid::parse_str(ROOT)?,json!({"subject":{"kind":"administrators"},"operations":operations(),"inherit":true,"effect":"allow"})).await?;
        for target in [
            Uuid::parse_str(ROOT)?,
            roots["users"].id,
            roots["groups"].id,
        ] {
            grant(client, target, allowance(vec!["read", "list"], false)).await?;
        }
        let home = directory(client, roots["users"].id, &id.to_string(), None).await?;
        let agent = state.attributes.get("agent") == Some(&json!(true));
        let manager = if agent {
            let manager = state
                .attributes
                .get("managerId")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("FILESYSTEM_INVALID"))?;
            let manager = identifier(manager)?;
            ensure!(
                client
                    .query_opt(
                        "SELECT 1 FROM moria.instance_principals WHERE id=$1",
                        &[&manager]
                    )
                    .await?
                    .is_some(),
                "FILESYSTEM_INVALID"
            );
            Some(manager)
        } else {
            None
        };
        grant(
            client,
            home.id,
            allowance(
                if agent {
                    vec!["read", "list"]
                } else {
                    operations()
                },
                true,
            ),
        )
        .await?;
        if let Some(manager) = manager {
            grant(client,home.id,json!({"subject":{"kind":"principal","id":manager},"operations":operations(),"inherit":true,"effect":"allow"})).await?;
        }
        let configuration = directory(client, home.id, "configuration", None).await?;
        let memory = directory(client, home.id, "memory", None).await?;
        let runs = directory(client, home.id, "runs", None).await?;
        if agent {
            grant(
                client,
                memory.id,
                allowance(
                    vec![
                        "read", "list", "create", "write", "move", "delete", "export",
                    ],
                    true,
                ),
            )
            .await?;
        }
        let projects = if agent {
            None
        } else {
            Some(directory(client, home.id, "projects", None).await?.id)
        };
        let kind = if agent { "agent" } else { "user" };
        client.execute("INSERT INTO moria.fs_roles(entry_id,kind,configuration) VALUES($1,$2,$3)",&[&home.id,&kind,&json!({"principalId":id,"managerId":manager,"configuration":configuration.id,"memory":memory.id,"runs":runs.id,"projects":projects})]).await?;
        client
            .execute(
                "UPDATE moria.instance_principals SET home_entry_id=$2 WHERE id=$1",
                &[&id, &home.id],
            )
            .await?;
        state.instance_epoch = audit(
            client,
            &id.to_string(),
            "provisionHome",
            &home.id.to_string(),
        )
        .await?;
        Authority::finish(client, session, &state).await?;
        Ok(json!({"principalId":id,"home":Self::entry(client,home.id,false).await?}))
    }
    pub(super) async fn create_project_in(
        client: &Client,
        session: &VerifiedSession,
        input: &Value,
    ) -> Result<Value> {
        let map = input
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
        ensure!(
            map.len() == 3
                && ["parentId", "name", "expectedRevision"]
                    .iter()
                    .all(|key| map.contains_key(*key)),
            "INPUT_INVALID"
        );
        let parent = identifier(
            input["parentId"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?,
        )?;
        let name = input["name"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
        let expected = input["expectedRevision"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
        revision(expected)?;
        let mut state = Authority::current(client, session).await?;
        Self::access(client, session, parent, "register").await?;
        let actor = instance_actor(client, session).await?;
        let parent = Self::entry(client, parent, false).await?;
        ensure!(
            parent.revision == expected && parent.project_id.is_none(),
            "FILESYSTEM_CONFLICT"
        );
        let project = Uuid::new_v4();
        let entry = directory(client, parent.id, name, Some(&project.to_string())).await?;
        for name in ["source", "environments"] {
            let child = directory(client, entry.id, name, None).await?;
            client.execute("INSERT INTO moria.fs_roles(entry_id,kind,configuration) VALUES($1,'system',$2)",&[&child.id,&json!({"managed":"project","name":name})]).await?;
        }
        client.execute("INSERT INTO moria.fs_roles(entry_id,kind,project_id,configuration) VALUES($1,'project',$2,$3)",&[&entry.id,&project,&json!({"name":name})]).await?;
        state.instance_epoch =
            audit(client, &actor, "createProject", &entry.id.to_string()).await?;
        Authority::finish(client, session, &state).await?;
        Ok(json!({"projectId":project,"entry":Self::entry(client,entry.id,false).await?}))
    }
    pub(super) async fn create_group_in(
        client: &Client,
        session: &VerifiedSession,
        input: &Value,
    ) -> Result<Value> {
        let map = input
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
        ensure!(
            map.len() == 3
                && ["name", "groupKey", "expectedRevision"]
                    .iter()
                    .all(|key| map.contains_key(*key)),
            "INPUT_INVALID"
        );
        let name = input["name"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
        let key = input["groupKey"]
            .as_str()
            .filter(|key| !key.is_empty() && key.encode_utf16().count() <= 128)
            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
        let expected = input["expectedRevision"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
        revision(expected)?;
        let mut state = Authority::current(client, session).await?;
        authorize(&state.identity, "instance", "manage", &[])?;
        let actor = instance_actor(client, session).await?;
        let groups:Uuid=client.query_opt("SELECT id FROM moria.fs_entries WHERE parent_id=$1 AND name='groups' AND NOT deleted",&[&Uuid::parse_str(ROOT)?]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?.get(0);
        ensure!(
            Self::entry(client, groups, false).await?.revision == expected,
            "FILESYSTEM_CONFLICT"
        );
        let entry = directory(client, groups, name, None).await?;
        let id = Uuid::new_v4();
        client.execute("INSERT INTO moria.fs_groups(id,environment,group_key,home_entry_id) VALUES($1,$2,$3,$4)",&[&id,&session.environment(),&key,&entry.id]).await?;
        client
            .execute(
                "INSERT INTO moria.fs_roles(entry_id,kind,configuration) VALUES($1,'group',$2)",
                &[
                    &entry.id,
                    &json!({"id":id,"environment":session.environment(),"groupKey":key}),
                ],
            )
            .await?;
        grant(client,entry.id,json!({"subject":{"kind":"group","id":key,"environment":session.environment()},"operations":operations().into_iter().filter(|op|*op!="administer").collect::<Vec<_>>(),"inherit":true,"effect":"allow"})).await?;
        directory(client, entry.id, "projects", None).await?;
        state.instance_epoch = audit(client, &actor, "createGroup", &entry.id.to_string()).await?;
        Authority::finish(client, session, &state).await?;
        Ok(json!({"id":id,"entry":Self::entry(client,entry.id,false).await?}))
    }
    pub(super) async fn principal_enabled_in(
        client: &Client,
        session: &VerifiedSession,
        input: &Value,
    ) -> Result<Value> {
        let map = input
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
        ensure!(
            map.len() == 2 && map.contains_key("id") && map.contains_key("enabled"),
            "INPUT_INVALID"
        );
        let id = identifier(
            input["id"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?,
        )?;
        let enabled = input["enabled"]
            .as_bool()
            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
        let state = Authority::current(client, session).await?;
        authorize(&state.identity, "instance", "manage", &[])?;
        let actor = instance_actor(client, session).await?;
        Authority::finish(client, session, &state).await?;
        ensure!(
            client
                .execute(
                    "UPDATE moria.instance_principals SET enabled=$2 WHERE id=$1",
                    &[&id, &enabled]
                )
                .await?
                == 1,
            "NOT_FOUND"
        );
        client.execute("UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id IN(SELECT environment FROM moria.instance_principal_bindings WHERE instance_id=$1)",&[&id]).await?;
        audit(
            client,
            &actor,
            if enabled {
                "enablePrincipal"
            } else {
                "disablePrincipal"
            },
            &id.to_string(),
        )
        .await?;
        Ok(json!({"id":id,"enabled":enabled}))
    }
}
