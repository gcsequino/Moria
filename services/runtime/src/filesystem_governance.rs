use super::{ENTRY, Entry, NativeFilesystem, identifier, key, name, revision};
use crate::{
    authority::{Authority, VerifiedSession},
    canonical::fingerprint,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;
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
fn text(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.encode_utf16().count() <= 128
            && !value.chars().any(|c| c <= '\u{1f}' || c == '\u{7f}'),
        "FILESYSTEM_INVALID"
    );
    Ok(())
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Marking {
    environment: String,
    marking: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum Subject {
    Principal { id: String },
    Group { id: String, environment: String },
    Administrators,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Grant {
    subject: Subject,
    operations: Vec<String>,
    inherit: bool,
    effect: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Policy {
    markings: Vec<Marking>,
    grants: Vec<Grant>,
}
impl Policy {
    fn parse(input: &Value) -> Result<Self> {
        let mut policy: Self = serde_json::from_value(input.clone())
            .map_err(|_| anyhow::anyhow!("FILESYSTEM_INVALID"))?;
        ensure!(
            policy.markings.len() <= 64 && policy.grants.len() <= 256,
            "FILESYSTEM_INVALID"
        );
        for mark in &policy.markings {
            text(&mark.environment)?;
            text(&mark.marking)?;
        }
        let mut seen = BTreeSet::new();
        policy
            .markings
            .retain(|mark| seen.insert((mark.environment.clone(), mark.marking.clone())));
        for grant in &policy.grants {
            ensure!(
                !grant.operations.is_empty()
                    && grant.operations.len() <= OPERATIONS.len()
                    && grant
                        .operations
                        .iter()
                        .all(|op| OPERATIONS.contains(&op.as_str()))
                    && ["allow", "deny"].contains(&grant.effect.as_str()),
                "FILESYSTEM_INVALID"
            );
            match &grant.subject {
                Subject::Principal { id } => {
                    identifier(id)?;
                }
                Subject::Group { id, environment } => {
                    text(id)?;
                    text(environment)?;
                }
                Subject::Administrators => {}
            }
        }
        Ok(policy)
    }
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CopyRequest {
    pub id: Uuid,
    pub expected_revision: String,
    pub parent_id: Uuid,
    pub expected_parent_revision: String,
    pub name: String,
}
pub(super) fn copy_request(input: &Value) -> Result<CopyRequest> {
    for field in ["id", "parentId"] {
        identifier(
            input[field]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("FILESYSTEM_INVALID"))?,
        )?;
    }
    let request: CopyRequest =
        serde_json::from_value(input.clone()).map_err(|_| anyhow::anyhow!("FILESYSTEM_INVALID"))?;
    revision(&request.expected_revision)?;
    revision(&request.expected_parent_revision)?;
    name(&request.name)?;
    Ok(request)
}
pub(super) async fn audit(
    client: &Client,
    session: &VerifiedSession,
    operation: &str,
    target: &str,
) -> Result<String> {
    let actor: String = client
        .query_one(
            "SELECT moria.native_serving_session($1)->>'instanceId'",
            &[&session.session_hash()],
        )
        .await?
        .get::<_, Option<String>>(0)
        .ok_or_else(|| anyhow::anyhow!("FORBIDDEN"))?;
    let epoch:String=client.query_one("UPDATE moria.instance_state SET security_epoch=security_epoch+1 RETURNING security_epoch::text",&[]).await?.get(0);
    client.execute("INSERT INTO moria.instance_audit(actor,operation,target,epoch) VALUES($1,$2,$3,$4::text::bigint)",&[&actor,&operation,&target,&epoch]).await?;
    Ok(epoch)
}
impl NativeFilesystem {
    pub(super) async fn policy_write_context(
        client: &Client,
        session: &VerifiedSession,
        id: Uuid,
        input: &Value,
        consumed: &[String],
        context: Option<&crate::sandbox::SandboxContext>,
    ) -> Result<Value> {
        ensure!(
            input.as_object().is_some_and(|object| object.len() == 2
                && object.contains_key("expectedRevision")
                && object.contains_key("policy")),
            "FILESYSTEM_INVALID"
        );
        let expected = input["expectedRevision"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("FILESYSTEM_INVALID"))?;
        ensure!(
            expected
                .parse::<i64>()
                .is_ok_and(|value| (0..i64::MAX).contains(&value) && value.to_string() == expected),
            "FILESYSTEM_INVALID"
        );
        let policy = Policy::parse(&input["policy"])?;
        let mut snapshot = Authority::current(client, session).await?;
        let anchor = if let Some(context) = context {
            Some(context.mutation_anchor_in(client).await?)
        } else {
            None
        };
        let floor = Self::access(client, session, id, "administer").await?;
        ensure!(
            consumed.iter().all(|mark| floor.contains(mark)),
            "MARKING_FLOW"
        );
        Self::entry(client, id, false).await?;
        let old = client
            .query_opt(
                "SELECT revision::text,markings FROM moria.fs_policies WHERE entry_id=$1",
                &[&id],
            )
            .await?;
        ensure!(
            old.as_ref()
                .map_or("0".into(), |row| row.get::<_, String>(0))
                == expected,
            "FILESYSTEM_CONFLICT"
        );
        let new = policy
            .markings
            .iter()
            .map(|mark| (mark.environment.as_str(), mark.marking.as_str()))
            .collect::<BTreeSet<_>>();
        if let Some(old) = old {
            let labels: Vec<Marking> = serde_json::from_value(old.get(1))?;
            ensure!(
                labels
                    .iter()
                    .all(|mark| new.contains(&(mark.environment.as_str(), mark.marking.as_str()))),
                "UNSUPPORTED_CAPABILITY"
            );
        }
        let environments = policy
            .markings
            .iter()
            .map(|mark| mark.environment.clone())
            .chain(policy.grants.iter().filter_map(|grant| {
                if let Subject::Group { environment, .. } = &grant.subject {
                    Some(environment.clone())
                } else {
                    None
                }
            }))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        ensure!(
            client
                .query(
                    "SELECT id FROM moria.environments WHERE id=ANY($1::text[])",
                    &[&environments]
                )
                .await?
                .len()
                == environments.len(),
            "FILESYSTEM_INVALID"
        );
        let principals = policy
            .grants
            .iter()
            .filter_map(|grant| {
                if let Subject::Principal { id } = &grant.subject {
                    Some(identifier(id))
                } else {
                    None
                }
            })
            .collect::<Result<BTreeSet<_>>>()?
            .into_iter()
            .collect::<Vec<_>>();
        ensure!(
            client
                .query(
                    "SELECT id FROM moria.instance_principals WHERE id=ANY($1::uuid[])",
                    &[&principals]
                )
                .await?
                .len()
                == principals.len(),
            "FILESYSTEM_INVALID"
        );
        let revision:String=client.query_one("INSERT INTO moria.fs_policies(entry_id,revision,markings,grants) VALUES($1,1,$2,$3) ON CONFLICT(entry_id) DO UPDATE SET revision=moria.fs_policies.revision+1,markings=EXCLUDED.markings,grants=EXCLUDED.grants RETURNING revision::text",&[&id,&json!(policy.markings),&json!(policy.grants)]).await?.get(0);
        snapshot.instance_epoch =
            audit(client, session, "setFilesystemPolicy", &id.to_string()).await?;
        if let Some(context) = context {
            context
                .acknowledge_own_mutation_in(client, anchor.as_ref().unwrap())
                .await?;
        }
        Authority::finish(client, session, &snapshot).await?;
        Ok(json!({"revision":revision}))
    }
    pub(super) async fn copy_authority(
        client: &Client,
        session: &VerifiedSession,
        request: &CopyRequest,
        consumed: &[String],
    ) -> Result<(Entry, Entry, Vec<String>)> {
        let mut labels = Self::access(client, session, request.id, "read").await?;
        labels.extend(Self::access(client, session, request.id, "export").await?);
        let floor = Self::access(client, session, request.parent_id, "create").await?;
        ensure!(
            consumed.iter().all(|mark| floor.contains(mark)),
            "MARKING_FLOW"
        );
        let source = Self::entry(client, request.id, false).await?;
        let parent = Self::directory(client, request.parent_id).await?;
        ensure!(
            source.kind == "file"
                && source.content.is_some()
                && source.project_id == parent.project_id
                && source.environment_id == parent.environment_id,
            "FILESYSTEM_INVALID"
        );
        labels.sort();
        labels.dedup();
        Ok((source, parent, labels))
    }
    pub(super) async fn copy_context(
        client: &Client,
        session: &VerifiedSession,
        key_value: &str,
        input: &Value,
        consumed: &[String],
        context: Option<&crate::sandbox::SandboxContext>,
    ) -> Result<Value> {
        key(key_value)?;
        let request = copy_request(input)?;
        let mut snapshot = Authority::current(client, session).await?;
        let anchor = if let Some(context) = context {
            Some(context.mutation_anchor_in(client).await?)
        } else {
            None
        };
        let actor = json!({"environment":session.environment(),"principal":session.principal(),"application":session.application(),"delegation":[]});
        let scope = fingerprint(&actor)?;
        let digest = fingerprint(&json!({"copy":input}))?;
        let (source, parent, labels) =
            Self::copy_authority(client, session, &request, consumed).await?;
        if let Some(row)=client.query_opt("SELECT request_hash,receipt FROM moria.fs_mutations WHERE scope_hash=$1 AND request_key=$2",&[&scope,&key_value]).await? {
            ensure!(row.get::<_,String>(0)==digest,"IDEMPOTENCY_CONFLICT");let receipt:Value=row.get(1);for entry in receipt["entries"].as_array().ok_or_else(||anyhow::anyhow!("FILESYSTEM_INVALID"))? {Self::access(client,session,identifier(entry["id"].as_str().ok_or_else(||anyhow::anyhow!("FILESYSTEM_INVALID"))?)?,"read").await?;}Authority::finish(client,session,&snapshot).await?;return Ok(receipt);
        }
        Self::expect(&source, &request.expected_revision)?;
        Self::expect(&parent, &request.expected_parent_revision)?;
        Self::placement(client, parent.id, &request.name, None).await?;
        let mutation = Uuid::new_v4();
        let id = Uuid::new_v4();
        let mut touched = vec![];
        let row=client.query_one(&format!("INSERT INTO moria.fs_entries(id,parent_id,name,kind,project_id,environment_id,content) VALUES($1,$2,$3,'file',$4,$5,$6) RETURNING {ENTRY}"),&[&id,&parent.id,&request.name,&parent.project_id,&parent.environment_id,&source.content]).await?;
        Self::remember(
            client,
            serde_json::from_value(row.get(0))?,
            mutation,
            &mut touched,
        )
        .await?;
        Self::touch(client, parent.id, mutation, &mut touched).await?;
        client
            .execute(
                "INSERT INTO moria.fs_provenance(entry_id,source_id) VALUES($1,$2)",
                &[&id, &source.id],
            )
            .await?;
        let markings = json!(
            labels
                .iter()
                .map(|mark| json!({"environment":session.environment(),"marking":mark}))
                .collect::<Vec<_>>()
        );
        client
            .execute(
                "INSERT INTO moria.fs_policies(entry_id,retained_markings) VALUES($1,$2)",
                &[&id, &markings],
            )
            .await?;
        Self::access(client, session, id, "read").await?;
        snapshot.instance_epoch =
            audit(client, session, "copyFilesystemFile", &id.to_string()).await?;
        if let Some(context) = context {
            context
                .acknowledge_own_mutation_in(client, anchor.as_ref().unwrap())
                .await?;
        }
        let receipt = json!({"mutationId":mutation,"entries":touched});
        client.execute("INSERT INTO moria.fs_mutations(id,scope_hash,request_key,request_hash,actor,receipt) VALUES($1,$2,$3,$4,$5,$6)",&[&mutation,&scope,&key_value,&digest,&actor,&receipt]).await?;
        Authority::finish(client, session, &snapshot).await?;
        Ok(receipt)
    }
}
