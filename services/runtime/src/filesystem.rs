use crate::{
    authority::{Authority, VerifiedSession},
    canonical::fingerprint,
    database::Database,
    serving::Admission,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use tokio_postgres::Client;
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

#[path = "filesystem_content.rs"]
mod content;
#[path = "filesystem_governance.rs"]
mod governance;
#[path = "filesystem_management.rs"]
mod management;
#[path = "filesystem_source.rs"]
mod source;
pub use content::{
    ContentDeclaration, FileContent, FileRead, FileUpload, FileWrite, UploadStart,
    configure_storage,
};
pub(crate) use source::validate_configuration;
pub use source::{
    CompiledManifest, CompiledSourceArtifact, CompilerArtifactRef, CompilerContext, NativeSources,
    SourceCheckout, SourceDependency, SourceDescriptor, SourceDirectory, SourceFile, SourceRole,
};

const ROOT: &str = "00000000-0000-4000-8000-000000000001";
const ENTRY: &str = "jsonb_build_object('id',id,'parentId',parent_id,'name',name,'kind',kind,'revision',revision::text,'projectId',project_id,'environmentId',environment_id,'resourceId',resource_id,'content',content,'deleted',deleted)";
#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Entry {
    pub id: Uuid,
    pub parent_id: Option<Uuid>,
    pub name: String,
    pub kind: String,
    pub revision: String,
    pub project_id: Option<String>,
    pub environment_id: Option<String>,
    pub resource_id: Option<Uuid>,
    pub content: Option<Value>,
    pub deleted: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn namespace_boundary_rejects_noncanonical_names_and_untrusted_registration() {
        assert!(name("é").is_ok());
        assert!(name("e\u{301}").is_err());
        assert!(name("..").is_err());
        assert!(name("a/b").is_err());
        assert!(name(&"x".repeat(129)).is_err());
        let parent = Uuid::new_v4();
        let valid = json!([{"op":"create","parentId":parent,"expectedParentRevision":"1","name":"drafts","kind":"directory"}]);
        assert!(changes(&valid).is_ok());
        let mut scoped = valid.clone();
        scoped[0]["scope"] = Value::Null;
        assert!(changes(&scoped).is_err());
        let mut content = valid.clone();
        content[0]["kind"] = "file".into();
        assert!(changes(&content).is_err());
        let mut noncanonical = valid.clone();
        noncanonical[0]["parentId"] = parent.to_string().to_ascii_uppercase().into();
        assert!(changes(&noncanonical).is_err());
        assert!(revision("0").is_err());
        assert!(revision("01").is_err());
        assert!(revision("9223372036854775808").is_err());
    }
}
#[derive(Clone, Deserialize)]
#[serde(
    tag = "op",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum Change {
    Create {
        parent_id: Uuid,
        expected_parent_revision: String,
        name: String,
        kind: String,
        scope: Option<Value>,
        resource_id: Option<Uuid>,
        content: Option<Value>,
    },
    Move {
        id: Uuid,
        expected_revision: String,
        parent_id: Uuid,
        expected_parent_revision: String,
        expected_source_parent_revision: String,
        name: String,
    },
    Delete {
        id: Uuid,
        expected_revision: String,
        expected_parent_revision: String,
    },
}
fn name(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value != "."
            && value != ".."
            && !value
                .chars()
                .any(|ch| ch <= '\u{1f}' || ch == '\u{7f}' || ch == '/' || ch == '\\')
            && value.nfc().eq(value.chars()),
        "FILESYSTEM_INVALID"
    );
    Ok(())
}
fn revision(value: &str) -> Result<()> {
    ensure!(
        !value.starts_with('0')
            && value.bytes().all(|byte| byte.is_ascii_digit())
            && value.parse::<i64>().is_ok_and(|value| value > 0),
        "FILESYSTEM_INVALID"
    );
    Ok(())
}
fn key(value: &str) -> Result<()> {
    ensure!(
        (1..=128).contains(&value.len())
            && value.as_bytes()[0].is_ascii_alphanumeric()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_.:/-".contains(&byte)),
        "FILESYSTEM_INVALID"
    );
    Ok(())
}
fn bounded(value: Value) -> Result<Value> {
    ensure!(
        serde_json::to_vec(&value)?.len() <= 1024 * 1024,
        "RESPONSE_TOO_LARGE"
    );
    Ok(value)
}
pub(crate) fn mutation_error(error: anyhow::Error) -> anyhow::Error {
    if error
        .chain()
        .filter_map(|cause| cause.downcast_ref::<tokio_postgres::Error>())
        .any(|error| {
            error
                .as_db_error()
                .is_some_and(|error| error.code().code() == "23505")
        })
    {
        anyhow::anyhow!("FILESYSTEM_CONFLICT")
    } else {
        error
    }
}
fn changes(input: &Value) -> Result<Vec<Change>> {
    let values = input
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("FILESYSTEM_INVALID"))?;
    ensure!((1..=100).contains(&values.len()), "FILESYSTEM_INVALID");
    for value in values {
        ensure!(
            value.get("op").and_then(Value::as_str) != Some("write"),
            "UNSUPPORTED_CAPABILITY"
        );
        if value.get("op").and_then(Value::as_str) == Some("create") {
            ensure!(
                ["scope", "resourceId", "content"]
                    .iter()
                    .all(|key| value.get(key).is_none()),
                "UNSUPPORTED_CAPABILITY"
            );
        }
        for field in ["id", "parentId", "resourceId"] {
            if let Some(value) = value.get(field) {
                let value = value
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("FILESYSTEM_INVALID"))?;
                ensure!(
                    Uuid::parse_str(value).is_ok_and(|uid| uid.to_string() == value),
                    "FILESYSTEM_INVALID"
                );
            }
        }
    }
    let result: Vec<Change> =
        serde_json::from_value(input.clone()).map_err(|_| anyhow::anyhow!("FILESYSTEM_INVALID"))?;
    for change in &result {
        match change {
            Change::Create {
                expected_parent_revision,
                name: entry_name,
                kind,
                scope,
                resource_id,
                content,
                ..
            } => {
                revision(expected_parent_revision)?;
                name(entry_name)?;
                ensure!(
                    kind == "directory"
                        && scope.is_none()
                        && resource_id.is_none()
                        && content.is_none(),
                    "UNSUPPORTED_CAPABILITY"
                );
            }
            Change::Move {
                expected_revision,
                expected_parent_revision,
                expected_source_parent_revision,
                name: entry_name,
                ..
            } => {
                revision(expected_revision)?;
                revision(expected_parent_revision)?;
                revision(expected_source_parent_revision)?;
                name(entry_name)?;
            }
            Change::Delete {
                expected_revision,
                expected_parent_revision,
                ..
            } => {
                revision(expected_revision)?;
                revision(expected_parent_revision)?;
            }
        }
    }
    Ok(result)
}
#[derive(Clone)]
pub struct NativeFilesystem {
    pub db: Database,
}
pub(crate) struct FilesystemRequest<'a> {
    pub method: &'a str,
    pub parts: &'a [String],
    pub query: &'a [(String, String)],
    pub input: &'a Value,
    pub key: Option<&'a str>,
    pub consumed: &'a [String],
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FilesystemEvidence {
    method: String,
    parts: Vec<String>,
    query: Vec<(String, String)>,
    input: Value,
    position: Option<Value>,
    hash: String,
    #[serde(default)]
    failure: Option<String>,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    consumed: Vec<String>,
}
impl FilesystemRequest<'_> {
    pub fn writes(&self) -> bool {
        if self.parts == ["releases", "read"] {
            return false;
        }
        if self.parts == ["home"] {
            return true;
        }
        if self.parts.len() == 3 && self.parts[0] == "sources" && self.parts[2] == "checkout" {
            return false;
        }
        self.method == "POST"
            && !self
                .parts
                .first()
                .is_some_and(|part| ["resolve", "resources"].contains(&part.as_str()))
    }
}
fn query_argument<'a>(
    query: &'a [(String, String)],
    name: Option<&str>,
) -> Result<Option<&'a str>> {
    ensure!(
        query.len() <= usize::from(name.is_some())
            && query.iter().all(|(key, _)| Some(key.as_str()) == name),
        "INPUT_INVALID"
    );
    Ok(query.first().map(|(_, value)| value.as_str()))
}
fn identifier(value: &str) -> Result<Uuid> {
    let id = Uuid::parse_str(value).map_err(|_| anyhow::anyhow!("FILESYSTEM_INVALID"))?;
    ensure!(id.to_string() == value, "FILESYSTEM_INVALID");
    Ok(id)
}
impl NativeFilesystem {
    pub(crate) fn request_entries(request: &FilesystemRequest<'_>) -> Result<Vec<Uuid>> {
        if !request.writes() {
            return Ok(vec![]);
        }
        if request.parts == ["copy"] {
            let copy = governance::copy_request(request.input)?;
            return Ok(vec![copy.id, copy.parent_id]);
        }
        if request.parts.len() == 3 && request.parts[0] == "entries" && request.parts[2] == "policy"
        {
            return Ok(vec![identifier(&request.parts[1])?]);
        }
        ensure!(request.parts == ["changes"], "UNSUPPORTED_CAPABILITY");
        let mut ids = BTreeSet::new();
        for change in changes(&request.input["changes"])? {
            match change {
                Change::Create { parent_id, .. } => {
                    ids.insert(parent_id);
                }
                Change::Move { id, parent_id, .. } => {
                    ids.extend([id, parent_id]);
                }
                Change::Delete { id, .. } => {
                    ids.insert(id);
                }
            }
        }
        Ok(ids.into_iter().collect())
    }
    pub(crate) async fn request_labels(
        client: &Client,
        session: &VerifiedSession,
        request: &FilesystemRequest<'_>,
    ) -> Result<Vec<String>> {
        if request.parts.first().is_some_and(|part| part == "entries") && request.parts.len() >= 2 {
            let operation = if request.parts.get(2).is_some_and(|part| part == "children") {
                "list"
            } else if request.parts.get(2).is_some_and(|part| part == "policy") {
                "administer"
            } else {
                "read"
            };
            return Self::access(client, session, identifier(&request.parts[1])?, operation).await;
        }
        let snapshot = Authority::current(client, session).await?;
        crate::authorization::authorize(&snapshot.identity, "filesystem", "read", &[])?;
        Ok(vec![])
    }
    pub(crate) fn failed_evidence(
        request: &FilesystemRequest<'_>,
        code: &str,
    ) -> FilesystemEvidence {
        FilesystemEvidence {
            method: request.method.into(),
            parts: request.parts.to_vec(),
            query: request.query.to_vec(),
            input: request.input.clone(),
            position: None,
            hash: String::new(),
            failure: Some(code.into()),
            key: request.key.map(String::from),
            consumed: request.consumed.to_vec(),
        }
    }
    fn comparable(mut value: Value) -> Value {
        if let Some(object) = value.as_object_mut() {
            let more = object.remove("nextCursor").is_some();
            if object.contains_key("entries") {
                object.insert("hasMore".into(), more.into());
            }
        }
        value
    }
    pub(crate) async fn evidence(
        client: &Client,
        session: &VerifiedSession,
        request: &FilesystemRequest<'_>,
        value: &Value,
    ) -> Result<(FilesystemEvidence, Vec<String>)> {
        ensure!(!request.writes(), "FUNCTION_EFFECT");
        let mut labels = BTreeSet::new();
        if request.parts.len() == 3
            && request.parts[0] == "sources"
            && request.parts[2] == "checkout"
        {
            labels.extend(
                Self::access(client, session, identifier(&request.parts[1])?, "export").await?,
            );
            for file in value["files"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?
            {
                labels.extend(
                    Self::access(
                        client,
                        session,
                        identifier(
                            file["entry"]["id"]
                                .as_str()
                                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
                        )?,
                        "read",
                    )
                    .await?,
                );
            }
            for directory in value["directories"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?
            {
                labels.extend(
                    Self::access(
                        client,
                        session,
                        identifier(
                            directory["id"]
                                .as_str()
                                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
                        )?,
                        "read",
                    )
                    .await?,
                );
            }
        }
        if request.parts.len() == 2 && request.parts[0] == "releases" {
            labels.extend(
                NativeSources::release_in(
                    client,
                    session,
                    if request.parts[1] == "read" {
                        request.input["digest"]
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?
                    } else {
                        &request.parts[1]
                    },
                )
                .await?
                .1,
            );
        }
        if request.parts.first().is_some_and(|part| part == "entries") && request.parts.len() >= 2 {
            let id = identifier(&request.parts[1])?;
            labels.extend(
                Self::access(
                    client,
                    session,
                    id,
                    if request.parts.get(2).is_some_and(|part| part == "children") {
                        "list"
                    } else if request.parts.get(2).is_some_and(|part| part == "policy") {
                        "administer"
                    } else {
                        "read"
                    },
                )
                .await?,
            );
        }
        if let Some(id) = value
            .get("id")
            .or_else(|| value.get("entryId"))
            .and_then(Value::as_str)
        {
            labels.extend(Self::access(client, session, identifier(id)?, "read").await?);
        }
        if let Some(entries) = value.get("entries").and_then(Value::as_array) {
            for entry in entries {
                if let Some(id) = entry.get("id").and_then(Value::as_str) {
                    labels.extend(Self::access(client, session, identifier(id)?, "read").await?);
                }
            }
        }
        let position = if request.parts.get(2).is_some_and(|part| part == "children") {
            if let Some((_, token)) = request.query.iter().find(|(key, _)| key == "cursor") {
                let row = client
                    .query_opt(
                        "SELECT position FROM moria.read_cursors WHERE token_hash=$1",
                        &[&crate::serving::hash(token)],
                    )
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("CURSOR_INVALID"))?;
                Some(row.get::<_, Value>(0))
            } else {
                None
            }
        } else {
            None
        };
        let evidence = FilesystemEvidence {
            method: request.method.into(),
            parts: request.parts.to_vec(),
            query: request.query.to_vec(),
            input: request.input.clone(),
            position,
            hash: fingerprint(&Self::comparable(value.clone()))?,
            failure: None,
            key: None,
            consumed: vec![],
        };
        Ok((evidence, labels.into_iter().collect()))
    }
    pub(crate) async fn verify_evidence(
        client: &Client,
        session: &VerifiedSession,
        evidence: &FilesystemEvidence,
        compare: bool,
    ) -> Result<Vec<String>> {
        let request = FilesystemRequest {
            method: &evidence.method,
            parts: &evidence.parts,
            query: &evidence.query,
            input: &evidence.input,
            key: evidence.key.as_deref(),
            consumed: &evidence.consumed,
        };
        if let Some(code) = &evidence.failure {
            if request.writes() {
                if request.parts == ["copy"] {
                    Self::copy_authority(
                        client,
                        session,
                        &governance::copy_request(request.input)?,
                        request.consumed,
                    )
                    .await?;
                    return Ok(vec![]);
                }
                if request.parts.len() == 3
                    && request.parts[0] == "entries"
                    && request.parts[2] == "policy"
                {
                    Self::access(
                        client,
                        session,
                        identifier(&request.parts[1])?,
                        "administer",
                    )
                    .await?;
                    return Ok(vec![]);
                }
                // Mutation failures also retain exact entry/parent revision witnesses captured
                // before dispatch. Recheck their write grants without executing a mutation
                // under the retained-result reader's shared admission.
                ensure!(
                    request.parts == ["changes"] && !Self::request_entries(&request)?.is_empty(),
                    "FUNCTION_INPUT_CHANGED"
                );
                for change in changes(&request.input["changes"])? {
                    Self::change_guard(client, session, &change, false, request.consumed).await?;
                }
                return Ok(vec![]);
            }
            client
                .batch_execute("SAVEPOINT native_filesystem_observation")
                .await?;
            let observed = Self::route_in(client, session, &request).await;
            client.batch_execute("ROLLBACK TO SAVEPOINT native_filesystem_observation; RELEASE SAVEPOINT native_filesystem_observation").await?;
            ensure!(
                observed
                    .as_ref()
                    .err()
                    .is_some_and(|error| crate::serving::error_code(error) == code),
                "FUNCTION_INPUT_CHANGED"
            );
            return Ok(vec![]);
        }
        ensure!(!request.writes(), "FUNCTION_EFFECT");
        let value = if evidence.parts.get(2).is_some_and(|part| part == "children") {
            let id = identifier(&evidence.parts[1])?;
            Self::access(client, session, id, "list").await?;
            Self::directory(client, id).await?;
            let name = evidence
                .position
                .as_ref()
                .and_then(|value| value["name"].as_str());
            let after = evidence
                .position
                .as_ref()
                .map(|value| {
                    value["id"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("CURSOR_INVALID"))
                        .and_then(identifier)
                })
                .transpose()?;
            let rows=client.query(&format!("SELECT {ENTRY} FROM moria.fs_entries WHERE parent_id=$1 AND NOT deleted AND ($2::text IS NULL OR (name,id)>($2::text COLLATE \"C\",$3::uuid)) AND moria.native_filesystem_visible($4,id) ORDER BY name COLLATE \"C\",id LIMIT 101"),&[&id,&name,&after,&session.session_hash()]).await?;
            let mut value = json!({"entries":rows.iter().take(100).map(|row|row.get::<_,Value>(0)).collect::<Vec<_>>()});
            if rows.len() > 100 {
                value["nextCursor"] = true.into();
            }
            value
        } else {
            Self::route_in(client, session, &request).await?
        };
        if compare {
            ensure!(
                fingerprint(&Self::comparable(value.clone()))? == evidence.hash,
                "FUNCTION_INPUT_CHANGED"
            );
        }
        let mut no_cursor = request;
        let query = evidence
            .query
            .iter()
            .filter(|(key, _)| key != "cursor")
            .cloned()
            .collect::<Vec<_>>();
        no_cursor.query = &query;
        Ok(Self::evidence(client, session, &no_cursor, &value).await?.1)
    }
    pub(crate) async fn refresh_evidence(
        client: &Client,
        session: &VerifiedSession,
        evidence: &mut FilesystemEvidence,
    ) -> Result<Vec<String>> {
        if evidence.failure.is_some() {
            return Self::verify_evidence(client, session, evidence, true).await;
        }
        if evidence.method == "GET" && evidence.parts.len() == 2 && evidence.parts[0] == "entries" {
            let entry = Self::entry(client, identifier(&evidence.parts[1])?, true).await?;
            if entry.deleted {
                evidence.parts.push("version".into());
                evidence.query = vec![("revision".into(), entry.revision)];
            }
        }
        let labels = Self::verify_evidence(client, session, evidence, false).await?;
        ensure!(evidence.position.is_none(), "FUNCTION_INPUT_CHANGED");
        let query = evidence
            .query
            .iter()
            .filter(|(key, _)| key != "cursor")
            .cloned()
            .collect::<Vec<_>>();
        let request = FilesystemRequest {
            method: &evidence.method,
            parts: &evidence.parts,
            query: &query,
            input: &evidence.input,
            key: None,
            consumed: &[],
        };
        let value = Self::route_in(client, session, &request).await?;
        evidence.hash = fingerprint(&Self::comparable(value))?;
        Ok(labels)
    }
    pub(crate) async fn route_in(
        client: &Client,
        session: &VerifiedSession,
        request: &FilesystemRequest<'_>,
    ) -> Result<Value> {
        Self::route_in_context(client, session, request, None).await
    }
    pub(crate) async fn route_in_context(
        client: &Client,
        session: &VerifiedSession,
        request: &FilesystemRequest<'_>,
        context: Option<&crate::sandbox::SandboxContext>,
    ) -> Result<Value> {
        let parts = request.parts;
        if parts == ["home"] && request.method == "GET" {
            query_argument(request.query, None)?;
            ensure!(context.is_none(), "FUNCTION_EFFECT");
            return Self::ensure_home_in(client, session).await;
        }
        if request.method == "POST"
            && parts.len() == 1
            && ["projects", "groups", "principals"].contains(&parts[0].as_str())
        {
            query_argument(request.query, None)?;
            ensure!(context.is_none(), "FUNCTION_EFFECT");
            return match parts[0].as_str() {
                "projects" => Self::create_project_in(client, session, request.input).await,
                "groups" => Self::create_group_in(client, session, request.input).await,
                _ => Self::principal_enabled_in(client, session, request.input).await,
            };
        }
        if parts == ["compiler-context"] {
            query_argument(request.query, None)?;
            return match request.method {
                "GET" => NativeSources::compiler_context_in(client, session).await,
                "POST" => {
                    NativeSources::register_compiler_context_in(client, session, request.input)
                        .await
                }
                _ => Err(anyhow::anyhow!("NOT_FOUND")),
            };
        }
        if request.method == "POST"
            && parts.len() == 3
            && parts[0] == "sources"
            && parts[2] == "checkout"
        {
            query_argument(request.query, None)?;
            ensure!(
                request
                    .input
                    .as_object()
                    .is_some_and(|value| value.is_empty()),
                "INPUT_INVALID"
            );
            let value =
                json!(NativeSources::capture(client, session, identifier(&parts[1])?).await?);
            ensure!(
                serde_json::to_vec(&value)?.len() <= 8 * 1024 * 1024,
                "RESPONSE_TOO_LARGE"
            );
            return Ok(value);
        }
        if request.method == "GET" && parts.len() == 2 && parts[0] == "releases" {
            query_argument(request.query, None)?;
            let value = NativeSources::release_in(client, session, &parts[1])
                .await?
                .0;
            ensure!(
                serde_json::to_vec(&value)?.len() <= 8 * 1024 * 1024,
                "RESPONSE_TOO_LARGE"
            );
            return Ok(value);
        }
        if request.method == "POST" && parts == ["releases", "read"] {
            query_argument(request.query, None)?;
            ensure!(
                request
                    .input
                    .as_object()
                    .is_some_and(|input| input.len() == 1 && input.contains_key("digest")),
                "INPUT_INVALID"
            );
            let value = NativeSources::release_in(
                client,
                session,
                request.input["digest"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?,
            )
            .await?
            .0;
            ensure!(
                serde_json::to_vec(&value)?.len() <= 8 * 1024 * 1024,
                "RESPONSE_TOO_LARGE"
            );
            return Ok(value);
        }
        if request.method == "POST"
            && parts.len() == 3
            && parts[0] == "sources"
            && parts[2] == "plan"
        {
            query_argument(request.query, None)?;
            let input = request
                .input
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
            ensure!(
                input.len() == 2 && input.contains_key("digest") && input.contains_key("deletions"),
                "INPUT_INVALID"
            );
            return bounded(
                NativeSources::plan_in(
                    client,
                    session,
                    identifier(&parts[1])?,
                    input["digest"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?,
                    &input["deletions"],
                )
                .await?,
            );
        }
        if request.method == "POST"
            && parts.len() == 3
            && parts[0] == "plans"
            && parts[2] == "apply"
        {
            query_argument(request.query, None)?;
            ensure!(
                request
                    .input
                    .as_object()
                    .is_some_and(|input| input.is_empty()),
                "INPUT_INVALID"
            );
            return bounded(
                NativeSources::activate_in(client, session, identifier(&parts[1])?).await?,
            );
        }
        if request.method == "POST"
            && parts.len() == 3
            && parts[0] == "entries"
            && parts[2] == "policy"
        {
            query_argument(request.query, None)?;
            return Self::policy_write_context(
                client,
                session,
                identifier(&parts[1])?,
                request.input,
                request.consumed,
                context,
            )
            .await;
        }
        if request.method == "GET"
            && parts.first().is_some_and(|part| part == "entries")
            && (parts.len() == 2 || parts.len() == 3)
        {
            let id = identifier(&parts[1])?;
            if parts.len() == 2 {
                query_argument(request.query, None)?;
                return bounded(json!(Self::get_in(client, session, id).await?.0));
            }
            if parts[2] == "children" {
                return Self::list_in(
                    client,
                    session,
                    id,
                    query_argument(request.query, Some("cursor"))?,
                )
                .await;
            }
            let argument = query_argument(
                request.query,
                match parts[2].as_str() {
                    "version" => Some("revision"),
                    "history" => Some("before"),
                    "role" | "policy" => None,
                    _ => return Err(anyhow::anyhow!("UNSUPPORTED_CAPABILITY")),
                },
            )?;
            return bounded(Self::metadata_in(client, session, id, &parts[2], argument).await?);
        }
        ensure!(
            request.method == "POST" && parts.len() == 1,
            "UNSUPPORTED_CAPABILITY"
        );
        query_argument(request.query, None)?;
        let input = request
            .input
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
        match parts[0].as_str() {
            "copy" => Self::copy_context(
                client,
                session,
                request
                    .key
                    .ok_or_else(|| anyhow::anyhow!("IDEMPOTENCY_KEY_INVALID"))?,
                request.input,
                request.consumed,
                context,
            )
            .await
            .map_err(mutation_error),
            "resolve" => {
                ensure!(
                    input.len() == 1 && input.contains_key("path"),
                    "INPUT_INVALID"
                );
                bounded(json!(
                    Self::resolve_in(
                        client,
                        session,
                        input["path"]
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?
                    )
                    .await?
                    .0
                ))
            }
            "resources" => {
                ensure!(
                    input.contains_key("reference")
                        && input
                            .keys()
                            .all(|key| ["reference", "projectId"].contains(&key.as_str())),
                    "INPUT_INVALID"
                );
                let project = input
                    .get("projectId")
                    .map(|value| {
                        identifier(
                            value
                                .as_str()
                                .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
                        )
                    })
                    .transpose()?;
                bounded(
                    Self::resource_in(
                        client,
                        session,
                        input["reference"]
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?,
                        project,
                    )
                    .await?,
                )
            }
            "changes" => {
                ensure!(
                    input.len() == 1 && input.contains_key("changes"),
                    "INPUT_INVALID"
                );
                Self::apply_in_context(
                    client,
                    session,
                    request
                        .key
                        .ok_or_else(|| anyhow::anyhow!("IDEMPOTENCY_KEY_INVALID"))?,
                    &input["changes"],
                    request.consumed,
                    context,
                )
                .await
                .map_err(mutation_error)
            }
            _ => Err(anyhow::anyhow!("UNSUPPORTED_CAPABILITY")),
        }
    }
    pub(crate) async fn route(
        &self,
        session: &VerifiedSession,
        request: &FilesystemRequest<'_>,
    ) -> Result<Value> {
        if request.method == "GET"
            && request.parts.len() == 3
            && request.parts[0] == "sources"
            && request.parts[2] == "runtime"
        {
            query_argument(request.query, None)?;
            return NativeSources {
                db: self.db.clone(),
            }
            .runtime_status(session, identifier(&request.parts[1])?)
            .await;
        }
        if request.method == "POST"
            && request.parts.len() == 3
            && request.parts[0] == "sources"
            && ["register", "retire", "publish"].contains(&request.parts[2].as_str())
        {
            query_argument(request.query, None)?;
            let sources = NativeSources {
                db: self.db.clone(),
            };
            let id = identifier(&request.parts[1])?;
            let input = request
                .input
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
            return tokio::time::timeout(std::time::Duration::from_secs(30), async {
                if request.parts[2] == "publish" {
                    ensure!(
                        input.len() == 2
                            && input.contains_key("expectedSourceDigest")
                            && input.contains_key("artifact"),
                        "INPUT_INVALID"
                    );
                    let artifact: CompilerArtifactRef =
                        serde_json::from_value(input["artifact"].clone())
                            .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?;
                    return sources
                        .publish(
                            session,
                            id,
                            input["expectedSourceDigest"]
                                .as_str()
                                .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?,
                            &artifact,
                        )
                        .await;
                }
                let expected = input
                    .get("expectedRevision")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
                if request.parts[2] == "register" {
                    ensure!(
                        input.len() == 2 && input.contains_key("kind"),
                        "INPUT_INVALID"
                    );
                    Ok(json!(
                        sources
                            .register(
                                session,
                                id,
                                expected,
                                input["kind"]
                                    .as_str()
                                    .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?
                            )
                            .await?
                    ))
                } else {
                    ensure!(input.len() == 1, "INPUT_INVALID");
                    sources.retire(session, id, expected).await
                }
            })
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")));
        }
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let write = request.writes();
            let admission = Admission::begin_filesystem(&self.db, session, write, write).await?;
            let outcome = async {
                let snapshot = Authority::current(admission.client(), session).await?;
                let value = Self::route_in(admission.client(), session, request).await?;
                if !write {
                    Authority::finish(admission.client(), session, &snapshot).await?;
                }
                Ok(value)
            }
            .await;
            admission.complete(outcome).await
        })
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")))
    }
    async fn directory(client: &Client, id: Uuid) -> Result<Entry> {
        let entry = Self::entry(client, id, false).await?;
        ensure!(entry.kind == "directory", "FILESYSTEM_INVALID");
        Ok(entry)
    }
    fn expect(entry: &Entry, revision: &str) -> Result<()> {
        ensure!(entry.revision == revision, "FILESYSTEM_CONFLICT");
        Ok(())
    }
    async fn placement(
        client: &Client,
        parent: Uuid,
        name: &str,
        moving: Option<Uuid>,
    ) -> Result<()> {
        let row=client.query_one("WITH RECURSIVE ancestors AS (SELECT id,parent_id,name FROM moria.fs_entries WHERE id=$1 UNION ALL SELECT e.id,e.parent_id,e.name FROM moria.fs_entries e JOIN ancestors a ON e.id=a.parent_id), descendants AS (SELECT id,0 AS depth,0 AS bytes FROM moria.fs_entries WHERE id=$2 UNION ALL SELECT e.id,d.depth+1,d.bytes+octet_length(e.name)+1 FROM moria.fs_entries e JOIN descendants d ON e.parent_id=d.id WHERE NOT e.deleted) SELECT (SELECT count(*) FROM ancestors)+COALESCE((SELECT max(depth) FROM descendants),0),(SELECT sum(octet_length(name)+1) FROM ancestors)+COALESCE((SELECT max(bytes) FROM descendants),0)",&[&parent,&moving]).await?;
        ensure!(
            row.get::<_, i64>(0) <= 64 && row.get::<_, i64>(1) + name.len() as i64 <= 4096,
            "FILESYSTEM_INVALID"
        );
        Ok(())
    }
    async fn managed(client: &Client, entry: &Entry, destination: Option<Uuid>) -> Result<()> {
        let id = entry.id.to_string();
        let configuration=client.query_opt("SELECT 1 FROM moria.fs_roles WHERE configuration->>'configuration'=$1 OR configuration->>'memory'=$1 OR configuration->>'runs'=$1 OR configuration->>'projects'=$1 LIMIT 1",&[&id]).await?.is_some();
        ensure!(
            !configuration || destination.is_some_and(|id| Some(id) == entry.parent_id),
            "FILESYSTEM_INVALID"
        );
        if let Some(role) = client
            .query_opt(
                "SELECT kind,retired FROM moria.fs_roles WHERE entry_id=$1",
                &[&entry.id],
            )
            .await?
        {
            let kind: String = role.get(0);
            let retired: bool = role.get(1);
            ensure!(
                kind != "system" && (destination.is_some() || retired),
                "FILESYSTEM_INVALID"
            );
            if ["group", "user", "agent"].contains(&kind.as_str()) {
                ensure!(destination == entry.parent_id, "FILESYSTEM_INVALID");
            }
        }
        if let Some(destination) = destination {
            let nested:bool=client.query_one("WITH RECURSIVE tree AS (SELECT id FROM moria.fs_entries WHERE id=$1 UNION ALL SELECT e.id FROM moria.fs_entries e JOIN tree t ON e.parent_id=t.id WHERE NOT e.deleted), ancestors AS (SELECT id,parent_id FROM moria.fs_entries WHERE id=$2 UNION ALL SELECT e.id,e.parent_id FROM moria.fs_entries e JOIN ancestors a ON e.id=a.parent_id) SELECT EXISTS(SELECT 1 FROM moria.fs_roles WHERE entry_id IN(SELECT id FROM tree) AND kind IN('ontology','repository') AND NOT retired) AND EXISTS(SELECT 1 FROM moria.fs_roles WHERE entry_id IN(SELECT id FROM ancestors) AND kind IN('ontology','repository') AND NOT retired)",&[&entry.id,&destination]).await?.get(0);
            ensure!(!nested, "SOURCE_INVALID");
        }
        Ok(())
    }
    async fn change_guard(
        client: &Client,
        session: &VerifiedSession,
        change: &Change,
        replay: bool,
        consumed: &[String],
    ) -> Result<()> {
        let mut checks = vec![];
        match change {
            Change::Create { parent_id, .. } => {
                checks.extend([(*parent_id, "read"), (*parent_id, "create")])
            }
            Change::Move { id, parent_id, .. } => {
                checks.extend([
                    (*id, "read"),
                    (*id, "move"),
                    (*parent_id, "read"),
                    (*parent_id, "create"),
                ]);
                if !replay {
                    let entry = Self::entry(client, *id, false).await?;
                    Self::managed(client, &entry, Some(*parent_id)).await?;
                    let parent = entry
                        .parent_id
                        .ok_or_else(|| anyhow::anyhow!("FILESYSTEM_INVALID"))?;
                    checks.extend([(parent, "read"), (parent, "delete")]);
                }
            }
            Change::Delete { id, .. } => {
                checks.extend([(*id, "read"), (*id, "delete")]);
                if !replay {
                    let entry = Self::entry(client, *id, false).await?;
                    Self::managed(client, &entry, None).await?;
                    let parent = entry
                        .parent_id
                        .ok_or_else(|| anyhow::anyhow!("FILESYSTEM_INVALID"))?;
                    checks.extend([(parent, "read"), (parent, "delete")]);
                }
            }
        }
        for (id, operation) in checks {
            let labels = Self::access(client, session, id, operation).await?;
            if operation != "read" {
                ensure!(
                    consumed.iter().all(|label| labels.contains(label)),
                    "MARKING_FLOW"
                );
            }
        }
        Ok(())
    }
    async fn remember(
        client: &Client,
        entry: Entry,
        mutation: Uuid,
        touched: &mut Vec<Entry>,
    ) -> Result<()> {
        client.execute("INSERT INTO moria.fs_entry_versions(entry_id,revision,snapshot,mutation_id) VALUES($1,$2::text::bigint,$3,$4)",&[&entry.id,&entry.revision,&json!(entry),&mutation]).await?;
        if let Some(position) = touched.iter().position(|previous| previous.id == entry.id) {
            touched[position] = entry;
        } else {
            touched.push(entry);
        }
        Ok(())
    }
    async fn touch(
        client: &Client,
        id: Uuid,
        mutation: Uuid,
        touched: &mut Vec<Entry>,
    ) -> Result<()> {
        let row = client
            .query_one(
                &format!(
                    "UPDATE moria.fs_entries SET revision=revision+1 WHERE id=$1 RETURNING {ENTRY}"
                ),
                &[&id],
            )
            .await?;
        Self::remember(
            client,
            serde_json::from_value(row.get(0))?,
            mutation,
            touched,
        )
        .await
    }
    pub async fn apply(
        &self,
        session: &VerifiedSession,
        key: &str,
        input: &Value,
    ) -> Result<Value> {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let admission = Admission::begin_filesystem(&self.db, session, true, true).await?;
            let outcome = Self::apply_in(admission.client(), session, key, input, &[])
                .await
                .map_err(mutation_error);
            admission.complete(outcome).await
        })
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")))
    }
    pub(crate) async fn apply_in(
        client: &Client,
        session: &VerifiedSession,
        key_value: &str,
        input: &Value,
        consumed: &[String],
    ) -> Result<Value> {
        Self::apply_in_context(client, session, key_value, input, consumed, None).await
    }
    pub(crate) async fn apply_in_context(
        client: &Client,
        session: &VerifiedSession,
        key_value: &str,
        input: &Value,
        consumed: &[String],
        context: Option<&crate::sandbox::SandboxContext>,
    ) -> Result<Value> {
        key(key_value)?;
        let changes = changes(input)?;
        let mut snapshot = Authority::current(client, session).await?;
        let actor = json!({"environment":session.environment(),"principal":session.principal(),"application":session.application(),"delegation":[]});
        let scope = fingerprint(&actor)?;
        let digest = fingerprint(input)?;
        if let Some(row)=client.query_opt("SELECT request_hash,receipt FROM moria.fs_mutations WHERE scope_hash=$1 AND request_key=$2",&[&scope,&key_value]).await? {
            ensure!(row.get::<_,String>(0)==digest,"IDEMPOTENCY_CONFLICT");
            for change in &changes {Self::change_guard(client,session,change,true,consumed).await?;}
            let receipt:Value=row.get(1);
            for entry in receipt["entries"].as_array().ok_or_else(||anyhow::anyhow!("FILESYSTEM_INVALID"))? {Self::access(client,session,Uuid::parse_str(entry["id"].as_str().ok_or_else(||anyhow::anyhow!("FILESYSTEM_INVALID"))?)?,"read").await?;}
            Authority::finish(client,session,&snapshot).await?;return Ok(receipt);
        }
        let mutation = Uuid::new_v4();
        let mut touched = Vec::new();
        for change in &changes {
            let anchor = if let Some(context) = context {
                Some(context.mutation_anchor_in(client).await?)
            } else {
                None
            };
            Self::change_guard(client, session, change, false, consumed).await?;
            match change {
                Change::Create {
                    parent_id,
                    expected_parent_revision,
                    name,
                    kind,
                    ..
                } => {
                    let parent = Self::directory(client, *parent_id).await?;
                    Self::expect(&parent, expected_parent_revision)?;
                    Self::placement(client, parent.id, name, None).await?;
                    let row=client.query_one(&format!("INSERT INTO moria.fs_entries(id,parent_id,name,kind,project_id,environment_id) VALUES($1,$2,$3,$4,$5,$6) RETURNING {ENTRY}"),&[&Uuid::new_v4(),&parent.id,&name,&kind,&parent.project_id,&parent.environment_id]).await?;
                    Self::remember(
                        client,
                        serde_json::from_value(row.get(0))?,
                        mutation,
                        &mut touched,
                    )
                    .await?;
                    Self::touch(client, parent.id, mutation, &mut touched).await?;
                }
                Change::Move {
                    id,
                    expected_revision,
                    parent_id,
                    expected_parent_revision,
                    expected_source_parent_revision,
                    name,
                } => {
                    let entry = Self::entry(client, *id, false).await?;
                    Self::expect(&entry, expected_revision)?;
                    ensure!(entry.id.to_string() != ROOT, "FILESYSTEM_INVALID");
                    let source = Self::directory(
                        client,
                        entry
                            .parent_id
                            .ok_or_else(|| anyhow::anyhow!("FILESYSTEM_INVALID"))?,
                    )
                    .await?;
                    Self::expect(&source, expected_source_parent_revision)?;
                    let target = Self::directory(client, *parent_id).await?;
                    Self::expect(&target, expected_parent_revision)?;
                    ensure!(
                        source.project_id == target.project_id
                            && source.environment_id == target.environment_id,
                        "FILESYSTEM_INVALID"
                    );
                    ensure!(!client.query_one("WITH RECURSIVE ancestry AS (SELECT id,parent_id FROM moria.fs_entries WHERE id=$1 UNION ALL SELECT e.id,e.parent_id FROM moria.fs_entries e JOIN ancestry a ON e.id=a.parent_id) SELECT EXISTS(SELECT 1 FROM ancestry WHERE id=$2)",&[&target.id,&entry.id]).await?.get::<_,bool>(0),"FILESYSTEM_INVALID");
                    Self::placement(client, target.id, name, Some(entry.id)).await?;
                    let labels = Self::access(client, session, entry.id, "read").await?;
                    if !labels.is_empty() {
                        let markings=json!(labels.iter().map(|marking|json!({"environment":session.environment(),"marking":marking})).collect::<Vec<_>>());
                        client.execute("INSERT INTO moria.fs_policies(entry_id,retained_markings) VALUES($1,$2) ON CONFLICT(entry_id) DO UPDATE SET retained_markings=EXCLUDED.retained_markings",&[&entry.id,&markings]).await?;
                        snapshot.instance_epoch = governance::audit(
                            client,
                            session,
                            "retainMoveMarkings",
                            &entry.id.to_string(),
                        )
                        .await?;
                    }
                    let row=client.query_one(&format!("UPDATE moria.fs_entries SET parent_id=$2,name=$3,revision=revision+1 WHERE id=$1 RETURNING {ENTRY}"),&[&entry.id,&target.id,&name]).await?;
                    Self::remember(
                        client,
                        serde_json::from_value(row.get(0))?,
                        mutation,
                        &mut touched,
                    )
                    .await?;
                    Self::touch(client, source.id, mutation, &mut touched).await?;
                    if target.id != source.id {
                        Self::touch(client, target.id, mutation, &mut touched).await?;
                    }
                }
                Change::Delete {
                    id,
                    expected_revision,
                    expected_parent_revision,
                } => {
                    let entry = Self::entry(client, *id, false).await?;
                    Self::expect(&entry, expected_revision)?;
                    ensure!(
                        entry.id.to_string() != ROOT && entry.kind != "resource",
                        "UNSUPPORTED_CAPABILITY"
                    );
                    let parent = Self::directory(
                        client,
                        entry
                            .parent_id
                            .ok_or_else(|| anyhow::anyhow!("FILESYSTEM_INVALID"))?,
                    )
                    .await?;
                    Self::expect(&parent, expected_parent_revision)?;
                    ensure!(client.query_opt("SELECT 1 FROM moria.fs_entries WHERE parent_id=$1 AND NOT deleted LIMIT 1",&[&entry.id]).await?.is_none(),"FILESYSTEM_INVALID");
                    let row=client.query_one(&format!("UPDATE moria.fs_entries SET deleted=true,revision=revision+1 WHERE id=$1 RETURNING {ENTRY}"),&[&entry.id]).await?;
                    Self::remember(
                        client,
                        serde_json::from_value(row.get(0))?,
                        mutation,
                        &mut touched,
                    )
                    .await?;
                    Self::touch(client, parent.id, mutation, &mut touched).await?;
                }
            }
            if let Some(context) = context {
                context
                    .acknowledge_own_mutation_in(client, anchor.as_ref().unwrap())
                    .await?;
            }
        }
        for entry in &touched {
            Self::access(client, session, entry.id, "read").await?;
        }
        let receipt = json!({"mutationId":mutation,"entries":touched});
        ensure!(
            serde_json::to_vec(&receipt)?.len() <= 1024 * 1024,
            "RESPONSE_TOO_LARGE"
        );
        client.execute("INSERT INTO moria.fs_mutations(id,scope_hash,request_key,request_hash,actor,receipt) VALUES($1,$2,$3,$4,$5,$6)",&[&mutation,&scope,&key_value,&digest,&actor,&receipt]).await?;
        Authority::finish(client, session, &snapshot).await?;
        Ok(receipt)
    }
    pub(crate) async fn access(
        client: &Client,
        session: &VerifiedSession,
        id: Uuid,
        operation: &str,
    ) -> Result<Vec<String>> {
        let labels: Value = client
            .query_one(
                "SELECT moria.native_filesystem_access($1,$2,$3)",
                &[&session.session_hash(), &id, &operation],
            )
            .await?
            .get(0);
        Ok(serde_json::from_value(labels)?)
    }
    pub(crate) async fn entry(client: &Client, id: Uuid, deleted: bool) -> Result<Entry> {
        let row = client
            .query_opt(
                &format!(
                    "SELECT {ENTRY} FROM moria.fs_entries WHERE id=$1 AND ($2 OR NOT deleted)"
                ),
                &[&id, &deleted],
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?;
        Ok(serde_json::from_value(row.get(0))?)
    }
    pub(crate) async fn get_in(
        client: &Client,
        session: &VerifiedSession,
        id: Uuid,
    ) -> Result<(Entry, Vec<String>)> {
        let labels = Self::access(client, session, id, "read")
            .await
            .map_err(|error| {
                if matches!(
                    crate::serving::error_code(&error),
                    "FORBIDDEN" | "NOT_FOUND"
                ) {
                    anyhow::anyhow!("NOT_FOUND")
                } else {
                    error
                }
            })?;
        Ok((Self::entry(client, id, false).await?, labels))
    }
    pub async fn get(&self, session: &VerifiedSession, id: Uuid) -> Result<Value> {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let admission = Admission::begin(&self.db, session, false).await?;
            let outcome = async {
                let snapshot = Authority::current(admission.client(), session).await?;
                let (entry, _) = Self::get_in(admission.client(), session, id).await?;
                Authority::finish(admission.client(), session, &snapshot).await?;
                bounded(json!(entry))
            }
            .await;
            admission.complete(outcome).await
        })
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")))
    }
    pub async fn resolve(&self, session: &VerifiedSession, path: &str) -> Result<Value> {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let admission = Admission::begin(&self.db, session, false).await?;
            let outcome = async {
                let snapshot = Authority::current(admission.client(), session).await?;
                let (entry, _) = Self::resolve_in(admission.client(), session, path).await?;
                Authority::finish(admission.client(), session, &snapshot).await?;
                bounded(json!(entry))
            }
            .await;
            admission.complete(outcome).await
        })
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")))
    }
    pub async fn resource(
        &self,
        session: &VerifiedSession,
        reference: &str,
        project: Option<Uuid>,
    ) -> Result<Value> {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let admission = Admission::begin(&self.db, session, false).await?;
            let outcome = async {
                let snapshot = Authority::current(admission.client(), session).await?;
                let value =
                    Self::resource_in(admission.client(), session, reference, project).await?;
                Authority::finish(admission.client(), session, &snapshot).await?;
                bounded(value)
            }
            .await;
            admission.complete(outcome).await
        })
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")))
    }
    pub(crate) async fn resource_in(
        client: &Client,
        session: &VerifiedSession,
        reference: &str,
        project: Option<Uuid>,
    ) -> Result<Value> {
        ensure!(
            !reference.is_empty() && reference.len() <= 512,
            "SOURCE_INVALID"
        );
        let uid = Uuid::parse_str(reference)
            .ok()
            .filter(|uid| uid.to_string() == reference);
        ensure!(uid.is_some() || project.is_some(), "SOURCE_INVALID");
        let row=client.query_opt("SELECT b.entry_id,b.digest,b.reference,e.resource_id,a.resources FROM moria.fs_resource_bindings b JOIN moria.fs_entries e ON e.id=b.entry_id AND NOT e.deleted JOIN moria.fs_roles role ON role.entry_id=b.root_id AND NOT role.retired JOIN moria.fs_active_releases a ON a.root_id=b.root_id AND a.environment=b.environment AND a.digest=b.digest WHERE b.environment=$1 AND b.active AND (($2::uuid IS NOT NULL AND e.resource_id=$2) OR ($2::uuid IS NULL AND b.reference=$3)) AND ($4::uuid IS NULL OR b.project_id=$4)",&[&session.environment(),&uid,&reference,&project]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
        let id: Uuid = row.get(0);
        let labels = Self::access(client, session, id, "read")
            .await
            .map_err(|error| {
                if matches!(
                    crate::serving::error_code(&error),
                    "FORBIDDEN" | "NOT_FOUND"
                ) {
                    anyhow::anyhow!("NOT_FOUND")
                } else {
                    error
                }
            })?;
        let resources: Value = row.get(4);
        let reference: String = row.get(2);
        let resource = resources
            .as_array()
            .and_then(|resources| {
                resources.iter().find(|resource| {
                    format!(
                        "{}/{}/{}",
                        resource["metadata"]["namespace"].as_str().unwrap_or(""),
                        resource["kind"].as_str().unwrap_or(""),
                        resource["metadata"]["name"].as_str().unwrap_or("")
                    ) == reference
                })
            })
            .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?;
        Ok(
            json!({"entryId":id,"resourceId":row.get::<_,Uuid>(3),"digest":row.get::<_,String>(1),"resource":resource,"markings":labels.into_iter().map(|marking|json!({"environment":session.environment(),"marking":marking})).collect::<Vec<_>>()}),
        )
    }
    pub async fn metadata(
        &self,
        session: &VerifiedSession,
        id: Uuid,
        operation: &str,
        argument: Option<&str>,
    ) -> Result<Value> {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let admission = Admission::begin(&self.db, session, false).await?;
            let outcome = async {
                let snapshot = Authority::current(admission.client(), session).await?;
                let value =
                    Self::metadata_in(admission.client(), session, id, operation, argument).await?;
                Authority::finish(admission.client(), session, &snapshot).await?;
                bounded(value)
            }
            .await;
            admission.complete(outcome).await
        })
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")))
    }
    pub(crate) async fn metadata_in(
        client: &Client,
        session: &VerifiedSession,
        id: Uuid,
        operation: &str,
        argument: Option<&str>,
    ) -> Result<Value> {
        Self::access(
            client,
            session,
            id,
            if operation == "policy" {
                "administer"
            } else {
                "read"
            },
        )
        .await
        .map_err(|error| {
            if matches!(
                crate::serving::error_code(&error),
                "FORBIDDEN" | "NOT_FOUND"
            ) {
                anyhow::anyhow!("NOT_FOUND")
            } else {
                error
            }
        })?;
        match operation {
            "version" => {
                let value = argument.ok_or_else(|| anyhow::anyhow!("FILESYSTEM_INVALID"))?;
                revision(value)?;
                let row=client.query_opt("SELECT snapshot FROM moria.fs_entry_versions WHERE entry_id=$1 AND revision=$2::text::bigint",&[&id,&value]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?;
                Ok(row.get(0))
            }
            "history" => {
                if let Some(value) = argument {
                    revision(value)?;
                }
                let rows=client.query("SELECT v.revision::text,v.snapshot FROM moria.fs_entry_versions v WHERE v.entry_id=$1 AND ($2::text IS NULL OR v.revision<$2::text::bigint) ORDER BY v.revision DESC LIMIT 101",&[&id,&argument]).await?;
                let mut value = json!({"entries":rows.iter().take(100).map(|row|row.get::<_,Value>(1)).collect::<Vec<_>>()});
                if rows.len() > 100 {
                    value["nextRevision"] = rows[99].get::<_, String>(0).into();
                }
                Ok(value)
            }
            "role" => {
                ensure!(argument.is_none(), "INPUT_INVALID");
                Ok(client.query_opt("SELECT jsonb_build_object('kind',kind,'projectId',project_id,'deploymentUnitId',deployment_unit_id,'namespace',namespace,'configuration',configuration,'revision',revision::text,'latestRelease',latest_release,'retired',retired) FROM moria.fs_roles WHERE entry_id=$1",&[&id]).await?.map_or(Value::Null,|row|row.get(0)))
            }
            "policy" => {
                ensure!(argument.is_none(), "INPUT_INVALID");
                Ok(client.query_opt("SELECT jsonb_build_object('revision',revision::text,'markings',markings,'grants',grants,'retainedMarkings',retained_markings) FROM moria.fs_policies WHERE entry_id=$1",&[&id]).await?.map_or(json!({"revision":"0","markings":[],"grants":[],"retainedMarkings":[]}),|row|row.get(0)))
            }
            _ => Err(anyhow::anyhow!("UNSUPPORTED_CAPABILITY")),
        }
    }
    pub async fn list(
        &self,
        session: &VerifiedSession,
        parent: Uuid,
        cursor: Option<&str>,
    ) -> Result<Value> {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let admission = Admission::begin(&self.db, session, false).await?;
            let outcome = Self::list_in(admission.client(), session, parent, cursor).await;
            admission.complete(outcome).await
        })
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")))
    }
    pub(crate) async fn list_in(
        client: &Client,
        session: &VerifiedSession,
        parent_id: Uuid,
        cursor: Option<&str>,
    ) -> Result<Value> {
        let snapshot = Authority::current(client, session).await?;
        Self::access(client, session, parent_id, "list")
            .await
            .map_err(|error| {
                if matches!(
                    crate::serving::error_code(&error),
                    "FORBIDDEN" | "NOT_FOUND"
                ) {
                    anyhow::anyhow!("NOT_FOUND")
                } else {
                    error
                }
            })?;
        let parent = Self::directory(client, parent_id).await?;
        let identity: String = client
            .query_one(
                "SELECT moria.native_serving_session($1)->>'instanceId'",
                &[&session.session_hash()],
            )
            .await?
            .get(0);
        let digest = fingerprint(
            &json!({"environment":session.environment(),"principal":identity,"application":session.application(),"delegation":[],"contract":"filesystem-list-v1","query":{"parentId":parent_id}}),
        )?;
        let (revision, expires, after) = if let Some(cursor) = cursor {
            ensure!(
                cursor.len() == 43
                    && cursor
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte)),
                "CURSOR_INVALID"
            );
            let row=client.query_opt("SELECT binding_hash,security_epoch::text,revision::text,position,expires_at::text,expires_at<=clock_timestamp() FROM moria.read_cursors WHERE token_hash=$1",&[&crate::serving::hash(cursor)]).await?.ok_or_else(||anyhow::anyhow!("CURSOR_INVALID"))?;
            ensure!(row.get::<_, String>(0) == digest, "CURSOR_INVALID");
            ensure!(!row.get::<_, bool>(5), "CURSOR_EXPIRED");
            ensure!(
                row.get::<_, String>(1) == snapshot.security_epoch,
                "CURSOR_INVALIDATED"
            );
            let position: Value = row.get(3);
            ensure!(
                position["parentRevision"] == parent.revision
                    && position["instanceEpoch"] == snapshot.instance_epoch,
                "CURSOR_INVALIDATED"
            );
            (
                row.get::<_, String>(2),
                row.get::<_, String>(4),
                Some(position),
            )
        } else {
            let row=client.query_one("SELECT revision::text,(clock_timestamp()+interval '15 minutes')::text FROM moria.environments WHERE id=$1",&[&session.environment()]).await?;
            (row.get(0), row.get(1), None)
        };
        let after_name = after
            .as_ref()
            .map(|position| {
                position["name"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("CURSOR_INVALID"))
            })
            .transpose()?;
        let after_id = after
            .as_ref()
            .map(|position| {
                position["id"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("CURSOR_INVALID"))
                    .and_then(|value| {
                        Uuid::parse_str(value).map_err(|_| anyhow::anyhow!("CURSOR_INVALID"))
                    })
            })
            .transpose()?;
        if let Some(after_name) = after_name {
            name(after_name)?;
        }
        let rows=client.query(&format!("SELECT {ENTRY} FROM moria.fs_entries WHERE parent_id=$1 AND NOT deleted AND ($2::text IS NULL OR (name,id)>($2::text COLLATE \"C\",$3::uuid)) AND moria.native_filesystem_visible($4,id) ORDER BY name COLLATE \"C\",id LIMIT 101"),&[&parent_id,&after_name,&after_id,&session.session_hash()]).await?;
        let mut entries = Vec::new();
        for row in rows.iter().take(100) {
            entries.push(serde_json::from_value::<Entry>(row.get(0))?);
        }
        let mut value = json!({"entries":entries});
        if rows.len() > 100 {
            let token = crate::serving::token()?;
            let last = &entries[99];
            let position = json!({"name":last.name,"id":last.id,"parentRevision":parent.revision,"instanceEpoch":snapshot.instance_epoch});
            ensure!(client.execute("INSERT INTO moria.read_cursors SELECT $1,$2,$3,$4::text::bigint,$5::text::bigint,$6,$7::text::timestamptz WHERE $7::text::timestamptz>clock_timestamp()",&[&crate::serving::hash(&token),&session.environment(),&digest,&snapshot.security_epoch,&revision,&position,&expires]).await?==1,"CURSOR_EXPIRED");
            value["nextCursor"] = token.into();
        }
        Authority::finish(client, session, &snapshot).await?;
        bounded(value)
    }
    pub(crate) async fn resolve_in(
        client: &Client,
        session: &VerifiedSession,
        path: &str,
    ) -> Result<(Entry, Vec<String>)> {
        ensure!(
            path.starts_with('/') && path.len() <= 4096,
            "FILESYSTEM_INVALID"
        );
        let parts = if path == "/" {
            vec![]
        } else {
            path[1..].split('/').collect::<Vec<_>>()
        };
        ensure!(parts.len() <= 64, "FILESYSTEM_INVALID");
        let mut id: Uuid = ROOT.parse()?;
        for part in parts {
            name(part)?;
            let current = Self::entry(client, id, false).await?;
            ensure!(current.kind == "directory", "NOT_FOUND");
            id=client.query_opt("SELECT id FROM moria.fs_entries WHERE parent_id=$1 AND name=$2 AND NOT deleted",&[&id,&part]).await?.ok_or_else(||anyhow::anyhow!("NOT_FOUND"))?.get(0);
        }
        Self::get_in(client, session, id).await
    }
}
