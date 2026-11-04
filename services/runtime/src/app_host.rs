use crate::{
    authority::{Authority, VerifiedSession},
    database::Database,
    serving::Admission,
};
use anyhow::{Result, ensure};
use axum::{
    body::Body,
    http::{Method, Response, StatusCode, header},
};
use rustix::fs::{CWD, Mode, OFlags, ResolveFlags, openat2};
use serde::{
    Deserialize, Serialize,
    de::{MapAccess, Visitor},
    ser::SerializeMap,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    fs::File,
    io::Read,
    path::{Component, PathBuf},
    sync::Arc,
};
use tokio::sync::Semaphore;
use uuid::Uuid;

const FILE_BYTES: u64 = 16 * 1024 * 1024;
const ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;
const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'self'; form-action 'self'; worker-src 'none'";

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Configuration {
    directory: PathBuf,
    app: String,
    entry_path: String,
    environment: String,
    application: String,
    source_resource: Option<Uuid>,
    #[serde(default)]
    public_shell: bool,
}
#[derive(Clone)]
pub(crate) struct NativeAppHost {
    configuration: Configuration,
    root: Arc<File>,
    transfers: Arc<Semaphore>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    app: String,
    version: String,
    files: Files,
}
struct Files(Vec<(String, String)>);
impl<'de> Deserialize<'de> for Files {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Entries;
        impl<'de> Visitor<'de> for Entries {
            type Value = Files;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("bounded immutable artifact files")
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Files, A::Error> {
                let mut result = Vec::new();
                let mut names = std::collections::BTreeSet::new();
                while let Some((name, digest)) = map.next_entry::<String, String>()? {
                    if result.len() >= 2000
                        || !safe_file(&name)
                        || !hex_digest(&digest)
                        || !names.insert(name.clone())
                    {
                        return Err(serde::de::Error::custom("APP_CORRUPT"));
                    }
                    result.push((name, digest));
                }
                Ok(Files(result))
            }
        }
        deserializer.deserialize_map(Entries)
    }
}
impl Serialize for Files {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, digest) in &self.0 {
            map.serialize_entry(name, digest)?;
        }
        map.end()
    }
}
fn hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 48
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !reserved(value)
}
fn reserved(value: &str) -> bool {
    let value = value
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    ["con", "prn", "aux", "nul"].contains(&value.as_str())
        || (value.len() == 4
            && (value.starts_with("com") || value.starts_with("lpt"))
            && value.as_bytes()[3].is_ascii_digit())
}
fn safe_file(value: &str) -> bool {
    value.len() <= 240
        && value != "artifact.json"
        && value.split('/').count() <= 9
        && value.split('/').all(|part| {
            !part.is_empty() && part.as_bytes()[0].is_ascii_alphanumeric() || part.starts_with('_')
        })
        && value.split('/').all(|part| {
            !part.ends_with('.')
                && !reserved(part)
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
        })
        && value.rsplit('.').next().is_some_and(|ext| {
            [
                "html", "js", "mjs", "css", "json", "svg", "png", "jpg", "jpeg", "webp", "gif",
                "ico", "woff", "woff2", "txt", "wasm",
            ]
            .contains(&ext)
        })
}
fn bytes(root: &File, path: &str, limit: u64) -> Result<Vec<u8>> {
    let mut file = File::from(openat2(
        root,
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
        ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
    )?);
    let metadata = file.metadata()?;
    ensure!(metadata.is_file() && metadata.len() <= limit, "APP_CORRUPT");
    let mut result = Vec::with_capacity(metadata.len() as usize);
    (&mut file).take(limit + 1).read_to_end(&mut result)?;
    ensure!(result.len() as u64 == metadata.len(), "APP_CORRUPT");
    Ok(result)
}
impl NativeAppHost {
    pub(crate) async fn frontend_available(resource: Uuid, artifact: &Value) -> Result<bool> {
        let Some(host) = Self::configured()? else {
            return Ok(false);
        };
        if host.configuration.source_resource != Some(resource) {
            return Ok(false);
        }
        let app = host.configuration.app.clone();
        let actual =
            tokio::task::spawn_blocking(move || host.asset(&host.configuration.entry_path)).await?;
        Ok(actual.is_ok_and(|(version, _, _, _)| {
            artifact["digest"] == format!("sha256:{version}")
                && artifact["uri"] == format!("moria-app://{app}/{version}")
        }))
    }
    pub(crate) fn protected(&self) -> bool {
        !self.configuration.public_shell
    }
    pub(crate) fn configured() -> Result<Option<Self>> {
        let Ok(text) = std::env::var("MORIA_NATIVE_APP_HOST") else {
            return Ok(None);
        };
        ensure!(text.len() <= 65536, "APP_INVALID");
        let configuration: Configuration = serde_json::from_str(&text)?;
        ensure!(
            identifier(&configuration.app)
                && configuration.directory.is_absolute()
                && configuration
                    .directory
                    .components()
                    .all(|part| !matches!(part, Component::ParentDir))
                && configuration.entry_path.starts_with('/')
                && configuration.entry_path.ends_with('/')
                && configuration.entry_path.len() <= 240
                && configuration
                    .entry_path
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"/_-".contains(&b))
                && !configuration.environment.is_empty()
                && !configuration.application.is_empty()
                && configuration
                    .source_resource
                    .is_none_or(|resource| configuration.application == resource.to_string())
                && (!configuration.public_shell
                    || (configuration.app == "management"
                        && configuration.source_resource.is_none())),
            "APP_INVALID"
        );
        let binding: Value = serde_json::from_str(
            &std::env::var("MORIA_NATIVE_BROWSER_BINDING")
                .map_err(|_| anyhow::anyhow!("APP_INVALID"))?,
        )?;
        ensure!(
            binding["environment"] == configuration.environment
                && binding["application"] == configuration.application,
            "APP_INVALID"
        );
        let root = File::from(openat2(
            CWD,
            &configuration.directory,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        )?);
        Ok(Some(Self {
            configuration,
            root: Arc::new(root),
            transfers: Arc::new(Semaphore::new(8)),
        }))
    }
    async fn authorize(
        &self,
        db: &Database,
        session: &VerifiedSession,
        version: Option<&str>,
    ) -> Result<String> {
        ensure!(
            session.environment() == self.configuration.environment
                && session.application() == self.configuration.application,
            "FORBIDDEN"
        );
        let admission = Admission::begin(db, session, false).await?;
        let result=async {
            let client=admission.client();let state=Authority::current(client,session).await?;
            if let Some(resource)=self.configuration.source_resource {
                client.query_one("SELECT moria.native_app_frontend_access($1,$2)",&[&session.session_hash(),&resource]).await?;
                let row=client.query_opt("SELECT a.resources,i.reference FROM moria.fs_resource_identities i JOIN moria.fs_entries e ON e.resource_id=i.resource_id AND e.environment_id=$1 AND NOT e.deleted JOIN moria.fs_resource_bindings b ON b.entry_id=e.id AND b.active AND b.environment=$1 JOIN moria.fs_active_releases a ON a.root_id=b.root_id AND a.environment=b.environment WHERE i.resource_id=$2",&[&session.environment(),&resource]).await?.ok_or_else(||anyhow::anyhow!("APP_UNAVAILABLE"))?;
                let resources:Value=row.get(0);
                let reference:String=row.get(1);
                let declaration=resources.as_array().and_then(|rows|rows.iter().find(|row|row["kind"]=="Application"&&format!("{}/Application/{}",row["metadata"]["namespace"].as_str().unwrap_or_default(),row["metadata"]["name"].as_str().unwrap_or_default())==reference)).ok_or_else(||anyhow::anyhow!("APP_UNAVAILABLE"))?;
                let actual=declaration["spec"]["frontend"]["digest"].as_str().and_then(|value|value.strip_prefix("sha256:")).ok_or_else(||anyhow::anyhow!("APP_UNAVAILABLE"))?;
                ensure!(hex_digest(actual)&&declaration["spec"]["mode"]=="browser"&&version.is_none_or(|version|version==actual)&&declaration["spec"]["frontend"]["uri"]==format!("moria-app://{}/{actual}",self.configuration.app),"APP_UNAVAILABLE");
                Authority::finish(client,session,&state).await?;
                Ok(actual.to_owned())
            } else {
                ensure!(self.configuration.public_shell,"APP_UNAVAILABLE");
                Authority::finish(client,session,&state).await?;
                Ok(version.unwrap_or_default().to_owned())
            }
        }.await;
        admission.complete(result).await
    }
    pub(crate) async fn contract(&self, db: &Database, session: &VerifiedSession) -> Result<Value> {
        self.authorize(db, session, None).await?;
        let host = self.clone();
        let (version, _, _, _) =
            tokio::task::spawn_blocking(move || host.asset(&host.configuration.entry_path))
                .await??;
        self.authorize(db, session, Some(&version)).await?;
        let admission = Admission::begin(db, session, false).await?;
        let result=async {
            let client=admission.client();let state=Authority::current(client,session).await?;
            let accepted=state.identity.application_scopes.intersection(&state.identity.granted_operations)
                .filter_map(|operation|operation.rsplit_once(':'))
                .filter(|(_,operation)|["read","invoke"].contains(operation))
                .filter_map(|(resource,_)|Uuid::parse_str(resource).ok().map(|id|id.to_string()))
                .collect::<std::collections::BTreeSet<_>>().into_iter().collect::<Vec<_>>();
            ensure!(accepted.len()<=2000,"RESOURCE_LIMIT");
            let rows=client.query("SELECT b.resource,'ObjectType' AS kind,COALESCE(i.reference,b.resource) AS reference FROM moria.native_serving_bindings b LEFT JOIN moria.fs_resource_identities i ON i.resource_id=b.object_type_id WHERE b.environment=$1 AND b.enabled AND b.resource=ANY($2::text[]) UNION ALL SELECT a.resource,'Action',COALESCE(i.reference,a.resource) FROM moria.native_serving_actions a LEFT JOIN moria.fs_resource_identities i ON i.resource_id::text=a.resource WHERE a.environment=$1 AND a.enabled AND a.resource=ANY($2::text[]) ORDER BY resource LIMIT 2001",&[&session.environment(),&accepted]).await?;
            ensure!(rows.len()<=2000,"RESOURCE_LIMIT");
            let mut names=std::collections::BTreeMap::<(String,String),usize>::new();
            for row in &rows {let reference:String=row.get(2);*names.entry((row.get(1),reference.rsplit('/').next().unwrap().to_owned())).or_default()+=1;}
            let mut types=serde_json::Map::new();let mut objects=serde_json::Map::new();let mut actions=serde_json::Map::new();let mut response_bytes=0usize;
            for row in rows {
                let id:String=row.get(0);let kind:String=row.get(1);let reference:String=row.get(2);
                let name=reference.rsplit('/').next().unwrap();
                let name=if names[&(kind.clone(),name.to_owned())]==1&&!reserved(name)&&name.len()<=100&&name.as_bytes()[0].is_ascii_alphabetic()&&name.bytes().all(|byte|byte.is_ascii_alphanumeric()||b"_-".contains(&byte)){name.to_owned()}else{format!("resource_{}",id.replace('-',""))};
                client.batch_execute("SAVEPOINT native_app_contract").await?;
                let object=async {
                    if kind=="ObjectType" {crate::serving::NativeServing::object_contract(client,session,&id).await}
                    else {
                        crate::serving::NativeServing::action_markings(client,session,&id).await?;
                        let (_,definition,_)=crate::serving::NativeServing::action_contract(client,session,&id).await?;
                        Ok(json!({"id":id,"inputs":definition["inputs"]}))
                    }
                }.await;
                if object.is_err(){client.batch_execute("ROLLBACK TO SAVEPOINT native_app_contract").await?;}
                client.batch_execute("RELEASE SAVEPOINT native_app_contract").await?;
                let object=match object {
                    Ok(object)=>object,
                    Err(error) if ["FORBIDDEN","NOT_FOUND"].contains(&crate::serving::error_code(&error))=>continue,
                    Err(error)=>return Err(error),
                };
                response_bytes=response_bytes.checked_add(serde_json::to_vec(&object)?.len()*2+name.len()+256).ok_or_else(||anyhow::anyhow!("RESPONSE_TOO_LARGE"))?;
                ensure!(response_bytes<=8*1024*1024,"RESPONSE_TOO_LARGE");
                if kind=="ObjectType" {
                    let type_name=format!("Object_{}",id.replace('-',""));
                    types.insert(type_name.clone(),json!({"type":"object","properties":object["properties"],"nullable":false}));
                    objects.insert(name,json!({"id":id,"type":type_name,"properties":object["properties"]}));
                }else{actions.insert(name,object);}
            }
            Authority::finish(client,session,&state).await?;
            Ok(json!({"version":1,"app":self.configuration.app,"types":types,"objects":objects,"actions":actions,"endpoints":{}}))
        }.await;
        let value = admission.complete(result).await?;
        self.authorize(db, session, Some(&version)).await?;
        Ok(value)
    }
    pub(crate) async fn response(
        &self,
        db: &Database,
        session: Option<&VerifiedSession>,
        method: &Method,
        path: &str,
    ) -> Result<Response<Body>> {
        ensure!([Method::GET, Method::HEAD].contains(method), "NOT_FOUND");
        let expected = if !self.configuration.public_shell {
            Some(
                self.authorize(
                    db,
                    session.ok_or_else(|| anyhow::anyhow!("UNAUTHENTICATED"))?,
                    None,
                )
                .await?,
            )
        } else {
            None
        };
        if path == "/" && self.configuration.entry_path != "/" {
            return Ok(Response::builder()
                .status(StatusCode::FOUND)
                .header(header::LOCATION, &self.configuration.entry_path)
                .header(header::CACHE_CONTROL, "no-store")
                .body(Body::empty())?);
        }
        let permit = self
            .transfers
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        let host = self.clone();
        let path = path.to_owned();
        let (version, name, mut content, _) =
            tokio::task::spawn_blocking(move || host.asset(&path)).await??;
        ensure!(
            expected.as_ref().is_none_or(|value| value == &version),
            "APP_UNAVAILABLE"
        );
        if !self.configuration.public_shell {
            self.authorize(db, session.unwrap(), Some(&version)).await?;
        }
        if name.ends_with(".html") {
            let text = String::from_utf8(content)?;
            static HEAD: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
            let at = HEAD
                .get_or_init(|| regex::Regex::new(r"(?i)<head(?:\s[^>]*)?>").unwrap())
                .find(&text)
                .map(|value| value.end())
                .ok_or_else(|| anyhow::anyhow!("APP_CORRUPT"))?;
            content = format!(
                "{}<base href=\"/releases/{version}/\">{}",
                &text[..at],
                &text[at..]
            )
            .into_bytes();
        }
        if !self.configuration.public_shell {
            self.authorize(db, session.unwrap(), Some(&version)).await?;
        }
        let hash = format!("{:x}", Sha256::digest(&content));
        let csp = std::env::var("MORIA_NATIVE_AUTH_ORIGIN").map_or_else(
            |_| CSP.to_owned(),
            |origin| format!("{CSP}; frame-src 'self' {origin}"),
        );
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, media_type(&name))
            .header(
                header::CACHE_CONTROL,
                if self.configuration.public_shell && !name.ends_with(".html") {
                    "public,max-age=31536000,immutable"
                } else {
                    "no-store"
                },
            )
            .header(header::CONTENT_SECURITY_POLICY, csp)
            .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
            .header(header::REFERRER_POLICY, "no-referrer")
            .header(header::ETAG, format!("\"{hash}\""))
            .header("x-moria-app", &self.configuration.app)
            .header("x-moria-version", version)
            .body(if *method == Method::HEAD {
                Body::empty()
            } else {
                Body::from(content)
            })?;
        Ok(crate::http_admission::hold_response(response, vec![permit]))
    }
    fn asset(&self, path: &str) -> Result<(String, String, Vec<u8>, String)> {
        let (version, mut name) = if let Some(rest) = path.strip_prefix("/releases/") {
            let (version, name) = rest
                .split_once('/')
                .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?;
            (
                version.to_owned(),
                if name.is_empty() {
                    "index.html".to_owned()
                } else {
                    crate::serving::decode(name)?
                },
            )
        } else {
            ensure!(
                path.starts_with(&self.configuration.entry_path)
                    && path[self.configuration.entry_path.len()..]
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"/_-".contains(&b)),
                "NOT_FOUND"
            );
            let active: Value = serde_json::from_slice(&bytes(
                &self.root,
                &format!("{}/active.json", self.configuration.app),
                16384,
            )?)?;
            (
                active["current"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("APP_CORRUPT"))?
                    .to_owned(),
                "index.html".to_owned(),
            )
        };
        ensure!(hex_digest(&version), "APP_CORRUPT");
        if !name.contains('.')
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"/_-".contains(&b))
        {
            name = "index.html".into();
        }
        ensure!(safe_file(&name), "NOT_FOUND");
        let prefix = format!("{}/releases/{version}", self.configuration.app);
        let artifact: Artifact = serde_json::from_slice(&bytes(
            &self.root,
            &format!("{prefix}/artifact.json"),
            1024 * 1024,
        )?)?;
        #[derive(Serialize)]
        struct HashInput<'a> {
            app: &'a str,
            files: &'a Files,
        }
        ensure!(
            artifact.app == self.configuration.app
                && artifact.version == version
                && artifact
                    .files
                    .0
                    .iter()
                    .any(|(name, _)| name == "index.html")
                && format!(
                    "{:x}",
                    Sha256::digest(serde_json::to_vec(&HashInput {
                        app: &artifact.app,
                        files: &artifact.files
                    })?)
                ) == version,
            "APP_CORRUPT"
        );
        let mut total = 0u64;
        for (name, _) in &artifact.files.0 {
            let file = File::from(openat2(
                &*self.root,
                format!("{prefix}/{name}"),
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
                ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
            )?);
            let metadata = file.metadata()?;
            total = total
                .checked_add(metadata.len())
                .ok_or_else(|| anyhow::anyhow!("APP_CORRUPT"))?;
            ensure!(
                metadata.is_file() && metadata.len() <= FILE_BYTES && total <= ARTIFACT_BYTES,
                "APP_CORRUPT"
            );
        }
        let expected = &artifact
            .files
            .0
            .iter()
            .find(|(path, _)| path == &name)
            .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?
            .1;
        let content = bytes(&self.root, &format!("{prefix}/{name}"), FILE_BYTES)?;
        let hash = format!("{:x}", Sha256::digest(&content));
        ensure!(&hash == expected, "APP_CORRUPT");
        ensure!(content.len() as u64 <= ARTIFACT_BYTES, "APP_CORRUPT");
        Ok((version, name, content, hash))
    }
}
fn media_type(name: &str) -> &'static str {
    match name.rsplit('.').next().unwrap_or_default() {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "gif" => "image/gif",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "txt" => "text/plain; charset=utf-8",
        "wasm" => "application/wasm",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_manifest_entry_order_is_preserved_and_duplicate_paths_are_rejected() {
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        let text = format!("{{\"z.js\":\"{a}\",\"A.js\":\"{b}\"}}");
        let files: Files = serde_json::from_str(&text).unwrap();
        assert_eq!(serde_json::to_string(&files).unwrap(), text);
        assert!(
            serde_json::from_str::<Files>(&format!("{{\"a.js\":\"{a}\",\"a.js\":\"{b}\"}}"))
                .is_err()
        );
        for path in [
            "../index.html",
            "assets/../../a.js",
            "/index.html",
            "a\\b.js",
            "con.html",
            "x/aux.js",
            "artifact.json",
            "x.js.",
        ] {
            assert!(!safe_file(path), "{path}");
        }
    }
    #[test]
    fn immutable_assets_verify_manifest_bytes_and_reject_symlink_replacement() {
        let root = std::env::temp_dir().join(format!("moria-app-host-{}", Uuid::new_v4()));
        let payload = b"<html><head></head><body>verified</body></html>";
        let hash = format!("{:x}", Sha256::digest(payload));
        let files = Files(vec![("index.html".into(), hash)]);
        #[derive(Serialize)]
        struct HashInput<'a> {
            app: &'a str,
            files: &'a Files,
        }
        let version = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&HashInput {
                    app: "example",
                    files: &files
                })
                .unwrap()
            )
        );
        let release = root.join(format!("example/releases/{version}"));
        std::fs::create_dir_all(&release).unwrap();
        std::fs::write(release.join("index.html"), payload).unwrap();
        std::fs::write(
            release.join("artifact.json"),
            serde_json::to_vec(&json!({"app":"example","version":version,"files":files})).unwrap(),
        )
        .unwrap();
        std::fs::write(
            root.join("example/active.json"),
            json!({"current":version}).to_string(),
        )
        .unwrap();
        let directory = File::from(
            openat2(
                CWD,
                &root,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
                ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
            )
            .unwrap(),
        );
        let host = NativeAppHost {
            configuration: Configuration {
                directory: root.clone(),
                app: "example".into(),
                entry_path: "/".into(),
                environment: "env".into(),
                application: "app".into(),
                source_resource: None,
                public_shell: false,
            },
            root: Arc::new(directory),
            transfers: Arc::new(Semaphore::new(8)),
        };
        assert_eq!(host.asset("/").unwrap().2, payload);
        std::fs::write(release.join("index.html"), b"corrupt").unwrap();
        assert!(host.asset("/").is_err());
        std::fs::remove_file(release.join("index.html")).unwrap();
        std::os::unix::fs::symlink(root.join("example/active.json"), release.join("index.html"))
            .unwrap();
        assert!(host.asset("/").is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
