use crate::{
    authority::{Authority, VerifiedSession},
    database::Database,
};
use anyhow::{Result, ensure};
use axum::http::{HeaderMap, Method, header};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};
use tokio::sync::{Mutex, OnceCell};
#[path = "http_browser.rs"]
mod browser;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct BrowserBinding {
    environment: String,
    application: String,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CliActor {
    environment: String,
    principal: String,
    application: String,
}

#[derive(Clone)]
pub(crate) struct HttpAuthentication {
    authority: Authority,
    origin: String,
    host: String,
    public: bool,
    browser: Option<BrowserBinding>,
    cli_key: Option<[u8; 32]>,
    profiles: BTreeMap<String, CliActor>,
    cached: Arc<Mutex<BTreeMap<String, VerifiedSession>>>,
    origin_admitted: Arc<OnceCell<()>>,
}
fn document_navigation(headers: &HeaderMap, method: &Method) -> bool {
    [Method::GET, Method::HEAD].contains(method)
        && headers
            .get("sec-fetch-mode")
            .is_some_and(|value| value == "navigate")
        && headers
            .get("sec-fetch-dest")
            .is_some_and(|value| value == "document")
}
impl HttpAuthentication {
    pub(crate) fn is_public(&self) -> bool {
        self.public
    }
    pub(crate) fn configured(db: Database, listen_host: &str) -> Result<Self> {
        let configured = std::env::var("MORIA_NATIVE_PUBLIC_ORIGIN").ok();
        let url = reqwest::Url::parse(
            &configured
                .clone()
                .unwrap_or_else(|| format!("http://{listen_host}")),
        )?;
        ensure!(
            url.path() == "/"
                && url.query().is_none()
                && url.fragment().is_none()
                && url.username().is_empty()
                && url.password().is_none()
                && (configured.is_none() || url.scheme() == "https"),
            "Invalid native public origin"
        );
        let origin = url.origin().ascii_serialization();
        let browser = std::env::var("MORIA_NATIVE_BROWSER_BINDING")
            .ok()
            .map(|s| serde_json::from_str::<BrowserBinding>(&s))
            .transpose()?;
        let key = std::env::var("MORIA_NATIVE_CLI_KEY").ok();
        ensure!(
            key.as_ref()
                .is_none_or(|v| v.len() >= 32 && v.len() <= 1024),
            "Invalid local CLI key"
        );
        let profiles: BTreeMap<String, CliActor> = std::env::var("MORIA_NATIVE_CLI_PROFILES")
            .ok()
            .map(|s| serde_json::from_str(&s))
            .transpose()?
            .unwrap_or_default();
        ensure!(
            profiles.len() <= 16
                && (key.is_some() == !profiles.is_empty())
                && (configured.is_none() || key.is_none()),
            "Invalid local CLI profiles"
        );
        ensure!(
            profiles.keys().all(|p| !p.is_empty()
                && p.len() <= 64
                && p.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')),
            "Invalid local CLI profiles"
        );
        Ok(Self {
            authority: Authority { db },
            origin,
            host: url
                .host_str()
                .map(|h| {
                    if h.contains(':') {
                        format!("[{h}]")
                    } else {
                        h.into()
                    }
                })
                .map(|h| url.port().map_or(h.clone(), |p| format!("{h}:{p}")))
                .ok_or_else(|| anyhow::anyhow!("Invalid native public origin"))?,
            public: configured.is_some(),
            browser,
            cli_key: key.map(|s| Sha256::digest(s.as_bytes()).into()),
            profiles,
            cached: Arc::new(Mutex::new(BTreeMap::new())),
            origin_admitted: Arc::new(OnceCell::new()),
        })
    }
    pub(crate) async fn admit_browser_origin(&self) -> Result<()> {
        let Some(binding) = &self.browser else {
            return Ok(());
        };
        self.origin_admitted
            .get_or_try_init(|| async {
                self.validate_browser_configuration()?;
                let issuer = std::env::var("MORIA_NATIVE_AUTH_ORIGIN").ok();
                let client = self.authority.db.get().await?;
                for origin in std::iter::once(self.origin.as_str()).chain(issuer.as_deref()) {
                    client
                        .query_one(
                            "SELECT moria.native_browser_origin_admit($1,$2,$3)",
                            &[&origin, &binding.environment, &binding.application],
                        )
                        .await?;
                }
                Ok::<(), anyhow::Error>(())
            })
            .await?;
        Ok(())
    }
    pub(crate) async fn authenticate(
        &self,
        remote: SocketAddr,
        headers: &HeaderMap,
        method: &Method,
    ) -> Result<VerifiedSession> {
        self.check_transport(remote, headers)?;
        self.admit_browser_origin().await?;
        let origin = headers
            .get(header::ORIGIN)
            .map(|v| v.to_str())
            .transpose()?;
        self.authenticate_checked(headers, method, origin).await
    }
    pub(crate) fn check_transport(&self, remote: SocketAddr, headers: &HeaderMap) -> Result<()> {
        ensure!(
            remote.ip().is_loopback()
                && headers.get(header::HOST).and_then(|v| v.to_str().ok())
                    == Some(self.host.as_str()),
            "FORBIDDEN"
        );
        if self.public {
            ensure!(
                headers
                    .get("x-forwarded-proto")
                    .and_then(|v| v.to_str().ok())
                    == Some("https"),
                "FORBIDDEN"
            );
        }
        let origin = headers
            .get(header::ORIGIN)
            .map(|v| v.to_str())
            .transpose()?;
        ensure!(origin.is_none_or(|v| v == self.origin), "FORBIDDEN");
        Ok(())
    }
    async fn authenticate_checked(
        &self,
        headers: &HeaderMap,
        method: &Method,
        origin: Option<&str>,
    ) -> Result<VerifiedSession> {
        if let Some(binding) = &self.browser {
            let name = format!(
                "moria_{}",
                &format!(
                    "{:x}",
                    Sha256::digest(format!("{}/{}", self.origin, binding.application).as_bytes())
                )[..16]
            );
            let cookie = headers
                .get(header::COOKIE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let values = cookie
                .split(';')
                .map(str::trim)
                .filter_map(|v| v.strip_prefix(&format!("{name}=")))
                .collect::<Vec<_>>();
            if !values.is_empty() {
                ensure!(
                    values.len() == 1
                        && !headers.contains_key(header::AUTHORIZATION)
                        && !headers.contains_key("x-moria-key"),
                    "UNAUTHENTICATED"
                );
                ensure!(
                    [Method::GET, Method::HEAD].contains(method)
                        || origin == Some(self.origin.as_str()),
                    "FORBIDDEN"
                );
                ensure!(
                    headers
                        .get("sec-fetch-site")
                        .is_none_or(|v| v == "same-origin" || v == "none")
                        || document_navigation(headers, method),
                    "FORBIDDEN"
                );
                let session = self.authority.authenticate(values[0]).await?;
                ensure!(
                    session.environment() == binding.environment
                        && session.application() == binding.application,
                    "FORBIDDEN"
                );
                let client = self.authority.db.get().await?;
                ensure!(client.query_opt("SELECT 1 FROM moria.browser_sessions WHERE token_hash=$1 AND environment=$2 AND application=$3 AND NOT disabled AND expires_at>clock_timestamp()",&[&session.session_hash(),&binding.environment,&binding.application]).await?.is_some(),"UNAUTHENTICATED");
                Authority::current(&**client, &session).await?;
                return Ok(session);
            }
        }
        ensure!(
            origin.is_none()
                && !headers.contains_key(header::COOKIE)
                && !headers.contains_key("sec-fetch-site"),
            "FORBIDDEN"
        );
        if let Some(value) = headers.get("x-moria-key") {
            ensure!(
                !self.public && !headers.contains_key(header::AUTHORIZATION),
                "FORBIDDEN"
            );
            let expected = self
                .cli_key
                .ok_or_else(|| anyhow::anyhow!("UNAUTHENTICATED"))?;
            let actual: [u8; 32] = Sha256::digest(value.as_bytes()).into();
            ensure!(
                actual
                    .iter()
                    .zip(expected)
                    .fold(0u8, |v, (a, b)| v | (a ^ b))
                    == 0,
                "UNAUTHENTICATED"
            );
            let profile = headers
                .get("x-moria-profile")
                .map(|v| v.to_str())
                .transpose()?
                .unwrap_or("operator");
            return self.cli_session(profile).await;
        }
        let token = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or_else(|| anyhow::anyhow!("UNAUTHENTICATED"))?;
        let session = self.authority.authenticate(token).await?;
        let client = self.authority.db.get().await?;
        Authority::current(&**client, &session).await?;
        Ok(session)
    }
    fn cli_approval(&self) -> Result<Option<(String, String, serde_json::Value)>> {
        let Some(key) = self.cli_key else {
            return Ok(None);
        };
        let profiles = serde_json::to_value(&self.profiles)?;
        Ok(Some((
            key.iter().map(|byte| format!("{byte:02x}")).collect(),
            crate::canonical::fingerprint(&profiles)?
                .trim_start_matches("sha256:")
                .into(),
            profiles,
        )))
    }
    pub(crate) async fn initialize_cli(&self) -> Result<()> {
        self.admit_browser_origin().await?;
        self.register_cli(false).await
    }
    pub(crate) async fn approve_cli(&self) -> Result<()> {
        self.register_cli(true).await
    }
    async fn register_cli(&self, explicit: bool) -> Result<()> {
        let Some((key, profile_hash, profiles)) = self.cli_approval()? else {
            ensure!(!explicit, "CLI_APPROVAL_INVALID");
            return Ok(());
        };
        let mut client = self.authority.db.get().await?;
        let tx = client.transaction().await?;
        let row=tx.query_one("SELECT incarnation,EXISTS(SELECT 1 FROM moria.runtime_restores) FROM moria.indexing_instance FOR SHARE",&[]).await?;
        let incarnation: uuid::Uuid = row.get(0);
        let restored: bool = row.get(1);
        if restored && !explicit {
            tx.rollback().await?;
            return Ok(());
        }
        if restored {
            ensure!(!tx.query_one("SELECT EXISTS(SELECT 1 FROM moria.native_cli_profile_approvals WHERE key_hash=$1 AND incarnation<>$2)",&[&key,&incarnation]).await?.get::<_,bool>(0),"CLI_KEY_REUSE_FORBIDDEN");
        }
        for actor in self.profiles.values() {
            ensure!(tx.query_opt("SELECT 1 FROM moria.principals p JOIN moria.application_scopes a ON a.environment=p.environment JOIN moria.permissions g ON g.environment=p.environment AND g.principal=p.principal AND g.application=a.application WHERE p.environment=$1 AND p.principal=$2 AND a.application=$3 AND p.enabled AND p.kind='workload' AND a.enabled",&[&actor.environment,&actor.principal,&actor.application]).await?.is_some(),"CLI_APPROVAL_INVALID");
        }
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended('moria/cli-key/'||$1,0))",
            &[&key],
        )
        .await?;
        if explicit {
            tx.execute("INSERT INTO moria.native_cli_profile_approvals(key_hash,profile_hash,incarnation,profiles) VALUES($1,$2,$3,$4) ON CONFLICT(key_hash,profile_hash,incarnation) DO UPDATE SET enabled=true WHERE native_cli_profile_approvals.profiles=EXCLUDED.profiles",&[&key,&profile_hash,&incarnation,&profiles]).await?;
        } else {
            tx.execute("INSERT INTO moria.native_cli_profile_approvals(key_hash,profile_hash,incarnation,profiles) SELECT $1,$2,$3,$4 WHERE NOT EXISTS(SELECT 1 FROM moria.native_cli_profile_approvals WHERE key_hash=$1) ON CONFLICT DO NOTHING",&[&key,&profile_hash,&incarnation,&profiles]).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn cli_session(&self, profile: &str) -> Result<VerifiedSession> {
        let actor = self
            .profiles
            .get(profile)
            .ok_or_else(|| anyhow::anyhow!("UNAUTHENTICATED"))?;
        let mut cache = self.cached.lock().await;
        let mut client = self.authority.db.get().await?;
        let (key_hash, profile_hash, _) = self
            .cli_approval()?
            .ok_or_else(|| anyhow::anyhow!("CLI_APPROVAL_REQUIRED"))?;
        ensure!(client.query_opt("SELECT 1 FROM moria.native_cli_profile_approvals a JOIN moria.indexing_instance i ON i.incarnation=a.incarnation WHERE key_hash=$1 AND profile_hash=$2 AND enabled",&[&key_hash,&profile_hash]).await?.is_some(),"CLI_APPROVAL_REQUIRED");
        if let Some(session) = cache.get(profile) {
            let row=client.query_opt("SELECT disabled,expires_at>clock_timestamp() FROM moria.workload_sessions WHERE token_hash=$1",&[&session.session_hash()]).await?.ok_or_else(||anyhow::anyhow!("UNAUTHENTICATED"))?;
            ensure!(!row.get::<_, bool>(0), "UNAUTHENTICATED");
            if row.get::<_, bool>(1) {
                Authority::current(&**client, session).await?;
                return Ok(session.clone());
            }
        }
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT 1 FROM moria.environments WHERE id=$1 FOR SHARE",
            &[&actor.environment],
        )
        .await?;
        ensure!(tx.query_opt("SELECT 1 FROM moria.principals WHERE environment=$1 AND principal=$2 AND kind='workload'",&[&actor.environment,&actor.principal]).await?.is_some(),"UNAUTHENTICATED");
        let mut random = [0u8; 32];
        getrandom::fill(&mut random)?;
        let token = URL_SAFE_NO_PAD.encode(random);
        let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
        tx.execute("INSERT INTO moria.workload_sessions(token_hash,environment,principal,application,expires_at) VALUES($1,$2,$3,$4,clock_timestamp()+interval '15 minutes')",&[&hash,&actor.environment,&actor.principal,&actor.application]).await?;
        let approval=tx.query_one("SELECT a.incarnation,a.generation FROM moria.native_cli_profile_approvals a JOIN moria.indexing_instance i ON i.incarnation=a.incarnation WHERE a.key_hash=$1 AND a.profile_hash=$2 AND a.enabled FOR SHARE OF a",&[&key_hash,&profile_hash]).await?;
        tx.execute("INSERT INTO moria.native_cli_sessions(token_hash,key_hash,profile_hash,incarnation,profile_name,approval_generation) VALUES($1,$2,$3,$4,$5,$6)",&[&hash,&key_hash,&profile_hash,&approval.get::<_,uuid::Uuid>(0),&profile,&approval.get::<_,i64>(1)]).await?;
        let session = Authority::stored_session_in(
            tx.client(),
            &hash,
            &actor.environment,
            &actor.principal,
            &actor.application,
        )
        .await?;
        let state = Authority::current(tx.client(), &session).await?;
        tx.execute("INSERT INTO moria.security_audit(environment,actor,operation,target,epoch) VALUES($1,$2,'mintWorkloadSession',$2,$3)",&[&actor.environment,&actor.principal,&state.security_epoch.parse::<i64>()?]).await?;
        tx.commit().await?;
        cache.insert(profile.into(), session.clone());
        Ok(session)
    }
}

#[cfg(test)]
mod navigation_tests {
    use super::*;
    #[test]
    fn cookie_navigation_is_top_level_safe_method_only() {
        let mut headers = HeaderMap::new();
        headers.insert("sec-fetch-mode", "navigate".parse().unwrap());
        headers.insert("sec-fetch-dest", "document".parse().unwrap());
        assert!(document_navigation(&headers, &Method::GET));
        assert!(document_navigation(&headers, &Method::HEAD));
        assert!(!document_navigation(&headers, &Method::POST));
        headers.insert("sec-fetch-dest", "iframe".parse().unwrap());
        assert!(!document_navigation(&headers, &Method::GET));
        headers.insert("sec-fetch-dest", "document".parse().unwrap());
        headers.insert("sec-fetch-mode", "cors".parse().unwrap());
        assert!(!document_navigation(&headers, &Method::GET));
    }
}
