use super::*;
use crate::serving::Admission;
use axum::{
    body::{Body, to_bytes},
    http::{Request, Response, StatusCode},
};
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Provider {
    id: String,
    label: String,
}
fn opaque() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| anyhow::anyhow!("INTERNAL"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}
fn token_hash(token: &str) -> String {
    format!("{:x}", Sha256::digest(token.as_bytes()))
}
fn cookie_name(origin: &str, application: &str) -> String {
    format!(
        "moria_{}",
        &token_hash(&format!("{origin}/{application}"))[..16]
    )
}

const SIGN_IN_HTML: &str = r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Sign in to Moria</title><script src="/api/auth/bootstrap.js" defer></script></head><body><main><h1>Sign in to Moria</h1><p id="state" role="status">Checking your session…</p><div id="providers"></div><a href="/">Open application</a></main></body></html>"#;
const SIGN_IN_SCRIPT: &str = r#"'use strict';
(()=>{
 const state=document.querySelector('#state'),providers=document.querySelector('#providers');
 const request=async(path,input)=>{const response=await fetch(path,{method:input===undefined?'GET':'POST',credentials:'same-origin',redirect:'error',headers:input===undefined?{}:{'content-type':'application/json'},body:input===undefined?undefined:JSON.stringify(input)});if(!response.ok)throw new Error('Sign-in is unavailable. Request a new recovery link or try your provider again.');return response.json();};
 let pending=null,running=false,revision=0;
 const drain=async()=>{
  if(running)return;
  running=true;
  try{
   while(pending){
    const attempt=pending;pending=null;
    state.textContent='Checking your session…';providers.replaceChildren();
    try{
     if(attempt.fragment){if(!/^recovery=[A-Za-z0-9_-]{43}$/.test(attempt.fragment))throw new Error('This recovery link is invalid.');await request('/api/auth/recover',{ticket:attempt.fragment.slice(9)});}
     if(attempt.revision!==revision)continue;
     const session=await request('/api/auth/session');
     if(attempt.revision!==revision)continue;
     if(session.user){location.replace('/');return;}
     state.textContent=session.providers.length?'Choose your sign-in provider.':'No sign-in provider is configured. Ask the local operator for a recovery link.';
     for(const provider of session.providers){const button=document.createElement('button');button.type='button';button.textContent='Continue with '+provider.label;button.addEventListener('click',async()=>{button.disabled=true;try{const value=await request('/api/auth/login',{provider:provider.id});if(attempt.revision!==revision)return;const destination=new URL(value.url,location.origin);if(!['http:','https:'].includes(destination.protocol))throw new Error('Invalid sign-in destination.');location.assign(destination.href);}catch(error){if(attempt.revision===revision){state.textContent=error.message;button.disabled=false;}}});providers.append(button);}
    }catch(error){if(attempt.revision===revision)state.textContent=error.message;}
   }
  }finally{running=false;}
 };
 const consume=()=>{
  const fragment=location.hash.slice(1);
  history.replaceState(null,'',location.pathname);
  pending={fragment,revision:++revision};
  void drain();
 };
 addEventListener('hashchange',consume);
 consume();
})();"#;

fn sign_in_response(script: bool) -> Result<Response<Body>> {
    Ok(Response::builder().status(StatusCode::OK)
  .header(header::CONTENT_TYPE,if script {"text/javascript; charset=utf-8"} else {"text/html; charset=utf-8"})
  .header(header::CACHE_CONTROL,"no-store")
  .header(header::CONTENT_SECURITY_POLICY,"default-src 'none'; script-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
  .header(header::X_CONTENT_TYPE_OPTIONS,"nosniff").header(header::REFERRER_POLICY,"no-referrer")
  .body(Body::from(if script {SIGN_IN_SCRIPT} else {SIGN_IN_HTML}))?)
}

impl HttpAuthentication {
    fn browser_configuration(&self) -> Result<(Vec<Provider>, Option<String>, Option<String>)> {
        let text = std::env::var("MORIA_NATIVE_AUTH_PROVIDERS").unwrap_or_else(|_| "[]".into());
        ensure!(text.len() <= 65536, "INPUT_INVALID");
        let providers: Vec<Provider> = serde_json::from_str(&text)?;
        let names = providers
            .iter()
            .map(|p| p.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        ensure!(
            providers.len() <= 32
                && names.len() == providers.len()
                && providers.iter().all(|p| !p.id.is_empty()
                    && p.id.len() <= 100
                    && p.id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
                    && !p.label.is_empty()
                    && p.label.len() <= 160),
            "INPUT_INVALID"
        );
        let issuer = std::env::var("MORIA_NATIVE_AUTH_ORIGIN").ok();
        if let Some(issuer) = &issuer {
            let actual = reqwest::Url::parse(issuer)?;
            let target = reqwest::Url::parse(&self.origin)?;
            ensure!(
                actual.origin().ascii_serialization() == *issuer
                    && actual.host_str() == target.host_str()
                    && actual.scheme() == target.scheme()
                    && actual.username().is_empty()
                    && actual.password().is_none(),
                "INPUT_INVALID"
            );
        }
        let recovery = std::env::var("MORIA_NATIVE_RECOVERY_PRINCIPAL").ok();
        ensure!(
            recovery
                .as_ref()
                .is_none_or(|value| !self.public && !value.is_empty() && value.len() <= 180),
            "INPUT_INVALID"
        );
        Ok((providers, issuer, recovery))
    }
    pub(crate) fn validate_browser_configuration(&self) -> Result<()> {
        self.browser_configuration().map(|_| ())
    }
    pub(crate) async fn browser_handle(
        &self,
        remote: SocketAddr,
        request: Request<Body>,
    ) -> Result<Response<Body>> {
        self.check_transport(remote, request.headers())?;
        self.admit_browser_origin().await?;
        let binding = self
            .browser
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("NOT_FOUND"))?;
        ensure!(
            request.uri().query().is_none()
                && [Method::GET, Method::POST].contains(request.method()),
            "INPUT_INVALID"
        );
        ensure!(
            request
                .headers()
                .get("sec-fetch-site")
                .is_none_or(|value| value == "same-origin" || value == "none")
                || document_navigation(request.headers(), request.method()),
            "FORBIDDEN"
        );
        let path = request.uri().path().to_owned();
        let method = request.method().clone();
        if method == Method::GET
            && ["/api/auth/bootstrap", "/api/auth/bootstrap.js"].contains(&path.as_str())
        {
            ensure!(
                request
                    .headers()
                    .get(header::CONTENT_LENGTH)
                    .is_none_or(|value| value == "0"),
                "INPUT_INVALID"
            );
            ensure!(
                to_bytes(request.into_body(), 0)
                    .await
                    .map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
                    .is_empty(),
                "INPUT_INVALID"
            );
            return sign_in_response(path.ends_with(".js"));
        }
        let headers = request.headers().clone();
        let ticket_route = path == "/api/auth/recovery-ticket";
        if method == Method::POST && !ticket_route {
            ensure!(
                headers
                    .get(header::ORIGIN)
                    .is_some_and(|value| value == self.origin.as_str()),
                "FORBIDDEN"
            );
        }
        ensure!(
            headers
                .get(header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<usize>().ok())
                .is_none_or(|value| value <= 65536),
            "BODY_TOO_LARGE"
        );
        let bytes = to_bytes(request.into_body(), 65536)
            .await
            .map_err(|_| anyhow::anyhow!("BODY_TOO_LARGE"))?;
        let input: Value = if method == Method::GET {
            ensure!(bytes.is_empty(), "INPUT_INVALID");
            json!({})
        } else if bytes.is_empty() && (ticket_route || path == "/api/auth/logout") {
            json!({})
        } else {
            ensure!(
                headers
                    .get(header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.split(';').next())
                    .is_some_and(|value| value.eq_ignore_ascii_case("application/json")),
                "CONTENT_TYPE"
            );
            serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("INPUT_INVALID"))?
        };
        let (providers, issuer, recovery) = self.browser_configuration()?;
        let name = cookie_name(&self.origin, &binding.application);
        let cookies = headers
            .get(header::COOKIE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .split(';')
            .map(str::trim)
            .filter_map(|value| value.strip_prefix(&format!("{name}=")))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        ensure!(cookies.len() <= 1, "UNAUTHENTICATED");
        let mut set_cookie = None;
        let result = if path == "/api/auth/session" && method == Method::GET {
            let client = self.authority.db.get().await?;
            let enabled=client.query("SELECT provider FROM moria.auth_providers p JOIN moria.application_scopes a USING(environment,application) WHERE p.environment=$1 AND p.application=$2 AND p.enabled AND a.enabled",&[&binding.environment,&binding.application]).await?.into_iter().map(|row|row.get::<_,String>(0)).collect::<std::collections::BTreeSet<_>>();
            drop(client);
            let mut refresh_required = false;
            let user = if cookies.is_empty() {
                Value::Null
            } else {
                match self.authenticate(remote, &headers, &method).await {
                    Ok(session) => {
                        let admission =
                            Admission::begin(&self.authority.db, &session, false).await?;
                        let result=async {let client=admission.client();let state=Authority::current(client,&session).await?;
                            let row=client.query_opt("SELECT display_name,provider,to_char(expires_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') FROM moria.browser_sessions WHERE token_hash=$1 AND environment=$2 AND application=$3 AND NOT disabled AND expires_at>clock_timestamp()",&[&session.session_hash(),&binding.environment,&binding.application]).await?.ok_or_else(||anyhow::anyhow!("UNAUTHENTICATED"))?;
                            Authority::finish(client,&session,&state).await?;
                            Ok(json!({"principal":session.principal(),"name":row.get::<_,String>(0),"recovery":row.get::<_,Option<String>>(1).is_none(),"expiresAt":row.get::<_,String>(2)}))}.await;
                        admission.complete(result).await?
                    }
                    Err(error)
                        if ["UNAUTHENTICATED", "FORBIDDEN", "IDENTITY_STALE"]
                            .contains(&crate::serving::error_code(&error)) =>
                    {
                        if issuer.is_some() {
                            let client = self.authority.db.get().await?;
                            refresh_required=client.query_opt("SELECT 1 FROM moria.browser_sessions s JOIN moria.principals p USING(environment,principal) JOIN moria.application_scopes a USING(environment,application) JOIN moria.auth_providers provider ON provider.environment=s.environment AND provider.application=s.application AND provider.provider=s.provider AND provider.revision=s.revision AND provider.enabled LEFT JOIN moria.instance_principal_bindings binding ON binding.environment=p.environment AND binding.principal=p.principal LEFT JOIN moria.instance_principals identity ON identity.id=binding.instance_id WHERE s.token_hash=$1 AND s.environment=$2 AND s.application=$3 AND NOT s.disabled AND s.expires_at>clock_timestamp() AND p.enabled AND p.kind='human' AND a.enabled AND (binding.instance_id IS NULL OR identity.enabled)",&[&token_hash(&cookies[0]),&binding.environment,&binding.application]).await?.is_some();
                        }
                        if !refresh_required {
                            set_cookie = Some(String::new());
                        }
                        Value::Null
                    }
                    Err(error) => return Err(error),
                }
            };
            let mut result = json!({"user":user,"providers":providers.iter().filter(|provider|enabled.contains(&provider.id)).map(|provider|json!({"id":provider.id,"label":provider.label})).collect::<Vec<_>>()});
            if let Some(origin) = &issuer {
                result["refresh"] = json!({"url":format!("{origin}/api/auth/refresh"),"origin":origin,"required":refresh_required});
            }
            result
        } else if path == "/api/auth/login" && method == Method::POST {
            ensure!(
                input
                    .as_object()
                    .is_some_and(|value| value.len() == 1 && value.contains_key("provider")),
                "INPUT_INVALID"
            );
            let provider = input["provider"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))?;
            ensure!(
                providers.iter().any(|value| value.id == provider),
                "FORBIDDEN"
            );
            let client = self.authority.db.get().await?;
            ensure!(client.query_opt("SELECT 1 FROM moria.auth_providers p JOIN moria.application_scopes a USING(environment,application) WHERE p.environment=$1 AND p.application=$2 AND p.provider=$3 AND p.enabled AND a.enabled",&[&binding.environment,&binding.application,&provider]).await?.is_some(),"FORBIDDEN");
            let mut location = reqwest::Url::parse(&format!(
                "{}/api/auth/login",
                issuer.ok_or_else(|| anyhow::anyhow!("APP_UNAVAILABLE"))?
            ))?;
            location.query_pairs_mut().append_pair("provider", provider);
            json!({"url":location.as_str()})
        } else if path == "/api/auth/logout" && method == Method::POST {
            ensure!(
                input.as_object().is_some_and(|value| value.is_empty()),
                "INPUT_INVALID"
            );
            let session = self.authenticate(remote, &headers, &method).await?;
            let admission = Admission::begin(&self.authority.db, &session, true).await?;
            let outcome=async {let client=admission.client();Authority::current(client,&session).await?;
                client.execute("UPDATE moria.browser_sessions SET disabled=true,access_token=NULL WHERE token_hash=$1 AND environment=$2 AND application=$3",&[&session.session_hash(),&binding.environment,&binding.application]).await?;
                browser_audit(client,&binding.environment,&binding.application,session.principal(),"logout").await
            }.await;
            admission.complete(outcome).await?;
            set_cookie = Some(String::new());
            json!({"signedOut":true})
        } else if ticket_route && method == Method::POST {
            ensure!(
                !self.public
                    && input.as_object().is_some_and(|value| value.is_empty())
                    && !headers.contains_key(header::ORIGIN)
                    && !headers.contains_key("sec-fetch-site")
                    && cookies.is_empty(),
                "FORBIDDEN"
            );
            let principal = recovery
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("FORBIDDEN"))?;
            let mut authentication = headers.clone();
            if let Some(value) = headers.get("x-moria-app-key") {
                ensure!(!headers.contains_key("x-moria-key"), "UNAUTHENTICATED");
                authentication.insert("x-moria-key", value.clone());
            }
            let key = authentication
                .get("x-moria-key")
                .ok_or_else(|| anyhow::anyhow!("UNAUTHENTICATED"))?;
            let digest: [u8; 32] = Sha256::digest(key.as_bytes()).into();
            ensure!(
                self.cli_key.as_ref().is_some_and(|expected| expected
                    .iter()
                    .zip(digest)
                    .fold(0u8, |result, (a, b)| result | (*a ^ b))
                    == 0),
                "UNAUTHENTICATED"
            );
            let session = self.authenticate(remote, &authentication, &method).await?;
            ensure!(
                session.environment() == binding.environment
                    && session.application() == binding.application
                    && session.principal() == principal,
                "FORBIDDEN"
            );
            let ticket = opaque()?;
            let admission = Admission::begin(&self.authority.db, &session, true).await?;
            let outcome=async {let client=admission.client();let state=Authority::current(client,&session).await?;
                client.execute("DELETE FROM moria.auth_flows WHERE environment=$1 AND application=$2 AND provider IS NULL AND expires_at<=clock_timestamp()",&[&binding.environment,&binding.application]).await?;
                ensure!(client.query_one("SELECT count(*)<32 FROM moria.auth_flows WHERE environment=$1 AND application=$2 AND provider IS NULL",&[&binding.environment,&binding.application]).await?.get::<_,bool>(0),"RESOURCE_LIMIT");
                client.execute("INSERT INTO moria.auth_flows(state_hash,environment,application,expires_at) VALUES($1,$2,$3,clock_timestamp()+interval '5 minutes')",&[&token_hash(&ticket),&binding.environment,&binding.application]).await?;
                Authority::finish(client,&session,&state).await
            }.await;
            admission.complete(outcome).await?;
            json!({"url":format!("{}/api/auth/bootstrap#recovery={ticket}",self.origin),"expiresIn":300})
        } else if path == "/api/auth/recover" && method == Method::POST {
            ensure!(
                !self.public
                    && input
                        .as_object()
                        .is_some_and(|value| value.len() == 1 && value.contains_key("ticket")),
                "FORBIDDEN"
            );
            let ticket = input["ticket"]
                .as_str()
                .filter(|value| {
                    value.len() == 43
                        && value
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
                })
                .ok_or_else(|| anyhow::anyhow!("UNAUTHENTICATED"))?;
            let principal = recovery
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("FORBIDDEN"))?;
            let token = opaque()?;
            let admission =
                Admission::begin_environment(&self.authority.db, &binding.environment, true, false)
                    .await?;
            let outcome=async {let client=admission.client();
                ensure!(client.execute("DELETE FROM moria.auth_flows WHERE state_hash=$1 AND environment=$2 AND application=$3 AND provider IS NULL AND expires_at>clock_timestamp() AND moria.native_browser_auth_flow_current(environment,auth_generation)",&[&token_hash(ticket),&binding.environment,&binding.application]).await?==1,"UNAUTHENTICATED");
                ensure!(client.query_opt("SELECT 1 FROM moria.principals WHERE environment=$1 AND principal=$2 AND kind='workload' AND enabled",&[&binding.environment,&principal]).await?.is_some(),"UNAUTHENTICATED");
                let hash=token_hash(&token);
                client.execute("INSERT INTO moria.browser_sessions(token_hash,environment,principal,application,display_name,verified_at,expires_at) VALUES($1,$2,$3,$4,'Local recovery',clock_timestamp(),clock_timestamp()+interval '15 minutes')",&[&hash,&binding.environment,&principal,&binding.application]).await?;
                Authority::stored_session_in(client,&hash,&binding.environment,principal,&binding.application).await?;
                if let Some(old)=cookies.first(){client.execute("UPDATE moria.browser_sessions SET disabled=true,access_token=NULL WHERE token_hash=$1 AND environment=$2 AND application=$3",&[&token_hash(old),&binding.environment,&binding.application]).await?;}
                browser_audit(client,&binding.environment,&binding.application,principal,"localRecoveryLogin").await?;
                Authority::stored_session_in(client,&hash,&binding.environment,principal,&binding.application).await?;
                Ok(())
            }.await;
            admission.complete(outcome).await?;
            set_cookie = Some(token);
            json!({"recovered":true})
        } else {
            anyhow::bail!("NOT_FOUND")
        };
        let mut response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
            .header(header::CACHE_CONTROL, "no-store")
            .header(header::REFERRER_POLICY, "no-referrer")
            .body(Body::from(serde_json::to_vec(&result)?))?;
        if let Some(token) = set_cookie {
            let secure = if self.public { "; Secure" } else { "" };
            response.headers_mut().append(
                header::SET_COOKIE,
                format!(
                    "{name}={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age={}{}",
                    if token.is_empty() { 0 } else { 900 },
                    secure
                )
                .parse()?,
            );
            response.headers_mut().append(
                header::SET_COOKIE,
                format!("{name}=; HttpOnly; SameSite=Lax; Path=/api/; Max-Age=0{secure}")
                    .parse()?,
            );
        }
        Ok(response)
    }
}
async fn browser_audit(
    client: &tokio_postgres::Client,
    environment: &str,
    application: &str,
    principal: &str,
    operation: &str,
) -> Result<()> {
    let epoch:i64=client.query_one("UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=$1 RETURNING security_epoch",&[&environment]).await?.get(0);
    client.execute("INSERT INTO moria.security_audit(environment,actor,operation,target,epoch) VALUES($1,$2,$3,$4,$5)",&[&environment,&principal,&operation,&application,&epoch]).await?;
    Ok(())
}
