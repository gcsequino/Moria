use anyhow::{Result, ensure};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use moria_runtime::{
    authority::Authority,
    database::Database,
    execution::Lease,
    sandbox::{
        ConsumedMarking, HttpCall, HttpResponse, PlatformBroker, SandboxContext, SandboxSupervisor,
    },
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::watch;

// This fixture broker serves only the real native serving fixture's Record reads.
struct FixtureBroker {
    db: Database,
    endpoint: String,
    token: String,
    client: reqwest::Client,
}
#[async_trait]
impl PlatformBroker for FixtureBroker {
    async fn start(&self, context: &SandboxContext) -> Result<()> {
        moria_runtime::platform::NativePlatformBroker::new(self.db.clone())
            .start(context)
            .await
    }
    async fn http(&self, context: &SandboxContext, call: HttpCall) -> Result<HttpResponse> {
        context.check().await?;
        let session = Authority {
            db: self.db.clone(),
        }
        .authenticate(&self.token)
        .await?;
        ensure!(
            session.environment() == context.environment()
                && session.principal() == context.principal()
                && session.application() == context.application()
                && context.original_session_hash() == Some(session.session_hash()),
            "BROKER_ACTOR_MISMATCH"
        );
        ensure!(
            context.effect() == "readOnly"
                && call.method == "GET"
                && call.body.is_none()
                && (call.path.starts_with("/api/v1/objects/records/")
                    || call
                        .path
                        .starts_with("/api/v1/objects/00000000-0000-4000-8000-000000000004/"))
                && !call.path.contains("..")
                && !call.path.contains('#')
                && !call.path.contains(char::from(92)),
            "BROKER_OPERATION_DENIED"
        );
        let response = self
            .client
            .get(format!("{}{}", self.endpoint, call.path))
            .bearer_auth(&self.token)
            .send()
            .await?;
        let status = response.status().as_u16();
        let body = response.bytes().await?;
        ensure!(body.len() <= 32768, "BROKER_RESPONSE_LIMIT");
        if (200..300).contains(&status) {
            context.record_consumed_markings(&[ConsumedMarking {
                environment: context.environment().into(),
                marking: "private".into(),
            }])?;
        }
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.execute("INSERT INTO public.sandbox_protected_callbacks(execution,call_id,body_hash,markings) VALUES($1,$2,$3,$4)",&[&context.lease().execution,&call.id,&moria_runtime::canonical::fingerprint(&json!({"status":status,"body":STANDARD.encode(&body)}))?,&serde_json::to_value(context.consumed_markings())?]).await?;
        context
            .complete_observation_in(tx.client(), &call.id)
            .await?;
        tx.commit().await?;
        Ok(HttpResponse {
            status,
            headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
            body: STANDARD.encode(body),
        })
    }
    async fn log(
        &self,
        context: &SandboxContext,
        message: &str,
        consumed: &[ConsumedMarking],
    ) -> Result<()> {
        context.check().await?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.execute("INSERT INTO public.sandbox_protected_logs(execution,message,markings) VALUES($1,$2,$3)", &[&context.lease().execution,&message,&serde_json::to_value(consumed)?]).await?;
        context.complete_current_observation_in(tx.client()).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn finish(&self, context: &SandboxContext, result: &Value) -> Result<()> {
        context.check().await?;
        ensure!(
            result
                .as_object()
                .is_some_and(|o| o.len() == 1 && o.get("value").is_some_and(Value::is_number)),
            "BROKER_OUTPUT_INVALID"
        );
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        context.complete_current_observation_in(tx.client()).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[tokio::test]
#[ignore = "invoked by the real Docker/PostgreSQL integration fixture"]
async fn native_sandbox_driver() {
    let db = Database::configured().unwrap();
    db.schema_lease().await.unwrap();
    if let Ok(owner) = std::env::var("MORIA_SANDBOX_TEST_CLAIM_OWNER") {
        let broker = Arc::new(moria_runtime::platform::NativePlatformBroker::new(
            db.clone(),
        ));
        let supervisor = SandboxSupervisor::new(
            db,
            std::env::var("MORIA_SANDBOX_TEST_HOST")
                .unwrap()
                .parse()
                .unwrap(),
            broker,
        )
        .unwrap();
        let claim = supervisor.claim(owner.parse().unwrap()).await.unwrap();
        println!("SANDBOX_RESULT={}", serde_json::to_value(claim).unwrap());
        return;
    }
    let broker = Arc::new(FixtureBroker {
        db: db.clone(),
        endpoint: std::env::var("MORIA_SANDBOX_TEST_ENDPOINT").unwrap(),
        token: std::env::var("MORIA_SANDBOX_TEST_TOKEN").unwrap(),
        client: reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
    });
    let supervisor = SandboxSupervisor::new(
        db,
        std::env::var("MORIA_SANDBOX_TEST_HOST")
            .unwrap()
            .parse()
            .unwrap(),
        broker,
    )
    .unwrap();
    if std::env::var("MORIA_SANDBOX_TEST_CLEANUP").is_ok() {
        println!(
            "SANDBOX_RESULT={}",
            json!({"cleaned":supervisor.cleanup_abandoned().await.unwrap()})
        );
        return;
    }
    let lease: Lease =
        serde_json::from_str(&std::env::var("MORIA_SANDBOX_TEST_LEASE").unwrap()).unwrap();
    let (tx, rx) = watch::channel(false);
    let signals = tokio::spawn(async move {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .unwrap()
            .recv()
            .await;
        let _ = tx.send(true);
    });
    let result = supervisor.execute(lease, rx).await;
    signals.abort();
    let output = match result {
        Ok(result) => json!({"ok":true,"output":result}),
        Err(error) => json!({"ok":false,"code":moria_runtime::sandbox::error_code(&error)}),
    };
    println!("SANDBOX_RESULT={output}");
}
include!("../native-common/artifact.rs");
