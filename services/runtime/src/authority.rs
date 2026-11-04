use crate::{
    authorization::{Identity, ValueMap},
    database::Database,
};
use anyhow::{Result, ensure};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio_postgres::GenericClient;

#[derive(Clone, Debug)]
pub struct VerifiedSession {
    environment: String,
    principal: String,
    application: String,
    token_hash: String,
}
impl VerifiedSession {
    pub fn environment(&self) -> &str {
        &self.environment
    }
    pub fn principal(&self) -> &str {
        &self.principal
    }
    pub fn application(&self) -> &str {
        &self.application
    }
    pub fn session_hash(&self) -> &str {
        &self.token_hash
    }
}
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecuritySnapshot {
    pub environment: String,
    pub principal: String,
    pub application: String,
    pub security_epoch: String,
    pub instance_epoch: String,
    pub attributes: ValueMap,
    pub identity: Identity,
    pub(crate) private_purpose: Option<String>,
}

#[derive(Clone)]
pub struct Authority {
    pub db: Database,
}
impl Authority {
    pub(crate) async fn sandbox_session(
        &self,
        context: &crate::sandbox::SandboxContext,
    ) -> Result<VerifiedSession> {
        let client = self.db.get().await?;
        Self::sandbox_session_in(&client, context).await
    }
    pub(crate) async fn sandbox_session_in(
        client: &tokio_postgres::Client,
        context: &crate::sandbox::SandboxContext,
    ) -> Result<VerifiedSession> {
        context.check_in(client).await?;
        let token_hash = context
            .session_hash()
            .ok_or_else(|| anyhow::anyhow!("UNAUTHENTICATED"))?;
        let session = Self::stored_session_in(
            client,
            token_hash,
            context.environment(),
            context.principal(),
            context.application(),
        )
        .await?;
        context.check_in(client).await?;
        Ok(session)
    }
    pub(crate) async fn stored_session_in(
        client: &tokio_postgres::Client,
        hash: &str,
        environment: &str,
        principal: &str,
        application: &str,
    ) -> Result<VerifiedSession> {
        let session = VerifiedSession {
            environment: environment.into(),
            principal: principal.into(),
            application: application.into(),
            token_hash: hash.into(),
        };
        Self::current(client, &session).await?;
        Ok(session)
    }
    pub async fn authenticate(&self, token: &str) -> Result<VerifiedSession> {
        ensure!(
            token.len() == 43
                && token
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'),
            "UNAUTHENTICATED"
        );
        let token_hash = format!("{:x}", Sha256::digest(token.as_bytes()));
        let client = self.db.get().await?;
        let row=client.query_opt("SELECT environment,principal,application FROM moria.workload_sessions WHERE token_hash=$1 AND NOT disabled AND expires_at>clock_timestamp() UNION ALL SELECT environment,principal,application FROM moria.browser_sessions WHERE token_hash=$1 AND NOT disabled AND expires_at>clock_timestamp()",&[&token_hash]).await?;
        let row = row.ok_or_else(|| anyhow::anyhow!("UNAUTHENTICATED"))?;
        Ok(VerifiedSession {
            environment: row.get(0),
            principal: row.get(1),
            application: row.get(2),
            token_hash,
        })
    }
    pub async fn current<C: GenericClient + Sync>(
        client: &C,
        session: &VerifiedSession,
    ) -> Result<SecuritySnapshot> {
        let value: Value = client
            .query_one(
                "SELECT moria.native_serving_session($1)",
                &[&session.token_hash],
            )
            .await?
            .get(0);
        let snapshot: SecuritySnapshot = serde_json::from_value(value)?;
        ensure!(
            snapshot.environment == session.environment
                && snapshot.principal == session.principal
                && snapshot.application == session.application,
            "AUTHORIZATION_CHANGED"
        );
        Ok(snapshot)
    }
    pub async fn finish<C: GenericClient + Sync>(
        client: &C,
        session: &VerifiedSession,
        snapshot: &SecuritySnapshot,
    ) -> Result<()> {
        let current = Self::current(client, session).await?;
        ensure!(
            current.security_epoch == snapshot.security_epoch
                && current.instance_epoch == snapshot.instance_epoch,
            "AUTHORIZATION_CHANGED"
        );
        Ok(())
    }
    pub(crate) async fn resource_access_for_operation<C: GenericClient + Sync>(
        client: &C,
        session: &VerifiedSession,
        resource: &str,
        operation: &str,
    ) -> Result<Vec<String>> {
        let labels: Value = client
            .query_one(
                "SELECT moria.native_resource_filesystem_access($1,$2,$3)",
                &[&session.token_hash, &resource, &operation],
            )
            .await?
            .get(0);
        Ok(serde_json::from_value(labels)?)
    }
    pub(crate) async fn resource_release_access<C: GenericClient + Sync>(
        client: &C,
        session: &VerifiedSession,
        resource: &str,
        operation: &str,
        release: &str,
    ) -> Result<Vec<String>> {
        let labels: Value = client
            .query_one(
                "SELECT moria.native_resource_release_access($1,$2,$3,$4)",
                &[&session.token_hash, &resource, &operation, &release],
            )
            .await?
            .get(0);
        Ok(serde_json::from_value(labels)?)
    }
    pub async fn resource_access<C: GenericClient + Sync>(
        client: &C,
        session: &VerifiedSession,
        resource: &str,
        operation: &str,
    ) -> Result<Vec<String>> {
        let labels: Value = client
            .query_one(
                "SELECT moria.native_serving_resource_access($1,$2,$3)",
                &[&session.token_hash, &resource, &operation],
            )
            .await?
            .get(0);
        Ok(serde_json::from_value(labels)?)
    }
}
