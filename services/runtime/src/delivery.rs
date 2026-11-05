use crate::database::Database;
use anyhow::{Result, bail, ensure};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{env, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::Transaction;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeliveryLease {
    pub incarnation: Uuid,
    pub delivery: Uuid,
    pub owner: Uuid,
    pub fence: String,
}
impl DeliveryLease {
    fn fence(&self) -> Result<i64> {
        let fence: i64 = self.fence.parse()?;
        ensure!(
            fence > 0 && fence.to_string() == self.fence,
            "Invalid delivery fence"
        );
        Ok(fence)
    }
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryClaim {
    pub lease: DeliveryLease,
    pub endpoint: String,
    pub secret_ref: Option<String>,
    pub body: String,
    pub payload_hash: String,
    pub idempotent: bool,
    pub maximum_attempts: i32,
    pub cycle_attempts: i32,
    pub retry_seconds: i32,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Receipt {
    version: u32,
    delivery_id: Uuid,
    payload_hash: String,
    receipt_id: String,
}

pub struct DeliveryAuthority {
    pub db: Database,
}
impl DeliveryAuthority {
    pub async fn claim(&self, owner: Uuid) -> Result<Option<DeliveryClaim>> {
        self.db
            .get()
            .await?
            .query_one("SELECT moria.runtime_delivery_reap()", &[])
            .await?;
        let classes=self.db.get().await?.query("SELECT name FROM moria.runtime_resource_classes WHERE enabled ORDER BY name LIMIT 64", &[]).await?;
        for class in classes {
            let class_name: String = class.get(0);
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            let Some(class)=tx.query_opt("SELECT * FROM moria.runtime_resource_classes WHERE name=$1 AND enabled FOR UPDATE SKIP LOCKED", &[&class_name]).await? else {continue};
            let active: i64 = tx
                .query_one("SELECT moria.runtime_resource_in_use($1)", &[&class_name])
                .await?
                .get(0);
            if active >= i64::from(class.get::<_, i32>("maximum_tasks")) {
                continue;
            }
            let Some(row)=tx.query_opt("SELECT d.*,x.endpoint,x.secret_ref,x.idempotent,x.maximum_attempts,x.retry_seconds FROM moria.runtime_deliveries d JOIN moria.runtime_delivery_destinations x ON x.id=d.destination WHERE d.resource_class=$1 AND moria.runtime_environment_cell_available(d.environment) AND d.status IN ('pending','retryScheduled') AND d.available_at<=statement_timestamp() AND d.deadline>clock_timestamp() AND moria.runtime_resource_in_use($1,d.environment)<$2 ORDER BY d.available_at,d.created_at FOR UPDATE OF d SKIP LOCKED LIMIT 1", &[&class_name,&i64::from(class.get::<_,i32>("maximum_environment_tasks"))]).await? else {continue};
            let id: Uuid = row.get("id");
            tx.batch_execute("SAVEPOINT delivery_admission").await?;
            if let Err(error) = tx
                .query_one("SELECT moria.runtime_delivery_check($1)", &[&id])
                .await
            {
                let code = error.as_db_error().map(|e| e.message()).unwrap_or("");
                if ![
                    "BINDING_CHANGED",
                    "AUTHORIZATION_CHANGED",
                    "UNAUTHENTICATED",
                    "DESTINATION_DISABLED",
                    "DELIVERY_FORBIDDEN",
                ]
                .contains(&code)
                {
                    return Err(error.into());
                }
                tx.batch_execute("ROLLBACK TO SAVEPOINT delivery_admission")
                    .await?;
                tx.execute("UPDATE moria.runtime_deliveries SET status='failed',error=$2,finished_at=clock_timestamp() WHERE id=$1", &[&id,&code]).await?;
                tx.commit().await?;
                continue;
            }
            let fence:i64=tx.query_one("UPDATE moria.runtime_deliveries SET status='delivering',owner=$2,fence=fence+1,attempts=attempts+1,cycle_attempts=cycle_attempts+1,sent_at=NULL,error=NULL,lease_until=LEAST(deadline,clock_timestamp()+interval '30 seconds') WHERE id=$1 RETURNING fence", &[&id,&owner]).await?.get(0);
            tx.execute("INSERT INTO moria.runtime_delivery_attempts(delivery,fence,owner) VALUES($1,$2,$3)", &[&id,&fence,&owner]).await?;
            let incarnation = tx
                .query_one("SELECT incarnation FROM moria.indexing_instance", &[])
                .await?
                .get(0);
            let claim = DeliveryClaim {
                lease: DeliveryLease {
                    incarnation,
                    delivery: id,
                    owner,
                    fence: fence.to_string(),
                },
                endpoint: row.get("endpoint"),
                secret_ref: row.get("secret_ref"),
                body: row.get("body"),
                payload_hash: row.get("payload_hash"),
                idempotent: row.get("idempotent"),
                maximum_attempts: row.get("maximum_attempts"),
                cycle_attempts: row.get::<_, i32>("cycle_attempts") + 1,
                retry_seconds: row.get("retry_seconds"),
            };
            tx.commit().await?;
            return Ok(Some(claim));
        }
        Ok(None)
    }

    async fn lease(tx: &Transaction<'_>, lease: &DeliveryLease) -> Result<()> {
        ensure!(
            tx.query_opt(
                "SELECT 1 FROM moria.indexing_instance WHERE incarnation=$1",
                &[&lease.incarnation]
            )
            .await?
            .is_some(),
            "Incarnation changed"
        );
        tx.query_one("SELECT 1 FROM moria.runtime_deliveries WHERE id=$1 AND owner=$2 AND fence=$3 AND status='delivering' AND lease_until>clock_timestamp() FOR UPDATE", &[&lease.delivery,&lease.owner,&lease.fence()?]).await?;
        Ok(())
    }
    pub async fn renew(&self, lease: &DeliveryLease) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        Self::lease(&tx, lease).await?;
        tx.query_one(
            "SELECT moria.runtime_delivery_check($1)",
            &[&lease.delivery],
        )
        .await?;
        tx.execute("UPDATE moria.runtime_deliveries SET lease_until=LEAST(deadline,clock_timestamp()+interval '30 seconds') WHERE id=$1", &[&lease.delivery]).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn mark_sent(&self, lease: &DeliveryLease) -> Result<u64> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let environment: String = tx
            .query_one(
                "SELECT environment FROM moria.runtime_deliveries WHERE id=$1",
                &[&lease.delivery],
            )
            .await?
            .get(0);
        tx.query_one(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))",
            &[&format!("moria/environment/{environment}")],
        )
        .await?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended('moria/filesystem',0))",
            &[],
        )
        .await?;
        Self::lease(&tx, lease).await?;
        tx.query_one(
            "SELECT moria.runtime_delivery_check($1)",
            &[&lease.delivery],
        )
        .await?;
        let remaining:i64=tx.query_one("UPDATE moria.runtime_deliveries SET sent_at=clock_timestamp() WHERE id=$1 RETURNING floor(extract(epoch FROM(LEAST(deadline,lease_until)-clock_timestamp()))*1000)::bigint", &[&lease.delivery]).await?.get(0);
        ensure!(remaining > 0, "Delivery deadline exceeded");
        tx.execute("UPDATE moria.runtime_delivery_attempts SET status='sent',sent_at=clock_timestamp() WHERE delivery=$1 AND fence=$2", &[&lease.delivery,&lease.fence()?]).await?;
        tx.commit().await?;
        Ok((remaining as u64).min(10000))
    }

    async fn delivered(&self, lease: &DeliveryLease, receipt: &Receipt) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        Self::lease(&tx, lease).await?;
        let receipt = serde_json::to_value(receipt)?;
        tx.execute("UPDATE moria.runtime_deliveries SET status='delivered',receipt=$2,finished_at=clock_timestamp(),lease_until=NULL,error=NULL WHERE id=$1", &[&lease.delivery,&receipt]).await?;
        tx.execute("UPDATE moria.runtime_delivery_attempts SET status='delivered',receipt=$3,finished_at=clock_timestamp() WHERE delivery=$1 AND fence=$2", &[&lease.delivery,&lease.fence()?,&receipt]).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn failed(
        &self,
        claim: &DeliveryClaim,
        code: &str,
        definite_configuration_failure: bool,
    ) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        Self::lease(&tx, &claim.lease).await?;
        let state = if definite_configuration_failure {
            "failed"
        } else if !claim.idempotent {
            "uncertain"
        } else if claim.cycle_attempts >= claim.maximum_attempts {
            "failed"
        } else {
            "retryScheduled"
        };
        let delay = claim
            .retry_seconds
            .saturating_mul(2_i32.pow((claim.cycle_attempts - 1).clamp(0, 9) as u32))
            .min(300);
        tx.execute("UPDATE moria.runtime_deliveries SET status=$2,error=$3,lease_until=NULL,available_at=clock_timestamp()+$4::integer*interval '1 second',finished_at=CASE WHEN $2='retryScheduled' THEN NULL ELSE clock_timestamp() END WHERE id=$1", &[&claim.lease.delivery,&state,&code,&delay]).await?;
        tx.execute("UPDATE moria.runtime_delivery_attempts SET status=$3,error=$4,finished_at=clock_timestamp() WHERE delivery=$1 AND fence=$2", &[&claim.lease.delivery,&claim.lease.fence()?,&state,&code]).await?;
        tx.commit().await?;
        Ok(())
    }

    fn endpoint(value: &str) -> Result<reqwest::Url> {
        let url = reqwest::Url::parse(value)?;
        ensure!(
            url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "Invalid delivery endpoint"
        );
        let loopback = url
            .host_str()
            .and_then(|h| h.trim_matches(['[', ']']).parse::<std::net::IpAddr>().ok())
            .is_some_and(|ip| ip.is_loopback());
        ensure!(
            url.scheme() == "https"
                || (url.scheme() == "http"
                    && loopback
                    && env::var("MORIA_DELIVERY_ALLOW_LOOPBACK_HTTP").as_deref() == Ok("1")),
            "Delivery requires HTTPS"
        );
        Ok(url)
    }

    fn client(claim: &DeliveryClaim) -> Result<(reqwest::Client, reqwest::Url, Option<String>)> {
        let url = Self::endpoint(&claim.endpoint)?;
        ensure!(
            claim.body.len() <= 65536
                && format!("{:x}", Sha256::digest(claim.body.as_bytes())) == claim.payload_hash,
            "Corrupt delivery payload"
        );
        let authorization = if let Some(reference) = &claim.secret_ref {
            ensure!(
                !reference.is_empty()
                    && reference.len() <= 128
                    && reference
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
                "Invalid destination secret reference"
            );
            let secret = env::var(reference)
                .map_err(|_| anyhow::anyhow!("Destination secret unavailable"))?;
            ensure!(
                !secret.is_empty() && secret.len() <= 8192,
                "Invalid destination secret"
            );
            let value = format!("Bearer {secret}");
            reqwest::header::HeaderValue::from_str(&value)?;
            Some(value)
        } else {
            None
        };
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(Duration::from_secs(3));
        if let Ok(path) = env::var("MORIA_DELIVERY_CA") {
            builder = builder
                .add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(path)?)?);
        }
        Ok((builder.build()?, url, authorization))
    }

    async fn send(
        claim: &DeliveryClaim,
        client: reqwest::Client,
        url: reqwest::Url,
        authorization: Option<String>,
        timeout: u64,
    ) -> Result<Receipt> {
        let mut request = client
            .post(url)
            .timeout(Duration::from_millis(timeout))
            .header("content-type", "application/json")
            .header("idempotency-key", claim.lease.delivery.to_string())
            .header("moria-delivery-id", claim.lease.delivery.to_string())
            .header("moria-delivery-attempt", &claim.lease.fence)
            .header("moria-payload-sha256", &claim.payload_hash)
            .body(claim.body.clone());
        if let Some(value) = authorization {
            request = request.header("authorization", value);
        }
        let response = request.send().await?;
        ensure!(
            response.status().is_success(),
            "Destination rejected delivery"
        );
        ensure!(
            response
                .headers()
                .get("content-type")
                .and_then(|h| h.to_str().ok())
                .is_some_and(|h| h
                    .split(';')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .eq_ignore_ascii_case("application/json")),
            "Invalid delivery receipt"
        );
        ensure!(
            response.content_length().is_none_or(|n| n <= 16384),
            "Oversized delivery receipt"
        );
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.try_next().await? {
            ensure!(
                bytes.len() + chunk.len() <= 16384,
                "Oversized delivery receipt"
            );
            bytes.extend_from_slice(&chunk);
        }
        let receipt: Receipt = serde_json::from_slice(&bytes)?;
        ensure!(
            receipt.version == 1
                && receipt.delivery_id == claim.lease.delivery
                && receipt.payload_hash == claim.payload_hash
                && !receipt.receipt_id.is_empty()
                && receipt.receipt_id.len() <= 180
                && receipt
                    .receipt_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.:/".contains(&b)),
            "Invalid delivery receipt"
        );
        Ok(receipt)
    }

    pub async fn dispatch_once(&self, owner: Uuid) -> Result<bool> {
        let Some(claim) = self.claim(owner).await? else {
            return Ok(false);
        };
        let (client, url, authorization) = match Self::client(&claim) {
            Ok(value) => value,
            Err(_) => {
                self.failed(&claim, "DESTINATION_CONFIGURATION_INVALID", true)
                    .await?;
                return Ok(true);
            }
        };
        let timeout = match self.mark_sent(&claim.lease).await {
            Ok(timeout) => timeout,
            Err(_) => return Ok(true),
        };
        match Self::send(&claim, client, url, authorization, timeout).await {
            Ok(receipt) => {
                // Lost acknowledgments recover from the sent marker and the destination's capability.
                if self.delivered(&claim.lease, &receipt).await.is_err() {
                    eprintln!(
                        "{}",
                        json!({"event":"deliveryCompletionDeferred","delivery":claim.lease.delivery})
                    );
                }
            }
            Err(_) => {
                if self
                    .failed(&claim, "DELIVERY_UNCONFIRMED", false)
                    .await
                    .is_err()
                {
                    eprintln!(
                        "{}",
                        json!({"event":"deliveryFailureDeferred","delivery":claim.lease.delivery})
                    );
                }
            }
        }
        Ok(true)
    }

    pub async fn status(&self, id: Uuid) -> Result<Value> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let environment: String = tx
            .query_one(
                "SELECT environment FROM moria.runtime_deliveries WHERE id=$1",
                &[&id],
            )
            .await?
            .get(0);
        tx.query_one(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))",
            &[&format!("moria/environment/{environment}")],
        )
        .await?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended('moria/filesystem',0))",
            &[],
        )
        .await?;
        tx.query_one("SELECT moria.runtime_delivery_check($1,false)", &[&id])
            .await?;
        let result=tx.query_one("SELECT jsonb_build_object('id',id,'status',status,'fence',fence::text,'attempts',attempts,'payloadHash',payload_hash,'receipt',receipt,'error',error,'origin',origin) FROM moria.runtime_deliveries WHERE id=$1", &[&id]).await?.get(0);
        tx.query_one("SELECT moria.runtime_delivery_check($1,false)", &[&id])
            .await?;
        tx.commit().await?;
        Ok(result)
    }

    pub async fn cancel(&self, id: Uuid) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let row = tx
            .query_one(
                "SELECT status FROM moria.runtime_deliveries WHERE id=$1 FOR UPDATE",
                &[&id],
            )
            .await?;
        tx.query_one("SELECT moria.runtime_delivery_check($1,false)", &[&id])
            .await?;
        if !["delivered", "cancelled", "uncertain"].contains(&row.get::<_, String>(0).as_str()) {
            let dispatched:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_delivery_attempts WHERE delivery=$1 AND sent_at IS NOT NULL AND fence>COALESCE((SELECT max(expected_fence) FROM moria.runtime_delivery_reconciliations WHERE delivery=$1 AND resolution='notDelivered'),0))", &[&id]).await?.get(0);
            let state = if dispatched { "uncertain" } else { "cancelled" };
            tx.execute("UPDATE moria.runtime_deliveries SET status=$2,fence=fence+1,owner=NULL,lease_until=NULL,error=CASE WHEN $2='uncertain' THEN 'CANCEL_AFTER_DISPATCH' ELSE NULL END,finished_at=clock_timestamp() WHERE id=$1", &[&id,&state]).await?;
            tx.execute("UPDATE moria.runtime_delivery_attempts SET status=$2,error='CANCELLED',finished_at=clock_timestamp() WHERE delivery=$1 AND status IN ('claimed','sent')", &[&id,&state]).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn retry(&self, id: Uuid, expected_fence: &str) -> Result<()> {
        let expected: i64 = expected_fence.parse()?;
        ensure!(
            expected >= 0 && expected.to_string() == expected_fence,
            "Invalid expected fence"
        );
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let row=tx.query_one("SELECT d.status,d.fence,x.idempotent FROM moria.runtime_deliveries d JOIN moria.runtime_delivery_destinations x ON x.id=d.destination WHERE d.id=$1 FOR UPDATE OF d", &[&id]).await?;
        tx.query_one("SELECT moria.runtime_delivery_check($1)", &[&id])
            .await?;
        ensure!(
            row.get::<_, String>(0) == "failed" && row.get::<_, i64>(1) == expected,
            "Only the current dead letter can be retried"
        );
        let sent:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_delivery_attempts WHERE delivery=$1 AND sent_at IS NOT NULL AND fence>COALESCE((SELECT max(expected_fence) FROM moria.runtime_delivery_reconciliations WHERE delivery=$1 AND resolution='notDelivered'),0))", &[&id]).await?.get(0);
        ensure!(
            row.get::<_, bool>(2) || !sent,
            "Reconciliation required before retry"
        );
        tx.execute("UPDATE moria.runtime_deliveries SET status='pending',cycle_attempts=0,available_at=clock_timestamp(),deadline=clock_timestamp()+interval '30 minutes',error=NULL,finished_at=NULL WHERE id=$1", &[&id]).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Privileged reconciliation records provider evidence; it is not a worker acknowledgment.
    pub async fn reconcile(
        &self,
        id: Uuid,
        expected_fence: &str,
        resolution: &str,
        evidence: &str,
    ) -> Result<()> {
        let expected: i64 = expected_fence.parse()?;
        ensure!(
            expected >= 0 && expected.to_string() == expected_fence,
            "Invalid expected fence"
        );
        ensure!(
            ["delivered", "notDelivered"].contains(&resolution)
                && !evidence.is_empty()
                && evidence.len() <= 1024
                && !evidence.contains('\0'),
            "Invalid reconciliation evidence"
        );
        self.db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_delivery_reconcile($1,$2,$3,$4)",
                &[&id, &expected, &resolution, &evidence],
            )
            .await?;
        Ok(())
    }
}

pub async fn cli(db: Database, args: Vec<String>) -> Result<()> {
    let command = args
        .first()
        .map(String::as_str)
        .ok_or_else(|| anyhow::anyhow!("Delivery command required"))?;
    let mut bytes = Vec::new();
    tokio::io::stdin()
        .take(65537)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 65536, "Delivery command exceeds bounds");
    let value: Value = if bytes.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&bytes)?
    };
    let authority = DeliveryAuthority { db };
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Id {
        id: Uuid,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Owner {
        owner: Uuid,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct Retry {
        id: Uuid,
        expected_fence: String,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct Reconcile {
        id: Uuid,
        expected_fence: String,
        resolution: String,
        evidence: String,
    }
    if command == "worker" {
        ensure!(value == json!({}), "Unexpected worker input");
        let owner = Uuid::new_v4();
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let shutdown = async {
            tokio::select! {result=tokio::signal::ctrl_c()=>result,_=terminate.recv()=>Ok(())}
        };
        tokio::pin!(shutdown);
        loop {
            let worked = tokio::select! {_=&mut shutdown=>break,result=authority.dispatch_once(owner)=>result?};
            if !worked {
                tokio::select! {_=&mut shutdown=>break,_=tokio::time::sleep(Duration::from_millis(200))=>{}}
            }
        }
        return Ok(());
    }
    let result = match command {
        "claim" => serde_json::to_value(
            authority
                .claim(serde_json::from_value::<Owner>(value)?.owner)
                .await?,
        )?,
        "once" => {
            json!({"worked":authority.dispatch_once(serde_json::from_value::<Owner>(value)?.owner).await?})
        }
        "renew" => {
            authority.renew(&serde_json::from_value(value)?).await?;
            json!({"renewed":true})
        }
        "status" => {
            authority
                .status(serde_json::from_value::<Id>(value)?.id)
                .await?
        }
        "cancel" => {
            authority
                .cancel(serde_json::from_value::<Id>(value)?.id)
                .await?;
            json!({"cancelled":true})
        }
        "retry" => {
            let r: Retry = serde_json::from_value(value)?;
            authority.retry(r.id, &r.expected_fence).await?;
            json!({"retried":true})
        }
        "reconcile" => {
            let r: Reconcile = serde_json::from_value(value)?;
            authority
                .reconcile(r.id, &r.expected_fence, &r.resolution, &r.evidence)
                .await?;
            json!({"reconciled":true})
        }
        _ => bail!("Unknown delivery command"),
    };
    tokio::io::stdout()
        .write_all(format!("{result}\n").as_bytes())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn endpoint_never_accepts_redirect_targets_credentials_or_plaintext_remote_hosts() {
        assert!(DeliveryAuthority::endpoint("https://provider.example/delivery").is_ok());
        for url in [
            "http://provider.example/delivery",
            "https://user:secret@provider.example/delivery",
            "https://provider.example/delivery?token=secret",
            "https://provider.example/delivery#fragment",
            "file:///tmp/data",
        ] {
            assert!(DeliveryAuthority::endpoint(url).is_err(), "{url}");
        }
    }
}
