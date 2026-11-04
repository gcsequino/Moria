//! Explicit operator retirement of exact issued backup objects. General Dataset expiration is gated.
use crate::{
    canonical::fingerprint,
    database::Database,
    recovery::RETENTION_ADMISSION,
    storage::{
        self, Cancellation, IndexNamespace, LogicalObject, ObjectStore, ObjectVersion,
        ReadAuthority, RemoveAuthority,
    },
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{fmt, sync::Arc, time::Duration};
use tokio_postgres::Client;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PolicyRequest {
    pub request_key: String,
    pub namespace: IndexNamespace,
    pub minimum_age_seconds: u64,
    pub recheck_seconds: u32,
}
#[derive(Clone)]
pub struct RetentionAuthority {
    pub db: Database,
}
struct Admission(Option<deadpool_postgres::Object>);
impl std::ops::Deref for Admission {
    type Target = Client;
    fn deref(&self) -> &Client {
        self.0.as_ref().unwrap()
    }
}
impl Drop for Admission {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            drop(deadpool_postgres::Object::take(client));
        }
    }
}
impl Admission {
    async fn acquire(db: &Database) -> Result<Self> {
        let client = Self(Some(db.get().await?));
        client
            .query_one(
                "SELECT pg_advisory_lock(hashtextextended($1,0))",
                &[&RETENTION_ADMISSION],
            )
            .await?;
        Ok(client)
    }
    async fn release(mut self) -> Result<()> {
        ensure!(
            self.query_one(
                "SELECT pg_advisory_unlock(hashtextextended($1,0))",
                &[&RETENTION_ADMISSION]
            )
            .await?
            .get::<_, bool>(0),
            "GC_ADMISSION_LOST"
        );
        drop(self.0.take());
        Ok(())
    }
}
fn validate(request: &PolicyRequest) -> Result<()> {
    LogicalObject::new(
        &request.namespace.warehouse_id,
        &request.namespace.table_id,
        "policy",
    )?;
    ensure!(
        !request.request_key.is_empty()
            && request.request_key.len() <= 128
            && !request.namespace.revision.is_empty()
            && request.namespace.revision.len() <= 128
            && request.minimum_age_seconds >= 86400
            && request.minimum_age_seconds <= 9_007_199_254_740_991
            && (30..=604800).contains(&request.recheck_seconds),
        "GC_POLICY_INVALID"
    );
    ensure!(
        !(request.namespace.warehouse_id == "filesystem"
            && request.namespace.table_id == "content"),
        "GC_UNSUPPORTED_CLASS"
    );
    Ok(())
}
impl RetentionAuthority {
    /// The caller must be the trusted operator command with its dedicated database credential.
    pub async fn approve(&self, request: PolicyRequest) -> Result<Value> {
        self.db
            .get()
            .await?
            .query_one("SELECT moria.runtime_recovery_operator()", &[])
            .await?;
        validate(&request)?;
        let physical = fingerprint(&storage::configured_physical_binding(&request.namespace)?)?;
        let namespace = serde_json::to_value(&request.namespace)?;
        let definition = json!({"class":"recovery-output","namespace":namespace,"physicalFingerprint":physical,"minimumAgeSeconds":request.minimum_age_seconds.to_string(),"recheckSeconds":request.recheck_seconds});
        let revision = fingerprint(&definition)?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one("SELECT moria.runtime_recovery_operator()", &[])
            .await?;
        tx.query_one("SELECT moria.runtime_recovery_check()", &[])
            .await?;
        tx.execute("INSERT INTO moria.runtime_retention_policies(id,request_key,revision,namespace,physical_fingerprint,minimum_age_seconds,recheck_seconds,incarnation,approved_role) SELECT $1,$2,$3,$4,$5,$6,$7,incarnation,session_user FROM moria.indexing_instance ON CONFLICT(request_key) DO NOTHING",&[&Uuid::new_v4(),&request.request_key,&revision,&namespace,&physical,&(request.minimum_age_seconds as i64),&(request.recheck_seconds as i32)]).await?;
        let row=tx.query_one("SELECT p.*,p.incarnation=i.incarnation AS current_incarnation FROM moria.runtime_retention_policies p CROSS JOIN moria.indexing_instance i WHERE p.request_key=$1",&[&request.request_key]).await?;
        ensure!(
            row.get::<_, String>("revision") == revision
                && row.get::<_, bool>("current_incarnation"),
            "GC_POLICY_CONFLICT"
        );
        let result = json!({"policy":row.get::<_,Uuid>("id"),"revision":revision,"namespace":request.namespace,"physicalFingerprint":physical,"enabled":row.get::<_,bool>("enabled")});
        tx.commit().await?;
        Ok(result)
    }
    pub async fn disable(&self, policy: Uuid) -> Result<()> {
        ensure!(
            self.db
                .get()
                .await?
                .execute(
                    "UPDATE moria.runtime_retention_policies SET enabled=false WHERE id=$1",
                    &[&policy]
                )
                .await?
                == 1,
            "GC_POLICY_UNKNOWN"
        );
        Ok(())
    }
    pub async fn retire_recovery(&self, policy: Uuid, set: Uuid) -> Result<()> {
        let admission = Admission::acquire(&self.db).await?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one("SELECT moria.runtime_recovery_operator()", &[])
            .await?;
        tx.query_one("SELECT moria.runtime_recovery_check()", &[])
            .await?;
        let row=tx.query_one("SELECT s.*,p.enabled AND p.incarnation=i.incarnation AS policy_current,p.namespace=b.namespace AND p.physical_fingerprint=b.physical_fingerprint AS matching_backend,s.created_at+make_interval(secs=>p.minimum_age_seconds::double precision)<=clock_timestamp() AND s.retained_until<=clock_timestamp() AS expired,(s.lease_until IS NULL OR s.lease_until<=clock_timestamp()) AS producer_expired,NOT EXISTS(SELECT 1 FROM moria.runtime_recovery_outputs WHERE recovery_set=s.id) AS empty_ledger FROM moria.runtime_recovery_sets s JOIN moria.runtime_recovery_backends b ON b.recovery_set=s.id JOIN moria.runtime_retention_policies p ON p.id=$2 CROSS JOIN moria.indexing_instance i WHERE s.id=$1 FOR UPDATE OF s",&[&set,&policy]).await?;
        ensure!(
            row.get::<_, bool>("policy_current") && row.get::<_, bool>("matching_backend"),
            "GC_POLICY_CHANGED"
        );
        let current_incarnation: Uuid = tx
            .query_one("SELECT incarnation FROM moria.indexing_instance", &[])
            .await?
            .get(0);
        ensure!(
            row.get::<_, Uuid>("incarnation") == current_incarnation,
            "GC_FOREIGN_CUSTODY"
        );
        ensure!(row.get::<_, bool>("expired"), "GC_RETENTION_PINNED");
        let status: String = row.get("status");
        ensure!(
            status != "preparing" || row.get::<_, bool>("producer_expired"),
            "GC_PRODUCER_ACTIVE"
        );
        ensure!(
            !(row.get::<_, Option<Value>>("sealed_manifest").is_some()
                && row.get::<_, bool>("empty_ledger")),
            "GC_REFERENCE_INVENTORY_REQUIRED"
        );
        tx.execute("INSERT INTO moria.runtime_gc_retirements(recovery_set,policy,approved_role) VALUES($1,$2,session_user) ON CONFLICT DO NOTHING",&[&set,&policy]).await?;
        if status != "retired" {
            tx.execute("UPDATE moria.runtime_recovery_sets SET status='retired',lease_until=LEAST(lease_until,clock_timestamp()) WHERE id=$1",&[&set]).await?;
        }
        tx.commit().await?;
        admission.release().await
    }
    pub async fn plan(
        &self,
        policy: Uuid,
        set: Uuid,
        after: Option<String>,
        limit: u32,
    ) -> Result<Value> {
        ensure!(
            (1..=256).contains(&limit) && after.as_ref().is_none_or(|x| x.len() <= 128),
            "GC_POLICY_INVALID"
        );
        let admission = Admission::acquire(&self.db).await?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one("SELECT moria.runtime_recovery_operator()", &[])
            .await?;
        tx.query_one("SELECT moria.runtime_recovery_check()", &[])
            .await?;
        let valid:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_gc_retirements r JOIN moria.runtime_retention_policies p ON p.id=r.policy JOIN moria.runtime_recovery_sets s ON s.id=r.recovery_set CROSS JOIN moria.indexing_instance i WHERE r.policy=$1 AND r.recovery_set=$2 AND p.enabled AND p.incarnation=i.incarnation AND s.incarnation=i.incarnation AND s.status='retired' AND s.retained_until<=clock_timestamp())",&[&policy,&set]).await?.get(0);
        ensure!(valid, "GC_POLICY_CHANGED");
        let namespace: Value = tx
            .query_one(
                "SELECT namespace FROM moria.runtime_retention_policies WHERE id=$1",
                &[&policy],
            )
            .await?
            .get(0);
        let rows=tx.query("SELECT warehouse_id,table_id,object_id FROM moria.runtime_recovery_outputs WHERE recovery_set=$1 AND warehouse_id=$4 AND table_id=$5 AND object_id>=COALESCE($2::text,'') COLLATE \"C\" AND ($2::text IS NULL OR object_id>$2 COLLATE \"C\") ORDER BY object_id LIMIT $3",&[&set,&after,&(limit as i64),&namespace["warehouseId"].as_str().ok_or_else(||anyhow::anyhow!("GC_POLICY_CHANGED"))?,&namespace["tableId"].as_str().ok_or_else(||anyhow::anyhow!("GC_POLICY_CHANGED"))?]).await?;
        let mut last = None;
        let mut inserted = 0u64;
        for row in &rows {
            let object = LogicalObject::new(
                row.get("warehouse_id"),
                row.get("table_id"),
                row.get("object_id"),
            )?;
            let value = serde_json::to_value(&object)?;
            tx.query_one(
                "SELECT moria.runtime_gc_reference_check($1,$2)",
                &[&set, &value],
            )
            .await?;
            inserted+=tx.execute("INSERT INTO moria.runtime_gc_candidates(policy,recovery_set,object) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",&[&policy,&set,&value]).await?;
            last = Some(object.object_id);
        }
        tx.commit().await?;
        admission.release().await?;
        Ok(json!({"planned":inserted,"next":last,"complete":rows.len()<limit as usize}))
    }
}
pub async fn cli(db: Database, args: Vec<String>) -> Result<()> {
    use tokio::io::AsyncReadExt;
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Policy {
        policy: Uuid,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Retire {
        policy: Uuid,
        set: Uuid,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Plan {
        policy: Uuid,
        set: Uuid,
        after: Option<String>,
        limit: u32,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct Once {
        policy: Uuid,
        owner: Uuid,
        time_seconds: u64,
    }
    ensure!(args.len() == 1, "GC_POLICY_INVALID");
    let mut bytes = Vec::new();
    tokio::io::stdin()
        .take(16385)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 16384, "GC_POLICY_INVALID");
    let authority = RetentionAuthority { db };
    let result = match args[0].as_str() {
        "approve" => {
            authority
                .approve(
                    serde_json::from_slice(&bytes)
                        .map_err(|_| anyhow::anyhow!("GC_POLICY_INVALID"))?,
                )
                .await?
        }
        "disable" => {
            let request: Policy =
                serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("GC_POLICY_INVALID"))?;
            authority.disable(request.policy).await?;
            json!({"disabled":true})
        }
        "retire" => {
            let request: Retire =
                serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("GC_POLICY_INVALID"))?;
            authority
                .retire_recovery(request.policy, request.set)
                .await?;
            json!({"retired":true})
        }
        "plan" => {
            let request: Plan =
                serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("GC_POLICY_INVALID"))?;
            authority
                .plan(request.policy, request.set, request.after, request.limit)
                .await?
        }
        "once" => {
            let request: Once =
                serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("GC_POLICY_INVALID"))?;
            authority
                .collect_once(
                    request.policy,
                    request.owner,
                    Duration::from_secs(request.time_seconds),
                )
                .await?
        }
        _ => anyhow::bail!("GC_POLICY_INVALID"),
    };
    println!("{result}");
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RemovalToken {
    pub claim: Uuid,
    pub owner: Uuid,
    pub fence: String,
    pub incarnation: Uuid,
}
#[derive(Clone)]
struct RemovalClaim {
    db: Database,
    token: RemovalToken,
    object: LogicalObject,
    namespace: IndexNamespace,
    physical: String,
    expected: Option<ObjectVersion>,
}
impl fmt::Debug for RemovalClaim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemovalClaim")
            .field("claim", &self.token.claim)
            .finish()
    }
}
impl RemovalClaim {
    async fn check_in(&self, client: &Client) -> Result<Value> {
        let fence = self.token.fence.parse::<i64>()?;
        ensure!(
            fence > 0 && fence.to_string() == self.token.fence,
            "GC_LEASE_LOST"
        );
        let value: Value = client
            .query_one(
                "SELECT moria.runtime_gc_claim_check($1,$2,$3,$4)",
                &[
                    &self.token.claim,
                    &self.token.owner,
                    &fence,
                    &self.token.incarnation,
                ],
            )
            .await?
            .get(0);
        ensure!(
            value["object"] == serde_json::to_value(&self.object)?
                && value["namespace"] == serde_json::to_value(&self.namespace)?
                && value["physicalFingerprint"].as_str() == Some(self.physical.as_str()),
            "GC_POLICY_CHANGED"
        );
        if let Some(expected) = &self.expected {
            ensure!(
                value["expectedVersion"] == serde_json::to_value(expected)?,
                "GC_LEASE_LOST"
            );
        }
        Ok(value)
    }
    async fn check(&self) -> Result<Value> {
        let client = self.db.get().await?;
        self.check_in(&client).await
    }
    async fn renew(&self) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT 1 FROM moria.runtime_gc_candidates WHERE id=$1 FOR UPDATE",
            &[&self.token.claim],
        )
        .await?;
        self.check_in(tx.client()).await?;
        tx.execute("UPDATE moria.runtime_gc_candidates SET lease_until=clock_timestamp()+interval '30 seconds' WHERE id=$1",&[&self.token.claim]).await?;
        tx.commit().await?;
        Ok(())
    }
}
#[async_trait]
impl ReadAuthority for RemovalClaim {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        ensure!(*object == self.object, "GC_OBJECT_DENIED");
        self.check().await?;
        Ok(())
    }
}
#[async_trait]
impl RemoveAuthority for RemovalClaim {
    async fn check_remove(&self, object: &LogicalObject, version: &ObjectVersion) -> Result<()> {
        ensure!(
            *object == self.object && self.expected.as_ref() == Some(version),
            "GC_OBJECT_DENIED"
        );
        self.check().await?;
        Ok(())
    }
}

struct Renewal(tokio::task::JoinHandle<()>);
impl Drop for Renewal {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl RetentionAuthority {
    pub async fn collect_once(
        &self,
        policy: Uuid,
        owner: Uuid,
        timeout: Duration,
    ) -> Result<Value> {
        ensure!((1..=600).contains(&timeout.as_secs()), "GC_POLICY_INVALID");
        let admission = Admission::acquire(&self.db).await?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one("SELECT moria.runtime_recovery_operator()", &[])
            .await?;
        tx.query_one("SELECT moria.runtime_recovery_check()", &[])
            .await?;
        let row=tx.query_opt("SELECT c.id FROM moria.runtime_gc_candidates c JOIN moria.runtime_retention_policies p ON p.id=c.policy CROSS JOIN moria.indexing_instance i WHERE c.policy=$1 AND p.enabled AND p.incarnation=i.incarnation AND c.next_check<=clock_timestamp() AND (c.status<>'running' OR c.lease_until<=clock_timestamp()) ORDER BY c.next_check,c.id FOR UPDATE OF c SKIP LOCKED LIMIT 1",&[&policy]).await?;
        let Some(row) = row else {
            tx.commit().await?;
            admission.release().await?;
            return Ok(json!({"claimed":false}));
        };
        let id: Uuid = row.get(0);
        let backend = admission
            .query_one(
                "SELECT pid,backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()",
                &[],
            )
            .await?;
        let row=tx.query_one("UPDATE moria.runtime_gc_candidates SET status='running',owner=$2,fence=fence+1,incarnation=(SELECT incarnation FROM moria.indexing_instance),lease_until=clock_timestamp()+interval '30 seconds',admission_pid=$3,admission_started=$4,expected_version=NULL,attempts=attempts+1 WHERE id=$1 RETURNING fence::text,incarnation",&[&id,&owner,&backend.get::<_,i32>("pid"),&backend.get::<_,std::time::SystemTime>("backend_start")]).await?;
        let token = RemovalToken {
            claim: id,
            owner,
            fence: row.get("fence"),
            incarnation: row.get("incarnation"),
        };
        let value: Value = tx
            .query_one(
                "SELECT moria.runtime_gc_claim_check($1,$2,$3,$4)",
                &[
                    &id,
                    &owner,
                    &token.fence.parse::<i64>()?,
                    &token.incarnation,
                ],
            )
            .await?
            .get(0);
        let mut claim = RemovalClaim {
            db: self.db.clone(),
            token,
            object: serde_json::from_value(value["object"].clone())?,
            namespace: serde_json::from_value(value["namespace"].clone())?,
            physical: value["physicalFingerprint"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("GC_POLICY_CHANGED"))?
                .into(),
            expected: None,
        };
        tx.commit().await?;
        drop(client);
        let cancel = Cancellation::default();
        let renew = claim.clone();
        let renew_cancel = cancel.clone();
        let renewal = Renewal(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                if renew.renew().await.is_err() {
                    renew_cancel.cancel();
                    break;
                }
            }
        }));
        let operation = async {
            let store: Arc<dyn ObjectStore> =
                storage::configured_store_with_fingerprint(&claim.namespace)?.0;
            ensure!(
                store.physical_fingerprint()? == claim.physical,
                "GC_PROVIDER_CHANGED"
            );
            let version = store.inspect(&claim.object, &claim, &cancel).await?;
            let Some(version) = version else {
                return Ok::<_, anyhow::Error>(("absent", false, None));
            };
            {
                let mut client = self.db.get().await?;
                let tx = client.transaction().await?;
                tx.query_one(
                    "SELECT 1 FROM moria.runtime_gc_candidates WHERE id=$1 FOR UPDATE",
                    &[&id],
                )
                .await?;
                claim.check_in(tx.client()).await?;
                tx.execute(
                    "UPDATE moria.runtime_gc_candidates SET expected_version=$2 WHERE id=$1",
                    &[&id, &serde_json::to_value(&version)?],
                )
                .await?;
                tx.commit().await?;
            }
            claim.expected = Some(version.clone());
            let removed = store
                .remove_object(
                    claim.object.clone(),
                    version.clone(),
                    Arc::new(claim.clone()),
                    cancel.clone(),
                )
                .await?;
            Ok((
                if removed {
                    "removed-version"
                } else {
                    "absent-version"
                },
                removed,
                if removed { Some(version) } else { None },
            ))
        };
        let result = match tokio::time::timeout(timeout, operation).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("GC_DEADLINE")),
        };
        renewal.0.abort();
        let (status, removed, version) = match result {
            Ok(value) => value,
            Err(error) if error.root_cause().to_string() == "CONDITIONAL_DELETE_UNSUPPORTED" => {
                ("unsupported", false, None)
            }
            Err(error) => {
                cancel.cancel();
                drop(admission);
                return Err(error);
            }
        };
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT 1 FROM moria.runtime_gc_candidates WHERE id=$1 FOR UPDATE",
            &[&id],
        )
        .await?;
        claim.check_in(tx.client()).await?;
        tx.execute("UPDATE moria.runtime_gc_candidates SET status=$2,last_result=$3,last_removed_version=COALESCE($4,last_removed_version),next_check=clock_timestamp()+make_interval(secs=>(SELECT recheck_seconds FROM moria.runtime_retention_policies WHERE id=runtime_gc_candidates.policy)),lease_until=NULL,owner=NULL,admission_pid=NULL,admission_started=NULL WHERE id=$1",&[&id,&if status=="unsupported"{"unsupported"}else{"watching"},&status,&version.map(serde_json::to_value).transpose()?]).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(
            json!({"claimed":true,"claim":id,"result":status,"removedVersion":removed,"reconciliationRequired":true}),
        )
    }
}
