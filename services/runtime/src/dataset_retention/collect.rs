use super::*;
use crate::storage::{
    self, AbortAuthority, DiscoveryAuthority, LogicalObject, ObjectStore, ObjectVersion,
    RemoveAuthority,
};
use anyhow::ensure;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{fmt, time::Duration};
use tokio_postgres::Client;
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Plan {
    policy: Uuid,
    request_key: String,
    after: Option<String>,
    limit: u32,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Once {
    policy: Uuid,
    owner: Uuid,
    time_seconds: u64,
}

struct Admission {
    client: Option<deadpool_postgres::Object>,
    key: String,
}
impl std::ops::Deref for Admission {
    type Target = Client;
    fn deref(&self) -> &Client {
        self.client.as_ref().unwrap()
    }
}
impl Drop for Admission {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            drop(deadpool_postgres::Object::take(client));
        }
    }
}
impl Admission {
    async fn acquire(db: &Database, token: &RemovalToken) -> Result<Self> {
        let key = format!(
            "moria/dataset-gc/{}/{}/{}/{}",
            token.claim, token.owner, token.fence, token.incarnation
        );
        let admission = Self {
            client: Some(db.get().await?),
            key,
        };
        admission
            .query_one(
                "SELECT pg_advisory_lock(hashtextextended($1,0))",
                &[&admission.key],
            )
            .await?;
        Ok(admission)
    }
    async fn release(mut self) -> Result<()> {
        ensure!(
            self.query_one(
                "SELECT pg_advisory_unlock(hashtextextended($1,0))",
                &[&self.key]
            )
            .await?
            .get::<_, bool>(0),
            "GC_ADMISSION_LOST"
        );
        drop(self.client.take());
        Ok(())
    }
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
                "SELECT moria.runtime_dataset_gc_claim_check($1,$2,$3,$4)",
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
            "SELECT 1 FROM moria.runtime_dataset_gc_candidates WHERE id=$1 FOR UPDATE",
            &[&self.token.claim],
        )
        .await?;
        self.check_in(tx.client()).await?;
        tx.execute("UPDATE moria.runtime_dataset_gc_candidates SET lease_until=clock_timestamp()+interval '30 seconds' WHERE id=$1",&[&self.token.claim]).await?;
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

#[async_trait]
impl DiscoveryAuthority for RemovalClaim {
    async fn check_discover(&self, object: &LogicalObject) -> Result<()> {
        ensure!(*object == self.object, "GC_OBJECT_DENIED");
        self.check().await?;
        Ok(())
    }
}
#[derive(Clone, Debug)]
struct MultipartRemoval {
    claim: RemovalClaim,
    upload_id: String,
}
#[async_trait]
impl AbortAuthority for MultipartRemoval {
    async fn check_abort(&self, object: &LogicalObject, upload_id: &str) -> Result<()> {
        ensure!(
            *object == self.claim.object && upload_id == self.upload_id,
            "GC_OBJECT_DENIED"
        );
        self.claim.check().await?;
        Ok(())
    }
}

struct Renewal(tokio::task::JoinHandle<()>);
impl Drop for Renewal {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn plan(db: &Database, input: Plan) -> Result<Value> {
    ensure!(
        !input.request_key.is_empty()
            && input.request_key.len() <= 128
            && (1..=256).contains(&input.limit)
            && input.after.as_ref().is_none_or(|v| v.len() <= 128),
        "GC_POLICY_INVALID"
    );
    let request = crate::canonical::fingerprint(&serde_json::to_value(&input)?)?;
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    // This short admission transaction establishes permanent no-resurrection tombstones before grace starts.
    super::admission(tx.client(), true).await?;
    let state: Value = tx
        .query_one(
            "SELECT moria.runtime_dataset_policy_check($1)",
            &[&input.policy],
        )
        .await?
        .get(0);
    if let Some(previous) = tx
        .query_opt(
            "SELECT request_hash,receipt FROM moria.runtime_dataset_gc_plans WHERE request_key=$1",
            &[&input.request_key],
        )
        .await?
    {
        ensure!(
            previous.get::<_, String>("request_hash") == request,
            "GC_POLICY_CONFLICT"
        );
        let receipt = previous.get("receipt");
        tx.commit().await?;
        return Ok(receipt);
    }
    let namespace: IndexNamespace = serde_json::from_value(state["namespace"].clone())?;
    let physical = state["physicalFingerprint"].as_str().unwrap();
    let incarnation: Uuid = tx
        .query_one("SELECT incarnation FROM moria.indexing_instance", &[])
        .await?
        .get(0);
    tx.query_one(
        "SELECT moria.runtime_dataset_graph_retire($1,$2,256)",
        &[&input.policy, &incarnation],
    )
    .await?;
    let operation = Uuid::new_v4();
    let rows=tx.query("SELECT o.object_id,o.producer FROM moria.runtime_dataset_issued_objects o JOIN moria.runtime_dataset_producers p ON p.scope=o.producer WHERE o.physical_fingerprint=$1 AND o.warehouse_id=$2 AND o.table_id=$3 AND p.namespace=$4 AND p.incarnation=$5 AND ($6::text IS NULL OR o.object_id>$6) AND o.created_at+make_interval(secs=>$7::bigint::double precision)<=clock_timestamp() ORDER BY o.object_id LIMIT $8",&[&physical,&namespace.warehouse_id,&namespace.table_id,&serde_json::to_value(&namespace)?,&incarnation,&input.after,&state["retainSeconds"].as_str().unwrap().parse::<i64>()?,&(input.limit as i64)]).await?;
    let mut added = 0u64;
    let mut blocked = 0u64;
    for row in &rows {
        let object: String = row.get("object_id");
        let producer: String = row.get("producer");
        if !tx
            .query_one(
                "SELECT moria.runtime_dataset_producer_closed($1)",
                &[&producer],
            )
            .await?
            .get::<_, bool>(0)
        {
            blocked += 1;
            continue;
        }
        tx.execute("UPDATE moria.runtime_dataset_producers SET tombstoned_at=clock_timestamp() WHERE scope=$1 AND tombstoned_at IS NULL",&[&producer]).await?;
        // Only unpublished preparations from a permanently closed producer can lose this reference.
        let private=tx.query("SELECT r.key FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_root_locations l ON l.root=r.key AND l.incarnation=(SELECT incarnation FROM moria.indexing_instance) AND l.sealed WHERE l.producer=$1 AND r.created_at+make_interval(secs=>$2::bigint::double precision)<=clock_timestamp() AND NOT EXISTS(SELECT 1 FROM moria.snapshot_history h WHERE h.metadata_location=r.metadata_location) AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_expired_roots e WHERE e.root=r.key) ORDER BY r.key LIMIT 64",&[&producer,&state["retainSeconds"].as_str().unwrap().parse::<i64>()?]).await?;
        for root in private {
            let key: String = root.get(0);
            if !tx
                .query_one("SELECT moria.runtime_dataset_root_pinned($1)", &[&key])
                .await?
                .get::<_, bool>(0)
            {
                tx.execute("INSERT INTO moria.runtime_dataset_expired_roots(root,policy,maintenance) VALUES($1,$2,$3)",&[&key,&input.policy,&operation]).await?;
            }
        }
        if tx
            .query_one(
                "SELECT moria.runtime_dataset_object_referenced($1,$2,$3,$4)",
                &[
                    &physical,
                    &namespace.warehouse_id,
                    &namespace.table_id,
                    &object,
                ],
            )
            .await?
            .get::<_, bool>(0)
        {
            blocked += 1;
            continue;
        }
        added+=tx.execute("INSERT INTO moria.runtime_dataset_gc_candidates(policy,physical_fingerprint,warehouse_id,table_id,object_id,producer) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",&[&input.policy,&physical,&namespace.warehouse_id,&namespace.table_id,&object,&producer]).await?;
    }
    let after: Option<String> = rows.last().map(|row| row.get("object_id"));
    let receipt = json!({"operation":operation,"scanned":rows.len(),"added":added,"blocked":blocked,"after":after,"reconciliationRequired":true,"graceSeconds":86400});
    tx.execute("INSERT INTO moria.runtime_dataset_gc_plans(id,request_key,request_hash,policy,approved_role,incarnation,receipt) VALUES($1,$2,$3,$4,session_user,$5,$6)",&[&operation,&input.request_key,&request,&input.policy,&incarnation,&receipt]).await?;
    tx.commit().await?;
    Ok(receipt)
}

pub(super) async fn once(db: &Database, input: Once) -> Result<Value> {
    ensure!((30..=60).contains(&input.time_seconds), "GC_POLICY_INVALID");
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    tx.query_one("SELECT moria.runtime_recovery_check()", &[])
        .await?;
    let row=tx.query_opt("SELECT c.id,c.fence,i.incarnation FROM moria.runtime_dataset_gc_candidates c JOIN moria.runtime_dataset_retention_policies p ON p.id=c.policy CROSS JOIN moria.indexing_instance i WHERE c.policy=$1 AND p.enabled AND p.incarnation=i.incarnation AND c.not_before<=clock_timestamp() AND c.next_check<=clock_timestamp() AND (c.status<>'running' OR c.lease_until<=clock_timestamp()) ORDER BY c.next_check,c.id FOR UPDATE OF c SKIP LOCKED LIMIT 1",&[&input.policy]).await?;
    let Some(row) = row else {
        tx.commit().await?;
        return Ok(json!({"claimed":false}));
    };
    let id: Uuid = row.get("id");
    let token = RemovalToken {
        claim: id,
        owner: input.owner,
        fence: (row.get::<_, i64>("fence") + 1).to_string(),
        incarnation: row.get("incarnation"),
    };
    let admission = Admission::acquire(db, &token).await?;
    let backend = admission
        .query_one(
            "SELECT pid,backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()",
            &[],
        )
        .await?;
    tx.execute("UPDATE moria.runtime_dataset_gc_candidates SET status='running',owner=$2,fence=$3,incarnation=$4,lease_until=clock_timestamp()+interval '30 seconds',admission_pid=$5,admission_started=$6,expected_version=NULL,attempts=attempts+1 WHERE id=$1",&[&id,&input.owner,&token.fence.parse::<i64>()?,&token.incarnation,&backend.get::<_,i32>("pid"),&backend.get::<_,std::time::SystemTime>("backend_start")]).await?;
    let value: Value = tx
        .query_one(
            "SELECT moria.runtime_dataset_gc_claim_check($1,$2,$3,$4)",
            &[
                &id,
                &input.owner,
                &token.fence.parse::<i64>()?,
                &token.incarnation,
            ],
        )
        .await?
        .get(0);
    let expected_size = value["expectedSize"].as_str().unwrap().parse::<u64>()?;
    let expected_hash = value["expectedSha256"].as_str().unwrap().to_owned();
    let mut claim = RemovalClaim {
        db: db.clone(),
        token,
        object: serde_json::from_value(value["object"].clone())?,
        namespace: serde_json::from_value(value["namespace"].clone())?,
        physical: value["physicalFingerprint"].as_str().unwrap().to_owned(),
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
    let mut multipart_outcomes = Vec::new();
    let operation = async {
        let store: Arc<dyn ObjectStore> =
            storage::configured_store_with_fingerprint(&claim.namespace)?.0;
        ensure!(
            store.physical_fingerprint()? == claim.physical,
            "GC_PROVIDER_CHANGED"
        );
        // Issued objects reserve exact unique keys before Initiate; restart bounded discovery after mutating a page.
        let page = store
            .discover_multipart(
                claim.object.clone(),
                None,
                Arc::new(claim.clone()),
                cancel.clone(),
            )
            .await?;
        let known=db.get().await?.query("SELECT u.upload_id FROM moria.runtime_dataset_multipart_uploads u WHERE u.physical_fingerprint=$1 AND u.warehouse_id=$2 AND u.table_id=$3 AND u.object_id=$4 AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_gc_multipart m WHERE m.candidate=$5 AND m.upload_id=u.upload_id) ORDER BY u.upload_id LIMIT 16",&[&claim.physical,&claim.object.warehouse_id,&claim.object.table_id,&claim.object.object_id,&id]).await?;
        let mut ids = page.upload_ids;
        for row in known {
            let value: String = row.get(0);
            if ids.len() < 16 && !ids.contains(&value) {
                ids.push(value);
            }
        }
        for upload_id in ids {
            let authority = Arc::new(MultipartRemoval {
                claim: claim.clone(),
                upload_id: upload_id.clone(),
            });
            let aborted = store
                .abort_multipart(
                    claim.object.clone(),
                    upload_id.clone(),
                    authority,
                    cancel.clone(),
                )
                .await?;
            multipart_outcomes.push((upload_id, aborted));
        }
        // Prefer exact qualified versions already certified by immutable native roots, including overwritten versions.
        let known=db.get().await?.query_opt("SELECT physical_version FROM(SELECT m.physical_version,m.root AS identity FROM moria.runtime_dataset_member_locations m JOIN moria.runtime_dataset_roots r ON r.key=m.root WHERE m.physical_fingerprint=$1 AND m.incarnation=(SELECT incarnation FROM moria.indexing_instance) AND r.reader_profile='iceberg-v2-cow-flat-bounded-v2' AND r.sealed AND m.warehouse_id=$2 AND m.table_id=$3 AND m.object_id=$4 AND m.content->>'size'=$5 AND m.content->>'sha256'=$6 UNION ALL SELECT q.physical_version,n.id AS identity FROM moria.runtime_dataset_graph_qualifications q JOIN moria.runtime_dataset_graph_nodes n ON n.id=q.node WHERE q.physical_fingerprint=$1 AND q.incarnation=(SELECT incarnation FROM moria.indexing_instance) AND n.content->'object'->>'warehouseId'=$2 AND n.content->'object'->>'tableId'=$3 AND n.content->'object'->>'objectId'=$4 AND n.content->>'size'=$5 AND n.content->>'sha256'=$6) versions WHERE NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_gc_versions v WHERE v.candidate=$7 AND v.version_key=encode(sha256(convert_to(versions.physical_version::text,'UTF8')),'hex')) ORDER BY identity LIMIT 1",&[&claim.physical,&claim.object.warehouse_id,&claim.object.table_id,&claim.object.object_id,&expected_size.to_string(),&expected_hash,&id]).await?;
        let (version, certified) = if let Some(known) = known {
            (
                Some(serde_json::from_value::<ObjectVersion>(known.get(0))?),
                true,
            )
        } else {
            (store.inspect(&claim.object, &claim, &cancel).await?, false)
        };
        let Some(version) = version else {
            return Ok::<_, anyhow::Error>(("absent-owned-versions", false, None));
        };
        storage::qualified_s3_version(&version)?;
        if !certified {
            let seen=db.get().await?.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_gc_versions WHERE candidate=$1 AND version_key=encode(sha256(convert_to($2::jsonb::text,'UTF8')),'hex'))",&[&id,&serde_json::to_value(&version)?]).await?.get::<_,bool>(0);
            if seen {
                return Ok(("absent-owned-versions", false, None));
            }
            if version.size != expected_size {
                return Ok(("ownership-changed", false, None));
            }
            let mut hash = Sha256::new();
            let mut offset = 0u64;
            while offset < version.size {
                let end = (offset + 1024 * 1024).min(version.size);
                let bytes = store
                    .range_at_version(&claim.object, offset..end, &version, &claim, &cancel)
                    .await?;
                ensure!(bytes.len() as u64 == end - offset, "CONTENT_MISMATCH");
                hash.update(bytes);
                offset = end;
            }
            if format!("sha256:{:x}", hash.finalize()) != expected_hash {
                return Ok(("ownership-changed", false, None));
            }
        }
        {
            let mut client = db.get().await?;
            let tx = client.transaction().await?;
            tx.query_one(
                "SELECT 1 FROM moria.runtime_dataset_gc_candidates WHERE id=$1 FOR UPDATE",
                &[&id],
            )
            .await?;
            claim.check_in(tx.client()).await?;
            tx.execute(
                "UPDATE moria.runtime_dataset_gc_candidates SET expected_version=$2 WHERE id=$1",
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
            Some(version),
        ))
    };
    let result =
        match tokio::time::timeout(Duration::from_secs(input.time_seconds), operation).await {
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
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    tx.query_one(
        "SELECT 1 FROM moria.runtime_dataset_gc_candidates WHERE id=$1 FOR UPDATE",
        &[&id],
    )
    .await?;
    claim.check_in(tx.client()).await?;
    for (upload_id, aborted) in &multipart_outcomes {
        tx.execute("INSERT INTO moria.runtime_dataset_gc_multipart(candidate,upload_id,result,owner,fence,incarnation) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",&[&id,&upload_id,&if *aborted{"aborted"}else{"absent"},&claim.token.owner,&claim.token.fence.parse::<i64>()?,&claim.token.incarnation]).await?;
    }
    if let Some(version) = &version {
        tx.execute("INSERT INTO moria.runtime_dataset_gc_versions(candidate,version_key,version,result) VALUES($1,encode(sha256(convert_to($2::jsonb::text,'UTF8')),'hex'),$2,$3) ON CONFLICT DO NOTHING",&[&id,&serde_json::to_value(version)?,&if removed{"removed"}else{"absent"}]).await?;
    }
    tx.execute("UPDATE moria.runtime_dataset_gc_candidates SET status=$2,last_result=$3,last_removed_version=CASE WHEN $4 THEN expected_version ELSE last_removed_version END,next_check=clock_timestamp()+make_interval(secs=>(SELECT recheck_seconds FROM moria.runtime_dataset_retention_policies WHERE id=runtime_dataset_gc_candidates.policy)),lease_until=NULL,owner=NULL,admission_pid=NULL,admission_started=NULL WHERE id=$1",&[&id,&if status=="unsupported"{"unsupported"}else{"watching"},&status,&removed]).await?;
    let fence = claim.token.fence.clone();
    tx.commit().await?;
    admission.release().await?;
    Ok(
        json!({"claimed":true,"claim":id,"fence":fence,"result":status,"removedVersion":removed,"multipartAborted":multipart_outcomes.iter().filter(|(_,aborted)|*aborted).count(),"reconciliationRequired":true}),
    )
}
