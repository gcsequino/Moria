use super::*;
use crate::{
    canonical::{canonical_json, fingerprint},
    snapshot::{self, SnapshotPin},
    storage::{
        self, ClosureLimits, LogicalObject, ObjectVersion, PosixStore, StorageLimits,
        VerifiedVersionAuthority,
    },
};
use anyhow::ensure;
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Approval {
    request_key: String,
    environment: String,
    dataset: Uuid,
    retain_seconds: u64,
    recheck_seconds: u32,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Expiration {
    policy: Uuid,
    request_key: String,
    owner: Uuid,
    before_revision: String,
    maximum_roots: u32,
    time_seconds: u64,
}
#[derive(Clone)]
struct Grant {
    db: Database,
    id: Uuid,
    owner: Uuid,
    fence: i64,
    incarnation: Uuid,
    namespace: IndexNamespace,
    state: Value,
    cancel: Cancellation,
}
impl fmt::Debug for Grant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatasetMaintenanceGrant")
            .field("run", &self.id)
            .finish_non_exhaustive()
    }
}
impl Grant {
    fn scope(&self) -> String {
        format!(
            "dataset-maintenance/{}/{}/{}/{}",
            self.id, self.owner, self.fence, self.incarnation
        )
    }
    async fn check(&self) -> Result<()> {
        self.cancel.check()?;
        let state: Value = self
            .db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_maintenance_check($1,$2,$3,$4)",
                &[&self.id, &self.owner, &self.fence, &self.incarnation],
            )
            .await?
            .get(0);
        ensure!(state == self.state, "GC_POLICY_CHANGED");
        self.cancel.check()?;
        Ok(())
    }
    fn bound(&self, object: &LogicalObject) -> Result<()> {
        ensure!(
            object.version == 1
                && LogicalObject::new(&object.warehouse_id, &object.table_id, &object.object_id)?
                    == *object
                && object.warehouse_id == self.namespace.warehouse_id
                && object.table_id == self.namespace.table_id,
            "GC_OBJECT_DENIED"
        );
        Ok(())
    }
}
#[async_trait]
impl ReadAuthority for Grant {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        self.bound(object)?;
        self.check().await
    }
}
#[async_trait]
impl WriteAuthority for Grant {
    fn scope_key(&self) -> String {
        self.scope()
    }
    async fn check_write(&self, object: &LogicalObject) -> Result<()> {
        self.bound(object)?;
        self.check().await
    }
    async fn admit_write(
        &self,
        object: &LogicalObject,
        size: u64,
        sha256: &str,
        physical: &str,
    ) -> Result<()> {
        self.bound(object)?;
        self.check().await?;
        self.db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_maintenance_issue($1,$2,$3,$4,$5,$6,$7,$8)",
                &[
                    &self.id,
                    &self.owner,
                    &self.fence,
                    &self.incarnation,
                    &physical,
                    &object.object_id,
                    &i64::try_from(size)?,
                    &sha256,
                ],
            )
            .await?;
        self.check().await
    }
}
#[async_trait]
impl VerifiedVersionAuthority for Grant {
    async fn verified_version(&self, content: &SealedObject) -> Result<ObjectVersion> {
        self.bound(&content.object)?;
        self.check().await?;
        let location = self.state["oldMetadata"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("GC_POLICY_CHANGED"))?;
        let row=self.db.get().await?.query_opt("SELECT m.content,m.physical_version FROM moria.runtime_dataset_member_locations m JOIN moria.runtime_dataset_roots r ON r.key=m.root WHERE r.key=moria.runtime_dataset_root_key($1) AND r.sealed AND r.reader_profile='iceberg-v2-cow-flat-bounded-v2' AND m.physical_fingerprint=$2 AND m.incarnation=(SELECT incarnation FROM moria.indexing_instance) AND m.warehouse_id=$3 AND m.table_id=$4 AND m.object_id=$5",&[&location,&self.state["physicalFingerprint"].as_str().unwrap(),&content.object.warehouse_id,&content.object.table_id,&content.object.object_id]).await?.ok_or_else(||anyhow::anyhow!("GC_REFERENCE_INVENTORY_REQUIRED"))?;
        ensure!(
            row.get::<_, Value>("content") == serde_json::to_value(content)?,
            "CONTENT_MISMATCH"
        );
        let version: ObjectVersion = serde_json::from_value(row.get("physical_version"))?;
        storage::qualified_s3_version(&version)?;
        self.check().await?;
        Ok(version)
    }
}
struct Heartbeat(tokio::task::JoinHandle<()>);
impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn heartbeat(grant: Arc<Grant>) -> Heartbeat {
    Heartbeat(tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let result=async {
                grant.check().await?;
                let count=grant.db.get().await?.execute("UPDATE moria.runtime_dataset_maintenance_runs SET lease_until=LEAST(clock_timestamp()+interval '30 seconds',deadline) WHERE id=$1 AND owner=$2 AND fence=$3 AND incarnation=$4 AND status='preparing' AND lease_until>clock_timestamp() AND deadline>clock_timestamp()",&[&grant.id,&grant.owner,&grant.fence,&grant.incarnation]).await?;
                ensure!(count==1,"GC_LEASE_LOST");Ok::<_,anyhow::Error>(())
            }.await;
            if result.is_err() {
                grant.cancel.cancel();
                break;
            }
        }
    }))
}

pub(super) async fn approve(db: &Database, input: Approval) -> Result<Value> {
    db.get()
        .await?
        .query_one("SELECT moria.runtime_recovery_operator()", &[])
        .await?;
    ensure!(
        !input.request_key.is_empty()
            && input.request_key.len() <= 128
            && !input.environment.is_empty()
            && input.environment.len() <= 256
            && (86400..=315360000).contains(&input.retain_seconds)
            && (30..=604800).contains(&input.recheck_seconds),
        "GC_POLICY_INVALID"
    );
    let dataset = input.dataset.to_string();
    let mut client = db.get().await?;
    let binding=client.query_opt("SELECT n.warehouse_id,n.table_id,n.revision AS namespace_revision,b.revision,b.source_digest,b.physical_binding_fingerprint,h.revision AS head_revision FROM moria.native_dataset_bindings b JOIN moria.native_storage_namespaces n ON n.environment=b.environment AND n.resource=b.resource::text AND n.warehouse_id=b.warehouse_id AND n.table_id=b.table_id AND n.revision=b.namespace_revision JOIN moria.dataset_heads h ON h.environment=b.environment AND h.dataset=b.resource::text WHERE b.environment=$1 AND b.resource=$2 AND b.enabled AND n.enabled AND n.purpose='dataset-source'",&[&input.environment,&input.dataset]).await?.ok_or_else(||anyhow::anyhow!("GC_UNSUPPORTED_CLASS"))?;
    let namespace = IndexNamespace {
        warehouse_id: binding.get("warehouse_id"),
        table_id: binding.get("table_id"),
        revision: binding.get("namespace_revision"),
    };
    let physical = fingerprint(&storage::configured_physical_binding(&namespace)?)?;
    ensure!(
        storage::configured_physical_binding(&namespace)?["backend"] == "s3",
        "CONDITIONAL_DELETE_UNSUPPORTED"
    );
    ensure!(
        physical == binding.get::<_, String>("physical_binding_fingerprint"),
        "GC_PROVIDER_CHANGED"
    );
    let head: i64 = binding.get("head_revision");
    let mut after = 0i64;
    loop {
        let rows=client.query("SELECT h.revision,r.key,l.physical_fingerprint,r.reader_profile,l.sealed FROM moria.snapshot_history h LEFT JOIN moria.runtime_dataset_roots r ON r.key=moria.runtime_dataset_root_key(h.metadata_location) LEFT JOIN moria.runtime_dataset_root_locations l ON l.root=r.key AND l.physical_fingerprint=$5 AND l.incarnation=(SELECT incarnation FROM moria.indexing_instance) LEFT JOIN moria.runtime_dataset_expired_roots e ON e.root=r.key WHERE h.environment=$1 AND h.dataset=$2 AND h.revision>$3 AND h.revision<=$4 AND e.root IS NULL ORDER BY h.revision LIMIT 256",&[&input.environment,&dataset,&after,&head,&physical]).await?;
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            ensure!(
                row.get::<_, Option<String>>("key").is_some()
                    && row
                        .get::<_, Option<String>>("physical_fingerprint")
                        .as_deref()
                        == Some(&physical)
                    && row.get::<_, Option<String>>("reader_profile").as_deref()
                        == Some("iceberg-v2-cow-flat-bounded-v2")
                    && row.get::<_, Option<bool>>("sealed") == Some(true),
                "GC_UNTRACKED_ROOT"
            );
        }
        after = rows.last().unwrap().get("revision");
    }
    let definition = json!({"class":"managed-native-dataset","environment":input.environment,"dataset":dataset,"namespace":namespace,"physicalFingerprint":physical,"bindingRevision":binding.get::<_,String>("revision"),"sourceDigest":binding.get::<_,String>("source_digest"),"retainSeconds":input.retain_seconds.to_string(),"recheckSeconds":input.recheck_seconds});
    let revision = fingerprint(&definition)?;
    let tx = client.transaction().await?;
    tx.query_one("SELECT moria.runtime_recovery_check()", &[])
        .await?;
    tx.query_one(
        "SELECT moria.runtime_dataset_kind($1,$2)",
        &[&input.environment, &dataset],
    )
    .await?;
    ensure!(tx.query_one("SELECT EXISTS(SELECT 1 FROM moria.dataset_heads h JOIN moria.native_dataset_bindings b ON b.environment=h.environment AND b.resource::text=h.dataset WHERE h.environment=$1 AND h.dataset=$2 AND h.revision=$3 AND b.enabled AND b.revision=$4 AND b.physical_binding_fingerprint=$5 FOR SHARE OF h,b)",&[&input.environment,&dataset,&head,&binding.get::<_,String>("revision"),&physical]).await?.get::<_,bool>(0),"HEAD_CONFLICT");
    tx.execute("INSERT INTO moria.runtime_dataset_retention_policies(id,request_key,environment,dataset,namespace,physical_fingerprint,revision,incarnation,binding_revision,source_digest,retain_seconds,recheck_seconds,approved_role) SELECT $1,$2,$3,$4,$5,$6,$7,incarnation,$8,$9,$10,$11,session_user FROM moria.indexing_instance ON CONFLICT(request_key) DO NOTHING",&[&Uuid::new_v4(),&input.request_key,&input.environment,&dataset,&serde_json::to_value(&namespace)?,&physical,&revision,&binding.get::<_,String>("revision"),&binding.get::<_,String>("source_digest"),&(input.retain_seconds as i64),&(input.recheck_seconds as i32)]).await?;
    let row=tx.query_one("SELECT p.id,p.revision,p.enabled,p.incarnation=i.incarnation AS current_incarnation FROM moria.runtime_dataset_retention_policies p CROSS JOIN moria.indexing_instance i WHERE p.request_key=$1",&[&input.request_key]).await?;
    ensure!(
        row.get::<_, String>("revision") == revision && row.get::<_, bool>("current_incarnation"),
        "GC_POLICY_CONFLICT"
    );
    let result = json!({"policy":row.get::<_,Uuid>("id"),"revision":revision,"enabled":row.get::<_,bool>("enabled"),"namespace":namespace,"physicalFingerprint":physical});
    tx.commit().await?;
    Ok(result)
}

async fn begin(db: &Database, input: &Expiration) -> Result<(Option<Arc<Grant>>, Value)> {
    ensure!(
        !input.request_key.is_empty()
            && input.request_key.len() <= 128
            && (1..=64).contains(&input.maximum_roots)
            && (30..=86400).contains(&input.time_seconds),
        "GC_POLICY_INVALID"
    );
    let before: i64 = input.before_revision.parse()?;
    ensure!(
        before >= 1 && before.to_string() == input.before_revision,
        "REVISION_INVALID"
    );
    let request = fingerprint(
        &json!({"policy":input.policy,"beforeRevision":input.before_revision,"maximumRoots":input.maximum_roots,"timeSeconds":input.time_seconds}),
    )?;
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    let state: Value = tx
        .query_one(
            "SELECT moria.runtime_dataset_policy_check($1)",
            &[&input.policy],
        )
        .await?
        .get(0);
    let environment = state["environment"].as_str().unwrap();
    let dataset = state["dataset"].as_str().unwrap();
    let head=tx.query_one("SELECT revision,metadata_location FROM moria.dataset_heads WHERE environment=$1 AND dataset=$2 FOR SHARE",&[&environment,&dataset]).await?;
    let expected: i64 = head.get("revision");
    let old: String = head.get("metadata_location");
    tx.execute("INSERT INTO moria.runtime_dataset_maintenance_runs(id,request_key,request_hash,policy,owner,fence,incarnation,lease_until,deadline,expected_revision,old_metadata,status) SELECT $1,$2,$3,$4,$5,1,incarnation,LEAST(clock_timestamp()+interval '30 seconds',clock_timestamp()+make_interval(secs=>$6::bigint::double precision)),clock_timestamp()+make_interval(secs=>$6::bigint::double precision),$7,$8,'preparing' FROM moria.indexing_instance ON CONFLICT(request_key) DO NOTHING",&[&Uuid::new_v4(),&input.request_key,&request,&input.policy,&input.owner,&(input.time_seconds as i64),&expected,&old]).await?;
    let row=tx.query_one("SELECT *,lease_until>clock_timestamp() AS live,deadline>clock_timestamp() AS within_deadline FROM moria.runtime_dataset_maintenance_runs WHERE request_key=$1 FOR UPDATE",&[&input.request_key]).await?;
    ensure!(
        row.get::<_, String>("request_hash") == request,
        "GC_POLICY_CONFLICT"
    );
    if row.get::<_, String>("status") == "committed" {
        let receipt = row.get("receipt");
        tx.commit().await?;
        return Ok((None, receipt));
    }
    ensure!(
        row.get::<_, String>("status") == "preparing",
        "GC_EXPIRATION_FAILED"
    );
    ensure!(row.get::<_, bool>("within_deadline"), "GC_DEADLINE");
    let id: Uuid = row.get("id");
    let old_owner: Uuid = row.get("owner");
    let mut fence: i64 = row.get("fence");
    let incarnation: Uuid = row.get("incarnation");
    if row.get::<_, bool>("live") {
        ensure!(old_owner == input.owner, "GC_PRODUCER_ACTIVE");
    } else {
        fence += 1;
        tx.execute("UPDATE moria.runtime_dataset_maintenance_runs SET owner=$2,fence=$3,lease_until=LEAST(clock_timestamp()+interval '30 seconds',deadline) WHERE id=$1",&[&id,&input.owner,&fence]).await?;
    }
    let actual: Value = tx
        .query_one(
            "SELECT moria.runtime_dataset_maintenance_check($1,$2,$3,$4)",
            &[&id, &input.owner, &fence, &incarnation],
        )
        .await?
        .get(0);
    let pin=tx.query_one("SELECT revision FROM moria.snapshot_history WHERE environment=$1 AND dataset=$2 AND metadata_location=$3 ORDER BY revision DESC LIMIT 1",&[&environment,&dataset,&old]).await?.get::<_,i64>(0);
    tx.query_one(
        "SELECT moria.runtime_dataset_root_pin($1,$2,$3,'maintenance',$4)",
        &[&environment, &dataset, &pin, &id.to_string()],
    )
    .await?;
    tx.commit().await?;
    let namespace = serde_json::from_value(actual["namespace"].clone())?;
    Ok((
        Some(Arc::new(Grant {
            db: db.clone(),
            id,
            owner: input.owner,
            fence,
            incarnation,
            namespace,
            state: actual,
            cancel: Cancellation::default(),
        })),
        Value::Null,
    ))
}

async fn bytes(
    store: &dyn ObjectStore,
    grant: Arc<Grant>,
    content: &SealedObject,
) -> Result<Value> {
    ensure!(content.size <= 8 * 1024 * 1024, "METADATA_BYTE_LIMIT");
    let mut data = Vec::with_capacity(content.size as usize);
    let mut stream = store
        .read_object(content.object.clone(), grant, Cancellation::default())
        .await?;
    while let Some(frame) = stream.next().await {
        let frame = frame?;
        ensure!(
            frame.len() <= content.size as usize - data.len(),
            "CONTENT_MISMATCH"
        );
        data.extend_from_slice(&frame);
    }
    ensure!(
        data.len() as u64 == content.size
            && format!("sha256:{:x}", Sha256::digest(&data)) == content.sha256,
        "CONTENT_MISMATCH"
    );
    Ok(serde_json::from_slice(&data)?)
}
async fn put(store: &dyn ObjectStore, grant: Arc<Grant>, value: &Value) -> Result<SealedObject> {
    let data = canonical_json(value)?.into_bytes();
    ensure!(data.len() <= 8 * 1024 * 1024, "METADATA_BYTE_LIMIT");
    let object = LogicalObject::new(
        &grant.namespace.warehouse_id,
        &grant.namespace.table_id,
        &Uuid::new_v4().to_string(),
    )?;
    let mut writer = store
        .begin_object(
            object,
            data.len() as u64,
            format!("sha256:{:x}", Sha256::digest(&data)),
            grant.clone(),
            grant.cancel.clone(),
        )
        .await?;
    for frame in data.chunks(1024 * 1024) {
        writer.append(frame).await?;
    }
    writer.seal().await
}
fn prune_metadata(mut value: Value, keep: &std::collections::BTreeSet<i64>) -> Result<Value> {
    let original = value.clone();
    let current = value["current-snapshot-id"]
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("SNAPSHOT_MISMATCH"))?;
    ensure!(keep.contains(&current), "SNAPSHOT_MISMATCH");
    if let Some(refs) = value["refs"].as_object() {
        for reference in refs.values() {
            ensure!(
                reference["snapshot-id"]
                    .as_i64()
                    .is_some_and(|id| keep.contains(&id)),
                "UNRESOLVED_SNAPSHOT_REFERENCE"
            );
        }
    }

    value["snapshots"]
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("MISSING_SNAPSHOTS"))?
        .retain(|v| {
            v["snapshot-id"]
                .as_i64()
                .is_some_and(|id| keep.contains(&id))
        });
    if let Some(log) = value["snapshot-log"].as_array_mut() {
        let mut seen = std::collections::BTreeSet::new();
        log.reverse();
        log.retain(|entry| {
            entry["snapshot-id"]
                .as_i64()
                .is_some_and(|id| keep.contains(&id) && seen.insert(id))
        });
        log.reverse();
    }
    value["metadata-log"] = json!([]);
    if value == original {
        return Ok(value);
    }
    let previous = value["last-updated-ms"].as_i64().unwrap_or(0);
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    value["last-updated-ms"] = json!(now.max(previous + 1));
    let _: iceberg::spec::TableMetadata = serde_json::from_value(value.clone())?;
    Ok(value)
}

pub(super) async fn expire(db: &Database, input: Expiration) -> Result<Value> {
    let (grant, replay) = begin(db, &input).await?;
    let Some(grant) = grant else {
        return Ok(replay);
    };
    let _heartbeat = heartbeat(grant.clone());
    let result = expire_current(grant.clone(), &input).await;
    if result.is_err() {
        let _=db.get().await?.execute("UPDATE moria.runtime_dataset_maintenance_runs SET status='failed',finished_at=clock_timestamp(),error='GC_EXPIRATION_FAILED' WHERE id=$1 AND owner=$2 AND fence=$3 AND status='preparing'",&[&grant.id,&grant.owner,&grant.fence]).await;
    }
    result
}
async fn commit_without_head(
    grant: Arc<Grant>,
    input: &Expiration,
    selected: &[String],
) -> Result<Value> {
    let mut client = grant.db.get().await?;
    let tx = client.transaction().await?;
    super::admission(tx.client(), true).await?;
    let current: Value = tx
        .query_one(
            "SELECT moria.runtime_dataset_maintenance_check($1,$2,$3,$4)",
            &[&grant.id, &grant.owner, &grant.fence, &grant.incarnation],
        )
        .await?
        .get(0);
    ensure!(current == grant.state, "GC_POLICY_CHANGED");
    for key in selected {
        ensure!(
            !tx.query_one(
                "SELECT moria.runtime_dataset_root_pinned_except($1,$2)",
                &[&key, &grant.id]
            )
            .await?
            .get::<_, bool>(0),
            "GC_REFERENCE_PRESENT"
        );
        tx.execute("INSERT INTO moria.runtime_dataset_expired_roots(root,policy,maintenance) VALUES($1,$2,$3)",&[&key,&input.policy,&grant.id]).await?;
    }
    let receipt =
        json!({"run":grant.id,"policy":input.policy,"expiredRoots":selected,"headChanged":false});
    tx.execute("UPDATE moria.runtime_dataset_maintenance_runs SET status='committed',finished_at=clock_timestamp(),receipt=$4 WHERE id=$1 AND owner=$2 AND fence=$3 AND status='preparing'",&[&grant.id,&grant.owner,&grant.fence,&receipt]).await?;
    tx.commit().await?;
    Ok(receipt)
}
async fn expire_current(grant: Arc<Grant>, input: &Expiration) -> Result<Value> {
    grant.check().await?;
    let environment = grant.state["environment"].as_str().unwrap();
    let dataset = grant.state["dataset"].as_str().unwrap();
    let old_location = grant.state["oldMetadata"].as_str().unwrap();
    let expected: i64 = grant.state["expectedRevision"].as_str().unwrap().parse()?;
    let before: i64 = input.before_revision.parse()?;
    let roots=grant.db.get().await?.query("SELECT DISTINCT r.key,r.snapshot_id FROM moria.runtime_dataset_roots r JOIN moria.snapshot_history h ON h.metadata_location=r.metadata_location AND h.environment=r.environment AND h.dataset=r.dataset LEFT JOIN moria.runtime_dataset_expired_roots e ON e.root=r.key WHERE r.environment=$1 AND r.dataset=$2 AND h.revision<$3 AND h.revision<=$4 AND e.root IS NULL AND r.retention_origin+make_interval(secs=>$6::bigint::double precision)<=clock_timestamp() AND NOT moria.runtime_dataset_root_pinned_except(r.key,$5) ORDER BY r.key LIMIT $7",&[&environment,&dataset,&before,&expected,&grant.id,&grant.state["retainSeconds"].as_str().unwrap().parse::<i64>()?,&(input.maximum_roots as i64)]).await?;
    let selected: Vec<String> = roots.iter().map(|r| r.get("key")).collect();
    let store = storage::configured_store_with_fingerprint(&grant.namespace)?.0;
    ensure!(
        store.physical_fingerprint()? == grant.state["physicalFingerprint"].as_str().unwrap(),
        "GC_PROVIDER_CHANGED"
    );
    let old_ref = NativeSnapshotRef::decode(old_location)?;
    let old_root = catalog::snapshot_root(
        store.as_ref(),
        grant.clone(),
        &old_ref,
        grant.cancel.clone(),
    )
    .await?;
    ensure!(
        old_root.proof.reader_profile == "iceberg-v2-cow-flat-bounded-v2",
        "GC_UNSUPPORTED_CLASS"
    );
    let original = bytes(store.as_ref(), grant.clone(), &old_root.proof.metadata).await?;
    let table: iceberg::spec::TableMetadata = serde_json::from_value(original.clone())?;
    let schema: iceberg::spec::Schema =
        serde_json::from_value(grant.state["icebergSchema"].clone())?;
    ensure!(
        table.current_schema().as_ref() == &schema
            && table.uuid().to_string() == grant.state["tableUuid"].as_str().unwrap(),
        "SCHEMA_CHANGED"
    );
    let mut keep = std::collections::BTreeSet::from([old_root.proof.snapshot_id]);
    let retained=grant.db.get().await?.query("SELECT DISTINCT r.snapshot_id FROM moria.runtime_dataset_roots r JOIN moria.snapshot_history h ON h.metadata_location=r.metadata_location AND h.environment=r.environment AND h.dataset=r.dataset LEFT JOIN moria.runtime_dataset_expired_roots e ON e.root=r.key WHERE r.environment=$1 AND r.dataset=$2 AND e.root IS NULL AND h.revision>=$3",&[&environment,&dataset,&before]).await?;
    for row in retained {
        keep.insert(row.get::<_, String>(0).parse::<i64>()?);
    }
    // Unmapped imported snapshots must remain in current metadata until a qualified historical root exists.
    let known=grant.db.get().await?.query("SELECT DISTINCT r.snapshot_id FROM moria.runtime_dataset_roots r JOIN moria.snapshot_history h ON h.metadata_location=r.metadata_location AND h.environment=r.environment AND h.dataset=r.dataset JOIN moria.runtime_dataset_root_locations l ON l.root=r.key AND l.sealed AND l.incarnation=(SELECT incarnation FROM moria.indexing_instance) LEFT JOIN moria.runtime_dataset_expired_roots e ON e.root=r.key WHERE r.environment=$1 AND r.dataset=$2 AND r.sealed AND e.root IS NULL AND l.physical_fingerprint=$3",&[&environment,&dataset,&grant.state["physicalFingerprint"].as_str().unwrap()]).await?.into_iter().map(|row|row.get::<_,String>(0)).collect::<std::collections::BTreeSet<_>>();
    for snapshot in original["snapshots"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("MISSING_SNAPSHOTS"))?
    {
        let id = snapshot["snapshot-id"]
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("SNAPSHOT_MISMATCH"))?;
        if !known.contains(&id.to_string()) {
            keep.insert(id);
        }
    }
    if let Some(refs) = original["refs"].as_object() {
        for reference in refs.values() {
            keep.insert(
                reference["snapshot-id"]
                    .as_i64()
                    .ok_or_else(|| anyhow::anyhow!("UNRESOLVED_SNAPSHOT_REFERENCE"))?,
            );
        }
    }
    let pruned = prune_metadata(original.clone(), &keep)?;
    if pruned == original {
        return commit_without_head(grant, input, &selected).await;
    }
    let metadata = put(store.as_ref(), grant.clone(), &pruned).await?;
    let scratch_root = std::env::var("MORIA_NATIVE_SNAPSHOT_SCRATCH_ROOT")
        .map_err(|_| anyhow::anyhow!("SNAPSHOT_SCRATCH_UNAVAILABLE"))?;
    let scratch = PosixStore::provision(
        std::path::Path::new(&scratch_root),
        &grant.namespace.warehouse_id,
        &grant.namespace.table_id,
        StorageLimits {
            max_object_bytes: 128 * 1024 * 1024,
            chunk_bytes: 1024 * 1024,
        },
    )?;
    let limits: ClosureLimits = serde_json::from_str(
        &std::env::var("MORIA_NATIVE_CLOSURE_LIMITS")
            .map_err(|_| anyhow::anyhow!("SNAPSHOT_BUDGET_UNAVAILABLE"))?,
    )?;
    let mut source = ClosureSpool::create(
        &scratch,
        LogicalObject::new(
            &grant.namespace.warehouse_id,
            &grant.namespace.table_id,
            &Uuid::new_v4().to_string(),
        )?,
        limits,
        grant.clone(),
        grant.cancel.clone(),
    )
    .await?;
    let mut current = old_root.last_page.clone();
    let mut count = 0u64;
    for ordinal in (0..old_root.pages).rev() {
        let page = catalog::snapshot_page(
            store.as_ref(),
            grant.clone(),
            &current
                .take()
                .ok_or_else(|| anyhow::anyhow!("INVALID_NATIVE_SNAPSHOT_ROOT"))?,
            ordinal,
            grant.cancel.clone(),
        )
        .await?;
        count += page.entries.len() as u64;
        source.append_page(&page.entries).await?;
        current = page.previous;
    }
    ensure!(
        current.is_none() && count == old_root.proof.closure.entries,
        "INVALID_NATIVE_SNAPSHOT_ROOT"
    );
    let closure = source
        .seal_verified(store.as_ref(), grant.clone(), grant.as_ref())
        .await?;
    ensure!(
        closure.sha256 == old_root.proof.closure.sha256
            && closure.entries == old_root.proof.closure.entries
            && closure.referenced_bytes == old_root.proof.closure.referenced_bytes,
        "CLOSURE_MISMATCH"
    );
    let pin = SnapshotPin {
        metadata,
        table_uuid: table.uuid(),
        snapshot_id: old_root.proof.snapshot_id,
    };
    let mut destination = ClosureSpool::create(
        &scratch,
        LogicalObject::new(
            &grant.namespace.warehouse_id,
            &grant.namespace.table_id,
            &Uuid::new_v4().to_string(),
        )?,
        limits,
        grant.clone(),
        grant.cancel.clone(),
    )
    .await?;
    snapshot::prune_snapshot_closure(
        &source,
        &mut destination,
        store.clone(),
        grant.clone(),
        pin.clone(),
    )
    .await?;
    destination
        .seal_verified(store.as_ref(), grant.clone(), grant.as_ref())
        .await?;
    let proof =
        snapshot::verify_retained_subset(&destination, store.clone(), grant.clone(), pin).await?;
    let (reference, root) = catalog::write_snapshot_root(
        store.clone(),
        grant.clone(),
        grant.clone(),
        &grant.namespace,
        &destination,
        proof,
        grant.cancel.clone(),
    )
    .await?;
    let retention_origin=grant.db.get().await?.query_one("SELECT retention_origin FROM moria.runtime_dataset_roots WHERE key=moria.runtime_dataset_root_key($1)",&[&old_location]).await?.get::<_,SystemTime>(0);
    super::register_root(
        &grant.db,
        &grant.namespace,
        environment,
        dataset,
        &grant.scope(),
        store,
        grant.clone(),
        &reference,
        &root,
        &destination,
        Some(retention_origin),
        grant.cancel.clone(),
    )
    .await?;
    grant.check().await?;
    let location = reference.encode()?;
    if let Some(binding)=grant.db.get().await?.query_opt("SELECT key_schema_hash,cardinality(primary_key)>0 FROM moria.native_dataset_bindings WHERE environment=$1 AND resource::text=$2",&[&environment,&dataset]).await? && binding.get::<_,bool>(1) {
            let key_hash:String=binding.get(0);
            let changed=grant.db.get().await?.execute("INSERT INTO moria.runtime_dataset_key_certificates(root,key_schema_hash,snapshot_id,key_rows) SELECT moria.runtime_dataset_root_key($1),key_schema_hash,snapshot_id,key_rows FROM moria.runtime_dataset_key_certificates WHERE root=moria.runtime_dataset_root_key($2) AND key_schema_hash=$3 AND snapshot_id=$4 ON CONFLICT DO NOTHING",&[&location,&old_location,&key_hash,&root.proof.snapshot_id.to_string()]).await?;
            ensure!(changed==1,"DATASET_PRIMARY_KEY_PROOF_REQUIRED");
    }

    let binding = grant.db.get().await?.query_one("SELECT primary_key,key_schema_hash,iceberg_schema,revision FROM moria.native_dataset_bindings WHERE environment=$1 AND resource::text=$2 AND enabled", &[&environment,&dataset]).await?;
    let delta_profile = crate::dataset_commit_delta::profile(
        table.current_schema(),
        &binding.get::<_, Value>(2),
        &binding.get::<_, String>(3),
        &binding.get::<_, Vec<String>>(0),
        &binding.get::<_, String>(1),
    )?;
    let equivalence = crate::dataset_commit_delta::metadata_prune_receipt(
        &original,
        &pruned,
        &grant
            .db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_dataset_root_key($1)",
                &[&old_location],
            )
            .await?
            .get::<_, String>(0),
        &grant
            .db
            .get()
            .await?
            .query_one("SELECT moria.runtime_dataset_root_key($1)", &[&location])
            .await?
            .get::<_, String>(0),
    )?;
    let mut client = grant.db.get().await?;
    let tx = client.transaction().await?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended('moria/environment/'||$1,0))",
        &[&environment],
    )
    .await?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock_shared(hashtextextended('moria/filesystem',0))",
        &[],
    )
    .await?;
    tx.query_one(
        "SELECT 1 FROM moria.environments WHERE id=$1 FOR UPDATE",
        &[&environment],
    )
    .await?;
    super::admission(tx.client(), true).await?;
    let fresh: Value = tx
        .query_one(
            "SELECT moria.runtime_dataset_maintenance_check($1,$2,$3,$4)",
            &[&grant.id, &grant.owner, &grant.fence, &grant.incarnation],
        )
        .await?
        .get(0);
    ensure!(fresh == grant.state, "GC_POLICY_CHANGED");
    for key in &selected {
        ensure!(
            !tx.query_one(
                "SELECT moria.runtime_dataset_root_pinned_except($1,$2)",
                &[&key, &grant.id]
            )
            .await?
            .get::<_, bool>(0),
            "GC_REFERENCE_PRESENT"
        );
    }
    let updated=tx.query_opt("UPDATE moria.dataset_heads SET revision=revision+1,metadata_location=$4,snapshot_id=$5 WHERE environment=$1 AND dataset=$2 AND revision=$3 AND metadata_location=$6 RETURNING revision",&[&environment,&dataset,&expected,&location,&root.proof.snapshot_id.to_string(),&old_location]).await?.ok_or_else(||anyhow::anyhow!("HEAD_CONFLICT"))?.get::<_,i64>(0);
    tx.execute("INSERT INTO moria.snapshot_history(environment,dataset,revision,metadata_location,snapshot_id,execution,lineage) VALUES($1,$2,$3,$4,$5,$6,$7)",&[&environment,&dataset,&updated,&location,&root.proof.snapshot_id.to_string(),&format!("dataset-maintenance/{}",grant.id),&json!({dataset:expected.to_string()})]).await?;
    for key in &selected {
        tx.execute("INSERT INTO moria.runtime_dataset_expired_roots(root,policy,maintenance) VALUES($1,$2,$3)",&[&key,&input.policy,&grant.id]).await?;
    }
    let revision: i64 = tx
        .query_one(
            "UPDATE moria.environments SET revision=revision+1 WHERE id=$1 RETURNING revision",
            &[&environment],
        )
        .await?
        .get(0);
    tx.query_one(
        "SELECT moria.indexing_enqueue($1,$2,$3,$4)",
        &[&environment, &dataset, &updated, &revision],
    )
    .await?;
    let receipt = json!({"run":grant.id,"policy":input.policy,"revision":revision.to_string(),"outputs":[{"dataset":dataset,"revision":updated.to_string(),"snapshotId":root.proof.snapshot_id.to_string()}],"expiredRoots":selected,"headChanged":true});
    tx.query_one(
        "SELECT moria.runtime_dataset_maintenance_check($1,$2,$3,$4)",
        &[&grant.id, &grant.owner, &grant.fence, &grant.incarnation],
    )
    .await?;
    tx.execute("UPDATE moria.runtime_dataset_maintenance_runs SET status='committed',finished_at=clock_timestamp(),new_metadata=$4,receipt=$5 WHERE id=$1 AND owner=$2 AND fence=$3 AND status='preparing'",&[&grant.id,&grant.owner,&grant.fence,&location,&receipt]).await?;
    tx.execute("INSERT INTO moria.publications(environment,execution,request_hash,receipt) VALUES($1,$2,$3,$4)",&[&environment,&format!("dataset-maintenance/{}",grant.id),&fingerprint(&serde_json::to_value(input)?)?,&receipt]).await?;
    let semantics: Value = tx
        .query_one(
            "SELECT moria.runtime_dataset_commit_record_maintenance($1,$2,$3,$4)",
            &[&grant.id, &delta_profile, &equivalence, &receipt],
        )
        .await?
        .get(0);
    crate::dataset_commit_delta::validate_semantics(semantics)?;
    tx.commit().await?;
    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metadata_pruning_retains_current_snapshot_and_schema() {
        let raw = json!({"format-version":2,"table-uuid":Uuid::new_v4(),"location":"moria://w/t","last-sequence-number":2,"last-updated-ms":1,"last-column-id":1,"schemas":[{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"long"}]}],"current-schema-id":0,"partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,"last-partition-id":999,"properties":{},"current-snapshot-id":2,"snapshots":[{"snapshot-id":1,"sequence-number":1,"timestamp-ms":1,"manifest-list":"moria://w/t/ml1","summary":{"operation":"append"},"schema-id":0},{"snapshot-id":2,"parent-snapshot-id":1,"sequence-number":2,"timestamp-ms":2,"manifest-list":"moria://w/t/ml2","summary":{"operation":"overwrite"},"schema-id":0}],"snapshot-log":[{"timestamp-ms":1,"snapshot-id":1},{"timestamp-ms":2,"snapshot-id":2}],"metadata-log":[{"timestamp-ms":1,"metadata-file":"moria://w/t/old"}],"sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0,"refs":{"main":{"snapshot-id":2,"type":"branch"}}});
        let pruned = prune_metadata(raw.clone(), &std::collections::BTreeSet::from([2])).unwrap();
        assert_eq!(pruned["schemas"], raw["schemas"]);
        assert_eq!(pruned["snapshots"].as_array().unwrap().len(), 1);
        assert_eq!(pruned["snapshots"][0], raw["snapshots"][1]);
        assert_eq!(pruned["metadata-log"], json!([]));
        assert_eq!(
            prune_metadata(pruned.clone(), &std::collections::BTreeSet::from([2])).unwrap(),
            pruned
        );
        let mut branch = raw.clone();
        branch["refs"]["retained"] = json!({"snapshot-id":1,"type":"tag"});
        assert!(prune_metadata(branch.clone(), &std::collections::BTreeSet::from([2])).is_err());
        let preserved =
            prune_metadata(branch.clone(), &std::collections::BTreeSet::from([1, 2])).unwrap();
        assert_eq!(preserved["snapshots"], branch["snapshots"]);
        assert_eq!(preserved["refs"], branch["refs"]);

        assert!(prune_metadata(raw, &std::collections::BTreeSet::from([1])).is_err());
    }
}
