//! Operator-only retirement of exact expired, uncommitted filesystem uploads.
use crate::{
    canonical::fingerprint,
    database::Database,
    recovery::RETENTION_ADMISSION,
    storage::{
        self, AbortAuthority, Cancellation, DiscoveryAuthority, IndexNamespace, LogicalObject,
        ObjectVersion, ReadAuthority, RemoveAuthority,
    },
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{fmt, sync::Arc, time::Duration};
use tokio_postgres::Client;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PolicyRequest {
    request_key: String,
    namespace: IndexNamespace,
    minimum_age_seconds: u32,
    recheck_seconds: u32,
}
fn validate(request: &PolicyRequest) -> Result<()> {
    ensure!(
        !request.request_key.is_empty()
            && request.request_key.len() <= 128
            && request.namespace.warehouse_id == "filesystem"
            && request.namespace.table_id == "content"
            && !request.namespace.revision.is_empty()
            && request.namespace.revision.len() <= 128
            && (86400..=31536000).contains(&request.minimum_age_seconds)
            && (30..=604800).contains(&request.recheck_seconds),
        "GC_POLICY_INVALID"
    );
    Ok(())
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
        let admission = Self(Some(db.get().await?));
        admission
            .query_one(
                "SELECT pg_advisory_lock(hashtextextended($1,0))",
                &[&RETENTION_ADMISSION],
            )
            .await?;
        admission
            .query_one(
                "SELECT pg_advisory_lock(hashtextextended('moria/filesystem',0))",
                &[],
            )
            .await?;
        admission
            .query_one("SELECT moria.native_filesystem_gc_operator()", &[])
            .await?;
        Ok(admission)
    }
    async fn release(mut self) -> Result<()> {
        ensure!(
            self.query_one(
                "SELECT pg_advisory_unlock(hashtextextended('moria/filesystem',0))",
                &[]
            )
            .await?
            .get::<_, bool>(0),
            "GC_ADMISSION_LOST"
        );
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
#[derive(Clone)]
pub struct FilesystemGc {
    pub db: Database,
}
impl FilesystemGc {
    pub async fn approve(&self, request: PolicyRequest) -> Result<Value> {
        validate(&request)?;
        let physical = fingerprint(&storage::configured_physical_binding(&request.namespace)?)?;
        let definition = json!({"class":"uncommitted-filesystem-upload","namespace":request.namespace,"physicalFingerprint":physical,"minimumAgeSeconds":request.minimum_age_seconds,"recheckSeconds":request.recheck_seconds});
        let revision = fingerprint(&definition)?;
        let admission = Admission::acquire(&self.db).await?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one("SELECT moria.runtime_recovery_check()", &[])
            .await?;
        tx.execute("INSERT INTO moria.native_filesystem_gc_policies(id,request_key,revision,namespace,physical_hash,minimum_age_seconds,recheck_seconds,incarnation,approved_role) SELECT $1,$2,$3,$4,$5,$6,$7,incarnation,session_user FROM moria.indexing_instance ON CONFLICT(request_key) DO NOTHING",&[&Uuid::new_v4(),&request.request_key,&revision,&json!(request.namespace),&physical,&(request.minimum_age_seconds as i64),&(request.recheck_seconds as i32)]).await?;
        let row=tx.query_one("SELECT id,revision,enabled,incarnation=(SELECT incarnation FROM moria.indexing_instance) current_incarnation,approved_role=session_user own_role FROM moria.native_filesystem_gc_policies WHERE request_key=$1",&[&request.request_key]).await?;
        ensure!(
            row.get::<_, String>("revision") == revision
                && row.get::<_, bool>("current_incarnation")
                && row.get::<_, bool>("own_role"),
            "GC_POLICY_CONFLICT"
        );
        let result = json!({"policy":row.get::<_,Uuid>("id"),"revision":revision,"enabled":row.get::<_,bool>("enabled"),"definition":definition});
        tx.commit().await?;
        admission.release().await?;
        Ok(result)
    }
    pub async fn disable(&self, policy: Uuid) -> Result<Value> {
        let client = self.db.get().await?;
        ensure!(client.execute("UPDATE moria.native_filesystem_gc_policies SET enabled=false WHERE id=$1 AND approved_role=session_user",&[&policy]).await?==1,"GC_POLICY_UNKNOWN");
        Ok(json!({"disabled":true}))
    }
    pub async fn plan(&self, policy: Uuid, after: Option<Uuid>, limit: u32) -> Result<Value> {
        ensure!((1..=64).contains(&limit), "GC_POLICY_INVALID");
        let admission = Admission::acquire(&self.db).await?;
        admission
            .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE; SET LOCAL statement_timeout='5s'")
            .await?;
        let tx = &*admission;
        let p=tx.query_opt("SELECT physical_hash FROM moria.native_filesystem_gc_policies WHERE id=$1 AND enabled AND approved_role=session_user AND incarnation=(SELECT incarnation FROM moria.indexing_instance) FOR SHARE",&[&policy]).await?.ok_or_else(||anyhow::anyhow!("GC_POLICY_CHANGED"))?;
        let rows=tx.query("SELECT id FROM moria.native_filesystem_uploads WHERE state IN ('writing','sealed') AND incarnation=(SELECT incarnation FROM moria.indexing_instance) AND lease_until<=clock_timestamp() AND physical_hash=$1 AND ($2::uuid IS NULL OR id>$2) ORDER BY id LIMIT $3 FOR UPDATE",&[&p.get::<_,String>(0),&after,&(limit as i64)]).await?;
        let mut last = None;
        let mut planned = 0u32;
        let mut referenced = 0u32;
        for row in &rows {
            let upload: Uuid = row.get(0);
            last = Some(upload);
            let present = tx
                .query_one(
                    "SELECT moria.native_filesystem_gc_reference_check($1,$2)",
                    &[&policy, &upload],
                )
                .await?
                .get::<_, bool>(0);
            if present {
                tx.execute("UPDATE moria.native_filesystem_gc_candidates SET first_unreferenced=NULL,status='referenced',expected_version=NULL WHERE upload=$1 AND policy=$2",&[&upload,&policy]).await?;
                referenced += 1;
                continue;
            }
            tx.execute("INSERT INTO moria.native_filesystem_upload_tombstones(upload,policy) VALUES($1,$2) ON CONFLICT DO NOTHING",&[&upload,&policy]).await?;
            ensure!(tx.query_one("SELECT policy=$2 FROM moria.native_filesystem_upload_tombstones WHERE upload=$1",&[&upload,&policy]).await?.get::<_,bool>(0),"GC_POLICY_CONFLICT");
            tx.execute("INSERT INTO moria.native_filesystem_gc_candidates(upload,policy,first_unreferenced,status) VALUES($1,$2,clock_timestamp(),'pending') ON CONFLICT(upload) DO UPDATE SET first_unreferenced=COALESCE(native_filesystem_gc_candidates.first_unreferenced,clock_timestamp()),status=CASE WHEN native_filesystem_gc_candidates.status='referenced' THEN 'pending' ELSE native_filesystem_gc_candidates.status END",&[&upload,&policy]).await?;
            planned += 1;
        }
        tx.batch_execute("COMMIT").await?;
        admission.release().await?;
        Ok(
            json!({"planned":planned,"referenced":referenced,"next":last,"complete":rows.len()<limit as usize}),
        )
    }
    pub async fn collect_once(&self, policy: Uuid, owner: Uuid, seconds: u32) -> Result<Value> {
        ensure!((1..=20).contains(&seconds), "GC_POLICY_INVALID");
        let admission = Admission::acquire(&self.db).await?;
        let backend = admission
            .query_one(
                "SELECT pid,backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()",
                &[],
            )
            .await?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let row=tx.query_opt("SELECT c.upload FROM moria.native_filesystem_gc_candidates c JOIN moria.native_filesystem_gc_policies p ON p.id=c.policy WHERE c.policy=$1 AND p.enabled AND p.approved_role=session_user AND p.incarnation=(SELECT incarnation FROM moria.indexing_instance) AND c.next_check<=clock_timestamp() AND c.first_unreferenced+make_interval(secs=>p.minimum_age_seconds::double precision)<=clock_timestamp() AND (c.status<>'running' OR c.lease_until<=clock_timestamp()) ORDER BY c.next_check,c.upload FOR UPDATE OF c SKIP LOCKED LIMIT 1",&[&policy]).await?;
        let Some(row) = row else {
            tx.commit().await?;
            admission.release().await?;
            return Ok(json!({"claimed":false}));
        };
        let upload: Uuid = row.get(0);
        if tx
            .query_one(
                "SELECT moria.native_filesystem_gc_reference_check($1,$2)",
                &[&policy, &upload],
            )
            .await?
            .get::<_, bool>(0)
        {
            tx.execute("UPDATE moria.native_filesystem_gc_candidates SET first_unreferenced=NULL,status='referenced',expected_version=NULL WHERE upload=$1",&[&upload]).await?;
            tx.commit().await?;
            admission.release().await?;
            return Ok(json!({"claimed":false,"referencePresent":true}));
        }
        let row=tx.query_one("UPDATE moria.native_filesystem_gc_candidates SET status='running',owner=$2,fence=fence+1,incarnation=(SELECT incarnation FROM moria.indexing_instance),lease_until=clock_timestamp()+interval '30 seconds',admission_pid=$3,admission_started=$4,expected_version=NULL,attempts=attempts+1 WHERE upload=$1 RETURNING fence,incarnation",&[&upload,&owner,&backend.get::<_,i32>("pid"),&backend.get::<_,std::time::SystemTime>("backend_start")]).await?;
        let mut guard = Claim {
            db: self.db.clone(),
            upload,
            owner,
            fence: row.get(0),
            incarnation: row.get(1),
            object: LogicalObject::new("filesystem", "content", "unresolved")?,
            namespace: IndexNamespace {
                warehouse_id: "filesystem".into(),
                table_id: "content".into(),
                revision: String::new(),
            },
            physical: String::new(),
            expected: None,
        };
        let value: Value = tx
            .query_one(
                "SELECT moria.native_filesystem_gc_check($1,$2,$3,$4)",
                &[&upload, &owner, &guard.fence, &guard.incarnation],
            )
            .await?
            .get(0);
        guard.object = serde_json::from_value(value["object"].clone())?;
        guard.namespace = serde_json::from_value(value["namespace"].clone())?;
        guard.physical = value["physicalFingerprint"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("GC_POLICY_CHANGED"))?
            .into();
        tx.commit().await?;
        drop(client);
        let cancel = Cancellation::default();
        let mut multipart_next: Option<String> = None;
        let mut multipart_aborted = 0usize;
        let mut multipart_supported = false;
        let operation = async {
            ensure!(
                fingerprint(&storage::configured_physical_binding(&guard.namespace)?)?
                    == guard.physical,
                "GC_PROVIDER_CHANGED"
            );
            let store = storage::configured_store_with_fingerprint(&guard.namespace)?.0;
            let journaled=self.db.get().await?.query_one("SELECT EXISTS(SELECT 1 FROM moria.native_filesystem_multipart_intents WHERE upload=$1 AND physical_hash=$2 AND location=$3::jsonb)",&[&upload,&guard.physical,&json!(guard.object)]).await?.get::<_,bool>(0);
            if journaled {
                // Aborting a page invalidates provider markers; the next bounded pass starts at the exact key again.
                match store
                    .discover_multipart(
                        guard.object.clone(),
                        None,
                        Arc::new(guard.clone()),
                        cancel.clone(),
                    )
                    .await
                {
                    Ok(page) => {
                        multipart_supported = true;
                        multipart_next = page.next_upload_id;
                        ensure!(page.upload_ids.len() <= 16, "RESOURCE_LIMIT");
                        let mut client = self.db.get().await?;
                        let tx = client.transaction().await?;
                        guard.check_in(tx.client()).await?;
                        for id in &page.upload_ids {
                            tx.execute("INSERT INTO moria.native_filesystem_gc_multipart(upload,upload_id) VALUES($1,$2) ON CONFLICT DO NOTHING",&[&upload,&id]).await?;
                        }
                        tx.commit().await?;
                        drop(client);
                        for id in page.upload_ids {
                            store
                                .abort_multipart(
                                    guard.object.clone(),
                                    id.clone(),
                                    Arc::new(guard.clone()),
                                    cancel.clone(),
                                )
                                .await?;
                            let mut client = self.db.get().await?;
                            let tx = client.transaction().await?;
                            guard.check_in(tx.client()).await?;
                            tx.execute("UPDATE moria.native_filesystem_gc_multipart SET aborted_at=COALESCE(aborted_at,clock_timestamp()) WHERE upload=$1 AND upload_id=$2",&[&upload,&id]).await?;
                            tx.commit().await?;
                            multipart_aborted += 1;
                        }
                    }
                    Err(error)
                        if error.root_cause().to_string() == "MULTIPART_DISCOVERY_UNSUPPORTED" => {}
                    Err(error) => return Err(error),
                }
            }
            let Some(version) = store.inspect(&guard.object, &guard, &cancel).await? else {
                return Ok::<_, anyhow::Error>(("absent", false, None));
            };
            ensure!(
                version.token.starts_with("s3v1/"),
                "CONDITIONAL_DELETE_UNSUPPORTED"
            );
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            guard.check_in(tx.client()).await?;
            tx.execute("UPDATE moria.native_filesystem_gc_candidates SET expected_version=$2 WHERE upload=$1",&[&upload,&json!(version)]).await?;
            tx.commit().await?;
            drop(client);
            guard.expected = Some(version.clone());
            let removed = store
                .remove_object(
                    guard.object.clone(),
                    version.clone(),
                    Arc::new(guard.clone()),
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
        let outcome = tokio::time::timeout(Duration::from_secs(seconds.into()), operation)
            .await
            .map_err(|_| anyhow::anyhow!("GC_DEADLINE"))
            .and_then(|result| result);
        let (status, removed, version) = match outcome {
            Ok(result) => result,
            Err(error) if error.root_cause().to_string() == "CONDITIONAL_DELETE_UNSUPPORTED" => {
                ("unsupported", false, None)
            }
            Err(error) => {
                cancel.cancel();
                return Err(error);
            }
        };
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        guard.check_in(tx.client()).await?;
        tx.execute("UPDATE moria.native_filesystem_gc_candidates c SET status=$2,lease_until=NULL,next_check=CASE WHEN $5::text IS NOT NULL THEN clock_timestamp() ELSE clock_timestamp()+make_interval(secs=>p.recheck_seconds) END,last_result=$3,last_removed_version=$4,multipart_after=$5 FROM moria.native_filesystem_gc_policies p WHERE c.upload=$1 AND p.id=c.policy",&[&upload,&if status=="unsupported"{"unsupported"}else{"watching"},&status,&version.as_ref().map(|v|json!(v)),&multipart_next]).await?;
        tx.commit().await?;
        admission.release().await?;
        Ok(
            json!({"claimed":true,"upload":upload,"result":status,"removed":removed,"version":version,"multipartAborted":multipart_aborted,"multipartSupported":multipart_supported,"multipartNext":multipart_next}),
        )
    }
}
#[derive(Clone)]
struct Claim {
    db: Database,
    upload: Uuid,
    owner: Uuid,
    fence: i64,
    incarnation: Uuid,
    object: LogicalObject,
    namespace: IndexNamespace,
    physical: String,
    expected: Option<ObjectVersion>,
}
impl fmt::Debug for Claim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilesystemRemovalClaim")
            .field("upload", &self.upload)
            .finish()
    }
}
impl Claim {
    async fn check_in(&self, client: &Client) -> Result<()> {
        let value: Value = client
            .query_one(
                "SELECT moria.native_filesystem_gc_check($1,$2,$3,$4)",
                &[&self.upload, &self.owner, &self.fence, &self.incarnation],
            )
            .await?
            .get(0);
        ensure!(
            value["object"] == json!(self.object)
                && value["namespace"] == json!(self.namespace)
                && value["physicalFingerprint"] == self.physical,
            "GC_POLICY_CHANGED"
        );
        if let Some(expected) = &self.expected {
            ensure!(value["expectedVersion"] == json!(expected), "GC_LEASE_LOST");
        }
        Ok(())
    }
}
#[async_trait]
impl DiscoveryAuthority for Claim {
    async fn check_discover(&self, object: &LogicalObject) -> Result<()> {
        ensure!(object == &self.object, "GC_OBJECT_DENIED");
        let client = self.db.get().await?;
        self.check_in(&client).await?;
        ensure!(client.query_one("SELECT EXISTS(SELECT 1 FROM moria.native_filesystem_multipart_intents WHERE upload=$1 AND physical_hash=$2 AND location=$3::jsonb)",&[&self.upload,&self.physical,&json!(self.object)]).await?.get::<_,bool>(0),"GC_OBJECT_DENIED");
        Ok(())
    }
}
#[async_trait]
impl AbortAuthority for Claim {
    async fn check_abort(&self, object: &LogicalObject, upload_id: &str) -> Result<()> {
        self.check_discover(object).await?;
        ensure!(self.db.get().await?.query_one("SELECT EXISTS(SELECT 1 FROM moria.native_filesystem_gc_multipart WHERE upload=$1 AND upload_id=$2)",&[&self.upload,&upload_id]).await?.get::<_,bool>(0),"GC_OBJECT_DENIED");
        Ok(())
    }
}
#[async_trait]
impl ReadAuthority for Claim {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        ensure!(object == &self.object, "GC_OBJECT_DENIED");
        let client = self.db.get().await?;
        self.check_in(&client).await
    }
}
#[async_trait]
impl RemoveAuthority for Claim {
    async fn check_remove(&self, object: &LogicalObject, version: &ObjectVersion) -> Result<()> {
        ensure!(
            object == &self.object && self.expected.as_ref() == Some(version),
            "GC_OBJECT_DENIED"
        );
        let client = self.db.get().await?;
        self.check_in(&client).await
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
    struct Plan {
        policy: Uuid,
        after: Option<Uuid>,
        limit: u32,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Once {
        policy: Uuid,
        owner: Uuid,
        time_seconds: u32,
    }
    ensure!(args.len() == 1, "GC_POLICY_INVALID");
    let mut bytes = vec![];
    tokio::io::stdin()
        .take(16385)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 16384, "GC_POLICY_INVALID");
    let gc = FilesystemGc { db };
    let result = match args[0].as_str() {
        "approve" => gc.approve(serde_json::from_slice(&bytes)?).await?,
        "disable" => {
            gc.disable(serde_json::from_slice::<Policy>(&bytes)?.policy)
                .await?
        }
        "plan" => {
            let request: Plan = serde_json::from_slice(&bytes)?;
            gc.plan(request.policy, request.after, request.limit)
                .await?
        }
        "once" => {
            let request: Once = serde_json::from_slice(&bytes)?;
            gc.collect_once(request.policy, request.owner, request.time_seconds)
                .await?
        }
        _ => anyhow::bail!("GC_POLICY_INVALID"),
    };
    println!("{result}");
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn policy_requires_orphan_grace_and_exact_filesystem_namespace() {
        let mut request = PolicyRequest {
            request_key: "policy".into(),
            namespace: IndexNamespace {
                warehouse_id: "filesystem".into(),
                table_id: "content".into(),
                revision: "v1".into(),
            },
            minimum_age_seconds: 86400,
            recheck_seconds: 30,
        };
        assert!(validate(&request).is_ok());
        request.minimum_age_seconds = 86399;
        assert!(validate(&request).is_err());
        request.minimum_age_seconds = 86400;
        request.namespace.table_id = "other".into();
        assert!(validate(&request).is_err());
    }
}
