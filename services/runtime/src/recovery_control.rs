use crate::{
    canonical::{canonical_json, fingerprint},
    database::Database,
    recovery::{RecoveryAuthority, RecoveryLease},
    recovery_content::{
        self, BackupAuthority, OfflineRecovery, RecoveredContentProof, RecoveryContentInventory,
        RecoveryContentLimits,
    },
    recovery_keys::{KeyRecoveryPlan, KeyRestoreProof, RecoveryKeys},
    recovery_metadata::{self, MetadataRestoreProof, RestoreIdentity},
    storage::{
        self, Cancellation, IndexNamespace, LogicalObject, ObjectReader, ReadAuthority,
        SealedObject,
    },
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RecoveryManifest {
    version: u32,
    set: Uuid,
    incarnation: Uuid,
    schema_checksum: String,
    metadata_dump: SealedObject,
    content_inventory: RecoveryContentInventory,
    keys: RecoveryKeys,
    exported_snapshot: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct CaptureRequest {
    request_key: String,
    owner: Uuid,
    retention_seconds: i64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RestoreRequest {
    restore: Uuid,
    manifest: SealedObject,
    target_namespaces: Vec<IndexNamespace>,
}
#[derive(Debug)]
struct ManifestAuthority(LogicalObject);
#[async_trait]
impl ReadAuthority for ManifestAuthority {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        ensure!(object == &self.0, "RECOVERY_OBJECT_DENIED");
        Ok(())
    }
}
fn schema_checksum() -> Result<String> {
    let schema: Value = serde_json::from_str(include_str!("../schema.json"))?;
    Ok(schema["checksum"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("RECOVERY_INVALID"))?
        .to_owned())
}
async fn manifest_bytes(
    reader: &dyn ObjectReader,
    authority: &dyn ReadAuthority,
    cancel: &Cancellation,
    sealed: &SealedObject,
) -> Result<Vec<u8>> {
    ensure!(
        sealed.size > 0 && sealed.size <= 256 * 1024,
        "RECOVERY_MANIFEST_CONFLICT"
    );
    let version = reader.version(&sealed.object, authority, cancel).await?;
    ensure!(version.size == sealed.size, "RECOVERY_MANIFEST_CONFLICT");
    let bytes = reader
        .range_at_version(&sealed.object, 0..sealed.size, &version, authority, cancel)
        .await?;
    ensure!(
        format!("sha256:{:x}", Sha256::digest(&bytes)) == sealed.sha256,
        "RECOVERY_MANIFEST_CONFLICT"
    );
    Ok(bytes.to_vec())
}
pub(crate) async fn capture(db: Database, request: CaptureRequest) -> Result<Value> {
    let keys = KeyRecoveryPlan::configured()?;
    let lease = RecoveryAuthority { db: db.clone() }
        .create(
            &request.request_key,
            request.owner,
            &keys.fingerprint()?,
            request.retention_seconds,
        )
        .await?;
    let cancel = Cancellation::default();
    let operation=async {
        let captured=recovery_content::capture(db.clone(),lease.clone(),keys,cancel.clone()).await?;
        lease.renew().await?;
        let manifest=RecoveryManifest{version:1,set:lease.set_id(),incarnation:lease.token().incarnation,schema_checksum:schema_checksum()?,metadata_dump:captured.metadata_dump,content_inventory:captured.content_inventory,keys:captured.keys,exported_snapshot:captured.exported_snapshot};
        let bytes=canonical_json(&serde_json::to_value(&manifest)?)?.into_bytes();
        ensure!(bytes.len()<=256*1024,"RECOVERY_MANIFEST_CONFLICT");
        let namespace:IndexNamespace=serde_json::from_str(&std::env::var("MORIA_NATIVE_RECOVERY_NAMESPACE")?)?;
        let store=storage::configured_store_with_fingerprint(&namespace)?.0;
        let authority=Arc::new(BackupAuthority::acquire(db.clone(),lease.clone(),namespace,store.as_ref()).await?);
        let object=authority.object("manifest")?;
        let hash=format!("sha256:{:x}",Sha256::digest(&bytes));
        let mut output=store.begin_object(object,bytes.len() as u64,hash.clone(),authority,cancel.clone()).await?;
        output.append(&bytes).await?;
        let sealed=output.seal().await?;
        let mut client=db.get().await?;
        let tx=client.transaction().await?;
        tx.query_one("SELECT 1 FROM moria.runtime_recovery_sets WHERE id=$1 FOR UPDATE",&[&lease.set_id()]).await?;
        lease.check_in(tx.client()).await?;
        recovery_content::check_capture_namespaces_in(tx.client(),&lease).await?;
        let inventory=serde_json::to_value(&manifest.content_inventory)?;
        let plain_hash=hash.strip_prefix("sha256:").unwrap();
        ensure!(tx.execute("UPDATE moria.runtime_recovery_sets SET status='ready',content_inventory=$2,manifest_hash=$3,sealed_manifest=$4 WHERE id=$1 AND status='preparing'",&[&lease.set_id(),&inventory,&plain_hash,&serde_json::to_value(&sealed)?]).await?==1,"RECOVERY_MANIFEST_CONFLICT");
        tx.commit().await?;
        Ok(json!({"manifest":sealed,"recoverySet":lease.set_id(),"manifestHash":plain_hash,"retention":RecoveryAuthority{db:db.clone()}.inspect(lease.set_id()).await?}))
    }.await;
    if operation.is_err() {
        fail_capture(&db, &lease).await;
    }
    operation
}
async fn fail_capture(db: &Database, lease: &RecoveryLease) {
    if let Ok(client) = db.get().await {
        let _=client.execute("UPDATE moria.runtime_recovery_sets SET status='failed',error='RECOVERY_CAPTURE_FAILED' WHERE id=$1 AND status='preparing' AND owner=$2 AND fence::text=$3",&[&lease.set_id(),&lease.token().owner,&lease.token().fence]).await;
    }
}
async fn completed_restore(db: &Database, request: &RestoreRequest) -> Result<Option<Value>> {
    let client = db.restore_bootstrap_connection().await?;
    let present:bool=client.query_one("SELECT to_regnamespace('moria') IS NOT NULL AND to_regclass('public.moria_restore_bootstrap') IS NOT NULL",&[]).await?.get(0);
    if !present {
        return Ok(None);
    }
    let schema: Value = serde_json::from_str(include_str!("../schema.json"))?;
    client
        .query_one(
            "SELECT moria.runtime_schema_admit($1,$2)",
            &[
                &(schema["version"].as_i64().unwrap() as i32),
                &schema["checksum"].as_str().unwrap(),
            ],
        )
        .await?;
    let sealed = serde_json::to_value(&request.manifest)?;
    let row=client.query_opt("SELECT jsonb_build_object('id',r.id,'recoverySet',r.recovery_set,'manifestHash',r.manifest_hash,'previousIncarnation',r.previous_incarnation,'newIncarnation',r.new_incarnation,'status',r.status,'leasesFenced',r.leases_fenced,'dispatchEnabled',c.dispatch_enabled,'verification',r.verification) FROM moria.runtime_restores r JOIN moria.runtime_recovery_control c ON c.restore_id=r.id JOIN moria.indexing_instance i ON i.incarnation=r.new_incarnation JOIN public.moria_restore_bootstrap b ON b.restore_id=r.id JOIN moria.runtime_recovery_sets s ON s.id=r.recovery_set WHERE r.id=$1 AND r.status='enabled' AND r.leases_fenced AND c.singleton AND c.dispatch_enabled AND b.singleton AND b.status='verified' AND b.dispatch_approved AND b.manifest_hash=r.manifest_hash AND b.dump_sha256=r.verification->>'metadataSha256' AND s.sealed_manifest=$2 AND s.manifest_hash=r.manifest_hash AND s.incarnation=r.previous_incarnation",&[&request.restore,&sealed]).await?;
    let receipt = row.map(|row| row.get::<_, Value>(0));
    if let Some(receipt) = &receipt {
        let mut requested = request
            .target_namespaces
            .iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut expected = receipt["verification"]["content"]["namespaces"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("RECOVERY_MANIFEST_CONFLICT"))?
            .iter()
            .map(|namespace| namespace["target"].clone())
            .collect::<Vec<_>>();
        requested.sort_by_key(Value::to_string);
        expected.sort_by_key(Value::to_string);
        ensure!(requested == expected, "RECOVERY_MANIFEST_CONFLICT");
    }
    Ok(receipt)
}
pub(crate) async fn restore_from(db: Database) -> Result<()> {
    use tokio::io::AsyncReadExt;
    ensure!(
        std::env::var("MORIA_ISOLATED_RESTORE").as_deref() == Ok("1"),
        "RECOVERY_CONFIG_INVALID"
    );
    let mut bytes = Vec::new();
    tokio::io::stdin()
        .take(65537)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 65536, "RECOVERY_INVALID");
    let request: RestoreRequest = serde_json::from_slice(&bytes)?;
    ensure!(request.target_namespaces.len() <= 256, "RECOVERY_INVALID");
    if let Some(receipt) = completed_restore(&db, &request).await? {
        println!("{receipt}");
        return Ok(());
    }
    let cancel = Cancellation::default();
    let namespace: IndexNamespace =
        serde_json::from_str(&std::env::var("MORIA_NATIVE_RECOVERY_NAMESPACE")?)?;
    ensure!(
        request.manifest.object.warehouse_id == namespace.warehouse_id
            && request.manifest.object.table_id == namespace.table_id,
        "RECOVERY_OBJECT_DENIED"
    );
    let store = storage::configured_store_with_fingerprint(&namespace)?.0;
    let bytes = manifest_bytes(
        store.as_ref(),
        &ManifestAuthority(request.manifest.object.clone()),
        &cancel,
        &request.manifest,
    )
    .await?;
    let manifest: RecoveryManifest = serde_json::from_slice(&bytes)?;
    ensure!(
        manifest.version == 1
            && manifest.schema_checksum == schema_checksum()?
            && request.manifest.object.object_id
                == format!("recovery_{}_manifest", manifest.set.simple()),
        "RECOVERY_MANIFEST_CONFLICT"
    );
    manifest.keys.validate()?;
    let plain_hash = request
        .manifest
        .sha256
        .strip_prefix("sha256:")
        .ok_or_else(|| anyhow::anyhow!("RECOVERY_MANIFEST_CONFLICT"))?;
    let offline = OfflineRecovery::open(
        manifest.set,
        manifest.content_inventory.clone(),
        &manifest.metadata_dump,
        &manifest.keys,
        cancel.clone(),
    )
    .await?;
    let reader = offline.reader();
    let authority = offline.authority();
    let limits = RecoveryContentLimits::configured()?;
    let archive = recovery_metadata::materialize(
        reader.as_ref(),
        authority.as_ref(),
        &cancel,
        manifest.set,
        &manifest.metadata_dump,
        limits.dump_bytes,
    )
    .await?;
    let metadata = recovery_metadata::restore_database(
        &db,
        &archive,
        RestoreIdentity {
            restore: request.restore,
            set: manifest.set,
            incarnation: manifest.incarnation,
            manifest_hash: plain_hash.into(),
            key_manifest_hash: manifest.keys.fingerprint.clone(),
        },
        Duration::from_secs(limits.time_seconds),
    )
    .await?;
    if let Some(row)=db.get().await?.query_opt("SELECT r.verification FROM moria.runtime_restores r JOIN moria.runtime_recovery_control c ON c.restore_id=r.id JOIN moria.indexing_instance i ON i.incarnation=r.new_incarnation JOIN public.moria_restore_bootstrap b ON b.restore_id=r.id WHERE r.id=$1 AND r.recovery_set=$2 AND r.manifest_hash=$3 AND r.previous_incarnation=$4 AND r.status='enabled' AND r.leases_fenced AND c.singleton AND c.dispatch_enabled AND b.singleton AND b.dispatch_approved AND b.status='verified' AND b.dump_sha256=$5 AND b.manifest_hash=r.manifest_hash",&[&request.restore,&manifest.set,&plain_hash,&manifest.incarnation,&metadata.sha256()]).await? {
        let verification:Value=row.get(0);
        ensure!(verification["keyManifestHash"]==manifest.keys.fingerprint,"RECOVERY_MANIFEST_CONFLICT");
        println!("{}",RecoveryAuthority{db}.inspect_restore(request.restore).await?);
        return Ok(());
    }
    RecoveryAuthority { db: db.clone() }
        .restore_disabled(
            request.restore,
            manifest.set,
            plain_hash,
            manifest.incarnation,
        )
        .await?;
    let content = offline
        .copy_inventory(request.restore, request.target_namespaces)
        .await?;
    let keys = manifest
        .keys
        .restore(reader.as_ref(), authority.as_ref(), &cancel)
        .await?;
    finish_restore(
        &db,
        &metadata,
        &content,
        &keys,
        &manifest,
        &request.manifest,
    )
    .await?;
    println!(
        "{}",
        RecoveryAuthority { db }
            .inspect_restore(request.restore)
            .await?
    );
    Ok(())
}
async fn finish_restore(
    db: &Database,
    metadata: &MetadataRestoreProof,
    content: &RecoveredContentProof,
    keys: &KeyRestoreProof,
    manifest: &RecoveryManifest,
    sealed_manifest: &SealedObject,
) -> Result<()> {
    content.check_current().await?;
    keys.check_current()?;
    let identity = metadata.identity();
    ensure!(
        identity.key_manifest_hash == keys.fingerprint(),
        "RECOVERY_KEY_CHANGED"
    );
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    let row=tx.query_one("SELECT r.status,r.leases_fenced,r.new_incarnation FROM moria.runtime_recovery_control c JOIN moria.runtime_restores r ON r.id=c.restore_id JOIN moria.indexing_instance i ON i.incarnation=r.new_incarnation WHERE c.singleton AND NOT c.dispatch_enabled AND r.id=$1 AND r.recovery_set=$2 AND r.manifest_hash=$3 AND r.previous_incarnation=$4 FOR UPDATE OF c,r",&[&identity.restore,&identity.set,&identity.manifest_hash,&identity.incarnation]).await?;
    ensure!(
        row.get::<_, String>(0) == "disabled" && row.get::<_, bool>(1),
        "RECOVERY_INVALID"
    );
    let bootstrap=tx.query_one("SELECT dump_sha256,manifest_hash,restore_id,dispatch_approved FROM public.moria_restore_bootstrap WHERE singleton FOR UPDATE",&[]).await?;
    ensure!(
        bootstrap.get::<_, Option<String>>(0).as_deref() == Some(metadata.sha256())
            && bootstrap.get::<_, Option<String>>(1).as_deref() == Some(&identity.manifest_hash)
            && bootstrap.get::<_, Option<Uuid>>(2) == Some(identity.restore)
            && !bootstrap.get::<_, bool>(3),
        "RECOVERY_MANIFEST_CONFLICT"
    );
    for namespace in content.namespaces() {
        let source = namespace.source();
        let target = namespace.target();
        ensure!(
            source.warehouse_id == target.warehouse_id
                && source.table_id == target.table_id
                && source.revision == target.revision
                && namespace.source_physical_fingerprint() != namespace.physical_fingerprint(),
            "RECOVERY_MANIFEST_CONFLICT"
        );
        tx.execute("INSERT INTO moria.runtime_restore_namespace_rebindings(restore_id,warehouse_id,table_id,namespace_revision,source_fingerprint,target_fingerprint,kind) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING",&[&identity.restore,&source.warehouse_id,&source.table_id,&source.revision,&namespace.source_physical_fingerprint(),&namespace.physical_fingerprint(),&namespace.kind()]).await?;
        if namespace.kind() == "dataset" {
            let bindings=tx.query("SELECT environment,resource,revision,physical_binding_fingerprint FROM moria.native_dataset_bindings WHERE warehouse_id=$1 AND table_id=$2 AND namespace_revision=$3 FOR UPDATE",&[&source.warehouse_id,&source.table_id,&source.revision]).await?;
            for binding in bindings {
                let current: String = binding.get(3);
                ensure!(
                    current == namespace.source_physical_fingerprint()
                        || current == namespace.physical_fingerprint(),
                    "RECOVERY_NAMESPACE_CHANGED"
                );
                if current == namespace.physical_fingerprint() {
                    continue;
                }
                let revision = fingerprint(
                    &json!({"restore":identity.restore,"previous":binding.get::<_,String>(2),"physical":namespace.physical_fingerprint()}),
                )?;
                tx.execute("UPDATE moria.native_dataset_bindings SET physical_binding_fingerprint=$3,revision=$4 WHERE environment=$1 AND resource=$2",&[&binding.get::<_,String>(0),&binding.get::<_,Uuid>(1),&namespace.physical_fingerprint(),&revision]).await?;
            }
        } else {
            let next = fingerprint(&storage::configured_physical_binding(target)?)?;
            tx.execute("UPDATE moria.fs_storage_bindings SET binding_hash=$3 WHERE warehouse_id=$1 AND table_id=$2",&[&source.warehouse_id,&source.table_id,&next]).await?;
        }
    }
    content
        .qualify_datasets_in(tx.client(), identity.restore, row.get::<_, Uuid>(2))
        .await?;
    ensure!(!tx.query_one("SELECT EXISTS(SELECT 1 FROM moria.workload_sessions WHERE NOT disabled) OR EXISTS(SELECT 1 FROM moria.browser_sessions WHERE NOT disabled) OR EXISTS(SELECT 1 FROM moria.runtime_workers WHERE enabled) OR EXISTS(SELECT 1 FROM moria.native_service_peers WHERE enabled) OR EXISTS(SELECT 1 FROM moria.runtime_service_authorities WHERE enabled) OR EXISTS(SELECT 1 FROM moria.runtime_execution_bindings WHERE enabled) OR EXISTS(SELECT 1 FROM moria.runtime_delivery_destinations WHERE enabled) OR EXISTS(SELECT 1 FROM moria.indexing_bindings WHERE enabled) OR EXISTS(SELECT 1 FROM moria.native_model_bindings WHERE enabled) OR EXISTS(SELECT 1 FROM moria.native_search_projector_bindings WHERE enabled)",&[]).await?.get::<_,bool>(0),"RESTORE_RECONCILIATION_REQUIRED");
    ensure!(
        manifest.set == identity.set
            && manifest.incarnation == identity.incarnation
            && manifest.keys.fingerprint == keys.fingerprint(),
        "RECOVERY_MANIFEST_CONFLICT"
    );
    tx.execute("UPDATE moria.runtime_recovery_sets SET status='ready',metadata_dump=$2,content_inventory=$3,exported_snapshot=$4,manifest_hash=$5,sealed_manifest=$6 WHERE id=$1",&[&identity.set,&serde_json::to_value(&manifest.metadata_dump)?,&serde_json::to_value(&manifest.content_inventory)?,&manifest.exported_snapshot,&identity.manifest_hash,&serde_json::to_value(sealed_manifest)?]).await?;
    let verification = json!({"version":1,"metadataSha256":metadata.sha256(),"metadataBytes":metadata.bytes().to_string(),"keyManifestHash":keys.fingerprint(),"content":content,"targetMappingHash":content.target_mapping_hash(),"entries":content.entries().to_string(),"bytes":content.bytes().to_string(),"inventory":content.inventory(),"oldConsumersDisabled":true});
    tx.execute("UPDATE moria.runtime_restores SET status='enabled',verification=$2,enabled_at=clock_timestamp() WHERE id=$1",&[&identity.restore,&verification]).await?;
    tx.execute("UPDATE moria.runtime_recovery_control SET dispatch_enabled=true WHERE singleton AND restore_id=$1",&[&identity.restore]).await?;
    tx.execute("UPDATE public.moria_restore_bootstrap SET dispatch_approved=true,status='verified' WHERE singleton AND restore_id=$1",&[&identity.restore]).await?;
    tx.commit().await?;
    Ok(())
}
