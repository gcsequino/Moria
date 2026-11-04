use anyhow::{Result, ensure};
use moria_runtime::{
    database::Database,
    recovery::{RecoveryLease, RecoveryLeaseToken},
    recovery_content::BackupAuthority,
    storage::{self, Cancellation, IndexNamespace},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Fixture {
    lease: RecoveryLeaseToken,
    namespace: IndexNamespace,
    mode: String,
}
#[test]
#[ignore = "requires mandatory real PostgreSQL, issued-object authority and MinIO"]
fn issued_backup_fixture() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run())
        .unwrap();
}
async fn run() -> Result<()> {
    let fixture: Fixture = serde_json::from_str(&std::env::var("MORIA_NATIVE_RETENTION_FIXTURE")?)?;
    let db = Database::configured()?;
    db.schema_lease().await?;
    if fixture.mode == "denied" {
        ensure!(
            RecoveryLease::resume(db, fixture.lease).await.is_err(),
            "OLD_PRODUCER_MUST_STAY_FENCED"
        );
        println!(
            "{}",
            serde_json::json!({"event":"verified","mode":"denied"})
        );
        return Ok(());
    }
    ensure!(fixture.mode == "issue", "INVALID_FIXTURE");
    let lease = RecoveryLease::resume(db.clone(), fixture.lease).await?;
    let (store, _) = storage::configured_store_with_fingerprint(&fixture.namespace)?;
    let authority = Arc::new(
        BackupAuthority::acquire(db.clone(), lease, fixture.namespace, store.as_ref()).await?,
    );
    let object = authority.object("metadata_part_0")?;
    let bytes = vec![31u8; 4096];
    let mut writer = store
        .begin_object(
            object,
            bytes.len() as u64,
            format!("sha256:{:x}", Sha256::digest(&bytes)),
            authority.clone(),
            Cancellation::default(),
        )
        .await?;
    writer.append(&bytes).await?;
    let sealed = writer.seal().await?;
    let version = store
        .version(&sealed.object, authority.as_ref(), &Cancellation::default())
        .await?;
    println!(
        "{}",
        serde_json::json!({"event":"verified","mode":"issue","content":sealed,"version":version})
    );
    Ok(())
}
include!("../native-common/artifact.rs");
