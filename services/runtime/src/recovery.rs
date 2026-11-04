use crate::database::Database;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

pub const RETENTION_ADMISSION: &str = "moria/recovery-retention/v1";
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RecoveryLeaseToken {
    pub set: Uuid,
    pub owner: Uuid,
    pub fence: String,
    pub incarnation: Uuid,
}
#[derive(Clone)]
pub struct RecoveryLease {
    db: Database,
    token: RecoveryLeaseToken,
}
impl std::fmt::Debug for RecoveryLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryLease")
            .field("set", &self.token.set)
            .field("owner", &self.token.owner)
            .field("fence", &self.token.fence)
            .field("incarnation", &self.token.incarnation)
            .finish()
    }
}
impl RecoveryLease {
    pub fn token(&self) -> &RecoveryLeaseToken {
        &self.token
    }
    pub fn set_id(&self) -> Uuid {
        self.token.set
    }
    pub fn scope_key(&self) -> String {
        format!("recovery_{}", self.token.set.simple())
    }
    pub async fn resume(db: Database, token: RecoveryLeaseToken) -> Result<Self> {
        let lease = Self { db, token };
        lease.check().await?;
        Ok(lease)
    }
    /// Use a current READ COMMITTED authority connection, never the exported data snapshot.
    pub async fn check_in(&self, client: &tokio_postgres::Client) -> Result<()> {
        self.check_query(client).await
    }
    async fn check_query<C: tokio_postgres::GenericClient + Sync>(&self, client: &C) -> Result<()> {
        let fence = self.token.fence.parse::<i64>()?;
        ensure!(
            fence > 0 && fence.to_string() == self.token.fence,
            "RECOVERY_LEASE_LOST"
        );
        client
            .query_one(
                "SELECT moria.runtime_recovery_lease_check($1,$2,$3,$4)",
                &[
                    &self.token.set,
                    &self.token.owner,
                    &fence,
                    &self.token.incarnation,
                ],
            )
            .await?;
        Ok(())
    }
    pub async fn check(&self) -> Result<()> {
        let client = self.db.get().await?;
        self.check_in(&client).await
    }
    pub async fn check_key_manifest(&self, hash: &str) -> Result<()> {
        let client = self.db.get().await?;
        self.check_in(&client).await?;
        ensure!(
            client
                .query_one(
                    "SELECT key_manifest_hash=$2 FROM moria.runtime_recovery_sets WHERE id=$1",
                    &[&self.token.set, &hash]
                )
                .await?
                .get::<_, bool>(0),
            "RECOVERY_KEY_CHANGED"
        );
        Ok(())
    }
    pub async fn renew(&self) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT 1 FROM moria.runtime_recovery_sets WHERE id=$1 FOR UPDATE",
            &[&self.token.set],
        )
        .await?;
        self.check_query(&*tx).await?;
        tx.execute("UPDATE moria.runtime_recovery_sets SET lease_until=clock_timestamp()+interval '30 seconds' WHERE id=$1",&[&self.token.set]).await?;
        tx.commit().await?;
        Ok(())
    }
}
pub struct RecoveryAuthority {
    pub db: Database,
}
impl RecoveryAuthority {
    pub async fn create(
        &self,
        key: &str,
        owner: Uuid,
        key_manifest_hash: &str,
        retention_seconds: i64,
    ) -> Result<RecoveryLease> {
        ensure!(
            !key.is_empty()
                && key.len() <= 256
                && key_manifest_hash.len() == 64
                && key_manifest_hash
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                && (86400..=315360000).contains(&retention_seconds),
            "RECOVERY_INVALID"
        );
        let checksum: Value = serde_json::from_str(include_str!("../schema.json"))?;
        let schema = checksum["checksum"].as_str().unwrap();
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one("SELECT moria.runtime_recovery_check()", &[])
            .await?;
        let row=tx.query_opt("INSERT INTO moria.runtime_recovery_sets(id,request_key,owner,fence,incarnation,lease_until,status,schema_checksum,key_manifest_hash,retained_until,retention_seconds) SELECT $1,$2,$3,1,incarnation,clock_timestamp()+interval '30 seconds','preparing',$4,$5,clock_timestamp()+$6::bigint*interval '1 second',$6 FROM moria.indexing_instance ON CONFLICT(request_key) DO UPDATE SET request_key=EXCLUDED.request_key WHERE runtime_recovery_sets.owner=EXCLUDED.owner AND runtime_recovery_sets.key_manifest_hash=EXCLUDED.key_manifest_hash AND runtime_recovery_sets.retention_seconds=EXCLUDED.retention_seconds AND runtime_recovery_sets.schema_checksum=EXCLUDED.schema_checksum AND runtime_recovery_sets.incarnation=EXCLUDED.incarnation AND runtime_recovery_sets.status='preparing' AND runtime_recovery_sets.lease_until>clock_timestamp() RETURNING id,fence::text,incarnation",&[&Uuid::new_v4(),&key,&owner,&schema,&key_manifest_hash,&retention_seconds]).await?.ok_or_else(||anyhow::anyhow!("RECOVERY_MANIFEST_CONFLICT"))?;
        let token = RecoveryLeaseToken {
            set: row.get(0),
            owner,
            fence: row.get(1),
            incarnation: row.get(2),
        };
        tx.commit().await?;
        Ok(RecoveryLease {
            db: self.db.clone(),
            token,
        })
    }
    pub async fn restore_disabled(
        &self,
        id: Uuid,
        set: Uuid,
        manifest_hash: &str,
        expected: Uuid,
    ) -> Result<Value> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT moria.runtime_restore_pause($1,$2,$3,$4)",
            &[&id, &set, &manifest_hash, &expected],
        )
        .await?;
        tx.commit().await?;
        if client
            .query_one(
                "SELECT status='enabled' AND leases_fenced FROM moria.runtime_restores WHERE id=$1",
                &[&id],
            )
            .await?
            .get::<_, bool>(0)
        {
            return self.inspect_restore(id).await;
        }
        let tx = client.transaction().await?;
        tx.query_one("SELECT moria.runtime_restore_fence($1)", &[&id])
            .await?;
        tx.commit().await?;
        self.inspect_restore(id).await
    }
    pub async fn inspect_restore(&self, id: Uuid) -> Result<Value> {
        Ok(self.db.get().await?.query_one("SELECT jsonb_build_object('id',r.id,'recoverySet',r.recovery_set,'manifestHash',r.manifest_hash,'previousIncarnation',r.previous_incarnation,'newIncarnation',r.new_incarnation,'status',r.status,'leasesFenced',r.leases_fenced,'dispatchEnabled',c.dispatch_enabled,'verification',r.verification) FROM moria.runtime_restores r JOIN moria.runtime_recovery_control c ON c.restore_id=r.id WHERE r.id=$1",&[&id]).await?.get(0))
    }
    pub async fn inspect(&self, set: Uuid) -> Result<Value> {
        Ok(self.db.get().await?.query_one("SELECT jsonb_build_object('id',id,'status',status,'incarnation',incarnation,'fence',fence::text,'schemaChecksum',schema_checksum,'retainedUntil',retained_until,'metadataDump',metadata_dump,'contentInventory',content_inventory,'manifestHash',manifest_hash,'sealedManifest',sealed_manifest,'error',error) FROM moria.runtime_recovery_sets WHERE id=$1",&[&set]).await?.get(0))
    }
}
pub async fn cli(db: Database, args: Vec<String>) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    tokio::io::stdin()
        .take(65537)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 65536, "RECOVERY_INVALID");
    let value: Value = serde_json::from_slice(&bytes)?;
    let authority = RecoveryAuthority { db: db.clone() };
    let text = |key: &str| {
        value[key]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("RECOVERY_INVALID"))
    };
    let result = match args.first().map(String::as_str) {
        Some("capture") => {
            crate::recovery_control::capture(db, serde_json::from_value(value)?).await?
        }
        Some("create") => json!(
            authority
                .create(
                    text("requestKey")?,
                    Uuid::parse_str(text("owner")?)?,
                    text("keyManifestHash")?,
                    value["retentionSeconds"]
                        .as_i64()
                        .ok_or_else(|| anyhow::anyhow!("RECOVERY_INVALID"))?
                )
                .await?
                .token()
        ),
        Some("renew") => {
            let lease = RecoveryLease::resume(db, serde_json::from_value(value)?).await?;
            lease.renew().await?;
            json!({"renewed":true})
        }
        Some("approve-cli") => {
            ensure!(value == json!({}), "RECOVERY_INVALID");
            crate::http_auth::HttpAuthentication::configured(db, "127.0.0.1:1")?
                .approve_cli()
                .await?;
            json!({"approved":true})
        }
        Some("restore-disabled") => {
            ensure!(
                std::env::var("MORIA_ISOLATED_RESTORE").as_deref() == Ok("1"),
                "RECOVERY_CONFIG_INVALID"
            );
            authority
                .restore_disabled(
                    Uuid::parse_str(text("restore")?)?,
                    Uuid::parse_str(text("set")?)?,
                    text("manifestHash")?,
                    Uuid::parse_str(text("expectedIncarnation")?)?,
                )
                .await?
        }
        Some("assign-cell") => {
            ensure!(
                std::env::var("MORIA_ISOLATED_RESTORE").as_deref() == Ok("1"),
                "RECOVERY_CONFIG_INVALID"
            );
            let owner: crate::identity::Ownership = serde_json::from_value(value["owner"].clone())?;
            let fence = owner.fence.parse::<i64>()?;
            ensure!(
                fence > 0 && fence.to_string() == owner.fence,
                "RECOVERY_INVALID"
            );
            db.get()
                .await?
                .query_one(
                    "SELECT moria.runtime_restore_assign_cell($1,$2,$3,$4,$5,$6)",
                    &[
                        &Uuid::parse_str(text("restore")?)?,
                        &text("environment")?,
                        &owner.cell,
                        &fence,
                        &owner.incarnation,
                        &Uuid::parse_str(text("cell")?)?,
                    ],
                )
                .await?;
            json!(
                crate::directory::EnvironmentDirectory { db }
                    .inspect(text("environment")?)
                    .await?
            )
        }
        Some("inspect-restore") => {
            authority
                .inspect_restore(Uuid::parse_str(text("restore")?)?)
                .await?
        }
        Some("inspect") => authority.inspect(Uuid::parse_str(text("set")?)?).await?,
        _ => anyhow::bail!("RECOVERY_INVALID"),
    };
    println!("{result}");
    Ok(())
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MetadataArchive {
    pub version: u32,
    pub format: String,
    pub part_bytes: u32,
    pub parts: u32,
    pub bytes: u64,
    pub sha256: String,
    pub last_page: crate::storage::SealedObject,
    pub database_roles: Vec<Value>,
    pub database_memberships: Vec<Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct MetadataPage {
    pub(crate) version: u32,
    pub(crate) start_index: u32,
    pub(crate) start_offset: u64,
    pub(crate) parts: Vec<crate::storage::SealedObject>,
    pub(crate) previous: Option<crate::storage::SealedObject>,
}
pub(crate) const METADATA_PART_BYTES: usize = 8 * 1024 * 1024;
struct MetadataSink<'a> {
    lease: &'a RecoveryLease,
    writer: &'a dyn crate::storage::ObjectWriter,
    root: crate::storage::LogicalObject,
    authority: std::sync::Arc<dyn crate::storage::WriteAuthority>,
    cancel: crate::storage::Cancellation,
    parts: Vec<crate::storage::SealedObject>,
    last_page: Option<crate::storage::SealedObject>,
    part_index: u32,
    page_index: u32,
    bytes: u64,
    database_roles: Vec<Value>,
    database_memberships: Vec<Value>,
}
impl MetadataSink<'_> {
    async fn write(
        &self,
        object: crate::storage::LogicalObject,
        bytes: &[u8],
    ) -> Result<crate::storage::SealedObject> {
        use sha2::{Digest, Sha256};
        self.lease.check().await?;
        let digest = format!("sha256:{:x}", Sha256::digest(bytes));
        let mut write = self
            .writer
            .begin_object(
                object,
                bytes.len() as u64,
                digest,
                self.authority.clone(),
                self.cancel.clone(),
            )
            .await?;
        for chunk in bytes.chunks(1024 * 1024) {
            write.append(chunk).await?;
        }
        let sealed = write.seal().await?;
        self.lease.check().await?;
        Ok(sealed)
    }
    fn object(&self, suffix: &str) -> Result<crate::storage::LogicalObject> {
        crate::storage::LogicalObject::new(
            &self.root.warehouse_id,
            &self.root.table_id,
            &format!("{}_metadata_{suffix}", self.lease.scope_key()),
        )
    }
    async fn part(&mut self, bytes: &[u8]) -> Result<()> {
        ensure!(
            !bytes.is_empty() && bytes.len() <= METADATA_PART_BYTES,
            "RECOVERY_DUMP_LIMIT"
        );
        let object = self.object(&format!("part_{}", self.part_index))?;
        self.parts.push(self.write(object, bytes).await?);
        self.part_index = self
            .part_index
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("RECOVERY_DUMP_LIMIT"))?;
        self.bytes += bytes.len() as u64;
        if self.parts.len() == 128 {
            self.page().await?;
        }
        Ok(())
    }
    async fn page(&mut self) -> Result<()> {
        if self.parts.is_empty() {
            return Ok(());
        }
        let page = MetadataPage {
            version: 1,
            start_index: self.part_index - self.parts.len() as u32,
            start_offset: self.bytes - self.parts.iter().map(|part| part.size).sum::<u64>(),
            parts: std::mem::take(&mut self.parts),
            previous: self.last_page.take(),
        };
        let bytes = serde_json::to_vec(&page)?;
        ensure!(bytes.len() <= 256 * 1024, "RECOVERY_DUMP_LIMIT");
        self.last_page = Some(
            self.write(self.object(&format!("page_{}", self.page_index))?, &bytes)
                .await?,
        );
        self.page_index += 1;
        Ok(())
    }
    async fn finish(mut self, sha256: String) -> Result<crate::storage::SealedObject> {
        self.page().await?;
        let archive = MetadataArchive {
            version: 1,
            format: "postgresql-custom-parts-v1".into(),
            part_bytes: METADATA_PART_BYTES as u32,
            parts: self.part_index,
            bytes: self.bytes,
            sha256,
            last_page: self
                .last_page
                .clone()
                .ok_or_else(|| anyhow::anyhow!("RECOVERY_DUMP_FAILED"))?,
            database_roles: self.database_roles.clone(),
            database_memberships: self.database_memberships.clone(),
        };
        let bytes = serde_json::to_vec(&archive)?;
        ensure!(bytes.len() <= 256 * 1024, "RECOVERY_DUMP_LIMIT");
        self.write(self.root.clone(), &bytes).await
    }
}
impl RecoveryLease {
    #[allow(clippy::too_many_arguments)]
    pub async fn dump_metadata(
        &self,
        snapshot_client: &tokio_postgres::Client,
        snapshot: &str,
        writer: &dyn crate::storage::ObjectWriter,
        object: crate::storage::LogicalObject,
        write_authority: std::sync::Arc<dyn crate::storage::WriteAuthority>,
        cancel: crate::storage::Cancellation,
        maximum_bytes: u64,
        timeout: std::time::Duration,
    ) -> Result<crate::storage::SealedObject> {
        use sha2::{Digest, Sha256};
        use std::process::Stdio;
        use tokio::io::AsyncReadExt;
        ensure!(
            !snapshot.is_empty()
                && snapshot.len() <= 128
                && snapshot.bytes().all(|c| c.is_ascii_hexdigit() || c == b'-')
                && maximum_bytes > 0
                && maximum_bytes <= 1 << 50
                && timeout.as_secs() > 0
                && timeout.as_secs() <= 86400
                && object.object_id == format!("{}_metadata", self.scope_key()),
            "RECOVERY_INVALID"
        );
        self.check().await?;
        let mut current = self.db.get().await?;
        let tx = current.transaction().await?;
        tx.query_one(
            "SELECT 1 FROM moria.runtime_recovery_sets WHERE id=$1 FOR UPDATE",
            &[&self.token.set],
        )
        .await?;
        self.check_query(&*tx).await?;
        ensure!(tx.execute("UPDATE moria.runtime_recovery_sets SET exported_snapshot=$2 WHERE id=$1 AND (exported_snapshot IS NULL OR exported_snapshot=$2)",&[&self.token.set,&snapshot]).await?==1,"RECOVERY_MANIFEST_CONFLICT");
        tx.commit().await?;
        drop(current);
        let operation = async {
            let server: String = snapshot_client
                .query_one("SHOW server_version_num", &[])
                .await?
                .get(0);
            ensure!(
                server.parse::<u32>()? / 10000 == 18,
                "RECOVERY_ENGINE_UNSUPPORTED"
            );
            let (mut command, _) = postgres_command("MORIA_RECOVERY_PG_DUMP", "pg_dump").await?;
            command
                .args([
                    "--format=custom",
                    "--exclude-table=public.moria_restore_bootstrap",
                    "--snapshot",
                    snapshot,
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null());
            let mut child = command.spawn()?;
            let mut output = child.stdout.take().unwrap();
            let (database_roles, database_memberships) =
                crate::recovery_metadata::capture_roles(snapshot_client).await?;
            let mut sink = MetadataSink {
                lease: self,
                writer,
                root: object,
                authority: write_authority,
                cancel: cancel.clone(),
                parts: vec![],
                last_page: None,
                part_index: 0,
                page_index: 0,
                bytes: 0,
                database_roles,
                database_memberships,
            };
            let mut buffer = vec![0u8; 1024 * 1024];
            let mut part = Vec::with_capacity(METADATA_PART_BYTES);
            let mut total = 0u64;
            let mut hash = Sha256::new();
            loop {
                cancel.check()?;
                let count = output.read(&mut buffer).await?;
                if count == 0 {
                    break;
                }
                total = total
                    .checked_add(count as u64)
                    .ok_or_else(|| anyhow::anyhow!("RECOVERY_DUMP_LIMIT"))?;
                ensure!(total <= maximum_bytes, "RECOVERY_DUMP_LIMIT");
                hash.update(&buffer[..count]);
                let mut remaining = &buffer[..count];
                while !remaining.is_empty() {
                    let count = remaining.len().min(METADATA_PART_BYTES - part.len());
                    part.extend_from_slice(&remaining[..count]);
                    remaining = &remaining[count..];
                    if part.len() == METADATA_PART_BYTES {
                        sink.part(&part).await?;
                        part.clear();
                    }
                }
            }
            ensure!(
                child.wait().await?.success() && total > 0,
                "RECOVERY_DUMP_FAILED"
            );
            if !part.is_empty() {
                sink.part(&part).await?;
            }
            let sealed = sink.finish(format!("sha256:{:x}", hash.finalize())).await?;
            let mut client = self.db.get().await?;
            let tx = client.transaction().await?;
            tx.query_one(
                "SELECT 1 FROM moria.runtime_recovery_sets WHERE id=$1 FOR UPDATE",
                &[&self.token.set],
            )
            .await?;
            self.check_query(&*tx).await?;
            let encoded = serde_json::to_value(&sealed)?;
            ensure!(tx.execute("UPDATE moria.runtime_recovery_sets SET metadata_dump=$2 WHERE id=$1 AND (metadata_dump IS NULL OR metadata_dump=$2)",&[&self.token.set,&encoded]).await?==1,"RECOVERY_MANIFEST_CONFLICT");
            tx.commit().await?;
            Ok(sealed)
        };
        let guarded = async {
            tokio::pin!(operation);
            let mut renewal = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tokio::select! {result=&mut operation=>return result,_=renewal.tick()=>{cancel.check()?;self.renew().await?;snapshot_client.query_one("SELECT 1",&[]).await?;}}
            }
        };
        tokio::time::timeout(timeout, guarded)
            .await
            .map_err(|_| anyhow::anyhow!("RECOVERY_DEADLINE"))?
    }
}

pub(crate) async fn postgres_command(
    binary_reference: &str,
    expected_name: &str,
) -> Result<(tokio::process::Command, String)> {
    use std::{os::unix::ffi::OsStrExt, str::FromStr};
    let binary = std::fs::canonicalize(
        std::env::var(binary_reference).map_err(|_| anyhow::anyhow!("RECOVERY_CONFIG_INVALID"))?,
    )?;
    ensure!(
        binary.file_name().is_some_and(|name| name == expected_name),
        "RECOVERY_CONFIG_INVALID"
    );
    let version = tokio::process::Command::new(&binary)
        .arg("--version")
        .env_clear()
        .output()
        .await?;
    ensure!(
        version.status.success()
            && version.stdout.len() <= 2048
            && String::from_utf8_lossy(&version.stdout).contains("PostgreSQL) 18."),
        "RECOVERY_ENGINE_UNSUPPORTED"
    );
    let config = tokio_postgres::Config::from_str(&std::env::var(std::env::var(
        "MORIA_INDEXING_DATABASE_SECRET_REF",
    )?)?)?;
    ensure!(
        config.get_hosts().len() == 1 && config.get_ports().len() <= 1,
        "RECOVERY_CONFIG_INVALID"
    );
    let host = match &config.get_hosts()[0] {
        tokio_postgres::config::Host::Tcp(host) => host.clone(),
        _ => anyhow::bail!("RECOVERY_CONFIG_INVALID"),
    };
    let database = config.get_dbname().unwrap_or("postgres").to_owned();
    let mut command = tokio::process::Command::new(binary);
    command
        .env_clear()
        .env("LC_ALL", "C")
        .env("PGHOST", host)
        .env(
            "PGPORT",
            config
                .get_ports()
                .first()
                .copied()
                .unwrap_or(5432)
                .to_string(),
        )
        .env(
            "PGUSER",
            config
                .get_user()
                .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONFIG_INVALID"))?,
        )
        .env("PGDATABASE", &database)
        .env(
            "PGPASSWORD",
            std::ffi::OsStr::from_bytes(
                config
                    .get_password()
                    .ok_or_else(|| anyhow::anyhow!("RECOVERY_CONFIG_INVALID"))?,
            ),
        )
        .env(
            "PGSSLMODE",
            if std::env::var("MORIA_INDEXING_DATABASE_TLS").as_deref() == Ok("disable") {
                "disable"
            } else {
                "verify-full"
            },
        )
        .kill_on_drop(true);
    if let Ok(ca) = std::env::var("MORIA_INDEXING_DATABASE_CA") {
        command.env("PGSSLROOTCERT", ca);
    }
    Ok((command, database))
}
