use crate::{
    recovery::{METADATA_PART_BYTES, MetadataArchive, MetadataPage},
    storage::{Cancellation, LogicalObject, ObjectReader, ReadAuthority, SealedObject},
};
use anyhow::{Result, ensure};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

pub(crate) struct VerifiedMetadataArchive {
    directory: PathBuf,
    path: PathBuf,
    sha256: String,
    bytes: u64,
    database_roles: Vec<serde_json::Value>,
    database_memberships: Vec<serde_json::Value>,
}
impl VerifiedMetadataArchive {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
    pub(crate) fn sha256(&self) -> &str {
        &self.sha256
    }
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}
impl Drop for VerifiedMetadataArchive {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_dir(&self.directory);
    }
}
fn object(root: &LogicalObject, scope: &str, suffix: &str) -> Result<LogicalObject> {
    LogicalObject::new(
        &root.warehouse_id,
        &root.table_id,
        &format!("{scope}_metadata{suffix}"),
    )
}
async fn read_json<T: DeserializeOwned>(
    reader: &dyn ObjectReader,
    authority: &dyn ReadAuthority,
    cancel: &Cancellation,
    sealed: &SealedObject,
) -> Result<T> {
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
    Ok(serde_json::from_slice(&bytes)?)
}
pub(crate) async fn materialize(
    reader: &dyn ObjectReader,
    authority: &dyn ReadAuthority,
    cancel: &Cancellation,
    set: Uuid,
    root: &SealedObject,
    maximum_bytes: u64,
) -> Result<VerifiedMetadataArchive> {
    ensure!(
        maximum_bytes > 0 && maximum_bytes <= 1 << 50,
        "RECOVERY_INVALID"
    );
    let scope = format!("recovery_{}", set.simple());
    ensure!(
        root.object == object(&root.object, &scope, "")?,
        "RECOVERY_MANIFEST_CONFLICT"
    );
    let archive: MetadataArchive = read_json(reader, authority, cancel, root).await?;
    ensure!(
        archive.version == 1
            && archive.format == "postgresql-custom-parts-v1"
            && archive.part_bytes as usize == METADATA_PART_BYTES
            && archive.bytes > 0
            && archive.bytes <= maximum_bytes
            && archive.parts as u64 == archive.bytes.div_ceil(METADATA_PART_BYTES as u64),
        "RECOVERY_MANIFEST_CONFLICT"
    );
    let parent = std::fs::canonicalize(
        std::env::var("MORIA_RECOVERY_STAGING_ROOT")
            .map_err(|_| anyhow::anyhow!("RECOVERY_CONFIG_INVALID"))?,
    )?;
    let metadata = std::fs::metadata(&parent)?;
    ensure!(
        metadata.is_dir()
            && metadata.uid() == std::fs::metadata("/proc/self")?.uid()
            && metadata.permissions().mode() & 0o077 == 0,
        "RECOVERY_CONFIG_INVALID"
    );
    let directory = parent.join(Uuid::new_v4().to_string());
    std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
    let verified = VerifiedMetadataArchive {
        path: directory.join("metadata.dump"),
        directory,
        sha256: archive.sha256.clone(),
        bytes: archive.bytes,
        database_roles: archive.database_roles.clone(),
        database_memberships: archive.database_memberships.clone(),
    };
    let mut file = tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&verified.path)
        .await?;
    let mut page = archive.last_page;
    let mut end_part = archive.parts;
    let mut end_byte = archive.bytes;
    let mut page_index = (archive.parts - 1) / 128;
    loop {
        cancel.check()?;
        ensure!(
            page.object == object(&root.object, &scope, &format!("_page_{page_index}"))?,
            "RECOVERY_MANIFEST_CONFLICT"
        );
        let descriptor: MetadataPage = read_json(reader, authority, cancel, &page).await?;
        ensure!(
            descriptor.version == 1
                && !descriptor.parts.is_empty()
                && descriptor.parts.len() <= 128
                && descriptor.start_index == page_index * 128
                && descriptor
                    .start_index
                    .checked_add(descriptor.parts.len() as u32)
                    == Some(end_part)
                && descriptor.start_offset
                    == descriptor.start_index as u64 * METADATA_PART_BYTES as u64,
            "RECOVERY_MANIFEST_CONFLICT"
        );
        let mut offset = descriptor.start_offset;
        for (index, part) in descriptor.parts.iter().enumerate() {
            let ordinal = descriptor.start_index + index as u32;
            let expected_size = (archive.bytes - ordinal as u64 * METADATA_PART_BYTES as u64)
                .min(METADATA_PART_BYTES as u64);
            ensure!(
                part.object == object(&root.object, &scope, &format!("_part_{ordinal}"))?
                    && part.size == expected_size,
                "RECOVERY_MANIFEST_CONFLICT"
            );
            let version = reader.version(&part.object, authority, cancel).await?;
            ensure!(version.size == part.size, "RECOVERY_MANIFEST_CONFLICT");
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            let mut position = 0u64;
            let mut hash = Sha256::new();
            while position < part.size {
                let end = (position + 1024 * 1024).min(part.size);
                let bytes = reader
                    .range_at_version(&part.object, position..end, &version, authority, cancel)
                    .await?;
                hash.update(&bytes);
                file.write_all(&bytes).await?;
                position = end;
            }
            ensure!(
                format!("sha256:{:x}", hash.finalize()) == part.sha256,
                "RECOVERY_MANIFEST_CONFLICT"
            );
            offset += part.size;
        }
        ensure!(offset == end_byte, "RECOVERY_MANIFEST_CONFLICT");
        end_part = descriptor.start_index;
        end_byte = descriptor.start_offset;
        if end_part == 0 {
            ensure!(
                descriptor.previous.is_none() && end_byte == 0,
                "RECOVERY_MANIFEST_CONFLICT"
            );
            break;
        }
        ensure!(page_index > 0, "RECOVERY_MANIFEST_CONFLICT");
        page_index -= 1;
        page = descriptor
            .previous
            .ok_or_else(|| anyhow::anyhow!("RECOVERY_MANIFEST_CONFLICT"))?;
    }
    file.sync_all().await?;
    file.rewind().await?;
    let mut hash = Sha256::new();
    let mut bytes = vec![0u8; 1024 * 1024];
    let mut total = 0u64;
    loop {
        cancel.check()?;
        let count = file.read(&mut bytes).await?;
        if count == 0 {
            break;
        }
        hash.update(&bytes[..count]);
        total += count as u64;
    }
    ensure!(
        total == archive.bytes && format!("sha256:{:x}", hash.finalize()) == archive.sha256,
        "RECOVERY_MANIFEST_CONFLICT"
    );
    drop(file);
    File::open(&verified.directory)?.sync_all()?;
    Ok(verified)
}

pub(crate) async fn capture_roles(
    client: &tokio_postgres::Client,
) -> Result<(Vec<serde_json::Value>, Vec<serde_json::Value>)> {
    let roles=client.query("SELECT jsonb_build_object('name',rolname,'superuser',rolsuper,'inherit',rolinherit,'createRole',rolcreaterole,'createDatabase',rolcreatedb,'login',rolcanlogin,'replication',rolreplication,'bypassRls',rolbypassrls,'connectionLimit',rolconnlimit,'validUntilEpoch',extract(epoch FROM rolvaliduntil)::text,'settings',COALESCE(to_jsonb(rolconfig),'[]')) FROM pg_roles WHERE rolname !~ '^pg_' ORDER BY rolname LIMIT 1025",&[]).await?.into_iter().map(|row|row.get(0)).collect::<Vec<serde_json::Value>>();
    ensure!(roles.len() <= 1024, "RECOVERY_DATABASE_ROLE_REQUIRED");
    let memberships=client.query("SELECT jsonb_build_object('role',r.rolname,'member',m.rolname,'grantor',g.rolname,'admin',a.admin_option,'inherit',a.inherit_option,'set',a.set_option) FROM pg_auth_members a JOIN pg_roles r ON r.oid=a.roleid JOIN pg_roles m ON m.oid=a.member JOIN pg_roles g ON g.oid=a.grantor WHERE r.rolname !~ '^pg_' OR m.rolname !~ '^pg_' ORDER BY r.rolname,m.rolname,g.rolname LIMIT 4097",&[]).await?.into_iter().map(|row|row.get(0)).collect::<Vec<serde_json::Value>>();
    ensure!(memberships.len() <= 4096, "RECOVERY_DATABASE_ROLE_REQUIRED");
    Ok((roles, memberships))
}
#[derive(Clone)]
pub(crate) struct RestoreIdentity {
    pub restore: Uuid,
    pub set: Uuid,
    pub incarnation: Uuid,
    pub manifest_hash: String,
    pub key_manifest_hash: String,
}
pub(crate) struct MetadataRestoreProof {
    identity: RestoreIdentity,
    dump_sha256: String,
    bytes: u64,
}
impl MetadataRestoreProof {
    pub(crate) fn identity(&self) -> &RestoreIdentity {
        &self.identity
    }
    pub(crate) fn sha256(&self) -> &str {
        &self.dump_sha256
    }
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}
pub(crate) async fn restore_database(
    db: &crate::database::Database,
    archive: &VerifiedMetadataArchive,
    identity: RestoreIdentity,
    timeout: std::time::Duration,
) -> Result<MetadataRestoreProof> {
    use std::process::Stdio;
    ensure!(
        identity.manifest_hash.len() == 64
            && identity.key_manifest_hash.len() == 64
            && timeout.as_secs() > 0
            && timeout.as_secs() <= 86400,
        "RECOVERY_INVALID"
    );
    let roles = &archive.database_roles;
    let memberships = &archive.database_memberships;
    let operation = async {
        let client = db.restore_bootstrap_connection().await?;
        client
            .query_one(
                "SELECT pg_advisory_lock(hashtextextended('moria/restore-bootstrap/v1',0))",
                &[],
            )
            .await?;
        let server: String = client
            .query_one("SHOW server_version_num", &[])
            .await?
            .get(0);
        ensure!(
            server.parse::<u32>()? / 10000 == 18,
            "RECOVERY_ENGINE_UNSUPPORTED"
        );
        let present=client.query_one("SELECT to_regnamespace('moria') IS NOT NULL,to_regclass('public.moria_restore_bootstrap') IS NOT NULL",&[]).await?;
        let exists: bool = present.get(0);
        let marker: bool = present.get(1);
        ensure!(!exists || marker, "RECOVERY_TARGET_NOT_EMPTY");
        if !marker {
            ensure!(!client.query_one("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname NOT IN ('information_schema','public')) OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p','m'))",&[]).await?.get::<_,bool>(0),"RECOVERY_TARGET_NOT_EMPTY");
            client.batch_execute("CREATE TABLE public.moria_restore_bootstrap(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),dispatch_approved boolean NOT NULL DEFAULT false,restore_id uuid,dump_sha256 text,manifest_hash text,status text NOT NULL DEFAULT 'prepared')").await?;
            client.execute("INSERT INTO public.moria_restore_bootstrap(singleton,dispatch_approved,restore_id,dump_sha256,manifest_hash,status) VALUES(true,false,$1,$2,$3,'prepared')",&[&identity.restore,&archive.sha256(),&identity.manifest_hash]).await?;
        }
        let row=client.query_one("SELECT restore_id,dump_sha256,manifest_hash,status,dispatch_approved FROM public.moria_restore_bootstrap WHERE singleton",&[]).await?;
        ensure!(
            row.get::<_, Option<Uuid>>(0) == Some(identity.restore)
                && row.get::<_, Option<String>>(1).as_deref() == Some(archive.sha256())
                && row.get::<_, Option<String>>(2).as_deref() == Some(&identity.manifest_hash)
                && ["prepared", "applied", "verified"].contains(&row.get::<_, String>(3).as_str())
                && (row.get::<_, bool>(4) == (row.get::<_, String>(3) == "verified")),
            "RECOVERY_TARGET_NOT_EMPTY"
        );
        let current = capture_roles(&client).await?;
        for expected in roles {
            let name = expected["name"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("RECOVERY_DATABASE_ROLE_REQUIRED"))?;
            ensure!(
                current
                    .0
                    .iter()
                    .any(|role| role["name"] == name && role == expected),
                "RECOVERY_DATABASE_ROLE_REQUIRED"
            );
        }
        let names = roles
            .iter()
            .filter_map(|role| role["name"].as_str())
            .collect::<Vec<_>>();
        let actual = current
            .1
            .into_iter()
            .filter(|row| {
                names
                    .iter()
                    .any(|name| row["role"] == *name || row["member"] == *name)
            })
            .collect::<Vec<_>>();
        ensure!(
            actual.as_slice() == memberships.as_slice(),
            "RECOVERY_DATABASE_ROLE_REQUIRED"
        );
        if !exists {
            ensure!(
                row.get::<_, String>(3) == "prepared",
                "RECOVERY_TARGET_NOT_EMPTY"
            );
            let (mut command, database) =
                crate::recovery::postgres_command("MORIA_RECOVERY_PG_RESTORE", "pg_restore")
                    .await?;
            command
                .args([
                    "--single-transaction",
                    "--exit-on-error",
                    "--dbname",
                    &database,
                ])
                .arg(archive.path())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let mut child = command.spawn()?;
            let status = child.wait().await?;
            // A lost COMMIT reply is reconciled only by the single-transaction import's exact bootstrap record.
            if !status.success() {
                ensure!(
                    client
                        .query_one("SELECT to_regnamespace('moria') IS NOT NULL", &[])
                        .await?
                        .get::<_, bool>(0),
                    "RECOVERY_RESTORE_FAILED"
                );
            }
        }
        let schema: serde_json::Value = serde_json::from_str(include_str!("../schema.json"))?;
        client
            .query_one(
                "SELECT moria.runtime_schema_admit($1,$2)",
                &[
                    &(schema["version"].as_i64().unwrap() as i32),
                    &schema["checksum"].as_str().unwrap(),
                ],
            )
            .await?;
        let set = client
            .query_one(
                "SELECT incarnation,key_manifest_hash FROM moria.runtime_recovery_sets WHERE id=$1",
                &[&identity.set],
            )
            .await?;
        ensure!(
            set.get::<_, Uuid>(0) == identity.incarnation
                && set.get::<_, String>(1) == identity.key_manifest_hash,
            "RECOVERY_MANIFEST_CONFLICT"
        );
        let actual: Uuid = client
            .query_one("SELECT incarnation FROM moria.indexing_instance", &[])
            .await?
            .get(0);
        if actual != identity.incarnation {
            ensure!(client.query_opt("SELECT 1 FROM moria.runtime_restores WHERE id=$1 AND previous_incarnation=$2 AND new_incarnation=$3 AND recovery_set=$4 AND manifest_hash=$5",&[&identity.restore,&identity.incarnation,&actual,&identity.set,&identity.manifest_hash]).await?.is_some(),"RECOVERY_MANIFEST_CONFLICT");
        }
        let grant:String=client.query_one("SELECT format('GRANT USAGE ON SCHEMA public TO %I; GRANT SELECT ON public.moria_restore_bootstrap TO %I',r.rolname,r.rolname) FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname='moria'",&[]).await?.get(0);
        client.batch_execute(&grant).await?;
        client
            .execute(
                "UPDATE public.moria_restore_bootstrap SET status='applied' WHERE singleton AND status='prepared'",
                &[],
            )
            .await?;
        drop(client);
        db.recovery_schema_lease().await?;
        Ok(MetadataRestoreProof {
            identity,
            dump_sha256: archive.sha256.clone(),
            bytes: archive.bytes(),
        })
    };
    tokio::time::timeout(timeout, operation)
        .await
        .map_err(|_| anyhow::anyhow!("RECOVERY_DEADLINE"))?
}
