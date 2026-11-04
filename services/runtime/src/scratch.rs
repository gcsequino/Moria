use crate::{database::Database, execution::Lease, sandbox::Docker};
use anyhow::{Result, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
const OWNER_FILE: &str = ".moria-scratch-owner";
use tokio_postgres::Client;
use uuid::Uuid;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Slot {
    id: Uuid,
    host_id: Uuid,
    mount_root: PathBuf,
    filesystem_uuid: Uuid,
    capacity_bytes: u64,
    resource_class: String,
}

/// Only an exact live engine attempt can obtain its operator-provisioned hard limit.
pub struct VerifiedScratch {
    db: Database,
    reservation: Uuid,
    lease: Lease,
    host: Uuid,
    slot: Slot,
}

impl std::fmt::Debug for VerifiedScratch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedScratch")
            .field("reservation", &self.reservation)
            .field("slot", &self.slot.id)
            .field("capacityBytes", &self.slot.capacity_bytes)
            .finish_non_exhaustive()
    }
}

fn mount_record(value: &Value, slot: &Slot) -> Result<()> {
    let files = value["filesystems"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("SCRATCH_UNAVAILABLE"))?;
    ensure!(files.len() == 1, "SCRATCH_UNAVAILABLE");
    let fs = &files[0];
    ensure!(
        fs["target"].as_str() == slot.mount_root.to_str()
            && fs["fstype"] == "ext4"
            && fs["fsroot"] == "/"
            && fs["uuid"].as_str().and_then(|v| Uuid::parse_str(v).ok())
                == Some(slot.filesystem_uuid),
        "SCRATCH_UNAVAILABLE"
    );
    let options = fs["options"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("SCRATCH_UNAVAILABLE"))?
        .split(',')
        .collect::<Vec<_>>();
    ensure!(
        ["rw", "nodev", "nosuid", "noexec"]
            .iter()
            .all(|v| options.contains(v)),
        "SCRATCH_UNAVAILABLE"
    );
    ensure!(
        fs.get("children")
            .is_none_or(|v| v.as_array().is_some_and(Vec::is_empty)),
        "SCRATCH_UNAVAILABLE"
    );
    Ok(())
}

async fn verify(slot: &Slot, empty: bool) -> Result<()> {
    ensure!(
        slot.mount_root.is_absolute()
            && slot.mount_root != Path::new("/")
            && slot
                .mount_root
                .to_str()
                .is_some_and(|s| s.len() <= 1024 && !s.contains([',', ':', '\n', '\r']))
            && (64 * 1024 * 1024..=1024 * 1024 * 1024 * 1024).contains(&slot.capacity_bytes),
        "SCRATCH_UNAVAILABLE"
    );
    let path = slot.mount_root.clone();
    let capacity = slot.capacity_bytes;
    tokio::task::spawn_blocking(move || -> Result<()> {
        ensure!(std::fs::canonicalize(&path)? == path, "SCRATCH_UNAVAILABLE");
        let directory = std::fs::File::open(&path)?;
        let metadata = directory.metadata()?;
        ensure!(
            metadata.is_dir() && metadata.uid() == 0 && metadata.mode() & 0o7777 == 0o1777,
            "SCRATCH_UNAVAILABLE"
        );
        let stats = rustix::fs::fstatvfs(&directory)?;
        let total = stats
            .f_frsize
            .checked_mul(stats.f_blocks)
            .ok_or_else(|| anyhow::anyhow!("SCRATCH_UNAVAILABLE"))?;
        ensure!(
            total >= 32 * 1024 * 1024 && total <= capacity,
            "SCRATCH_UNAVAILABLE"
        );
        if empty {
            ensure!(
                std::fs::read_dir(&path)?
                    .all(|entry| entry.is_ok_and(|entry| entry.file_name() == OWNER_FILE)),
                "SCRATCH_NOT_CLEAN"
            );
        }
        Ok(())
    })
    .await??;
    let mut command = tokio::process::Command::new("/usr/bin/findmnt");
    command
        .env_clear()
        .args([
            "--json",
            "--submounts",
            "--output",
            "TARGET,SOURCE,FSTYPE,OPTIONS,UUID,FSROOT",
            "--mountpoint",
        ])
        .arg(&slot.mount_root)
        .kill_on_drop(true);
    let result = tokio::time::timeout(Duration::from_secs(5), command.output()).await??;
    ensure!(
        result.status.success() && result.stdout.len() <= 65536,
        "SCRATCH_UNAVAILABLE"
    );
    mount_record(&serde_json::from_slice(&result.stdout)?, slot)
}

/// Trusted host control needs traversal rights even when a guest chmods its own spill tree.
pub fn verify_cleanup_privilege() -> Result<()> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let effective = status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))
        .and_then(|v| v.split_whitespace().nth(1))
        .and_then(|v| v.parse::<u32>().ok());
    let capabilities = status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:"))
        .and_then(|v| u64::from_str_radix(v.trim(), 16).ok());
    ensure!(
        effective == Some(0) && capabilities.is_some_and(|bits| bits & 2 != 0),
        "SCRATCH_CLEANUP_PRIVILEGE_REQUIRED"
    );
    Ok(())
}

async fn verify_owner(slot: &Slot, incarnation: Uuid, create: bool) -> Result<()> {
    let path = slot.mount_root.clone();
    let id = slot.id;
    tokio::task::spawn_blocking(move || -> Result<()> {
        use std::io::{Read, Write};
        let marker = path.join(OWNER_FILE);
        if create {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&marker)
            {
                Ok(mut file) => {
                    file.write_all(
                        serde_json::to_string(
                            &json!({"version":1,"slot":id,"incarnation":incarnation}),
                        )?
                        .as_bytes(),
                    )?;
                    file.sync_all()?;
                    std::fs::File::open(&path)?.sync_all()?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(&marker)?;
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file()
                && metadata.uid() == 0
                && metadata.mode() & 0o7777 == 0o600
                && metadata.nlink() == 1,
            "SCRATCH_INSTANCE_MISMATCH"
        );
        let mut bytes = Vec::new();
        file.take(4097).read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= 4096, "SCRATCH_INSTANCE_MISMATCH");
        let owner: Value = serde_json::from_slice(&bytes)?;
        ensure!(
            owner == json!({"version":1,"slot":id,"incarnation":incarnation}),
            "SCRATCH_INSTANCE_MISMATCH"
        );
        Ok(())
    })
    .await??;
    Ok(())
}

impl VerifiedScratch {
    pub async fn for_lease(db: Database, lease: &Lease, host: Uuid) -> Result<Self> {
        verify_cleanup_privilege()?;
        let client = db.get().await?;
        let task = lease
            .task
            .ok_or_else(|| anyhow::anyhow!("JOB_LEASE_LOST"))?;
        let row=client.query_opt("SELECT r.id,s.id,s.mount_root,s.filesystem_uuid,s.capacity_bytes,s.resource_class FROM moria.runtime_engine_scratch_reservations r JOIN moria.runtime_engine_scratch_slots s ON s.id=r.slot AND s.enabled WHERE r.execution=$1 AND r.task=$2 AND r.owner=$3 AND r.fence=$4 AND r.host_id=$5 AND r.incarnation=$6 AND r.cleaned_at IS NULL AND s.host_id=r.host_id AND s.incarnation=r.incarnation", &[&lease.execution,&task,&lease.owner,&lease.fence.parse::<i64>()?,&host,&lease.incarnation]).await?.ok_or_else(||anyhow::anyhow!("SCRATCH_UNAVAILABLE"))?;
        let result = Self {
            db: db.clone(),
            reservation: row.get(0),
            lease: lease.clone(),
            host,
            slot: Slot {
                id: row.get(1),
                host_id: host,
                mount_root: PathBuf::from(row.get::<_, String>(2)),
                filesystem_uuid: row.get(3),
                capacity_bytes: u64::try_from(row.get::<_, i64>(4))?,
                resource_class: row.get(5),
            },
        };
        result.check_in(&client).await?;
        verify(&result.slot, false).await?;
        verify_owner(&result.slot, lease.incarnation, false).await?;
        Ok(result)
    }
    pub fn capacity_bytes(&self) -> u64 {
        self.slot.capacity_bytes
    }
    pub fn container_name(&self) -> String {
        format!("moria-engine-scratch-{}", self.reservation)
    }
    async fn check_in(&self, client: &Client) -> Result<()> {
        client
            .query_one(
                "SELECT moria.runtime_task_check($1,$2,$3)",
                &[
                    &self.lease.task,
                    &self.lease.owner,
                    &self.lease.fence.parse::<i64>()?,
                ],
            )
            .await?;
        client.query_one("SELECT moria.runtime_worker_check($1,$2,$3,e.spec->>'domain') FROM moria.runtime_executions e,moria.indexing_instance i WHERE e.id=$4 AND i.incarnation=$5", &[&self.lease.owner,&self.host,&self.slot.resource_class,&self.lease.execution,&self.lease.incarnation]).await?;
        ensure!(client.query_opt("SELECT 1 FROM moria.runtime_engine_scratch_reservations WHERE id=$1 AND cleaned_at IS NULL FOR UPDATE",&[&self.reservation]).await?.is_some(),"SCRATCH_UNAVAILABLE");
        Ok(())
    }
    /// Creation and cleanup serialize on the durable reservation, including uncertain create replies.
    pub async fn create_container(&self, mut config: Value) -> Result<String> {
        verify_cleanup_privilege()?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        self.check_in(tx.client()).await?;
        verify(&self.slot, true).await?;
        verify_owner(&self.slot, self.lease.incarnation, false).await?;
        let started:bool=tx.query_one("SELECT creation_started FROM moria.runtime_engine_scratch_reservations WHERE id=$1",&[&self.reservation]).await?.get(0);
        ensure!(!started, "SCRATCH_CREATION_UNCERTAIN");
        tx.execute("UPDATE moria.runtime_engine_scratch_reservations SET creation_started=true WHERE id=$1",&[&self.reservation]).await?;
        tx.commit().await?;
        let tx = client.transaction().await?;
        self.check_in(tx.client()).await?;
        let docker = Docker::new()?;
        let name = self.container_name();
        ensure!(
            docker.inspect(&name).await?.is_none(),
            "SCRATCH_UNAVAILABLE"
        );
        let labels = config
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("SCRATCH_UNAVAILABLE"))?
            .entry("Labels")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("SCRATCH_UNAVAILABLE"))?;
        labels.insert("moria.scratch-reservation".into(), json!(self.reservation));
        let host = config
            .get_mut("HostConfig")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| anyhow::anyhow!("SCRATCH_UNAVAILABLE"))?;
        let mounts = host
            .entry("Mounts")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .ok_or_else(|| anyhow::anyhow!("SCRATCH_UNAVAILABLE"))?;
        ensure!(
            !mounts.iter().any(|m| m["Target"] == "/spill"),
            "SCRATCH_UNAVAILABLE"
        );
        mounts.push(json!({"Type":"bind","Source":self.slot.mount_root,"Target":"/spill","ReadOnly":false,"BindOptions":{"Propagation":"rprivate","NonRecursive":true}}));
        let mut url = Docker::url(&["containers", "create"])?;
        url.query_pairs_mut().append_pair("name", &name);
        let result =
            Docker::json_response(docker.client.post(url).json(&config).send().await?).await?;
        let id = result["Id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("SCRATCH_UNAVAILABLE"))?;
        ensure!(
            id.len() == 64
                && id
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "SCRATCH_UNAVAILABLE"
        );
        tx.execute("UPDATE moria.runtime_engine_scratch_reservations SET container_id=$2,creation_acknowledged=true WHERE id=$1",&[&self.reservation,&id]).await?;
        tx.commit().await?;
        Ok(id.into())
    }
    /// A lost lease does not make a possibly running container's storage reusable.
    pub async fn cleanup(&self) -> Result<()> {
        cleanup_reservation(&self.db, self.reservation, self.host).await
    }
}

pub(crate) async fn cleanup_reservation(
    db: &Database,
    reservation: Uuid,
    host: Uuid,
) -> Result<()> {
    verify_cleanup_privilege()?;
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    let row=tx.query_opt("SELECT s.id,s.mount_root,s.filesystem_uuid,s.capacity_bytes,s.resource_class,r.container_id,r.creation_started,r.creation_acknowledged,r.cleanup_nonce,r.incarnation FROM moria.runtime_engine_scratch_reservations r JOIN moria.runtime_engine_scratch_slots s ON s.id=r.slot WHERE r.id=$1 AND r.host_id=$2 AND r.cleaned_at IS NULL FOR UPDATE OF r,s",&[&reservation,&host]).await?;
    let Some(row) = row else { return Ok(()) };
    ensure!(
        row.get::<_, Option<Uuid>>(8).is_none(),
        "SCRATCH_CLEANUP_UNCERTAIN"
    );
    let slot = Slot {
        id: row.get(0),
        host_id: host,
        mount_root: PathBuf::from(row.get::<_, String>(1)),
        filesystem_uuid: row.get(2),
        capacity_bytes: u64::try_from(row.get::<_, i64>(3))?,
        resource_class: row.get(4),
    };
    verify(&slot, false).await?;
    verify_owner(&slot, row.get(9), false).await?;
    let docker = Docker::new()?;
    let name = format!("moria-engine-scratch-{reservation}");
    if let Some(actual) = docker.inspect(&name).await? {
        ensure!(
            actual["Config"]["Labels"]["moria.scratch-reservation"] == reservation.to_string()
                && actual["State"]["Running"] == false
                && row
                    .get::<_, Option<String>>(5)
                    .is_none_or(|id| actual["Id"] == id),
            "SCRATCH_CONTAINER_NOT_TERMINAL"
        );
        Docker::json_response(
            docker
                .client
                .delete(Docker::url(&["containers", &name])?)
                .send()
                .await?,
        )
        .await?;
    } else {
        ensure!(
            !row.get::<_, bool>(6) || row.get::<_, bool>(7),
            "SCRATCH_CREATION_UNCERTAIN"
        );
    }
    // A cancelled future cannot make a still-running blocking deleter's slot reusable.
    let cleanup_nonce = Uuid::new_v4();
    tx.execute(
        "UPDATE moria.runtime_engine_scratch_reservations SET cleanup_nonce=$2 WHERE id=$1",
        &[&reservation, &cleanup_nonce],
    )
    .await?;
    tx.commit().await?;
    // The exact mounted filesystem is verified again before each destructive cleanup.
    verify(&slot, false).await?;
    let path = slot.mount_root.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        for child in std::fs::read_dir(&path)? {
            let child = child?;
            if child.file_name() == OWNER_FILE {
                continue;
            }
            if child.file_type()?.is_dir() {
                std::fs::remove_dir_all(child.path())?;
            } else {
                std::fs::remove_file(child.path())?;
            }
        }
        rustix::fs::syncfs(std::fs::File::open(&path)?)?;
        Ok(())
    })
    .await??;
    verify(&slot, true).await?;
    ensure!(db.get().await?.execute("UPDATE moria.runtime_engine_scratch_reservations SET cleaned_at=clock_timestamp() WHERE id=$1 AND cleanup_nonce=$2 AND cleaned_at IS NULL",&[&reservation,&cleanup_nonce]).await?==1,"SCRATCH_CLEANUP_UNCERTAIN");
    Ok(())
}

pub async fn cli(db: Database, args: Vec<String>) -> Result<()> {
    match args.first().map(String::as_str) {
        Some("register") => {
            use tokio::io::AsyncReadExt;
            let mut bytes = Vec::new();
            tokio::io::stdin()
                .take(65537)
                .read_to_end(&mut bytes)
                .await?;
            verify_cleanup_privilege()?;
            ensure!(bytes.len() <= 65536, "SCRATCH_UNAVAILABLE");
            let slot: Slot = serde_json::from_slice(&bytes)?;
            verify(&slot, true).await?;
            let instance: Uuid = db
                .get()
                .await?
                .query_one("SELECT incarnation FROM moria.indexing_instance", &[])
                .await?
                .get(0);
            verify_owner(&slot, instance, true).await?;
            let capacity = i64::try_from(slot.capacity_bytes)?;
            db.get().await?.execute("INSERT INTO moria.runtime_engine_scratch_slots(id,host_id,mount_root,filesystem_uuid,capacity_bytes,resource_class,incarnation) SELECT $1,$2,$3,$4,$5,$6,incarnation FROM moria.indexing_instance",&[&slot.id,&slot.host_id,&slot.mount_root.to_str().unwrap(),&slot.filesystem_uuid,&capacity,&slot.resource_class]).await?;
            println!(
                "{}",
                json!({"id":slot.id,"capacityBytes":slot.capacity_bytes})
            );
        }
        Some("cleanup") if args.len() == 3 => {
            cleanup_reservation(&db, Uuid::parse_str(&args[1])?, Uuid::parse_str(&args[2])?).await?
        }
        _ => anyhow::bail!("SCRATCH_UNAVAILABLE"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dedicated_filesystem_required() {
        let slot = Slot {
            id: Uuid::nil(),
            host_id: Uuid::nil(),
            mount_root: "/srv/spill/slot".into(),
            filesystem_uuid: Uuid::new_v4(),
            capacity_bytes: 67108864,
            resource_class: "engine".into(),
        };
        let valid = json!({"filesystems":[{"target":slot.mount_root,"fstype":"ext4","fsroot":"/","uuid":slot.filesystem_uuid,"options":"rw,nosuid,nodev,noexec,relatime"}]});
        assert!(mount_record(&valid, &slot).is_ok());
        for (key, value) in [
            ("fstype", json!("tmpfs")),
            ("fsroot", json!("/directory")),
            ("uuid", json!(Uuid::new_v4())),
            ("options", json!("rw,nosuid,nodev")),
            ("children", json!([{}])),
        ] {
            let mut invalid = valid.clone();
            invalid["filesystems"][0][key] = value;
            assert!(mount_record(&invalid, &slot).is_err());
        }
    }
}
