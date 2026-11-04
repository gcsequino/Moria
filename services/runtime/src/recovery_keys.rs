use crate::{
    canonical::fingerprint,
    recovery::RecoveryLease,
    storage::{
        Cancellation, LogicalObject, ObjectReader, ObjectWriter, ReadAuthority, SealedObject,
        WriteAuthority,
    },
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::PathBuf,
    sync::Arc,
};

struct Key {
    id: String,
    path: PathBuf,
    file: File,
    version: Vec<u64>,
    bytes: Vec<u8>,
    sha256: String,
}
pub struct KeyRecoveryPlan {
    keys: Vec<Key>,
    active_session: Option<Key>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RecoveryKey {
    pub key_id: String,
    pub copy: SealedObject,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RecoveryKeys {
    pub version: u32,
    pub fingerprint: String,
    pub entries: Vec<RecoveryKey>,
}
fn version(metadata: &std::fs::Metadata) -> Vec<u64> {
    vec![
        metadata.dev(),
        metadata.ino(),
        metadata.size(),
        metadata.uid() as u64,
        metadata.gid() as u64,
        metadata.mode() as u64,
        metadata.nlink(),
        metadata.mtime() as u64,
        metadata.mtime_nsec() as u64,
        metadata.ctime() as u64,
        metadata.ctime_nsec() as u64,
    ]
}
fn open(path: &std::path::Path) -> Result<File> {
    Ok(std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path)?)
}
fn key_fingerprint(entries: impl Iterator<Item = (String, u64, String)>) -> Result<String> {
    let mut values = Vec::new();
    for (id, size, sha) in entries {
        ensure!(
            sha.strip_prefix("sha256:")
                .is_some_and(|digest| digest.len() == 64
                    && digest
                        .bytes()
                        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))),
            "RECOVERY_KEY_INVALID"
        );
        values.push(serde_json::json!({"keyId":id,"size":size.to_string(),"sha256":sha}));
    }
    Ok(fingerprint(&serde_json::json!(values))?
        .trim_start_matches("sha256:")
        .to_owned())
}
impl KeyRecoveryPlan {
    pub fn configured() -> Result<Self> {
        let value = std::env::var("MORIA_RECOVERY_KEYS")
            .map_err(|_| anyhow::anyhow!("RECOVERY_KEYS_REQUIRED"))?;
        ensure!(value.len() <= 65536, "RECOVERY_INVALID");
        let mut plan = Self::from_bindings(serde_json::from_str(&value)?)?;
        let directory = PathBuf::from(
            std::env::var("MORIA_LOCAL_DIR")
                .map_err(|_| anyhow::anyhow!("RECOVERY_KEYS_REQUIRED"))?,
        );
        ensure!(directory.is_absolute(), "RECOVERY_KEY_INVALID");
        let mut active = Self::from_bindings(BTreeMap::from([(
            "session-encryption".into(),
            directory.join("auth/session-key"),
        )]))?;
        ensure!(
            plan.keys
                .iter()
                .find(|key| key.id == "session-encryption")
                .unwrap()
                .bytes
                == active.keys[0].bytes,
            "RECOVERY_KEY_CHANGED"
        );
        active.check_unchanged()?;
        plan.active_session = active.keys.pop();
        plan.check_unchanged()?;
        Ok(plan)
    }
    fn from_bindings(bindings: BTreeMap<String, PathBuf>) -> Result<Self> {
        ensure!(
            !bindings.is_empty()
                && bindings.len() <= 64
                && bindings.contains_key("session-encryption"),
            "RECOVERY_KEYS_REQUIRED"
        );
        let uid = std::fs::metadata("/proc/self")?.uid();
        let mut keys = Vec::new();
        let mut total = 0usize;
        for (id, path) in bindings {
            ensure!(
                !id.is_empty()
                    && id.len() <= 64
                    && id
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
                    && path.is_absolute(),
                "RECOVERY_INVALID"
            );
            let mut file = open(&path)?;
            let metadata = file.metadata()?;
            ensure!(
                metadata.is_file()
                    && metadata.uid() == uid
                    && metadata.mode() & 0o077 == 0
                    && metadata.nlink() == 1
                    && metadata.len() > 0
                    && metadata.len() <= 1024 * 1024,
                "RECOVERY_KEY_INVALID"
            );
            if id == "session-encryption" {
                ensure!(metadata.len() == 32, "RECOVERY_KEY_INVALID");
            }
            let mut bytes = Vec::new();
            file.by_ref()
                .take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            total += bytes.len();
            ensure!(
                total <= 8 * 1024 * 1024
                    && bytes.len() as u64 == metadata.len()
                    && version(&file.metadata()?) == version(&metadata),
                "RECOVERY_KEY_CHANGED"
            );
            let sha256 = format!("sha256:{:x}", Sha256::digest(&bytes));
            keys.push(Key {
                id,
                path,
                file,
                version: version(&metadata),
                bytes,
                sha256,
            });
        }
        Ok(Self {
            keys,
            active_session: None,
        })
    }
    pub fn fingerprint(&self) -> Result<String> {
        key_fingerprint(
            self.keys
                .iter()
                .map(|key| (key.id.clone(), key.bytes.len() as u64, key.sha256.clone())),
        )
    }
    pub fn check_unchanged(&self) -> Result<()> {
        for key in self.keys.iter().chain(self.active_session.iter()) {
            ensure!(
                version(&key.file.metadata()?) == key.version
                    && version(&open(&key.path)?.metadata()?) == key.version,
                "RECOVERY_KEY_CHANGED"
            );
        }
        Ok(())
    }
    pub async fn copy(
        &self,
        lease: &RecoveryLease,
        warehouse: &str,
        table: &str,
        store: &dyn ObjectWriter,
        authority: Arc<dyn WriteAuthority>,
        cancel: Cancellation,
    ) -> Result<RecoveryKeys> {
        self.check_unchanged()?;
        lease.check_key_manifest(&self.fingerprint()?).await?;
        let mut entries = Vec::new();
        for (index, key) in self.keys.iter().enumerate() {
            let object = LogicalObject::new(
                warehouse,
                table,
                &format!("{}_key_{index}", lease.scope_key()),
            )?;
            let mut write = store
                .begin_object(
                    object,
                    key.bytes.len() as u64,
                    key.sha256.clone(),
                    authority.clone(),
                    cancel.clone(),
                )
                .await?;
            write.append(&key.bytes).await?;
            let copy = write.seal().await?;
            lease.check().await?;
            self.check_unchanged()?;
            entries.push(RecoveryKey {
                key_id: key.id.clone(),
                copy,
            });
        }
        Ok(RecoveryKeys {
            version: 1,
            fingerprint: self.fingerprint()?,
            entries,
        })
    }
}
impl RecoveryKeys {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.entries.iter().all(|key| !key.key_id.is_empty()
                && key.key_id.len() <= 64
                && key
                    .key_id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')),
            "RECOVERY_KEY_INVALID"
        );
        ensure!(
            self.version == 1
                && !self.entries.is_empty()
                && self.entries.len() <= 64
                && self
                    .entries
                    .windows(2)
                    .all(|pair| pair[0].key_id < pair[1].key_id),
            "RECOVERY_KEY_INVALID"
        );
        ensure!(
            self.entries
                .iter()
                .any(|key| key.key_id == "session-encryption" && key.copy.size == 32),
            "RECOVERY_KEYS_REQUIRED"
        );
        ensure!(
            self.entries
                .iter()
                .all(|key| key.copy.size > 0 && key.copy.size <= 1024 * 1024)
                && self.entries.iter().map(|key| key.copy.size).sum::<u64>() <= 8 * 1024 * 1024,
            "RECOVERY_KEY_INVALID"
        );
        ensure!(
            key_fingerprint(self.entries.iter().map(|key| (
                key.key_id.clone(),
                key.copy.size,
                key.copy.sha256.clone()
            )))? == self.fingerprint,
            "RECOVERY_KEY_CHANGED"
        );
        Ok(())
    }
    pub async fn verify(
        &self,
        reader: &dyn ObjectReader,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<()> {
        self.validate()?;
        for key in &self.entries {
            let version = reader.version(&key.copy.object, authority, cancel).await?;
            ensure!(version.size == key.copy.size, "RECOVERY_KEY_CHANGED");
            let bytes = reader
                .range_at_version(
                    &key.copy.object,
                    0..key.copy.size,
                    &version,
                    authority,
                    cancel,
                )
                .await?;
            ensure!(
                format!("sha256:{:x}", Sha256::digest(&bytes)) == key.copy.sha256,
                "RECOVERY_KEY_CHANGED"
            );
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    #[test]
    fn actual_key_files_are_private_versioned_and_never_followed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("key");
        std::fs::write(&path, [7u8; 32]).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let plan = KeyRecoveryPlan::from_bindings(BTreeMap::from([(
            "session-encryption".into(),
            path.clone(),
        )]))
        .unwrap();
        plan.check_unchanged().unwrap();
        assert_eq!(plan.fingerprint().unwrap().len(), 64);
        let manifest = RecoveryKeys {
            version: 1,
            fingerprint: plan.fingerprint().unwrap(),
            entries: vec![RecoveryKey {
                key_id: "session-encryption".into(),
                copy: SealedObject {
                    object: LogicalObject::new("backup", "set", "key").unwrap(),
                    size: 32,
                    sha256: plan.keys[0].sha256.clone(),
                },
            }],
        };
        manifest.validate().unwrap();
        let mut wrong = manifest;
        wrong.entries[0].copy.sha256 = wrong.entries[0]
            .copy
            .sha256
            .trim_start_matches("sha256:")
            .to_owned();
        assert!(wrong.validate().is_err());
        std::fs::write(&path, [8u8; 32]).unwrap();
        assert!(plan.check_unchanged().is_err());
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(
            KeyRecoveryPlan::from_bindings(BTreeMap::from([("session-encryption".into(), link)]))
                .is_err()
        );
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            KeyRecoveryPlan::from_bindings(BTreeMap::from([("session-encryption".into(), path)]))
                .is_err()
        );
    }
}

pub struct KeyRestoreProof {
    fingerprint: String,
    keys: KeyRecoveryPlan,
}
impl KeyRestoreProof {
    pub fn check_current(&self) -> Result<()> {
        self.keys.check_unchanged()?;
        ensure!(
            self.keys.fingerprint()? == self.fingerprint,
            "RECOVERY_KEY_CHANGED"
        );
        Ok(())
    }
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}
struct KeyTemp(PathBuf);
impl Drop for KeyTemp {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
impl RecoveryKeys {
    pub async fn restore(
        &self,
        reader: &dyn ObjectReader,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<KeyRestoreProof> {
        use std::os::unix::fs::PermissionsExt;
        use tokio::io::AsyncWriteExt;
        self.validate()?;
        let configured = std::env::var("MORIA_RECOVERY_RESTORE_KEYS")
            .map_err(|_| anyhow::anyhow!("RECOVERY_KEYS_REQUIRED"))?;
        ensure!(configured.len() <= 65536, "RECOVERY_INVALID");
        let targets: BTreeMap<String, PathBuf> = serde_json::from_str(&configured)?;
        ensure!(
            targets.len() == self.entries.len()
                && self
                    .entries
                    .iter()
                    .all(|key| targets.contains_key(&key.key_id)),
            "RECOVERY_KEYS_REQUIRED"
        );
        let local = PathBuf::from(
            std::env::var("MORIA_LOCAL_DIR")
                .map_err(|_| anyhow::anyhow!("RECOVERY_KEYS_REQUIRED"))?,
        );
        ensure!(local.is_absolute(), "RECOVERY_KEY_INVALID");
        let session = &targets["session-encryption"];
        ensure!(
            session
                .file_name()
                .is_some_and(|name| name == "session-key")
                && session.parent().map(std::fs::canonicalize).transpose()?
                    == Some(std::fs::canonicalize(local.join("auth"))?),
            "RECOVERY_KEY_INVALID"
        );
        let uid = std::fs::metadata("/proc/self")?.uid();
        for key in &self.entries {
            cancel.check()?;
            let target = &targets[&key.key_id];
            ensure!(target.is_absolute(), "RECOVERY_KEY_INVALID");
            let parent = target
                .parent()
                .ok_or_else(|| anyhow::anyhow!("RECOVERY_KEY_INVALID"))?;
            let actual_parent = std::fs::canonicalize(parent)?;
            let metadata = std::fs::metadata(&actual_parent)?;
            ensure!(
                metadata.is_dir()
                    && metadata.uid() == uid
                    && metadata.permissions().mode() & 0o077 == 0,
                "RECOVERY_KEY_INVALID"
            );
            let target = actual_parent.join(
                target
                    .file_name()
                    .ok_or_else(|| anyhow::anyhow!("RECOVERY_KEY_INVALID"))?,
            );
            let version = reader.version(&key.copy.object, authority, cancel).await?;
            ensure!(version.size == key.copy.size, "RECOVERY_KEY_CHANGED");
            let bytes = reader
                .range_at_version(
                    &key.copy.object,
                    0..key.copy.size,
                    &version,
                    authority,
                    cancel,
                )
                .await?;
            ensure!(
                format!("sha256:{:x}", Sha256::digest(&bytes)) == key.copy.sha256,
                "RECOVERY_KEY_CHANGED"
            );
            if target.try_exists()? {
                let mut existing = open(&target)?;
                let metadata = existing.metadata()?;
                ensure!(
                    metadata.is_file()
                        && metadata.uid() == uid
                        && metadata.permissions().mode() & 0o077 == 0
                        && metadata.nlink() == 1
                        && metadata.len() == key.copy.size,
                    "RECOVERY_KEY_INVALID"
                );
                let mut content = Vec::new();
                existing
                    .by_ref()
                    .take(1024 * 1024 + 1)
                    .read_to_end(&mut content)?;
                ensure!(
                    content == bytes && version_metadata_unchanged(&existing, &metadata)?,
                    "RECOVERY_KEY_CHANGED"
                );
                continue;
            }
            let temporary =
                KeyTemp(actual_parent.join(format!(".{}.key.tmp", uuid::Uuid::new_v4())));
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary.0)
                .await?;
            file.write_all(&bytes).await?;
            file.sync_all().await?;
            drop(file);
            cancel.check()?;
            std::fs::hard_link(&temporary.0, &target)?;
            drop(temporary);
            File::open(&actual_parent)?.sync_all()?;
        }
        let keys = KeyRecoveryPlan::from_bindings(targets)?;
        ensure!(
            keys.fingerprint()? == self.fingerprint,
            "RECOVERY_KEY_CHANGED"
        );
        Ok(KeyRestoreProof {
            fingerprint: self.fingerprint.clone(),
            keys,
        })
    }
}
fn version_metadata_unchanged(file: &File, metadata: &std::fs::Metadata) -> Result<bool> {
    Ok(version(&file.metadata()?) == version(metadata))
}
