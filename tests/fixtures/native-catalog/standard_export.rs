//! Fixture-only export of exact bytes from an actual captured native graph read.
use anyhow::{Result, ensure};
use futures::TryStreamExt;
use iceberg::spec::{FormatVersion, Manifest, ManifestList};
use moria_runtime::native_snapshot_v3::NativeSnapshot;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

struct Export {
    root: PathBuf,
    names: BTreeSet<String>,
    objects: Vec<Value>,
    bytes: u64,
}
impl Export {
    async fn member(
        &mut self,
        snapshot: &NativeSnapshot,
        name: &str,
        metadata: bool,
        collect: bool,
    ) -> Result<Vec<u8>> {
        ensure!(
            self.names.insert(name.to_owned()),
            "DUPLICATE_EXPORT_MEMBER"
        );
        let file = snapshot.file(name).await?;
        let content = file.content();
        self.bytes = self
            .bytes
            .checked_add(content.size)
            .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
        ensure!(
            self.names.len() <= 1024
                && self.bytes <= 64 * 1024 * 1024
                && content.size > 0
                && content.size <= 16 * 1024 * 1024,
            "RESOURCE_LIMIT"
        );
        let filename = if metadata {
            format!("member-{}.metadata.json", self.names.len())
        } else {
            format!("member-{}.bin", self.names.len())
        };
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.root.join(&filename))?;
        let mut stream = file.stream().await?;
        let mut digest = Sha256::new();
        let mut count = 0u64;
        let mut captured = Vec::new();
        while let Some(frame) = stream.try_next().await? {
            count = count
                .checked_add(frame.len() as u64)
                .ok_or_else(|| anyhow::anyhow!("RESOURCE_LIMIT"))?;
            ensure!(count <= content.size, "CONTENT_MISMATCH");
            output.write_all(&frame)?;
            digest.update(&frame);
            if collect {
                captured.extend_from_slice(&frame);
            }
        }
        ensure!(
            count == content.size && format!("sha256:{:x}", digest.finalize()) == content.sha256,
            "CONTENT_MISMATCH"
        );
        file.check_current().await?;
        self.objects
            .push(json!({"name":name,"file":filename,"size":content.size,"sha256":content.sha256}));
        Ok(captured)
    }
}
pub async fn write(snapshot: &NativeSnapshot, directory: &str, metadata: &str) -> Result<usize> {
    let root = Path::new(directory).canonicalize()?;
    ensure!(
        root.is_dir() && std::fs::read_dir(&root)?.next().is_none(),
        "EXPORT_DIRECTORY_NOT_EMPTY"
    );
    let mut export = Export {
        root,
        names: BTreeSet::new(),
        objects: Vec::new(),
        bytes: 0,
    };
    export.member(snapshot, metadata, true, false).await?;
    let head = snapshot
        .metadata()
        .current_snapshot()
        .ok_or_else(|| anyhow::anyhow!("SNAPSHOT_PIN_MISMATCH"))?;
    let list = export
        .member(snapshot, head.manifest_list(), false, true)
        .await?;
    let list = ManifestList::parse_with_version(&list, FormatVersion::V2)?;
    ensure!(list.entries().len() <= 1024, "RESOURCE_LIMIT");
    for manifest in list.entries() {
        let bytes = export
            .member(snapshot, &manifest.manifest_path, false, true)
            .await?;
        let manifest = Manifest::parse_avro(&bytes)?;
        for entry in manifest.entries() {
            if entry.is_alive() && !export.names.contains(entry.file_path()) {
                export
                    .member(snapshot, entry.file_path(), false, false)
                    .await?;
            }
        }
    }
    snapshot.check_current().await?;
    let manifest = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(export.root.join("export.json"))?;
    serde_json::to_writer(
        manifest,
        &json!({"metadata":metadata,"objects":export.objects}),
    )?;
    Ok(export.names.len())
}
