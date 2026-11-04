use anyhow::{Result, ensure};
use futures::StreamExt;
use moria_runtime::snapshot::{SnapshotPin, verify_snapshot};
use moria_runtime::{database::Database, io::Scope, storage::*};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    io::{BufRead, Write},
    path::Path,
    sync::Arc,
};
use uuid::Uuid;

#[derive(Debug)]
struct RecoveryPause {
    write: Arc<dyn WriteAuthority>,
    checks: std::sync::atomic::AtomicUsize,
    pause_at: usize,
    build: Uuid,
}
#[derive(Debug)]
struct FixtureRemoveAuthority {
    write: Arc<dyn WriteAuthority>,
    object: LogicalObject,
    version: ObjectVersion,
}
#[async_trait::async_trait]
impl RemoveAuthority for FixtureRemoveAuthority {
    async fn check_remove(&self, object: &LogicalObject, version: &ObjectVersion) -> Result<()> {
        ensure!(
            *object == self.object && *version == self.version,
            "FIXTURE_REMOVE_DENIED"
        );
        self.write.check_write(object).await
    }
}
#[async_trait::async_trait]
impl WriteAuthority for RecoveryPause {
    fn scope_key(&self) -> String {
        self.write.scope_key()
    }
    async fn check_write(&self, object: &LogicalObject) -> Result<()> {
        self.write.check_write(object).await?;
        if self
            .checks
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1
            == self.pause_at
        {
            event("recovery-checkpoint", self.build);
            proceed();
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    root: String,
    build: Uuid,
    owner: Uuid,
    fence: i64,
    revision: String,
    endpoint: String,
    bucket: String,
    mode: String,
    spool_id: Option<Uuid>,
    snapshot: Option<SnapshotFixture>,
    snapshot_provider: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotFixture {
    metadata: String,
    snapshot_id: String,
    table_uuid: Uuid,
    objects: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    retired_files: Vec<String>,
}

fn hash(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}
fn object(id: &str) -> LogicalObject {
    LogicalObject::new("artifactwarehouse", "artifacttable", id).unwrap()
}
fn entries(start: usize, end: usize) -> Vec<ClosureEntry> {
    (start..end)
        .map(|i| ClosureEntry {
            name: format!(
                "moria://artifactwarehouse/artifacttable/data/{i:08}-{}.parquet",
                "x".repeat(350)
            ),
            content: SealedObject {
                object: object("small"),
                size: 3,
                sha256: hash(b"abc"),
            },
        })
        .collect()
}
fn event(name: &str, id: Uuid) {
    println!("{}", serde_json::json!({"event":name,"id":id}));
    std::io::stdout().flush().unwrap();
}
fn proceed() {
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
}

async fn assert_recovery(
    store: Arc<dyn ObjectStore>,
    content: SealedObject,
    read: Arc<dyn ReadAuthority>,
    write: Arc<dyn WriteAuthority>,
    cancel: Cancellation,
) -> Result<()> {
    ensure!(
        store
            .recover_object(
                content.object.clone(),
                content.size,
                content.sha256.clone(),
                read.clone(),
                write.clone(),
                cancel.clone()
            )
            .await?
            == Some(content.clone())
    );
    ensure!(
        store
            .recover_object(
                content.object.clone(),
                content.size,
                hash(b"wrong"),
                read.clone(),
                write.clone(),
                cancel.clone()
            )
            .await
            .is_err()
    );
    ensure!(
        store
            .recover_object(
                content.object.clone(),
                content.size + 1,
                content.sha256.clone(),
                read.clone(),
                write.clone(),
                cancel.clone()
            )
            .await
            .is_err()
    );
    ensure!(
        store
            .recover_object(
                object("never-sealed"),
                3,
                hash(b"abc"),
                read.clone(),
                write.clone(),
                cancel.clone()
            )
            .await?
            .is_none()
    );
    Ok(())
}
async fn put(
    store: &dyn ObjectStore,
    bytes: &[u8],
    read: Arc<dyn ReadAuthority>,
    write: Arc<dyn WriteAuthority>,
) -> Result<SealedObject> {
    let sha256 = hash(bytes);
    let object = object(&sha256[7..]);
    let cancel = Cancellation::default();
    let mut pending = store
        .begin_object(
            object.clone(),
            bytes.len() as u64,
            sha256.clone(),
            write,
            cancel.clone(),
        )
        .await?;
    for chunk in bytes.chunks(1024 * 1024) {
        pending.append(chunk).await?;
    }
    match pending.seal().await {
        Ok(sealed) => Ok(sealed),
        Err(_) => {
            let mut stream = store.read_object(object.clone(), read, cancel).await?;
            let mut actual = Sha256::new();
            let mut size = 0;
            while let Some(frame) = stream.next().await {
                let frame = frame?;
                size += frame.len();
                actual.update(frame);
            }
            ensure!(size == bytes.len() && format!("sha256:{:x}", actual.finalize()) == sha256);
            Ok(SealedObject {
                object,
                size: size as u64,
                sha256,
            })
        }
    }
}

#[test]
#[ignore = "requires the mandatory real PostgreSQL/MinIO Node integration fixture"]
fn provider_and_authority() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(run()).unwrap();
}

async fn run() -> Result<()> {
    let fixture: Fixture = serde_json::from_str(&std::env::var("MORIA_NATIVE_STORAGE_FIXTURE")?)?;
    let db = Database::configured()?;
    db.schema_lease().await?;
    let scope = Scope {
        db: db.clone(),
        build: fixture.build,
        task: None,
        owner: fixture.owner,
        fence: fixture.fence,
    };
    let namespace = IndexNamespace {
        warehouse_id: "artifactwarehouse".into(),
        table_id: "artifacttable".into(),
        revision: fixture.revision.clone(),
    };
    let read = Arc::new(IndexNamespaceReadAuthority {
        scope: scope.clone(),
        namespace: namespace.clone(),
    });
    let write = Arc::new(IndexArtifactWriteAuthority {
        scope: scope.clone(),
        namespace,
    });
    let source_namespace = IndexNamespace {
        warehouse_id: "sourcewarehouse".into(),
        table_id: "sourcetable".into(),
        revision: fixture.revision.clone(),
    };
    let source_read = Arc::new(IndexNamespaceReadAuthority {
        scope: scope.clone(),
        namespace: source_namespace.clone(),
    });
    let source_object = LogicalObject::new("sourcewarehouse", "sourcetable", "payload")?;
    let limits = StorageLimits {
        max_object_bytes: 128 * 1024 * 1024,
        chunk_bytes: 1024 * 1024,
    };
    let posix = PosixStore::provision(
        Path::new(&fixture.root),
        "artifactwarehouse",
        "artifacttable",
        limits,
    )?;
    let s3 = S3ReadStore::provision(S3Binding {
        warehouse_id: "sourcewarehouse".into(),
        table_id: "sourcetable".into(),
        endpoint: fixture.endpoint.clone(),
        bucket: fixture.bucket.clone(),
        prefix: "native".into(),
        region: "us-east-1".into(),
        access_key_secret_ref: "NATIVE_STORAGE_ACCESS_KEY".into(),
        secret_key_secret_ref: "NATIVE_STORAGE_SECRET_KEY".into(),
        session_token_secret_ref: None,
        limits,
    })?;
    let artifact_s3 = S3ReadStore::provision(S3Binding {
        warehouse_id: "artifactwarehouse".into(),
        table_id: "artifacttable".into(),
        endpoint: fixture.endpoint.clone(),
        bucket: fixture.bucket.clone(),
        prefix: "native".into(),
        region: "us-east-1".into(),
        access_key_secret_ref: "NATIVE_STORAGE_ACCESS_KEY".into(),
        secret_key_secret_ref: "NATIVE_STORAGE_SECRET_KEY".into(),
        session_token_secret_ref: None,
        limits,
    })?;
    let cancel = Cancellation::default();
    match fixture.mode.as_str() {
        "versioned-delete" => {
            let content = put(
                &artifact_s3,
                b"versioned payload",
                read.clone(),
                write.clone(),
            )
            .await?;
            let version = artifact_s3
                .version(&content.object, read.as_ref(), &cancel)
                .await?;
            ensure!(
                version.token.starts_with("s3v1/"),
                "VERSIONED_PROVIDER_REQUIRED"
            );
            println!(
                "{}",
                serde_json::json!({"event":"version-ready","content":content,"version":version})
            );
            std::io::stdout().flush()?;
            proceed();
            let current = artifact_s3
                .version(&content.object, read.as_ref(), &cancel)
                .await?;
            ensure!(current != version, "VERSION_REPLACEMENT_REQUIRED");
            ensure!(
                artifact_s3
                    .range_at_version(
                        &content.object,
                        0..content.size,
                        &version,
                        read.as_ref(),
                        &cancel
                    )
                    .await?
                    .as_ref()
                    == b"versioned payload"
            );
            let remove = Arc::new(FixtureRemoveAuthority {
                write: write.clone(),
                object: content.object.clone(),
                version: version.clone(),
            });
            ensure!(
                artifact_s3
                    .remove_object(
                        content.object.clone(),
                        current.clone(),
                        remove.clone(),
                        cancel.clone()
                    )
                    .await
                    .is_err()
            );
            ensure!(
                artifact_s3
                    .remove_object(
                        content.object.clone(),
                        version.clone(),
                        remove.clone(),
                        cancel.clone()
                    )
                    .await?
            );
            ensure!(
                artifact_s3
                    .version(&content.object, read.as_ref(), &cancel)
                    .await?
                    == current
            );
            ensure!(
                artifact_s3
                    .range_at_version(
                        &content.object,
                        0..content.size,
                        &version,
                        read.as_ref(),
                        &cancel
                    )
                    .await
                    .is_err()
            );
            ensure!(
                !artifact_s3
                    .remove_object(content.object.clone(), version, remove, cancel.clone())
                    .await?
            );
        }
        mode if mode.starts_with("snapshot-") => {
            let destination: Arc<dyn ObjectStore> = match fixture.snapshot_provider.as_deref() {
                Some("s3") => Arc::new(artifact_s3.clone()),
                None | Some("posix") => Arc::new(posix.clone()),
                _ => anyhow::bail!("Unknown snapshot provider"),
            };
            let fixture_snapshot = fixture.snapshot.as_ref().unwrap();
            let mut metadata: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&fixture_snapshot.metadata)?)?;
            let variant = mode.strip_prefix("snapshot-").unwrap();
            let mut source_objects = fixture_snapshot
                .objects
                .iter()
                .map(|(name, path)| Ok((name.clone(), std::fs::read(path)?)))
                .collect::<Result<Vec<_>>>()?;
            let mut pin_uuid = fixture_snapshot.table_uuid;
            let mut pin_snapshot: i64 = fixture_snapshot.snapshot_id.parse()?;
            match variant {
                "valid" | "deleted-valid" | "version-hold" => {}
                "deleted-corrupt" => {
                    let (_, bytes) = source_objects
                        .iter_mut()
                        .find(|(name, _)| fixture_snapshot.retired_files.contains(name))
                        .unwrap();
                    bytes.fill(0);
                }
                "wrong-table" => pin_uuid = Uuid::new_v4(),
                "wrong-snapshot" => pin_snapshot += 1,
                "missing" => {
                    source_objects.remove(0);
                }
                "extra" => source_objects.push((
                    "moria://artifactwarehouse/artifacttable/data/extra.bin".into(),
                    b"abc".to_vec(),
                )),
                "malformed-manifest" => {
                    let (_, data) = source_objects
                        .iter_mut()
                        .find(|(name, _)| name.ends_with(".avro"))
                        .unwrap();
                    data.fill(0);
                }
                "malformed-parquet" => {
                    let (_, data) = source_objects
                        .iter_mut()
                        .find(|(name, _)| name.ends_with(".parquet"))
                        .unwrap();
                    data.fill(0);
                }
                "nullable-array" => {
                    metadata["schemas"][0]["fields"]
                        .as_array_mut()
                        .unwrap()
                        .iter_mut()
                        .find(|f| f["name"] == "tags")
                        .unwrap()["type"]["element-required"] = false.into()
                }
                "historical-schema" => {
                    let (_, data) = source_objects
                        .iter_mut()
                        .find(|(name, _)| name.ends_with(".metadata.json"))
                        .unwrap();
                    let mut historical: serde_json::Value = serde_json::from_slice(data)?;
                    historical["schemas"][0]["fields"][0]["type"] = "binary".into();
                    *data = serde_json::to_vec(&historical)?;
                }
                "cycle" => {
                    let (name, data) = source_objects
                        .iter_mut()
                        .find(|(name, _)| name.ends_with(".metadata.json"))
                        .unwrap();
                    let mut historical: serde_json::Value = serde_json::from_slice(data)?;
                    let timestamp = historical["last-updated-ms"].clone();
                    historical["metadata-log"] =
                        serde_json::json!([{"metadata-file":name,"timestamp-ms":timestamp}]);
                    *data = serde_json::to_vec(&historical)?;
                }
                "duplicate-schema" => {
                    let duplicate = metadata["schemas"][0].clone();
                    metadata["schemas"].as_array_mut().unwrap().push(duplicate);
                }
                "current-projection" => {
                    let mut changed = metadata["schemas"][0].clone();
                    changed["schema-id"] = 1.into();
                    changed["fields"][0]["name"] = "renamed_id".into();
                    metadata["schemas"].as_array_mut().unwrap().push(changed);
                    metadata["current-schema-id"] = 1.into();
                }
                "missing-sort-orders" => {
                    metadata.as_object_mut().unwrap().remove("sort-orders");
                }
                "foreign-reference" => {
                    metadata["snapshots"][0]["manifest-list"] =
                        "https://127.0.0.1/not-a-grant".into()
                }
                _ => anyhow::bail!("unknown snapshot variant"),
            }
            let metadata = put(
                destination.as_ref(),
                &serde_json::to_vec(&metadata)?,
                read.clone(),
                write.clone(),
            )
            .await?;
            let mut spool = ClosureSpool::create(
                &posix,
                object(&format!("snapshot-{}", Uuid::new_v4())),
                ClosureLimits {
                    max_entries: 4096,
                    max_referenced_bytes: 512 * 1024 * 1024,
                    max_spool_bytes: 16 * 1024 * 1024,
                },
                write.clone(),
                cancel.clone(),
            )
            .await?;
            let mut page = Vec::new();
            for (name, bytes) in source_objects {
                page.push(ClosureEntry {
                    name,
                    content: put(destination.as_ref(), &bytes, read.clone(), write.clone()).await?,
                });
                if page.len() == 256 {
                    spool.append_page(&page).await?;
                    page.clear();
                }
            }
            if !page.is_empty() {
                spool.append_page(&page).await?;
            }
            spool.seal(destination.as_ref(), read.clone()).await?;
            if variant == "version-hold" {
                let mut after = None;
                loop {
                    let entries = spool.read_page(after.as_deref(), read.as_ref()).await?;
                    ensure!(!entries.is_empty(), "No Parquet fixture");
                    if let Some(entry) = entries
                        .iter()
                        .find(|entry| entry.name.ends_with(".parquet"))
                    {
                        println!(
                            "{}",
                            serde_json::json!({"event":"snapshot-hashed","content":entry.content})
                        );
                        std::io::stdout().flush()?;
                        break;
                    }
                    after = Some(entries.last().unwrap().name.clone());
                }
                proceed();
            }
            let proof = verify_snapshot(
                &spool,
                destination,
                read.clone(),
                SnapshotPin {
                    metadata,
                    table_uuid: pin_uuid,
                    snapshot_id: pin_snapshot,
                },
            )
            .await;
            if variant == "valid" || variant == "deleted-valid" {
                let proof = proof?;
                ensure!(
                    proof.data_files == if variant == "valid" { 2 } else { 3 }
                        && proof.physical_rows == if variant == "valid" { 300 } else { 301 }
                        && proof.scanned_snapshots >= 2
                );
            } else {
                ensure!(
                    proof.is_err(),
                    "malformed snapshot must not produce a proof"
                );
                if variant == "version-hold" {
                    println!("{}", serde_json::json!({"event":"version-denied"}));
                }
            }
        }
        "exercise" => {
            let payload: Vec<u8> = (0..9 * 1024 * 1024 + 137)
                .map(|i| ((i * 37) % 251) as u8)
                .collect();
            let mut multipart = artifact_s3
                .begin_write(
                    object("s3-multipart"),
                    payload.len() as u64,
                    hash(&payload),
                    write.clone(),
                    cancel.clone(),
                )
                .await?;
            for chunk in payload.chunks(36_773) {
                multipart.append(chunk).await?;
            }
            ensure!(
                artifact_s3
                    .read_range(&object("s3-multipart"), 0..1, read.as_ref(), &cancel)
                    .await
                    .is_err()
            );
            let sealed = multipart.seal().await?;
            assert_recovery(
                Arc::new(artifact_s3.clone()),
                sealed.clone(),
                read.clone(),
                write.clone(),
                cancel.clone(),
            )
            .await?;
            ensure!(
                artifact_s3
                    .recover_write(
                        sealed.object.clone(),
                        sealed.size,
                        sealed.sha256.clone(),
                        read.clone(),
                        write.clone(),
                        cancel.clone()
                    )
                    .await?
                    == sealed
            );
            let mut duplicate = artifact_s3
                .begin_write(
                    sealed.object.clone(),
                    3,
                    hash(b"abc"),
                    write.clone(),
                    cancel.clone(),
                )
                .await?;
            duplicate.append(b"abc").await?;
            ensure!(duplicate.seal().await.is_err());
            artifact_s3
                .recover_write(
                    sealed.object,
                    sealed.size,
                    sealed.sha256,
                    read.clone(),
                    write.clone(),
                    cancel.clone(),
                )
                .await?;
            let mut wrong_hash = artifact_s3
                .begin_write(
                    object("s3-bad-hash"),
                    3,
                    hash(b"wrong"),
                    write.clone(),
                    cancel.clone(),
                )
                .await?;
            wrong_hash.append(b"abc").await?;
            ensure!(wrong_hash.seal().await.is_err());
            let empty = artifact_s3
                .begin_write(
                    object("s3-empty"),
                    0,
                    hash(b""),
                    write.clone(),
                    cancel.clone(),
                )
                .await?
                .seal()
                .await?;
            artifact_s3
                .recover_write(
                    empty.object,
                    0,
                    empty.sha256,
                    read.clone(),
                    write.clone(),
                    cancel.clone(),
                )
                .await?;
            let cancellation = Cancellation::default();
            let mut aborted = artifact_s3
                .begin_write(
                    object("s3-aborted"),
                    payload.len() as u64,
                    hash(&payload),
                    write.clone(),
                    cancellation.clone(),
                )
                .await?;
            for chunk in payload[..8 * 1024 * 1024].chunks(1024 * 1024) {
                aborted.append(chunk).await?;
            }
            cancellation.cancel();
            ensure!(aborted.append(b"x").await.is_err());
            aborted.abort().await?;
            ensure!(
                artifact_s3
                    .read_range(&object("s3-aborted"), 0..1, read.as_ref(), &cancel)
                    .await
                    .is_err()
            );
            let source_write = IndexArtifactWriteAuthority {
                scope: scope.clone(),
                namespace: source_namespace,
            };
            let underclassified = IndexArtifactWriteAuthority {
                scope: scope.clone(),
                namespace: IndexNamespace {
                    warehouse_id: "underclassified".into(),
                    table_id: "artifacttable".into(),
                    revision: fixture.revision.clone(),
                },
            };
            ensure!(
                underclassified
                    .check_write(&LogicalObject::new(
                        "underclassified",
                        "artifacttable",
                        "sensitive"
                    )?)
                    .await
                    .is_err(),
                "classified indexing artifacts must not enter an underclassified namespace"
            );
            ensure!(
                source_write.check_write(&source_object).await.is_err(),
                "source-read permission must never grant source writes"
            );
            ensure!(
                scope.check_read(&source_object).await.is_err(),
                "raw indexing read scope must not grant logical namespace access"
            );
            let wrong = LogicalObject::new("sourcewarehouse", "other", "payload")?;
            ensure!(
                format!(
                    "{}",
                    s3.read_range(&wrong, 0..1, source_read.as_ref(), &cancel)
                        .await
                        .unwrap_err()
                )
                .contains("BINDING_MISMATCH")
            );
            let stale = IndexNamespaceReadAuthority {
                scope: scope.clone(),
                namespace: IndexNamespace {
                    revision: "stale".into(),
                    ..source_read.namespace.clone()
                },
            };
            ensure!(
                s3.read_range(&source_object, 0..1, &stale, &cancel)
                    .await
                    .is_err()
            );
            let range = s3
                .read_range(
                    &source_object,
                    8191..8191 + 2 * 1024 * 1024,
                    source_read.as_ref(),
                    &cancel,
                )
                .await?;
            ensure!(
                range.len() == 2 * 1024 * 1024
                    && range
                        .iter()
                        .enumerate()
                        .all(|(i, b)| *b == (((i + 8191) * 37) % 251) as u8)
            );
            ensure!(
                s3.read_range(
                    &source_object,
                    0..16 * 1024 * 1024 + 1,
                    source_read.as_ref(),
                    &cancel
                )
                .await
                .is_err()
            );
            let mut small = posix
                .begin_write(
                    object("small"),
                    3,
                    hash(b"abc"),
                    write.clone(),
                    cancel.clone(),
                )
                .await?;
            small.append(b"abc").await?;
            let sealed = small.seal().await?;
            ensure!(sealed.size == 3 && sealed.sha256 == hash(b"abc"));
            assert_recovery(
                Arc::new(posix.clone()),
                sealed.clone(),
                read.clone(),
                write.clone(),
                cancel.clone(),
            )
            .await?;
            ensure!(
                posix
                    .read_range(&object("small"), 0..3, read.as_ref(), &cancel)
                    .await?
                    == &b"abc"[..]
            );
            let mut duplicate = posix
                .begin_write(
                    object("small"),
                    3,
                    hash(b"def"),
                    write.clone(),
                    cancel.clone(),
                )
                .await?;
            duplicate.append(b"def").await?;
            ensure!(duplicate.seal().await.is_err());
            ensure!(
                posix
                    .read_range(&object("small"), 0..3, read.as_ref(), &cancel)
                    .await?
                    == &b"abc"[..]
            );
            let mut bad = posix
                .begin_write(
                    object("bad-digest"),
                    3,
                    hash(b"def"),
                    write.clone(),
                    cancel.clone(),
                )
                .await?;
            bad.append(b"abc").await?;
            ensure!(bad.seal().await.is_err());
            ensure!(
                posix
                    .read_range(&object("bad-digest"), 0..3, read.as_ref(), &cancel)
                    .await
                    .is_err()
            );
            let mut short = posix
                .begin_write(
                    object("short"),
                    4,
                    hash(b"abcd"),
                    write.clone(),
                    cancel.clone(),
                )
                .await?;
            short.append(b"abc").await?;
            ensure!(short.seal().await.is_err());
            ensure!(
                posix
                    .read_range(&object("short"), 0..3, read.as_ref(), &cancel)
                    .await
                    .is_err()
            );
            let aborted = Cancellation::default();
            let mut incomplete = posix
                .begin_write(
                    object("cancelled"),
                    3,
                    hash(b"abc"),
                    write.clone(),
                    aborted.clone(),
                )
                .await?;
            incomplete.append(b"a").await?;
            aborted.cancel();
            ensure!(incomplete.append(b"bc").await.is_err());
            ensure!(incomplete.seal().await.is_err());
            ensure!(
                posix
                    .read_range(&object("cancelled"), 0..3, read.as_ref(), &cancel)
                    .await
                    .is_err()
            );
            posix
                .begin_write(object("empty"), 0, hash(b""), write.clone(), cancel.clone())
                .await?
                .seal()
                .await?;
            ensure!(
                posix
                    .read_range(&object("empty"), 0..0, read.as_ref(), &cancel)
                    .await?
                    .is_empty()
            );
            let objects = Path::new(&fixture.root).join("artifactwarehouse/artifacttable/objects");
            std::os::unix::fs::symlink(objects.join("small"), objects.join("symlink"))?;
            ensure!(
                posix
                    .recover_object(
                        object("symlink"),
                        3,
                        hash(b"abc"),
                        read.clone(),
                        write.clone(),
                        cancel.clone()
                    )
                    .await
                    .is_err()
            );
            ensure!(
                posix
                    .read_range(&object("symlink"), 0..3, read.as_ref(), &cancel)
                    .await
                    .is_err()
            );
            std::fs::hard_link(objects.join("small"), objects.join("hardlink"))?;
            ensure!(
                posix
                    .read_range(&object("hardlink"), 0..3, read.as_ref(), &cancel)
                    .await
                    .is_err()
            );
            std::fs::remove_file(objects.join("hardlink"))?;
            ensure!(LogicalObject::new("artifactwarehouse", "artifacttable", "../secret").is_err());
            let mut content_hash = Sha256::new();
            let block = vec![42; 1024 * 1024];
            for _ in 0..70 {
                content_hash.update(&block);
            }
            let expected = format!("sha256:{:x}", content_hash.finalize());
            let mut large = posix
                .begin_write(
                    object("large"),
                    70 * 1024 * 1024,
                    expected.clone(),
                    write.clone(),
                    cancel.clone(),
                )
                .await?;
            for _ in 0..70 {
                large.append(&block).await?;
            }
            large.seal().await?;
            let mut stream = posix
                .read(object("large"), read.clone(), cancel.clone())
                .await?;
            let mut actual = Sha256::new();
            let mut count = 0;
            while let Some(frame) = stream.next().await {
                let frame = frame?;
                ensure!(frame.len() <= 1024 * 1024);
                count += frame.len();
                actual.update(frame);
            }
            ensure!(
                count == 70 * 1024 * 1024 && format!("sha256:{:x}", actual.finalize()) == expected
            );
            let mut spool = ClosureSpool::create(
                &posix,
                object("closure"),
                ClosureLimits {
                    max_entries: 2048,
                    max_referenced_bytes: 100_000,
                    max_spool_bytes: 8 * 1024 * 1024,
                },
                write.clone(),
                cancel.clone(),
            )
            .await?;
            for start in [768, 512, 256, 0] {
                spool.append_page(&entries(start, start + 256)).await?;
            }
            ensure!(
                spool.append_page(&entries(0, 1)).await.is_err(),
                "duplicates across pages must not advance closure state"
            );
            let receipt = spool.seal(&posix, read.clone()).await?;
            ensure!(receipt.entries == 1024 && receipt.referenced_bytes == 3072);
            ensure!(spool.seal(&posix, read.clone()).await? == receipt);
            ensure!(spool.append_page(&entries(1024, 1025)).await.is_err());
            let mut after: Option<String> = None;
            let mut total = 0;
            loop {
                let page = spool.read_page(after.as_deref(), read.as_ref()).await?;
                ensure!(page.len() <= 256);
                if page.is_empty() {
                    break;
                }
                total += page.len();
                after = Some(page.last().unwrap().name.clone());
            }
            ensure!(total == 1024);
            let mut bad_spool = ClosureSpool::create(
                &posix,
                object("bad-closure"),
                ClosureLimits {
                    max_entries: 2,
                    max_referenced_bytes: 100,
                    max_spool_bytes: 1024 * 1024,
                },
                write.clone(),
                cancel.clone(),
            )
            .await?;
            let mut invalid = entries(0, 1);
            invalid[0].content.sha256 = hash(b"def");
            bad_spool.append_page(&invalid).await?;
            ensure!(bad_spool.seal(&posix, read.clone()).await.is_err());
            ensure!(bad_spool.read_page(None, read.as_ref()).await.is_err());
            let mut bounded = ClosureSpool::create(
                &posix,
                object("bounded-closure"),
                ClosureLimits {
                    max_entries: 2,
                    max_referenced_bytes: 100,
                    max_spool_bytes: 1024 * 1024,
                },
                write.clone(),
                cancel.clone(),
            )
            .await?;
            ensure!(bounded.append_page(&entries(0, 3)).await.is_err());
            bounded.append_page(&entries(0, 2)).await?;
            ensure!(bounded.seal(&posix, read.clone()).await?.entries == 2);
        }
        "crash" => {
            let mut spool = ClosureSpool::create(
                &posix,
                object("crash-closure"),
                ClosureLimits {
                    max_entries: 2048,
                    max_referenced_bytes: 100_000,
                    max_spool_bytes: 8 * 1024 * 1024,
                },
                write.clone(),
                cancel.clone(),
            )
            .await?;
            spool.append_page(&entries(0, 256)).await?;
            event("spool-committed", spool.id());
            std::future::pending::<()>().await;
        }
        "resume" => {
            let mut spool = ClosureSpool::reopen(
                &posix,
                fixture.spool_id.unwrap(),
                object("crash-closure"),
                write.clone(),
                cancel.clone(),
            )
            .await?;
            ensure!(spool.append_page(&entries(0, 1)).await.is_err());
            spool.append_page(&entries(256, 512)).await?;
            let receipt = spool.seal(&posix, read.clone()).await?;
            ensure!(receipt.entries == 512 && receipt.referenced_bytes == 1536);
        }
        "provider-change" => {
            let mut stream = s3.read(source_object, source_read, cancel).await?;
            ensure!(!stream.next().await.unwrap()?.is_empty());
            event("provider-read-started", fixture.build);
            proceed();
            ensure!(
                stream.next().await.unwrap().is_err(),
                "changing a pinned provider version must stop subsequent release"
            );
        }
        "read-revoke" => {
            let mut stream = posix.read(object("large"), read, cancel).await?;
            ensure!(!stream.next().await.unwrap()?.is_empty());
            event("posix-read-started", fixture.build);
            proceed();
            ensure!(
                stream.next().await.unwrap().is_err(),
                "revocation must deny the next emitted batch"
            );
        }
        "recover-revoke" | "recover-revoke-s3" => {
            let is_s3 = fixture.mode == "recover-revoke-s3";
            let (store, name, value, count, pause_at): (
                Arc<dyn ObjectStore>,
                &str,
                u8,
                usize,
                usize,
            ) = if is_s3 {
                (Arc::new(artifact_s3.clone()), "s3-uncertain", 7, 8, 4)
            } else {
                (Arc::new(posix.clone()), "large", 42, 70, 3)
            };
            let block = vec![value; 1024 * 1024];
            let mut digest = Sha256::new();
            for _ in 0..count {
                digest.update(&block);
            }
            let write = Arc::new(RecoveryPause {
                write,
                checks: std::sync::atomic::AtomicUsize::new(0),
                pause_at,
                build: fixture.build,
            });
            ensure!(
                store
                    .recover_object(
                        object(name),
                        (count * 1024 * 1024) as u64,
                        format!("sha256:{:x}", digest.finalize()),
                        read,
                        write,
                        cancel
                    )
                    .await
                    .is_err(),
                "revocation must stop recovery before any sealed result"
            );
        }
        "write-revoke" => {
            let mut pending = posix
                .begin_write(object("revoked-write"), 3, hash(b"abc"), write, cancel)
                .await?;
            pending.append(b"a").await?;
            event("posix-write-started", fixture.build);
            proceed();
            ensure!(pending.append(b"bc").await.is_err());
            ensure!(pending.seal().await.is_err());
            ensure!(
                !Path::new(&fixture.root)
                    .join("artifactwarehouse/artifacttable/objects/revoked-write")
                    .exists()
            );
        }
        "s3-write-revoke" => {
            let mut pending = artifact_s3
                .begin_write(object("s3-revoked-write"), 3, hash(b"abc"), write, cancel)
                .await?;
            pending.append(b"a").await?;
            event("s3-write-started", fixture.build);
            proceed();
            ensure!(pending.append(b"bc").await.is_err());
            pending.abort().await?;
        }
        "s3-uncertain" => {
            let block = vec![7u8; 1024 * 1024];
            let mut digest = Sha256::new();
            for _ in 0..8 {
                digest.update(&block);
            }
            let mut pending = artifact_s3
                .begin_write(
                    object("s3-uncertain"),
                    8 * 1024 * 1024,
                    format!("sha256:{:x}", digest.finalize()),
                    write,
                    cancel,
                )
                .await?;
            for _ in 0..8 {
                pending.append(&block).await?;
            }
            event("s3-before-complete", fixture.build);
            proceed();
            let _ = pending.seal().await?;
        }
        "s3-recover" => {
            let block = vec![7u8; 1024 * 1024];
            let mut digest = Sha256::new();
            for _ in 0..8 {
                digest.update(&block);
            }
            artifact_s3
                .recover_write(
                    object("s3-uncertain"),
                    8 * 1024 * 1024,
                    format!("sha256:{:x}", digest.finalize()),
                    read,
                    write,
                    cancel,
                )
                .await?;
        }
        "denied" => {
            ensure!(
                s3.read_range(&source_object, 0..1, source_read.as_ref(), &cancel)
                    .await
                    .is_err()
            );
            ensure!(
                posix
                    .read_range(&object("small"), 0..3, read.as_ref(), &cancel)
                    .await
                    .is_err()
            );
            ensure!(
                posix
                    .begin_write(object("forbidden"), 3, hash(b"abc"), write, cancel)
                    .await
                    .is_err()
            );
        }
        "takeover" => {
            scope.check().await?;
            ensure!(
                ClosureSpool::reopen(
                    &posix,
                    fixture.spool_id.unwrap(),
                    object("crash-closure"),
                    write,
                    cancel
                )
                .await
                .is_err(),
                "a replacement claim must not share mutable prior-attempt scratch"
            );
        }
        _ => anyhow::bail!("unknown fixture mode"),
    }
    println!(
        "{}",
        serde_json::json!({"event":"verified","mode":fixture.mode})
    );
    Ok(())
}
include!("../native-common/artifact.rs");
