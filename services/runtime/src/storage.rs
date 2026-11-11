use anyhow::{Result, bail, ensure};
use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, SignableBody, SignableRequest, SigningSettings, sign,
};
use aws_sigv4::sign::v4::SigningParams;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use rusqlite::OptionalExtension;
use rustix::fs::{
    AtFlags, CWD, Mode, OFlags, RenameFlags, ResolveFlags, mkdirat, openat2, renameat_with,
    unlinkat,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fmt::Debug,
    fs::File,
    ops::Range,
    os::fd::AsRawFd,
    os::unix::fs::{FileExt, MetadataExt, PermissionsExt},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::{io::AsyncWriteExt, sync::Notify};
use uuid::Uuid;

const CHUNK_MAX: usize = 1024 * 1024;
const RANGE_MAX: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogicalObject {
    pub version: u32,
    pub warehouse_id: String,
    pub table_id: String,
    pub object_id: String,
}

fn identity(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value.as_bytes()[0].is_ascii_alphanumeric()
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
        "INVALID_INPUT"
    );
    Ok(())
}

impl LogicalObject {
    pub fn new(warehouse: &str, table: &str, object: &str) -> Result<Self> {
        for value in [warehouse, table, object] {
            identity(value)?;
        }
        Ok(Self {
            version: 1,
            warehouse_id: warehouse.into(),
            table_id: table.into(),
            object_id: object.into(),
        })
    }
    fn validate(&self) -> Result<()> {
        ensure!(self.version == 1, "INVALID_INPUT");
        for value in [&self.warehouse_id, &self.table_id, &self.object_id] {
            identity(value)?;
        }
        Ok(())
    }
}

#[async_trait]
pub trait ReadAuthority: Debug + Send + Sync {
    async fn check_read(&self, object: &LogicalObject) -> Result<()>;
}
/// Narrow discovery never authorizes a prefix-wide cleanup or removal.
#[async_trait]
pub trait DiscoveryAuthority: Debug + Send + Sync {
    async fn check_discover(&self, object: &LogicalObject) -> Result<()>;
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MultipartPage {
    pub upload_ids: Vec<String>,
    pub next_upload_id: Option<String>,
}
/// Reconciliation may abort only one exact journaled multipart identity under its own fenced authority.
#[async_trait]
pub trait AbortAuthority: Debug + Send + Sync {
    async fn check_abort(&self, object: &LogicalObject, upload_id: &str) -> Result<()>;
}
/// Physical reclamation is separate from immutable object creation and requires an exact version.
#[async_trait]
pub trait RemoveAuthority: Debug + Send + Sync {
    async fn check_remove(&self, object: &LogicalObject, version: &ObjectVersion) -> Result<()>;
}

#[async_trait]
pub trait ObjectWrite: Send {
    async fn append(&mut self, bytes: &[u8]) -> Result<()>;
    async fn seal(self: Box<Self>) -> Result<SealedObject>;
    async fn abort(self: Box<Self>) -> Result<()>;
}
#[async_trait]
pub trait ObjectWriter: Debug + Send + Sync {
    async fn discover_multipart(
        &self,
        _object: LogicalObject,
        _after: Option<String>,
        _authority: Arc<dyn DiscoveryAuthority>,
        _cancel: Cancellation,
    ) -> Result<MultipartPage> {
        bail!("MULTIPART_DISCOVERY_UNSUPPORTED")
    }
    async fn abort_multipart(
        &self,
        _object: LogicalObject,
        _upload_id: String,
        _authority: Arc<dyn AbortAuthority>,
        _cancel: Cancellation,
    ) -> Result<bool> {
        bail!("MULTIPART_ABORT_UNSUPPORTED")
    }
    async fn remove_object(
        &self,
        object: LogicalObject,
        expected: ObjectVersion,
        authority: Arc<dyn RemoveAuthority>,
        cancel: Cancellation,
    ) -> Result<bool>;
    async fn begin_object(
        &self,
        object: LogicalObject,
        expected_size: u64,
        expected_sha256: String,
        authority: Arc<dyn WriteAuthority>,
        cancel: Cancellation,
    ) -> Result<Box<dyn ObjectWrite>>;
    async fn recover_object(
        &self,
        object: LogicalObject,
        expected_size: u64,
        expected_sha256: String,
        read: Arc<dyn ReadAuthority>,
        write: Arc<dyn WriteAuthority>,
        cancel: Cancellation,
    ) -> Result<Option<SealedObject>>;
}
pub trait ObjectStore: ObjectReader + ObjectWriter {}
impl<T: ObjectReader + ObjectWriter> ObjectStore for T {}

#[async_trait]
impl ObjectWrite for PrivateWrite {
    async fn append(&mut self, bytes: &[u8]) -> Result<()> {
        PrivateWrite::append(self, bytes).await
    }
    async fn seal(self: Box<Self>) -> Result<SealedObject> {
        PrivateWrite::seal(*self).await
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        drop(self);
        Ok(())
    }
}
#[async_trait]
impl ObjectWrite for PrivateS3Write {
    async fn append(&mut self, bytes: &[u8]) -> Result<()> {
        PrivateS3Write::append(self, bytes).await
    }
    async fn seal(self: Box<Self>) -> Result<SealedObject> {
        PrivateS3Write::seal(*self).await
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        PrivateS3Write::abort(*self).await
    }
}
#[async_trait]
impl ObjectWriter for PosixStore {
    async fn remove_object(
        &self,
        object: LogicalObject,
        expected: ObjectVersion,
        authority: Arc<dyn RemoveAuthority>,
        cancel: Cancellation,
    ) -> Result<bool> {
        self.scope(&object)?;
        cancel.check()?;
        authority.check_remove(&object, &expected).await?;
        bail!("CONDITIONAL_DELETE_UNSUPPORTED")
    }
    async fn begin_object(
        &self,
        object: LogicalObject,
        size: u64,
        sha: String,
        authority: Arc<dyn WriteAuthority>,
        cancel: Cancellation,
    ) -> Result<Box<dyn ObjectWrite>> {
        Ok(Box::new(
            self.begin_write(object, size, sha, authority, cancel)
                .await?,
        ))
    }
    async fn recover_object(
        &self,
        object: LogicalObject,
        size: u64,
        sha: String,
        read: Arc<dyn ReadAuthority>,
        write: Arc<dyn WriteAuthority>,
        cancel: Cancellation,
    ) -> Result<Option<SealedObject>> {
        digest(&sha)?;
        ensure!(size <= self.limits.max_object_bytes, "LIMIT_EXCEEDED");
        self.scope(&object)?;
        check_write(write.as_ref(), &object, &cancel).await?;
        let version = match self.version(&object, read.as_ref(), &cancel).await {
            Ok(version) => version,
            Err(error)
                if error.downcast_ref::<rustix::io::Errno>() == Some(&rustix::io::Errno::NOENT) =>
            {
                check_read(read.as_ref(), &object, &cancel).await?;
                check_write(write.as_ref(), &object, &cancel).await?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        Ok(Some(
            recover_version(
                self,
                SealedObject {
                    object,
                    size,
                    sha256: sha,
                },
                version,
                read,
                write,
                cancel,
            )
            .await?,
        ))
    }
}
#[async_trait]
impl ObjectWriter for S3ReadStore {
    async fn discover_multipart(
        &self,
        object: LogicalObject,
        after: Option<String>,
        authority: Arc<dyn DiscoveryAuthority>,
        cancel: Cancellation,
    ) -> Result<MultipartPage> {
        let _ = self.url(&object)?;
        let key = [
            self.binding.prefix.as_str(),
            object.warehouse_id.as_str(),
            object.table_id.as_str(),
            "objects",
            object.object_id.as_str(),
        ]
        .into_iter()
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join("/");
        let mut url = reqwest::Url::parse(&self.binding.endpoint)?;
        url.set_path(&format!("/{}", self.binding.bucket));
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("uploads", "")
                .append_pair("prefix", &key)
                .append_pair("max-uploads", "16");
            if let Some(marker) = &after {
                ensure!(
                    !marker.is_empty()
                        && marker.len() <= 2048
                        && marker.bytes().all(|b| b.is_ascii_graphic()),
                    "STORAGE_PROTOCOL_ERROR"
                );
                query
                    .append_pair("key-marker", &key)
                    .append_pair("upload-id-marker", marker);
            }
        }
        let check = || async {
            cancel.check()?;
            tokio::select! {
                _=cancel.cancelled()=>bail!("CANCELLED"),
                result=tokio::time::timeout(Duration::from_secs(30),authority.check_discover(&object))=>result.map_err(|_|anyhow::anyhow!("AUTHORITY_TIMEOUT"))??,
            }
            cancel.check()
        };
        check().await?;
        let response = self
            .request_body("GET", url, &[], Bytes::new(), &cancel)
            .await?;
        ensure!(response.status().is_success(), "STORAGE_PROVIDER_ERROR");
        let body = provider_body(response, &cancel).await?;
        let page = multipart_page(&body, &key, after.as_deref())?;
        check().await?;
        Ok(page)
    }

    async fn abort_multipart(
        &self,
        object: LogicalObject,
        upload_id: String,
        authority: Arc<dyn AbortAuthority>,
        cancel: Cancellation,
    ) -> Result<bool> {
        ensure!(
            !upload_id.is_empty()
                && upload_id.len() <= 4096
                && upload_id.bytes().all(|b| b.is_ascii_graphic()),
            "STORAGE_PROTOCOL_ERROR"
        );
        let mut url = self.url(&object)?;
        url.query_pairs_mut().append_pair("uploadId", &upload_id);
        let check = || async {
            cancel.check()?;
            tokio::select! {
                _=cancel.cancelled()=>bail!("CANCELLED"),
                result=tokio::time::timeout(Duration::from_secs(30),authority.check_abort(&object,&upload_id))=>result.map_err(|_|anyhow::anyhow!("AUTHORITY_TIMEOUT"))??,
            }
            cancel.check()
        };
        check().await?;
        let response = self
            .request_body("DELETE", url, &[], Bytes::new(), &cancel)
            .await?;
        let present = response.status() != reqwest::StatusCode::NOT_FOUND;
        ensure!(
            response.status().is_success() || !present,
            "STORAGE_PROVIDER_ERROR"
        );
        let body = provider_body(response, &cancel).await?;
        ensure!(
            present || provider_xml(&body, "Error", "Code")? == "NoSuchUpload",
            "STORAGE_PROVIDER_ERROR"
        );
        check().await?;
        Ok(present)
    }

    async fn remove_object(
        &self,
        object: LogicalObject,
        expected: ObjectVersion,
        authority: Arc<dyn RemoveAuthority>,
        cancel: Cancellation,
    ) -> Result<bool> {
        self.remove_version(object, expected, authority, cancel)
            .await
    }
    async fn begin_object(
        &self,
        object: LogicalObject,
        size: u64,
        sha: String,
        authority: Arc<dyn WriteAuthority>,
        cancel: Cancellation,
    ) -> Result<Box<dyn ObjectWrite>> {
        Ok(Box::new(
            self.begin_write(object, size, sha, authority, cancel)
                .await?,
        ))
    }
    async fn recover_object(
        &self,
        object: LogicalObject,
        size: u64,
        sha: String,
        read: Arc<dyn ReadAuthority>,
        write: Arc<dyn WriteAuthority>,
        cancel: Cancellation,
    ) -> Result<Option<SealedObject>> {
        digest(&sha)?;
        ensure!(
            size <= self.binding.limits.max_object_bytes,
            "LIMIT_EXCEEDED"
        );
        self.url(&object)?;
        check_read(read.as_ref(), &object, &cancel).await?;
        check_write(write.as_ref(), &object, &cancel).await?;
        let head = self.request("HEAD", &object, &[], &cancel).await?;
        check_read(read.as_ref(), &object, &cancel).await?;
        check_write(write.as_ref(), &object, &cancel).await?;
        if head.status() == reqwest::StatusCode::NOT_FOUND {
            // HEAD Object does not distinguish a missing key from a missing bucket.
            let mut bucket = self.url(&object)?;
            bucket.set_path(&format!("/{}", self.binding.bucket));
            check_read(read.as_ref(), &object, &cancel).await?;
            check_write(write.as_ref(), &object, &cancel).await?;
            let present = self
                .request_body("HEAD", bucket, &[], Bytes::new(), &cancel)
                .await?;
            check_read(read.as_ref(), &object, &cancel).await?;
            check_write(write.as_ref(), &object, &cancel).await?;
            ensure!(present.status().is_success(), "STORAGE_PROVIDER_ERROR");
            return Ok(None);
        }
        let version = self.head_version(&head)?;
        Ok(Some(
            recover_version(
                self,
                SealedObject {
                    object,
                    size,
                    sha256: sha,
                },
                version,
                read,
                write,
                cancel,
            )
            .await?,
        ))
    }
}

async fn recover_version<R: ObjectReader + ?Sized>(
    store: &R,
    expected: SealedObject,
    version: ObjectVersion,
    read: Arc<dyn ReadAuthority>,
    write: Arc<dyn WriteAuthority>,
    cancel: Cancellation,
) -> Result<SealedObject> {
    let object = &expected.object;
    let expected_size = expected.size;
    ensure!(version.size == expected_size, "CONTENT_MISMATCH");
    let chunk = store.chunk_bytes();
    ensure!((1..=CHUNK_MAX).contains(&chunk), "INVALID_BINDING");
    let mut offset = 0u64;
    let mut hash = Sha256::new();
    while offset < expected_size {
        let end = offset.saturating_add(chunk as u64).min(expected_size);
        check_write(write.as_ref(), object, &cancel).await?;
        let bytes = store
            .range_at_version(object, offset..end, &version, read.as_ref(), &cancel)
            .await?;
        check_write(write.as_ref(), object, &cancel).await?;
        ensure!(bytes.len() as u64 == end - offset, "CONTENT_MISMATCH");
        hash.update(bytes);
        offset = end;
    }
    check_write(write.as_ref(), object, &cancel).await?;
    ensure!(
        store.version(object, read.as_ref(), &cancel).await? == version,
        "STORAGE_VERSION_CHANGED"
    );
    check_write(write.as_ref(), object, &cancel).await?;
    ensure!(
        format!("sha256:{:x}", hash.finalize()) == expected.sha256,
        "CONTENT_MISMATCH"
    );
    Ok(expected)
}

#[async_trait]
pub trait WriteAuthority: Debug + Send + Sync {
    fn scope_key(&self) -> String;
    async fn check_write(&self, object: &LogicalObject) -> Result<()>;
    /// Persist an exact object-key attempt before initiating multipart provider effects.
    async fn prepare_multipart(&self, object: &LogicalObject, _physical: &str) -> Result<()> {
        self.check_write(object).await
    }
    /// Journal a returned provider upload identity before parts or completion; the default grants no GC ownership.
    async fn record_multipart(
        &self,
        object: &LogicalObject,
        _upload_id: &str,
        _physical: &str,
    ) -> Result<()> {
        self.check_write(object).await
    }
    /// Admit an exact immutable output before any provider effect. Untracked authorities remain ineligible for GC.
    async fn admit_write(
        &self,
        object: &LogicalObject,
        _size: u64,
        _sha256: &str,
        _physical: &str,
    ) -> Result<()> {
        self.check_write(object).await
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IndexNamespace {
    pub warehouse_id: String,
    pub table_id: String,
    pub revision: String,
}

impl IndexNamespace {
    fn validate(&self, object: &LogicalObject) -> Result<()> {
        object.validate()?;
        identity(&self.warehouse_id)?;
        identity(&self.table_id)?;
        identity(&self.revision)?;
        ensure!(
            object.warehouse_id == self.warehouse_id && object.table_id == self.table_id,
            "BINDING_MISMATCH"
        );
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct IndexNamespaceReadAuthority {
    pub scope: crate::io::Scope,
    pub namespace: IndexNamespace,
}

#[async_trait]
impl ReadAuthority for IndexNamespaceReadAuthority {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        self.namespace.validate(object)?;
        self.scope
            .db
            .get()
            .await?
            .query_one(
                "SELECT moria.native_storage_check($1,$2,$3,$4,$5,$6,$7,false)",
                &[
                    &self.scope.build,
                    &self.scope.task,
                    &self.scope.owner,
                    &self.scope.fence,
                    &self.namespace.warehouse_id,
                    &self.namespace.table_id,
                    &self.namespace.revision,
                ],
            )
            .await?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct IndexArtifactWriteAuthority {
    pub scope: crate::io::Scope,
    pub namespace: IndexNamespace,
}

#[async_trait]
impl WriteAuthority for IndexArtifactWriteAuthority {
    fn scope_key(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}/{}/{}",
            self.scope.build,
            self.scope.task.unwrap_or(self.scope.build),
            self.scope.owner,
            self.scope.fence,
            self.namespace.warehouse_id,
            self.namespace.table_id,
            self.namespace.revision
        )
    }
    async fn check_write(&self, object: &LogicalObject) -> Result<()> {
        self.namespace.validate(object)?;
        self.scope
            .db
            .get()
            .await?
            .query_one(
                "SELECT moria.native_storage_check($1,$2,$3,$4,$5,$6,$7,true)",
                &[
                    &self.scope.build,
                    &self.scope.task,
                    &self.scope.owner,
                    &self.scope.fence,
                    &self.namespace.warehouse_id,
                    &self.namespace.table_id,
                    &self.namespace.revision,
                ],
            )
            .await?;
        Ok(())
    }
}

#[derive(Clone, Default, Debug)]
pub struct Cancellation(Arc<CancelState>);

#[derive(Default, Debug)]
struct CancelState {
    cancelled: AtomicBool,
    notify: Notify,
}

impl Cancellation {
    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::SeqCst);
        self.0.notify.notify_waiters();
    }
    pub fn check(&self) -> Result<()> {
        ensure!(!self.0.cancelled.load(Ordering::SeqCst), "CANCELLED");
        Ok(())
    }
    async fn cancelled(&self) {
        loop {
            let notified = self.0.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.0.cancelled.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

async fn check_read(
    authority: &dyn ReadAuthority,
    object: &LogicalObject,
    cancel: &Cancellation,
) -> Result<()> {
    cancel.check()?;
    tokio::select! {
        result = tokio::time::timeout(Duration::from_secs(30), authority.check_read(object)) => result??,
        _ = cancel.cancelled() => bail!("CANCELLED"),
    }
    cancel.check()
}

async fn check_write(
    authority: &dyn WriteAuthority,
    object: &LogicalObject,
    cancel: &Cancellation,
) -> Result<()> {
    cancel.check()?;
    tokio::select! {
        result = tokio::time::timeout(Duration::from_secs(30), authority.check_write(object)) => result??,
        _ = cancel.cancelled() => bail!("CANCELLED"),
    }
    cancel.check()
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StorageLimits {
    pub max_object_bytes: u64,
    pub chunk_bytes: usize,
}

impl StorageLimits {
    fn validate(self) -> Result<()> {
        ensure!(
            self.max_object_bytes <= 9_007_199_254_740_991
                && (1..=CHUNK_MAX).contains(&self.chunk_bytes),
            "INVALID_INPUT"
        );
        Ok(())
    }
}

fn directory(parent: &File, name: &str) -> Result<Arc<File>> {
    identity(name)?;
    match mkdirat(parent, name, Mode::from_raw_mode(0o700)) {
        Ok(()) => parent.sync_all()?,
        Err(error) if error == rustix::io::Errno::EXIST => {}
        Err(error) => return Err(error.into()),
    }
    Ok(Arc::new(File::from(openat2(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
        ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
    )?)))
}

fn regular(parent: &File, name: &str) -> Result<File> {
    identity(name)?;
    let file = File::from(openat2(
        parent,
        name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
        ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
    )?);
    let metadata = file.metadata()?;
    ensure!(metadata.is_file() && metadata.nlink() == 1, "UNSAFE_PATH");
    Ok(file)
}

#[derive(Clone, Debug)]
pub struct PosixStore {
    warehouse: String,
    table: String,
    objects: Arc<File>,
    staging: Arc<File>,
    limits: StorageLimits,
    physical_fingerprint: String,
}

impl PosixStore {
    pub fn provision(
        root: &Path,
        warehouse: &str,
        table: &str,
        limits: StorageLimits,
    ) -> Result<Self> {
        limits.validate()?;
        identity(warehouse)?;
        identity(table)?;
        ensure!(root.is_absolute(), "INVALID_INPUT");
        let physical_fingerprint = crate::canonical::fingerprint(
            &serde_json::json!({"backend":"local","root":std::fs::canonicalize(root)?.to_str().ok_or_else(||anyhow::anyhow!("INVALID_BINDING"))?}),
        )?;
        let root = File::from(openat2(
            CWD,
            root,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        )?);
        let warehouse_dir = directory(&root, warehouse)?;
        let table_dir = directory(&warehouse_dir, table)?;
        Ok(Self {
            warehouse: warehouse.into(),
            table: table.into(),
            objects: directory(&table_dir, "objects")?,
            staging: directory(&table_dir, "staging")?,
            limits,
            physical_fingerprint,
        })
    }

    fn scope(&self, object: &LogicalObject) -> Result<()> {
        object.validate()?;
        ensure!(
            object.warehouse_id == self.warehouse && object.table_id == self.table,
            "BINDING_MISMATCH"
        );
        Ok(())
    }

    pub async fn read_range(
        &self,
        object: &LogicalObject,
        range: Range<u64>,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Bytes> {
        self.scope(object)?;
        check_read(authority, object, cancel).await?;
        let file = Arc::new(regular(&self.objects, &object.object_id)?);
        ensure!(
            file.metadata()?.len() <= self.limits.max_object_bytes,
            "LIMIT_EXCEEDED"
        );
        read_posix_range(
            file,
            range,
            self.limits.chunk_bytes,
            authority,
            object,
            cancel,
        )
        .await
    }

    pub async fn read(
        &self,
        object: LogicalObject,
        authority: Arc<dyn ReadAuthority>,
        cancel: Cancellation,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        self.scope(&object)?;
        check_read(authority.as_ref(), &object, &cancel).await?;
        let file = Arc::new(regular(&self.objects, &object.object_id)?);
        let size = file.metadata()?.len();
        ensure!(size <= self.limits.max_object_bytes, "LIMIT_EXCEEDED");
        let chunk_bytes = self.limits.chunk_bytes;
        Ok(futures::stream::try_unfold(
            (0, file, object, authority, cancel),
            move |(offset, file, object, authority, cancel)| async move {
                if offset == size {
                    check_read(authority.as_ref(), &object, &cancel).await?;
                    return Ok(None);
                }
                let end = size.min(offset + chunk_bytes as u64);
                let bytes = read_posix_range(
                    file.clone(),
                    offset..end,
                    chunk_bytes,
                    authority.as_ref(),
                    &object,
                    &cancel,
                )
                .await?;
                Ok(Some((bytes, (end, file, object, authority, cancel))))
            },
        )
        .boxed())
    }

    pub async fn begin_write(
        &self,
        object: LogicalObject,
        expected_size: u64,
        expected_sha256: String,
        authority: Arc<dyn WriteAuthority>,
        cancel: Cancellation,
    ) -> Result<PrivateWrite> {
        self.scope(&object)?;
        digest(&expected_sha256)?;
        ensure!(
            expected_size <= self.limits.max_object_bytes,
            "LIMIT_EXCEEDED"
        );
        cancel.check()?;
        let physical = self.physical_fingerprint()?;
        tokio::select! {
            _ = cancel.cancelled() => return Err(anyhow::anyhow!("CANCELLED")),
            result = tokio::time::timeout(std::time::Duration::from_secs(30), authority.admit_write(&object, expected_size, &expected_sha256, &physical)) => result.map_err(|_|anyhow::anyhow!("AUTHORITY_TIMEOUT"))??,
        }
        cancel.check()?;
        let name = Uuid::new_v4().to_string();
        let file = File::from(openat2(
            self.staging.as_ref(),
            &name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o600),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        )?);
        Ok(PrivateWrite {
            store: self.clone(),
            object,
            name,
            file: Some(tokio::fs::File::from_std(file)),
            expected_size,
            expected_sha256,
            size: 0,
            hash: Sha256::new(),
            authority,
            cancel,
            poisoned: false,
        })
    }
}

pub(crate) async fn read_posix_range(
    file: Arc<File>,
    range: Range<u64>,
    chunk_bytes: usize,
    authority: &dyn ReadAuthority,
    object: &LogicalObject,
    cancel: &Cancellation,
) -> Result<Bytes> {
    let length = range
        .end
        .checked_sub(range.start)
        .filter(|n| *n <= RANGE_MAX)
        .ok_or_else(|| anyhow::anyhow!("INVALID_INPUT"))?;
    ensure!(
        (1..=CHUNK_MAX).contains(&chunk_bytes) && range.end <= file.metadata()?.len(),
        "INVALID_INPUT"
    );
    check_read(authority, object, cancel).await?;
    let mut result = vec![0; length as usize];
    let mut offset = range.start;
    while offset < range.end {
        if offset != range.start {
            check_read(authority, object, cancel).await?;
        }
        let count = (range.end - offset).min(chunk_bytes as u64) as usize;
        let start = (offset - range.start) as usize;
        let fd = Arc::clone(&file);
        let read = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<u8>> {
            fd.read_exact_at(&mut result[start..start + count], offset)?;
            Ok(result)
        });
        result = tokio::select! { result = read => result??, _ = cancel.cancelled() => bail!("CANCELLED") };
        check_read(authority, object, cancel).await?;
        offset += count as u64;
    }
    Ok(result.into())
}

fn digest(value: &str) -> Result<()> {
    ensure!(
        value.len() == 71
            && value.starts_with("sha256:")
            && value.as_bytes()[7..]
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b)),
        "INVALID_INPUT"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SealedObject {
    pub object: LogicalObject,
    pub size: u64,
    pub sha256: String,
}

pub struct PrivateWrite {
    store: PosixStore,
    object: LogicalObject,
    name: String,
    file: Option<tokio::fs::File>,
    expected_size: u64,
    expected_sha256: String,
    size: u64,
    hash: Sha256,
    authority: Arc<dyn WriteAuthority>,
    cancel: Cancellation,
    poisoned: bool,
}

impl Drop for PrivateWrite {
    fn drop(&mut self) {
        self.file.take();
        let _ = unlinkat(self.store.staging.as_ref(), &self.name, AtFlags::empty());
    }
}

impl PrivateWrite {
    pub async fn append(&mut self, bytes: &[u8]) -> Result<()> {
        ensure!(!self.poisoned, "WRITE_INACTIVE");
        let result = async {
            ensure!(bytes.len() <= self.store.limits.chunk_bytes && bytes.len() as u64 <= self.expected_size - self.size, "LIMIT_EXCEEDED");
            check_write(self.authority.as_ref(), &self.object, &self.cancel).await?;
            tokio::select! {
                result = self.file.as_mut().ok_or_else(|| anyhow::anyhow!("WRITE_INACTIVE"))?.write_all(bytes) => result?,
                _ = self.cancel.cancelled() => bail!("CANCELLED"),
            }
            check_write(self.authority.as_ref(), &self.object, &self.cancel).await?;
            self.hash.update(bytes);
            self.size += bytes.len() as u64;
            Ok(())
        }.await;
        self.poisoned = result.is_err();
        result
    }

    pub async fn seal(mut self) -> Result<SealedObject> {
        ensure!(
            !self.poisoned && self.size == self.expected_size,
            "CONTENT_MISMATCH"
        );
        ensure!(
            format!("sha256:{:x}", self.hash.clone().finalize()) == self.expected_sha256,
            "CONTENT_MISMATCH"
        );
        check_write(self.authority.as_ref(), &self.object, &self.cancel).await?;
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("WRITE_INACTIVE"))?;
        file.flush().await?;
        file.set_permissions(std::fs::Permissions::from_mode(0o400))
            .await?;
        file.sync_all().await?;
        self.file.take();
        check_write(self.authority.as_ref(), &self.object, &self.cancel).await?;
        renameat_with(
            self.store.staging.as_ref(),
            &self.name,
            self.store.objects.as_ref(),
            &self.object.object_id,
            RenameFlags::NOREPLACE,
        )?;
        let result = async {
            self.store.objects.sync_all()?;
            self.store.staging.sync_all()?;
            check_write(self.authority.as_ref(), &self.object, &self.cancel).await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        result.map_err(|_| anyhow::anyhow!("WRITE_OUTCOME_UNCERTAIN"))?;
        Ok(SealedObject {
            object: self.object.clone(),
            size: self.size,
            sha256: self.expected_sha256.clone(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct S3Binding {
    pub warehouse_id: String,
    pub table_id: String,
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub access_key_secret_ref: String,
    pub secret_key_secret_ref: String,
    pub session_token_secret_ref: Option<String>,
    pub limits: StorageLimits,
}

#[derive(Clone)]
pub struct S3ReadStore {
    binding: S3Binding,
    credentials: Credentials,
    client: reqwest::Client,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReaderBinding {
    namespace: IndexNamespace,
    provider: ReaderProvider,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum ReaderProvider {
    Posix { root: String, limits: StorageLimits },
    S3 { binding: S3Binding },
}

/// Resolve only trusted operator configuration. Head references never carry provider locations.
pub fn configured_reader(namespace: &IndexNamespace) -> Result<Arc<dyn ObjectReader>> {
    Ok(configured_reader_with_fingerprint(namespace)?.0)
}
pub fn configured_reader_with_fingerprint(
    namespace: &IndexNamespace,
) -> Result<(Arc<dyn ObjectReader>, String)> {
    let (store, fingerprint) = configured_store_with_fingerprint(namespace)?;
    Ok((store, fingerprint))
}
/// Discover only an operator-installed logical revision; no provider IO or caller location is accepted.
pub(crate) fn configured_namespace(warehouse: &str, table: &str) -> Result<IndexNamespace> {
    identity(warehouse)?;
    identity(table)?;
    let config = std::env::var("MORIA_NATIVE_STORAGE_BINDINGS")
        .map_err(|_| anyhow::anyhow!("STORAGE_BINDING_UNAVAILABLE"))?;
    ensure!(config.len() <= 64 * 1024, "INVALID_BINDING");
    let bindings: Vec<ReaderBinding> = serde_json::from_str(&config)?;
    let namespace = bindings
        .into_iter()
        .find(|binding| {
            binding.namespace.warehouse_id == warehouse && binding.namespace.table_id == table
        })
        .ok_or_else(|| anyhow::anyhow!("STORAGE_BINDING_UNAVAILABLE"))?
        .namespace;
    configured_binding(&namespace)?;
    Ok(namespace)
}
fn configured_binding(namespace: &IndexNamespace) -> Result<ReaderBinding> {
    let config = std::env::var("MORIA_NATIVE_STORAGE_BINDINGS")
        .map_err(|_| anyhow::anyhow!("STORAGE_BINDING_UNAVAILABLE"))?;
    ensure!(config.len() <= 64 * 1024, "INVALID_BINDING");
    let bindings: Vec<ReaderBinding> = serde_json::from_str(&config)?;
    ensure!(
        !bindings.is_empty() && bindings.len() <= 256,
        "INVALID_BINDING"
    );
    let mut seen = std::collections::BTreeSet::new();
    let mut selected = None;
    for binding in bindings {
        identity(&binding.namespace.warehouse_id)?;
        identity(&binding.namespace.table_id)?;
        ensure!(
            !binding.namespace.revision.is_empty()
                && binding.namespace.revision.len() <= 128
                && seen.insert((
                    binding.namespace.warehouse_id.clone(),
                    binding.namespace.table_id.clone()
                )),
            "AMBIGUOUS_STORAGE_BINDING"
        );
        if binding.namespace.warehouse_id == namespace.warehouse_id
            && binding.namespace.table_id == namespace.table_id
        {
            ensure!(
                binding.namespace.revision == namespace.revision,
                "STORAGE_BINDING_REVOKED"
            );
            selected = Some(binding);
        }
    }
    let selected = selected.ok_or_else(|| anyhow::anyhow!("STORAGE_BINDING_UNAVAILABLE"))?;
    if let ReaderProvider::S3 { binding } = &selected.provider {
        ensure!(
            binding.warehouse_id == namespace.warehouse_id
                && binding.table_id == namespace.table_id,
            "BINDING_MISMATCH"
        );
    }
    Ok(selected)
}
pub fn configured_store_with_fingerprint(
    namespace: &IndexNamespace,
) -> Result<(Arc<dyn ObjectStore>, String)> {
    let selected = configured_binding(namespace)?;
    let fingerprint = crate::canonical::fingerprint(&serde_json::to_value(&selected)?)?;
    let reader: Arc<dyn ObjectStore> = match selected.provider {
        ReaderProvider::Posix { root, limits } => Arc::new(PosixStore::provision(
            Path::new(&root),
            &namespace.warehouse_id,
            &namespace.table_id,
            limits,
        )?),
        ReaderProvider::S3 { binding } => {
            ensure!(
                binding.warehouse_id == namespace.warehouse_id
                    && binding.table_id == namespace.table_id,
                "BINDING_MISMATCH"
            );
            Arc::new(S3ReadStore::provision(binding)?)
        }
    };
    Ok((reader, fingerprint))
}
/// Metadata admission validates trusted bindings and secret reference names without resolving credentials or touching providers.
pub(crate) fn configured_binding_fingerprint(namespace: &IndexNamespace) -> Result<String> {
    let selected = configured_binding(namespace)?;
    match &selected.provider {
        ReaderProvider::Posix { root, limits } => {
            limits.validate()?;
            let _ = lexical_root(root)?;
        }
        ReaderProvider::S3 { binding } => {
            binding.validate()?;
        }
    }
    crate::canonical::fingerprint(&serde_json::to_value(selected)?)
}
fn lexical_root(root: &str) -> Result<String> {
    let path = Path::new(root);
    ensure!(
        path.is_absolute()
            && !path.components().any(|part| matches!(
                part,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )),
        "INVALID_BINDING"
    );
    Ok(path
        .components()
        .collect::<std::path::PathBuf>()
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("INVALID_BINDING"))?
        .into())
}

/// Legacy physical-binding identity, excluding credentials, limits and logical grant metadata.
pub fn configured_physical_binding(namespace: &IndexNamespace) -> Result<serde_json::Value> {
    let selected = configured_binding(namespace)?;
    Ok(match selected.provider {
        ReaderProvider::Posix { root, .. } => {
            serde_json::json!({"backend":"local","root":lexical_root(&root)?})
        }
        ReaderProvider::S3 { binding } => {
            let endpoint = binding.validate()?;
            serde_json::json!({"backend":"s3","endpoint":endpoint.as_str(),"bucket":binding.bucket,"region":binding.region,"prefix":binding.prefix})
        }
    })
}

impl Debug for S3ReadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ReadStore")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

fn secret(reference: &str) -> Result<String> {
    ensure!(
        !reference.is_empty()
            && reference
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
        "INVALID_SECRET_REFERENCE"
    );
    let value = std::env::var(reference).map_err(|_| anyhow::anyhow!("SECRET_UNAVAILABLE"))?;
    ensure!(!value.is_empty(), "SECRET_UNAVAILABLE");
    Ok(value)
}

impl S3Binding {
    fn validate(&self) -> Result<reqwest::Url> {
        self.limits.validate()?;
        identity(&self.warehouse_id)?;
        identity(&self.table_id)?;
        let endpoint = reqwest::Url::parse(&self.endpoint)?;
        ensure!(
            endpoint.username().is_empty()
                && endpoint.password().is_none()
                && endpoint.query().is_none()
                && endpoint.fragment().is_none()
                && endpoint.path() == "/"
                && (endpoint.scheme() == "https"
                    || (endpoint.scheme() == "http"
                        && matches!(endpoint.host_str(), Some("127.0.0.1" | "[::1]" | "::1")))),
            "INVALID_BINDING"
        );
        ensure!(
            (3..=63).contains(&self.bucket.len())
                && self
                    .bucket
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                && self.bucket.as_bytes()[0].is_ascii_alphanumeric()
                && self.bucket.as_bytes()[self.bucket.len() - 1].is_ascii_alphanumeric(),
            "INVALID_BINDING"
        );
        ensure!(
            !self.region.is_empty()
                && self.region.len() <= 64
                && self
                    .region
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
            "INVALID_BINDING"
        );
        ensure!(self.prefix.len() <= 512, "INVALID_BINDING");
        if !self.prefix.is_empty() {
            for part in self.prefix.split('/') {
                identity(part)?;
            }
        }
        Ok(endpoint)
    }
}

impl S3ReadStore {
    async fn remove_version(
        &self,
        object: LogicalObject,
        expected: ObjectVersion,
        authority: Arc<dyn RemoveAuthority>,
        cancel: Cancellation,
    ) -> Result<bool> {
        let (_, id) = s3_version_parts(&expected)?;
        let id = id
            .filter(|id| id != "null")
            .ok_or_else(|| anyhow::anyhow!("CONDITIONAL_DELETE_UNSUPPORTED"))?;
        self.url(&object)?;
        cancel.check()?;
        authority.check_remove(&object, &expected).await?;
        let mut bucket = self.url(&object)?;
        bucket.set_path(&format!("/{}", self.binding.bucket));
        bucket.set_query(Some("versioning"));
        let status = self
            .request_body("GET", bucket.clone(), &[], Bytes::new(), &cancel)
            .await?;
        ensure!(status.status().is_success(), "STORAGE_PROVIDER_ERROR");
        let body = provider_body(status, &cancel).await?;
        authority.check_remove(&object, &expected).await?;
        ensure!(
            provider_xml(&body, "VersioningConfiguration", "Status")? == "Enabled",
            "CONDITIONAL_DELETE_UNSUPPORTED"
        );
        let mut url = self.url(&object)?;
        url.query_pairs_mut().append_pair("versionId", &id);
        authority.check_remove(&object, &expected).await?;
        let head = self
            .request_body("HEAD", url.clone(), &[], Bytes::new(), &cancel)
            .await?;
        authority.check_remove(&object, &expected).await?;
        if head.status() == reqwest::StatusCode::NOT_FOUND {
            cancel.check()?;
            return Ok(false);
        }
        ensure!(
            self.head_version(&head)? == expected,
            "STORAGE_VERSION_CHANGED"
        );
        authority.check_remove(&object, &expected).await?;
        let removed = self
            .request_body("DELETE", url.clone(), &[], Bytes::new(), &cancel)
            .await?;
        authority.check_remove(&object, &expected).await?;
        ensure!(
            removed.status() == reqwest::StatusCode::NO_CONTENT
                && removed
                    .headers()
                    .get("x-amz-version-id")
                    .and_then(|v| v.to_str().ok())
                    == Some(id.as_str()),
            "STORAGE_DELETE_UNCERTAIN"
        );
        let absent = self
            .request_body("HEAD", url, &[], Bytes::new(), &cancel)
            .await?;
        authority.check_remove(&object, &expected).await?;
        ensure!(
            absent.status() == reqwest::StatusCode::NOT_FOUND,
            "STORAGE_DELETE_UNCERTAIN"
        );
        let status = self
            .request_body("GET", bucket, &[], Bytes::new(), &cancel)
            .await?;
        ensure!(status.status().is_success(), "STORAGE_DELETE_UNCERTAIN");
        let body = provider_body(status, &cancel).await?;
        authority.check_remove(&object, &expected).await?;
        cancel.check()?;
        ensure!(
            provider_xml(&body, "VersioningConfiguration", "Status")? == "Enabled",
            "STORAGE_DELETE_UNCERTAIN"
        );
        Ok(true)
    }
    fn head_version(&self, response: &reqwest::Response) -> Result<ObjectVersion> {
        ensure!(response.status().is_success(), "STORAGE_PROVIDER_ERROR");
        let size = response
            .headers()
            .get("content-length")
            .ok_or_else(|| anyhow::anyhow!("STORAGE_PROTOCOL_ERROR"))?
            .to_str()?
            .parse::<u64>()?;
        let etag = response
            .headers()
            .get("etag")
            .ok_or_else(|| anyhow::anyhow!("STORAGE_PROTOCOL_ERROR"))?
            .to_str()?;
        ensure!(
            size <= self.binding.limits.max_object_bytes
                && !etag.is_empty()
                && etag.len() <= 1024
                && etag.bytes().all(|b| b.is_ascii_graphic()),
            "STORAGE_PROTOCOL_ERROR"
        );
        let id = response
            .headers()
            .get("x-amz-version-id")
            .map(|id| id.to_str())
            .transpose()?;
        if let Some(id) = id {
            ensure!(
                !id.is_empty() && id.len() <= 1024 && id.bytes().all(|b| b.is_ascii_graphic()),
                "STORAGE_PROTOCOL_ERROR"
            );
        }
        Ok(ObjectVersion {
            size,
            token: match id {
                Some(id) => format!("s3v1/{}", serde_json::to_string(&[etag, id])?),
                None => format!("s3/{etag}"),
            },
        })
    }
    pub fn provision(binding: S3Binding) -> Result<Self> {
        binding.validate()?;
        let credentials = Credentials::new(
            secret(&binding.access_key_secret_ref)?,
            secret(&binding.secret_key_secret_ref)?,
            binding
                .session_token_secret_ref
                .as_deref()
                .map(secret)
                .transpose()?,
            None,
            "moria-provisioned",
        );
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self {
            binding,
            credentials,
            client,
        })
    }

    fn url(&self, object: &LogicalObject) -> Result<reqwest::Url> {
        object.validate()?;
        ensure!(
            object.warehouse_id == self.binding.warehouse_id
                && object.table_id == self.binding.table_id,
            "BINDING_MISMATCH"
        );
        let mut url = reqwest::Url::parse(&self.binding.endpoint)?;
        let key = [
            &self.binding.prefix,
            &object.warehouse_id,
            &object.table_id,
            "objects",
            &object.object_id,
        ]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("/");
        url.set_path(&format!("/{}/{key}", self.binding.bucket));
        Ok(url)
    }

    async fn request(
        &self,
        method: &str,
        object: &LogicalObject,
        headers: &[(&str, String)],
        cancel: &Cancellation,
    ) -> Result<reqwest::Response> {
        self.request_body(method, self.url(object)?, headers, Bytes::new(), cancel)
            .await
    }

    async fn request_body(
        &self,
        method: &str,
        url: reqwest::Url,
        headers: &[(&str, String)],
        body: Bytes,
        cancel: &Cancellation,
    ) -> Result<reqwest::Response> {
        cancel.check()?;
        let mut request = http::Request::builder().method(method).uri(url.as_str());
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        let mut request = request.body(())?;
        let identity = self.credentials.clone().into();
        let mut settings = SigningSettings::default();
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
        let params = SigningParams::builder()
            .identity(&identity)
            .region(&self.binding.region)
            .name("s3")
            .time(SystemTime::now())
            .settings(settings)
            .build()?
            .into();
        let signable = SignableRequest::new(
            method,
            url.as_str(),
            request
                .headers()
                .iter()
                .map(|(k, v)| (k.as_str(), v.to_str().unwrap_or(""))),
            SignableBody::Bytes(&body),
        )?;
        let (instructions, _) = sign(signable, &params)?.into_parts();
        instructions.apply_to_request_http1x(&mut request);
        let request = self
            .client
            .request(reqwest::Method::from_bytes(method.as_bytes())?, url)
            .headers(request.headers().clone())
            .body(body);
        tokio::select! { result = request.send() => Ok(result?), _ = cancel.cancelled() => bail!("CANCELLED") }
    }

    pub async fn read_range(
        &self,
        object: &LogicalObject,
        range: Range<u64>,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Bytes> {
        let version = self.version(object, authority, cancel).await?;
        self.range_at_version(object, range, &version, authority, cancel)
            .await
    }
    pub async fn read(
        &self,
        object: LogicalObject,
        authority: Arc<dyn ReadAuthority>,
        cancel: Cancellation,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        let version = self.version(&object, authority.as_ref(), &cancel).await?;
        let store = self.clone();
        Ok(futures::stream::try_unfold(
            (store, object, authority, cancel, version, 0u64),
            |(store, object, authority, cancel, version, offset)| async move {
                ensure!(
                    store.version(&object, authority.as_ref(), &cancel).await? == version,
                    "STORAGE_OBJECT_CHANGED"
                );
                if offset == version.size {
                    return Ok(None);
                }
                let end = version
                    .size
                    .min(offset + store.binding.limits.chunk_bytes as u64);
                let bytes = store
                    .range_at_version(&object, offset..end, &version, authority.as_ref(), &cancel)
                    .await?;
                ensure!(
                    store.version(&object, authority.as_ref(), &cancel).await? == version,
                    "STORAGE_OBJECT_CHANGED"
                );
                Ok(Some((
                    bytes,
                    (store, object, authority, cancel, version, end),
                )))
            },
        )
        .boxed())
    }
}

const S3_PART_BYTES: usize = 8 * 1024 * 1024;
const S3_PARTS_MAX: usize = 10_000;

async fn provider_body(response: reqwest::Response, cancel: &Cancellation) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(frame) =
        tokio::select! {frame=stream.next()=>frame,_=cancel.cancelled()=>bail!("CANCELLED")}
    {
        let frame = frame?;
        ensure!(
            body.len()
                .checked_add(frame.len())
                .is_some_and(|n| n <= 64 * 1024),
            "STORAGE_PROTOCOL_ERROR"
        );
        body.extend_from_slice(&frame);
    }
    Ok(body)
}
fn provider_xml(body: &[u8], root_name: &str, field: &str) -> Result<String> {
    let text = std::str::from_utf8(body)?;
    let document = roxmltree::Document::parse_with_options(
        text,
        roxmltree::ParsingOptions {
            allow_dtd: false,
            nodes_limit: 4096,
            ..Default::default()
        },
    )?;
    let root = document.root_element();
    ensure!(
        root.tag_name().name() == root_name,
        "STORAGE_PROTOCOL_ERROR"
    );
    let matches: Vec<_> = root
        .children()
        .filter(|node| node.is_element() && node.tag_name().name() == field)
        .collect();
    ensure!(
        matches.len() == 1 && matches[0].children().all(|n| n.is_text()),
        "STORAGE_PROTOCOL_ERROR"
    );
    let value = matches[0]
        .text()
        .ok_or_else(|| anyhow::anyhow!("STORAGE_PROTOCOL_ERROR"))?;
    ensure!(
        !value.is_empty() && value.len() <= 2048,
        "STORAGE_PROTOCOL_ERROR"
    );
    Ok(value.into())
}
fn multipart_page(body: &[u8], key: &str, after: Option<&str>) -> Result<MultipartPage> {
    ensure!(body.len() <= 64 * 1024, "STORAGE_PROTOCOL_ERROR");
    let document = roxmltree::Document::parse_with_options(
        std::str::from_utf8(body)?,
        roxmltree::ParsingOptions {
            allow_dtd: false,
            nodes_limit: 4096,
            ..Default::default()
        },
    )?;
    let root = document.root_element();
    ensure!(
        root.tag_name().name() == "ListMultipartUploadsResult",
        "STORAGE_PROTOCOL_ERROR"
    );
    fn field(node: roxmltree::Node<'_, '_>, name: &str) -> Result<String> {
        let fields = node
            .children()
            .filter(|n| n.is_element() && n.tag_name().name() == name)
            .collect::<Vec<_>>();
        ensure!(
            fields.len() == 1 && fields[0].children().all(|n| n.is_text()),
            "STORAGE_PROTOCOL_ERROR"
        );
        let value = fields[0].text().unwrap_or("");
        ensure!(value.len() <= 2048, "STORAGE_PROTOCOL_ERROR");
        Ok(value.into())
    }
    let truncated = field(root, "IsTruncated")?;
    ensure!(
        truncated == "true" || truncated == "false",
        "STORAGE_PROTOCOL_ERROR"
    );
    let mut ids = Vec::new();
    let mut count = 0;
    let mut previous = key.to_string();
    let mut all_exact = true;
    for upload in root
        .children()
        .filter(|n| n.is_element() && n.tag_name().name() == "Upload")
    {
        count += 1;
        ensure!(count <= 16, "STORAGE_PROTOCOL_ERROR");
        let found = field(upload, "Key")?;
        ensure!(
            found.starts_with(key) && found >= previous,
            "STORAGE_PROTOCOL_ERROR"
        );
        previous = found.clone();
        all_exact &= found == key;
        if found == key {
            let id = field(upload, "UploadId")?;
            ensure!(
                !id.is_empty() && id.bytes().all(|b| b.is_ascii_graphic()) && !ids.contains(&id),
                "STORAGE_PROTOCOL_ERROR"
            );
            ids.push(id);
        }
    }
    let next = if truncated == "true" {
        ensure!(count > 0, "STORAGE_PROTOCOL_ERROR");
        let next_key = field(root, "NextKeyMarker")?;
        ensure!(
            (next_key.starts_with(key) && next_key >= previous)
                || (next_key.is_empty() && all_exact),
            "STORAGE_PROTOCOL_ERROR"
        );
        if next_key == key || next_key.is_empty() {
            let marker = field(root, "NextUploadIdMarker")?;
            ensure!(
                !marker.is_empty()
                    && marker.bytes().all(|b| b.is_ascii_graphic())
                    && Some(marker.as_str()) != after
                    && ids.last() == Some(&marker),
                "STORAGE_PROTOCOL_ERROR"
            );
            Some(marker)
        } else {
            None
        }
    } else {
        None
    };
    Ok(MultipartPage {
        upload_ids: ids,
        next_upload_id: next,
    })
}
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// An owned hidden multipart attempt. The upload ID never escapes as a logical object reference.
pub struct PrivateS3Write {
    store: S3ReadStore,
    object: LogicalObject,
    authority: Arc<dyn WriteAuthority>,
    cancel: Cancellation,
    upload_id: Option<String>,
    expected_size: u64,
    expected_sha256: String,
    bytes: u64,
    hash: Sha256,
    buffer: Vec<u8>,
    parts: Vec<String>,
    complete: bool,
    failed: bool,
}

impl S3ReadStore {
    pub async fn begin_write(
        &self,
        object: LogicalObject,
        expected_size: u64,
        expected_sha256: String,
        authority: Arc<dyn WriteAuthority>,
        cancel: Cancellation,
    ) -> Result<PrivateS3Write> {
        self.url(&object)?;
        digest(&expected_sha256)?;
        ensure!(
            expected_size <= self.binding.limits.max_object_bytes
                && expected_size.div_ceil(S3_PART_BYTES as u64) <= S3_PARTS_MAX as u64,
            "LIMIT_EXCEEDED"
        );
        cancel.check()?;
        let physical = self.physical_fingerprint()?;
        tokio::select! {
            _ = cancel.cancelled() => return Err(anyhow::anyhow!("CANCELLED")),
            result = tokio::time::timeout(std::time::Duration::from_secs(30), authority.admit_write(&object, expected_size, &expected_sha256, &physical)) => result.map_err(|_|anyhow::anyhow!("AUTHORITY_TIMEOUT"))??,
        }
        cancel.check()?;
        let mut pending = PrivateS3Write {
            store: self.clone(),
            object,
            authority,
            cancel,
            upload_id: None,
            expected_size,
            expected_sha256,
            bytes: 0,
            hash: Sha256::new(),
            buffer: Vec::new(),
            parts: Vec::new(),
            complete: false,
            failed: false,
        };
        if expected_size > 0 {
            pending.cancel.check()?;
            tokio::select! {
                _=pending.cancel.cancelled()=>return Err(anyhow::anyhow!("CANCELLED")),
                result=tokio::time::timeout(Duration::from_secs(30),pending.authority.prepare_multipart(&pending.object,&physical))=>result.map_err(|_|anyhow::anyhow!("AUTHORITY_TIMEOUT"))??,
            }
            pending.cancel.check()?;
            let mut url = pending.store.url(&pending.object)?;
            url.set_query(Some("uploads="));
            let response = pending
                .store
                .request_body(
                    "POST",
                    url,
                    &[
                        ("x-amz-meta-moria-sha256", pending.expected_sha256.clone()),
                        ("x-amz-meta-moria-size", expected_size.to_string()),
                    ],
                    Bytes::new(),
                    &pending.cancel,
                )
                .await?;
            ensure!(response.status().is_success(), "STORAGE_PROVIDER_ERROR");
            let body = provider_body(response, &pending.cancel).await?;
            pending.upload_id = Some(provider_xml(
                &body,
                "InitiateMultipartUploadResult",
                "UploadId",
            )?);
        }
        if let Some(upload_id) = pending.upload_id.as_deref() {
            pending.cancel.check()?;
            tokio::select! {
                _ = pending.cancel.cancelled() => return Err(anyhow::anyhow!("CANCELLED")),
                result = tokio::time::timeout(Duration::from_secs(30),pending.authority.record_multipart(&pending.object,upload_id,&physical)) => result.map_err(|_|anyhow::anyhow!("AUTHORITY_TIMEOUT"))??,
            }
            pending.cancel.check()?;
        }
        check_write(pending.authority.as_ref(), &pending.object, &pending.cancel).await?;
        Ok(pending)
    }

    /// Recover an uncertain completion by verifying the exact immutable object under fresh read+write authority.
    /// This does not retry Complete and never accepts ETag or metadata as a content hash.
    pub async fn recover_write(
        &self,
        object: LogicalObject,
        expected_size: u64,
        expected_sha256: String,
        read: Arc<dyn ReadAuthority>,
        write: Arc<dyn WriteAuthority>,
        cancel: Cancellation,
    ) -> Result<SealedObject> {
        self.recover_object(object, expected_size, expected_sha256, read, write, cancel)
            .await?
            .ok_or_else(|| anyhow::anyhow!("STORAGE_OBJECT_NOT_FOUND"))
    }
}
impl PrivateS3Write {
    async fn part(&mut self) -> Result<()> {
        ensure!(
            !self.buffer.is_empty() && self.parts.len() < S3_PARTS_MAX,
            "LIMIT_EXCEEDED"
        );
        check_write(self.authority.as_ref(), &self.object, &self.cancel).await?;
        self.failed = true;
        let mut url = self.store.url(&self.object)?;
        url.query_pairs_mut()
            .append_pair("partNumber", &(self.parts.len() + 1).to_string())
            .append_pair("uploadId", self.upload_id.as_deref().unwrap());
        let body = Bytes::from(std::mem::take(&mut self.buffer));
        let response = self
            .store
            .request_body("PUT", url, &[], body, &self.cancel)
            .await?;
        ensure!(response.status().is_success(), "STORAGE_PROVIDER_ERROR");
        let etag = response
            .headers()
            .get("etag")
            .ok_or_else(|| anyhow::anyhow!("STORAGE_PROTOCOL_ERROR"))?
            .to_str()?
            .to_string();
        ensure!(
            !etag.is_empty() && etag.len() <= 256 && etag.bytes().all(|b| b.is_ascii_graphic()),
            "STORAGE_PROTOCOL_ERROR"
        );
        provider_body(response, &self.cancel).await?;
        check_write(self.authority.as_ref(), &self.object, &self.cancel).await?;
        self.parts.push(etag);
        self.failed = false;
        Ok(())
    }
    pub async fn append(&mut self, bytes: &[u8]) -> Result<()> {
        let result = self.append_inner(bytes).await;
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    async fn append_inner(&mut self, bytes: &[u8]) -> Result<()> {
        ensure!(
            !self.failed
                && !self.complete
                && bytes.len() <= self.store.binding.limits.chunk_bytes
                && self
                    .bytes
                    .checked_add(bytes.len() as u64)
                    .is_some_and(|n| n <= self.expected_size),
            "CONTENT_MISMATCH"
        );
        check_write(self.authority.as_ref(), &self.object, &self.cancel).await?;
        self.hash.update(bytes);
        self.bytes += bytes.len() as u64;
        let mut remaining = bytes;
        while !remaining.is_empty() {
            let take = remaining.len().min(S3_PART_BYTES - self.buffer.len());
            self.buffer.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];
            if self.buffer.len() == S3_PART_BYTES {
                self.part().await?;
            }
        }
        check_write(self.authority.as_ref(), &self.object, &self.cancel).await?;
        Ok(())
    }
    async fn abort_owned(&mut self) -> Result<()> {
        if let Some(upload_id) = self.upload_id.take() {
            let mut url = self.store.url(&self.object)?;
            url.query_pairs_mut().append_pair("uploadId", &upload_id);
            let response = self
                .store
                .request_body("DELETE", url, &[], Bytes::new(), &Cancellation::default())
                .await?;
            ensure!(
                response.status().is_success()
                    || response.status() == reqwest::StatusCode::NOT_FOUND,
                "STORAGE_PROVIDER_ERROR"
            );
        }
        self.buffer.clear();
        Ok(())
    }
    pub async fn abort(mut self) -> Result<()> {
        self.abort_owned().await
    }
    pub async fn seal(mut self) -> Result<SealedObject> {
        ensure!(
            !self.failed
                && self.bytes == self.expected_size
                && format!("sha256:{:x}", self.hash.clone().finalize()) == self.expected_sha256,
            "CONTENT_MISMATCH"
        );
        if !self.buffer.is_empty() {
            self.part().await?;
        }
        check_write(self.authority.as_ref(), &self.object, &self.cancel).await?;
        let mut url = self.store.url(&self.object)?;
        let body;
        let method = if self.expected_size == 0 {
            body = Bytes::new();
            "PUT"
        } else {
            url.query_pairs_mut()
                .append_pair("uploadId", self.upload_id.as_deref().unwrap());
            let parts = self
                .parts
                .iter()
                .enumerate()
                .map(|(i, etag)| {
                    format!(
                        "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                        i + 1,
                        xml_escape(etag)
                    )
                })
                .collect::<String>();
            let xml = format!("<CompleteMultipartUpload>{parts}</CompleteMultipartUpload>");
            ensure!(xml.len() <= 16 * 1024 * 1024, "LIMIT_EXCEEDED");
            body = Bytes::from(xml);
            "POST"
        };
        let response = self
            .store
            .request_body(
                method,
                url,
                &[
                    ("if-none-match", "*".into()),
                    ("content-type", "application/xml".into()),
                ],
                body,
                &self.cancel,
            )
            .await
            .map_err(|_| anyhow::anyhow!("WRITE_OUTCOME_UNCERTAIN"))?;
        if response.status() == reqwest::StatusCode::PRECONDITION_FAILED {
            bail!("IMMUTABLE_OBJECT_EXISTS");
        }
        ensure!(response.status().is_success(), "WRITE_OUTCOME_UNCERTAIN");
        let body = provider_body(response, &self.cancel)
            .await
            .map_err(|_| anyhow::anyhow!("WRITE_OUTCOME_UNCERTAIN"))?;
        if self.expected_size > 0 {
            provider_xml(&body, "CompleteMultipartUploadResult", "ETag")
                .map_err(|_| anyhow::anyhow!("WRITE_OUTCOME_UNCERTAIN"))?;
        }
        self.complete = true;
        self.upload_id = None;
        check_write(self.authority.as_ref(), &self.object, &self.cancel)
            .await
            .map_err(|_| anyhow::anyhow!("WRITE_OUTCOME_UNCERTAIN"))?;
        Ok(SealedObject {
            object: self.object.clone(),
            size: self.expected_size,
            sha256: self.expected_sha256.clone(),
        })
    }
}
impl Drop for PrivateS3Write {
    fn drop(&mut self) {
        if let Some(upload_id) = self.upload_id.take() {
            let store = self.store.clone();
            let object = self.object.clone();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let Ok(mut url) = store.url(&object) else {
                        return;
                    };
                    url.query_pairs_mut().append_pair("uploadId", &upload_id);
                    let _ = store
                        .request_body("DELETE", url, &[], Bytes::new(), &Cancellation::default())
                        .await;
                });
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectVersion {
    pub size: u64,
    pub token: String,
}
fn s3_version_parts(version: &ObjectVersion) -> Result<(String, Option<String>)> {
    if let Some(token) = version.token.strip_prefix("s3v1/") {
        ensure!(token.len() <= 4096, "INVALID_STORAGE_VERSION");
        let [etag, id]: [String; 2] = serde_json::from_str(token)?;
        ensure!(
            !etag.is_empty()
                && etag.len() <= 1024
                && etag.bytes().all(|b| b.is_ascii_graphic())
                && !id.is_empty()
                && id.len() <= 1024
                && id.bytes().all(|b| b.is_ascii_graphic())
                && serde_json::to_string(&[&etag, &id])? == token,
            "INVALID_STORAGE_VERSION"
        );
        return Ok((etag, Some(id)));
    }
    let etag = version
        .token
        .strip_prefix("s3/")
        .filter(|tag| {
            !tag.is_empty() && tag.len() <= 1024 && tag.bytes().all(|b| b.is_ascii_graphic())
        })
        .ok_or_else(|| anyhow::anyhow!("INVALID_STORAGE_VERSION"))?;
    Ok((etag.into(), None))
}
fn posix_version(file: &File) -> Result<ObjectVersion> {
    let stat = file.metadata()?;
    ensure!(stat.is_file() && stat.nlink() == 1, "UNSAFE_PATH");
    Ok(ObjectVersion {
        size: stat.len(),
        token: format!(
            "posix/{}/{}/{}/{}/{}/{}",
            stat.dev(),
            stat.ino(),
            stat.mtime(),
            stat.mtime_nsec(),
            stat.ctime(),
            stat.ctime_nsec()
        ),
    })
}

#[async_trait]
pub trait ObjectReader: Debug + Send + Sync {
    async fn inspect(
        &self,
        object: &LogicalObject,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Option<ObjectVersion>> {
        Ok(Some(self.version(object, authority, cancel).await?))
    }
    /// Provider identity is separate from authority; managed Dataset grants require an exact match.
    fn physical_fingerprint(&self) -> Result<String> {
        anyhow::bail!("STORAGE_BINDING_UNAVAILABLE")
    }
    fn chunk_bytes(&self) -> usize;
    async fn version(
        &self,
        object: &LogicalObject,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<ObjectVersion>;
    async fn range_at_version(
        &self,
        object: &LogicalObject,
        range: Range<u64>,
        version: &ObjectVersion,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Bytes>;
    async fn range(
        &self,
        object: &LogicalObject,
        range: Range<u64>,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Bytes>;
    async fn read_object(
        &self,
        object: LogicalObject,
        authority: Arc<dyn ReadAuthority>,
        cancel: Cancellation,
    ) -> Result<BoxStream<'static, Result<Bytes>>>;
}

/// Private decoder buffering for one reader operation; every new window revalidates authority and version.
/// Callers must retain their current-authority checks before releasing rows or sealing a proof.
#[derive(Debug)]
pub struct ObjectReadWindow {
    object: LogicalObject,
    reader: Arc<dyn ObjectReader>,
    authority: Arc<dyn ReadAuthority>,
    cancel: Cancellation,
    version: Option<ObjectVersion>,
    start: u64,
    bytes: Bytes,
    validated: std::time::Instant,
}
impl ObjectReadWindow {
    const LIMIT: u64 = 1024 * 1024;
    pub fn new(
        object: LogicalObject,
        reader: Arc<dyn ObjectReader>,
        authority: Arc<dyn ReadAuthority>,
        cancel: Cancellation,
    ) -> Self {
        Self {
            object,
            reader,
            authority,
            cancel,
            version: None,
            start: 0,
            bytes: Bytes::new(),
            validated: std::time::Instant::now(),
        }
    }
    pub fn contains(&self, range: &Range<u64>) -> bool {
        self.version.is_some()
            && self.validated.elapsed() < Duration::from_secs(1)
            && range.start >= self.start
            && range.end <= self.start + self.bytes.len() as u64
    }
    pub async fn read(&mut self, range: Range<u64>) -> Result<Bytes> {
        self.cancel.check()?;
        ensure!(
            range.start <= range.end && range.end - range.start <= 16 * 1024 * 1024,
            "PARQUET_RANGE_LIMIT"
        );
        if self.contains(&range) {
            return Ok(self
                .bytes
                .slice((range.start - self.start) as usize..(range.end - self.start) as usize));
        }
        let version = self
            .reader
            .version(&self.object, self.authority.as_ref(), &self.cancel)
            .await?;
        ensure!(
            range.end <= version.size && self.version.as_ref().is_none_or(|old| old == &version),
            "STORAGE_VERSION_CHANGED"
        );
        self.version = Some(version.clone());
        if range.end - range.start > Self::LIMIT {
            self.bytes = Bytes::new();
            return self
                .reader
                .range_at_version(
                    &self.object,
                    range,
                    &version,
                    self.authority.as_ref(),
                    &self.cancel,
                )
                .await;
        }
        let end = range.start.saturating_add(Self::LIMIT).min(version.size);
        let bytes = self
            .reader
            .range_at_version(
                &self.object,
                range.start..end,
                &version,
                self.authority.as_ref(),
                &self.cancel,
            )
            .await?;
        ensure!(bytes.len() as u64 == end - range.start, "CONTENT_MISMATCH");
        self.start = range.start;
        self.bytes = bytes;
        self.validated = std::time::Instant::now();
        self.cancel.check()?;
        Ok(self.bytes.slice(..(range.end - range.start) as usize))
    }
    pub async fn check_current(&self) -> Result<()> {
        self.cancel.check()?;
        let current = self
            .reader
            .version(&self.object, self.authority.as_ref(), &self.cancel)
            .await?;
        ensure!(
            self.version.as_ref().is_none_or(|old| old == &current),
            "STORAGE_VERSION_CHANGED"
        );
        self.cancel.check()
    }
}

pub async fn versioned_stream(
    reader: Arc<dyn ObjectReader>,
    object: LogicalObject,
    version: ObjectVersion,
    authority: Arc<dyn ReadAuthority>,
    cancel: Cancellation,
) -> Result<BoxStream<'static, Result<Bytes>>> {
    check_read(authority.as_ref(), &object, &cancel).await?;
    let chunk = reader.chunk_bytes();
    ensure!((1..=CHUNK_MAX).contains(&chunk), "INVALID_BINDING");
    Ok(futures::stream::try_unfold(
        (reader, object, version, authority, cancel, 0u64),
        move |(reader, object, version, authority, cancel, position)| async move {
            if position == version.size {
                check_read(authority.as_ref(), &object, &cancel).await?;
                return Ok(None);
            }
            let end = position.saturating_add(chunk as u64).min(version.size);
            let bytes = reader
                .range_at_version(
                    &object,
                    position..end,
                    &version,
                    authority.as_ref(),
                    &cancel,
                )
                .await?;
            ensure!(bytes.len() as u64 == end - position, "CONTENT_MISMATCH");
            Ok(Some((
                bytes,
                (reader, object, version, authority, cancel, end),
            )))
        },
    )
    .boxed())
}

#[async_trait]
impl ObjectReader for PosixStore {
    async fn inspect(
        &self,
        object: &LogicalObject,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Option<ObjectVersion>> {
        match self.version(object, authority, cancel).await {
            Ok(version) => Ok(Some(version)),
            Err(error)
                if error.downcast_ref::<rustix::io::Errno>() == Some(&rustix::io::Errno::NOENT) =>
            {
                check_read(authority, object, cancel).await?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
    fn physical_fingerprint(&self) -> Result<String> {
        Ok(self.physical_fingerprint.clone())
    }
    fn chunk_bytes(&self) -> usize {
        self.limits.chunk_bytes
    }
    async fn version(
        &self,
        object: &LogicalObject,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<ObjectVersion> {
        self.scope(object)?;
        check_read(authority, object, cancel).await?;
        let version = posix_version(&regular(&self.objects, &object.object_id)?)?;
        ensure!(
            version.size <= self.limits.max_object_bytes,
            "LIMIT_EXCEEDED"
        );
        check_read(authority, object, cancel).await?;
        Ok(version)
    }
    async fn range_at_version(
        &self,
        object: &LogicalObject,
        range: Range<u64>,
        version: &ObjectVersion,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Bytes> {
        self.scope(object)?;
        check_read(authority, object, cancel).await?;
        let file = Arc::new(regular(&self.objects, &object.object_id)?);
        ensure!(
            posix_version(&file)? == *version && version.size <= self.limits.max_object_bytes,
            "STORAGE_VERSION_CHANGED"
        );
        let result = read_posix_range(
            file.clone(),
            range,
            self.limits.chunk_bytes,
            authority,
            object,
            cancel,
        )
        .await?;
        ensure!(posix_version(&file)? == *version, "STORAGE_VERSION_CHANGED");
        check_read(authority, object, cancel).await?;
        Ok(result)
    }
    async fn range(
        &self,
        object: &LogicalObject,
        range: Range<u64>,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Bytes> {
        self.read_range(object, range, authority, cancel).await
    }
    async fn read_object(
        &self,
        object: LogicalObject,
        authority: Arc<dyn ReadAuthority>,
        cancel: Cancellation,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        self.read(object, authority, cancel).await
    }
}

#[async_trait]
impl ObjectReader for S3ReadStore {
    async fn inspect(
        &self,
        object: &LogicalObject,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Option<ObjectVersion>> {
        self.url(object)?;
        check_read(authority, object, cancel).await?;
        let response = self.request("HEAD", object, &[], cancel).await?;
        check_read(authority, object, cancel).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            let mut bucket = self.url(object)?;
            bucket.set_path(&format!("/{}", self.binding.bucket));
            let present = self
                .request_body("HEAD", bucket, &[], Bytes::new(), cancel)
                .await?;
            check_read(authority, object, cancel).await?;
            ensure!(present.status().is_success(), "STORAGE_PROVIDER_ERROR");
            return Ok(None);
        }
        Ok(Some(self.head_version(&response)?))
    }
    fn physical_fingerprint(&self) -> Result<String> {
        crate::canonical::fingerprint(
            &serde_json::json!({"backend":"s3","endpoint":self.binding.validate()?.as_str(),"bucket":self.binding.bucket,"region":self.binding.region,"prefix":self.binding.prefix}),
        )
    }
    fn chunk_bytes(&self) -> usize {
        self.binding.limits.chunk_bytes
    }
    async fn version(
        &self,
        object: &LogicalObject,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<ObjectVersion> {
        self.url(object)?;
        check_read(authority, object, cancel).await?;
        let response = self.request("HEAD", object, &[], cancel).await?;
        let version = self.head_version(&response)?;
        check_read(authority, object, cancel).await?;
        Ok(version)
    }
    async fn range_at_version(
        &self,
        object: &LogicalObject,
        range: Range<u64>,
        version: &ObjectVersion,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Bytes> {
        let length = range
            .end
            .checked_sub(range.start)
            .filter(|n| *n <= RANGE_MAX)
            .ok_or_else(|| anyhow::anyhow!("INVALID_INPUT"))?;
        ensure!(
            range.end <= version.size && version.size <= self.binding.limits.max_object_bytes,
            "INVALID_INPUT"
        );
        let (etag, id) = s3_version_parts(version)?;
        self.url(object)?;
        check_read(authority, object, cancel).await?;
        if length == 0 {
            ensure!(
                self.version(object, authority, cancel).await? == *version,
                "STORAGE_VERSION_CHANGED"
            );
            return Ok(Bytes::new());
        }
        let mut url = self.url(object)?;
        if let Some(id) = &id {
            url.query_pairs_mut().append_pair("versionId", id);
        }
        let response = self
            .request_body(
                "GET",
                url,
                &[
                    ("range", format!("bytes={}-{}", range.start, range.end - 1)),
                    ("if-match", etag.clone()),
                ],
                Bytes::new(),
                cancel,
            )
            .await?;
        ensure!(
            response.status() == reqwest::StatusCode::PARTIAL_CONTENT
                && response.headers().get("etag").and_then(|v| v.to_str().ok())
                    == Some(etag.as_str())
                && response
                    .headers()
                    .get("x-amz-version-id")
                    .and_then(|v| v.to_str().ok())
                    == id.as_deref()
                && response
                    .headers()
                    .get("content-range")
                    .and_then(|v| v.to_str().ok())
                    == Some(
                        format!("bytes {}-{}/{}", range.start, range.end - 1, version.size)
                            .as_str()
                    )
                && response.content_length() == Some(length),
            "STORAGE_VERSION_CHANGED"
        );
        let mut stream = response.bytes_stream();
        let mut result = Vec::with_capacity(length as usize);
        while let Some(frame) =
            tokio::select! {frame=stream.next()=>frame,_=cancel.cancelled()=>bail!("CANCELLED")}
        {
            check_read(authority, object, cancel).await?;
            let frame = frame?;
            ensure!(
                frame.len() <= self.binding.limits.chunk_bytes
                    && result
                        .len()
                        .checked_add(frame.len())
                        .is_some_and(|n| n <= length as usize),
                "STORAGE_PROTOCOL_ERROR"
            );
            result.extend_from_slice(&frame);
            check_read(authority, object, cancel).await?;
        }
        ensure!(result.len() == length as usize, "CONTENT_MISMATCH");
        check_read(authority, object, cancel).await?;
        Ok(result.into())
    }
    async fn range(
        &self,
        object: &LogicalObject,
        range: Range<u64>,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Bytes> {
        self.read_range(object, range, authority, cancel).await
    }
    async fn read_object(
        &self,
        object: LogicalObject,
        authority: Arc<dyn ReadAuthority>,
        cancel: Cancellation,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        self.read(object, authority, cancel).await
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClosureEntry {
    pub name: String,
    pub content: SealedObject,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClosureLimits {
    pub max_entries: u64,
    pub max_referenced_bytes: u64,
    pub max_spool_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SealedClosure {
    pub version: u32,
    pub id: Uuid,
    pub object: LogicalObject,
    pub entries: u64,
    pub referenced_bytes: u64,
    pub sha256: String,
}
pub(crate) fn closure_digest_start() -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(b"moria-closure-v1\0");
    hash
}
pub(crate) fn closure_digest_entry(hash: &mut Sha256, entry: &ClosureEntry) -> Result<()> {
    let encoded = serde_json::to_vec(entry)?;
    hash.update((encoded.len() as u64).to_be_bytes());
    hash.update(encoded);
    Ok(())
}

/// A trusted retained-root verifier, never a caller-supplied content assertion.
#[async_trait]
pub(crate) trait VerifiedVersionAuthority: Send + Sync {
    async fn verified_version(&self, content: &SealedObject) -> Result<ObjectVersion>;
}
pub(crate) fn qualified_s3_version(version: &ObjectVersion) -> Result<()> {
    let (_, id) = s3_version_parts(version)?;
    ensure!(
        id.is_some_and(|id| id != "null"),
        "CONDITIONAL_DELETE_UNSUPPORTED"
    );
    Ok(())
}

#[derive(Clone)]
pub struct ClosureSpool {
    id: Uuid,
    directory: Arc<File>,
    object: LogicalObject,
    authority: SpoolAuthority,
    cancel: Cancellation,
}

#[derive(Clone)]
enum SpoolAuthority {
    Producer(Arc<dyn WriteAuthority>),
    ReadCache {
        read: Arc<dyn ReadAuthority>,
        scope: String,
    },
}
impl SpoolAuthority {
    fn scope_key(&self) -> String {
        match self {
            Self::Producer(write) => write.scope_key(),
            Self::ReadCache { scope, .. } => scope.clone(),
        }
    }
    async fn check(&self, object: &LogicalObject, cancel: &Cancellation) -> Result<()> {
        match self {
            Self::Producer(write) => check_write(write.as_ref(), object, cancel).await,
            Self::ReadCache { read, .. } => check_read(read.as_ref(), object, cancel).await,
        }
    }
}

impl ClosureSpool {
    pub async fn create(
        store: &PosixStore,
        object: LogicalObject,
        limits: ClosureLimits,
        authority: Arc<dyn WriteAuthority>,
        cancel: Cancellation,
    ) -> Result<Self> {
        Self::create_private(
            store,
            object,
            limits,
            SpoolAuthority::Producer(authority),
            cancel,
        )
        .await
    }

    /// Read authority permits only private SQLite scratch mutation, never Dataset object writes.
    /// The store is provisioned by the service in its trusted private scratch root.
    pub(crate) async fn create_read_cache(
        store: &PosixStore,
        object: LogicalObject,
        limits: ClosureLimits,
        read: Arc<dyn ReadAuthority>,
        cancel: Cancellation,
    ) -> Result<Self> {
        Self::create_private(
            store,
            object,
            limits,
            SpoolAuthority::ReadCache {
                read,
                scope: format!("read-cache/{}", Uuid::new_v4()),
            },
            cancel,
        )
        .await
    }

    async fn create_private(
        store: &PosixStore,
        object: LogicalObject,
        limits: ClosureLimits,
        authority: SpoolAuthority,
        cancel: Cancellation,
    ) -> Result<Self> {
        store.scope(&object)?;
        ensure!(
            limits.max_entries > 0
                && limits.max_entries <= i64::MAX as u64
                && limits.max_referenced_bytes <= i64::MAX as u64
                && limits.max_spool_bytes >= 64 * 1024
                && limits.max_spool_bytes <= i64::MAX as u64,
            "INVALID_INPUT"
        );
        let scope = authority.scope_key();
        ensure!(!scope.is_empty() && scope.len() <= 512, "INVALID_SCOPE");
        authority.check(&object, &cancel).await?;
        let id = Uuid::new_v4();
        let name = format!("closure-{id}");
        mkdirat(store.staging.as_ref(), &name, Mode::from_raw_mode(0o700))?;
        store.staging.sync_all()?;
        let directory = Arc::new(File::from(openat2(
            store.staging.as_ref(),
            &name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        )?));
        let spool = Self {
            id,
            directory,
            object,
            authority,
            cancel,
        };
        {
            let mut db = spool.database()?;
            db.pragma_update(None, "max_page_count", limits.max_spool_bytes / 4096)?;
            let tx = db.transaction()?;
            tx.execute_batch("CREATE TABLE info(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID; CREATE TABLE entries(name TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;")?;
            for (key, value) in [
                ("version", "1".to_string()),
                ("state", "loading".to_string()),
                ("scope", scope),
                ("object", serde_json::to_string(&spool.object)?),
                ("entries", "0".to_string()),
                ("referenced_bytes", "0".to_string()),
                (
                    "limits",
                    serde_json::to_string(&(limits.max_entries, limits.max_referenced_bytes))?,
                ),
                ("max_spool_bytes", limits.max_spool_bytes.to_string()),
            ] {
                tx.execute(
                    "INSERT INTO info VALUES(?1,?2)",
                    rusqlite::params![key, value],
                )?;
            }
            tx.commit()?;
        }
        spool.directory.sync_all()?;
        spool.authority.check(&spool.object, &spool.cancel).await?;
        Ok(spool)
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub async fn reopen(
        store: &PosixStore,
        id: Uuid,
        object: LogicalObject,
        authority: Arc<dyn WriteAuthority>,
        cancel: Cancellation,
    ) -> Result<Self> {
        store.scope(&object)?;
        check_write(authority.as_ref(), &object, &cancel).await?;
        let directory = Arc::new(File::from(openat2(
            store.staging.as_ref(),
            format!("closure-{id}"),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        )?));
        let index = File::from(openat2(
            directory.as_ref(),
            "index.sqlite",
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        )?);
        ensure!(
            index.metadata()?.is_file() && index.metadata()?.nlink() == 1,
            "UNSAFE_PATH"
        );
        let spool = Self {
            id,
            directory,
            object,
            authority: SpoolAuthority::Producer(authority),
            cancel,
        };
        let db = spool.database()?;
        spool.assert_scope(&db)?;
        Ok(spool)
    }

    pub(crate) fn database(&self) -> Result<rusqlite::Connection> {
        // SQLite rejects proc-fd links with NOFOLLOW; resolve only the held private capability.
        let directory =
            std::fs::read_link(format!("/proc/self/fd/{}", self.directory.as_raw_fd()))?;
        let matches = || -> Result<()> {
            let held = self.directory.metadata()?;
            let current = std::fs::metadata(&directory)?;
            ensure!(
                current.is_dir() && current.dev() == held.dev() && current.ino() == held.ino(),
                "UNSAFE_PATH"
            );
            Ok(())
        };
        matches()?;
        let create = openat2(
            self.directory.as_ref(),
            "index.sqlite",
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o600),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        );
        if let Err(error) = create {
            ensure!(error == rustix::io::Errno::EXIST, "SPOOL_IO_ERROR");
        }
        let index = File::from(openat2(
            self.directory.as_ref(),
            "index.sqlite",
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        )?);
        ensure!(
            index.metadata()?.is_file() && index.metadata()?.nlink() == 1,
            "UNSAFE_PATH"
        );
        let path = directory.join("index.sqlite");
        let db = rusqlite::Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
                | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        matches()?;
        let page_size: u64 = db.pragma_query_value(None, "page_size", |row| row.get(0))?;
        ensure!(page_size == 4096, "INVALID_SPOOL_FORMAT");
        db.busy_timeout(Duration::from_secs(5))?;
        db.pragma_update(None, "journal_mode", "DELETE")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        db.pragma_update(None, "cache_size", -2048)?;
        db.pragma_update(None, "temp_store", "FILE")?;
        let initialized: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='info')",
            [],
            |row| row.get(0),
        )?;
        if initialized {
            let limit: u64 = Self::info(&db, "max_spool_bytes")?.parse()?;
            ensure!(
                limit >= 64 * 1024 && limit <= i64::MAX as u64,
                "INVALID_SPOOL_LIMIT"
            );
            db.pragma_update(None, "max_page_count", limit / 4096)?;
        }
        Ok(db)
    }

    fn info(db: &rusqlite::Connection, key: &str) -> Result<String> {
        Ok(
            db.query_row("SELECT value FROM info WHERE key=?1", [key], |row| {
                row.get(0)
            })?,
        )
    }

    pub(crate) fn assert_scope(&self, db: &rusqlite::Connection) -> Result<()> {
        ensure!(
            Self::info(db, "version")? == "1"
                && Self::info(db, "scope")? == self.authority.scope_key()
                && Self::info(db, "object")? == serde_json::to_string(&self.object)?,
            "SPOOL_SCOPE_CHANGED"
        );
        Ok(())
    }

    pub(crate) async fn check_read_authority(&self, read: &dyn ReadAuthority) -> Result<()> {
        self.authority.check(&self.object, &self.cancel).await?;
        check_read(read, &self.object, &self.cancel).await
    }

    pub(crate) fn sealed_database(&self) -> Result<rusqlite::Connection> {
        let db = self.database()?;
        self.assert_scope(&db)?;
        ensure!(Self::info(&db, "state")? == "sealed", "SPOOL_UNSEALED");
        Ok(db)
    }

    pub(crate) async fn check_current(&self, read: &dyn ReadAuthority) -> Result<()> {
        self.check_read_authority(read).await?;
        self.sealed_database()?;
        Ok(())
    }

    pub(crate) fn lookup(&self, name: &str) -> Result<ClosureEntry> {
        Self::lookup_in(&self.sealed_database()?, name)
    }

    pub(crate) fn lookup_in(db: &rusqlite::Connection, name: &str) -> Result<ClosureEntry> {
        let value: String =
            db.query_row("SELECT value FROM entries WHERE name=?1", [name], |row| {
                row.get(0)
            })?;
        Ok(serde_json::from_str(&value)?)
    }

    pub(crate) fn cancellation(&self) -> Cancellation {
        self.cancel.clone()
    }

    pub async fn append_page(&mut self, page: &[ClosureEntry]) -> Result<()> {
        ensure!(!page.is_empty() && page.len() <= 256, "LIMIT_EXCEEDED");
        for entry in page {
            entry.content.object.validate()?;
            digest(&entry.content.sha256)?;
            let prefix = format!(
                "moria://{}/{}/",
                self.object.warehouse_id, self.object.table_id
            );
            ensure!(
                entry.name.len() <= 512
                    && entry.name.starts_with(&prefix)
                    && entry.name[prefix.len()..]
                        .split('/')
                        .all(|part| !part.is_empty()
                            && part != "."
                            && part != ".."
                            && part.as_bytes()[0].is_ascii_alphanumeric()
                            && part.bytes().all(|b| b.is_ascii_alphanumeric()
                                || b == b'_'
                                || b == b'-'
                                || b == b'.'))
                    && entry.content.object.warehouse_id == self.object.warehouse_id
                    && entry.content.object.table_id == self.object.table_id,
                "BINDING_MISMATCH"
            );
        }
        ensure!(
            serde_json::to_vec(page)?.len() <= 256 * 1024,
            "LIMIT_EXCEEDED"
        );
        self.authority.check(&self.object, &self.cancel).await?;
        {
            let mut db = self.database()?;
            let tx = db.transaction()?;
            self.assert_scope(&tx)?;
            ensure!(Self::info(&tx, "state")? == "loading", "SPOOL_SEALED");
            let (max_entries, max_bytes): (u64, u64) =
                serde_json::from_str(&Self::info(&tx, "limits")?)?;
            let mut entries: u64 = Self::info(&tx, "entries")?.parse()?;
            let mut bytes: u64 = Self::info(&tx, "referenced_bytes")?.parse()?;
            for entry in page {
                entries = entries
                    .checked_add(1)
                    .filter(|n| *n <= max_entries)
                    .ok_or_else(|| anyhow::anyhow!("LIMIT_EXCEEDED"))?;
                bytes = bytes
                    .checked_add(entry.content.size)
                    .filter(|n| *n <= max_bytes)
                    .ok_or_else(|| anyhow::anyhow!("LIMIT_EXCEEDED"))?;
                tx.execute(
                    "INSERT INTO entries VALUES(?1,?2)",
                    rusqlite::params![entry.name, serde_json::to_string(entry)?],
                )?;
            }
            for (key, value) in [
                ("entries", entries.to_string()),
                ("referenced_bytes", bytes.to_string()),
            ] {
                tx.execute(
                    "UPDATE info SET value=?2 WHERE key=?1",
                    rusqlite::params![key, value],
                )?;
            }
            tx.commit()?;
        }
        self.authority.check(&self.object, &self.cancel).await?;
        Ok(())
    }

    fn page(&self, after: Option<&str>) -> Result<Vec<ClosureEntry>> {
        let db = self.database()?;
        self.assert_scope(&db)?;
        let mut statement = db.prepare(
            "SELECT value FROM entries WHERE (?1 IS NULL OR name>?1) ORDER BY name LIMIT 256",
        )?;
        let rows = statement.query_map([after], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub async fn seal(
        &mut self,
        reader: &dyn ObjectReader,
        read_authority: Arc<dyn ReadAuthority>,
    ) -> Result<SealedClosure> {
        self.seal_checked(reader, read_authority, None).await
    }
    /// Reuse hashes only from an already qualified immutable root and exact non-null S3 versions.
    pub(crate) async fn seal_verified(
        &mut self,
        reader: &dyn ObjectReader,
        read_authority: Arc<dyn ReadAuthority>,
        known: &dyn VerifiedVersionAuthority,
    ) -> Result<SealedClosure> {
        self.seal_checked(reader, read_authority, Some(known)).await
    }
    async fn seal_checked(
        &mut self,
        reader: &dyn ObjectReader,
        read_authority: Arc<dyn ReadAuthority>,
        known: Option<&dyn VerifiedVersionAuthority>,
    ) -> Result<SealedClosure> {
        self.authority.check(&self.object, &self.cancel).await?;
        check_read(read_authority.as_ref(), &self.object, &self.cancel).await?;
        {
            let db = self.database()?;
            self.assert_scope(&db)?;
            if Self::info(&db, "state")? == "sealed" {
                return Ok(serde_json::from_str(&Self::info(&db, "receipt")?)?);
            }
        }
        let mut after: Option<String> = None;
        let mut hash = closure_digest_start();
        let mut entries = 0_u64;
        let mut referenced_bytes = 0_u64;
        loop {
            self.authority.check(&self.object, &self.cancel).await?;
            let page = self.page(after.as_deref())?;
            if page.is_empty() {
                break;
            }
            for entry in &page {
                let version = reader
                    .version(&entry.content.object, read_authority.as_ref(), &self.cancel)
                    .await?;
                let size = if let Some(known) = known {
                    let expected = known.verified_version(&entry.content).await?;
                    qualified_s3_version(&expected)?;
                    ensure!(
                        version == expected && version.size == entry.content.size,
                        "STORAGE_VERSION_CHANGED"
                    );
                    entry.content.size
                } else {
                    let mut stream = reader
                        .read_object(
                            entry.content.object.clone(),
                            read_authority.clone(),
                            self.cancel.clone(),
                        )
                        .await?;
                    let mut content_hash = Sha256::new();
                    let mut size = 0_u64;
                    while let Some(frame) = stream.next().await {
                        let frame = frame?;
                        size = size
                            .checked_add(frame.len() as u64)
                            .filter(|n| *n <= entry.content.size)
                            .ok_or_else(|| anyhow::anyhow!("CONTENT_MISMATCH"))?;
                        content_hash.update(frame);
                    }
                    ensure!(
                        size == entry.content.size
                            && format!("sha256:{:x}", content_hash.finalize())
                                == entry.content.sha256,
                        "CONTENT_MISMATCH"
                    );
                    size
                };
                ensure!(
                    reader
                        .version(&entry.content.object, read_authority.as_ref(), &self.cancel)
                        .await?
                        == version,
                    "STORAGE_VERSION_CHANGED"
                );
                RecordingReader::remember_spool(
                    self,
                    &entry.content.object,
                    &version,
                    Some(&entry.content.sha256),
                )?;
                closure_digest_entry(&mut hash, entry)?;
                entries += 1;
                referenced_bytes = referenced_bytes
                    .checked_add(size)
                    .ok_or_else(|| anyhow::anyhow!("LIMIT_EXCEEDED"))?;
            }
            after = Some(page.last().unwrap().name.clone());
        }
        let receipt = SealedClosure {
            version: 1,
            id: self.id,
            object: self.object.clone(),
            entries,
            referenced_bytes,
            sha256: format!("sha256:{:x}", hash.finalize()),
        };
        check_read(read_authority.as_ref(), &self.object, &self.cancel).await?;
        self.authority.check(&self.object, &self.cancel).await?;
        {
            let mut db = self.database()?;
            let tx = db.transaction()?;
            self.assert_scope(&tx)?;
            ensure!(
                Self::info(&tx, "state")? == "loading"
                    && Self::info(&tx, "entries")? == entries.to_string()
                    && Self::info(&tx, "referenced_bytes")? == referenced_bytes.to_string(),
                "SPOOL_CHANGED"
            );
            tx.execute("UPDATE info SET value='sealed' WHERE key='state'", [])?;
            if known.is_some() {
                tx.execute(
                    "INSERT INTO info VALUES('qualified_versions','versioned-qualified-root-v1')",
                    [],
                )?;
            }
            tx.execute(
                "INSERT INTO info VALUES('receipt',?1)",
                [serde_json::to_string(&receipt)?],
            )?;
            tx.commit()?;
        }
        self.directory.sync_all()?;
        self.authority.check(&self.object, &self.cancel).await?;
        Ok(receipt)
    }

    pub async fn read_page(
        &self,
        after: Option<&str>,
        authority: &dyn ReadAuthority,
    ) -> Result<Vec<ClosureEntry>> {
        check_read(authority, &self.object, &self.cancel).await?;
        ensure!(
            Self::info(&self.database()?, "state")? == "sealed",
            "SPOOL_UNSEALED"
        );
        let page = self.page(after)?;
        check_read(authority, &self.object, &self.cancel).await?;
        Ok(page)
    }
}

#[derive(Clone)]
pub(crate) struct RecordingReader {
    pub(crate) inner: Arc<dyn ObjectReader>,
    pub(crate) spool: ClosureSpool,
}
impl Debug for RecordingReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingReader").finish_non_exhaustive()
    }
}
impl RecordingReader {
    pub(crate) fn new(inner: Arc<dyn ObjectReader>, spool: ClosureSpool) -> Result<Self> {
        Self::initialize(&spool)?;
        Ok(Self { inner, spool })
    }
    fn initialize(spool: &ClosureSpool) -> Result<()> {
        spool.database()?.execute_batch("CREATE TABLE IF NOT EXISTS verified_versions(object_id TEXT PRIMARY KEY,version TEXT NOT NULL,actual_hash TEXT) WITHOUT ROWID;")?;
        Ok(())
    }
    fn recorded(&self, object: &LogicalObject) -> Result<Option<(ObjectVersion, Option<String>)>> {
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let row: Option<(String, Option<String>)> = db
            .query_row(
                "SELECT version,actual_hash FROM verified_versions WHERE object_id=?1",
                [&object.object_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        row.map(|(version, hash)| Ok((serde_json::from_str(&version)?, hash)))
            .transpose()
    }
    fn remember(
        &self,
        object: &LogicalObject,
        version: &ObjectVersion,
        hash: Option<&str>,
    ) -> Result<()> {
        Self::remember_spool(&self.spool, object, version, hash)
    }
    fn remember_spool(
        spool: &ClosureSpool,
        object: &LogicalObject,
        version: &ObjectVersion,
        hash: Option<&str>,
    ) -> Result<()> {
        Self::initialize(spool)?;
        let mut db = spool.database()?;
        spool.assert_scope(&db)?;
        let tx = db.transaction()?;
        let old: Option<(String, Option<String>)> = tx
            .query_row(
                "SELECT version,actual_hash FROM verified_versions WHERE object_id=?1",
                [&object.object_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let encoded = serde_json::to_string(version)?;
        if let Some((previous, previous_hash)) = old {
            ensure!(
                previous == encoded
                    && hash.is_none_or(|hash| previous_hash.as_ref().is_none_or(|old| old == hash)),
                "STORAGE_VERSION_CHANGED"
            );
            if let Some(hash) = hash {
                tx.execute(
                    "UPDATE verified_versions SET actual_hash=?2 WHERE object_id=?1",
                    rusqlite::params![object.object_id, hash],
                )?;
            }
        } else {
            tx.execute(
                "INSERT INTO verified_versions VALUES(?1,?2,?3)",
                rusqlite::params![object.object_id, encoded, hash],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    pub(crate) fn verified(&self, content: &SealedObject) -> Result<ObjectVersion> {
        let (version, hash) = self
            .recorded(&content.object)?
            .ok_or_else(|| anyhow::anyhow!("OBJECT_UNVERIFIED"))?;
        ensure!(
            version.size == content.size && hash.as_deref() == Some(content.sha256.as_str()),
            "CONTENT_MISMATCH"
        );
        Ok(version)
    }
    pub(crate) async fn check_versions(
        &self,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<()> {
        let mut after = String::new();
        loop {
            let rows: Vec<(String, String)> = {
                let db = self.spool.database()?;
                self.spool.assert_scope(&db)?;
                let mut statement=db.prepare("SELECT object_id,version FROM verified_versions WHERE object_id>?1 ORDER BY object_id LIMIT 256")?;
                statement
                    .query_map([&after], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect::<rusqlite::Result<_>>()?
            };
            if rows.is_empty() {
                break;
            }
            for (id, encoded) in rows {
                let object = LogicalObject::new(
                    &self.spool.object.warehouse_id,
                    &self.spool.object.table_id,
                    &id,
                )?;
                let version: ObjectVersion = serde_json::from_str(&encoded)?;
                ensure!(
                    self.inner.version(&object, authority, cancel).await? == version,
                    "STORAGE_VERSION_CHANGED"
                );
                after = id;
            }
        }
        authority.check_read(&self.spool.object).await?;
        Ok(())
    }
}
#[async_trait]
impl ObjectReader for RecordingReader {
    fn physical_fingerprint(&self) -> Result<String> {
        self.inner.physical_fingerprint()
    }
    fn chunk_bytes(&self) -> usize {
        self.inner.chunk_bytes()
    }
    async fn version(
        &self,
        object: &LogicalObject,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<ObjectVersion> {
        let version = self.inner.version(object, authority, cancel).await?;
        self.remember(object, &version, None)?;
        Ok(version)
    }
    async fn range_at_version(
        &self,
        object: &LogicalObject,
        range: Range<u64>,
        version: &ObjectVersion,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Bytes> {
        self.remember(object, version, None)?;
        self.inner
            .range_at_version(object, range, version, authority, cancel)
            .await
    }
    async fn range(
        &self,
        object: &LogicalObject,
        range: Range<u64>,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Bytes> {
        let version = self.version(object, authority, cancel).await?;
        self.inner
            .range_at_version(object, range, &version, authority, cancel)
            .await
    }
    async fn read_object(
        &self,
        object: LogicalObject,
        authority: Arc<dyn ReadAuthority>,
        cancel: Cancellation,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        let version = self.version(&object, authority.as_ref(), &cancel).await?;
        let stream = versioned_stream(
            self.inner.clone(),
            object.clone(),
            version.clone(),
            authority.clone(),
            cancel,
        )
        .await?;
        let recorder = self.clone();
        Ok(futures::stream::try_unfold(
            (stream, Sha256::new(), 0u64),
            move |(mut stream, mut hash, mut bytes)| {
                let recorder = recorder.clone();
                let object = object.clone();
                let version = version.clone();
                let authority = authority.clone();
                async move {
                    if let Some(frame) = stream.next().await {
                        let frame = frame?;
                        hash.update(&frame);
                        bytes = bytes
                            .checked_add(frame.len() as u64)
                            .ok_or_else(|| anyhow::anyhow!("LIMIT_EXCEEDED"))?;
                        return Ok(Some((frame, (stream, hash, bytes))));
                    }
                    ensure!(bytes == version.size, "CONTENT_MISMATCH");
                    authority.check_read(&object).await?;
                    recorder.remember(
                        &object,
                        &version,
                        Some(&format!("sha256:{:x}", hash.finalize())),
                    )?;
                    Ok(None)
                }
            },
        )
        .boxed())
    }
}

#[cfg(test)]
mod read_window_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    #[derive(Debug)]
    struct Guard {
        live: AtomicBool,
        checks: AtomicUsize,
    }
    #[async_trait]
    impl ReadAuthority for Guard {
        async fn check_read(&self, _: &LogicalObject) -> Result<()> {
            self.checks.fetch_add(1, Ordering::SeqCst);
            ensure!(self.live.load(Ordering::SeqCst), "REVOKED");
            Ok(())
        }
    }
    #[test]
    fn exact_multipart_discovery_never_returns_sibling_uploads_or_markers() -> Result<()> {
        let key = "prefix/warehouse/table/objects/exact";
        let page=multipart_page(br#"<ListMultipartUploadsResult><IsTruncated>true</IsTruncated><Upload><Key>prefix/warehouse/table/objects/exact</Key><UploadId>owned</UploadId></Upload><Upload><Key>prefix/warehouse/table/objects/exact-sibling</Key><UploadId>foreign</UploadId></Upload><NextKeyMarker>prefix/warehouse/table/objects/exact-sibling</NextKeyMarker><NextUploadIdMarker>foreign</NextUploadIdMarker></ListMultipartUploadsResult>"#,key,None)?;
        assert_eq!(page.upload_ids, vec!["owned"]);
        assert!(page.next_upload_id.is_none());
        assert!(!serde_json::to_string(&page)?.contains("foreign"));
        let bytes=br#"<ListMultipartUploadsResult><IsTruncated>true</IsTruncated><Upload><Key>prefix/warehouse/table/objects/exact</Key><UploadId>owned</UploadId></Upload><NextKeyMarker>prefix/warehouse/table/objects/exact</NextKeyMarker><NextUploadIdMarker>owned</NextUploadIdMarker></ListMultipartUploadsResult>"#;
        assert_eq!(
            multipart_page(bytes, key, None)?.next_upload_id.as_deref(),
            Some("owned")
        );
        assert!(multipart_page(bytes, key, Some("owned")).is_err());
        assert!(multipart_page(br#"<ListMultipartUploadsResult><IsTruncated>false</IsTruncated><Upload><Key>foreign</Key><UploadId>foreign</UploadId></Upload></ListMultipartUploadsResult>"#,key,None).is_err());
        Ok(())
    }
    #[test]
    fn exact_multipart_discovery_accepts_qualified_empty_key_marker_only_for_exact_rows()
    -> Result<()> {
        let key = "prefix/object";
        let exact = "<ListMultipartUploadsResult><IsTruncated>true</IsTruncated><Upload><Key>prefix/object</Key><UploadId>one</UploadId></Upload><NextKeyMarker/><NextUploadIdMarker>one</NextUploadIdMarker></ListMultipartUploadsResult>";
        assert_eq!(
            multipart_page(exact.as_bytes(), key, None)?
                .next_upload_id
                .as_deref(),
            Some("one")
        );
        let sibling = exact.replace(
            "<Key>prefix/object</Key>",
            "<Key>prefix/object-sibling</Key>",
        );
        assert!(multipart_page(sibling.as_bytes(), key, None).is_err());
        let wrong = exact.replace(
            "<NextUploadIdMarker>one</NextUploadIdMarker>",
            "<NextUploadIdMarker>foreign</NextUploadIdMarker>",
        );
        assert!(multipart_page(wrong.as_bytes(), key, None).is_err());
        Ok(())
    }
    #[tokio::test]
    async fn private_window_is_bounded_version_pinned_and_never_authorizes_result_release()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = Arc::new(PosixStore::provision(
            root.path(),
            "warehouse",
            "table",
            StorageLimits {
                max_object_bytes: 4 * 1024 * 1024,
                chunk_bytes: 1024 * 1024,
            },
        )?);
        let object = LogicalObject::new("warehouse", "table", "object")?;
        let path = root.path().join("warehouse/table/objects/object");
        let data = (0..2 * 1024 * 1024)
            .map(|n| (n % 251) as u8)
            .collect::<Vec<_>>();
        std::fs::write(&path, &data)?;
        let guard = Arc::new(Guard {
            live: AtomicBool::new(true),
            checks: AtomicUsize::new(0),
        });
        let cancel = Cancellation::default();
        let mut window =
            ObjectReadWindow::new(object.clone(), store.clone(), guard.clone(), cancel.clone());
        for n in 0..1024 {
            assert_eq!(window.read(n..n + 1).await?[0], data[n as usize]);
        }
        assert!(guard.checks.load(Ordering::SeqCst) < 32);
        assert_eq!(window.bytes.len(), 1024 * 1024);
        guard.live.store(false, Ordering::SeqCst);
        // An already authorized private slice may decode, but sealing and independent reader operations recheck authority.
        assert_eq!(window.read(1024..1025).await?[0], data[1024]);
        assert!(window.check_current().await.is_err());
        let mut independent = ObjectReadWindow::new(
            object.clone(),
            store.clone(),
            guard.clone(),
            Cancellation::default(),
        );
        assert!(independent.read(0..1).await.is_err());
        cancel.cancel();
        assert!(window.read(1025..1026).await.is_err());
        guard.live.store(true, Ordering::SeqCst);
        let mut window = ObjectReadWindow::new(object, store, guard, Cancellation::default());
        window.read(0..1).await?;
        std::fs::write(&path, b"changed")?;
        assert!(window.check_current().await.is_err());
        assert!(window.read(1024 * 1024..1024 * 1024 + 1).await.is_err());
        Ok(())
    }
}
