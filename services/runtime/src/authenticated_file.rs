//! Bounded authenticated native bytes. Certificates prove bytes only; Catalog separately qualifies the logical reader/layout profile and grants current read authority.
use crate::{
    catalog::DatasetIoGrant,
    storage::{
        Cancellation, LogicalObject, ObjectReader, ObjectStore, ObjectVersion, ReadAuthority,
        SealedObject, versioned_stream,
    },
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, stream::BoxStream};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{ops::Range, sync::Arc};
use uuid::Uuid;

const CHUNK_BYTES: u64 = 1024 * 1024;
const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;
const PROFILE: &str = "moria-sha256-chunks-1m-v1";
const INDEX_CHUNK_BYTES: u64 = 64 * 1024;
const INDEX_PROFILE: &str = "moria-sha256-chunks-64k-v1";
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileCertificate {
    version: u32,
    reader_profile: String,
    content: SealedObject,
    chunk_bytes: u64,
    chunks: Vec<String>,
}
impl FileCertificate {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 1
                && ((self.reader_profile == PROFILE && self.chunk_bytes == CHUNK_BYTES)
                    || (self.reader_profile == INDEX_PROFILE
                        && self.chunk_bytes == INDEX_CHUNK_BYTES))
                && self.content.size > 0
                && self.content.size <= MAX_FILE_BYTES
                && self.chunks.len() as u64 == self.content.size.div_ceil(self.chunk_bytes),
            "INVALID_NATIVE_FILE_CERTIFICATE"
        );
        for digest in self.chunks.iter().chain([&self.content.sha256]) {
            ensure!(
                digest.len() == 71
                    && digest.starts_with("sha256:")
                    && digest[7..]
                        .bytes()
                        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
                "INVALID_NATIVE_FILE_CERTIFICATE"
            );
        }
        Ok(())
    }
    pub fn content(&self) -> &SealedObject {
        &self.content
    }
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }
}
/// Created only after real producer IO or the private receiver/recovery qualification path.
#[derive(Clone, Debug)]
pub struct QualifiedFile {
    certificate: FileCertificate,
    physical_version: ObjectVersion,
    physical_fingerprint: String,
    incarnation: Uuid,
}
impl QualifiedFile {
    pub(crate) fn from_graph_proof(
        proof: crate::dataset_graph_read::GraphCertificateProof,
    ) -> Result<Self> {
        let (certificate, physical_version, physical_fingerprint, incarnation) = proof.into_parts();
        certificate.validate()?;
        ensure!(
            physical_version.size == certificate.content.size,
            "CONTENT_MISMATCH"
        );
        Ok(Self {
            certificate,
            physical_version,
            physical_fingerprint,
            incarnation,
        })
    }
    pub(crate) fn from_producer_graph(
        proof: crate::dataset_graph::ProducerFileProof,
    ) -> Result<Self> {
        let (certificate, physical_version, physical_fingerprint, incarnation) = proof.into_parts();
        certificate.validate()?;
        ensure!(
            physical_version.size == certificate.content.size,
            "CONTENT_MISMATCH"
        );
        Ok(Self {
            certificate,
            physical_version,
            physical_fingerprint,
            incarnation,
        })
    }
    pub fn certificate(&self) -> &FileCertificate {
        &self.certificate
    }
    pub fn physical_version(&self) -> &ObjectVersion {
        &self.physical_version
    }
    pub fn physical_fingerprint(&self) -> &str {
        &self.physical_fingerprint
    }
    pub fn incarnation(&self) -> Uuid {
        self.incarnation
    }
}

/// Qualify only an exact file issued by this real current producer. Bootstrap/import qualification
/// has a separate full-root verification path and cannot manufacture native write ownership.
pub async fn certify_new_file(
    store: Arc<dyn ObjectStore>,
    grant: Arc<DatasetIoGrant>,
    content: SealedObject,
    cancel: Cancellation,
) -> Result<QualifiedFile> {
    certify_file_profile(store, grant, content, cancel, PROFILE, CHUNK_BYTES).await
}
pub(crate) async fn certify_new_index_file(
    store: Arc<dyn ObjectStore>,
    grant: Arc<DatasetIoGrant>,
    content: SealedObject,
    cancel: Cancellation,
) -> Result<QualifiedFile> {
    certify_file_profile(
        store,
        grant,
        content,
        cancel,
        INDEX_PROFILE,
        INDEX_CHUNK_BYTES,
    )
    .await
}
async fn certify_file_profile(
    store: Arc<dyn ObjectStore>,
    grant: Arc<DatasetIoGrant>,
    content: SealedObject,
    cancel: Cancellation,
    profile: &str,
    chunk_bytes: u64,
) -> Result<QualifiedFile> {
    grant.check_current().await?;
    grant.validate_store(store.as_ref())?;
    cancel.check()?;
    ensure!(
        content.size > 0 && content.size <= MAX_FILE_BYTES,
        "DATASET_WRITE_CAPABILITY_REQUIRED"
    );
    let namespace = grant.namespace();
    ensure!(
        content.object.warehouse_id == namespace.warehouse_id
            && content.object.table_id == namespace.table_id,
        "BINDING_MISMATCH"
    );
    let physical = store.physical_fingerprint()?;
    let lease = grant.lease();
    let owned:bool=grant.database().get().await?.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_issued_objects i JOIN moria.runtime_dataset_producers p ON p.scope=i.producer WHERE i.physical_fingerprint=$1 AND i.warehouse_id=$2 AND i.table_id=$3 AND i.object_id=$4 AND i.expected_size=$5 AND i.expected_sha256=$6 AND p.scope=moria.runtime_dataset_producer_scope($7,$8,$9,$10,$11,$2,$3,$12) AND p.tombstoned_at IS NULL)",&[&physical,&namespace.warehouse_id,&namespace.table_id,&content.object.object_id,&i64::try_from(content.size)?,&content.sha256,&lease.execution,&lease.task,&lease.owner,&lease.fence.parse::<i64>()?,&lease.incarnation,&namespace.revision]).await?.get(0);
    ensure!(owned, "DATASET_PUBLICATION_CONTRACT_REQUIRED");
    let physical_version = store
        .version(&content.object, grant.as_ref(), &cancel)
        .await?;
    ensure!(physical_version.size == content.size, "CONTENT_MISMATCH");
    certify_versioned_bytes(
        store,
        grant,
        content,
        physical_version,
        cancel,
        profile,
        chunk_bytes,
    )
    .await
}
async fn certify_versioned_bytes(
    store: Arc<dyn ObjectStore>,
    grant: Arc<DatasetIoGrant>,
    content: SealedObject,
    physical_version: ObjectVersion,
    cancel: Cancellation,
    profile: &str,
    chunk_bytes: u64,
) -> Result<QualifiedFile> {
    let physical = store.physical_fingerprint()?;
    let incarnation = grant.lease().incarnation;
    let mut stream = versioned_stream(
        store,
        content.object.clone(),
        physical_version.clone(),
        grant.clone(),
        cancel.clone(),
    )
    .await?;
    let (mut whole, mut chunk, mut used, mut total, mut chunks) =
        (Sha256::new(), Sha256::new(), 0u64, 0u64, Vec::new());
    while let Some(frame) = stream.next().await {
        let frame = frame?;
        cancel.check()?;
        total = total
            .checked_add(frame.len() as u64)
            .filter(|total| *total <= content.size)
            .ok_or_else(|| anyhow::anyhow!("CONTENT_MISMATCH"))?;
        whole.update(&frame);
        let mut at = 0usize;
        while at < frame.len() {
            let take = ((chunk_bytes - used) as usize).min(frame.len() - at);
            chunk.update(&frame[at..at + take]);
            used += take as u64;
            at += take;
            if used == chunk_bytes {
                chunks.push(format!("sha256:{:x}", chunk.finalize_reset()));
                used = 0;
            }
        }
    }
    if used > 0 {
        chunks.push(format!("sha256:{:x}", chunk.finalize()));
    }
    ensure!(
        total == content.size && format!("sha256:{:x}", whole.finalize()) == content.sha256,
        "CONTENT_MISMATCH"
    );
    grant.check_current().await?;
    let certificate = FileCertificate {
        version: 1,
        reader_profile: profile.into(),
        content,
        chunk_bytes,
        chunks,
    };
    certificate.validate()?;
    Ok(QualifiedFile {
        certificate,
        physical_version,
        physical_fingerprint: physical,
        incarnation,
    })
}

pub(crate) async fn certify_retained_member(
    store: Arc<dyn ObjectStore>,
    grant: Arc<DatasetIoGrant>,
    content: SealedObject,
    claims: &serde_json::Value,
    cancel: Cancellation,
) -> Result<QualifiedFile> {
    grant.check_current().await?;
    grant.validate_store(store.as_ref())?;
    ensure!(
        content.size > 0 && content.size <= MAX_FILE_BYTES,
        "DATASET_WRITE_CAPABILITY_REQUIRED"
    );
    let wanted = serde_json::to_value(&content)?;
    let state: serde_json::Value = grant
        .database()
        .get()
        .await?
        .query_one(
            "SELECT moria.runtime_dataset_graph_bootstrap_member($1,$2)",
            &[claims, &wanted],
        )
        .await?
        .get(0);
    ensure!(
        state["content"] == wanted
            && state["physicalFingerprint"] == store.physical_fingerprint()?
            && state["incarnation"] == grant.lease().incarnation.to_string(),
        "DATASET_GRAPH_ANCESTRY_REQUIRED"
    );
    let version: ObjectVersion = serde_json::from_value(state["physicalVersion"].clone())?;
    ensure!(version.size == content.size, "CONTENT_MISMATCH");
    let file = certify_versioned_bytes(
        store,
        grant.clone(),
        content,
        version,
        cancel,
        PROFILE,
        CHUNK_BYTES,
    )
    .await?;
    let current: serde_json::Value = grant
        .database()
        .get()
        .await?
        .query_one(
            "SELECT moria.runtime_dataset_graph_bootstrap_member($1,$2)",
            &[claims, &wanted],
        )
        .await?
        .get(0);
    ensure!(current == state, "DATASET_GRAPH_ANCESTRY_REQUIRED");
    Ok(file)
}

/// Stateless across external calls. The enclosing decoder may use its own bounded per-operation
/// window; every provider fill here checks current authority and exact certified bytes.
#[derive(Clone, Debug)]
pub struct AuthenticatedFileReader {
    inner: Arc<dyn ObjectReader>,
    file: QualifiedFile,
}
impl AuthenticatedFileReader {
    pub fn from_qualified_file(inner: Arc<dyn ObjectReader>, file: QualifiedFile) -> Result<Self> {
        file.certificate.validate()?;
        ensure!(
            inner.physical_fingerprint()? == file.physical_fingerprint
                && file.physical_version.size == file.certificate.content.size,
            "BINDING_MISMATCH"
        );
        Ok(Self { inner, file })
    }
    async fn current(
        &self,
        object: &LogicalObject,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<()> {
        cancel.check()?;
        ensure!(
            object == &self.file.certificate.content.object
                && self.inner.physical_fingerprint()? == self.file.physical_fingerprint,
            "BINDING_MISMATCH"
        );
        authority.check_read(object).await?;
        cancel.check()
    }
}
#[async_trait]
impl ObjectReader for AuthenticatedFileReader {
    fn physical_fingerprint(&self) -> Result<String> {
        self.inner.physical_fingerprint()
    }
    fn chunk_bytes(&self) -> usize {
        self.file.certificate.chunk_bytes as usize
    }
    async fn version(
        &self,
        object: &LogicalObject,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<ObjectVersion> {
        self.current(object, authority, cancel).await?;
        Ok(self.file.physical_version.clone())
    }
    async fn range_at_version(
        &self,
        object: &LogicalObject,
        range: Range<u64>,
        version: &ObjectVersion,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Bytes> {
        self.current(object, authority, cancel).await?;
        ensure!(
            version == &self.file.physical_version
                && range.start <= range.end
                && range.end <= version.size
                && range.end - range.start <= MAX_FILE_BYTES,
            "CONTENT_MISMATCH"
        );
        let mut result = BytesMut::with_capacity((range.end - range.start) as usize);
        if range.start < range.end {
            for index in range.start / self.file.certificate.chunk_bytes
                ..range.end.div_ceil(self.file.certificate.chunk_bytes)
            {
                let start = index * self.file.certificate.chunk_bytes;
                let end = (start + self.file.certificate.chunk_bytes).min(version.size);
                let chunk = self
                    .inner
                    .range_at_version(object, start..end, version, authority, cancel)
                    .await?;
                ensure!(
                    chunk.len() as u64 == end - start
                        && format!("sha256:{:x}", Sha256::digest(&chunk))
                            == self.file.certificate.chunks[index as usize],
                    "CONTENT_MISMATCH"
                );
                let from = (range.start.max(start) - start) as usize;
                let to = (range.end.min(end) - start) as usize;
                result.extend_from_slice(&chunk[from..to]);
            }
        }
        self.current(object, authority, cancel).await?;
        Ok(result.freeze())
    }
    async fn range(
        &self,
        object: &LogicalObject,
        range: Range<u64>,
        authority: &dyn ReadAuthority,
        cancel: &Cancellation,
    ) -> Result<Bytes> {
        self.range_at_version(
            object,
            range,
            &self.file.physical_version,
            authority,
            cancel,
        )
        .await
    }
    async fn read_object(
        &self,
        object: LogicalObject,
        authority: Arc<dyn ReadAuthority>,
        cancel: Cancellation,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        self.current(&object, authority.as_ref(), &cancel).await?;
        versioned_stream(
            Arc::new(self.clone()),
            object,
            self.file.physical_version.clone(),
            authority,
            cancel,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };
    #[derive(Debug, Default)]
    struct Gate(AtomicBool);
    #[async_trait]
    impl ReadAuthority for Gate {
        async fn check_read(&self, _: &LogicalObject) -> Result<()> {
            ensure!(!self.0.load(Ordering::Relaxed), "FORBIDDEN");
            Ok(())
        }
    }
    #[derive(Debug)]
    struct Memory {
        data: Bytes,
        version: ObjectVersion,
        corrupt: AtomicBool,
        ranges: Mutex<Vec<Range<u64>>>,
    }
    #[async_trait]
    impl ObjectReader for Memory {
        fn physical_fingerprint(&self) -> Result<String> {
            Ok("test-physical".into())
        }
        fn chunk_bytes(&self) -> usize {
            CHUNK_BYTES as usize
        }
        async fn version(
            &self,
            _: &LogicalObject,
            _: &dyn ReadAuthority,
            _: &Cancellation,
        ) -> Result<ObjectVersion> {
            Ok(self.version.clone())
        }
        async fn range_at_version(
            &self,
            object: &LogicalObject,
            range: Range<u64>,
            version: &ObjectVersion,
            authority: &dyn ReadAuthority,
            cancel: &Cancellation,
        ) -> Result<Bytes> {
            authority.check_read(object).await?;
            cancel.check()?;
            ensure!(version == &self.version, "CONTENT_MISMATCH");
            self.ranges.lock().unwrap().push(range.clone());
            let mut data = self
                .data
                .slice(range.start as usize..range.end as usize)
                .to_vec();
            if self.corrupt.load(Ordering::Relaxed) && !data.is_empty() {
                data[0] ^= 1;
            }
            Ok(Bytes::from(data))
        }
        async fn range(
            &self,
            object: &LogicalObject,
            range: Range<u64>,
            authority: &dyn ReadAuthority,
            cancel: &Cancellation,
        ) -> Result<Bytes> {
            self.range_at_version(object, range, &self.version, authority, cancel)
                .await
        }
        async fn read_object(
            &self,
            _: LogicalObject,
            _: Arc<dyn ReadAuthority>,
            _: Cancellation,
        ) -> Result<BoxStream<'static, Result<Bytes>>> {
            anyhow::bail!("UNUSED_TEST_OPERATION")
        }
    }
    fn fixture() -> (Arc<Memory>, QualifiedFile) {
        let data = Bytes::from(
            (0..CHUNK_BYTES as usize * 2 + 13)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<_>>(),
        );
        let version = ObjectVersion {
            size: data.len() as u64,
            token: "fixture-version".into(),
        };
        let content = SealedObject {
            object: LogicalObject::new("warehouse", "table", &Uuid::new_v4().to_string()).unwrap(),
            size: version.size,
            sha256: format!("sha256:{:x}", Sha256::digest(&data)),
        };
        let certificate = FileCertificate {
            version: 1,
            reader_profile: PROFILE.into(),
            content,
            chunk_bytes: CHUNK_BYTES,
            chunks: data
                .chunks(CHUNK_BYTES as usize)
                .map(|chunk| format!("sha256:{:x}", Sha256::digest(chunk)))
                .collect(),
        };
        (
            Arc::new(Memory {
                data,
                version: version.clone(),
                corrupt: AtomicBool::new(false),
                ranges: Mutex::new(vec![]),
            }),
            QualifiedFile {
                certificate,
                physical_version: version,
                physical_fingerprint: "test-physical".into(),
                incarnation: Uuid::new_v4(),
            },
        )
    }
    #[tokio::test]
    async fn partial_reads_authenticate_complete_bounded_chunks() {
        let (memory, file) = fixture();
        let object = file.certificate.content.object.clone();
        let reader = AuthenticatedFileReader::from_qualified_file(memory.clone(), file).unwrap();
        let gate = Gate::default();
        let cancel = Cancellation::default();
        let range = CHUNK_BYTES - 7..CHUNK_BYTES + 9;
        assert_eq!(
            reader
                .range(&object, range.clone(), &gate, &cancel)
                .await
                .unwrap(),
            memory.data.slice(range.start as usize..range.end as usize)
        );
        assert_eq!(
            *memory.ranges.lock().unwrap(),
            vec![0..CHUNK_BYTES, CHUNK_BYTES..CHUNK_BYTES * 2]
        );
        memory.corrupt.store(true, Ordering::Relaxed);
        assert!(
            reader
                .range(&object, 500..501, &gate, &cancel)
                .await
                .is_err()
        );
        memory.corrupt.store(false, Ordering::Relaxed);
        gate.0.store(true, Ordering::Relaxed);
        assert!(
            reader
                .range(&object, 500..501, &gate, &cancel)
                .await
                .is_err()
        );
        gate.0.store(false, Ordering::Relaxed);
        let mut wrong = reader.file.physical_version.clone();
        wrong.token = "other-version".into();
        assert!(
            reader
                .range_at_version(&object, 0..1, &wrong, &gate, &cancel)
                .await
                .is_err()
        );
        cancel.cancel();
        assert!(reader.range(&object, 0..1, &gate, &cancel).await.is_err());
    }
    #[test]
    fn malformed_certificate_and_physical_binding_rejected() {
        let (memory, mut file) = fixture();
        file.certificate.chunks.pop();
        assert!(AuthenticatedFileReader::from_qualified_file(memory.clone(), file).is_err());
        let (_, mut file) = fixture();
        file.physical_fingerprint = "other".into();
        assert!(AuthenticatedFileReader::from_qualified_file(memory, file).is_err());
    }
    #[tokio::test]
    async fn index_profile_authenticates_only_bounded_sixty_four_kib_chunks() {
        let (memory, mut file) = fixture();
        file.certificate.reader_profile = INDEX_PROFILE.into();
        file.certificate.chunk_bytes = INDEX_CHUNK_BYTES;
        file.certificate.chunks = memory
            .data
            .chunks(INDEX_CHUNK_BYTES as usize)
            .map(|bytes| format!("sha256:{:x}", Sha256::digest(bytes)))
            .collect();
        let object = file.certificate.content.object.clone();
        let reader = AuthenticatedFileReader::from_qualified_file(memory.clone(), file).unwrap();
        let range = INDEX_CHUNK_BYTES - 7..INDEX_CHUNK_BYTES + 9;
        assert_eq!(
            reader
                .range(
                    &object,
                    range.clone(),
                    &Gate::default(),
                    &Cancellation::default()
                )
                .await
                .unwrap(),
            memory.data.slice(range.start as usize..range.end as usize)
        );
        assert_eq!(
            *memory.ranges.lock().unwrap(),
            vec![
                0..INDEX_CHUNK_BYTES,
                INDEX_CHUNK_BYTES..INDEX_CHUNK_BYTES * 2
            ]
        );
    }
}
