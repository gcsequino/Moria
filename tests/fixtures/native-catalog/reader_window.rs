use super::*;
use moria_runtime::storage;
use std::sync::atomic::{AtomicUsize, Ordering};
#[derive(Debug)]
struct Counted {
    inner: Arc<dyn ObjectStore>,
    versions: AtomicUsize,
    ranges: AtomicUsize,
    bytes: AtomicUsize,
}
#[async_trait::async_trait]
impl ObjectReader for Counted {
    fn chunk_bytes(&self) -> usize {
        self.inner.chunk_bytes()
    }
    fn physical_fingerprint(&self) -> Result<String> {
        self.inner.physical_fingerprint()
    }
    async fn version(
        &self,
        o: &LogicalObject,
        a: &dyn ReadAuthority,
        c: &Cancellation,
    ) -> Result<ObjectVersion> {
        self.versions.fetch_add(1, Ordering::SeqCst);
        self.inner.version(o, a, c).await
    }
    async fn range_at_version(
        &self,
        o: &LogicalObject,
        r: std::ops::Range<u64>,
        v: &ObjectVersion,
        a: &dyn ReadAuthority,
        c: &Cancellation,
    ) -> Result<bytes::Bytes> {
        self.ranges.fetch_add(1, Ordering::SeqCst);
        self.bytes
            .fetch_add((r.end - r.start) as usize, Ordering::SeqCst);
        self.inner.range_at_version(o, r, v, a, c).await
    }
    async fn range(
        &self,
        o: &LogicalObject,
        r: std::ops::Range<u64>,
        a: &dyn ReadAuthority,
        c: &Cancellation,
    ) -> Result<bytes::Bytes> {
        let v = self.version(o, a, c).await?;
        self.range_at_version(o, r, &v, a, c).await
    }
    async fn read_object(
        &self,
        o: LogicalObject,
        a: Arc<dyn ReadAuthority>,
        c: Cancellation,
    ) -> Result<futures::stream::BoxStream<'static, Result<bytes::Bytes>>> {
        self.inner.read_object(o, a, c).await
    }
}
impl Counted {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            versions: AtomicUsize::new(0),
            ranges: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
        }
    }
    fn counts(&self) -> Value {
        json!({"providerVersions":self.versions.load(Ordering::SeqCst),"providerRanges":self.ranges.load(Ordering::SeqCst),"providerBytes":self.bytes.load(Ordering::SeqCst)})
    }
}
async fn flush(db: &Database) -> Result<()> {
    ensure!(
        std::env::var("MORIA_DATABASE_POOL_MAX").as_deref() == Ok("4"),
        "FIXTURE_POOL_SIZE_REQUIRED"
    );
    // One of the four slots is the schema lease; flush every IO backend before resetting global counters.
    let mut clients = Vec::new();
    for _ in 0..3 {
        clients.push(db.get().await?);
    }
    for client in &clients {
        client
            .query_one("SELECT pg_stat_force_next_flush()", &[])
            .await?;
    }
    drop(clients);
    Ok(())
}

pub async fn probe(
    db: &Database,
    store: Arc<dyn ObjectStore>,
    grant: Arc<DatasetIoGrant>,
    spool: &ClosureSpool,
    metadata: SealedObject,
    source: &Snapshot,
    cancel: Cancellation,
    execution: Uuid,
) -> Result<()> {
    let content = spool
        .read_page(None, grant.as_ref())
        .await?
        .into_iter()
        .max_by_key(|entry| entry.content.size)
        .ok_or_else(|| anyhow::anyhow!("FIXTURE_OBJECT_MISSING"))?
        .content;
    let count = 128u64.min(content.size);
    ensure!(count >= 128, "FIXTURE_OBJECT_TOO_SMALL");
    flush(db).await?;
    event(json!({"event":"reader_window_ready","execution":execution,"decoderReads":count}));
    proceed();
    let baseline = Counted::new(store.clone());
    let started = std::time::Instant::now();
    let mut expected = Vec::new();
    for position in 0..count {
        expected.push(
            baseline
                .range(
                    &content.object,
                    position..position + 1,
                    grant.as_ref(),
                    &cancel,
                )
                .await?[0],
        );
    }
    flush(db).await?;
    event(
        json!({"event":"reader_window_baseline","counts":baseline.counts(),"elapsedMillis":started.elapsed().as_millis().to_string()}),
    );
    proceed();
    let reader = Arc::new(Counted::new(store.clone()));
    let mut window = ObjectReadWindow::new(
        content.object.clone(),
        reader.clone(),
        grant.clone(),
        cancel.clone(),
    );
    let started = std::time::Instant::now();
    for position in 0..count / 2 {
        ensure!(window.read(position..position + 1).await?[0] == expected[position as usize]);
    }
    flush(db).await?;
    event(
        json!({"event":"reader_window_buffered","counts":reader.counts(),"elapsedMillis":started.elapsed().as_millis().to_string(),"privateWindowBytesMaximum":1048576}),
    );
    proceed();
    // Buffered bytes are private decoder input; current authority is still mandatory before any proof or result release.
    let buffered = window.read(count / 2..count / 2 + 1).await;
    ensure!(
        window.check_current().await.is_err(),
        "REVOKED_WINDOW_FINISHED"
    );
    ensure!(
        prepare_snapshot(
            store,
            grant,
            spool,
            SnapshotPin {
                metadata,
                table_uuid: source.table_uuid,
                snapshot_id: source.snapshot_id.parse()?
            },
            cancel
        )
        .await
        .is_err(),
        "REVOKED_PROOF_SEALED"
    );
    let candidates: i64 = db
        .get()
        .await?
        .query_one(
            "SELECT count(*) FROM moria.runtime_snapshot_candidates WHERE execution=$1",
            &[&execution],
        )
        .await?
        .get(0);
    ensure!(candidates == 0, "REVOKED_PROOF_REGISTERED");
    event(
        json!({"event":"reader_window_denied","bufferedPrivateRead":buffered.is_ok(),"candidateCount":candidates,"counts":reader.counts(),"execution":execution}),
    );
    Ok(())
}

#[path = "../../../services/runtime/src/snapshot_parquet_bounds.rs"]
mod bounds;
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HeaderProbe {
    build: Uuid,
    build_owner: Uuid,
    build_fence: i64,
    verifier_key: String,
    verifier_owner: Uuid,
    verifier_fence: i64,
}
#[derive(Debug)]
struct HeaderAuthority {
    source: IndexNamespaceReadAuthority,
    key: String,
    owner: Uuid,
    fence: i64,
    source_checks: AtomicUsize,
    verifier_checks: AtomicUsize,
}
#[async_trait::async_trait]
impl ReadAuthority for HeaderAuthority {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        self.source.check_read(object).await?;
        self.source_checks.fetch_add(1, Ordering::SeqCst);
        self.source
            .scope
            .db
            .get()
            .await?
            .query_one(
                "SELECT moria.runtime_snapshot_verifier_check($1,$2,$3)",
                &[&self.key, &self.owner, &self.fence],
            )
            .await?;
        self.verifier_checks.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl HeaderAuthority {
    fn counts(&self) -> (usize, usize) {
        (
            self.source_checks.load(Ordering::SeqCst),
            self.verifier_checks.load(Ordering::SeqCst),
        )
    }
}
pub async fn headers(db: &Database, fixture: &Fixture, input: &HeaderProbe) -> Result<()> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let source = &fixture.snapshots[0];
    let namespace = IndexNamespace {
        warehouse_id: source.warehouse_id.clone(),
        table_id: source.table_id.clone(),
        revision: source.revision.clone().unwrap_or_else(|| "v1".into()),
    };
    let authority = Arc::new(HeaderAuthority {
        source: IndexNamespaceReadAuthority {
            scope: moria_runtime::io::Scope {
                db: db.clone(),
                build: input.build,
                task: None,
                owner: input.build_owner,
                fence: input.build_fence,
            },
            namespace: namespace.clone(),
        },
        key: input.verifier_key.clone(),
        owner: input.verifier_owner,
        fence: input.verifier_fence,
        source_checks: AtomicUsize::new(0),
        verifier_checks: AtomicUsize::new(0),
    });
    let store = storage::configured_store_with_fingerprint(&namespace)?.0;
    let cancel = Cancellation::default();
    let reference = NativeSnapshotRef::decode(fixture.native_ref.as_deref().unwrap())?;
    let root = snapshot_root(
        store.as_ref(),
        authority.clone(),
        &reference,
        cancel.clone(),
    )
    .await?;
    let mut page = root.last_page.clone();
    let mut content = None;
    for ordinal in (0..root.pages).rev() {
        let found = snapshot_page(
            store.as_ref(),
            authority.clone(),
            &page.take().unwrap(),
            ordinal,
            cancel.clone(),
        )
        .await?;
        page = found.previous;
        for entry in found.entries {
            if entry.name.ends_with(".parquet") {
                ensure!(content.is_none(), "FIXTURE_ONE_DATA_FILE_REQUIRED");
                content = Some(entry.content);
            }
        }
    }
    let content = content.ok_or_else(|| anyhow::anyhow!("FIXTURE_OBJECT_MISSING"))?;
    ensure!(content.size <= 8 * 1024 * 1024, "FIXTURE_OBJECT_LIMIT");
    let mut stream = store
        .read_object(content.object.clone(), authority.clone(), cancel.clone())
        .await?;
    let mut encoded = Vec::new();
    while let Some(bytes) = stream.try_next().await? {
        encoded.extend_from_slice(&bytes);
    }
    ensure!(
        encoded.len() as u64 == content.size
            && format!("sha256:{:x}", Sha256::digest(&encoded)) == content.sha256,
        "CONTENT_MISMATCH"
    );
    ensure!(encoded.len() >= 12, "PARQUET_ALLOCATION_LIMIT");
    let footer =
        u32::from_le_bytes(encoded[encoded.len() - 8..encoded.len() - 4].try_into()?) as usize;
    ensure!(footer <= encoded.len() - 12, "PARQUET_ALLOCATION_LIMIT");
    bounds::footer(&encoded[encoded.len() - 8 - footer..encoded.len() - 8])?;
    let file = SerializedFileReader::new(bytes::Bytes::from(encoded))?;
    bounds::arrow_metadata(file.metadata())?;
    ensure!(
        file.metadata().num_row_groups() >= 2 && file.metadata().row_group(0).num_columns() >= 4,
        "FIXTURE_MULTICOLUMN_GROUPS_REQUIRED"
    );
    let chunks = bounds::metadata(file.metadata(), content.size)?;
    let before = authority.counts();
    let baseline = Counted::new(store.clone());
    let mut headers = 0usize;
    for chunk in &chunks {
        let mut walk = bounds::PageWalk::new();
        while walk.position() < chunk.size {
            let start = chunk.offset + walk.position();
            let bytes = baseline
                .range(
                    &content.object,
                    start..(start + 65536).min(chunk.offset + chunk.size),
                    authority.as_ref(),
                    &cancel,
                )
                .await?;
            walk.header(&bytes, chunk)?;
            headers += 1;
        }
        walk.finish(chunk)?;
    }
    let after = authority.counts();
    let baseline_guards = (after.0 - before.0, after.1 - before.1);
    let reader = Arc::new(Counted::new(store));
    let mut window = ObjectReadWindow::new(
        content.object.clone(),
        reader.clone(),
        authority.clone(),
        cancel,
    );
    let before = authority.counts();
    for chunk in &chunks {
        let mut walk = bounds::PageWalk::new();
        while walk.position() < chunk.size {
            let start = chunk.offset + walk.position();
            let bytes = window
                .read(start..(start + 65536).min(chunk.offset + chunk.size))
                .await?;
            walk.header(&bytes, chunk)?;
        }
        walk.finish(chunk)?;
    }
    window.check_current().await?;
    let after = authority.counts();
    let window_guards = (after.0 - before.0, after.1 - before.1);
    ensure!(
        headers >= 8
            && reader.ranges.load(Ordering::SeqCst) * 2 < baseline.ranges.load(Ordering::SeqCst)
            && window_guards.0 < baseline_guards.0
            && window_guards.1 < baseline_guards.1,
        "HEADER_WINDOW_DID_NOT_BOUND_IO"
    );
    event(
        json!({"event":"qualified_header_counts","rowGroups":file.metadata().num_row_groups(),"columns":file.metadata().row_group(0).num_columns(),"headers":headers,"baseline":baseline.counts(),"window":reader.counts(),"actualBaselineGuards":{"nativeStorage":baseline_guards.0,"verifier":baseline_guards.1},"actualWindowGuards":{"nativeStorage":window_guards.0,"verifier":window_guards.1}}),
    );
    Ok(())
}
