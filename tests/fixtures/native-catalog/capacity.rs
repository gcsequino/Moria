use super::*;
use std::io::{BufReader, Read};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Allocation {
    domain: String,
    cell: Uuid,
    output: String,
    lookup_output: Option<String>,
    count: u64,
}

pub async fn allocate(
    db: &Database,
    session: &moria_runtime::authority::VerifiedSession,
    request: &Allocation,
) -> Result<()> {
    ensure!((1..=100_000_000).contains(&request.count));
    let registry = moria_runtime::identity::IdentityRegistry { db: db.clone() };
    registry
        .register_cell(request.cell, "CAPACITY_CELL")
        .await?;
    let ownership = registry
        .register_domain(&request.domain, request.cell)
        .await?;
    let output = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&request.output)?;
    let mut output = std::io::BufWriter::new(output);
    let mut lookup = request
        .lookup_output
        .as_ref()
        .map(|path| {
            std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .map(std::io::BufWriter::new)
        })
        .transpose()?;
    let started = std::time::Instant::now();
    for start in (0..request.count).step_by(1000) {
        Authority::current(&**db.get().await?, session).await?;
        let keys: Vec<String> = (start..(start + 1000).min(request.count))
            .map(|n| n.to_string())
            .collect();
        let ids = registry.resolve(&request.domain, &keys, &ownership).await?;
        let sample = registry
            .resolve(
                &request.domain,
                &[keys[0].clone(), keys.last().unwrap().clone()],
                &ownership,
            )
            .await?;
        ensure!(sample[0] == ids[0] && sample[1] == *ids.last().unwrap());
        for (offset, id) in ids.into_iter().enumerate() {
            if let Some(lookup) = lookup.as_mut() {
                writeln!(lookup, "{id}")?;
            }
            writeln!(
                output,
                "{}",
                json!({"ordinal":start+offset as u64,"uid":id})
            )?;
        }
        if start % 100_000 == 0 {
            event(
                json!({"event":"identity_population","through":(start+keys.len() as u64).to_string()}),
            );
        }
    }
    output.flush()?;
    output.get_ref().sync_all()?;
    if let Some(lookup) = lookup.as_mut() {
        lookup.flush()?;
        lookup.get_ref().sync_all()?;
    }
    let count: i64 = db
        .get()
        .await?
        .query_one(
            "SELECT count(*) FROM moria.identities WHERE domain=$1",
            &[&request.domain],
        )
        .await?
        .get(0);
    ensure!(count as u64 == request.count);
    event(
        json!({"event":"identity_population_complete","count":request.count.to_string(),"elapsedMillis":started.elapsed().as_millis().to_string(),"ownership":ownership}),
    );
    Ok(())
}

pub struct Objects {
    values: Box<dyn Iterator<Item = Result<(String, String)>>>,
}
impl Objects {
    pub fn new(snapshot: &Snapshot) -> Result<Self> {
        let values: Box<dyn Iterator<Item = Result<(String, String)>>> =
            if let Some(path) = &snapshot.objects_file {
                ensure!(snapshot.objects.is_empty());
                let mut reader = BufReader::with_capacity(16 * 1024, std::fs::File::open(path)?);
                Box::new(std::iter::from_fn(move || {
                    let mut bytes = Vec::new();
                    match Read::by_ref(&mut reader)
                        .take(4097)
                        .read_until(b'\n', &mut bytes)
                    {
                        Ok(0) => None,
                        Ok(n) => Some((|| {
                            ensure!(n <= 4096 && bytes.last() == Some(&b'\n'));
                            let pair: (String, String) = serde_json::from_slice(&bytes)?;
                            Ok(pair)
                        })()),
                        Err(error) => Some(Err(error.into())),
                    }
                }))
            } else {
                Box::new(snapshot.objects.clone().into_iter().map(Ok))
            };
        Ok(Self { values })
    }
}
impl Iterator for Objects {
    type Item = Result<(String, String)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.values.next()
    }
}

/// Fixture source files are streamed twice: expected digest, then the actual guarded provider write.
pub async fn transfer(
    store: &dyn ObjectStore,
    path: &str,
    object: LogicalObject,
    grant: Arc<DatasetIoGrant>,
    cancel: Cancellation,
) -> Result<SealedObject> {
    let mut input = tokio::fs::File::open(path).await?;
    let size = input.metadata().await?.len();
    ensure!(size <= 128 * 1024 * 1024, "FIXTURE_FILE_LIMIT");
    let mut buffer = vec![0; 1024 * 1024];
    let mut hash = Sha256::new();
    let mut observed = 0u64;
    loop {
        let n = input.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        observed += n as u64;
    }
    ensure!(observed == size);
    input.rewind().await?;
    let mut pending = store
        .begin_object(
            object,
            size,
            format!("sha256:{:x}", hash.finalize()),
            grant,
            cancel,
        )
        .await?;
    loop {
        let n = input.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        pending.append(&buffer[..n]).await?;
    }
    pending.seal().await
}

pub struct Renewal(tokio::task::JoinHandle<()>);
impl Renewal {
    pub fn start(db: Database, lease: Lease) -> Self {
        Self(tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                if (ExecutionAuthority { db: db.clone() })
                    .renew(&lease)
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }))
    }
}
impl Drop for Renewal {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Verification {
    stage: Uuid,
    resource: String,
    domain: String,
    count: u64,
    source_sum: String,
    source_xor: String,
    #[serde(default = "default_page_rows")]
    page_rows: i64,
}
fn default_page_rows() -> i64 {
    1000
}
pub async fn verify(
    db: &Database,
    session: &moria_runtime::authority::VerifiedSession,
    request: &Verification,
) -> Result<()> {
    ensure!(
        (1..=1000).contains(&request.page_rows),
        "Invalid oracle page budget"
    );
    let mut client = db.get().await?;
    let tx = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let row=tx.query_one("SELECT p.id,e.revision,b.definition->'markings' AS markings FROM moria.runtime_projections p JOIN moria.indexing_builds b ON b.id=p.stage JOIN moria.environments e ON e.id=p.environment WHERE p.stage=$1 AND p.environment=$2 AND p.resource=$3 AND p.status='active'",&[&request.stage,&session.environment(),&request.resource]).await?;
    let projection: Uuid = row.get(0);
    let revision: i64 = row.get(1);
    let labels: Vec<String> = serde_json::from_value(row.get(2))?;
    let table = format!("moria.\"runtime_projection_{}\"", projection.simple());
    let wrong:i64=tx.query_one("SELECT count(*) FROM moria.staged_source_rows r LEFT JOIN moria.identities i ON i.uid=r.uid WHERE r.stage=$1 AND (i.uid IS NULL OR i.domain<>$2 OR i.canonical_key<>r.value->>'id')",&[&request.stage,&request.domain]).await?.get(0);
    ensure!(wrong == 0, "CAPACITY_UID_MEMBERSHIP");
    let mut count = 0u64;
    let mut after: Option<Uuid> = None;
    let mut sum = [0u8; 32];
    let mut xor = [0u8; 32];
    loop {
        let current = Authority::current(&**db.get().await?, session).await?;
        moria_runtime::authorization::authorize(
            &current.identity,
            &request.resource,
            "read",
            &labels,
        )?;
        let rows=tx.query(&format!("SELECT p.uid,p.source,(p.source=r.value AND p.effective=(r.value||COALESCE(o.overrides,'{{}}')) AND p.deleted=COALESCE(o.tombstone,false)) AS valid FROM {table} p JOIN moria.staged_source_rows r ON r.stage=$1 AND r.uid=p.uid LEFT JOIN LATERAL(SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$2 AND object_type=$3 AND uid=p.uid AND revision<=$4 ORDER BY revision DESC LIMIT 1) o ON true WHERE p.from_revision<=$4 AND (p.to_revision IS NULL OR p.to_revision>$4) AND ($5::uuid IS NULL OR p.uid>$5) ORDER BY p.uid LIMIT $6"),&[&request.stage,&session.environment(),&request.resource,&revision,&after,&request.page_rows]).await?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            ensure!(
                row.get::<_, bool>(2),
                "CAPACITY_EFFECTIVE_OR_SOURCE_MISMATCH"
            );
            let source: Value = row.get(1);
            let bytes = moria_runtime::canonical::canonical_json(&source)?;
            let hash = Sha256::digest(bytes.as_bytes());
            let mut carry = 0u16;
            for i in (0..32).rev() {
                let n = sum[i] as u16 + hash[i] as u16 + carry;
                sum[i] = n as u8;
                carry = n >> 8;
                xor[i] ^= hash[i];
            }
            count += 1;
            after = Some(row.get(0));
        }
    }
    let encode = |bytes: &[u8; 32]| bytes.iter().map(|n| format!("{n:02x}")).collect::<String>();
    ensure!(
        count == request.count
            && encode(&sum) == request.source_sum
            && encode(&xor) == request.source_xor,
        "CAPACITY_SOURCE_CHECKSUM"
    );
    let source_count: i64 = tx
        .query_one(
            "SELECT count(*) FROM moria.staged_source_rows WHERE stage=$1",
            &[&request.stage],
        )
        .await?
        .get(0);
    ensure!(source_count as u64 == count, "CAPACITY_SOURCE_COUNT");
    let projected:i64=tx.query_one(&format!("SELECT count(*) FROM {table} WHERE from_revision<=$1 AND (to_revision IS NULL OR to_revision>$1)"),&[&revision]).await?.get(0);
    ensure!(projected as u64 == count, "CAPACITY_PROJECTION_COUNT");
    let size: i64 = tx
        .query_one(
            "SELECT pg_total_relation_size($1::text::regclass)",
            &[&table],
        )
        .await?
        .get(0);
    tx.commit().await?;
    event(
        json!({"event":"capacity_verified","count":count.to_string(),"sourceSum":encode(&sum),"sourceXor":encode(&xor),"projection":projection,"projectionBytes":size.to_string(),"revision":revision.to_string()}),
    );
    Ok(())
}
