//! Explicit retention of managed native Dataset roots and exact issued objects.
use crate::{
    catalog::{self, DatasetIoGrant, NativeSnapshotRef, SnapshotRoot},
    database::Database,
    recovery::RETENTION_ADMISSION,
    storage::{
        Cancellation, ClosureSpool, IndexNamespace, ObjectStore, ReadAuthority, RecordingReader,
        SealedObject, WriteAuthority,
    },
};
use anyhow::{Result, ensure};
use serde_json::Value;
use std::sync::Arc;

async fn admission(client: &tokio_postgres::Client, exclusive: bool) -> Result<()> {
    client
        .query_one(
            if exclusive {
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))"
            } else {
                "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))"
            },
            &[&RETENTION_ADMISSION],
        )
        .await?;
    Ok(())
}
async fn members(
    db: &Database,
    key: &str,
    physical: &str,
    values: &[SealedObject],
    recording: &RecordingReader,
) -> Result<()> {
    ensure!(values.len() <= 256, "GC_MEMBER_LIMIT");
    let values=Value::Array(values.iter().map(|content| Ok(serde_json::json!({"content":content,"physicalVersion":recording.verified(content)?}))).collect::<Result<_>>()?);
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    admission(tx.client(), false).await?;
    ensure!(tx.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_producers p ON p.scope=r.producer WHERE r.key=$1 AND NOT r.sealed AND p.tombstoned_at IS NULL)",&[&key]).await?.get::<_,bool>(0),"GC_PRODUCER_TOMBSTONED");
    ensure!(!tx.query_one("SELECT EXISTS(SELECT 1 FROM jsonb_array_elements($2) v JOIN moria.runtime_dataset_gc_candidates c ON c.physical_fingerprint=$1 AND c.warehouse_id=v->'content'->'object'->>'warehouseId' AND c.table_id=v->'content'->'object'->>'tableId' AND c.object_id=v->'content'->'object'->>'objectId')",&[&physical,&values]).await?.get::<_,bool>(0),"GC_OBJECT_RETIRED");
    tx.execute("INSERT INTO moria.runtime_dataset_root_members(root,warehouse_id,table_id,object_id,content,physical_version) SELECT $1,v->'content'->'object'->>'warehouseId',v->'content'->'object'->>'tableId',v->'content'->'object'->>'objectId',v->'content',v->'physicalVersion' FROM jsonb_array_elements($2) v ON CONFLICT DO NOTHING",&[&key,&values]).await?;
    ensure!(!tx.query_one("SELECT EXISTS(SELECT 1 FROM jsonb_array_elements($2) v JOIN moria.runtime_dataset_root_members m ON m.root=$1 AND m.warehouse_id=v->'content'->'object'->>'warehouseId' AND m.table_id=v->'content'->'object'->>'tableId' AND m.object_id=v->'content'->'object'->>'objectId' WHERE m.content<>v->'content' OR m.physical_version<>v->'physicalVersion')",&[&key,&values]).await?.get::<_,bool>(0),"GC_OBJECT_OWNERSHIP_CHANGED");
    tx.commit().await?;
    Ok(())
}
/// A root's exact closure becomes durable before any Catalog pointer can reference it.
pub(crate) async fn register_native_root(
    store: Arc<dyn ObjectStore>,
    grant: Arc<DatasetIoGrant>,
    reference: &NativeSnapshotRef,
    root: &SnapshotRoot,
    spool: &ClosureSpool,
    cancel: Cancellation,
) -> Result<()> {
    if !grant.is_managed() {
        return Ok(());
    }
    grant.check_current().await?;
    grant.validate_store(store.as_ref())?;
    register_root(
        grant.database(),
        grant.namespace(),
        grant.session().environment(),
        grant.resource(),
        &grant.scope_key(),
        store,
        grant.clone(),
        reference,
        root,
        spool,
        None,
        cancel,
    )
    .await
}
/// Register only the actual qualified graph root; descendants remain in immutable graph custody.
pub(crate) async fn register_graph_root(
    graph: &crate::dataset_graph::NativeGraph,
    node: &crate::dataset_graph::QualifiedNode,
) -> Result<NativeSnapshotRef> {
    graph.check_current().await?;
    let grant = graph.grant();
    ensure!(grant.coordinator(), "COORDINATOR_REQUIRED");
    let descriptor: crate::native_snapshot_v3::RootDescriptor =
        serde_json::from_value(graph.read_descriptor(node).await?)?;
    ensure!(
        node.kind() == "root"
            && descriptor.version == 3
            && descriptor.reader_profile == crate::native_snapshot_v3::PROFILE
            && descriptor.profile == *node.profile()
            && descriptor.table_uuid == grant.table_uuid()?
            && descriptor.key_schema_hash == grant.key_schema_hash()?
            && descriptor.primary_key == grant.primary_key()?
            && descriptor.rows == node.entries()
            && descriptor.index_summary.is_some(),
        "DATASET_GRAPH_INVALID"
    );
    let reference = NativeSnapshotRef {
        version: 3,
        root: node.content().clone(),
    };
    let location = reference.encode()?;
    let physical = node.physical_fingerprint();
    let producer = grant.scope_key();
    let ns = grant.namespace();
    let mut client = grant.database().get().await?;
    let tx = client.transaction().await?;
    admission(tx.client(), false).await?;
    ensure!(tx.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_nodes n JOIN moria.runtime_dataset_graph_qualifications q ON q.node=n.id JOIN moria.runtime_dataset_issued_objects i ON i.producer=q.producer AND i.physical_fingerprint=q.physical_fingerprint AND i.warehouse_id=n.content->'object'->>'warehouseId' AND i.table_id=n.content->'object'->>'tableId' AND i.object_id=n.content->'object'->>'objectId' JOIN moria.runtime_dataset_producers p ON p.scope=i.producer WHERE n.id=$1 AND n.kind='root' AND n.content=$2 AND n.profile=$3 AND q.physical_fingerprint=$4 AND q.incarnation=$5 AND q.producer=$6 AND q.physical_version=$7 AND p.tombstoned_at IS NULL AND i.expected_size=$8 AND i.expected_sha256=$9)",&[&node.id(),&serde_json::to_value(node.content())?,node.profile(),&physical,&grant.incarnation(),&producer,&serde_json::to_value(node.physical_version())?,&i64::try_from(node.content().size)?,&node.content().sha256]).await?.get::<_,bool>(0),"GC_OBJECT_OWNERSHIP_CHANGED");
    let value = serde_json::to_value(&reference)?;
    let key: String = tx
        .query_one("SELECT moria.runtime_dataset_root_key($1)", &[&location])
        .await?
        .get(0);
    tx.execute("INSERT INTO moria.runtime_dataset_roots(key,environment,dataset,namespace,physical_fingerprint,metadata_location,root,snapshot_id,reader_profile,producer,retention_origin) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,COALESCE((SELECT retention_origin FROM moria.runtime_dataset_roots WHERE key=moria.runtime_dataset_root_key($11)),clock_timestamp())) ON CONFLICT DO NOTHING",&[&key,&grant.environment(),&grant.resource(),&serde_json::to_value(ns)?,&physical,&location,&value,&descriptor.snapshot_id,&crate::native_snapshot_v3::PROFILE,&producer,&grant.maintenance_basis()]).await?;
    ensure!(tx.query_one("SELECT producer=$2 AND root=$3 AND snapshot_id=$4 AND reader_profile=$5 FROM moria.runtime_dataset_roots WHERE key=$1",&[&key,&producer,&value,&descriptor.snapshot_id,&crate::native_snapshot_v3::PROFILE]).await?.get::<_,bool>(0),"GC_OBJECT_OWNERSHIP_CHANGED");
    tx.execute("INSERT INTO moria.runtime_dataset_root_members(root,warehouse_id,table_id,object_id,content,physical_version) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",&[&key,&ns.warehouse_id,&ns.table_id,&node.content().object.object_id,&serde_json::to_value(node.content())?,&serde_json::to_value(node.physical_version())?]).await?;
    ensure!(tx.query_one("SELECT content=$3 AND physical_version=$4 FROM moria.runtime_dataset_root_members WHERE root=$1 AND object_id=$2 AND warehouse_id=$5 AND table_id=$6",&[&key,&node.content().object.object_id,&serde_json::to_value(node.content())?,&serde_json::to_value(node.physical_version())?,&ns.warehouse_id,&ns.table_id]).await?.get::<_,bool>(0),"GC_OBJECT_OWNERSHIP_CHANGED");
    tx.execute(
        "UPDATE moria.runtime_dataset_roots SET sealed=true WHERE key=$1 AND NOT sealed",
        &[&key],
    )
    .await?;
    tx.commit().await?;
    graph.check_current().await?;
    Ok(reference)
}
#[allow(clippy::too_many_arguments)]
async fn register_root(
    db: &Database,
    namespace: &IndexNamespace,
    environment: &str,
    dataset: &str,
    producer: &str,
    store: Arc<dyn ObjectStore>,
    read: Arc<dyn ReadAuthority>,
    reference: &NativeSnapshotRef,
    root: &SnapshotRoot,
    spool: &ClosureSpool,
    retention_origin: Option<std::time::SystemTime>,
    cancel: Cancellation,
) -> Result<()> {
    read.check_read(&reference.root.object).await?;
    let recording = RecordingReader::new(store.clone(), spool.clone())?;
    let location = reference.encode()?;
    let physical = store.physical_fingerprint()?;
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    admission(tx.client(), false).await?;
    let key: String = tx
        .query_one("SELECT moria.runtime_dataset_root_key($1)", &[&location])
        .await?
        .get(0);
    let owner=tx.query_opt("SELECT p.scope FROM moria.runtime_dataset_producers p JOIN moria.runtime_dataset_issued_objects o ON o.producer=p.scope WHERE p.scope=$1 AND p.tombstoned_at IS NULL AND o.physical_fingerprint=$2 AND o.warehouse_id=$3 AND o.table_id=$4 AND o.object_id=$5 AND o.expected_size=$6 AND o.expected_sha256=$7 FOR SHARE OF p",&[&producer,&physical,&namespace.warehouse_id,&namespace.table_id,&reference.root.object.object_id,&i64::try_from(reference.root.size)?,&reference.root.sha256]).await?;
    ensure!(owner.is_some(), "GC_OBJECT_OWNERSHIP_CHANGED");
    let value = serde_json::to_value(reference)?;
    tx.execute("INSERT INTO moria.runtime_dataset_roots(key,environment,dataset,namespace,physical_fingerprint,metadata_location,root,snapshot_id,reader_profile,producer,retention_origin) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,COALESCE($11,clock_timestamp())) ON CONFLICT DO NOTHING",&[&key,&environment,&dataset,&serde_json::to_value(namespace)?,&physical,&location,&value,&root.proof.snapshot_id.to_string(),&root.proof.reader_profile,&producer,&retention_origin]).await?;
    let previous = tx
        .query_one(
            "SELECT producer,root,sealed FROM moria.runtime_dataset_roots WHERE key=$1",
            &[&key],
        )
        .await?;
    ensure!(
        previous.get::<_, String>("producer") == producer
            && previous.get::<_, Value>("root") == value,
        "GC_OBJECT_OWNERSHIP_CHANGED"
    );
    if previous.get::<_, bool>("sealed") {
        tx.commit().await?;
        return Ok(());
    }
    tx.commit().await?;
    members(
        db,
        &key,
        &physical,
        &[reference.root.clone(), root.proof.metadata.clone()],
        &recording,
    )
    .await?;
    let mut current = root.last_page.clone();
    let mut count = 0u64;
    for ordinal in (0..root.pages).rev() {
        cancel.check()?;
        let object = current
            .take()
            .ok_or_else(|| anyhow::anyhow!("INVALID_NATIVE_SNAPSHOT_ROOT"))?;
        let page = catalog::snapshot_page(
            store.as_ref(),
            read.clone(),
            &object,
            ordinal,
            cancel.clone(),
        )
        .await?;
        members(db, &key, &physical, &[object], &recording).await?;
        let objects: Vec<_> = page
            .entries
            .iter()
            .map(|entry| entry.content.clone())
            .collect();
        members(db, &key, &physical, &objects, &recording).await?;
        count += objects.len() as u64;
        current = page.previous;
    }
    ensure!(
        current.is_none() && count == root.proof.closure.entries,
        "INVALID_NATIVE_SNAPSHOT_ROOT"
    );
    read.check_read(&reference.root.object).await?;
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    admission(tx.client(), false).await?;
    ensure!(tx.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_producers WHERE scope=$1 AND tombstoned_at IS NULL)",&[&producer]).await?.get::<_,bool>(0),"GC_PRODUCER_TOMBSTONED");
    tx.execute(
        "UPDATE moria.runtime_dataset_roots SET sealed=true WHERE key=$1 AND NOT sealed",
        &[&key],
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

mod collect;
pub(crate) mod operator;
mod prune_graph;
pub async fn cli(db: Database, arguments: Vec<String>) -> Result<()> {
    use tokio::io::AsyncReadExt;
    ensure!(arguments.len() == 1, "INPUT_INVALID");
    let mut bytes = Vec::new();
    tokio::io::stdin()
        .take(65537)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 65536, "INPUT_INVALID");
    let input: Value = serde_json::from_slice(&bytes)?;
    let output = match arguments[0].as_str() {
        "approve" => operator::approve(&db, serde_json::from_value(input)?).await?,
        "expire" => operator::expire(&db, serde_json::from_value(input)?).await?,
        "plan" => collect::plan(&db, serde_json::from_value(input)?).await?,
        "once" => collect::once(&db, serde_json::from_value(input)?).await?,
        "disable" => {
            let policy = serde_json::from_value::<Disable>(input)?;
            ensure!(db.get().await?.execute("UPDATE moria.runtime_dataset_retention_policies SET enabled=false WHERE id=$1",&[&policy.policy]).await?==1,"GC_POLICY_UNKNOWN");
            serde_json::json!({"disabled":true})
        }
        _ => anyhow::bail!("INPUT_INVALID"),
    };
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Disable {
    policy: uuid::Uuid,
}
