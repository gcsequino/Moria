use anyhow::{Context, Result, ensure};
use futures::{StreamExt, TryStreamExt};
use moria_runtime::{
    authority::Authority,
    canonical::canonical_json,
    catalog::*,
    database::Database,
    execution::{Domain, ExecutionAuthority, Lease, TaskPlan},
    snapshot::SnapshotPin,
    storage::*,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::{BufRead, Write},
    sync::Arc,
};
use uuid::Uuid;
mod capacity;
mod reader_window;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Snapshot {
    resource: String,
    warehouse_id: String,
    table_id: String,
    metadata: String,
    snapshot_id: String,
    table_uuid: Uuid,
    #[serde(default)]
    objects: BTreeMap<String, String>,
    objects_file: Option<String>,
    revision: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    root: String,
    token: String,
    binding: Uuid,
    mode: String,
    snapshots: Vec<Snapshot>,
    lease: Option<Lease>,
    candidates: Option<Vec<Uuid>>,
    checkpoint: Option<Value>,
    native_ref: Option<String>,
    retained_snapshot: Option<String>,
    selected_snapshot: Option<String>,
    as_of_revision: Option<String>,
    #[serde(default)]
    expected_native_rows: bool,
    #[serde(default)]
    writer_rows: BTreeMap<String, Vec<Value>>,
    expected_rows: Option<usize>,
    expected_markings: Option<Vec<String>>,
    #[serde(default)]
    graph_output: bool,
    #[serde(default)]
    prepare_only: bool,
    #[serde(default)]
    s3_bindings: Vec<S3Binding>,
    allocation: Option<capacity::Allocation>,
    closure_limits: Option<ClosureLimits>,
    verification: Option<capacity::Verification>,
    resolve_resource: Option<String>,
    registered_host: Option<Uuid>,
    registered_owner: Option<Uuid>,
    header_probe: Option<reader_window::HeaderProbe>,
}
fn event(value: Value) {
    println!("{}", value);
    std::io::stdout().flush().unwrap();
}
fn proceed() {
    let mut value = String::new();
    std::io::stdin().lock().read_line(&mut value).unwrap();
}
fn supplied_batch(
    schema: &iceberg::spec::Schema,
    rows: &[Value],
    descriptors: &Value,
) -> Result<arrow_array::RecordBatch> {
    ensure!(
        !rows.is_empty() && rows.len() <= 1000 && serde_json::to_vec(rows)?.len() <= 1024 * 1024,
        "FIXTURE_ROW_LIMIT"
    );
    let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(schema)?);
    ensure!(
        rows.iter()
            .all(|row| row
                .as_object()
                .is_some_and(|value| value.len() == schema.fields().len()
                    && schema
                        .fields()
                        .iter()
                        .all(|field| value.contains_key(field.name())))),
        "FIXTURE_SCHEMA_INVALID"
    );
    let arrays = schema
        .fields()
        .iter()
        .map(|field| -> Result<arrow_array::ArrayRef> {
            let values = rows
                .iter()
                .map(|row| {
                    let value = &row[field.name()];
                    let wire = if descriptors[field.name()]["type"] == "int64"
                        && value.as_i64().is_some()
                    {
                        Value::String(value.as_i64().unwrap().to_string())
                    } else {
                        value.clone()
                    };
                    moria_runtime::values::normalize(&wire, &descriptors[field.name()])
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(match field.data_type() {
                arrow_schema::DataType::Utf8 => Arc::new(arrow_array::StringArray::from_iter(
                    values.iter().map(|value| value.as_str()),
                )),
                arrow_schema::DataType::Int64 => Arc::new(arrow_array::Int64Array::from_iter(
                    values
                        .iter()
                        .map(|value| value.as_str().map(str::parse::<i64>).transpose())
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                arrow_schema::DataType::Float64 => Arc::new(arrow_array::Float64Array::from_iter(
                    values.iter().map(Value::as_f64),
                )),
                arrow_schema::DataType::Decimal128(precision, scale) => {
                    let values = values
                        .iter()
                        .map(|value| {
                            value
                                .as_str()
                                .map(|value| value.replace('.', "").parse::<i128>())
                                .transpose()
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Arc::new(
                        arrow_array::Decimal128Array::from(values)
                            .with_precision_and_scale(*precision, *scale)?,
                    )
                }
                arrow_schema::DataType::Date32 => {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let values = values
                        .iter()
                        .map(|value| {
                            value
                                .as_str()
                                .map(|value| {
                                    Ok::<_, anyhow::Error>(i32::try_from(
                                        chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")?
                                            .signed_duration_since(epoch)
                                            .num_days(),
                                    )?)
                                })
                                .transpose()
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Arc::new(arrow_array::Date32Array::from(values))
                }
                arrow_schema::DataType::Timestamp(
                    arrow_schema::TimeUnit::Microsecond,
                    Some(zone),
                ) => {
                    let values = values
                        .iter()
                        .map(|value| {
                            value
                                .as_str()
                                .map(|value| {
                                    chrono::DateTime::parse_from_rfc3339(value)
                                        .map(|value| value.timestamp_micros())
                                })
                                .transpose()
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Arc::new(
                        arrow_array::TimestampMicrosecondArray::from(values)
                            .with_timezone(zone.clone()),
                    )
                }
                arrow_schema::DataType::Boolean => Arc::new(arrow_array::BooleanArray::from_iter(
                    values.iter().map(|value| value.as_bool()),
                )),
                arrow_schema::DataType::List(child)
                    if child.data_type() == &arrow_schema::DataType::Utf8 =>
                {
                    let mut builder = arrow_array::builder::ListBuilder::new(
                        arrow_array::builder::StringBuilder::new(),
                    )
                    .with_field(child.clone());
                    for value in values {
                        if value.is_null() {
                            builder.append(false);
                            continue;
                        }
                        for item in value
                            .as_array()
                            .ok_or_else(|| anyhow::anyhow!("FIXTURE_TYPE_INVALID"))?
                        {
                            if item.is_null() {
                                ensure!(child.is_nullable(), "FIXTURE_TYPE_INVALID");
                                builder.values().append_null();
                            } else {
                                builder.values().append_value(
                                    item.as_str()
                                        .ok_or_else(|| anyhow::anyhow!("FIXTURE_TYPE_INVALID"))?,
                                );
                            }
                        }
                        builder.append(true);
                    }
                    Arc::new(builder.finish())
                }
                arrow_schema::DataType::FixedSizeBinary(16) => {
                    let mut builder = arrow_array::builder::FixedSizeBinaryBuilder::new(16);
                    for value in values {
                        if value.is_null() {
                            builder.append_null();
                        } else {
                            builder.append_value(
                                Uuid::parse_str(
                                    value
                                        .as_str()
                                        .ok_or_else(|| anyhow::anyhow!("FIXTURE_UUID_INVALID"))?,
                                )?
                                .as_bytes(),
                            )?;
                        }
                    }
                    Arc::new(builder.finish())
                }
                _ => anyhow::bail!("FIXTURE_TYPE_UNSUPPORTED"),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(arrow_array::RecordBatch::try_new(schema, arrays)?)
}
fn typed_rows(batch: &arrow_array::RecordBatch, columns: &Value) -> Result<Vec<String>> {
    let columns = columns
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("FIXTURE_SCHEMA_INVALID"))?;
    ensure!(
        batch.num_columns() == columns.len(),
        "FIXTURE_SCHEMA_INVALID"
    );
    (0..batch.num_rows())
        .map(|row| {
            let mut value = serde_json::Map::new();
            for (name, descriptor) in columns {
                let column = batch
                    .column_by_name(name)
                    .ok_or_else(|| anyhow::anyhow!("FIXTURE_SCHEMA_INVALID"))?;
                value.insert(
                    name.clone(),
                    moria_runtime::values::arrow_value(column.as_ref(), row, descriptor)?,
                );
            }
            moria_runtime::canonical::canonical_json(&Value::Object(value))
        })
        .collect()
}
#[test]
fn supplied_array_batch_preserves_exact_iceberg_child_metadata_and_nullability() -> Result<()> {
    let schema: iceberg::spec::Schema = serde_json::from_value(
        json!({"type":"struct","schema-id":0,"fields":[{"id":1,"name":"tags","required":false,"type":{"type":"list","element-id":2,"element-required":true,"element":"string"}}]}),
    )?;
    let columns =
        json!({"tags":{"type":"array","nullable":true,"items":{"type":"string","nullable":false}}});
    let batch = supplied_batch(
        &schema,
        &[
            json!({"tags":["東京","😀"]}),
            json!({"tags":null}),
            json!({"tags":[]}),
        ],
        &columns,
    )?;
    let converted = iceberg::arrow::schema_to_arrow_schema(&schema)?;
    ensure!(batch.schema().field(0) == converted.field(0));
    ensure!(
        typed_rows(&batch, &columns)?
            == vec![
                "{\"tags\":[\"東京\",\"😀\"]}",
                "{\"tags\":null}",
                "{\"tags\":[]}"
            ]
    );
    ensure!(supplied_batch(&schema, &[json!({"tags":[null]})], &columns).is_err());
    ensure!(supplied_batch(&schema, &[json!({"tags":[1]})], &columns).is_err());
    Ok(())
}
#[test]
#[ignore = "requires the mandatory real PostgreSQL and Iceberg native catalog fixture"]
fn catalog_publication() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run())
        .unwrap();
}
async fn run() -> Result<()> {
    let fixture: Fixture = serde_json::from_str(&std::env::var("MORIA_NATIVE_CATALOG_FIXTURE")?)?;
    let db = Database::configured()?;
    db.schema_lease().await?;
    let session = Authority { db: db.clone() }
        .authenticate(&fixture.token)
        .await?;
    if fixture.mode == "header-window" {
        let input = fixture
            .header_probe
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("FIXTURE_HEADER_PROBE_REQUIRED"))?;
        return reader_window::headers(&db, &fixture, input).await;
    }
    if fixture.mode == "allocate" {
        return capacity::allocate(&db, &session, fixture.allocation.as_ref().unwrap()).await;
    }
    if fixture.mode == "verify-capacity" {
        return capacity::verify(&db, &session, fixture.verification.as_ref().unwrap()).await;
    }
    if fixture.mode == "resolve-pin-at" {
        let resource = fixture
            .resolve_resource
            .as_deref()
            .unwrap_or(&fixture.snapshots[0].resource);
        let snapshot = fixture
            .selected_snapshot
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("FIXTURE_SNAPSHOT_REQUIRED"))?;
        event(
            json!({"event":"resolved","pin":resolve_dataset_pin_at(&db,&session,resource,snapshot,fixture.as_of_revision.as_deref()).await?}),
        );
        return Ok(());
    }
    if fixture.mode == "resolve-pin" {
        let resource = fixture
            .resolve_resource
            .as_deref()
            .unwrap_or(&fixture.snapshots[0].resource);
        event(json!({"event":"resolved","pin":resolve_dataset_pin(&db,&session,resource).await?}));
        return Ok(());
    }
    if fixture.mode == "index" || fixture.mode == "index-plan" {
        let worker = moria_runtime::worker::Worker {
            db: db.clone(),
            owner: Uuid::new_v4(),
        };
        if fixture.mode == "index-plan" {
            ensure!(worker.plan().await?);
            event(json!({"event":"planned"}));
            return Ok(());
        }
        for _ in 0..2000 {
            let worked = worker.tick().await?;
            let pending:i64=db.get().await?.query_one("SELECT count(*) FROM moria.indexing_builds WHERE status IN ('pending','planning','running','preparing','ready')",&[]).await?.get(0);
            if pending == 0 {
                break;
            }
            if !worked {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
        let states:Vec<String>=db.get().await?.query("SELECT b.status FROM moria.indexing_builds b JOIN moria.dataset_heads h ON h.environment=b.environment AND h.dataset=b.source AND h.revision=b.source_revision JOIN moria.indexing_bindings x ON x.environment=b.environment AND x.target=b.target AND x.revision=b.binding_revision ORDER BY b.created_at",&[]).await?.iter().map(|row|row.get(0)).collect();
        ensure!(
            !states.is_empty() && states.iter().all(|s| s == "active" || s == "superseded"),
            "INDEXING_INCOMPLETE {states:?}"
        );
        event(json!({"event":"indexed","states":states}));
        return Ok(());
    }
    if fixture.mode == "replay" {
        let receipt = publish_snapshots(
            &db,
            &session,
            fixture.lease.as_ref().unwrap(),
            fixture.candidates.as_deref().unwrap(),
            fixture.checkpoint,
        )
        .await?;
        event(json!({"event":"receipt","receipt":receipt}));
        return Ok(());
    }
    let execution = ExecutionAuthority { db: db.clone() };
    let id = execution
        .request(
            fixture.binding,
            &Uuid::new_v4().to_string(),
            json!({}),
            vec![TaskPlan {
                key: "source".into(),
                partition: None,
            }],
        )
        .await?;
    ensure!(
        fixture.registered_host.is_some() == fixture.registered_owner.is_some(),
        "Incomplete registered worker fixture"
    );
    let task = match (fixture.registered_host, fixture.registered_owner) {
        (Some(host), Some(owner)) => {
            execution
                .claim_registered(owner, host, &[Domain::Sync], false, Some(id))
                .await?
        }
        _ => {
            execution
                .claim(Uuid::new_v4(), &[Domain::Sync], false)
                .await?
        }
    }
    .unwrap();
    ensure!(task.lease.execution == id);
    let session = match fixture.registered_host {
        Some(host) => moria_runtime::execution::job_session(&db, &task.lease, host).await?,
        None => session,
    };
    if matches!(
        fixture.mode.as_str(),
        "graph-bootstrap" | "graph-bootstrap-held" | "graph-read" | "graph-read-held"
    ) {
        let source = &fixture.snapshots[0];
        let namespace = IndexNamespace {
            warehouse_id: source.warehouse_id.clone(),
            table_id: source.table_id.clone(),
            revision: source.revision.clone().unwrap_or_else(|| "v1".into()),
        };
        let (lease, graph_session) = if fixture.mode.starts_with("graph-bootstrap") {
            execution
                .seal(&task.lease, json!({"complete":true}), &"b".repeat(64))
                .await?;
            let coordinator = match (fixture.registered_host, fixture.registered_owner) {
                (Some(host), Some(owner)) => {
                    execution
                        .claim_registered(owner, host, &[Domain::Sync], true, Some(id))
                        .await?
                }
                _ => {
                    execution
                        .claim(Uuid::new_v4(), &[Domain::Sync], true)
                        .await?
                }
            }
            .ok_or_else(|| anyhow::anyhow!("FIXTURE_COORDINATOR_REQUIRED"))?;
            ensure!(
                coordinator.lease.execution == id && coordinator.lease.task.is_none(),
                "FIXTURE_COORDINATOR_REQUIRED"
            );
            let coordinator_session = match fixture.registered_host {
                Some(host) => {
                    moria_runtime::execution::job_session(&db, &coordinator.lease, host).await?
                }
                None => session.clone(),
            };
            (coordinator.lease, coordinator_session)
        } else {
            (task.lease.clone(), session.clone())
        };
        let grant = Arc::new(
            DatasetIoGrant::acquire(
                db.clone(),
                graph_session.clone(),
                lease.clone(),
                namespace.clone(),
            )
            .await?,
        );
        let store = moria_runtime::storage::configured_store_with_fingerprint(&namespace)?.0;
        let scratch_path = std::path::Path::new(&fixture.root).join(format!("graph-scratch-{id}"));
        std::fs::create_dir(&scratch_path)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&scratch_path, std::fs::Permissions::from_mode(0o700))?;
        let scratch = PosixStore::provision(
            &scratch_path,
            &namespace.warehouse_id,
            &namespace.table_id,
            StorageLimits {
                max_object_bytes: 128 * 1024 * 1024,
                chunk_bytes: 1024 * 1024,
            },
        )?;
        let cancel = Cancellation::default();
        let _renewal = capacity::Renewal::start(db.clone(), lease);
        let result = async {
            let basis = if fixture.mode.starts_with("graph-bootstrap") {
                let receipt = moria_runtime::dataset_writer::bootstrap_v2::bootstrap_expected_head(
                    store.clone(),
                    &scratch,
                    grant.clone(),
                    ClosureLimits {
                        max_entries: 10000,
                        max_referenced_bytes: 1024 * 1024 * 1024,
                        max_spool_bytes: 32 * 1024 * 1024,
                    },
                    moria_runtime::dataset_writer::visibility::BootstrapBudget {
                        maximum_data_rows: 1000,
                        maximum_delete_rows: 0,
                    },
                    cancel.clone(),
                )
                .await
                .context("native graph bootstrap")?;
                event(json!({"event":"graph_bootstrap_ready","execution":id,"root":receipt.root,"graphNode":receipt.graph_node,"rows":receipt.rows}));
                moria_runtime::dataset_graph_read::ReadBasis::ExpectedHead
            } else {
                moria_runtime::dataset_graph_read::ReadBasis::Input
            };
            let (context, root) = moria_runtime::dataset_graph_read::GraphReadContext::execution(
                store,
                grant.clone(),
                basis,
                cancel,
            )
            .await
            .context("native graph rooted read")?;
            let snapshot = Arc::new(
                moria_runtime::native_snapshot_v3::NativeSnapshot::open(context, root)
                    .await
                    .context("native graph root decoder")?,
            );
            if fixture.mode == "graph-read-held" {
                let keys: Vec<String> = db.get().await?.query_one(
                    "SELECT primary_key FROM moria.native_dataset_bindings WHERE environment=$1 AND resource=$2",
                    &[&graph_session.environment(), &source.resource.parse::<Uuid>()?],
                ).await?.get(0);
                let row = fixture.writer_rows.get(&source.resource).and_then(|rows|rows.first()).ok_or_else(||anyhow::anyhow!("FIXTURE_ROWS_REQUIRED"))?;
                let values = keys.iter().map(|key|row[key].clone()).collect::<Vec<_>>();
                ensure!(snapshot.lookup_key(&values).await?.is_some(), "FIXTURE_KEY_NOT_FOUND");
                event(json!({"event":"graph_cached_ready","execution":id}));
                proceed();
                snapshot.lookup_key(&values).await?;
            } else if fixture.mode.ends_with("-held") {
                proceed();
            }
            let columns: Value = db.get().await?.query_one(
                "SELECT columns FROM moria.native_dataset_bindings WHERE environment=$1 AND resource=$2",
                &[&session.environment(), &source.resource.parse::<Uuid>()?],
            ).await?.get(0);
            let mut stream = snapshot.clone().scan().await?;
            let mut actual = BTreeMap::<String, usize>::new();
            let mut count = 0usize;
            while let Some(batch) = stream.try_next().await? {
                for row in typed_rows(&batch, &columns)? {
                    *actual.entry(row).or_insert(0) += 1;
                }
                count += batch.num_rows();
            }
            ensure!(count == fixture.expected_rows.unwrap_or(64), "FIXTURE_READBACK_MISMATCH");
            if let Some(expected) = fixture.writer_rows.get(&source.resource) {
                let schema = snapshot.metadata().current_schema();
                let expected_rows=if expected.is_empty(){Vec::new()}else{typed_rows(&supplied_batch(schema.as_ref(),expected,&columns)?,&columns)?};
                for row in expected_rows {
                    let value = actual.get_mut(&row).ok_or_else(|| anyhow::anyhow!("FIXTURE_READBACK_MISMATCH"))?;
                    ensure!(*value > 0, "FIXTURE_READBACK_MISMATCH");
                    *value -= 1;
                }
                ensure!(actual.values().all(|count| *count == 0), "FIXTURE_READBACK_MISMATCH");
                let keys: Vec<String> = db.get().await?.query_one(
                    "SELECT primary_key FROM moria.native_dataset_bindings WHERE environment=$1 AND resource=$2",
                    &[&session.environment(), &source.resource.parse::<Uuid>()?],
                ).await?.get(0);
                for row in expected {
                    let values = keys.iter().map(|key| row[key].clone()).collect::<Vec<_>>();
                    let locator = snapshot.lookup_key(&values).await?.ok_or_else(|| anyhow::anyhow!("FIXTURE_KEY_NOT_FOUND"))?;
                    ensure!(locator.file.starts_with("moria://"), "FIXTURE_KEY_NOT_FOUND");
                    if fixture.mode.starts_with("graph-bootstrap") {
                        ensure!(locator.row_hash.is_none(), "FIXTURE_BOOTSTRAP_DIGEST_NOT_DEFERRED");
                    }
                    if row == &expected[0] {
                        let verified = snapshot.read_key_row(&values).await?.ok_or_else(||anyhow::anyhow!("FIXTURE_KEY_NOT_FOUND"))?;
                        ensure!(verified.locator.row_hash.is_some(), "FIXTURE_ROW_DIGEST_NOT_VERIFIED");
                        ensure!(canonical_json(&verified.value)? == canonical_json(row)?, "FIXTURE_READBACK_MISMATCH");
                    }
                }
            }
            event(json!({"event":"graph_bootstrap_read","execution":id,"rows":count,"headUnchanged":true}));
            Ok::<_, anyhow::Error>(())
        }.await;
        return result;
    }
    if fixture.mode == "marker-denied" {
        let source = &fixture.snapshots[0];
        let namespace = IndexNamespace {
            warehouse_id: source.warehouse_id.clone(),
            table_id: source.table_id.clone(),
            revision: source.revision.clone().unwrap_or_else(|| "v1".into()),
        };
        let grant = DatasetIoGrant::acquire(
            db.clone(),
            session.clone(),
            task.lease.clone(),
            namespace.clone(),
        )
        .await?;
        let store = moria_runtime::storage::configured_store_with_fingerprint(&namespace)?.0;
        let object = LogicalObject::new(
            &namespace.warehouse_id,
            &namespace.table_id,
            "moria_restore_target_custody_v1",
        )?;
        let error = grant
            .admit_write(
                &object,
                0,
                &format!("sha256:{:x}", Sha256::digest([])),
                &store.physical_fingerprint()?,
            )
            .await
            .expect_err("permanent custody marker must never become issued Dataset data");
        ensure!(
            format!("{error:#}").contains("GC_OBJECT_DENIED"),
            "FIXTURE_WRONG_MARKER_DENIAL {error:#}"
        );
        ensure!(!db.get().await?.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_issued_objects WHERE object_id=$1)",&[&object.object_id]).await?.get::<_,bool>(0),"FIXTURE_MARKER_ISSUED");
        event(json!({"event":"marker_denied","execution":id}));
        return Ok(());
    }
    if fixture.mode == "multipart-crash" {
        let source = &fixture.snapshots[0];
        let namespace = IndexNamespace {
            warehouse_id: source.warehouse_id.clone(),
            table_id: source.table_id.clone(),
            revision: source.revision.clone().unwrap_or_else(|| "v1".into()),
        };
        let grant = Arc::new(
            DatasetIoGrant::acquire(
                db.clone(),
                session.clone(),
                task.lease.clone(),
                namespace.clone(),
            )
            .await?,
        );
        let object = LogicalObject {
            version: 1,
            warehouse_id: namespace.warehouse_id.clone(),
            table_id: namespace.table_id.clone(),
            object_id: format!("dataset_multipart_{}", Uuid::new_v4().simple()),
        };
        let store = moria_runtime::storage::configured_store_with_fingerprint(&namespace)?.0;
        let chunk = vec![71u8; 1024 * 1024];
        let mut digest = Sha256::new();
        for _ in 0..9 {
            digest.update(&chunk);
        }
        event(json!({"event":"dataset_multipart_intent","execution":id,"object":object}));
        let mut write = store
            .begin_object(
                object,
                9 * 1024 * 1024,
                format!("sha256:{:x}", digest.finalize()),
                grant,
                Cancellation::default(),
            )
            .await?;
        for _ in 0..8 {
            write.append(&chunk).await?;
        }
        event(json!({"event":"dataset_multipart_part"}));
        proceed();
        write.abort().await?;
        return Ok(());
    }
    if fixture.mode == "read-undeclared" {
        let _ = read_pinned_dataset(
            db.clone(),
            session.clone(),
            task.lease.clone(),
            &fixture.snapshots[0].resource,
            Cancellation::default(),
        )
        .await?;
        anyhow::bail!("undeclared Dataset unexpectedly opened");
    }
    if fixture.mode == "read" || fixture.mode == "read-old" || fixture.mode == "read-held" {
        let source = &fixture.snapshots[0];
        let grant = Arc::new(
            DatasetIoGrant::acquire(
                db.clone(),
                session.clone(),
                task.lease.clone(),
                IndexNamespace {
                    warehouse_id: source.warehouse_id.clone(),
                    table_id: source.table_id.clone(),
                    revision: source.revision.clone().unwrap_or_else(|| "v1".into()),
                },
            )
            .await?,
        );
        let reference = NativeSnapshotRef::decode(fixture.native_ref.as_deref().unwrap())?;
        ensure!(grant.check_write(&reference.root.object).await.is_err());
        if fixture.mode == "read-held" {
            grant.check_read(&reference.root.object).await?;
            event(json!({"event":"held_read_ready","execution":id}));
            proceed();
        }

        let selected = resolve_dataset_pin(&db, &session, &source.resource).await?;
        ensure!(
            selected.resource.to_string() == source.resource
                && selected.table_uuid == source.table_uuid
                && selected.markings
                    == fixture
                        .expected_markings
                        .clone()
                        .unwrap_or_else(|| vec!["private".into()])
        );
        let loaded = read_pinned_dataset(
            db.clone(),
            session.clone(),
            task.lease.clone(),
            &source.resource,
            Cancellation::default(),
        )
        .await?;
        ensure!(
            loaded.pin.metadata_location == fixture.native_ref.as_ref().unwrap().as_str()
                && loaded.consumed_markings
                    == fixture
                        .expected_markings
                        .clone()
                        .unwrap_or_else(|| vec!["private".into()])
        );
        let pin_revision = loaded.pin.revision.clone();
        ensure!(
            loaded.pin.snapshot == source.snapshot_id,
            "FIXTURE_INPUT_PIN_CHANGED"
        );
        let loaded = loaded.snapshot;
        let mut historical_rows = 0usize;
        if let Some(snapshot) = &fixture.retained_snapshot {
            let moria_runtime::catalog::PinnedDatasetSnapshot::FlatV2(legacy) = &loaded else {
                anyhow::bail!("FIXTURE_RETAINED_FLAT_SNAPSHOT_REQUIRED")
            };
            let table = legacy.table().await?.into_table();
            let scan = table
                .scan()
                .snapshot_id(snapshot.parse()?)
                .select_all()
                .build()?;
            let mut batches = scan.to_arrow().await?;
            while let Some(batch) = batches.try_next().await? {
                historical_rows += batch.num_rows();
            }
            ensure!(historical_rows == 300);
            let metadata = table.metadata();
            ensure!(metadata.last_sequence_number() == 2 && metadata.snapshots().count() == 2);
        }
        let mut batches = loaded.scan(1000).await?;
        let mut rows = 0usize;
        let expected_ids: std::collections::BTreeMap<String, Uuid> = if fixture.expected_native_rows
        {
            let rows = db
                .get()
                .await?
                .query(
                    "SELECT canonical_key,uid FROM moria.identities WHERE domain=$1 LIMIT 301",
                    &[&format!("writer-{}", source.resource)],
                )
                .await?;
            ensure!(rows.len() == 300);
            rows.into_iter()
                .map(|row| (row.get(0), row.get(1)))
                .collect()
        } else {
            BTreeMap::new()
        };
        let expected_count = fixture.expected_rows.unwrap_or(300);
        ensure!(expected_count <= 1000, "FIXTURE_ROW_LIMIT");
        let mut expected_typed: Option<(Value, BTreeMap<String, usize>)> = if let Some(expected) =
            fixture.writer_rows.get(&source.resource)
        {
            ensure!(expected.len() == expected_count, "FIXTURE_ROW_LIMIT");
            let definition=db.get().await?.query_one("SELECT iceberg_schema,columns FROM moria.native_dataset_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&source.resource.parse::<Uuid>()?]).await?;
            let schema: iceberg::spec::Schema =
                serde_json::from_value(definition.get("iceberg_schema"))?;
            let columns: Value = definition.get("columns");
            let expected_rows = if expected.is_empty() {
                Vec::new()
            } else {
                typed_rows(&supplied_batch(&schema, expected, &columns)?, &columns)?
            };
            let mut rows = BTreeMap::new();
            for row in expected_rows {
                *rows.entry(row).or_insert(0) += 1;
            }
            Some((columns, rows))
        } else {
            None
        };
        while let Some(batch) = batches.try_next().await? {
            if let Some((columns, expected)) = &mut expected_typed {
                for row in typed_rows(&batch, columns)? {
                    let count = expected
                        .get_mut(&row)
                        .ok_or_else(|| anyhow::anyhow!("FIXTURE_READBACK_MISMATCH"))?;
                    ensure!(*count > 0, "FIXTURE_READBACK_MISMATCH");
                    *count -= 1;
                }
            }
            if fixture.expected_native_rows {
                for row in 0..batch.num_rows() {
                    let id = moria_runtime::values::arrow_value(
                        batch.column_by_name("id").unwrap().as_ref(),
                        row,
                        &json!({"type":"int64","nullable":false}),
                    )?;
                    let key = id.as_str().unwrap();
                    let ordinal: i64 = key.parse()?;
                    let value = moria_runtime::values::arrow_value(
                        batch.column_by_name("value").unwrap().as_ref(),
                        row,
                        &json!({"type":"string","nullable":true}),
                    )?;
                    ensure!(
                        value
                            == if ordinal % 7 == 0 {
                                Value::Null
                            } else {
                                json!(format!("native-東京-😀-{ordinal}"))
                            }
                    );
                    let uid = moria_runtime::values::arrow_value(
                        batch.column_by_name("uid").unwrap().as_ref(),
                        row,
                        &json!({"type":"uid","nullable":false}),
                    )?;
                    ensure!(uid == expected_ids.get(key).unwrap().to_string());
                    let tags = moria_runtime::values::arrow_value(
                        batch.column_by_name("tags").unwrap().as_ref(),
                        row,
                        &json!({"type":"array","nullable":true,"items":{"type":"string","nullable":false}}),
                    )?;
                    ensure!(
                        tags == if ordinal % 7 == 0 {
                            Value::Null
                        } else {
                            json!(["東京", format!("tag-{ordinal}")])
                        }
                    );
                }
            }
            rows += batch.num_rows();
        }
        ensure!(rows == expected_count);
        loaded.check_scan_count(rows as u64)?;
        ensure!(
            expected_typed
                .as_ref()
                .is_none_or(|(_, rows)| rows.values().all(|count| *count == 0)),
            "FIXTURE_READBACK_MISMATCH"
        );
        let mut read_event = json!({"event":"read","rows":rows,"revision":pin_revision});
        if fixture.retained_snapshot.is_some() {
            read_event["historicalRows"] = json!(historical_rows);
        }
        event(read_event);
        return Ok(());
    }
    let mut native_parts = BTreeMap::new();
    let native_schema: iceberg::spec::Schema = serde_json::from_value(
        json!({"type":"struct","schema-id":0,"identifier-field-ids":[],"fields":[
            {"id":1,"name":"id","required":true,"type":"long"},
            {"id":2,"name":"value","required":false,"type":"string"},
            {"id":3,"name":"enabled","required":false,"type":"boolean"},
            {"id":4,"name":"amount","required":false,"type":"decimal(20, 4)"},
            {"id":5,"name":"uid","required":true,"type":"uuid"},
            {"id":6,"name":"tags","required":false,"type":{"type":"list","element-id":7,"element-required":true,"element":"string"}}
        ]}),
    )?;
    let mut native_schemas = BTreeMap::new();
    if fixture.mode == "writer-supplied" {
        ensure!(
            fixture.writer_rows.len() == fixture.snapshots.len()
                && fixture
                    .snapshots
                    .iter()
                    .all(|source| fixture.writer_rows.contains_key(&source.resource)),
            "FIXTURE_ROWS_REQUIRED"
        );
    }
    if fixture.mode.starts_with("writer") {
        for source in &fixture.snapshots {
            let selected_schema: iceberg::spec::Schema = if fixture.mode == "writer-supplied" {
                let schema:Value=db.get().await?.query_one("SELECT iceberg_schema FROM moria.native_dataset_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&source.resource.parse::<Uuid>()?]).await?.get(0);
                serde_json::from_value(schema)?
            } else {
                native_schema.clone()
            };
            native_schemas.insert(source.resource.clone(), selected_schema.clone());
            let namespace = IndexNamespace {
                warehouse_id: source.warehouse_id.clone(),
                table_id: source.table_id.clone(),
                revision: source.revision.clone().unwrap_or_else(|| "v1".into()),
            };
            let grant = Arc::new(
                DatasetIoGrant::acquire(db.clone(), session.clone(), task.lease.clone(), namespace)
                    .await?,
            );
            let store: Arc<dyn ObjectStore> =
                match fixture.s3_bindings.iter().find(|b| {
                    b.warehouse_id == source.warehouse_id && b.table_id == source.table_id
                }) {
                    Some(binding) => Arc::new(S3ReadStore::provision(binding.clone())?),
                    None => Arc::new(PosixStore::provision(
                        std::path::Path::new(&fixture.root),
                        &source.warehouse_id,
                        &source.table_id,
                        StorageLimits {
                            max_object_bytes: 128 * 1024 * 1024,
                            chunk_bytes: 1024 * 1024,
                        },
                    )?),
                };
            let mut writer = if fixture.graph_output {
                moria_runtime::dataset_writer::StreamingDatasetWriter::open_graph(
                    store,
                    grant,
                    selected_schema.clone(),
                    Cancellation::default(),
                )
                .await
            } else {
                moria_runtime::dataset_writer::StreamingDatasetWriter::open(
                    store,
                    grant,
                    selected_schema.clone(),
                    Cancellation::default(),
                )
                .await
            }
            .context("WRITER_OPEN")?;
            if fixture.mode == "writer-supplied" {
                let rows = fixture
                    .writer_rows
                    .get(&source.resource)
                    .ok_or_else(|| anyhow::anyhow!("FIXTURE_ROWS_REQUIRED"))?;
                if !rows.is_empty() {
                    let descriptors:Value=db.get().await?.query_one("SELECT columns FROM moria.native_dataset_bindings WHERE environment=$1 AND resource=$2 AND enabled",&[&session.environment(),&source.resource.parse::<Uuid>()?]).await?.get(0);
                    writer
                        .append(supplied_batch(&selected_schema, rows, &descriptors)?)
                        .await
                        .context("WRITER_APPEND")?;
                }
                native_parts.insert(
                    source.resource.clone(),
                    writer.finish().await.context("WRITER_FINISH")?,
                );
                continue;
            }
            let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&native_schema)?);
            let registry = moria_runtime::identity::IdentityRegistry { db: db.clone() };
            let cell: Uuid = source.resource.parse()?;
            registry.register_cell(cell, "WRITER_FIXTURE").await?;
            let domain = format!("writer-{}", source.resource);
            let ownership = registry.register_domain(&domain, cell).await?;
            let ids = registry
                .resolve(
                    &domain,
                    &(0..300).map(|i| i.to_string()).collect::<Vec<_>>(),
                    &ownership,
                )
                .await?;
            for start in (0..300).step_by(100) {
                let arrow_schema::DataType::List(child) = schema.field(5).data_type() else {
                    anyhow::bail!("UNSUPPORTED_OUTPUT_SCHEMA")
                };
                let mut tags = arrow_array::builder::ListBuilder::new(
                    arrow_array::builder::StringBuilder::new(),
                )
                .with_field(child.clone());
                for i in start..start + 100 {
                    if i % 7 == 0 {
                        tags.append(false);
                    } else {
                        tags.values().append_value("東京");
                        tags.values().append_value(format!("tag-{i}"));
                        tags.append(true);
                    }
                }
                let batch = arrow_array::RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(arrow_array::Int64Array::from_iter_values(
                            start..start + 100,
                        )),
                        Arc::new(arrow_array::StringArray::from_iter(
                            (start..start + 100).map(|i| {
                                if i % 7 == 0 {
                                    None
                                } else {
                                    Some(format!("native-東京-😀-{i}"))
                                }
                            }),
                        )),
                        Arc::new(arrow_array::BooleanArray::from_iter(
                            (start..start + 100).map(|i| Some(i % 2 == 0)),
                        )),
                        Arc::new(
                            arrow_array::Decimal128Array::from_iter_values(
                                (start..start + 100)
                                    .map(|i| i128::from(i) * 12345 - 9007199254740993_i128),
                            )
                            .with_precision_and_scale(20, 4)?,
                        ),
                        Arc::new(arrow_array::FixedSizeBinaryArray::try_from_iter(
                            ids[start as usize..(start + 100) as usize]
                                .iter()
                                .map(|id| id.as_bytes()),
                        )?),
                        Arc::new(tags.finish()),
                    ],
                )?;
                writer.append(batch).await.context("WRITER_APPEND")?;
                if fixture.mode == "writer-fail" {
                    ensure!(
                        writer
                            .append(arrow_array::RecordBatch::new_empty(Arc::new(
                                arrow_schema::Schema::empty()
                            )))
                            .await
                            .is_err()
                    );
                    ensure!(writer.finish().await.is_err());
                    event(json!({"event":"denied","error":"WRITER_FAILED","execution":id}));
                    return Ok(());
                }
            }
            native_parts.insert(
                source.resource.clone(),
                writer.finish().await.context("WRITER_FINISH")?,
            );
        }
    }
    execution
        .seal(
            &task.lease,
            if native_parts.is_empty() {
                json!({"complete":true})
            } else {
                json!({"datasetOutputs":native_parts})
            },
            &"b".repeat(64),
        )
        .await?;
    let claim = match (fixture.registered_host, fixture.registered_owner) {
        (Some(host), Some(owner)) => {
            execution
                .claim_registered(owner, host, &[Domain::Sync], true, Some(id))
                .await?
        }
        _ => {
            execution
                .claim(Uuid::new_v4(), &[Domain::Sync], true)
                .await?
        }
    }
    .unwrap();
    ensure!(claim.lease.execution == id);
    let session = match fixture.registered_host {
        Some(host) => moria_runtime::execution::job_session(&db, &claim.lease, host).await?,
        None => session,
    };
    let _renewal = capacity::Renewal::start(db.clone(), claim.lease.clone());
    // Keep special fault modes serial; valid grouped producers use bounded preparation.
    let parallel = if fixture.mode == "valid" && fixture.closure_limits.is_none() {
        2
    } else {
        1
    };
    let prepared = futures::stream::iter(&fixture.snapshots)
        .map(|fixture_snapshot| {
            let (db, fixture, session, lease, native_parts, native_schemas) =
                (&db, &fixture, &session, &claim.lease, &native_parts, &native_schemas);
            async move {
        let started = std::time::Instant::now();
        let grant = Arc::new(
            DatasetIoGrant::acquire(
                db.clone(),
                session.clone(),
                lease.clone(),
                IndexNamespace {
                    warehouse_id: fixture_snapshot.warehouse_id.clone(),
                    table_id: fixture_snapshot.table_id.clone(),
                    revision: fixture_snapshot
                        .revision
                        .clone()
                        .unwrap_or_else(|| "v1".into()),
                },
            )
            .await?,
        );
        ensure!(grant.resource() == fixture_snapshot.resource);
        let scratch = PosixStore::provision(
            std::path::Path::new(&fixture.root),
            &fixture_snapshot.warehouse_id,
            &fixture_snapshot.table_id,
            StorageLimits {
                max_object_bytes: 128 * 1024 * 1024,
                chunk_bytes: 1024 * 1024,
            },
        )?;
        let store: Arc<dyn ObjectStore> = match fixture.s3_bindings.iter().find(|binding| {
            binding.warehouse_id == fixture_snapshot.warehouse_id
                && binding.table_id == fixture_snapshot.table_id
        }) {
            Some(binding) => Arc::new(S3ReadStore::provision(binding.clone())?),
            None => Arc::new(scratch.clone()),
        };
        let cancel = Cancellation::default();
        if let Some(partset) = native_parts.get(&fixture_snapshot.resource) {
            if fixture.mode == "writer-schema-deny" {
                let wrong: iceberg::spec::Schema = serde_json::from_value(
                    json!({"type":"struct","schema-id":0,"fields":[{"id":1,"name":"different","required":true,"type":"long"}]}),
                )?;
                ensure!(
                    moria_runtime::dataset_writer::prepare_replacement(
                        store.clone(),
                        &scratch,
                        grant.clone(),
                        wrong,
                        std::slice::from_ref(partset),
                        ClosureLimits {
                            max_entries: 10000,
                            max_referenced_bytes: 1024 * 1024 * 1024,
                            max_spool_bytes: 32 * 1024 * 1024
                        },
                        cancel
                    )
                    .await
                    .is_err()
                );
                event(json!({"event":"denied","error":"OUTPUT_INVALID","execution":id}));
                return Ok(None);
            }
            let candidate = if fixture.graph_output {
                moria_runtime::dataset_writer::prepare_graph_replacement(
                    store.clone(),
                    &scratch,
                    grant.clone(),
                    native_schemas
                        .get(&fixture_snapshot.resource)
                        .ok_or_else(|| anyhow::anyhow!("FIXTURE_SCHEMA_INVALID"))?
                        .clone(),
                    std::slice::from_ref(partset),
                    ClosureLimits {
                        max_entries: 10000,
                        max_referenced_bytes: 1024 * 1024 * 1024,
                        max_spool_bytes: 32 * 1024 * 1024,
                    },
                    cancel,
                )
                .await
            } else {
                moria_runtime::dataset_writer::prepare_replacement(
                    store.clone(),
                    &scratch,
                    grant.clone(),
                    native_schemas
                        .get(&fixture_snapshot.resource)
                        .ok_or_else(|| anyhow::anyhow!("FIXTURE_SCHEMA_INVALID"))?
                        .clone(),
                    std::slice::from_ref(partset),
                    ClosureLimits {
                        max_entries: 10000,
                        max_referenced_bytes: 1024 * 1024 * 1024,
                        max_spool_bytes: 32 * 1024 * 1024,
                    },
                    cancel,
                )
                .await
            }
            .context("WRITER_PREPARE")?;
            return Ok(Some(candidate));
        }
        let spool_object = LogicalObject::new(
            &fixture_snapshot.warehouse_id,
            &fixture_snapshot.table_id,
            &Uuid::new_v4().to_string(),
        )?;
        let mut spool = ClosureSpool::create(
            &scratch,
            spool_object,
            fixture.closure_limits.unwrap_or(ClosureLimits {
                max_entries: 10000,
                max_referenced_bytes: 1024 * 1024 * 1024,
                max_spool_bytes: 32 * 1024 * 1024,
            }),
            grant.clone(),
            cancel.clone(),
        )
        .await?;
        let mut metadata = None;
        for pair in capacity::Objects::new(fixture_snapshot)? {
            let (name, path) = pair?;
            let object = LogicalObject::new(
                &fixture_snapshot.warehouse_id,
                &fixture_snapshot.table_id,
                &Uuid::new_v4().to_string(),
            )?;
            let content =
                capacity::transfer(store.as_ref(), &path, object, grant.clone(), cancel.clone())
                    .await?;
            if name == fixture_snapshot.metadata {
                metadata = Some(content.clone());
            } else {
                spool.append_page(&[ClosureEntry { name, content }]).await?;
            }
        }
        spool.seal(store.as_ref(), grant.clone()).await?;
        event(
            json!({"event":"closure_hashed","elapsedMillis":started.elapsed().as_millis().to_string()}),
        );
        if fixture.mode == "reader-window" {
            reader_window::probe(
                &db,
                store.clone(),
                grant.clone(),
                &spool,
                metadata.unwrap(),
                fixture_snapshot,
                cancel.clone(),
                id,
            )
            .await?;
            return Ok(None);
        }
        let candidate = prepare_snapshot(
            store.clone(),
            grant.clone(),
            &spool,
            SnapshotPin {
                metadata: metadata.unwrap(),
                table_uuid: fixture_snapshot.table_uuid,
                snapshot_id: fixture_snapshot.snapshot_id.parse()?,
            },
            cancel.clone(),
        )
        .await?;
        event(
            json!({"event":"candidate_registered","elapsedMillis":started.elapsed().as_millis().to_string()}),
        );
        let reference: String = db
            .get()
            .await?
            .query_one(
                "SELECT metadata_location FROM moria.runtime_snapshot_candidates WHERE id=$1",
                &[&candidate],
            )
            .await?
            .get(0);
        let reference = NativeSnapshotRef::decode(&reference)?;
        let root = snapshot_root(store.as_ref(), grant.clone(), &reference, cancel.clone()).await?;
        let mut current = root.last_page.clone();
        let mut count = 0u64;
        for ordinal in (0..root.pages).rev() {
            let page = snapshot_page(
                store.as_ref(),
                grant.clone(),
                &current.unwrap(),
                ordinal,
                cancel.clone(),
            )
            .await?;
            count += page.entries.len() as u64;
            current = page.previous;
        }
        ensure!(current.is_none() && count == root.proof.closure.entries);
        // A native reference is canonical, versioned, bounded and contains no host location.
        ensure!(
            NativeSnapshotRef::decode(&format!(
                "moria-snapshot-v2: {}",
                serde_json::to_string(&reference)?
            ))
            .is_err()
        );
        ensure!(
            NativeSnapshotRef::decode(&format!("moria-snapshot-v2:{}", "x".repeat(4096))).is_err()
        );
                Ok::<_, anyhow::Error>(Some(candidate))
            }
        }).buffered(parallel);
    futures::pin_mut!(prepared);
    let mut candidates = Vec::new();
    while let Some(candidate) = prepared.try_next().await? {
        let Some(candidate) = candidate else {
            return Ok(());
        };
        candidates.push(candidate);
    }
    event(json!({"event":"prepared","candidates":candidates,"lease":claim.lease}));
    if fixture.prepare_only {
        ensure!(
            fixture.graph_output && fixture.mode == "writer-supplied",
            "FIXTURE_MODE_INVALID"
        );
        return Ok(());
    }
    if fixture.mode != "valid" && fixture.mode != "valid-s3" && !fixture.mode.starts_with("writer")
    {
        proceed();
    }
    let result = publish_snapshots(
        &db,
        &session,
        &claim.lease,
        &candidates,
        fixture.checkpoint.clone(),
    )
    .await;
    if fixture.mode == "denied" {
        ensure!(result.is_err());
        event(json!({"event":"denied","error":format!("{:?}",result.unwrap_err())}));
        return Ok(());
    }
    let receipt = result?;
    if fixture.registered_host.is_some() {
        ensure!(
            Authority::current(&**db.get().await?, &session)
                .await
                .is_err(),
            "Completed private credential remained active"
        );
        let source = &fixture.snapshots[0];
        ensure!(
            DatasetIoGrant::acquire(
                db.clone(),
                session.clone(),
                claim.lease.clone(),
                IndexNamespace {
                    warehouse_id: source.warehouse_id.clone(),
                    table_id: source.table_id.clone(),
                    revision: source.revision.clone().unwrap_or_else(|| "v1".into())
                }
            )
            .await
            .is_err(),
            "Completed private credential reopened Dataset IO"
        );
    }
    let again = publish_snapshots(
        &db,
        &session,
        &claim.lease,
        &candidates,
        fixture.checkpoint.clone(),
    )
    .await?;
    ensure!(again == receipt);
    event(json!({"event":"receipt","receipt":receipt}));
    Ok(())
}
include!("../native-common/artifact.rs");
