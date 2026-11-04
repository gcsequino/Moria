use crate::{database::Database, storage::Cancellation, values};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use tokio_postgres::Row;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Direction {
    Asc,
    Desc,
}
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SortKey {
    pub property: String,
    pub direction: Direction,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LayoutDefinition {
    pub name: String,
    pub sort: Vec<SortKey>,
    pub key_bytes: u32,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Budgets {
    pub row_bytes: u32,
    pub batch_rows: u32,
    pub batch_bytes: u32,
    pub commit_replay_rows: u32,
    pub commit_replay_bytes: u32,
    pub ddl_time_ms: u32,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Definition {
    pub version: u32,
    pub source_fields: Vec<String>,
    pub effective_fields: Vec<String>,
    pub layouts: Vec<LayoutDefinition>,
    pub reader_roles: Vec<String>,
    pub budgets: Budgets,
}
impl Definition {
    pub fn validate(&self, schema: &Value) -> Result<()> {
        self.validate_source(schema, &schema["properties"])
    }
    pub fn validate_source(&self, schema: &Value, source_properties: &Value) -> Result<()> {
        let descriptor = values::descriptor(
            &json!({"type":"object","properties":schema["properties"]}),
            &Value::Null,
            0,
        )?;
        let fields = descriptor["properties"]
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("SCHEMA_INVALID"))?;
        ensure!(
            self.version == 1
                && self.source_fields.len() + self.effective_fields.len() <= 128
                && !self.layouts.is_empty()
                && self.layouts.len() <= 16,
            "PROJECTION_INVALID"
        );
        let source = values::descriptor(
            &json!({"type":"object","properties":source_properties}),
            &Value::Null,
            0,
        )?;
        let source = source["properties"]
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("SCHEMA_INVALID"))?;
        for (selected, available) in [
            (&self.source_fields, source),
            (&self.effective_fields, fields),
        ] {
            let mut seen = BTreeSet::new();
            for field in selected {
                ensure!(
                    field.len() <= 256 && available.contains_key(field) && seen.insert(field),
                    "PROJECTION_INVALID"
                );
            }
        }
        let mut names = BTreeSet::new();
        for layout in &self.layouts {
            ensure!(
                !layout.name.is_empty()
                    && layout.name.len() <= 64
                    && layout
                        .name
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"-_".contains(&c))
                    && names.insert(&layout.name)
                    && layout.sort.len() <= 8
                    && (64..=1800).contains(&layout.key_bytes),
                "PROJECTION_INVALID"
            );
            let mut keys = BTreeSet::new();
            for key in &layout.sort {
                ensure!(
                    keys.insert(&key.property)
                        && self.effective_fields.contains(&key.property)
                        && fields[&key.property]["type"] != "array",
                    "PROJECTION_INVALID"
                );
            }
        }
        ensure!(
            !self.reader_roles.is_empty()
                && self.reader_roles.len() <= 32
                && self
                    .reader_roles
                    .iter()
                    .all(|name| !name.is_empty() && name.len() <= 63 && !name.contains('\0')),
            "PROJECTION_INVALID"
        );
        let b = &self.budgets;
        ensure!(
            (1..=16 * 1024 * 1024).contains(&b.row_bytes)
                && (1..=1000).contains(&b.batch_rows)
                && b.batch_bytes >= b.row_bytes
                && b.batch_bytes <= 64 * 1024 * 1024
                && (1..=256).contains(&b.commit_replay_rows)
                && b.commit_replay_bytes >= b.row_bytes
                && b.commit_replay_bytes <= 16 * 1024 * 1024
                && (1000..=900000).contains(&b.ddl_time_ms),
            "PROJECTION_INVALID"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TypedColumn {
    pub source: Option<String>,
    pub effective: Option<String>,
    pub descriptor: Value,
    #[serde(default)]
    pub source_descriptor: Option<Value>,
}
#[derive(Clone, Debug)]
pub struct Layout {
    pub name: String,
    pub sort: Vec<SortKey>,
    pub key_safe: String,
    pub current_index: String,
    pub history_index: String,
    pub overflow_index: String,
}
#[derive(Clone, Debug)]
pub struct ProjectionRelation {
    pub id: Uuid,
    pub stage: Uuid,
    pub source_revision: String,
    pub activation_revision: String,
    pub closed_revision: Option<String>,
    pub schema: Value,
    pub source_schema: Value,
    pub columns: BTreeMap<String, TypedColumn>,
    pub source_columns: BTreeMap<String, TypedColumn>,
    pub layouts: Vec<Layout>,
    pub current_rows: bool,
}
impl ProjectionRelation {
    pub fn table_sql(&self) -> String {
        format!("moria.\"runtime_projection_{}\"", self.id.simple())
    }
    pub fn layout(&self, sort: &[SortKey]) -> Option<&Layout> {
        self.layouts.iter().find(|layout| layout.sort == sort)
    }
}
#[derive(Clone, Debug)]
pub struct ProjectionPlan {
    pub pinned: ProjectionRelation,
    pub current: ProjectionRelation,
}

fn relation(row: &Row, revision: &str, current_revision: &str) -> Result<ProjectionRelation> {
    let id: Uuid = row.get("id");
    let definition: Definition = serde_json::from_value(row.get("definition"))?;
    let schema: Value = row.get("object_schema");
    let source_schema: Value = row.get("source_schema");
    definition.validate_source(&schema, &source_schema)?;
    let columns: BTreeMap<String, TypedColumn> = serde_json::from_value(row.get("columns"))?;
    let expected = column_map_source(&definition, &schema, &source_schema)?;
    ensure!(
        serde_json::to_value(&columns)? == serde_json::to_value(&expected)?,
        "PROJECTION_INVALID"
    );
    let activation: i64 = row.get("activation_revision");
    let closed: Option<i64> = row.get("closed_revision");
    let pinned: i64 = revision.parse()?;
    ensure!(
        activation <= pinned && closed.is_none_or(|closed| pinned < closed),
        "PROJECTION_WINDOW_INVALID"
    );
    let layouts = definition
        .layouts
        .into_iter()
        .enumerate()
        .map(|(ordinal, layout)| Layout {
            name: layout.name,
            sort: layout.sort,
            key_safe: format!("k{ordinal}"),
            current_index: format!("runtime_pi_{}_{}_c", id.simple(), ordinal),
            history_index: format!("runtime_pi_{}_{}_h", id.simple(), ordinal),
            overflow_index: format!("runtime_pi_{}_{}_o", id.simple(), ordinal),
        })
        .collect();
    let source_columns = columns
        .iter()
        .filter_map(|(name, column)| {
            column.source.as_ref().map(|source| {
                (
                    name.clone(),
                    TypedColumn {
                        source: Some(source.clone()),
                        effective: None,
                        descriptor: column
                            .source_descriptor
                            .clone()
                            .unwrap_or_else(|| column.descriptor.clone()),
                        source_descriptor: None,
                    },
                )
            })
        })
        .collect();
    Ok(ProjectionRelation {
        id,
        stage: row.get("stage"),
        source_revision: row.get::<_, i64>("source_revision").to_string(),
        activation_revision: activation.to_string(),
        closed_revision: closed.map(|n| n.to_string()),
        schema,
        source_schema,
        columns,
        source_columns,
        layouts,
        current_rows: revision == current_revision
            && closed.is_none()
            && row.get::<_, String>("status") == "active",
    })
}
pub fn column_map(
    definition: &Definition,
    schema: &Value,
) -> Result<BTreeMap<String, TypedColumn>> {
    column_map_source(definition, schema, &schema["properties"])
}
pub fn column_map_source(
    definition: &Definition,
    schema: &Value,
    source_properties: &Value,
) -> Result<BTreeMap<String, TypedColumn>> {
    definition.validate_source(schema, source_properties)?;
    let mut columns: BTreeMap<String, TypedColumn> = BTreeMap::new();
    for (is_source, fields) in [
        (true, &definition.source_fields),
        (false, &definition.effective_fields),
    ] {
        for (ordinal, field) in fields.iter().enumerate() {
            let column = columns.entry(field.clone()).or_insert_with(|| TypedColumn {
                source: None,
                effective: None,
                descriptor: schema["properties"][field].clone(),
                source_descriptor: None,
            });
            if is_source {
                column.source = Some(format!("s{ordinal}"));
                column.source_descriptor = Some(source_properties[field].clone());
                column.descriptor = source_properties[field].clone();
            } else {
                column.effective = Some(format!("e{ordinal}"));
                column.descriptor = schema["properties"][field].clone();
            }
        }
    }
    Ok(columns)
}

/// Return only complete exact-generation relations. A caller may use its bounded canonical path when absent.
pub async fn resolve(
    client: &tokio_postgres::Client,
    environment: &str,
    resource: &str,
    pinned_revision: &str,
    current_revision: &str,
) -> Result<Option<ProjectionPlan>> {
    let rows=client.query("SELECT pins.kind,g.revision AS source_revision,p.* FROM (VALUES('pinned',$3::text::bigint),('current',$4::text::bigint)) pins(kind,revision) JOIN LATERAL (SELECT revision,stage,object_schema FROM moria.source_generations WHERE environment=$1 AND object_type=$2 AND revision<=pins.revision ORDER BY revision DESC LIMIT 1) g ON TRUE JOIN moria.runtime_projections p ON p.stage=g.stage AND p.activation_revision=g.revision AND p.object_schema=g.object_schema AND p.status IN ('active','retired') JOIN moria.indexing_builds origin ON origin.id=p.stage AND p.source_schema=COALESCE(origin.definition->'sourceColumns',p.object_schema->'properties') JOIN moria.runtime_projection_bindings b ON b.environment=p.environment AND b.resource=p.resource AND b.enabled WHERE p.environment=$1 AND p.resource=$2 AND (pins.kind='pinned' OR (p.status='active' AND p.binding_revision=b.revision AND p.definition=b.definition AND p.applied_through>=COALESCE((SELECT max(revision) FROM moria.overlay_versions WHERE environment=$1 AND object_type=$2 AND revision<=$4::text::bigint),0)))",
        &[&environment,&resource,&pinned_revision,&current_revision]).await?;
    let Some(pinned) = rows
        .iter()
        .find(|row| row.get::<_, String>("kind") == "pinned")
    else {
        return Ok(None);
    };
    let Some(current) = rows
        .iter()
        .find(|row| row.get::<_, String>("kind") == "current")
    else {
        return Ok(None);
    };
    Ok(Some(ProjectionPlan {
        pinned: relation(pinned, pinned_revision, current_revision)?,
        current: relation(current, current_revision, current_revision)?,
    }))
}

/// Bulk work remains outside the environment publication barrier and uses the existing native source owner/fence.
async fn step(
    db: &Database,
    stage: Uuid,
    time_ms: u32,
    statement: &str,
    parameters: &[&(dyn ToSql + Sync)],
) -> Result<Row> {
    let mut client = db.get().await?;
    let tx = client.transaction().await?;
    let remaining: i64 = tx.query_one("SELECT LEAST($2::bigint,ceil(extract(epoch FROM (deadline-clock_timestamp()))*1000)::bigint) FROM moria.indexing_builds WHERE id=$1", &[&stage,&i64::from(time_ms)]).await?.get(0);
    ensure!(remaining > 0, "DEADLINE_EXCEEDED");
    tx.query_one("SELECT set_config('statement_timeout',$1,true),set_config('maintenance_work_mem','64MB',true)",&[&format!("{remaining}ms")]).await?;
    let row = tx.query_one(statement, parameters).await?;
    tx.commit().await?;
    Ok(row)
}

pub async fn prepare(
    db: &Database,
    stage: Uuid,
    owner: Uuid,
    fence: i64,
    cancel: Cancellation,
) -> Result<Option<Uuid>> {
    cancel.check()?;
    let row=db.get().await?.query_one("SELECT b.definition->>'projectionRevision' AS expected,p.revision,p.definition,s.object_schema,COALESCE(b.definition->'sourceColumns',s.object_schema->'properties') AS source_schema FROM moria.indexing_builds b JOIN moria.source_stages s ON s.id=b.id LEFT JOIN moria.runtime_projection_bindings p ON p.environment=b.environment AND p.resource=b.target AND p.enabled WHERE b.id=$1",&[&stage]).await?;
    let expected: Option<String> = row.get("expected");
    let revision: Option<String> = row.get("revision");
    ensure!(expected == revision, "BINDING_CHANGED");
    let Some(_) = revision else { return Ok(None) };
    let definition: Definition = serde_json::from_value(row.get("definition"))?;
    definition.validate_source(
        &row.get::<_, Value>("object_schema"),
        &row.get::<_, Value>("source_schema"),
    )?;
    let id: Uuid = step(
        db,
        stage,
        definition.budgets.ddl_time_ms,
        "SELECT moria.runtime_projection_begin($1,$2,$3)",
        &[&stage, &owner, &fence],
    )
    .await?
    .get(0);
    println!(
        "{}",
        json!({"event":"projection_preparing","stage":stage,"projection":id})
    );
    loop {
        cancel.check()?;
        let complete: bool = step(
            db,
            stage,
            definition.budgets.ddl_time_ms,
            "SELECT moria.runtime_projection_build_page($1,$2,$3)",
            &[&id, &owner, &fence],
        )
        .await?
        .get(0);
        println!(
            "{}",
            json!({"event":"projection_page","stage":stage,"projection":id,"complete":complete})
        );
        if complete {
            break;
        }
    }
    for ordinal in 0..definition.layouts.len() {
        cancel.check()?;
        step(
            db,
            stage,
            definition.budgets.ddl_time_ms,
            "SELECT moria.runtime_projection_build_index($1,$2,$3,$4)",
            &[&id, &owner, &fence, &(ordinal as i32)],
        )
        .await?;
    }
    loop {
        cancel.check()?;
        let complete: bool = step(
            db,
            stage,
            definition.budgets.ddl_time_ms,
            "SELECT moria.runtime_projection_replay($1,$2,$3,NULL)",
            &[&id, &owner, &fence],
        )
        .await?
        .get(0);
        if complete {
            break;
        }
    }
    println!(
        "{}",
        json!({"event":"projection_ready","stage":stage,"projection":id})
    );
    Ok(Some(id))
}
