use anyhow::{Context, Result, bail, ensure};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use postgres_native_tls::MakeTlsConnector;
use serde_json::Value;
use std::{
    env,
    str::FromStr,
    sync::{Arc, OnceLock},
};
use tokio_postgres::{Config, NoTls};
use uuid::Uuid;

struct PendingCheckout(Option<deadpool_postgres::Object>);
impl Drop for PendingCheckout {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            drop(deadpool_postgres::Object::take(client));
        }
    }
}

#[derive(Clone, Debug)]
pub struct Database {
    pool: Pool,
    restore_operator: bool,
    lease: Arc<OnceLock<Arc<deadpool_postgres::Object>>>,
}

impl Database {
    pub fn from_pool(pool: Pool) -> Self {
        Self {
            pool,
            restore_operator: false,
            lease: Arc::new(OnceLock::new()),
        }
    }

    pub async fn get(&self) -> Result<deadpool_postgres::Object> {
        ensure!(
            !self.lease.get().is_some_and(|client| client.is_closed()),
            "Schema lease lost"
        );
        let mut checkout = PendingCheckout(Some(self.pool.get().await?));
        let client = checkout.0.as_ref().unwrap();
        let admission = async {
            let expected: Value = serde_json::from_str(include_str!("../schema.json"))?;
            client
                .query_one(
                    if self.restore_operator {
                        "SELECT moria.runtime_schema_admit($1,$2)"
                    } else {
                        "SELECT moria.runtime_connection_admit($1,$2)"
                    },
                    &[
                        &(expected["version"].as_i64().unwrap() as i32),
                        &expected["checksum"].as_str().unwrap(),
                    ],
                )
                .await?;
            ensure!(
                !self.lease.get().is_some_and(|client| client.is_closed()),
                "Schema lease lost"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        admission?;
        Ok(checkout.0.take().unwrap())
    }
    pub(crate) fn configured_recovery() -> Result<Self> {
        let mut db = Self::configured()?;
        db.restore_operator = true;
        Ok(db)
    }
    pub(crate) async fn restore_bootstrap_connection(
        &self,
    ) -> Result<deadpool_postgres::ClientWrapper> {
        ensure!(
            self.restore_operator
                && self.lease.get().is_none()
                && env::var("MORIA_ISOLATED_RESTORE").as_deref() == Ok("1"),
            "RECOVERY_CONFIG_INVALID"
        );
        let client = self.pool.get().await?;
        ensure!(
            client
                .query_one(
                    "SELECT has_database_privilege(session_user,current_database(),'CREATE')",
                    &[]
                )
                .await?
                .get::<_, bool>(0),
            "RECOVERY_OPERATOR_REQUIRED"
        );
        Ok(deadpool_postgres::Object::take(client))
    }
    pub fn configured() -> Result<Self> {
        let reference = env::var("MORIA_INDEXING_DATABASE_SECRET_REF")
            .context("MORIA_INDEXING_DATABASE_SECRET_REF is required")?;
        ensure!(
            !reference.is_empty()
                && reference
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "Invalid database secret reference"
        );
        let secret =
            env::var(reference).map_err(|_| anyhow::anyhow!("Database secret unavailable"))?;
        ensure!(
            (secret.starts_with("postgresql://") || secret.starts_with("postgres://"))
                && !secret.contains('?'),
            "Database requires a URL without query options"
        );
        let mut config = Config::from_str(&secret)
            .map_err(|_| anyhow::anyhow!("Invalid database configuration"))?;
        config.application_name("moria-indexing");
        config.connect_timeout(std::time::Duration::from_secs(10));
        let cell = env::var("MORIA_NATIVE_CELL_ID")
            .ok()
            .map(|v| Uuid::parse_str(&v))
            .transpose()?;
        let options = format!(
            "-c statement_timeout=30000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=30000{}",
            cell.map(|v| format!(" -c moria.cell_id={v}"))
                .unwrap_or_default()
        );
        config.options(&options);
        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Custom("SELECT pg_advisory_unlock_all()".into()),
        };
        let manager = match env::var("MORIA_INDEXING_DATABASE_TLS")
            .as_deref()
            .unwrap_or("verify")
        {
            "disable" => Manager::from_config(config, NoTls, manager_config),
            "verify" => {
                config.ssl_mode(tokio_postgres::config::SslMode::Require);
                let mut builder = native_tls::TlsConnector::builder();
                if let Ok(path) = env::var("MORIA_INDEXING_DATABASE_CA") {
                    builder.add_root_certificate(native_tls::Certificate::from_pem(
                        &std::fs::read(path)?,
                    )?);
                }
                Manager::from_config(
                    config,
                    MakeTlsConnector::new(builder.build()?),
                    manager_config,
                )
            }
            _ => bail!("Invalid database TLS mode"),
        };
        let integer = |name: &str, default: usize| -> Result<usize> {
            match env::var(name) {
                Ok(value) => value
                    .parse()
                    .map_err(|_| anyhow::anyhow!("Invalid connection budget")),
                Err(_) => Ok(default),
            }
        };
        let maximum = integer("MORIA_DATABASE_POOL_MAX", 4)?;
        let replicas = integer("MORIA_DATABASE_REPLICAS", 1)?;
        let budget = integer("MORIA_DATABASE_CONNECTION_BUDGET", 32)?;
        ensure!(
            (4..=128).contains(&maximum)
                && replicas >= 1
                && maximum
                    .checked_mul(replicas)
                    .and_then(|v| v.checked_add(1))
                    .is_some_and(|v| v <= budget),
            "Invalid aggregate database connection budget"
        );
        Ok(Self::from_pool(
            Pool::builder(manager).max_size(maximum).build()?,
        ))
    }

    pub async fn guard(&self, build: Uuid) -> Result<()> {
        self.get()
            .await?
            .query_one("SELECT moria.indexing_check($1)", &[&build])
            .await?;
        Ok(())
    }

    pub async fn guard_claim(
        &self,
        build: Uuid,
        task: Option<Uuid>,
        owner: Uuid,
        fence: i64,
    ) -> Result<()> {
        let client = self.get().await?;
        client
            .query_one(
                "SELECT moria.indexing_claim_check($1,$2,$3,$4)",
                &[&build, &task, &owner, &fence],
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn recovery_schema_lease(&self) -> Result<()> {
        ensure!(self.restore_operator, "RECOVERY_CONFIG_INVALID");
        let client = self.get().await?;
        ensure!(
            client
                .query_one(
                    "SELECT has_schema_privilege(session_user,'moria','CREATE')",
                    &[]
                )
                .await?
                .get::<_, bool>(0),
            "RECOVERY_OPERATOR_REQUIRED"
        );
        ensure!(
            self.lease.set(Arc::new(client)).is_ok(),
            "Schema lease already initialized"
        );
        Ok(())
    }

    pub async fn schema_lease(&self) -> Result<()> {
        let client = self.get().await?;
        let row = client.query_one("SELECT rolsuper OR rolbypassrls OR rolcreatedb OR rolcreaterole OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='moria' AND pg_has_role(current_user,c.relowner,'MEMBER')) OR has_schema_privilege(current_user,'moria','CREATE') OR EXISTS(SELECT 1 FROM pg_roles r WHERE (r.rolsuper OR r.rolbypassrls OR r.rolcreatedb OR r.rolcreaterole) AND pg_has_role(current_user,r.oid,'MEMBER')) FROM pg_roles WHERE rolname=current_user", &[]).await?;
        ensure!(
            !row.get::<_, bool>(0),
            "Indexing runtime requires a non-owning, non-administrative database role"
        );
        ensure!(
            self.lease.set(Arc::new(client)).is_ok(),
            "Schema lease already initialized"
        );
        Ok(())
    }

    pub async fn fail_owned(
        &self,
        build: Uuid,
        task: Option<Uuid>,
        owner: Uuid,
        fence: i64,
        code: &str,
    ) -> Result<()> {
        let mut client = self.get().await?;
        let tx = client.transaction().await?;
        let owned = if let Some(task) = task {
            tx.query_opt("SELECT 1 FROM moria.indexing_tasks WHERE id=$1 AND build=$2 AND owner=$3 AND fence=$4 AND status='running' AND lease_until>clock_timestamp() FOR UPDATE",&[&task,&build,&owner,&fence]).await?.is_some()
        } else {
            tx.query_opt("SELECT 1 FROM moria.indexing_builds WHERE id=$1 AND owner=$2 AND fence=$3 AND lease_until>clock_timestamp() FOR UPDATE",&[&build,&owner,&fence]).await?.is_some()
        };
        if owned {
            tx.execute("UPDATE moria.indexing_builds SET status='failed',error=$2 WHERE id=$1 AND status NOT IN ('active','failed','cancelled','superseded','conflicted')", &[&build, &code]).await?;
            tx.execute("UPDATE moria.indexing_attempts SET outcome=$5,finished_at=clock_timestamp() WHERE build=$1 AND unit=$2 AND owner=$3 AND fence=$4 AND outcome='running'",&[&build,&task.unwrap_or(build),&owner,&fence,&code]).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

pub fn authority_error_code(error: &anyhow::Error) -> Option<&'static str> {
    const CODES: &[&str] = &[
        "EXECUTION_INACTIVE",
        "IMMUTABLE_JOB_AUTHORITY",
        "IMMUTABLE_SERVICE_PURPOSE",
        "JOB_ACCEPTANCE_REQUIRED",
        "JOB_ACTOR_MISMATCH",
        "JOB_CAPABILITY_MISMATCH",
        "JOB_CREDENTIAL_COLLISION",
        "JOB_DELEGATION_DEPTH",
        "JOB_DELEGATION_INACTIVE",
        "JOB_DELEGATION_REQUIRED",
        "JOB_LEASE_LOST",
        "JOB_ORIGIN_MISMATCH",
        "JOB_ORIGIN_UNSUPPORTED",
        "JOB_PARENT_INACTIVE",
        "JOB_RECEIPT_AUTHORITY_REQUIRED",
        "SERVICE_PURPOSE_REVOKED",
        "SERVICE_ENVIRONMENT_MISMATCH",
        "CELL_OWNERSHIP_LOST",
        "INDEXING_TASK_AUTHORITY_REQUIRED",
        "SCRATCH_CLEANUP_PRIVILEGE_REQUIRED",
        "ENGINE_IO_INVALID",
        "ENGINE_INPUT_ALREADY_OPEN",
        "ENGINE_SCAN_INVALID",
        "ENGINE_INPUT_LIMIT",
        "ENGINE_OUTPUT_ALREADY_OPEN",
        "ENGINE_OUTPUT_NOT_OPEN",
        "ENGINE_OUTPUT_LIMIT",
        "ENGINE_IPC_INCOMPLETE",
        "ENGINE_OUTPUT_INCOMPLETE",
        "ENGINE_IPC_INVALID",
        "ENGINE_IPC_UNSUPPORTED",
        "ENGINE_IPC_COMPLEXITY",
        "ENGINE_FRAME_LIMIT",
        "SCRATCH_UNAVAILABLE",
        "SCRATCH_NOT_CLEAN",
        "SCRATCH_CONTAINER_NOT_TERMINAL",
        "SCRATCH_CREATION_UNCERTAIN",
        "SCRATCH_CLEANUP_UNCERTAIN",
        "SCRATCH_INSTANCE_MISMATCH",
        "IMMUTABLE_SCRATCH_SLOT",
        "INDEXING_ACCEPTANCE_REQUIRED",
        "INDEXING_DELEGATION_INACTIVE",
        "JOB_TOKEN_COLLISION",
        "FUNCTION_SANDBOX_REQUIRED",
        "ACTION_IDENTITY_DOMAIN_RESERVED",
        "LIFECYCLE_CONFIG_INVALID",
        "NATIVE_DRAIN_TIMEOUT",
        "NATIVE_DRAINING",
        "RESTORE_DISPATCH_DISABLED",
        "RECOVERY_LEASE_LOST",
        "PARQUET_ALLOCATION_LIMIT",
        "HISTORY_EXPIRED",
        "HISTORY_PIN_INVALID",
        "HISTORY_FLOOR_RESERVED",
        "HISTORY_OPERATOR_REQUIRED",
        "HISTORY_POLICY_RESERVED",
        "HISTORY_POLICY_CONFLICT",
        "HISTORY_POLICY_REVOKED",
        "HISTORY_PIN_BELOW_FLOOR",
        "HISTORY_BUSY",
        "GC_OPERATOR_REQUIRED",
        "RECOVERY_OPERATOR_REQUIRED",
        "DELIVERY_RECONCILIATION_STALE",
        "MODEL_ORIGIN_IMMUTABLE",
        "SNAPSHOT_SCRATCH_UNAVAILABLE",
        "CLOSURE_BUDGET_UNAVAILABLE",
        "DATASET_PRIMARY_KEY_INVALID",
        "DATASET_PRIMARY_KEY_MISSING",
        "DATASET_PRIMARY_KEY_NULL",
        "DATASET_PRIMARY_KEY_DUPLICATE",
        "DATASET_PRIMARY_KEY_LIMIT",
        "DATASET_PRIMARY_KEY_PROOF_REQUIRED",
        "DATASET_WRITE_MODE_INVALID",
        "DATASET_WRITE_MODE_DENIED",
        "DATASET_WRITE_CAPABILITY_REQUIRED",
        "DATASET_PUBLICATION_CONTRACT_REQUIRED",
        "BROWSER_ORIGIN_RESERVED",
        "BROWSER_ORIGIN_INVALID",
        "UNDECLARED_DATA_PROVENANCE",
        "RECOVERY_TARGET_NOT_FRESH",
        "STORAGE_PROTOCOL_ERROR",
        "MULTIPART_DISCOVERY_UNSUPPORTED",
        "MULTIPART_ABORT_UNSUPPORTED",
        "IMMUTABLE_MULTIPART_INTENT",
        "UPLOAD_RETIRED",
        "IMMUTABLE_UPLOAD",
        "IMMUTABLE_UPLOAD_TOMBSTONE",
        "DATASET_SNAPSHOT_EXPIRED",
        "DATASET_SNAPSHOT_NOT_FOUND",
        "SECRET_BINDING_INVALID",
        "SECRET_SOURCE_CHANGED",
        "SECRET_CONSUMER_DENIED",
        "SECRET_APPROVAL_REQUIRED",
        "SECRET_APPROVAL_REVOKED",
        "SECRET_APPROVAL_INVALID",
        "SECRET_APPROVAL_IMMUTABLE",
        "SECRET_APPROVAL_CONFLICT",
        "SECRET_CONFIGURATION_UNAVAILABLE",
        "GC_EXPIRATION_FAILED",
        "GC_ADMISSION_BUSY",
        "GC_PIN_INVALID",
        "GC_UNTRACKED_ROOT",
        "GC_PRODUCER_TOMBSTONED",
        "GC_OBJECT_OWNERSHIP_CHANGED",
        "GC_OBJECT_RETIRED",
        "GC_MEMBER_LIMIT",
        "RECOVERY_INVALID",
        "GC_ADMISSION_LOST",
        "GC_POLICY_INVALID",
        "GC_UNSUPPORTED_CLASS",
        "GC_POLICY_CONFLICT",
        "GC_POLICY_UNKNOWN",
        "GC_POLICY_CHANGED",
        "GC_RETENTION_PINNED",
        "GC_PRODUCER_ACTIVE",
        "GC_REFERENCE_INVENTORY_REQUIRED",
        "GC_FOREIGN_CUSTODY",
        "GC_LEASE_LOST",
        "GC_REFERENCE_PRESENT",
        "GC_OBJECT_DENIED",
        "GC_PROVIDER_CHANGED",
        "GC_DEADLINE",
        "IMMUTABLE_RETENTION_POLICY",
        "IMMUTABLE_GC_CANDIDATE",
        "CONDITIONAL_DELETE_UNSUPPORTED",
        "STORAGE_DELETE_UNCERTAIN",
        "RECOVERY_TARGET_NOT_EMPTY",
        "RECOVERY_DATABASE_ROLE_REQUIRED",
        "RECOVERY_RESTORE_FAILED",
        "CLI_APPROVAL_REQUIRED",
        "CLI_APPROVAL_INVALID",
        "CLI_KEY_REUSE_FORBIDDEN",
        "RECOVERY_MANIFEST_IMMUTABLE",
        "RECOVERY_KEYS_REQUIRED",
        "RECOVERY_KEY_INVALID",
        "RECOVERY_KEY_CHANGED",
        "RECOVERY_CONFIG_INVALID",
        "RECOVERY_MANIFEST_CONFLICT",
        "RECOVERY_DUMP_LIMIT",
        "RECOVERY_DUMP_FAILED",
        "RECOVERY_SNAPSHOT_LOST",
        "RECOVERY_CONFIGURATION_FAILED",
        "RECOVERY_ENUMERATION_FAILED",
        "RECOVERY_METADATA_FAILED",
        "RECOVERY_COPY_FAILED",
        "RECOVERY_INVENTORY_FAILED",
        "RECOVERY_KEYS_FAILED",
        "RECOVERY_DEADLINE",
        "RECOVERY_ENGINE_UNSUPPORTED",
        "RECOVERY_CONTENT_CONFLICT",
        "RECOVERY_CONTENT_DEADLINE",
        "RECOVERY_CONTENT_INCOMPLETE",
        "RECOVERY_CONTENT_INVALID",
        "RECOVERY_CONTENT_LIMIT",
        "RECOVERY_CONTENT_MISMATCH",
        "RECOVERY_LIMITS_REQUIRED",
        "RECOVERY_MEMBERSHIP_DENIED",
        "RECOVERY_NAMESPACE_CHANGED",
        "RECOVERY_NAMESPACE_INVALID",
        "RECOVERY_NAMESPACE_RESERVED",
        "RECOVERY_NAMESPACE_LIMIT",
        "RECOVERY_OBJECT_DENIED",
        "RECOVERY_PROVIDER_CHANGED",
        "RECOVERY_SNAPSHOT_CLEANUP_FAILED",
        "RECOVERY_TARGET_CHANGED",
        "RECOVERY_TARGET_CONFLICT",
        "RECOVERY_TARGET_NOT_ISOLATED",
        "RECOVERY_TARGET_REQUIRED",
        "RECOVERY_TARGET_REVISION_CHANGED",
        "RESTORE_RECONCILIATION_REQUIRED",
    ];
    for cause in error.chain() {
        let text = cause
            .downcast_ref::<tokio_postgres::Error>()
            .and_then(|error| error.as_db_error())
            .map(|error| error.message().to_owned())
            .unwrap_or_else(|| cause.to_string());
        if let Some(code) = CODES.iter().find(|code| **code == text) {
            return Some(code);
        }
    }
    None
}

pub fn safe_error(error: &anyhow::Error) -> &'static str {
    if let Some(code) = authority_error_code(error) {
        return code;
    }
    if let Some(code) = crate::automations::domain_error_code(error) {
        return code;
    }
    if let Some(code) = crate::evaluations::domain_error_code(error) {
        return code;
    }
    if let Some(code) = crate::pipelines::domain_error_code(error) {
        return code;
    }
    let domain = crate::search::error_code(error);
    if domain != "INTERNAL" {
        return domain;
    }
    for cause in error.chain() {
        if let Some(db) = cause
            .downcast_ref::<tokio_postgres::Error>()
            .and_then(|e| e.as_db_error())
        {
            return match db.message() {
                "AUTHORIZATION_CHANGED" | "UNAUTHENTICATED" | "BINDING_CHANGED" => {
                    "AUTHORIZATION_CHANGED"
                }
                "LEASE_LOST" | "BUILD_INACTIVE" => "LEASE_LOST",
                "HEAD_CONFLICT" => "HEAD_CONFLICT",
                "MIGRATION_REQUIRED" => "MIGRATION_REQUIRED",
                "INCARNATION_CHANGED" => "INCARNATION_CHANGED",
                _ if db.code().code() == "23505" => "DUPLICATE_UID",
                _ => "INDEXING_DATABASE_ERROR",
            };
        }
    }
    match error.root_cause().to_string().as_str() {
        "Duplicate canonical UID" => "DUPLICATE_UID",
        "Unregistered UID" => "UID_UNREGISTERED",
        "UID home conflict" => "UID_HOME_CONFLICT",
        "Lease lost" => "LEASE_LOST",
        "Incomplete generation" | "Scan row count mismatch" => "GENERATION_INCOMPLETE",
        "Scan plan byte budget exceeded" => "SCAN_PLAN_LIMIT",
        "DECODED_ROW_LIMIT" => "DECODED_ROW_LIMIT",
        "DECODED_BATCH_LIMIT" => "DECODED_BATCH_LIMIT",
        "METADATA_BYTE_LIMIT" => "METADATA_BYTE_LIMIT",
        "PARQUET_RANGE_LIMIT" => "PARQUET_RANGE_LIMIT",
        "PROJECTION_INVALID" => "PROJECTION_INVALID",
        "SCHEMA_CHANGED" => "SCHEMA_CHANGED",
        "Unsupported Iceberg partition/name mapping"
        | "Iceberg delete files are unsupported by this reader release" => "UNSUPPORTED_CAPABILITY",
        _ => "INDEXING_FAILED",
    }
}

#[cfg(test)]
mod error_tests {
    #[test]
    fn public_diagnostics_return_only_known_error_codes() {
        for code in ["MODEL_CANCELLED", "SEARCH_NOT_READY", "PROPERTY_FORBIDDEN"] {
            assert_eq!(super::safe_error(&anyhow::anyhow!(code.to_string())), code);
        }
        assert_eq!(
            super::safe_error(&anyhow::anyhow!("secret endpoint and token")),
            "INDEXING_FAILED"
        );
    }
}
