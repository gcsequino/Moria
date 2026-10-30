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

#[derive(Clone, Debug)]
pub struct Database {
    pool: Pool,
    lease: Arc<OnceLock<Arc<deadpool_postgres::Object>>>,
}

impl Database {
    pub fn from_pool(pool: Pool) -> Self {
        Self {
            pool,
            lease: Arc::new(OnceLock::new()),
        }
    }

    pub async fn get(&self) -> Result<deadpool_postgres::Object> {
        ensure!(
            !self.lease.get().is_some_and(|client| client.is_closed()),
            "Schema lease lost"
        );
        let client = self.pool.get().await?;
        let admission = async {
            ensure!(
                client
                    .query_one("SELECT pg_try_advisory_lock_shared(731804135219)", &[])
                    .await?
                    .get::<_, bool>(0),
                "Database migration in progress"
            );
            let expected: Value = serde_json::from_str(include_str!("../schema.json"))?;
            let row = client
                .query_one(
                    "SELECT version,checksum FROM moria.schema_version WHERE singleton",
                    &[],
                )
                .await?;
            ensure!(
                row.get::<_, i32>(0) == expected["version"].as_i64().unwrap() as i32
                    && row.get::<_, String>(1) == expected["checksum"].as_str().unwrap(),
                "Incompatible database schema"
            );
            ensure!(
                !self.lease.get().is_some_and(|client| client.is_closed()),
                "Schema lease lost"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = admission {
            drop(deadpool_postgres::Object::take(client));
            return Err(error);
        }
        Ok(client)
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
        config.options("-c statement_timeout=30000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=30000");
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
        self.guard(build).await?;
        let client = self.get().await?;
        let valid = if let Some(task) = task {
            client.query_opt("SELECT 1 FROM moria.indexing_tasks WHERE id=$1 AND build=$2 AND owner=$3 AND fence=$4 AND status='running' AND lease_until>clock_timestamp()", &[&task,&build,&owner,&fence]).await?.is_some()
        } else {
            client.query_opt("SELECT 1 FROM moria.indexing_builds WHERE id=$1 AND owner=$2 AND fence=$3 AND lease_until>clock_timestamp()", &[&build,&owner,&fence]).await?.is_some()
        };
        ensure!(valid, "Lease lost");
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

pub fn safe_error(error: &anyhow::Error) -> &'static str {
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
        "Unsupported Iceberg partition/name mapping"
        | "Iceberg delete files are unsupported by this reader release" => "UNSUPPORTED_CAPABILITY",
        _ => "INDEXING_FAILED",
    }
}
