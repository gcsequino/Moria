use crate::database::Database;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Clone)]
pub struct IdentityRegistry {
    pub db: Database,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Ownership {
    pub cell: Uuid,
    pub fence: String,
    pub incarnation: Uuid,
}

impl IdentityRegistry {
    pub(crate) async fn peer(
        tx: &tokio_postgres::Transaction<'_>,
        fingerprint: &str,
        owner: Uuid,
        role: &str,
    ) -> Result<()> {
        tx.query_one("SELECT 1 FROM moria.native_service_peers p JOIN moria.indexing_instance i ON i.incarnation=p.incarnation WHERE fingerprint=$1 AND owner=$2 AND enabled AND expires_at>clock_timestamp() AND $3=ANY(roles) FOR SHARE OF p", &[&fingerprint,&owner,&role]).await?;
        Ok(())
    }

    pub async fn inspect(&self, domain: &str) -> Result<Ownership> {
        let row=self.db.get().await?.query_one("SELECT cell,fence::text,incarnation FROM moria.runtime_identity_domains WHERE domain=$1",&[&domain]).await?;
        Ok(Ownership {
            cell: row.get(0),
            fence: row.get(1),
            incarnation: row.get(2),
        })
    }
    pub async fn register_cell(&self, cell: Uuid, endpoint_ref: &str) -> Result<Uuid> {
        ensure!(
            !endpoint_ref.is_empty()
                && endpoint_ref.len() <= 128
                && endpoint_ref.as_bytes()[0].is_ascii_uppercase()
                && endpoint_ref
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'),
            "Invalid cell endpoint reference"
        );
        let row = self.db.get().await?.query_one(
            "INSERT INTO moria.runtime_cells(id,endpoint_ref,incarnation) SELECT $1,$2,incarnation FROM moria.indexing_instance ON CONFLICT(id) DO UPDATE SET endpoint_ref=EXCLUDED.endpoint_ref WHERE runtime_cells.incarnation=EXCLUDED.incarnation RETURNING incarnation",
            &[&cell, &endpoint_ref],
        ).await?;
        Ok(row.get(0))
    }

    pub async fn register_domain(&self, domain: &str, cell: Uuid) -> Result<Ownership> {
        ensure!(
            !domain.is_empty() && domain.len() <= 1024 && !domain.contains('\0'),
            "Invalid identity domain"
        );
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended('moria/identity-domain/'||$1,0))",
            &[&domain],
        )
        .await?;
        let row = tx.query_one(
            "INSERT INTO moria.runtime_identity_domains(domain,cell,incarnation) SELECT $1,id,incarnation FROM moria.runtime_cells WHERE id=$2 AND enabled AND incarnation=(SELECT incarnation FROM moria.indexing_instance) ON CONFLICT(domain) DO UPDATE SET domain=EXCLUDED.domain WHERE runtime_identity_domains.cell=EXCLUDED.cell AND runtime_identity_domains.incarnation=EXCLUDED.incarnation RETURNING cell,fence::text,incarnation",
            &[&domain, &cell],
        ).await?;
        tx.commit().await?;
        Ok(Ownership {
            cell: row.get(0),
            fence: row.get(1),
            incarnation: row.get(2),
        })
    }

    pub async fn resolve(
        &self,
        domain: &str,
        keys: &[String],
        owner: &Ownership,
    ) -> Result<Vec<Uuid>> {
        self.resolve_authorized(domain, keys, owner, None).await
    }

    pub(crate) async fn resolve_for_service(
        &self,
        domain: &str,
        keys: &[String],
        owner: &Ownership,
        fingerprint: &str,
        peer: Uuid,
    ) -> Result<Vec<Uuid>> {
        self.resolve_authorized(domain, keys, owner, Some((fingerprint, peer)))
            .await
    }

    async fn resolve_authorized(
        &self,
        domain: &str,
        keys: &[String],
        owner: &Ownership,
        peer: Option<(&str, Uuid)>,
    ) -> Result<Vec<Uuid>> {
        ensure!(
            !keys.is_empty() && keys.len() <= 1000,
            "Invalid identity batch"
        );
        let fence: i64 = owner.fence.parse()?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        if let Some((fingerprint, peer)) = peer {
            Self::peer(&tx, fingerprint, peer, "identity-owner").await?;
        }
        tx.query_one("SELECT moria.runtime_recovery_check()", &[])
            .await?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended('moria/identity-domain/'||$1,0))",
            &[&domain],
        )
        .await?;
        ensure!(
            !tx.query_one(
                "SELECT EXISTS(SELECT 1 FROM moria.native_action_identity_domains WHERE domain=$1)",
                &[&domain]
            )
            .await?
            .get::<_, bool>(0),
            "ACTION_IDENTITY_DOMAIN_RESERVED"
        );
        let rows = tx
            .query(
                "SELECT uid FROM moria.runtime_identity_resolve($1,$2,$3,$4,$5)",
                &[&domain, &keys, &owner.cell, &fence, &owner.incarnation],
            )
            .await?;
        ensure!(rows.len() == keys.len(), "Incomplete identity allocation");
        tx.commit().await?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    pub async fn transfer(
        &self,
        domain: &str,
        expected: &Ownership,
        next: Uuid,
    ) -> Result<Ownership> {
        self.transfer_authorized(domain, expected, next, None).await
    }

    pub(crate) async fn transfer_for_service(
        &self,
        domain: &str,
        expected: &Ownership,
        next: Uuid,
        fingerprint: &str,
        peer: Uuid,
    ) -> Result<Ownership> {
        self.transfer_authorized(domain, expected, next, Some((fingerprint, peer)))
            .await
    }

    async fn transfer_authorized(
        &self,
        domain: &str,
        expected: &Ownership,
        next: Uuid,
        peer: Option<(&str, Uuid)>,
    ) -> Result<Ownership> {
        let fence: i64 = expected.fence.parse()?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        if let Some((fingerprint, peer)) = peer {
            Self::peer(&tx, fingerprint, peer, "identity-admin").await?;
        }
        let row = tx.query_one(
            "UPDATE moria.runtime_identity_domains d SET cell=$5,fence=fence+1 WHERE domain=$1 AND cell=$2 AND fence=$3 AND incarnation=$4 AND incarnation=(SELECT incarnation FROM moria.indexing_instance) AND EXISTS(SELECT 1 FROM moria.runtime_cells c WHERE c.id=$5 AND c.enabled AND c.incarnation=d.incarnation) RETURNING cell,fence::text,incarnation",
            &[&domain,&expected.cell,&fence,&expected.incarnation,&next],
        ).await?;
        let result = Ownership {
            cell: row.get(0),
            fence: row.get(1),
            incarnation: row.get(2),
        };
        tx.execute("INSERT INTO moria.runtime_ownership_events(kind,resource,previous_cell,next_cell,fence,incarnation) VALUES('identity',$1,$2,$3,$4,$5)", &[&domain,&expected.cell,&next,&result.fence.parse::<i64>()?,&result.incarnation]).await?;
        tx.commit().await?;
        Ok(result)
    }
}

pub async fn cli(db: Database, arguments: Vec<String>) -> Result<()> {
    use tokio::io::{AsyncReadExt, stdin};
    let mut bytes = Vec::new();
    stdin()
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 1024 * 1024, "Identity request byte limit");
    let input: Value = serde_json::from_slice(&bytes)?;
    let registry = IdentityRegistry { db };
    let text = |name: &str| {
        input[name]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing identity field"))
    };
    let result = match arguments.first().map(String::as_str) {
        Some("register-cell") => {
            json!({"incarnation":registry.register_cell(Uuid::parse_str(text("cell")?)?,text("endpointRef")?).await?})
        }
        Some("register-domain") => serde_json::to_value(
            registry
                .register_domain(text("domain")?, Uuid::parse_str(text("cell")?)?)
                .await?,
        )?,
        Some("inspect") => serde_json::to_value(registry.inspect(text("domain")?).await?)?,
        Some("resolve") => {
            let keys: Vec<String> = serde_json::from_value(input["keys"].clone())?;
            let owner: Ownership = serde_json::from_value(input["owner"].clone())?;
            json!({"uids":registry.resolve(text("domain")?,&keys,&owner).await?})
        }
        Some("transfer") => serde_json::to_value(
            registry
                .transfer(
                    text("domain")?,
                    &serde_json::from_value(input["owner"].clone())?,
                    Uuid::parse_str(text("next")?)?,
                )
                .await?,
        )?,
        _ => anyhow::bail!("Unknown identity command"),
    };
    println!("{result}");
    Ok(())
}
