use crate::{
    database::Database,
    identity::{IdentityRegistry, Ownership},
};
use anyhow::{Result, ensure};
use serde::Serialize;
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Placement {
    pub environment: String,
    pub owner: Ownership,
    pub endpoint_ref: String,
    pub serving: bool,
    pub leases_fenced: bool,
}
#[derive(Clone)]
pub struct EnvironmentDirectory {
    pub db: Database,
}
impl EnvironmentDirectory {
    pub async fn inspect(&self, environment: &str) -> Result<Placement> {
        let row=self.db.get().await?.query_opt("SELECT p.cell,p.fence::text,p.incarnation,c.endpoint_ref,p.serving AND c.enabled AND p.incarnation=c.incarnation AND p.incarnation=i.incarnation,p.leases_fenced FROM moria.runtime_placements p JOIN moria.runtime_cells c ON c.id=p.cell CROSS JOIN moria.indexing_instance i WHERE p.environment=$1",&[&environment]).await?.ok_or_else(||anyhow::anyhow!("CELL_OWNERSHIP_LOST"))?;
        Ok(Placement {
            environment: environment.into(),
            owner: Ownership {
                cell: row.get(0),
                fence: row.get(1),
                incarnation: row.get(2),
            },
            endpoint_ref: row.get(3),
            serving: row.get(4),
            leases_fenced: row.get(5),
        })
    }
    /// Initial placement is paused and fences any previous unmanaged attempts before activation.
    pub async fn place(&self, environment: &str, cell: Uuid) -> Result<Placement> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        let row=tx.query_one("INSERT INTO moria.runtime_placements(environment,cell,incarnation) SELECT $1,c.id,c.incarnation FROM moria.runtime_cells c JOIN moria.indexing_instance i ON i.incarnation=c.incarnation WHERE c.id=$2 AND c.enabled ON CONFLICT(environment) DO UPDATE SET environment=EXCLUDED.environment WHERE runtime_placements.cell=EXCLUDED.cell AND NOT runtime_placements.serving RETURNING cell,fence::text,incarnation",&[&environment,&cell]).await?;
        let expected = Ownership {
            cell: row.get(0),
            fence: row.get(1),
            incarnation: row.get(2),
        };
        tx.commit().await?;
        self.fence(environment, &expected, None).await?;
        self.inspect(environment).await
    }
    pub(crate) async fn freeze(
        &self,
        environment: &str,
        expected: &Ownership,
        next: Uuid,
        peer: Option<(&str, Uuid)>,
    ) -> Result<Placement> {
        let fence = fence(expected)?;
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        if let Some((fingerprint, owner)) = peer {
            IdentityRegistry::peer(&tx, fingerprint, owner, "environment-admin").await?;
        }
        tx.query_one(
            "SELECT moria.runtime_environment_freeze($1,$2,$3,$4,$5)",
            &[
                &environment,
                &expected.cell,
                &fence,
                &expected.incarnation,
                &next,
            ],
        )
        .await?;
        tx.commit().await?;
        let expected = Ownership {
            cell: next,
            fence: (fence + 1).to_string(),
            incarnation: expected.incarnation,
        };
        self.fence(environment, &expected, peer).await?;
        self.inspect(environment).await
    }
    async fn fence(
        &self,
        environment: &str,
        expected: &Ownership,
        peer: Option<(&str, Uuid)>,
    ) -> Result<()> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        if let Some((fingerprint, owner)) = peer {
            IdentityRegistry::peer(&tx, fingerprint, owner, "environment-admin").await?;
        }
        tx.query_one(
            "SELECT moria.runtime_environment_fence_leases($1,$2,$3,$4)",
            &[
                &environment,
                &expected.cell,
                &fence(expected)?,
                &expected.incarnation,
            ],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    pub(crate) async fn activate(
        &self,
        environment: &str,
        expected: &Ownership,
        peer: Option<(&str, Uuid)>,
    ) -> Result<Placement> {
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        if let Some((fingerprint, owner)) = peer {
            IdentityRegistry::peer(&tx, fingerprint, owner, "environment-admin").await?;
        }
        tx.query_one(
            "SELECT moria.runtime_environment_activate($1,$2,$3,$4)",
            &[
                &environment,
                &expected.cell,
                &fence(expected)?,
                &expected.incarnation,
            ],
        )
        .await?;
        tx.commit().await?;
        self.inspect(environment).await
    }
}
fn fence(owner: &Ownership) -> Result<i64> {
    let value = owner.fence.parse::<i64>()?;
    ensure!(
        value > 0 && value.to_string() == owner.fence && value < i64::MAX,
        "CELL_OWNERSHIP_LOST"
    );
    Ok(value)
}
pub async fn cli(db: Database, args: Vec<String>) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    tokio::io::stdin()
        .take(65537)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 65536, "INPUT_INVALID");
    let input: Value = serde_json::from_slice(&bytes)?;
    let text = |key: &str| {
        input[key]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("INPUT_INVALID"))
    };
    let environment = text("environment")?;
    ensure!(
        !environment.is_empty() && environment.len() <= 1024,
        "INPUT_INVALID"
    );
    let directory = EnvironmentDirectory { db };
    let result = match args.first().map(String::as_str) {
        Some("place") => {
            directory
                .place(environment, Uuid::parse_str(text("cell")?)?)
                .await?
        }
        Some("inspect") => directory.inspect(environment).await?,
        Some("freeze") => {
            directory
                .freeze(
                    environment,
                    &serde_json::from_value(input["owner"].clone())?,
                    Uuid::parse_str(text("next")?)?,
                    None,
                )
                .await?
        }
        Some("activate") => {
            directory
                .activate(
                    environment,
                    &serde_json::from_value(input["owner"].clone())?,
                    None,
                )
                .await?
        }
        _ => anyhow::bail!("INPUT_INVALID"),
    };
    println!("{}", json!(result));
    Ok(())
}
