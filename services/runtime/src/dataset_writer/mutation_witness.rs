//! Private staged evidence derived from accepted inputs and actual qualified output writes.
use crate::storage::{ClosureSpool, SealedObject};
use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, params};
use uuid::Uuid;

pub(super) struct Witness {
    pub(super) spool: ClosureSpool,
    files: String,
    positions: String,
}
impl Witness {
    pub(super) fn new(spool: ClosureSpool) -> Result<Self> {
        let suffix = Uuid::new_v4().simple().to_string();
        let files = format!("mutation_files_{suffix}");
        let positions = format!("mutation_positions_{suffix}");
        let db = spool.database()?;
        spool.assert_scope(&db)?;
        db.execute_batch(&format!(
            "CREATE TABLE {files}(name TEXT PRIMARY KEY,kind TEXT NOT NULL,operation TEXT,rows INTEGER NOT NULL,
                content TEXT NOT NULL,manifest_seen INTEGER NOT NULL DEFAULT 0,footprint_seen INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID;
             CREATE TABLE {positions}(file TEXT NOT NULL,position INTEGER NOT NULL,seen INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(file,position)) WITHOUT ROWID"
        ))?;
        Ok(Self {
            spool,
            files,
            positions,
        })
    }
    pub(super) fn file(
        &self,
        name: &str,
        kind: &str,
        operation: Option<&str>,
        rows: u64,
        content: &SealedObject,
    ) -> Result<()> {
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        db.execute(
            &format!(
                "INSERT INTO {}(name,kind,operation,rows,content) VALUES(?1,?2,?3,?4,?5)",
                self.files
            ),
            params![
                name,
                kind,
                operation,
                i64::try_from(rows)?,
                serde_json::to_string(content)?
            ],
        )?;
        Ok(())
    }
    pub(super) fn positions(&self, file: &str, rows: &[u64]) -> Result<()> {
        let mut db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let tx = db.transaction()?;
        {
            let mut statement = tx.prepare(&format!(
                "INSERT INTO {}(file,position)VALUES(?1,?2)",
                self.positions
            ))?;
            for row in rows {
                statement.execute(params![file, i64::try_from(*row)?])?;
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub(super) fn verify_manifest(
        &self,
        name: &str,
        kind: &str,
        rows: u64,
        content: &SealedObject,
    ) -> Result<()> {
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let changed = db.execute(&format!(
            "UPDATE {} SET manifest_seen=1 WHERE name=?1 AND kind=?2 AND rows=?3 AND content=?4 AND manifest_seen=0",
            self.files), params![name,kind,i64::try_from(rows)?,serde_json::to_string(content)?])?;
        ensure!(changed == 1, "DATASET_MUTATION_PROOF_CHANGED");
        Ok(())
    }
    pub(super) fn verify_footprint(
        &self,
        name: &str,
        operation: &str,
        rows: u64,
        content: &SealedObject,
    ) -> Result<()> {
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let changed = db.execute(
            &format!(
                "UPDATE {} SET footprint_seen=1 WHERE name=?1 AND kind='data' AND operation=?2
             AND rows=?3 AND content=?4 AND footprint_seen=0",
                self.files
            ),
            params![
                name,
                operation,
                i64::try_from(rows)?,
                serde_json::to_string(content)?
            ],
        )?;
        ensure!(changed == 1, "DATASET_MUTATION_PROOF_CHANGED");
        Ok(())
    }
    pub(super) fn verify_positions(&self, file: &str, rows: &[u64]) -> Result<()> {
        let mut db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let tx = db.transaction()?;
        {
            let mut statement = tx.prepare(&format!(
                "UPDATE {} SET seen=1 WHERE file=?1 AND position=?2 AND seen=0",
                self.positions
            ))?;
            for row in rows {
                ensure!(
                    statement.execute(params![file, i64::try_from(*row)?])? == 1,
                    "DATASET_MUTATION_PROOF_CHANGED"
                );
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub(super) fn next_file(&self, after: &str) -> Result<Option<(String, u64)>> {
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let value: Option<(String,i64)> = db.query_row(&format!(
            "SELECT file,count(*) FROM {} WHERE file>?1 GROUP BY file ORDER BY file LIMIT 1",self.positions),
            [after], |row|Ok((row.get(0)?,row.get(1)?))).optional()?;
        value
            .map(|(file, count)| Ok((file, u64::try_from(count)?)))
            .transpose()
    }
    pub(super) fn position_page(&self, file: &str, after: i64) -> Result<Vec<u64>> {
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let mut query = db.prepare(&format!(
            "SELECT position FROM {} WHERE file=?1 AND position>?2 ORDER BY position LIMIT 1000",
            self.positions
        ))?;
        let rows = query.query_map(params![file, after], |row| row.get::<_, i64>(0))?;
        rows.map(|row| Ok(u64::try_from(row?)?)).collect()
    }
    pub(super) fn finish(&self) -> Result<()> {
        let db = self.spool.database()?;
        self.spool.assert_scope(&db)?;
        let missing: bool = db.query_row(&format!(
            "SELECT EXISTS(SELECT 1 FROM {} WHERE manifest_seen=0 OR (kind='data' AND footprint_seen=0))
                OR EXISTS(SELECT 1 FROM {} WHERE seen=0)",self.files,self.positions), [], |row|row.get(0))?;
        ensure!(!missing, "DATASET_MUTATION_PROOF_CHANGED");
        Ok(())
    }
}
impl Drop for Witness {
    fn drop(&mut self) {
        if let Ok(db) = self.spool.database() {
            let _ = db.execute_batch(&format!(
                "DROP TABLE IF EXISTS {};DROP TABLE IF EXISTS {}",
                self.files, self.positions
            ));
        }
    }
}
