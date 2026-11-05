//! Independent bounded-spill visibility bootstrap for the native unpartitioned exact-PK profile.
//! Equality-delete sequence comparison follows Iceberg v2 scan planning: data_seq < delete_seq.
use crate::storage::{ClosureSpool, ReadAuthority};
use anyhow::{Result, ensure};
use rusqlite::{Connection, params};
use std::sync::Arc;
use uuid::Uuid;
const KEY_MAX: usize = 4 * 1024 * 1024;
const PAGE_MAX: usize = 8 * 1024 * 1024;
const PAGE_ROWS: usize = 1000;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VisibleRow {
    pub key: Vec<u8>,
    pub file: String,
    pub row: u64,
    pub data_sequence: u64,
    pub row_hash: [u8; 32],
}
#[derive(Clone, Debug)]
pub struct EqualityKey {
    pub key: Vec<u8>,
    pub data_sequence: u64,
}
#[derive(Clone, Debug)]
pub struct PositionDelete {
    pub file: String,
    pub row: u64,
    pub data_sequence: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct BootstrapBudget {
    pub maximum_data_rows: u64,
    pub maximum_delete_rows: u64,
}
struct Tables {
    data: String,
    deletes: String,
    positions: String,
    visible: String,
}
/// Allocation and bytes remain in the already admitted private SQLite spool. No PostgreSQL/provider per row.
pub struct VisibilityBootstrap {
    spool: ClosureSpool,
    authority: Arc<dyn ReadAuthority>,
    db: Connection,
    tables: Tables,
    budget: BootstrapBudget,
    data_rows: u64,
    delete_rows: u64,
    sealed: bool,
    ready: bool,
    failed: bool,
}
impl VisibilityBootstrap {
    pub async fn create(
        spool: ClosureSpool,
        authority: Arc<dyn ReadAuthority>,
        budget: BootstrapBudget,
    ) -> Result<Self> {
        spool.check_current(authority.as_ref()).await?;
        ensure!(
            budget.maximum_data_rows <= 9_007_199_254_740_991
                && budget.maximum_delete_rows <= 9_007_199_254_740_991,
            "RESOURCE_LIMIT"
        );
        let db = spool.database()?;
        spool.assert_scope(&db)?;
        let suffix = Uuid::new_v4().simple().to_string();
        let tables = Tables {
            data: format!("visibility_data_{suffix}"),
            deletes: format!("visibility_delete_{suffix}"),
            positions: format!("visibility_position_{suffix}"),
            visible: format!("visibility_result_{suffix}"),
        };
        db.execute_batch(&format!("CREATE TABLE {data}(key BLOB NOT NULL,key_hash BLOB NOT NULL CHECK(length(key_hash)=32),file TEXT NOT NULL,ordinal INTEGER NOT NULL,data_sequence INTEGER NOT NULL,row_hash BLOB NOT NULL CHECK(length(row_hash)=32),PRIMARY KEY(file,ordinal)) WITHOUT ROWID;CREATE INDEX {data}_keys ON {data}(key,file,ordinal);CREATE TABLE {deletes}(key BLOB NOT NULL,data_sequence INTEGER NOT NULL,PRIMARY KEY(key,data_sequence)) WITHOUT ROWID;CREATE TABLE {positions}(file TEXT NOT NULL,ordinal INTEGER NOT NULL,data_sequence INTEGER NOT NULL,PRIMARY KEY(file,ordinal,data_sequence)) WITHOUT ROWID;CREATE TABLE {visible}(key BLOB PRIMARY KEY NOT NULL,key_hash BLOB NOT NULL CHECK(length(key_hash)=32),file TEXT NOT NULL,ordinal INTEGER NOT NULL,data_sequence INTEGER NOT NULL,row_hash BLOB NOT NULL CHECK(length(row_hash)=32),UNIQUE(file,ordinal)) WITHOUT ROWID;CREATE INDEX {visible}_route ON {visible}(key_hash,key);",data=tables.data,deletes=tables.deletes,positions=tables.positions,visible=tables.visible))?;
        Ok(Self {
            spool,
            authority,
            db,
            tables,
            budget,
            data_rows: 0,
            delete_rows: 0,
            sealed: false,
            ready: false,
            failed: false,
        })
    }
    async fn current(&self) -> Result<()> {
        self.spool.check_current(self.authority.as_ref()).await?;
        self.spool.assert_scope(&self.db)
    }
    pub async fn data_page(&mut self, rows: &[VisibleRow]) -> Result<()> {
        ensure!(!self.failed, "WRITER_FAILED");
        self.failed = true;
        self.data_page_checked(rows).await?;
        self.failed = false;
        Ok(())
    }
    async fn data_page_checked(&mut self, rows: &[VisibleRow]) -> Result<()> {
        self.current().await?;
        ensure!(
            !self.sealed
                && rows.len() <= PAGE_ROWS
                && rows
                    .iter()
                    .try_fold(0usize, |sum, row| sum
                        .checked_add(row.key.len() + row.file.len() + 64))
                    .is_some_and(|bytes| bytes <= PAGE_MAX),
            "RESOURCE_LIMIT"
        );
        ensure!(
            self.data_rows
                .checked_add(rows.len() as u64)
                .is_some_and(|rows| rows <= self.budget.maximum_data_rows),
            "RESOURCE_LIMIT"
        );
        let tx = self.db.transaction()?;
        {
            let mut insert = tx.prepare(&format!(
                "INSERT INTO {}(key,key_hash,file,ordinal,data_sequence,row_hash)VALUES(?1,?2,?3,?4,?5,?6)",
                self.tables.data
            ))?;
            for row in rows {
                ensure!(
                    !row.key.is_empty()
                        && row.key.len() <= KEY_MAX
                        && !row.file.is_empty()
                        && row.file.len() <= 4096
                        && row.file.starts_with("moria://")
                        && row.row <= 9_007_199_254_740_991
                        && row.data_sequence <= 9_007_199_254_740_991,
                    "DATASET_KEY_INDEX_INVALID"
                );
                self.spool.cancellation().check()?;
                map_sql(
                    insert.execute(params![
                        row.key,
                        super::key_run::key_route(&row.key).as_slice(),
                        row.file,
                        row.row as i64,
                        row.data_sequence as i64,
                        row.row_hash.as_slice()
                    ]),
                    "DATASET_KEY_INDEX_INVALID",
                )?;
            }
        }
        tx.commit()?;
        self.data_rows += rows.len() as u64;
        self.current().await
    }
    pub async fn delete_page(&mut self, keys: &[EqualityKey]) -> Result<()> {
        ensure!(!self.failed, "WRITER_FAILED");
        self.failed = true;
        self.delete_page_checked(keys).await?;
        self.failed = false;
        Ok(())
    }
    async fn delete_page_checked(&mut self, keys: &[EqualityKey]) -> Result<()> {
        self.current().await?;
        ensure!(
            !self.sealed
                && keys.len() <= PAGE_ROWS
                && keys
                    .iter()
                    .try_fold(0usize, |sum, key| sum.checked_add(key.key.len() + 8))
                    .is_some_and(|bytes| bytes <= PAGE_MAX)
                && self
                    .delete_rows
                    .checked_add(keys.len() as u64)
                    .is_some_and(|rows| rows <= self.budget.maximum_delete_rows),
            "RESOURCE_LIMIT"
        );
        let tx = self.db.transaction()?;
        {
            let mut insert = tx.prepare(&format!(
                "INSERT OR IGNORE INTO {}(key,data_sequence)VALUES(?1,?2)",
                self.tables.deletes
            ))?;
            for key in keys {
                ensure!(
                    !key.key.is_empty()
                        && key.key.len() <= KEY_MAX
                        && key.data_sequence <= 9_007_199_254_740_991,
                    "DATASET_KEY_INDEX_INVALID"
                );
                self.spool.cancellation().check()?;
                map_sql(
                    insert.execute(params![key.key, key.data_sequence as i64]),
                    "DATASET_KEY_INDEX_INVALID",
                )?;
            }
        }
        tx.commit()?;
        self.delete_rows += keys.len() as u64;
        self.current().await
    }
    pub async fn position_page(&mut self, rows: &[PositionDelete]) -> Result<()> {
        ensure!(!self.failed, "WRITER_FAILED");
        self.failed = true;
        self.position_page_checked(rows).await?;
        self.failed = false;
        Ok(())
    }
    async fn position_page_checked(&mut self, rows: &[PositionDelete]) -> Result<()> {
        self.current().await?;
        ensure!(
            !self.sealed
                && rows.len() <= PAGE_ROWS
                && rows
                    .iter()
                    .try_fold(0usize, |bytes, row| bytes.checked_add(row.file.len() + 24))
                    .is_some_and(|bytes| bytes <= PAGE_MAX)
                && self
                    .delete_rows
                    .checked_add(rows.len() as u64)
                    .is_some_and(|n| n <= self.budget.maximum_delete_rows),
            "RESOURCE_LIMIT"
        );
        let tx = self.db.transaction()?;
        {
            let mut insert = tx.prepare(&format!(
                "INSERT OR IGNORE INTO {}(file,ordinal,data_sequence)VALUES(?1,?2,?3)",
                self.tables.positions
            ))?;
            for row in rows {
                ensure!(
                    !row.file.is_empty()
                        && row.file.starts_with("moria://")
                        && row.file.len() <= 4096
                        && row.row <= 9_007_199_254_740_991
                        && row.data_sequence <= 9_007_199_254_740_991,
                    "DATASET_KEY_INDEX_INVALID"
                );
                self.spool.cancellation().check()?;
                map_sql(
                    insert.execute(params![row.file, row.row as i64, row.data_sequence as i64]),
                    "DATASET_KEY_INDEX_INVALID",
                )?;
            }
        }
        tx.commit()?;
        self.delete_rows += rows.len() as u64;
        self.current().await
    }
    /// The source is raw data files plus exact declared-key unpartitioned equality deletes and exact file/ordinal position deletes;
    /// imported accelerators are never used as the oracle. Partitioned/subset-key deletes require another profile.
    pub async fn seal(&mut self) -> Result<u64> {
        ensure!(!self.failed, "WRITER_FAILED");
        self.failed = true;
        let count = self.seal_checked().await?;
        self.failed = false;
        Ok(count)
    }
    async fn seal_checked(&mut self) -> Result<u64> {
        self.current().await?;
        ensure!(!self.sealed, "WRITER_CLOSED");
        self.sealed = true;
        let mut after: Option<VisibleRow> = None;
        let mut count = 0u64;
        loop {
            self.current().await?;
            let page = raw_visible_page(&self.db, &self.tables, after.as_ref())?;
            if page.is_empty() {
                break;
            }
            let tx = self.db.transaction()?;
            {
                let mut insert = tx.prepare(&format!(
                    "INSERT INTO {}(key,key_hash,file,ordinal,data_sequence,row_hash)VALUES(?1,?2,?3,?4,?5,?6)",
                    self.tables.visible
                ))?;
                for row in &page {
                    self.spool.cancellation().check()?;
                    map_sql(
                        insert.execute(params![
                            row.key,
                            super::key_run::key_route(&row.key).as_slice(),
                            row.file,
                            row.row as i64,
                            row.data_sequence as i64,
                            row.row_hash.as_slice()
                        ]),
                        "DATASET_PRIMARY_KEY_DUPLICATE",
                    )?;
                }
            }
            tx.commit()?;
            count += page.len() as u64;
            after = page.last().cloned();
        }
        self.current().await?;
        self.ready = true;
        Ok(count)
    }
    pub(crate) async fn route_page(&self, after: Option<&[u8]>) -> Result<Vec<VisibleRow>> {
        self.current().await?;
        ensure!(
            self.ready && !self.failed,
            "DATASET_KEY_INDEX_PROOF_REQUIRED"
        );
        let query = format!(
            "SELECT key,file,ordinal,data_sequence,row_hash FROM {}{} ORDER BY key_hash,key LIMIT 1000",
            self.tables.visible,
            if after.is_some() {
                " WHERE (key_hash,key)>(?1,?2)"
            } else {
                ""
            }
        );
        let mut statement = self.db.prepare(&query)?;
        let rows = match after {
            Some(key) => {
                statement.query(params![super::key_run::key_route(key).as_slice(), key])?
            }
            None => statement.query([])?,
        };
        let output = bounded_rows(rows)?;
        self.current().await?;
        Ok(output)
    }
    pub async fn page(&self, after: Option<&[u8]>) -> Result<Vec<VisibleRow>> {
        self.current().await?;
        ensure!(
            self.ready && !self.failed,
            "DATASET_KEY_INDEX_PROOF_REQUIRED"
        );
        let query = format!(
            "SELECT key,file,ordinal,data_sequence,row_hash FROM {}{} ORDER BY key LIMIT 1000",
            self.tables.visible,
            if after.is_some() { " WHERE key>?1" } else { "" }
        );
        let mut statement = self.db.prepare(&query)?;
        let rows = match after {
            Some(key) => statement.query([key])?,
            None => statement.query([])?,
        };
        let output = bounded_rows(rows)?;
        self.current().await?;
        Ok(output)
    }
}
fn map_sql(result: rusqlite::Result<usize>, constraint: &str) -> Result<usize> {
    match result {
        Ok(value) => Ok(value),
        Err(rusqlite::Error::SqliteFailure(code, _))
            if code.code == rusqlite::ErrorCode::DiskFull =>
        {
            anyhow::bail!("RESOURCE_LIMIT")
        }
        Err(rusqlite::Error::SqliteFailure(code, _))
            if code.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            anyhow::bail!("{constraint}")
        }
        Err(error) => Err(error.into()),
    }
}
fn bounded_rows(mut rows: rusqlite::Rows<'_>) -> Result<Vec<VisibleRow>> {
    let mut output = Vec::new();
    let mut bytes = 0usize;
    while let Some(row) = rows.next()? {
        let key: Vec<u8> = row.get(0)?;
        let file: String = row.get(1)?;
        ensure!(
            !key.is_empty() && key.len() <= KEY_MAX && file.len() <= 4096,
            "DATASET_KEY_INDEX_INVALID"
        );
        let size = key.len() + file.len() + 64;
        if !output.is_empty() && bytes + size > PAGE_MAX {
            break;
        }
        bytes += size;
        let hash: Vec<u8> = row.get(4)?;
        output.push(VisibleRow {
            key,
            file,
            row: u64::try_from(row.get::<_, i64>(2)?)?,
            data_sequence: u64::try_from(row.get::<_, i64>(3)?)?,
            row_hash: hash
                .try_into()
                .map_err(|_| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?,
        });
    }
    Ok(output)
}
fn raw_visible_page(
    db: &Connection,
    tables: &Tables,
    after: Option<&VisibleRow>,
) -> Result<Vec<VisibleRow>> {
    let query = format!(
        "SELECT d.key,d.file,d.ordinal,d.data_sequence,d.row_hash FROM {data} d WHERE {cursor}NOT EXISTS(SELECT 1 FROM {deletes} x WHERE x.key=d.key AND d.data_sequence<x.data_sequence)AND NOT EXISTS(SELECT 1 FROM {positions} x WHERE x.file=d.file AND x.ordinal=d.ordinal AND d.data_sequence<=x.data_sequence)ORDER BY d.key,d.file,d.ordinal LIMIT 1000",
        data = tables.data,
        deletes = tables.deletes,
        positions = tables.positions,
        cursor = if after.is_some() {
            "(d.key,d.file,d.ordinal)>(?1,?2,?3) AND "
        } else {
            ""
        }
    );
    let mut statement = db.prepare(&query)?;
    let rows = match after {
        Some(row) => statement.query(params![row.key, row.file, row.row as i64])?,
        None => statement.query([])?,
    };
    bounded_rows(rows)
}
impl Drop for VisibilityBootstrap {
    fn drop(&mut self) {
        let _ = self.db.execute_batch(&format!(
            "DROP TABLE IF EXISTS {};DROP TABLE IF EXISTS {};DROP TABLE IF EXISTS {};DROP TABLE IF EXISTS {};",
            self.tables.visible, self.tables.positions, self.tables.deletes, self.tables.data
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn equality_delete_sequence_is_strict_and_reinsert_survives() {
        let db = Connection::open_in_memory().unwrap();
        let tables = Tables {
            data: "data".into(),
            deletes: "deletes".into(),
            positions: "positions".into(),
            visible: "visible".into(),
        };
        db.execute_batch("CREATE TABLE data(key BLOB,file TEXT,ordinal INTEGER,data_sequence INTEGER,row_hash BLOB);CREATE INDEX data_keys ON data(key,file,ordinal);CREATE TABLE deletes(key BLOB,data_sequence INTEGER,PRIMARY KEY(key,data_sequence));CREATE TABLE positions(file TEXT,ordinal INTEGER,data_sequence INTEGER,PRIMARY KEY(file,ordinal,data_sequence));").unwrap();
        for (file, sequence, ordinal) in [
            ("moria://w/t/data/old", 4, 0),
            ("moria://w/t/data/same", 5, 0),
            ("moria://w/t/data/new", 6, 0),
        ] {
            db.execute(
                "INSERT INTO data VALUES(?1,?2,?3,?4,?5)",
                params![
                    b"key".as_slice(),
                    file,
                    ordinal,
                    sequence,
                    [1u8; 32].as_slice()
                ],
            )
            .unwrap();
        }
        db.execute("INSERT INTO deletes VALUES(?1,5)", [b"key".as_slice()])
            .unwrap();
        let rows = raw_visible_page(&db, &tables, None).unwrap();
        assert_eq!(
            rows.iter().map(|row| row.data_sequence).collect::<Vec<_>>(),
            vec![6, 5]
        );
        db.execute("INSERT INTO deletes VALUES(?1,6)", [b"key".as_slice()])
            .unwrap();
        let rows = raw_visible_page(&db, &tables, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].data_sequence, 6);
    }
    #[test]
    fn position_deletes_match_exact_file_ordinal_and_include_equal_sequence() {
        let db = Connection::open_in_memory().unwrap();
        let tables = Tables {
            data: "data".into(),
            deletes: "deletes".into(),
            positions: "positions".into(),
            visible: "visible".into(),
        };
        db.execute_batch("CREATE TABLE data(key BLOB,file TEXT,ordinal INTEGER,data_sequence INTEGER,row_hash BLOB);CREATE INDEX data_keys ON data(key,file,ordinal);CREATE TABLE deletes(key BLOB,data_sequence INTEGER,PRIMARY KEY(key,data_sequence));CREATE TABLE positions(file TEXT,ordinal INTEGER,data_sequence INTEGER,PRIMARY KEY(file,ordinal,data_sequence));").unwrap();
        for (file, ordinal, sequence, key) in [
            ("moria://w/t/data/a", 0, 5, b"a"),
            ("moria://w/t/data/a", 1, 5, b"b"),
            ("moria://w/t/data/b", 0, 5, b"c"),
            ("moria://w/t/data/c", 0, 6, b"d"),
        ] {
            db.execute(
                "INSERT INTO data VALUES(?1,?2,?3,?4,?5)",
                params![
                    key.as_slice(),
                    file,
                    ordinal,
                    sequence,
                    [7u8; 32].as_slice()
                ],
            )
            .unwrap();
        }
        db.execute_batch("INSERT INTO positions VALUES('moria://w/t/data/a',0,5),('moria://w/t/data/b',0,4),('moria://w/t/data/c',0,5)").unwrap();
        let rows = raw_visible_page(&db, &tables, None).unwrap();
        assert_eq!(
            rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>(),
            vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );
    }
}
