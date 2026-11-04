use crate::database::Database;
use crate::storage::{Cancellation, LogicalObject, ReadAuthority, read_posix_range};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::io::{
    FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use rustix::fs::{CWD, Mode, OFlags, ResolveFlags, openat2};
use std::{
    fs::File,
    ops::Range,
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

fn denied() -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::DataInvalid,
        "Scoped indexing I/O denied",
    )
}

#[derive(Clone, Debug)]
pub struct Scope {
    pub db: Database,
    pub build: Uuid,
    pub task: Option<Uuid>,
    pub owner: Uuid,
    pub fence: i64,
}

impl Scope {
    /// Private, minimal accepted-job authority; ordinary bearer authentication stays strict.
    pub async fn active_session(&self) -> Result<crate::authority::VerifiedSession> {
        use sha2::{Digest, Sha256};
        self.check().await?;
        let client = self.db.get().await?;
        let random = format!("{}{}", Uuid::new_v4(), Uuid::new_v4());
        let candidate = format!("{:x}", Sha256::digest(random.as_bytes()));
        let hash: String = client
            .query_one(
                "SELECT moria.runtime_indexing_session_issue($1,$2,$3,$4,$5)",
                &[
                    &self.build,
                    &self.task,
                    &self.owner,
                    &self.fence,
                    &candidate,
                ],
            )
            .await?
            .get(0);
        let row = client.query_one("SELECT environment,context->>'principal',context->>'application' FROM moria.indexing_builds WHERE id=$1", &[&self.build]).await?;
        crate::authority::Authority::stored_session_in(
            &client,
            &hash,
            &row.get::<_, String>(0),
            &row.get::<_, String>(1),
            &row.get::<_, String>(2),
        )
        .await
    }
    pub async fn check(&self) -> Result<()> {
        self.db
            .guard_claim(self.build, self.task, self.owner, self.fence)
            .await
    }
}

#[async_trait]
impl ReadAuthority for Scope {
    async fn check_read(&self, object: &LogicalObject) -> Result<()> {
        ensure!(
            object == &LogicalObject::new("indexing", &self.build.to_string(), "pinned")?,
            "BINDING_MISMATCH"
        );
        self.check().await
    }
}

#[derive(Debug, Clone)]
pub struct ScopedFiles {
    root: PathBuf,
    directory: Arc<File>,
    scope: Scope,
}

impl ScopedFiles {
    pub fn new(root: &str, scope: Scope) -> Result<Self> {
        ensure!(Path::new(root).is_absolute(), "Absolute warehouse required");
        let fd = openat2(
            CWD,
            root,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        )?;
        Ok(Self {
            root: PathBuf::from(root),
            directory: Arc::new(File::from(fd)),
            scope,
        })
    }

    pub fn file_io(&self) -> FileIO {
        FileIOBuilder::new(Arc::new(self.clone())).build()
    }

    fn open(&self, path: &str) -> Result<File> {
        let path = Path::new(path.strip_prefix("file://").unwrap_or(path));
        let relative = path.strip_prefix(&self.root)?;
        ensure!(
            relative
                .components()
                .all(|c| matches!(c, Component::Normal(_))),
            "Invalid warehouse path"
        );
        let fd = openat2(
            self.directory.as_ref(),
            relative,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        )?;
        let file = File::from(fd);
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file() && metadata.nlink() == 1,
            "Only singly linked regular input files are allowed"
        );
        Ok(file)
    }
}

impl serde::Serialize for ScopedFiles {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "Live storage grants cannot be serialized",
        ))
    }
}
impl<'de> serde::Deserialize<'de> for ScopedFiles {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "Live storage grants must be injected",
        ))
    }
}

#[typetag::serde(name = "moria-scoped-files-v1")]
impl StorageFactory for ScopedFiles {
    fn build(&self, _: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.clone()))
    }
}

#[derive(Debug)]
struct Reader {
    file: Arc<File>,
    scope: Scope,
}

#[async_trait]
impl FileRead for Reader {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        let object = LogicalObject::new("indexing", &self.scope.build.to_string(), "pinned")
            .map_err(|_| denied())?;
        read_posix_range(
            self.file.clone(),
            range,
            1024 * 1024,
            &self.scope,
            &object,
            &Cancellation::default(),
        )
        .await
        .map_err(|_| denied())
    }
}

#[typetag::serde(name = "moria-scoped-files-v1")]
#[async_trait]
impl Storage for ScopedFiles {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        self.scope.check().await.map_err(|_| denied())?;
        self.open(path).map(|_| true).map_err(|_| denied())
    }
    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        self.scope.check().await.map_err(|_| denied())?;
        Ok(FileMetadata {
            size: self
                .open(path)
                .map_err(|_| denied())?
                .metadata()
                .map_err(|_| denied())?
                .len(),
        })
    }
    async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
        let size = self.metadata(path).await?.size;
        if size > 8 * 1024 * 1024 {
            return Err(denied());
        }
        self.reader(path).await?.read(0..size).await
    }
    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        self.scope.check().await.map_err(|_| denied())?;
        Ok(Box::new(Reader {
            file: Arc::new(self.open(path).map_err(|_| denied())?),
            scope: self.scope.clone(),
        }))
    }
    fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
        self.open(path).map_err(|_| denied())?;
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }
    async fn write(&self, _: &str, _: Bytes) -> iceberg::Result<()> {
        Err(denied())
    }
    async fn writer(&self, _: &str) -> iceberg::Result<Box<dyn FileWrite>> {
        Err(denied())
    }
    async fn delete(&self, _: &str) -> iceberg::Result<()> {
        Err(denied())
    }
    async fn delete_prefix(&self, _: &str) -> iceberg::Result<()> {
        Err(denied())
    }
    async fn delete_stream(&self, _: BoxStream<'static, String>) -> iceberg::Result<()> {
        Err(denied())
    }
    fn new_output(&self, _: &str) -> iceberg::Result<OutputFile> {
        Err(denied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn kernel_scope_rejects_escapes_symlinks_and_non_regular_files() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("valid"), b"fixture").unwrap();
        std::fs::write(outside.path().join("secret"), b"private").unwrap();
        std::fs::hard_link(outside.path().join("secret"), root.path().join("hardlink")).unwrap();
        std::fs::create_dir(root.path().join("directory")).unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("escape")).unwrap();
        let pool = deadpool_postgres::Pool::builder(deadpool_postgres::Manager::new(
            tokio_postgres::Config::new(),
            tokio_postgres::NoTls,
        ))
        .max_size(4)
        .build()
        .unwrap();
        let scope = Scope {
            db: Database::from_pool(pool),
            build: Uuid::new_v4(),
            task: None,
            owner: Uuid::new_v4(),
            fence: 1,
        };
        let files = ScopedFiles::new(root.path().to_str().unwrap(), scope.clone()).unwrap();
        assert!(
            files
                .open(root.path().join("valid").to_str().unwrap())
                .is_ok()
        );
        assert!(
            files
                .open(root.path().join("hardlink").to_str().unwrap())
                .is_err()
        );
        assert!(
            files
                .open(root.path().join("directory").to_str().unwrap())
                .is_err()
        );
        assert!(
            files
                .open(outside.path().join("secret").to_str().unwrap())
                .is_err()
        );
        assert!(
            files
                .open(root.path().join("escape/secret").to_str().unwrap())
                .is_err()
        );
        assert!(
            files
                .open(root.path().join("../secret").to_str().unwrap())
                .is_err()
        );
        assert!(files.open(root.path().to_str().unwrap()).is_err());
        assert!(ScopedFiles::new(root.path().join("escape").to_str().unwrap(), scope).is_err());
    }
}
