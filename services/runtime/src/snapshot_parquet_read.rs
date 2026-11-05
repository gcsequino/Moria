//! Bounded, version-pinned Parquet metadata for native graph readers.
use crate::{snapshot::parquet_bounds, storage::ObjectReadWindow};
use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};
use std::{ops::Range, sync::Arc};
pub(crate) struct BoundedParquetRead {
    size: u64,
    window: ObjectReadWindow,
}
impl BoundedParquetRead {
    pub(crate) fn new(window: ObjectReadWindow, size: u64) -> Self {
        Self { window, size }
    }
}
fn external(error: anyhow::Error) -> parquet::errors::ParquetError {
    parquet::errors::ParquetError::External(error.into())
}
impl AsyncFileReader for BoundedParquetRead {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            if range.start > range.end || range.end > self.size {
                return Err(external(anyhow::anyhow!("PARQUET_RANGE_LIMIT")));
            }
            self.window.read(range).await.map_err(external)
        }
        .boxed()
    }
    fn get_metadata<'a>(
        &'a mut self,
        _: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let size = self.size;
            if size < 12 {
                return Err(external(anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT")));
            }
            let footer = self.get_bytes(size - 8..size).await?;
            if &footer[4..] != b"PAR1" {
                return Err(external(anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT")));
            }
            let length = u64::from(u32::from_le_bytes(footer[..4].try_into().unwrap()));
            if length > 8 * 1024 * 1024 || length > size - 12 {
                return Err(external(anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT")));
            }
            let raw = self.get_bytes(size - 8 - length..size - 8).await?;
            parquet_bounds::footer(&raw).map_err(external)?;
            let metadata = ParquetMetaDataReader::new()
                .load_and_finish(&mut *self, size)
                .await?;
            parquet_bounds::arrow_metadata(&metadata).map_err(external)?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}
