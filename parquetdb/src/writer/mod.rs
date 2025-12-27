pub(crate) mod file_parquet;
pub(crate) mod memory_merge_parquet;

pub use file_parquet::ParquetFileWriter;
pub use memory_merge_parquet::MemoryMergeParquetWriter;

use std::sync::Arc;

use anyhow::Result;
use arrow2::{array::Array, chunk::Chunk, datatypes::Schema};

pub trait ParquetWriter {
    fn try_new(path: &str, schema: Arc<Schema>) -> Result<Self>
    where
        Self: Sized;
    fn write_batch(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()>;
    fn close(self) -> Result<Self>
    where
        Self: Sized;
}
