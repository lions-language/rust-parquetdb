pub(crate) mod file_parquet;
pub(crate) mod memory_merge_parquet;

use anyhow::Result;
use arrow2::{array::Array, chunk::Chunk};

pub(crate) trait ParquetWriter {
    fn write_batch(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()>;
    fn close(self) -> Result<()>;
}
