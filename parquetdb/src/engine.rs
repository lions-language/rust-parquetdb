use anyhow::Result;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;

pub trait StorageEngine {
    fn write(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

use crate::writer::file_parquet::ParquetFileWriter;

pub struct EngineX {
    writer: ParquetFileWriter,
}

impl EngineX {
    pub fn open(path: &str, schema: Schema) -> Result<Self> {
        Ok(Self {
            writer: ParquetFileWriter::try_new(path, schema)?,
        })
    }
}

impl super::engine::StorageEngine for EngineX {
    fn write(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()> {
        self.writer.write_batch(batch)
    }

    fn flush(&mut self) -> Result<()> {
        // parquet 是 row-group 粒度
        self.writer.close()?;
        Ok(())
    }
}
