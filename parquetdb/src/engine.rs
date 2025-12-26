use std::sync::Arc;

use anyhow::Result;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;

pub trait StorageEngine {
    fn write(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

use crate::writer::file_parquet::ParquetFileWriter;

pub struct Engine {
    path: String,
    schema: Arc<Schema>,
    writer: Option<ParquetFileWriter>,
}

impl Engine {
    pub fn open(path: &str, schema: Arc<Schema>) -> Result<Self> {
        Ok(Self {
            path: path.to_string(),
            schema: schema.clone(),
            writer: Some(ParquetFileWriter::try_new(path, schema.clone())?),
        })
    }
}

impl super::engine::StorageEngine for Engine {
    fn write(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()> {
        self.writer.as_mut().unwrap().write_batch(batch)
    }

    fn flush(&mut self) -> Result<()> {
        let writer = self.writer.take().unwrap();
        // parquet 是 row-group 粒度
        writer.close()?;
        self.writer = Some(ParquetFileWriter::try_new(&self.path, self.schema.clone())?);
        Ok(())
    }
}
