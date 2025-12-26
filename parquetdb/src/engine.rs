use std::sync::Arc;

use anyhow::Result;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;

pub trait StorageEngine {
    fn write(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

use crate::writer::ParquetWriter;

pub use crate::writer::MemoryMergeParquetWriter;
pub use crate::writer::ParquetFileWriter;

pub struct Engine<Writer: ParquetWriter> {
    path: String,
    schema: Arc<Schema>,
    writer: Option<Writer>,
}

impl<Writer: ParquetWriter> Engine<Writer> {
    pub fn open(path: &str, schema: Arc<Schema>) -> Result<Self> {
        Ok(Self {
            path: path.to_string(),
            schema: schema.clone(),
            writer: Some(Writer::try_new(path, schema.clone())?),
        })
    }
}

impl<Writer: ParquetWriter> super::engine::StorageEngine for Engine<Writer> {
    fn write(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()> {
        self.writer.as_mut().unwrap().write_batch(batch)
    }

    fn flush(&mut self) -> Result<()> {
        let writer = self.writer.take().unwrap();
        // parquet 是 row-group 粒度
        writer.close()?;
        self.writer = Some(Writer::try_new(&self.path, self.schema.clone())?);
        Ok(())
    }
}
