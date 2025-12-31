use std::sync::Arc;

use anyhow::Result;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;

pub trait StorageEngine {
    fn write(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()>;
    // fn read(&mut self) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

use crate::reader::ParquetReader;
use crate::writer::ParquetWriter;

pub use crate::writer::ColumnParallelParquetWriter;
pub use crate::writer::ColumnParallelV2ParquetWriter;
pub use crate::writer::DirectIoParquetWriter;
pub use crate::writer::DirectIoV2ParquetWriter;
pub use crate::writer::MemoryMergeParquetWriter;
pub use crate::writer::ParquetFileWriter;

pub struct Engine<Writer: ParquetWriter> {
    writer: Option<Writer>,
    // reader: ParquetReader,
}

impl<Writer: ParquetWriter> Engine<Writer> {
    pub fn open(path: &str, schema: Arc<Schema>) -> Result<Self> {
        Ok(Self {
            writer: Some(Writer::try_new(path, schema.clone())?),
            // reader: ParquetReader::try_new(path)?,
        })
    }
}

impl<Writer: ParquetWriter> super::engine::StorageEngine for Engine<Writer> {
    fn write(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()> {
        self.writer.as_mut().unwrap().write_batch(batch)
    }

    // fn read(&mut self) -> Result<()> {
    //     println!("{:?}", self.reader.schema());

    //     while let Some(batch) = self.reader.next() {
    //         let chunk = batch?;
    //         println!("rows = {}", chunk.len());
    //     }

    //     Ok(())
    // }

    fn flush(&mut self) -> Result<()> {
        let writer = self.writer.take().unwrap();
        // parquet 是 row-group 粒度
        self.writer = Some(writer.close()?);
        Ok(())
    }
}

pub fn read(path: &str) -> Result<()> {
    let mut reader = ParquetReader::try_new(path)?;

    println!("{:?}", reader.schema());

    while let Some(batch) = reader.next() {
        let chunk = batch?;
        println!("rows = {}", chunk.len());
    }

    Ok(())
}
