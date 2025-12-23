use std::fs::File;
use std::sync::Arc;

use anyhow::Result;
use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::Schema,
    io::parquet::write::{
        CompressionOptions, Encoding, RowGroupIterator, Version, WriteOptions, to_parquet_schema,
    },
};

pub struct ParquetWriter {
    schema: Arc<Schema>,
    writer: parquet2::write::FileWriter<File>,
}

impl ParquetWriter {
    pub fn try_new(path: &str, schema: Schema) -> Result<Self> {
        let file = File::create(path)?;

        let parquet_schema = to_parquet_schema(&schema)?;

        let writer = parquet2::write::FileWriter::new(
            file,
            parquet_schema,
            parquet2::write::WriteOptions {
                write_statistics: true,
                version: parquet2::write::Version::V2,
            },
            None,
        );

        Ok(Self {
            schema: Arc::new(schema),
            writer,
        })
    }

    pub fn write_batch(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()> {
        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Zstd(None),
            version: Version::V2,
            data_pagesize_limit: None,
        };

        let encodings: Vec<Vec<_>> = self
            .schema
            .fields
            .iter()
            .map(|_| vec![Encoding::Plain])
            .collect();

        let row_groups = RowGroupIterator::try_new(
            std::iter::once(Ok(batch)),
            &self.schema,
            options,
            encodings,
        )?;

        for row_group in row_groups {
            self.writer.write(row_group?)?;
        }

        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        self.writer.end(None)?;
        Ok(())
    }
}
