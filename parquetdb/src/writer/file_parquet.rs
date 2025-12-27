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
use parquet2::metadata::SchemaDescriptor;

pub struct ParquetFileWriter {
    schema: Arc<Schema>,
    parquet_schema: Arc<SchemaDescriptor>,
    writer: parquet2::write::FileWriter<File>,
}

impl super::ParquetWriter for ParquetFileWriter {
    fn try_new(path: &str, schema: Arc<Schema>) -> Result<Self> {
        let file = File::create(path)?;

        let parquet_schema = to_parquet_schema(&*schema)?;

        let writer = parquet2::write::FileWriter::new(
            file,
            parquet_schema.clone(),
            parquet2::write::WriteOptions {
                write_statistics: true,
                version: parquet2::write::Version::V2,
            },
            None,
        );

        Ok(Self {
            schema: schema.clone(),
            parquet_schema: Arc::new(parquet_schema),
            writer,
        })
    }

    fn write_batch(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()> {
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

    fn close(mut self) -> Result<Self> {
        self.writer.end(None)?;
        let schema = self.schema.clone();
        let parquet_schema = self.parquet_schema.clone();
        let file = self.writer.into_inner();
        Ok(Self {
            schema,
            parquet_schema: parquet_schema.clone(),
            writer: parquet2::write::FileWriter::new(
                file,
                (*parquet_schema).clone(),
                parquet2::write::WriteOptions {
                    write_statistics: true,
                    version: parquet2::write::Version::V2,
                },
                None,
            ),
        })
    }
}
