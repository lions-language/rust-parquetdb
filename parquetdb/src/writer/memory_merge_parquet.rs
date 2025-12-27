use std::sync::Arc;
use std::{fs::File, io::Write};

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

pub struct MemoryMergeParquetWriter {
    schema: Arc<Schema>,
    parquet_schema: Arc<SchemaDescriptor>,
    options: Arc<WriteOptions>,
    encodings: Arc<Vec<Vec<Encoding>>>,
    writer: parquet2::write::FileWriter<Vec<u8>>,
    file: File,
}

impl super::ParquetWriter for MemoryMergeParquetWriter {
    fn try_new(path: &str, schema: Arc<Schema>) -> Result<Self> {
        let parquet_schema = to_parquet_schema(&*schema)?;

        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Zstd(None),
            version: Version::V2,
            data_pagesize_limit: None,
        };

        let encodings: Vec<Vec<_>> = schema
            .fields
            .iter()
            .map(|_| vec![Encoding::Plain])
            .collect();

        let writer = parquet2::write::FileWriter::new(
            vec![],
            parquet_schema.clone(),
            parquet2::write::WriteOptions {
                write_statistics: true,
                version: parquet2::write::Version::V2,
            },
            None,
        );

        Ok(Self {
            schema,
            parquet_schema: Arc::new(parquet_schema),
            options: Arc::new(options),
            encodings: Arc::new(encodings),
            writer,
            file: File::create(path)?,
        })
    }

    fn write_batch(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()> {
        let row_groups = RowGroupIterator::try_new(
            std::iter::once(Ok(batch)),
            &self.schema,
            (&*self.options).clone(),   /*try optimize*/
            (&*self.encodings).clone(), /*try optimize*/
        )?;

        for row_group in row_groups {
            self.writer.write(row_group?)?;
        }

        Ok(())
    }

    fn close(mut self) -> Result<Self> {
        self.writer.end(None)?;
        let mut buf = self.writer.into_inner();
        self.file.write(&mut buf)?;
        Ok(Self {
            schema: self.schema.clone(),
            parquet_schema: self.parquet_schema.clone(),
            options: self.options,
            encodings: self.encodings,
            writer: parquet2::write::FileWriter::new(
                buf,
                (*self.parquet_schema).clone(),
                parquet2::write::WriteOptions {
                    write_statistics: true,
                    version: parquet2::write::Version::V2,
                },
                None,
            ),
            file: self.file,
        })
    }
}
