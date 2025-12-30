use std::fs::File;
use std::sync::Arc;

use anyhow::Result;
use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::Schema,
    io::parquet::read::{FileReader, RowGroupDeserializer, infer_schema, read_metadata},
};

pub struct ParquetReader {
    schema: Arc<Schema>,
    reader: FileReader<File>,
}

impl ParquetReader {
    pub fn try_new(path: &str) -> Result<Self> {
        let mut file = File::open(path)?;

        // parquet metadata
        let metadata = read_metadata(&mut file)?;

        // parquet -> arrow schema
        let schema = infer_schema(&metadata)?;

        let reader = FileReader::new(
            file,
            metadata.row_groups.clone(),
            schema.clone(),
            None, // chunk size（None = 一个 row group 一个 chunk）
            None, // projection（后面可以做列裁剪）
            None, // predicate（后面你会替换）
        );

        Ok(Self {
            schema: Arc::new(schema),
            reader,
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl Iterator for ParquetReader {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.next() {
            Some(r) => match r {
                Ok(r) => Some(Ok(r)),
                Err(err) => Some(Err(err.into())),
            },
            None => None,
        }
    }
}
