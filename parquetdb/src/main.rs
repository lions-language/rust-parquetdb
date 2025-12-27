use std::{collections::HashMap, sync::Arc};

use arrow2::{
    array::{Array, Int32Array, Utf8Array},
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
};

use parquetdb::engine::{Engine, MemoryMergeParquetWriter, ParquetFileWriter, StorageEngine as _};

fn main() -> anyhow::Result<()> {
    let vars = std::env::vars().collect::<HashMap<_, _>>();
    let parquet_writer_mode = match vars.get("parquet_writer_mode") {
        Some(value) => value.clone(),
        None => "file".to_string(),
    };

    let schema = Schema::from(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let batch = Chunk::new(vec![
        Box::new(Int32Array::from_slice([1, 2, 3])) as Box<dyn Array>,
        Box::new(Utf8Array::<i32>::from([Some("a"), None, Some("c")])) as Box<dyn Array>,
    ]);

    if parquet_writer_mode == "file" {
        let mut engine = Engine::<ParquetFileWriter>::open("./tmp/x.parquet", Arc::new(schema))?;

        engine.write(batch)?;
        engine.flush()?;
    } else if parquet_writer_mode == "memory_merge" {
        let mut engine =
            Engine::<MemoryMergeParquetWriter>::open("./tmp/x.parquet", Arc::new(schema))?;

        engine.write(batch)?;
        engine.flush()?;
    } else {
        panic!("unsupport parquet writer mode {}", parquet_writer_mode);
    }

    Ok(())
}
