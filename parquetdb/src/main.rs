use std::{collections::HashMap, sync::Arc};

use arrow2::{
    array::{Array, Int32Array, Utf8Array},
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
};
use rand::Rng;
use rand::distr::Alphanumeric;

use parquetdb::engine::{
    ColumnParallelParquetWriter, Engine, MemoryMergeParquetWriter, ParquetFileWriter,
    StorageEngine as _,
};

fn random_string(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn main() -> anyhow::Result<()> {
    let vars = std::env::vars().collect::<HashMap<_, _>>();
    let parquet_writer_mode = match vars.get("PARQUET_WRITER_MODE") {
        Some(value) => value.clone(),
        None => "file".to_string(),
    };

    let schema = Schema::from(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]);

    let mut ids = Vec::new();
    for _ in 0..1024 {
        let mut rng = rand::rng();
        ids.push(rng.random::<i32>());
    }
    let mut names = Vec::new();
    for _ in 0..1024 {
        names.push(random_string(32));
    }
    let batch = Chunk::new(vec![
        Box::new(Int32Array::from_slice(&ids)) as Box<dyn Array>,
        Box::new(Utf8Array::<i32>::from_slice(&names)) as Box<dyn Array>,
    ]);

    if parquet_writer_mode == "file" {
        let mut engine: Engine<ParquetFileWriter> =
            Engine::<ParquetFileWriter>::open("./tmp/x.parquet", Arc::new(schema))?;

        for _ in 0..1 {
            for _ in 0..1024 {
                engine.write(batch.clone())?;
            }
            engine.flush()?;
        }
    } else if parquet_writer_mode == "memory_merge" {
        let mut engine =
            Engine::<MemoryMergeParquetWriter>::open("./tmp/x.parquet", Arc::new(schema))?;

        for _ in 0..1 {
            for _ in 0..1024 {
                engine.write(batch.clone())?;
            }
            engine.flush()?;
        }
    } else if parquet_writer_mode == "column_parallel" {
        let mut engine: Engine<ColumnParallelParquetWriter> =
            Engine::<ColumnParallelParquetWriter>::open("./tmp/x.parquet", Arc::new(schema))?;

        for _ in 0..1 {
            for _ in 0..1024 {
                engine.write(batch.clone())?;
            }
        }
        engine.flush()?;
    } else {
        panic!("unsupport parquet writer mode {}", parquet_writer_mode);
    }

    Ok(())
}
