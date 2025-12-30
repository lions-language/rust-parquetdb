use std::{collections::HashMap, sync::Arc};

use arrow2::{
    array::{Array, Int32Array, Utf8Array},
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
};
use rand::Rng;
use rand::distr::Alphanumeric;

use parquetdb::engine::{
    ColumnParallelParquetWriter, ColumnParallelV2ParquetWriter, DirectIoParquetWriter,
    DirectIoV2ParquetWriter, Engine, MemoryMergeParquetWriter, ParquetFileWriter,
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
    let mode = match vars.get("MODE") {
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

    if mode == "file" {
        let mut engine: Engine<ParquetFileWriter> =
            Engine::<ParquetFileWriter>::open("./tmp/x.parquet", Arc::new(schema))?;

        for _ in 0..1 {
            for _ in 0..1024 {
                engine.write(batch.clone())?;
            }
            engine.flush()?;
        }
    } else if mode == "memory_merge" {
        let mut engine =
            Engine::<MemoryMergeParquetWriter>::open("./tmp/x.parquet", Arc::new(schema))?;

        for _ in 0..1 {
            for _ in 0..1024 {
                engine.write(batch.clone())?;
            }
            engine.flush()?;
        }
    } else if mode == "column_parallel" {
        let mut engine: Engine<ColumnParallelParquetWriter> =
            Engine::<ColumnParallelParquetWriter>::open("./tmp/x.parquet", Arc::new(schema))?;

        for _ in 0..1 {
            for _ in 0..1024 {
                engine.write(batch.clone())?;
            }
        }
        engine.flush()?;
    } else if mode == "column_parallel_v2" {
        let mut engine: Engine<ColumnParallelV2ParquetWriter> =
            Engine::<ColumnParallelV2ParquetWriter>::open("./tmp/x.parquet", Arc::new(schema))?;

        for _ in 0..1 {
            for _ in 0..1024 {
                engine.write(batch.clone())?;
            }
        }
        engine.flush()?;
    } else if mode == "direct_io" {
        let mut engine: Engine<DirectIoParquetWriter> =
            Engine::<DirectIoParquetWriter>::open("./tmp/x.parquet", Arc::new(schema))?;

        for _ in 0..1 {
            for _ in 0..1024 {
                engine.write(batch.clone())?;
            }
        }
        engine.flush()?;
    } else if mode == "direct_io_v2" {
        let mut engine: Engine<DirectIoV2ParquetWriter> =
            Engine::<DirectIoV2ParquetWriter>::open("./tmp/x.parquet", Arc::new(schema))?;

        for _ in 0..1 {
            for _ in 0..1024 {
                engine.write(batch.clone())?;
            }
        }
        engine.flush()?;
    } else if mode == "read" {
        let mut engine: Engine<ParquetFileWriter> =
            Engine::<ParquetFileWriter>::open("./tmp/x.parquet", Arc::new(schema))?;

        for _ in 0..1 {
            for _ in 0..1024 {
                engine.write(batch.clone())?;
            }
            engine.flush()?;
        }

        // engine.read()?;
    } else {
        panic!("unsupport mode {}", mode);
    }

    Ok(())
}
