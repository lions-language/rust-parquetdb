use arrow2::{
    array::{Array, Int32Array, Utf8Array},
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
};

use parquetdb::engine::{EngineX, StorageEngine as _};

fn main() -> anyhow::Result<()> {
    let schema = Schema::from(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let mut engine = EngineX::open("./tmp/x.parquet", schema)?;

    let batch = Chunk::new(vec![
        Box::new(Int32Array::from_slice([1, 2, 3])) as Box<dyn Array>,
        Box::new(Utf8Array::<i32>::from([Some("a"), None, Some("c")])) as Box<dyn Array>,
    ]);

    engine.write(batch)?;
    engine.flush()?;

    Ok(())
}
