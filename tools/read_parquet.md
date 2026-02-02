JAVE_HOME=$HOME/packages/jdk/jdk-17.0.10 ./bin/spark-shell --master local[10]   --driver-memory 20G

```shell
spark.read.parquet("/tmp/x.parquet").show()
```

如果要换类型，可以这样
```shell
val schema = StructType(Seq(
  StructField("s", StringType, nullable=true)
))

spark.read.schema(schema).parquet("/home/liujun.coder/workspace/gitlab/stream_engine/storages/storage1/ssd_disk01/test/ltc/0_2026-01-31_13:00:00_0_0_0/test.parquet")
```