JAVE_HOME=$HOME/packages/jdk/jdk-17.0.10 ./bin/spark-shell --master local[10]   --driver-memory 20G

spark.read.parquet("/tmp/x.parquet").show()