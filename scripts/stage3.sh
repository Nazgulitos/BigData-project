#!/bin/bash
echo "Starting Stage 3: Flight cancellation prediction pipeline"

spark-submit \
  --master yarn \
  --conf spark.sql.hive.metastore.uris="thrift://hadoop-02.uni.innopolis.ru:9883" \
  --conf spark.sql.warehouse.dir="project/hive/warehouse" \
  --conf spark.sql.hive.metastore.jars=path \
  --conf spark.sql.hive.metastore.jars.path="hdfs://hadoop-02.uni.innopolis.ru/user/team15/.hiveJars/*" \
  --conf spark.sql.hive.metastore.version=3.1.3 \
  --conf spark.driver.extraClassPath="/user/hive" \
  "scripts/ml_pipeline.py"

echo "Stage 3 automation script finished."

