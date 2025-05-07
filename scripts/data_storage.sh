#!/bin/bash

# echo "Building Postgres database is starting!"
# python scripts/build_projectdb.py
# echo "Building Postgres database is done!"

echo "Copying data into HDFS starts.."
password=$(head -n 1 secrets/.psql.pass)

# 1. Clear existing HDFS warehouse directory and output folder
echo "Clearing previous HDFS data..."
hdfs dfs -rm -r -skipTrash /user/team15/project/warehouse 2>/dev/null
rm -rf ~/BigData-project/output
mkdir -p ~/BigData-project/output

# 2. List Databases
sqoop list-databases --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team15_projectdb --username team15 --password $password

# 3. List tables (for debugging)
sqoop list-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team15_projectdb --username team15 --password $password

# 4. Import all tables with Avro format
echo "Starting Sqoop import..."
sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team15_projectdb --username team15 --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=project/warehouse --outdir ~/BigData-project/output --m 1

# Verify import succeeded
if [ $? -eq 0 ]; then
  echo "Copying data into HDFS is done!"
  echo "Avro schemas saved to: ~/BigData-project/output/"
else
  echo "Sqoop import failed! Check logs." >&2
  exit 1
fi