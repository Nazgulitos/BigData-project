"""
This script initializes a Spark session with Hive support for team15's Big Data project.
It connects to the Hive metastore and sets up the Spark environment for running ML tasks.
"""

from pyspark.sql import SparkSession

TEAM = "team15"
WAREHOUSE = "project/hive/warehouse"

spark = SparkSession.builder\
        .appName(f"{TEAM} - spark ML")\
        .master("yarn")\
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", WAREHOUSE)\
        .enableHiveSupport()\
        .getOrCreate()

print("Spark Session Created.")
