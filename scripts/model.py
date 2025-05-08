from pyspark.sql import SparkSession

team = "team15"
warehouse = "project/hive/warehouse"

spark = SparkSession.builder\
        .appName(f"{team} - spark ML")\
        .master("yarn")\
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", warehouse)\
        .enableHiveSupport()\
        .getOrCreate()

print("Spark Session Created.")
