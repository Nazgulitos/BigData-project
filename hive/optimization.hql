USE team15_projectdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true; 
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

SET parquet.memory.min.allocation.size=2097152;

DROP TABLE IF EXISTS flight_optimized;
DROP TABLE IF EXISTS airport_optimized;

CREATE EXTERNAL TABLE flight_optimized(
  flightid INT,      
  origin STRING,
  dest STRING,
  cancelled INT,
  cancellationcode STRING,
  arrdelay INT, 
  depdelay INT
)

PARTITIONED BY (flight_dt DATE) 
CLUSTERED BY (flightid) INTO 8 BUCKETS 
STORED AS PARQUET 
LOCATION 'project/hive/warehouse/flight_optimized'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

CREATE EXTERNAL TABLE airport_optimized(
  code STRING,    
  city STRING,
  state STRING,
  name STRING
)
CLUSTERED BY (code) INTO 4 BUCKETS 
STORED AS PARQUET
LOCATION 'project/hive/warehouse/airport_optimized'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

INSERT OVERWRITE TABLE flight_optimized PARTITION(flight_dt)
SELECT
  flightid,
  origin,
  dest,
  cancelled,
  cancellationcode,
  arrdelay,
  depdelay,
-- TO_DATE(from_unixtime(CAST(flightdate / 1000 AS BIGINT))) AS flight_dt 
  CAST(CONCAT_WS('-', CAST(year AS STRING), LPAD(CAST(month AS STRING), 2, '0'), LPAD(CAST(dayofmonth AS STRING), 2, '0')) AS DATE) AS flight_dt 
FROM flight;

INSERT OVERWRITE TABLE airport_optimized
SELECT
  code,
  city,
  state,
  name
FROM airport;

DROP TABLE IF EXISTS flight;
DROP TABLE IF EXISTS airport;

ALTER TABLE cancellationreason_external RENAME TO cancellationreason; 

SHOW TABLES; 