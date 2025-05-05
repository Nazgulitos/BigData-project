USE team15_projectdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true; 
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

DROP TABLE IF EXISTS flight_optimized;
DROP TABLE IF EXISTS airport_optimized;

CREATE EXTERNAL TABLE flight_optimized(
  flightid INT,      
  origin STRING,
  dest STRING,
  cancelled INT,
  cancellationcode STRING,
  arrdelay INT, 
  depdelay INT,
  flightdate BIGINT
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
  TO_DATE(from_unixtime(CAST(flightdate / 1000 AS BIGINT))) AS flight_dt 
FROM flight;

INSERT OVERWRITE TABLE airport_optimized
SELECT
  airportid,
  city,
  state,
  name
FROM airport;

DROP TABLE IF EXISTS flight;
DROP TABLE IF EXISTS airport;

ALTER TABLE cancellationreason_external RENAME TO cancellationreason; 

SHOW TABLES; 