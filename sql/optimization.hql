USE team15_projectdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing=true;
SET hive.exec.max.dynamic.partitions=5000;
SET hive.exec.max.dynamic.partitions.pernode=5000;
SET hive.tez.container.size=4096;
SET parquet.memory.min.allocation.size=2097152;
SET hive.tez.auto.reducer.parallelism=true;
SET parquet.block.size=134217728;

CREATE EXTERNAL TABLE flight_temp (
    FlightID BIGINT,
    Year INT,
    Month INT,
    DayOfMonth INT,
    DayOfWeek INT,
    DepTime FLOAT,
    CRSDepTime FLOAT,
    ArrTime FLOAT,
    CRSArrTime FLOAT,
    ActualElapsedTime FLOAT,
    CRSElapsedTime FLOAT,
    AirTime FLOAT,
    ArrDelay FLOAT,
    DepDelay FLOAT,
    Origin STRING,
    Dest STRING,
    Distance FLOAT,
    TaxiIn FLOAT,
    TaxiOut FLOAT,
    Cancelled FLOAT,
    CancellationCode STRING,
    Diverted FLOAT
)
STORED AS PARQUET
LOCATION 'project/hive/warehouse/flight_temp'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

INSERT OVERWRITE TABLE flight_temp
SELECT *
FROM flight;


CREATE EXTERNAL TABLE flight_optimized (
    FlightID BIGINT,
    DayOfMonth INT,
    DayOfWeek INT,
    DepTime FLOAT,
    CRSDepTime FLOAT,
    ArrTime FLOAT,
    CRSArrTime FLOAT,
    ActualElapsedTime FLOAT,
    CRSElapsedTime FLOAT,
    AirTime FLOAT,
    ArrDelay FLOAT,
    DepDelay FLOAT,
    Origin STRING,
    Dest STRING,
    Distance FLOAT,
    TaxiIn FLOAT,
    TaxiOut FLOAT,
    Cancelled FLOAT,
    CancellationCode STRING,
    Diverted FLOAT
)
PARTITIONED BY (Year_partition INT, Month_partition INT)
CLUSTERED BY (Origin) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION 'project/hive/warehouse/flight_optimized'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

INSERT INTO TABLE flight_optimized
PARTITION (Year_partition, Month_partition)
SELECT 
    FlightID,
    DayOfMonth,
    DayOfWeek,
    DepTime,
    CRSDepTime,
    ArrTime,
    CRSArrTime,
    ActualElapsedTime,
    CRSElapsedTime,
    AirTime,
    ArrDelay,
    DepDelay,
    Origin,
    Dest,
    Distance,
    TaxiIn,
    TaxiOut,
    Cancelled,
    CancellationCode,
    Diverted,
    Year AS Year_partition,
    Month AS Month_partition
FROM flight_temp;

DROP TABLE IF EXISTS flight_temp;

CREATE EXTERNAL TABLE airport_optimized(
  code STRING  
)
CLUSTERED BY (code) INTO 4 BUCKETS 
STORED AS PARQUET
LOCATION 'project/hive/warehouse/airport_optimized'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

INSERT OVERWRITE TABLE airport_optimized
SELECT
  code
FROM airport;


CREATE EXTERNAL TABLE cancellationreason_optimized (
    Code STRING,
    Description STRING
)
CLUSTERED BY (Code) INTO 2 BUCKETS
STORED AS PARQUET
LOCATION 'project/hive/warehouse/cancellationreason'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

INSERT OVERWRITE TABLE cancellationreason_optimized
SELECT Code, Description
FROM cancellationreason;

SHOW TABLES;
SHOW PARTITIONS flight_optimized;
DESCRIBE FORMATTED flight_optimized;
SELECT * FROM flight_optimized LIMIT 5;