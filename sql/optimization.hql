USE team15_projectdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true; 
SET hive.exec.max.dynamic.partitions=5000;
SET hive.exec.max.dynamic.partitions.pernode=5000;

SET hive.tez.container.size=4096;
SET parquet.memory.min.allocation.size=2097152;

DROP TABLE IF EXISTS flight;
DROP TABLE IF EXISTS flight_optimized;
DROP TABLE IF EXISTS airport_optimized;

CREATE EXTERNAL TABLE flight (
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
    Origin TEXT,
    Dest TEXT,
    Distance FLOAT,
    TaxiIn FLOAT,
    TaxiOut FLOAT,
    Cancelled FLOAT,
    CancellationCode TEXT,
    Diverted FLOAT,
    CarrierDelay FLOAT,
    WeatherDelay FLOAT,
    NASDelay FLOAT,
    SecurityDelay FLOAT,
    LateAircraftDelay FLOAT
)
PARTITIONED BY (Year INT, Month INT)
CLUSTERED BY (Origin) INTO 32 BUCKETS
STORED AS AVRO
LOCATION 'project/warehouse/flight'
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/flight.avsc');


CREATE EXTERNAL TABLE flight_temp (
    FlightID SERIAL,
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
    Origin TEXT,
    Dest TEXT,
    Distance FLOAT,
    TaxiIn FLOAT,
    TaxiOut FLOAT,
    Cancelled FLOAT,
    CancellationCode TEXT,
    Diverted FLOAT,
    CarrierDelay FLOAT,
    WeatherDelay FLOAT,
    NASDelay FLOAT,
    SecurityDelay FLOAT,
    LateAircraftDelay FLOAT
)
STORED AS AVRO
LOCATION 'project/warehouse/flight'
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/flight.avsc');

INSERT INTO TABLE flight
PARTITION (Year, Month)
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
    CarrierDelay,
    WeatherDelay,
    NASDelay,
    SecurityDelay,
    LateAircraftDelay,
    Year,
    Month
FROM flight_temp;

DROP TABLE IF EXISTS flight_temp;

SHOW TABLES; 
SHOW PARTITIONS flight;
DESCRIBE FORMATTED flight;
SELECT * FROM flight LIMIT 5;

