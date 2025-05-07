USE team15_projectdb;

DROP TABLE IF EXISTS q1_results;

CREATE EXTERNAL TABLE q1_results(
    status STRING,
    rate DOUBLE
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q1_results';

INSERT OVERWRITE TABLE q1_results
SELECT 'cancelled' AS status,
       ROUND(SUM(CAST(cancelled AS INT)) * 100.0 / COUNT(flightid), 2) AS rate
FROM flight_optimized
UNION ALL
SELECT 'uncancelled' AS status,
       ROUND(100.0 - (SUM(CAST(cancelled AS INT)) * 100.0 / COUNT(flightid)), 2) AS rate
FROM flight_optimized;

SET hive.resultset.use.unique.column.names=false;
SELECT * FROM q1_results;