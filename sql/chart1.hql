USE team15_projectdb;

DROP TABLE IF EXISTS q1_results;

CREATE EXTERNAL TABLE q1_results(
    `Cancelled (%)` DOUBLE,
    `Uncancelled (%)` DOUBLE
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q1_results';

INSERT OVERWRITE TABLE q1_results
SELECT
    ROUND(SUM(CAST(cancelled AS INT)) * 100.0 / COUNT(flightid), 2),
    ROUND(100.0 - (SUM(CAST(cancelled AS INT)) * 100.0 / COUNT(flightid)), 2)
FROM flight_optimized;

SET hive.resultset.use.unique.column.names=false;
SELECT * FROM q1_results;