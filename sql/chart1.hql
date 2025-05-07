USE team15_projectdb;
DROP TABLE IF EXISTS q1_results;
CREATE EXTERNAL TABLE q1_results(overall_cancellation_rate DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q1_results';

INSERT OVERWRITE TABLE q1_results
SELECT
    SUM(CAST(cancelled AS INT)) / COUNT(flightid) AS overall_cancellation_rate
FROM flight;

SET hive.resultset.use.unique.column.names=false; 
SELECT * FROM q1_results;