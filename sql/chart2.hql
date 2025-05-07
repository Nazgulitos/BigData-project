USE team15_projectdb;
DROP TABLE IF EXISTS q2_results;
CREATE EXTERNAL TABLE q2_results(flight_month INT, total_cancellations BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q2_results';

INSERT OVERWRITE TABLE q2_results
SELECT
    month_partition AS flight_month,
    SUM(CAST(cancelled AS INT)) AS total_cancellations
FROM flight
WHERE CAST(cancelled AS INT) = 1
GROUP BY month_partition
ORDER BY flight_month;

SET hive.resultset.use.unique.column.names=false; 
SELECT * FROM q2_results;