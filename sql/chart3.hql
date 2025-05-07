USE team15_projectdb;
DROP TABLE IF EXISTS q3_results;
CREATE EXTERNAL TABLE q3_results(origin_airport STRING, cancellation_count BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q3_results';

INSERT OVERWRITE TABLE q3_results
SELECT
    origin AS origin_airport,
    SUM(CAST(cancelled AS INT)) AS cancellation_count
FROM flight
WHERE CAST(cancelled AS INT) = 1
GROUP BY origin
ORDER BY cancellation_count DESC
LIMIT 10;

SET hive.resultset.use.unique.column.names=false; 
SELECT * FROM q3_results;