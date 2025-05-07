USE team15_projectdb;
DROP TABLE IF EXISTS q5_results;
CREATE EXTERNAL TABLE q5_results(day_of_week INT, cancellation_rate DOUBLE, total_flights BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q5_results';

INSERT OVERWRITE TABLE q5_results
SELECT
    dayofweek AS day_of_week,
    SUM(CAST(cancelled AS INT)) / COUNT(flightid) AS cancellation_rate,
    COUNT(flightid) as total_flights
FROM flight
GROUP BY dayofweek
ORDER BY day_of_week;

SET hive.resultset.use.unique.column.names=false; 
SELECT * FROM q5_results;