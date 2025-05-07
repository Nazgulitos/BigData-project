USE team15_projectdb;
DROP TABLE IF EXISTS q6_results;
CREATE EXTERNAL TABLE q6_results(delay_bucket STRING, cancellation_rate DOUBLE, total_flights BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q6_results';

INSERT OVERWRITE TABLE q6_results
SELECT
    CASE
        WHEN depdelay <= 0 THEN '00. On Time or Early'
        WHEN depdelay > 0 AND depdelay <= 15 THEN '01. Delayed 1-15 min'
        WHEN depdelay > 15 AND depdelay <= 30 THEN '02. Delayed 16-30 min'
        WHEN depdelay > 30 AND depdelay <= 60 THEN '03. Delayed 31-60 min'
        WHEN depdelay > 60 AND depdelay <= 120 THEN '04. Delayed 61-120 min'
        WHEN depdelay > 120 AND depdelay <= 240 THEN '05. Delayed 121-240 min'
        ELSE '06. Delayed > 240 min'
    END AS delay_bucket,
    SUM(CAST(cancelled AS INT)) / COUNT(flightid) AS cancellation_rate,
    COUNT(flightid) as total_flights
FROM flight_optimized
WHERE depdelay IS NOT NULL 
GROUP BY
    CASE
        WHEN depdelay <= 0 THEN '00. On Time or Early'
        WHEN depdelay > 0 AND depdelay <= 15 THEN '01. Delayed 1-15 min'
        WHEN depdelay > 15 AND depdelay <= 30 THEN '02. Delayed 16-30 min'
        WHEN depdelay > 30 AND depdelay <= 60 THEN '03. Delayed 31-60 min'
        WHEN depdelay > 60 AND depdelay <= 120 THEN '04. Delayed 61-120 min'
        WHEN depdelay > 120 AND depdelay <= 240 THEN '05. Delayed 121-240 min'
        ELSE '06. Delayed > 240 min'
    END
ORDER BY delay_bucket;

SET hive.resultset.use.unique.column.names=false; 
SELECT * FROM q6_results;