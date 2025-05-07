USE team15_projectdb;
DROP TABLE IF EXISTS q4_results;
CREATE EXTERNAL TABLE q4_results(reason_description STRING, reason_count BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q4_results';

INSERT OVERWRITE TABLE q4_results
SELECT
    cr.description AS reason_description,
    COUNT(f.flightid) AS reason_count
FROM flight_optimized f
JOIN cancellationreason cr ON f.cancellationcode = cr.code
WHERE CAST(f.cancelled AS INT) = 1 AND f.cancellationcode IS NOT NULL AND f.cancellationcode != ''
GROUP BY cr.description
ORDER BY reason_count DESC
LIMIT 5;

SET hive.resultset.use.unique.column.names=false; 
SELECT * FROM q4_results;