USE team15_projectdb;
DROP TABLE IF EXISTS q3_results;

CREATE EXTERNAL TABLE q3_results(origin_airport_code STRING, airport_name STRING, cancellation_count BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q3_results';

INSERT OVERWRITE TABLE q3_results
SELECT
    f.origin AS origin_airport_code,
    COALESCE(a.name, f.origin) AS airport_name,
    SUM(CAST(f.cancelled AS INT)) AS cancellation_count
FROM flight f
LEFT JOIN airport_optimized a ON f.origin = a.code 
WHERE CAST(f.cancelled AS INT) = 1
GROUP BY f.origin, COALESCE(a.name, f.origin)
ORDER BY cancellation_count DESC
LIMIT 10;

SET hive.resultset.use.unique.column.names=false; 
SELECT * FROM q3_results;