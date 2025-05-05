USE team15_projectdb;

DROP TABLE IF EXISTS q1_results;

CREATE EXTERNAL TABLE q1_results(
  airport_code STRING, 
  departure_count BIGINT 
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q1_results'; 

INSERT OVERWRITE TABLE q1_results
SELECT
    a.code AS airport_code,            
    COUNT(f.flightid) AS departure_count 
FROM flight_optimized f                
JOIN airport_optimized a ON f.origin = a.code 
GROUP BY a.code                           
ORDER BY departure_count DESC             
LIMIT 10;                                 

SET hive.resultset.use.unique.column.names=false; 
SELECT * FROM q1_results;