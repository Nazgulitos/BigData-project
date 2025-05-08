USE team15_projectdb;

SELECT
    SUM(CASE WHEN CAST(cancelled AS INT) = 1 THEN 1 ELSE 0 END) AS cancelled_count,
    SUM(CASE WHEN CAST(cancelled AS INT) = 0 THEN 1 ELSE 0 END) AS not_cancelled_count,
    COUNT(flightid) AS total_flights 
FROM 
    flight
; 
