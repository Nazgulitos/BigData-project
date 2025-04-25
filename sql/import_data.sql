START TRANSACTION;

-- Load data into lookup tables
\COPY Airport(Code)
FROM STDIN WITH (FORMAT csv, HEADER true);

\COPY CancellationReason(Code, Description)
FROM STDIN WITH (FORMAT csv, HEADER true);

-- Load data into main flight table
\COPY Flight (
    Year, Month, DayOfMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime,
    ActualElapsedTime, CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin, Dest,
    Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode, Diverted, CarrierDelay,
    WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay
)
FROM STDIN WITH (FORMAT csv, HEADER true);

COMMIT;


-- -- always test if you can import the data from PgAdmin then you automate it by writing the script
-- COPY depts FROM STDIN WITH CSV HEADER DELIMITER ',' NULL AS 'null';

-- COPY emps FROM STDIN WITH CSV HEADER DELIMITER ',' NULL AS 'null';