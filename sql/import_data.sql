START TRANSACTION;

-- Load data into lookup tables
\COPY Airport(Code)
FROM 'data/airports.csv' WITH (FORMAT csv, HEADER true);

\COPY CancellationReason(Code, Description)
FROM 'data/cancellation_reasons.csv' WITH (FORMAT csv, HEADER true);

-- Load data into main flight table
\COPY Flight (
    Year, Month, DayOfMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime,
    ActualElapsedTime, CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin, Dest,
    Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode, Diverted, CarrierDelay,
    WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay
)
FROM 'data/preprocessed_combine_files.csv' WITH (FORMAT csv, HEADER true);

COMMIT;