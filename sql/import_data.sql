-- Load data into lookup tables
COPY Airport FROM STDIN WITH (FORMAT csv, HEADER true);

COPY CancellationReason FROM STDIN WITH (FORMAT csv, HEADER true);

-- Load data into main flight table
COPY Flight (
    Year, Month, DayOfMonth, DayOfWeek, 
    DepTime, CRSDepTime, ArrTime, CRSArrTime,
    ActualElapsedTime, CRSElapsedTime, AirTime, 
    ArrDelay, DepDelay, Origin, Dest, Distance, 
    TaxiIn, TaxiOut, Cancelled, CancellationCode, Diverted, 
    CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay
)
FROM STDIN WITH (FORMAT csv, HEADER true);