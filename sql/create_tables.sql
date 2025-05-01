-- Drop tables if they exist
DROP TABLE IF EXISTS Flight CASCADE;
DROP TABLE IF EXISTS Airport CASCADE;
DROP TABLE IF EXISTS CancellationReason CASCADE;

-- Airport lookup table
CREATE TABLE Airport (
    Code TEXT PRIMARY KEY
);

-- CancellationReason lookup table
CREATE TABLE CancellationReason (
    Code TEXT PRIMARY KEY,
    Description TEXT
);

-- Main Flight table
CREATE TABLE Flight (
    FlightID SERIAL PRIMARY KEY,
    Year INT NOT NULL,
    Month INT NOT NULL,
    DayOfMonth INT NOT NULL,
    DayOfWeek INT NOT NULL,
    DepTime INT,
    CRSDepTime INT NOT NULL,
    ArrTime INT,
    CRSArrTime INT,
    ActualElapsedTime FLOAT,
    CRSElapsedTime FLOAT,
    AirTime FLOAT,
    ArrDelay FLOAT,
    DepDelay FLOAT,
    Origin TEXT REFERENCES Airport(Code),
    Dest TEXT REFERENCES Airport(Code),
    Distance FLOAT NOT NULL,
    TaxiIn FLOAT,
    TaxiOut FLOAT,
    Cancelled FLOAT,
    CancellationCode TEXT REFERENCES CancellationReason(Code),
    Diverted FLOAT,
    CarrierDelay FLOAT,
    WeatherDelay FLOAT,
    NASDelay FLOAT,
    SecurityDelay FLOAT,
    LateAircraftDelay FLOAT
);