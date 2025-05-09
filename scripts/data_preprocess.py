"""
This script preprocesses flight data.
It performs the following tasks:
1. Reads the input CSV file in chunks to handle large datasets.
2. Drops unnecessary columns to reduce data size.
3. Extracts unique airport codes and saves them to a separate CSV file.
4. Creates a CSV file for cancellation reasons with predefined codes and descriptions.
5. Outputs the cleaned and processed data to a new CSV file.
"""

import pandas as pd

dtypes = {
    'Year': 'int32',
    'Month': 'int8',
    'Day': 'int8',
    'DayOfWeek': 'int8',
    'DepTime': 'Int32',
    'CRSDepTime': 'Int32',
    'ArrTime': 'Int32',
    'CRSArrTime': 'Int32',
    'ActualElapsedTime': 'float32',
    'CRSElapsedTime': 'float32',
    'AirTime': 'float32',
    'ArrDelay': 'float32',
    'DepDelay': 'float32',
    'Origin': 'object',
    'Dest': 'object',
    'Distance': 'float32',
    'TaxiIn': 'float32',
    'TaxiOut': 'float32',
    'Cancelled': 'float32',
    'CancellationCode': 'object',
    'Diverted': 'float32',
    'CarrierDelay': 'float32',
    'WeatherDelay': 'float32',
    'NASDelay': 'float32',
    'SecurityDelay': 'float32',
    'LateAircraftDelay': 'float32'
}

all_airports = set()

OUTPUT_CSV = "data/combine_files_upd.csv"
CHUNK_SIZE = 1_000_000
FIRST_CHUNK = True

columns_to_drop = [
    'CarrierDelay',
    'WeatherDelay',
    'NASDelay',
    'SecurityDelay',
    'LateAircraftDelay'
]

with open(OUTPUT_CSV, "w", encoding="utf-8") as f_out:
    for chunk in pd.read_csv("./data/combine_files.csv", dtype=dtypes, chunksize=CHUNK_SIZE):
        chunk.drop(columns=columns_to_drop, inplace=True)

        all_airports.update(chunk["Origin"].dropna().unique())
        all_airports.update(chunk["Dest"].dropna().unique())

        chunk.to_csv(f_out, index=False, header=FIRST_CHUNK)
        FIRST_CHUNK = False

airports_df = pd.DataFrame({"Code": sorted(all_airports)})
airports_df.to_csv("data/airports.csv", index=False)

cancellation_reasons = pd.DataFrame({
    "Code": ["A", "B", "C", "D"],
    "Description": ["Carrier", "Weather", "NAS", "Security"]
})
cancellation_reasons.to_csv("data/cancellation_reasons.csv", index=False)
