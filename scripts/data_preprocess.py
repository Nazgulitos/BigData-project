import pandas as pd
import numpy as np

# Define dtypes for columns based on PostgreSQL schema
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

# Set of all unique airport codes
all_airports = set()

# Output CSV with only required columns
output_csv = "data/combine_files_upd.csv"
chunk_size = 1_000_000
first_chunk = True

# Columns to drop
columns_to_drop = [
    'CarrierDelay',
    'WeatherDelay',
    'NASDelay',
    'SecurityDelay',
    'LateAircraftDelay'
]

# Process chunks
with open(output_csv, "w") as f_out:
    for chunk in pd.read_csv("./data/combine_files.csv", dtype=dtypes, chunksize=chunk_size):
        # Drop unused columns
        chunk.drop(columns=columns_to_drop, inplace=True)

        # Collect unique airport codes
        all_airports.update(chunk["Origin"].dropna().unique())
        all_airports.update(chunk["Dest"].dropna().unique())

        # Write cleaned chunk to file
        chunk.to_csv(f_out, index=False, header=first_chunk)
        first_chunk = False

# Save airport codes
airports_df = pd.DataFrame({"Code": sorted(all_airports)})
airports_df.to_csv("data/airports.csv", index=False)

# Save cancellation reasons
cancellation_reasons = pd.DataFrame({
    "Code": ["A", "B", "C", "D"],
    "Description": ["Carrier", "Weather", "NAS", "Security"]
})
cancellation_reasons.to_csv("data/cancellation_reasons.csv", index=False)