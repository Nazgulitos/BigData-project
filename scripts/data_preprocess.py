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

# Initialize set to collect unique airport codes
all_airports = set()

# Process CSV in chunks to avoid memory errors
chunk_size = 1000000  # Adjust based on available memory
chunks = pd.read_csv("./data/combine_files.csv", dtype=dtypes, chunksize=chunk_size)

# Collect processed chunks and airport codes
processed_chunks = []
for chunk in chunks:
    # Extract unique airports from this chunk
    chunk_airports = pd.Series(pd.concat([chunk["Origin"], chunk["Dest"]])).dropna().unique()
    all_airports.update(chunk_airports)
    processed_chunks.append(chunk)

# Concatenate chunks into final DataFrame
df_final = pd.concat(processed_chunks, ignore_index=True)

with open("data/combine_files_upd.csv", "w") as f:
    first = True
    for chunk in pd.read_csv("./data/combine_files.csv", dtype=dtypes, chunksize=chunk_size):
        chunk.drop(columns=[
            'CarrierDelay',
            'WeatherDelay',
            'NASDelay',
            'SecurityDelay',
            'LateAircraftDelay'
        ], inplace=True)

        # Extract airports
        chunk_airports = pd.Series(pd.concat([chunk["Origin"], chunk["Dest"]])).dropna().unique()
        all_airports.update(chunk_airports)

        chunk.to_csv(f, index=False, header=first)
        first = False

# Save unique airport codes
airports_df = pd.DataFrame({"Code": list(all_airports)})
airports_df.to_csv("data/airports.csv", index=False)

# Create cancellation reasons table
cancellation_reasons = pd.DataFrame({
    "Code": ["A", "B", "C", "D"],
    "Description": ["Carrier", "Weather", "NAS", "Security"]
})
cancellation_reasons.to_csv("data/cancellation_reasons.csv", index=False)