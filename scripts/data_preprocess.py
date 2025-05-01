import pandas as pd
import numpy as np

# Define dtypes for columns based on PostgreSQL schema
dtypes = {
    'Year': 'int32',
    'Month': 'int8',
    'Day': 'int8',  # Assuming 'Day' in CSV; adjust to 'DayOfMonth' if needed
    'DayOfWeek': 'int8',
    'DepTime': 'Int32',  # Nullable integer for minutes since midnight
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

# Process CSV in chunks to avoid MemoryError
chunk_size = 100000 
chunks = pd.read_csv("./data/combine_files.csv", dtype=dtypes, chunksize=chunk_size, low_memory=False)

# Collect processed chunks
processed_chunks = []
for chunk in chunks:
    processed_chunks.append(chunk)

# Concatenate chunks
df_final = pd.concat(processed_chunks, ignore_index=True)

# Save preprocessed DataFrame
df_final.to_csv("./data/preprocessed_combine_files.csv", index=False)

# Extract unique airport codes
airports = pd.Series(pd.concat([df_final["Origin"], df_final["Dest"]])).dropna().unique()
airports_df = pd.DataFrame({"Code": airports})
airports_df.to_csv("data/airports.csv", index=False)

# Create cancellation reasons table
cancellation_reasons = pd.DataFrame({
    "Code": ["A", "B", "C", "D"],
    "Description": ["Carrier", "Weather", "NAS", "Security"]
})
cancellation_reasons.to_csv("data/cancellation_reasons.csv", index=False)