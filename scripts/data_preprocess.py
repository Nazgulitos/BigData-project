import pandas as pd
import numpy as np
from datetime import time

# Load the dataset
df_final = pd.read_csv("./data/combine_files.csv", low_memory=False)

# Save the preprocessed DataFrame to a new CSV file
# df_final.to_csv("./data/preprocessed_combine_files.csv", index=False)

# Extract unique airport codes from Origin and Dest
airports = pd.Series(pd.concat([df_final["Origin"], df_final["Dest"]])).dropna().unique()
airports_df = pd.DataFrame({"Code": airports})
airports_df.to_csv("data/airports.csv", index=False)

# Cancellation reasons are fixed and known (A–D)
cancellation_reasons = pd.DataFrame({
    "Code": ["A", "B", "C", "D"],
    "Description": ["Carrier", "Weather", "NAS", "Security"]
})
cancellation_reasons.to_csv("data/cancellation_reasons.csv", index=False)