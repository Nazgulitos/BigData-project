import pandas as pd
import numpy as np
from datetime import time

# Load the dataset
df_final = pd.read_csv("./data/combine_files.csv")

# Separate the two classes: Cancelled (1.0) and Not Cancelled (0.0)
# cancelled_1 = df[df['Cancelled'] == 1.0]
# cancelled_0 = df[df['Cancelled'] == 0.0].sample(
#     n=600000 - len(cancelled_1), random_state=42
# )
# Combine the two classes into a balanced dataset
# df_balanced = pd.concat([cancelled_1, cancelled_0])
# Shuffle the resulting DataFrame
# df_balanced = df.sample(frac=1, random_state=42).reset_index(drop=True)
# Create a copy of the balanced DataFrame for further processing
# df_final = df_balanced.copy()

# Function to convert hhmm time columns to datetime.time format
def hhmm_to_time(x):
    if pd.isnull(x):
        return None
    x = int(x)
    h = x // 100
    m = x % 100
    if h < 24 and m < 60:
        return time(hour=h, minute=m)
    return None

# Apply the time conversion function to specific columns
for col in ['DepTime', 'CRSDepTime', 'ArrTime', 'CRSArrTime']:
    df_final[col] = df_final[col].apply(hhmm_to_time)

# Save the preprocessed DataFrame to a new CSV file
df_final.to_csv("./data/preprocessed_combine_files.csv", index=False)

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