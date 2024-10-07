import pandas as pd
import json
from datetime import datetime, timedelta

# Function to create date list based on start year, start month, periodicity, and number of values
def generate_dates(start_year, start_month, num_values, periodicity):
    dates = []
    for i in range(num_values):
        if periodicity == 12:  # Monthly
            month = (start_month + i - 1) % 12 + 1
            year = start_year + (start_month + i - 1) // 12
            dates.append(f"{year}-{month:02d}-01")  # YYYY-mm-DD format
        elif periodicity == 4:  # Quarterly
            month = (start_month + (i * 3) - 1) % 12 + 1
            year = start_year + (start_month + (i * 3) - 1) // 12
            dates.append(f"{year}-{month:02d}-01")  # YYYY-mm-DD format
    return dates

# Read CSV files
original_df = pd.read_csv('series_y.csv', header=None)
seasonally_adjusted_df = pd.read_csv('series_sa.csv', header=None)
trend_df = pd.read_csv('series_t.csv', header=None)

# Initialize the JSON structure
data = {
    "series_group": "TEST_01",
    "series_list": []
}

# Process each series
for index, row in original_df.iterrows():
    series_id = row[0]  # seriesName
    periodicity = row[1]  # periodicity
    start_year = row[2]  # start year
    start_month = row[3]  # start month
    num_values = row[4]  # number of time series values

    # Generate date list for original values based on periodicity
    dates = generate_dates(start_year, start_month, num_values, periodicity)
    
    # Extract original values
    original_values = row[5:5 + num_values].tolist()
    
    # Prepare series entry
    series_entry = {
        "series_id": series_id,
        "original": [{"date": date, "value": value} for date, value in zip(dates, original_values)],
        "seasonally_adjusted": [],
        "trend": []
    }

    # Repeat for seasonally adjusted and trend values
    sa_row = seasonally_adjusted_df.iloc[index]
    trend_row = trend_df.iloc[index]

    sa_values = sa_row[5:5 + num_values].tolist()
    trend_values = trend_row[5:5 + num_values].tolist()

    series_entry["seasonally_adjusted"] = [{"date": date, "value": value} for date, value in zip(dates, sa_values)]
    series_entry["trend"] = [{"date": date, "value": value} for date, value in zip(dates, trend_values)]

    # Append the series entry to the series list
    data["series_list"].append(series_entry)

# Write the JSON to a file
with open('output.json', 'w') as json_file:
    json.dump(data, json_file, indent=4)

print("JSON file created successfully!")
