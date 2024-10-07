import pandas as pd
import json
import boto3
import os
from datetime import datetime

# Initialize Boto3 clients
s3_client = boto3.client('s3')
dynamodb_client = boto3.resource('dynamodb')

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

def read_csv_from_s3(bucket, key, temp_file_name):
    # Define the full temporary file path with a unique name
    file_path = f'/tmp/{temp_file_name}'  # Use a unique name for each file
    s3_client.download_file(bucket, key, file_path)

    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path, header=None)
    return df

def write_to_dynamodb(data, table_name):
    table = dynamodb_client.Table(table_name)
    for item in data:
        # Assuming item is a dictionary with the necessary keys
        table.put_item(Item=item)

def lambda_handler(event, context):
    # Extract workspaceId from event
    workspace_id = event.get('workspaceId', None)
    print(f"Received workspaceId: {workspace_id}")

    # S3 bucket and file keys
    bucket_name = 'your-s3-bucket-name'  # Replace with your S3 bucket name
    original_key = 'path/to/series_y.csv'  # Replace with your S3 key for series_y
    seasonally_adjusted_key = 'path/to/series_sa.csv'  # Replace with your S3 key for series_sa
    trend_key = 'path/to/series_t.csv'  # Replace with your S3 key for series_t

    # Read CSV files from S3 with unique filenames
    original_df = read_csv_from_s3(bucket_name, original_key, 'series_y.csv')
    seasonally_adjusted_df = read_csv_from_s3(bucket_name, seasonally_adjusted_key, 'series_sa.csv')
    trend_df = read_csv_from_s3(bucket_name, trend_key, 'series_t.csv')

    # Initialize the data structure for DynamoDB
    dynamo_items = []

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
        
        # Prepare the series entry for DynamoDB
        series_entry = {
            "series_id": series_id,
            "workspace_id": workspace_id,  # Add workspace_id to the entry
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

        # Append the series entry to the items list for DynamoDB
        dynamo_items.append(series_entry)

    # Write the items to the DynamoDB table
    dynamodb_table_name = 'your-dynamodb-table-name'  # Replace with your DynamoDB table name
    write_to_dynamodb(dynamo_items, dynamodb_table_name)

    # Return a success response
    return {
        'statusCode': 200,
        'body': json.dumps({"message": "Data processed successfully!", "workspace_id": workspace_id})
    }
