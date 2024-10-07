import csv
import json
import boto3
import os
from datetime import datetime
from decimal import Decimal

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

def read_csv_from_s3(bucket, key):
    # Download the file to a temporary location
    file_path = f'/tmp/{key.split("/")[-1]}'
    s3_client.download_file(bucket, key, file_path)

    # Read the CSV file into a list
    data = []
    with open(file_path, mode='r') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            data.append(row)
    return data

def write_to_dynamodb(data, table_name):
    table = dynamodb_client.Table(table_name)
    for item in data:
        # Each item should contain all required attributes
        table.put_item(Item=item)

def lambda_handler(event, context):
    # Extract workspaceId from event
    workspace_id = event.get('workspaceId', None)
    print(f"Received workspaceId: {workspace_id}")

    # S3 bucket and file keys
    bucket_name = 'my-data-bucket'  # Example S3 bucket name
    original_key = 'data/series_y.csv'  # Path to the original data CSV file
    seasonally_adjusted_key = 'data/series_sa.csv'  # Path to the seasonally adjusted data CSV file
    trend_key = 'data/series_t.csv'  # Path to the trend data CSV file

    # Read CSV files from S3
    original_data = read_csv_from_s3(bucket_name, original_key)
    seasonally_adjusted_data = read_csv_from_s3(bucket_name, seasonally_adjusted_key)
    trend_data = read_csv_from_s3(bucket_name, trend_key)

    # Initialize the data structure for DynamoDB
    dynamo_items = []

    # Process each series
    for index in range(len(original_data)):  # Don't Skip the header row
        row = original_data[index]
        series_id = row[0]  # seriesName
        periodicity = int(row[1])  # periodicity
        start_year = int(row[2])  # start year
        start_month = int(row[3])  # start month
        num_values = int(row[4])  # number of time series values

        # Generate date list for original values based on periodicity
        dates = generate_dates(start_year, start_month, num_values, periodicity)

        # Extract original values and convert to Decimal
        original_values = [Decimal(value) for value in row[5:5 + num_values]]

        # Prepare the series entry for DynamoDB
        series_entry = {
            "itemId": series_id,
            "seriesGroupId": "TEST_01",  # Add the seriesGroupId here
            "tsAdded": int(datetime.now().timestamp()),
            "workspace_id": workspace_id,  # Add workspace_id to the entry
            "original": [{"date": date, "value": value} for date, value in zip(dates, original_values)],
            "seasonally_adjusted": [],
            "trend": []
        }

        # Repeat for seasonally adjusted and trend values
        sa_row = seasonally_adjusted_data[index]
        trend_row = trend_data[index]

        sa_values = [Decimal(value) for value in sa_row[5:5 + num_values]]
        trend_values = [Decimal(value) for value in trend_row[5:5 + num_values]]

        series_entry["seasonally_adjusted"] = [{"date": date, "value": value} for date, value in zip(dates, sa_values)]
        series_entry["trend"] = [{"date": date, "value": value} for date, value in zip(dates, trend_values)]

        # Print the series entry to debug
        print(f"Appending to DynamoDB: {series_entry}")

        # Append the series entry to the items list for DynamoDB
        dynamo_items.append(series_entry)

    # Write the items to the DynamoDB table
    dynamodb_table_name = 'your-dynamodb-table-name'  # Replace with your DynamoDB table name
    write_to_dynamodb(dynamo_items, dynamodb_table_name)

    # Generate a JSON file from the items
    json_file_path = '/tmp/processed_data.json'
    with open(json_file_path, 'w') as json_file:
        json.dump(dynamo_items, json_file, default=str)  # Using default=str to handle Decimal

    print(f"JSON file created at: {json_file_path}")

    # Upload the JSON file to another S3 bucket
    target_bucket_name = 'my-json-bucket'  # Replace with your target S3 bucket name
    target_key = 'processed_data/processed_data.json'  # Path in S3 where the JSON file will be stored

    # Upload the JSON file
    s3_client.upload_file(json_file_path, target_bucket_name, target_key)

    print(f"Uploaded JSON file to s3://{target_bucket_name}/{target_key}")

    # Return a success response
    return {
        'statusCode': 200,
        'body': json.dumps({"message": "Data processed successfully!", "workspace_id": workspace_id, "json_file_path": json_file_path})
    }
