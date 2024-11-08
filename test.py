import csv
import json
import html
import boto3
import os
import tempfile
from datetime import datetime
from decimal import Decimal

# Initialize Boto3 clients
s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')
sk_table_name = os.environ.get("SERIES_KNOWLEDGE_TABLE_NAME")
pl_table_name = os.environ.get("PIPELINE_LOGS_TABLE_NAME")
pipeline_bucket_name = os.environ.get("PIPELINE_BUCKET_NAME")

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
    try:
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
            s3_client.download_file(bucket, key, temp_file.name)
            temp_file.seek(0)
            csvreader = csv.reader(temp_file)
            data = [row for row in csvreader]
        return data
    except Exception as e:
        print(f"Error reading CSV from S3: {e}")
        return []

def fetch_decomposition_model(seriesGroupId, table_name):
    try:
        latest_item = dynamodb_client.get_item(
            TableName=table_name,
            Key={
                'seriesGroupId': {'S': seriesGroupId},
                'itemId': {'S': 'latest'}
            }
        )
        
        version = latest_item.get('Item', {}).get('version', {}).get('S')
        if not version:
            print("Version not found in the latest item.")
            return {}
        
        response = dynamodb_client.query(
            TableName=table_name,
            KeyConditionExpression="seriesGroupId = :series_group_id AND begins_with(itemId, :prefix)",
            ExpressionAttributeValues={
                ":series_group_id": {"S": seriesGroupId},
                ":prefix": {"S": f"{version}_series_"}
            }
        )
        
        decomposition_models = {}
        for item in response.get('Items', []):
            series_name = item.get("KeyParam", {}).get("M", {}).get("SeriesName", {}).get("S")
            decomposition_model = item.get("X13", {}).get("M", {}).get("DecompositionModel", {}).get("S")
            if series_name and decomposition_model:
                decomposition_models[series_name] = decomposition_model
        return decomposition_models

    except Exception as e:
        print(f"Error fetching decomposition model: {e}")
        return {}

def write_to_dynamodb(data, table_name):
    try:
        with dynamodb_client.batch_writer(TableName=table_name) as batch:
            for item in data:
                batch.put_item(
                    Item={
                        'series_id': {'S': item['series_id']},
                        'seriesGroupId': {'S': item['seriesGroupId']},
                        'executionTime': {'N': str(item['executionTime'])},
                        'runId': {'S': item['runId']},
                        'original': {'L': [{'M': {'date': {'S': val['date']}, 'value': {'N': str(val['value'])}}} for val in item['original']]},
                        'seasonally_adjusted': {'L': [{'M': {'date': {'S': val['date']}, 'value': {'N': str(val['value'])}}} for val in item['seasonally_adjusted']]},
                        'trend': {'L': [{'M': {'date': {'S': val['date']}, 'value': {'N': str(val['value'])}}} for val in item['trend']]},
                        'adjustment_factor': {'L': [{'M': {'date': {'S': val['date']}, 'value': {'N': str(val['value'])}}} for val in item['adjustment_factor']]}
                    }
                )
    except Exception as e:
        print(f"Error writing to DynamoDB: {e}")

def lambda_handler(event, context):
    runid = event.get('runid')
    series_group_id = event.get('seriesGroupId')
    bucket_name = event.get('bucketName', pipeline_bucket_name)
    output_prefix = event.get('outputPrefix', '')
    errors = event.get('errors')

    if errors:
        target_key = f"{output_prefix}/{runid}/error.json"
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as json_file:
            json.dump({"errors": errors}, json_file, indent=4)
            s3_client.upload_file(json_file.name, bucket_name, target_key)
        print(f"Uploaded error JSON file to s3://{bucket_name}/{target_key}")
        return {"bucketName": bucket_name, "objectKey": target_key}

    original_key = f"CruncherWorkspace/{runid}/cruncherOutput/output/SaProcessing-1/series_y.csv"
    seasonally_adjusted_key = f"CruncherWorkspace/{runid}/cruncherOutput/output/SaProcessing-1/series_sa.csv"
    trend_key = f"CruncherWorkspace/{runid}/cruncherOutput/output/SaProcessing-1/series_t.csv"
    
    original_data = read_csv_from_s3(bucket_name, original_key)
    seasonally_adjusted_data = read_csv_from_s3(bucket_name, seasonally_adjusted_key)
    trend_data = read_csv_from_s3(bucket_name, trend_key)

    dynamo_items = []
    currentTsAdded = int(datetime.now().timestamp())
    
    decomposition_model_map = fetch_decomposition_model(series_group_id, sk_table_name)

    for original_row, sa_row, trend_row in zip(original_data, seasonally_adjusted_data, trend_data):
        try:
            series_id = original_row[0]
            periodicity = int(original_row[1])
            start_year = int(original_row[2])
            start_month = int(original_row[3])
            num_values = int(original_row[4])

            dates = generate_dates(start_year, start_month, num_values, periodicity)
            original_values = [float(value) for value in original_row[5:5 + num_values]]
            sa_values = [float(value) for value in sa_row[5:5 + num_values]]
            trend_values = [float(value) for value in trend_row[5:5 + num_values]]
            decomposition_model = decomposition_model_map.get(html.escape(series_id), "Additive")
            
            series_entry = {
                "series_id": html.escape(series_id),
                "seriesGroupId": html.escape(series_group_id),
                "executionTime": currentTsAdded,
                "runId": html.escape(runid),
                "original": [{"date": date, "value": value} for date, value in zip(dates, original_values)],
                "seasonally_adjusted": [{"date": date, "value": value} for date, value in zip(dates, sa_values)],
                "trend": [{"date": date, "value": value} for date, value in zip(dates, trend_values)],
                "adjustment_factor": [
                    {
                        "date": date,
                        "value": (orig_val / sa_val if decomposition_model == "Multiplicative" else orig_val - sa_val)
                    } for date, orig_val, sa_val in zip(dates, original_values, sa_values)
                ]
            }
            
            dynamo_items.append(series_entry)
        
        except (ValueError, IndexError) as e:
            print(f"Data validation error for series_id {series_id}: {e}")
            continue
    
    write_to_dynamodb(dynamo_items, pl_table_name)

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as json_file:
        json.dump(dynamo_items, json_file, default=str)
        target_key = f"{output_prefix}/{runid}/output.json"
        s3_client.upload_file(json_file.name, bucket_name, target_key)
        print(f"Uploaded JSON file to s3://{bucket_name}/{target_key}")
    
    return {"bucketName": bucket_name, "objectKey": target_key}
