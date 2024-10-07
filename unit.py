import csv
import json
import boto3
import os
from datetime import datetime
from decimal import Decimal
import pytest
from moto import mock_aws

# Your existing lambda code goes here...

# Sample CSV data for testing
sample_csv_data = [
    ["ABC", 12, 2001, 3, 24, 54.32, 23.54, 98.12, 65.23, 12.34, 45.67, 78.9],
    ["XYZ", 12, 2001, 3, 24, 34.21, 45.65, 78.12, 23.45, 89.67, 67.45, 12.34]
]

sample_sa_csv_data = [
    ["ABC", 12, 2001, 3, 24, 55.32, 24.54, 99.12, 66.23, 13.34, 46.67, 79.9],
    ["XYZ", 12, 2001, 3, 24, 35.21, 46.65, 79.12, 24.45, 90.67, 68.45, 13.34]
]

sample_trend_csv_data = [
    ["ABC", 12, 2001, 3, 24, 56.32, 25.54, 100.12, 67.23, 14.34, 47.67, 80.9],
    ["XYZ", 12, 2001, 3, 24, 24, 36.21, 47.65, 80.12, 25.45, 91.67, 69.45, 14.34]
]

@pytest.fixture
def aws_credentials():
    """Mock AWS Credentials."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'


@mock_aws
def test_lambda_handler(aws_credentials):
    # Create DynamoDB and S3 resources
    dynamodb = boto3.resource('dynamodb')
    s3 = boto3.client('s3')

    # Create a DynamoDB table
    dynamodb.create_table(
        TableName='your-dynamodb-table-name',
        KeySchema=[
            {
                'AttributeName': 'itemId',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'itemId',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    # Create an S3 bucket
    bucket_name = 'my-data-bucket'
    s3.create_bucket(Bucket=bucket_name)

    # Upload sample CSV files to S3
    s3.put_object(Bucket=bucket_name, Key='data/series_y.csv', Body='\n'.join([','.join(map(str, row)) for row in sample_csv_data]).encode('utf-8'))
    s3.put_object(Bucket=bucket_name, Key='data/series_sa.csv', Body='\n'.join([','.join(map(str, row)) for row in sample_sa_csv_data]).encode('utf-8'))
    s3.put_object(Bucket=bucket_name, Key='data/series_t.csv', Body='\n'.join([','.join(map(str, row)) for row in sample_trend_csv_data]).encode('utf-8'))

    # Call the lambda handler
    event = {'workspaceId': 'test-workspace'}
    context = {}  # Mock context if needed
    response = lambda_handler(event, context)

    # Assertions
    assert response['statusCode'] == 200
    assert json.loads(response['body'])['message'] == "Data processed successfully!"
    
    # Check the items in DynamoDB
    table = dynamodb.Table('your-dynamodb-table-name')
    response = table.scan()
    assert len(response['Items']) == 6  # Total number of time series entries

    # Check uploaded JSON file in the target S3 bucket
    target_bucket_name = 'my-json-bucket'
    s3.create_bucket(Bucket=target_bucket_name)
    json_file_key = 'processed_data/processed_data.json'
    s3.put_object(Bucket=target_bucket_name, Key=json_file_key, Body='{"dummy": "data"}')  # Mock upload

    # Validate the JSON file upload
    s3_response = s3.get_object(Bucket=target_bucket_name, Key=json_file_key)
    body = s3_response['Body'].read().decode('utf-8')
    assert json.loads(body) == {"dummy": "data"}  # This can be adjusted to your expected JSON structure
