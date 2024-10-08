import pytest
import boto3
from moto import mock_s3, mock_dynamodb
from decimal import Decimal
import json
import os

# Import your Lambda handler and related functions
from my_lambda_module import lambda_handler, write_to_dynamodb, read_csv_from_s3

# Sample CSV data
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
    ["XYZ", 12, 2001, 3, 24, 24.00, 36.21, 47.65, 80.12, 25.45, 91.67, 69.45, 14.34]
]

@pytest.fixture
def s3_setup():
    """Setup mock S3 bucket and upload sample CSV files."""
    with mock_s3():
        s3_client = boto3.client('s3', region_name='us-east-1')
        bucket_name = 'my-data-bucket'
        s3_client.create_bucket(Bucket=bucket_name)

        # Upload sample CSV data to S3
        s3_client.put_object(Bucket=bucket_name, Key='data/series_y.csv', Body=csv_content(sample_csv_data))
        s3_client.put_object(Bucket=bucket_name, Key='data/series_sa.csv', Body=csv_content(sample_sa_csv_data))
        s3_client.put_object(Bucket=bucket_name, Key='data/series_t.csv', Body=csv_content(sample_trend_csv_data))

        yield s3_client

@pytest.fixture
def dynamodb_setup():
    """Setup mock DynamoDB table."""
    with mock_dynamodb():
        dynamodb_client = boto3.client('dynamodb', region_name='us-east-1')
        table_name = 'your-dynamodb-table-name'
        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'series_id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'series_id', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        yield dynamodb_client

def csv_content(data):
    """Helper function to convert data to CSV string."""
    return "\n".join([",".join(map(str, row)) for row in data])

@pytest.mark.usefixtures("s3_setup", "dynamodb_setup")
@mock_aws
def test_lambda_handler(s3_setup, dynamodb_setup):
    """Test the lambda_handler function."""
    event = {'workspaceId': 'test_workspace'}
    context = {}  # Mocking the context

    # Invoke the Lambda handler
    response = lambda_handler(event, context)

    # Verify the response
    assert response['statusCode'] == 200
    assert json.loads(response['body'])['message'] == "Data processed successfully!"
    assert json.loads(response['body'])['workspace_id'] == "test_workspace"

    # Verify DynamoDB entries
    dynamodb_client = boto3.client('dynamodb', region_name='us-east-1')
    response = dynamodb_client.scan(TableName='your-dynamodb-table-name')
    items = response['Items']
    
    assert len(items) == 2  # Two series should be inserted
    assert items[0]['series_id']['S'] == 'ABC'
    assert items[1]['series_id']['S'] == 'XYZ'

    # Verify one of the series values
    original_series = items[0]['original']['L']
    assert len(original_series) == 24  # 24 time series values
    assert original_series[0]['M']['date']['S'] == '2001-03-01'
    assert original_series[0]['M']['value']['N'] == '54.32'

def test_csv_reading(s3_setup):
    """Test reading CSV from S3."""
    bucket_name = 'my-data-bucket'
    original_key = 'data/series_y.csv'
    
    # Read the CSV file
    data = read_csv_from_s3(bucket_name, original_key)

    # Verify CSV content
    assert len(data) == 2
    assert data[0][0] == 'ABC'
    assert data[1][0] == 'XYZ'

def test_write_to_dynamodb(dynamodb_setup):
    """Test writing items to DynamoDB."""
    table_name = 'your-dynamodb-table-name'
    data = [
        {
            "series_id": "TEST_ID",
            "seriesGroupId": "TEST_01",
            "workspace_id": "test_workspace",
            "original": [{"date": "2001-03-01", "value": Decimal('54.32')}],
            "seasonally_adjusted": [{"date": "2001-03-01", "value": Decimal('55.32')}],
            "trend": [{"date": "2001-03-01", "value": Decimal('56.32')}]
        }
    ]
    
    # Write to DynamoDB
    write_to_dynamodb(data, table_name)

    # Verify DynamoDB entry
    dynamodb_client = boto3.client('dynamodb', region_name='us-east-1')
    response = dynamodb_client.scan(TableName=table_name)
    items = response['Items']

    assert len(items) == 1
    assert items[0]['series_id']['S'] == 'TEST_ID'
