import pytest
import json
import boto3
from moto import mock_s3, mock_dynamodb2
from your_lambda_module import lambda_handler  # Update with your actual module name

# Sample test data for the CSV files
original_data = [
    ["series_id", "periodicity", "start_year", "start_month", "num_values", "value1", "value2", "value3"],
    ["ABC", "12", "2001", "1", "3", "10.1", "20.2", "30.3"],
    ["XYZ", "4", "2002", "1", "2", "40.4", "50.5"]
]

seasonally_adjusted_data = [
    ["series_id", "periodicity", "start_year", "start_month", "num_values", "value1", "value2"],
    ["ABC", "12", "2001", "1", "3", "12.1", "22.2"],
    ["XYZ", "4", "2002", "1", "2", "42.4", "52.5"]
]

trend_data = [
    ["series_id", "periodicity", "start_year", "start_month", "num_values", "value1", "value2"],
    ["ABC", "12", "2001", "1", "3", "14.1", "24.2"],
    ["XYZ", "4", "2002", "1", "2", "44.4", "54.5"]
]

@pytest.fixture
def aws_setup():
    # Mock S3 and DynamoDB
    with mock_s3():
        # Set up S3 for input
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='my-data-bucket')
        s3.put_object(Bucket='my-data-bucket', Key='data/series_y.csv', Body='\n'.join(','.join(row) for row in original_data).encode('utf-8'))
        s3.put_object(Bucket='my-data-bucket', Key='data/series_sa.csv', Body='\n'.join(','.join(row) for row in seasonally_adjusted_data).encode('utf-8'))
        s3.put_object(Bucket='my-data-bucket', Key='data/series_t.csv', Body='\n'.join(','.join(row) for row in trend_data).encode('utf-8'))

        # Set up S3 for output
        s3.create_bucket(Bucket='my-json-bucket')

        # Set up DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.create_table(
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
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )

        yield  # This is where the test runs

@pytest.mark.parametrize("workspace_id", ["test_workspace"])
def test_lambda_handler(aws_setup, workspace_id):
    event = {
        "workspaceId": workspace_id
    }
    context = {}  # Mock context if needed

    response = lambda_handler(event, context)

    # Assertions
    assert response['statusCode'] == 200
    assert "Data processed successfully!" in response['body']
    assert workspace_id in response['body']

    # Check if data was written to DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('your-dynamodb-table-name')
    
    # Retrieve items from DynamoDB
    items = table.scan()['Items']
    assert len(items) == 4  # We have 2 series with 2 entries each (original, SA, trend)

    # Check the uploaded JSON in the S3 bucket
    s3 = boto3.client('s3')
    target_bucket = 'my-json-bucket'
    target_key = 'processed_data/processed_data.json'

    # Retrieve the JSON file uploaded to S3
    json_obj = s3.get_object(Bucket=target_bucket, Key=target_key)
    json_content = json_obj['Body'].read().decode('utf-8')
    json_data = json.loads(json_content)

    assert len(json_data) == 4  # The same number of entries as in DynamoDB
    assert all('itemId' in item for item in json_data)  # Check if each item has an 'itemId'

if __name__ == "__main__":
    pytest.main()
