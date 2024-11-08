import pytest
import boto3
from moto import mock_s3, mock_dynamodb2
import os
import json
from datetime import datetime
from my_module import lambda_handler, generate_dates, read_csv_from_s3, fetch_decomposition_model, write_to_dynamodb  # Assuming your code is in my_module.py

# Environment variable setup
os.environ["SERIES_KNOWLEDGE_TABLE_NAME"] = "SeriesKnowledgeTable"
os.environ["PIPELINE_LOGS_TABLE_NAME"] = "PipelineLogsTable"
os.environ["PIPELINE_BUCKET_NAME"] = "test-bucket"

# Sample data for tests
sample_csv_data = [
    ["series1", "12", "2020", "1", "3", "100", "101", "102"],
    ["series2", "4", "2021", "1", "2", "200", "201"]
]

sample_dynamo_data = {
    "Items": [
        {
            "KeyParam": {"M": {"SeriesName": {"S": "series1"}}},
            "X13": {"M": {"DecompositionModel": {"S": "Additive"}}}
        },
        {
            "KeyParam": {"M": {"SeriesName": {"S": "series2"}}},
            "X13": {"M": {"DecompositionModel": {"S": "Multiplicative"}}}
        }
    ]
}

sample_event = {
    "runid": "test-run",
    "seriesGroupId": "test-group",
    "errors": None
}

@pytest.fixture
def s3_bucket():
    with mock_s3():
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket="test-bucket")
        # Upload mock CSV files
        for file_key, data in [("series_y.csv", sample_csv_data), ("series_sa.csv", sample_csv_data), ("series_t.csv", sample_csv_data)]:
            s3_client.put_object(Bucket="test-bucket", Key=f"CruncherWorkspace/test-run/cruncherOutput/output/SaProcessing-1/{file_key}", Body="\n".join([",".join(row) for row in data]))
        yield

@pytest.fixture
def dynamodb_tables():
    with mock_dynamodb2():
        dynamodb = boto3.client("dynamodb")

        # Mock SeriesKnowledge table
        dynamodb.create_table(
            TableName="SeriesKnowledgeTable",
            KeySchema=[{"AttributeName": "seriesGroupId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "seriesGroupId", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
        )
        # Insert mock data into SeriesKnowledge table
        for item in sample_dynamo_data["Items"]:
            dynamodb.put_item(TableName="SeriesKnowledgeTable", Item=item)

        # Mock PipelineLogs table
        dynamodb.create_table(
            TableName="PipelineLogsTable",
            KeySchema=[{"AttributeName": "series_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "series_id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
        )

        yield

def test_lambda_handler(s3_bucket, dynamodb_tables):
    result = lambda_handler(sample_event, None)

    # Check the result
    assert result["bucketName"] == "test-bucket"
    assert result["objectKey"] == "output.json"

    # Verify JSON output
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket="test-bucket", Key=result["objectKey"])
    output_data = json.loads(response["Body"].read().decode("utf-8"))

    # Assert structure and values in DynamoDB
    assert isinstance(output_data, list)
    assert all("series_id" in item for item in output_data)

    # Verify DynamoDB entries
    dynamodb = boto3.client("dynamodb")
    dynamo_response = dynamodb.scan(TableName="PipelineLogsTable")
    assert len(dynamo_response["Items"]) > 0
