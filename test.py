import boto3
from botocore.exceptions import ClientError

def update_dynamodb_item(table_name, series_group_id, series_id, length_desired=None, start_date_desired=None):
    # Initialize the DynamoDB client
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Initialize parts of the update expression dynamically
    update_expression = []
    expression_attribute_values = {}

    # Add LengthDesired to the update expression if it's not None
    if length_desired is not None:
        update_expression.append("KeyParam.UnfrozenSpan.LengthDesired = :length")
        expression_attribute_values[':length'] = length_desired

    # Add StartDateDesired to the update expression if it's not None
    if start_date_desired is not None:
        update_expression.append("KeyParam.UnfrozenSpan.StartDateDesired = :start_date")
        expression_attribute_values[':start_date'] = start_date_desired

    # Ensure there's something to update
    if not update_expression:
        print("No attributes to update.")
        return

    # Join the update expression parts with commas
    update_expression_str = "SET " + ", ".join(update_expression)

    try:
        # Update the item in DynamoDB
        response = table.update_item(
            Key={
                'SeriesGroupID': series_group_id,
                'SeriesID': series_id,
            },
            UpdateExpression=update_expression_str,
            ExpressionAttributeValues=expression_attribute_values,
            ConditionExpression="attribute_exists(SeriesGroupID) AND attribute_exists(SeriesID)",
            ReturnValues="UPDATED_NEW"
        )
        print("Update succeeded:", response)
        return response

    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            print("Item does not exist.")
        else:
            print(f"Unexpected error: {e}")
        raise

# Example usage
if __name__ == "__main__":
    table_name = "YourDynamoDBTable"
    series_group_id = "group123"
    series_id = "item456"

    # Test with both attributes
    update_dynamodb_item(table_name, series_group_id, series_id, length_desired=10, start_date_desired="2023-12-01")

    # Test with only LengthDesired
    update_dynamodb_item(table_name, series_group_id, series_id, length_desired=15)

    # Test with only StartDateDesired
    update_dynamodb_item(table_name, series_group_id, series_id, start_date_desired="2024-01-01")

    # Test with no attributes (should print "No attributes to update")
    update_dynamodb_item(table_name, series_group_id, series_id)
