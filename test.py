def fetch_decomposition_model(seriesGroupId, table_name):
    try:
        # Step 1: Fetch the 'latest' item to get the version
        latest_item = dynamodb_client.get_item(
            TableName=table_name,
            Key={'seriesGroupId': {'S': seriesGroupId}, 'itemId': {'S': 'latest'}}
        )
        
        # Extract the version from the latest item
        version = latest_item.get('Item', {}).get('version', {}).get('S')
        if not version:
            print("Version not found in the latest item.")
            return {}
        
        # Step 2: Fetch series items where itemId starts with "{version}_series_"
        response = dynamodb_client.query(
            TableName=table_name,
            KeyConditionExpression="seriesGroupId = :series_group_id AND begins_with(itemId, :prefix)",
            ExpressionAttributeValues={
                ":series_group_id": {"S": seriesGroupId},
                ":prefix": {"S": f"{version}_series_"}
            }
        )
        
        # Safely extract decomposition models
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
