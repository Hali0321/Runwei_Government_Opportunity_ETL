import azure.functions as func
import json
import logging
import os
from azure.data.tables import TableServiceClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('SearchGrants function processed a request.')
    
    try:
        # Get connection string
        connection_string = os.environ.get('STORAGE_CONNECTION_STRING') or os.environ.get('AzureWebJobsStorage')
        if not connection_string:
            raise Exception("No storage connection string found")
        
        # Get search parameters
        keyword = req.params.get('keyword', '')
        limit = int(req.params.get('limit', 20))
        
        # Connect to Azure Table
        table_service = TableServiceClient.from_connection_string(connection_string)
        table_client = table_service.get_table_client('GrantDetails')
        
        # Build query filter
        if keyword:
            query_filter = f"PartitionKey eq 'Grant' and (contains(Title, '{keyword}') or contains(Description, '{keyword}'))"
        else:
            query_filter = "PartitionKey eq 'Grant'"
        
        # Query grants
        entities = list(table_client.query_entities(
            query_filter=query_filter,
            results_per_page=limit
        ))
        
        # Format results
        grants_data = []
        for entity in entities:
            grant = {
                'id': entity.get('RowKey'),
                'title': entity.get('Title'),
                'agency': entity.get('AgencyName'),
                'description': entity.get('Description', '')[:200] + '...',
                'status': entity.get('Status'),
                'postDate': entity.get('PostedDate'),
                'closeDate': entity.get('CloseDate')
            }
            grants_data.append(grant)
        
        return func.HttpResponse(
            json.dumps({
                'query': keyword,
                'total': len(grants_data),
                'grants': grants_data
            }),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )
        
    except Exception as e:
        logging.error(f"SearchGrants error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )
