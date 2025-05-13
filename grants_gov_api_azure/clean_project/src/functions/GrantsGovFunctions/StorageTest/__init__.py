import logging
import azure.functions as func
import os
import json
from azure.data.tables import TableServiceClient
from datetime import datetime

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('StorageTest HTTP trigger function processed a request.')
    
    try:
        # Get connection string from environment
        connection_string = os.environ.get("AzureWebJobsStorage")
        if not connection_string:
            return func.HttpResponse(
                "AzureWebJobsStorage connection string not found in environment variables",
                status_code=500
            )
            
        logging.info("Got storage connection string")
        
        # Initialize table client
        table_service = TableServiceClient.from_connection_string(connection_string)
        logging.info("Created TableServiceClient")
        
        # Create table if it doesn't exist
        table_name = "GrantDetails"
        try:
            table_service.create_table(table_name)
            logging.info(f"Created table: {table_name}")
        except Exception as e:
            logging.info(f"Table exists or couldn't be created: {str(e)}")
        
        # Get table client
        table_client = table_service.get_table_client(table_name)
        logging.info("Got table client")
        
        # Generate a test ID
        test_id = f"TEST-{datetime.now().strftime('%H%M%S')}"
        
        # Prepare entity with correct URL format
        entity = {
            "PartitionKey": "Grant",
            "RowKey": test_id,
            "Title": "Test Grant Entry",
            "AgencyName": "Test Agency",
            "PostDate": "2025-05-12",
            "CloseDate": "2025-06-12",
            "OpportunityURL": f"https://www.grants.gov/search-results-detail/{test_id}",
            "LastUpdated": datetime.now().isoformat()
        }
        
        # Add to table
        table_client.upsert_entity(entity)
        logging.info(f"Successfully saved test entry with ID: {test_id}")
        
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "message": f"Test entry created with ID: {test_id}",
                "details": {
                    "connection_string_exists": connection_string is not None,
                    "table_name": table_name,
                    "entity_id": test_id,
                    "opportunity_url": entity["OpportunityURL"]
                }
            }),
            mimetype="application/json",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"Error in StorageTest function: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "message": str(e),
                "traceback": traceback.format_exc()
            }),
            mimetype="application/json",
            status_code=500
        )