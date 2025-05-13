import logging
import azure.functions as func
import os
import json
from azure.data.tables import TableServiceClient

def main(msg: func.QueueMessage) -> None:
    opportunity_id = msg.get_body().decode('utf-8')
    logging.info(f'Processing grant data for opportunity ID: {opportunity_id}')
    
    try:
        # Get connection string from environment
        connection_string = os.environ.get("AzureWebJobsStorage")
        
        # Get the grant data from Table Storage
        table_service = TableServiceClient.from_connection_string(connection_string)
        table_client = table_service.get_table_client("GrantDetails")
        
        # Query for the specific grant
        entity = table_client.get_entity(partition_key="Grant", row_key=opportunity_id)
        
        # Process the data (in this example, we're just logging it)
        logging.info(f"Grant title: {entity.get('Title')}")
        logging.info(f"Agency: {entity.get('AgencyName')}")
        logging.info(f"Close date: {entity.get('CloseDate')}")
        
        # Here you could add more processing logic
        # For example, send to another storage system, generate reports, etc.
        
        logging.info(f"Successfully processed grant data for: {opportunity_id}")
        
    except Exception as e:
        logging.error(f"Error in ProcessGrantData function: {str(e)}")
