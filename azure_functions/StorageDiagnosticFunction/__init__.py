import azure.functions as func
import json
import logging
import os
from azure.data.tables import TableServiceClient
from typing import Dict, List

def main(req: func.HttpRequest) -> func.HttpResponse:
    """Diagnostic function to check Azure Storage connectivity and available data"""
    
    logging.info('StorageDiagnostic function processing request')
    
    try:
        # Use the provided connection string
        connection_string = "DefaultEndpointsProtocol=https;AccountName=grantsgov225756;AccountKey=UXwW5dfy9MY9nh2BGmWhYUbzBve+6LUyT3F7+N3Cp0kWUoEk4AO3z5U6LrBYvo/VwO+Nduq2ay9E+AStKQb86Q==;EndpointSuffix=core.windows.net"
        
        diagnostic_info = {
            "storage_account": "grantsgov225756",
            "connection_status": {},
            "available_tables": [],
            "table_details": {},
            "grant_tables_found": []
        }
        
        # Test direct connection string
        try:
            table_service = TableServiceClient.from_connection_string(connection_string)
            diagnostic_info["connection_status"]["direct"] = "SUCCESS"
            
            # List all tables
            tables = list(table_service.list_tables())
            diagnostic_info["available_tables"] = [table.name for table in tables]
            
            # Focus on grant-related tables
            grant_tables = [t for t in diagnostic_info["available_tables"] 
                          if any(keyword in t.lower() for keyword in ['grant', 'company', 'transformed'])]
            diagnostic_info["grant_tables_found"] = grant_tables
            
            # Get details for grant tables
            for table_name in grant_tables:
                try:
                    table_client = table_service.get_table_client(table_name)
                    
                    # Query entities without filter (get all)
                    entities = list(table_client.list_entities(results_per_page=10))
                    
                    # Get sample entity structure
                    sample_entity = entities[0] if entities else None
                    
                    diagnostic_info["table_details"][table_name] = {
                        "entity_count": f"{len(entities)}+ (showing first 10)",
                        "sample_fields": list(sample_entity.keys()) if sample_entity else [],
                        "sample_partition_keys": list(set([e.get('PartitionKey', '') for e in entities])),
                        "sample_row_keys": [e.get('RowKey', '') for e in entities[:3]],
                        "sample_data": dict(sample_entity) if sample_entity else {}
                    }
                    
                except Exception as e:
                    diagnostic_info["table_details"][table_name] = {
                        "error": str(e)
                    }
            
        except Exception as e:
            diagnostic_info["connection_status"]["direct"] = f"FAILED: {str(e)}"
        
        return func.HttpResponse(
            json.dumps(diagnostic_info, indent=2, default=str),
            mimetype="application/json",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"Diagnostic error: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "error": f"Diagnostic failed: {str(e)}",
                "storage_account": "grantsgov225756"
            }),
            status_code=500,
            mimetype="application/json"
        )