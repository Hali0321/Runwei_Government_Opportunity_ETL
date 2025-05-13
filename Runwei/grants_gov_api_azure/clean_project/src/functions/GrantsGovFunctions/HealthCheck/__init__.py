import logging
import azure.functions as func
import os
import json
import requests
from azure.data.tables import TableServiceClient
from azure.storage.queue import QueueClient
from datetime import datetime

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HealthCheck function processing request')
    
    health_status = {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "components": {}
    }
    
    try:
        # Check Azure Storage connection
        connection_string = os.environ.get("AzureWebJobsStorage")
        if not connection_string:
            health_status["components"]["storage"] = {
                "status": "error",
                "message": "AzureWebJobsStorage connection string not found"
            }
            health_status["status"] = "error"
        else:
            # Check Table Storage
            try:
                table_service = TableServiceClient.from_connection_string(connection_string)
                table_client = table_service.get_table_client("GrantDetails")
                grants = list(table_client.query_entities("PartitionKey eq 'Grant'", top=1))
                health_status["components"]["table_storage"] = {
                    "status": "ok",
                    "message": f"Found {len(grants)} grants in table (sample check)"
                }
            except Exception as e:
                health_status["components"]["table_storage"] = {
                    "status": "error",
                    "message": str(e)
                }
                health_status["status"] = "degraded"
            
            # Check Queue Storage
            try:
                queue_client = QueueClient.from_connection_string(connection_string, "grants-processing")
                props = queue_client.get_queue_properties()
                health_status["components"]["queue"] = {
                    "status": "ok",
                    "message": f"Queue exists. Approx. {props.approximate_message_count} messages in queue"
                }
            except Exception as e:
                health_status["components"]["queue"] = {
                    "status": "error",
                    "message": str(e)
                }
                health_status["status"] = "degraded"
        
        # Check Grants.gov API
        try:
            search_url = "https://api.grants.gov/v1/api/search2"
            headers = {"Content-Type": "application/json"}
            payload = {"keyword": "health", "rows": 1}
            
            response = requests.post(search_url, headers=headers, json=payload)
            
            if response.status_code == 200:
                health_status["components"]["grants_gov_api"] = {
                    "status": "ok",
                    "message": "API responded with status 200"
                }
            else:
                health_status["components"]["grants_gov_api"] = {
                    "status": "error",
                    "message": f"API responded with status {response.status_code}"
                }
                health_status["status"] = "degraded"
        except Exception as e:
            health_status["components"]["grants_gov_api"] = {
                "status": "error",
                "message": str(e)
            }
            health_status["status"] = "degraded"
            
        # Check if we have recent data
        try:
            yesterday = (datetime.now() - timedelta(days=1)).isoformat()
            recent_grants_query = list(table_client.query_entities(f"PartitionKey eq 'Grant' and LastUpdated gt '{yesterday}'"))
            
            if recent_grants_query:
                health_status["components"]["data_freshness"] = {
                    "status": "ok",
                    "message": f"Found {len(recent_grants_query)} grants updated in the last 24 hours"
                }
            else:
                health_status["components"]["data_freshness"] = {
                    "status": "warning",
                    "message": "No grants have been updated in the last 24 hours"
                }
                if health_status["status"] == "ok":
                    health_status["status"] = "warning"
        except Exception as e:
            health_status["components"]["data_freshness"] = {
                "status": "error",
                "message": f"Error checking data freshness: {str(e)}"
            }
    
    except Exception as e:
        health_status["status"] = "error"
        health_status["message"] = str(e)
    
    return func.HttpResponse(
        json.dumps(health_status, indent=2),
        mimetype="application/json",
        status_code=200 if health_status["status"] == "ok" else 503
    )
