import azure.functions as func
import json
import logging
import requests
import os
from azure.data.tables import TableServiceClient
from datetime import datetime
from typing import Dict, List, Optional

def main(req: func.HttpRequest) -> func.HttpResponse:
    """Collect grant opportunities from Grants.gov API and store in Azure Storage"""
    
    logging.info('GrantsCollector function processing request')
    
    try:
        # Get parameters
        limit = int(req.params.get('limit', 100))
        agency = req.params.get('agency', '')
        keyword = req.params.get('keyword', '')
        
        # Collect grant data
        grants_data = collect_grants_data(limit, agency, keyword)
        
        # Store in Azure Storage
        storage_result = store_grants_data(grants_data)
        
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "message": f"Collected and stored {len(grants_data)} grant opportunities",
                "grants_collected": len(grants_data),
                "storage_result": storage_result,
                "timestamp": datetime.utcnow().isoformat()
            }),
            mimetype="application/json",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"Error in GrantsCollector: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "error": f"Collection failed: {str(e)}",
                "timestamp": datetime.utcnow().isoformat()
            }),
            status_code=500,
            mimetype="application/json"
        )

def collect_grants_data(limit: int, agency: str = "", keyword: str = "") -> List[Dict]:
    """Collect grant opportunities from Grants.gov API"""
    
    base_url = "https://www.grants.gov/grantsws/rest/opportunities/search/"
    
    # Build search parameters
    params = {
        "format": "json",
        "rows": min(limit, 1000),  # API limit
        "sortby": "opendate|desc"
    }
    
    if agency:
        params["agencycode"] = agency
    if keyword:
        params["keyword"] = keyword
    
    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract opportunities from API response
        if "oppHits" in data and data["oppHits"]:
            opportunities = data["oppHits"]
            logging.info(f"Successfully collected {len(opportunities)} opportunities")
            return opportunities
        else:
            logging.warning("No opportunities found in API response")
            return []
            
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {str(e)}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse API response: {str(e)}")
        return []

def store_grants_data(grants_data: List[Dict]) -> Dict:
    """Store grant data in Azure Table Storage"""
    
    try:
        connection_string = os.environ.get('STORAGE_CONNECTION_STRING') or os.environ.get('AzureWebJobsStorage')
        if not connection_string:
            return {"error": "No storage connection string available"}
        
        table_service = TableServiceClient.from_connection_string(connection_string)
        table_name = 'GrantsData'
        
        # Create table if it doesn't exist
        table_client = table_service.create_table_if_not_exists(table_name)
        
        stored_count = 0
        errors = []
        
        for grant in grants_data:
            try:
                # Create entity for Azure Table Storage
                entity = {
                    'PartitionKey': 'Grant',
                    'RowKey': str(grant.get('id', f'grant_{stored_count}')),
                    'OpportunityId': str(grant.get('id', '')),
                    'Title': str(grant.get('title', ''))[:1000],  # Limit length
                    'AgencyName': str(grant.get('agency', '')),
                    'Description': str(grant.get('synopsis', ''))[:1000],
                    'PostedDate': grant.get('postedDate', ''),
                    'CloseDate': grant.get('closeDate', ''),
                    'CollectedDate': datetime.utcnow().isoformat(),
                    'RawData': json.dumps(grant)[:10000]  # Store original data (limited)
                }
                
                table_client.upsert_entity(entity)
                stored_count += 1
                
            except Exception as e:
                errors.append(f"Failed to store grant {grant.get('id', 'unknown')}: {str(e)}")
        
        return {
            "stored_count": stored_count,
            "total_grants": len(grants_data),
            "errors": errors[:5],  # Limit error list
            "table_name": table_name
        }
        
    except Exception as e:
        return {"error": f"Storage operation failed: {str(e)}"}