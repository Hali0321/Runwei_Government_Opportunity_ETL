import logging
import azure.functions as func
import requests
import json
import os
from azure.data.tables import TableServiceClient
from datetime import datetime

def main(msg: func.QueueMessage) -> None:
    logging.info('GetGrantDetails function triggered - UPDATED VERSION V3')
    
    try:
        # Get message content (opportunity ID)
        opportunity_id = msg.get_body().decode('utf-8').strip()
        logging.info(f'Processing opportunity ID: {opportunity_id}')
        
        # Use search API to get details for a specific opportunity ID
        search_url = "https://api.grants.gov/v1/api/search2"
        
        headers = {
            "Content-Type": "application/json"
        }
        
        # Construct a search payload using the opportunity ID
        # Search by number and/or ID to try to find the grant
        payload = {
            "oppNum": opportunity_id,  # Try using the ID as the opportunity number
            "rows": 1                  # We only need one result
        }
        
        logging.info(f'Searching for grant with ID: {opportunity_id}')
        response = requests.post(search_url, headers=headers, json=payload)
        
        grant_data = None
        
        if response.status_code != 200:
            logging.error(f'Search API call failed with status {response.status_code}: {response.text[:200]}')
            
            # Try a different search approach using the ID field
            alt_payload = {
                "keyword": opportunity_id,  # Try using the ID as a keyword
                "rows": 1
            }
            
            logging.info(f'Trying alternative search for grant with ID: {opportunity_id}')
            alt_response = requests.post(search_url, headers=headers, json=alt_payload)
            
            if alt_response.status_code != 200:
                logging.error(f'Alternative search API call also failed with status {alt_response.status_code}')
                return
            else:
                response = alt_response
                logging.info('Alternative search API call succeeded')
        
        # Parse response JSON
        search_results = response.json()
        logging.info(f'Search response received for grant: {opportunity_id}')
        
        # Check for error code
        error_code = search_results.get("errorcode")
        if error_code and error_code != 0:
            logging.error(f"API returned error code {error_code}: {search_results.get('msg', 'Unknown error')}")
            return
            
        # Extract the grant data from the search results
        if "data" in search_results and isinstance(search_results["data"], dict):
            data = search_results["data"]
            
            if "oppHits" in data and isinstance(data["oppHits"], list) and len(data["oppHits"]) > 0:
                # Use the first result
                grant_data = data["oppHits"][0]
                logging.info(f"Found grant details in search results for {opportunity_id}")
            else:
                logging.warning(f"No grant details found in search results for {opportunity_id}")
                # Create a basic grant data structure with the ID
                grant_data = {
                    "id": opportunity_id,
                    "title": f"Grant {opportunity_id}",
                    "agency": "Unknown",
                    "description": "Details not available"
                }
                
        # If we still don't have grant data, use a fallback with the ID
        if not grant_data:
            logging.warning(f"Creating fallback grant data for {opportunity_id}")
            grant_data = {
                "id": opportunity_id,
                "title": f"Grant {opportunity_id}",
                "agency": "Unknown",
                "description": "Details not available"
            }
        
        # Get connection string from environment
        connection_string = os.environ.get("AzureWebJobsStorage")
        
        # Initialize table client
        table_service = TableServiceClient.from_connection_string(connection_string)
        
        # Create table if it doesn't exist
        table_name = "GrantDetails"
        try:
            table_service.create_table(table_name)
        except Exception as e:
            # Table likely exists
            pass
        
        # Get table client
        table_client = table_service.get_table_client(table_name)
        
        # Clean text fields to prevent storage issues
        def clean_text(text, max_len=32000):
            if text is None:
                return ""
            if isinstance(text, str):
                text = text.replace("\u0000", "")  # Remove null characters
                if len(text) > max_len:
                    return text[:max_len] + "... (truncated)"
                return text
            return str(text)
        
        # Extract fields from grant_data based on the search API response structure
        try:
            # Log what fields we have available
            logging.info(f"Grant data fields: {list(grant_data.keys())}")
            
            # Extract fields based on the API response format we've seen
            title = clean_text(grant_data.get("title", ""))
            agency = clean_text(grant_data.get("agency", ""))
            
            # These fields are not in the search results, but we'll include them for completeness
            description = clean_text(grant_data.get("description", ""))
            open_date = clean_text(grant_data.get("openDate", ""))
            close_date = clean_text(grant_data.get("closeDate", ""))
            agency_code = clean_text(grant_data.get("agencyCode", ""))
            opp_status = clean_text(grant_data.get("oppStatus", ""))
            
            # Extract CFDA numbers if available
            cfda_numbers = []
            if "cfdaList" in grant_data and isinstance(grant_data["cfdaList"], list):
                cfda_numbers = grant_data["cfdaList"]
            cfda_string = ", ".join(str(cfda) for cfda in cfda_numbers)
            
        except Exception as extract_error:
            logging.error(f'Error extracting fields: {str(extract_error)}')
            import traceback
            logging.error(traceback.format_exc())
            
            # Use default values if extraction fails
            title = f"Grant {opportunity_id}"
            agency = "Unknown"
            description = "Error extracting details"
            open_date = ""
            close_date = ""
            agency_code = ""
            opp_status = ""
            cfda_string = ""
        
        # Prepare entity - using fields that match what we found in the API response
        entity = {
            "PartitionKey": "Grant",
            "RowKey": opportunity_id,
            "Title": title,
            "AgencyName": agency,
            "Description": description,
            "OpenDate": open_date,
            "CloseDate": close_date,
            "AgencyCode": agency_code,
            "Status": opp_status,
            "CFDANumbers": cfda_string,
            "OpportunityURL": f"https://www.grants.gov/search-results-detail/{opportunity_id}",
            "LastUpdated": datetime.now().isoformat()
        }
        
        # Add to table
        table_client.upsert_entity(entity)
        logging.info(f"Successfully saved grant details for {opportunity_id}")
        
    except Exception as e:
        logging.error(f'Error in GetGrantDetails: {str(e)}')
        import traceback
        logging.error(traceback.format_exc())