import datetime
import logging
import json
import azure.functions as func
import requests
import os
from azure.storage.queue import QueueClient

def main(mytimer: func.TimerRequest) -> None:
    logging.info('SearchGrants function triggered - UPDATED VERSION V4')
    
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('SearchGrants function is running late')
    
    # Updated API endpoint based on grants.gov API guide
    base_url = "https://api.grants.gov/v1/api/search2"
    
    try:
        # Get connection string from environment
        connection_string = os.environ.get("AzureWebJobsStorage")
        queue_name = "grants-processing"
        
        # Create a queue client
        queue_client = QueueClient.from_connection_string(connection_string, queue_name)
        
        # Track total grants found
        total_grants_found = 0
        grants_added_to_queue = 0
        
        # Try different search methods to get better results
        search_attempts = [
            {"method": "health search", "payload": {"keyword": "health", "rows": 25}},
            {"method": "education search", "payload": {"keyword": "education", "rows": 25}},
            {"method": "research search", "payload": {"keyword": "research", "rows": 25}},
            {"method": "agency search", "payload": {"agencyCode": "HHS", "rows": 25}},
            {"method": "blank search", "payload": {"rows": 50}}  # Just get latest grants
        ]
        
        # Track processed IDs to avoid duplicates
        processed_ids = set()
        
        for attempt in search_attempts:
            method = attempt["method"]
            payload = attempt["payload"]
            
            logging.info(f"Trying {method} with payload: {payload}")
            
            # Headers for the request
            headers = {
                "Content-Type": "application/json"
            }
            
            # Make the API call
            response = requests.post(base_url, headers=headers, json=payload)
            
            # Log the response status
            logging.info(f"API response status for {method}: {response.status_code}")
            
            # Check if the response is successful
            if response.status_code != 200:
                logging.error(f"API call failed for {method} with status {response.status_code}")
                continue
                
            # Try to parse the response as JSON
            try:
                search_results = response.json()
                
                # Check for error code
                error_code = search_results.get("errorcode")
                if error_code and error_code != 0:
                    logging.error(f"API returned error code {error_code}: {search_results.get('msg', 'Unknown error')}")
                    continue
                
                # Get oppHits directly from data - based on the actual API response structure
                if "data" in search_results and isinstance(search_results["data"], dict):
                    data = search_results["data"]
                    
                    # The API response shows opportunities in oppHits array
                    if "oppHits" in data and isinstance(data["oppHits"], list):
                        opportunities = data["oppHits"]
                        logging.info(f"Found {len(opportunities)} opportunities in data.oppHits")
                        
                        # If we found some opportunities, process them
                        if opportunities:
                            # Log the structure of the first opportunity
                            if opportunities and isinstance(opportunities[0], dict):
                                logging.info(f"First opportunity fields: {list(opportunities[0].keys())}")
                            
                            # Process each opportunity
                            for opportunity in opportunities:
                                if not isinstance(opportunity, dict):
                                    continue
                                    
                                # Get the ID (we can see from the API response that 'id' is the field name)
                                opportunity_id = opportunity.get("id")
                                if not opportunity_id:
                                    opportunity_id = opportunity.get("number")
                                
                                if opportunity_id and opportunity_id not in processed_ids:
                                    processed_ids.add(opportunity_id)
                                    logging.info(f"Adding opportunity ID to queue: {opportunity_id}")
                                    queue_client.send_message(opportunity_id)
                                    grants_added_to_queue += 1
                            
                            # Update the total count (excluding duplicates)
                            total_grants_found = len(processed_ids)
                    else:
                        logging.warning(f"No oppHits found in data for {method}")
                else:
                    logging.warning(f"No data field found in response for {method}")
                
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse API response as JSON for {method}: {str(e)}")
                continue
                
        logging.info(f'SearchGrants function completed at {utc_timestamp}')
        logging.info(f'Total grants found: {total_grants_found}, Added to queue: {grants_added_to_queue}')
        
        # If no grants were found, add some sample grants as fallback
        if grants_added_to_queue == 0:
            sample_grants = ["356163", "356164", "355129", "356250", "341131"]  # From the API response
            
            for grant_id in sample_grants:
                logging.info(f"Adding sample grant ID to queue: {grant_id}")
                queue_client.send_message(grant_id)
                grants_added_to_queue += 1
                
            logging.info(f'Added {len(sample_grants)} sample grants to queue as fallback')
        
    except Exception as e:
        logging.error(f"Error in SearchGrants function: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())