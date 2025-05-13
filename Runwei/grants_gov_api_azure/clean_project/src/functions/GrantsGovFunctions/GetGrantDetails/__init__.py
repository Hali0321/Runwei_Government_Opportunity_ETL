import logging
import azure.functions as func
import requests
import json
import os
from azure.data.tables import TableServiceClient
from datetime import datetime

def main(msg: func.QueueMessage) -> None:
    logging.info('GetGrantDetails function triggered')
    
    try:
        # Get message content (opportunity ID)
        opportunity_id = msg.get_body().decode('utf-8').strip()
        logging.info(f'Processing opportunity ID: {opportunity_id}')
        
        # Use the fetchOpportunity endpoint from the API guide
        api_url = f"https://api.grants.gov/v1/api/fetchOpportunity/{opportunity_id}"
        
        # Call Grants.gov API
        logging.info(f'Calling API: {api_url}')
        headers = {
            "Content-Type": "application/json"
        }
        
        response = requests.get(api_url, headers=headers, timeout=30)
        
        if response.status_code != 200:
            logging.error(f'API call failed with status {response.status_code}: {response.text[:200]}')
            
            # For troubleshooting - try the legacy endpoint
            legacy_url = f"https://www.grants.gov/grantsws/rest/opportunity/{opportunity_id}"
            logging.info(f'Trying legacy API: {legacy_url}')
            legacy_response = requests.get(legacy_url, timeout=30)
            
            if legacy_response.status_code != 200:
                logging.error(f'Legacy API call also failed with status {legacy_response.status_code}')
                return
            else:
                response = legacy_response
                logging.info('Legacy API call succeeded')
        
        # Parse response JSON
        grant_data = response.json()
        logging.info(f'Successfully retrieved data for grant: {opportunity_id}')
        
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
        
        # Log the structure of the response to understand the data format
        if isinstance(grant_data, dict):
            logging.info(f"Response keys: {list(grant_data.keys())}")
            if "opportunity" in grant_data:
                logging.info(f"Opportunity keys: {list(grant_data['opportunity'].keys())}")
        
        # Extract key fields with proper error handling
        try:
            title = ""
            agency = ""
            description = ""
            close_date = ""
            post_date = ""
            agency_code = ""
            award_ceiling = ""
            award_floor = ""
            category = ""
            
            # Handle different response formats based on the API
            if isinstance(grant_data, dict):
                # New API format with nested structure
                if "opportunity" in grant_data:
                    opp = grant_data["opportunity"]
                    title = clean_text(opp.get("title", ""))
                    agency = clean_text(opp.get("agency", ""))
                    description = clean_text(opp.get("description", ""))
                    close_date = clean_text(opp.get("closeDate", ""))
                    post_date = clean_text(opp.get("postDate", ""))
                    agency_code = clean_text(opp.get("agencyCode", ""))
                    award_ceiling = clean_text(opp.get("awardCeiling", ""))
                    award_floor = clean_text(opp.get("awardFloor", ""))
                    category = clean_text(opp.get("category", ""))
                # Direct opportunity response (legacy API)
                else:
                    title = clean_text(grant_data.get("opportunityTitle", ""))
                    agency = clean_text(grant_data.get("agencyName", ""))
                    description = clean_text(grant_data.get("description", ""))
                    close_date = clean_text(grant_data.get("closeDate", ""))
                    post_date = clean_text(grant_data.get("postDate", ""))
                    agency_code = clean_text(grant_data.get("agencyCode", ""))
                    award_ceiling = clean_text(grant_data.get("awardCeiling", ""))
                    award_floor = clean_text(grant_data.get("awardFloor", ""))
                    category = clean_text(grant_data.get("categoryOfFunding", ""))
            
        except Exception as extract_error:
            logging.error(f'Error extracting fields: {str(extract_error)}')
            import traceback
            logging.error(traceback.format_exc())
            # Use default values if extraction fails
            title = "Error extracting title"
            agency = "Error extracting agency"
            description = "Error extracting description"
        
        # Prepare entity
        entity = {
            "PartitionKey": "Grant",
            "RowKey": opportunity_id,
            "Title": title,
            "AgencyName": agency,
            "Description": description[:32000] if len(description) > 32000 else description,
            "CloseDate": close_date,
            "PostDate": post_date,
            "AgencyCode": agency_code,
            "AwardCeiling": award_ceiling,
            "AwardFloor": award_floor,
            "Category": category,
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
