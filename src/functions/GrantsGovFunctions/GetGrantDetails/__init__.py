import logging
import azure.functions as func
import requests
import json
import os
from azure.data.tables import TableServiceClient, UpdateMode
from datetime import datetime
import re

def main(msg: func.QueueMessage) -> None:
    logging.info('GetGrantDetails function triggered - UPDATED VERSION V4 (Enhanced Field Extraction)')
    
    try:
        # Get message content (opportunity ID)
        opportunity_id = msg.get_body().decode('utf-8').strip()
        logging.info(f'Processing opportunity ID: {opportunity_id}')
        
        # Use search API to get details for a specific opportunity ID
        search_url = "https://api.grants.gov/v1/api/search2"
        detail_url = f"https://api.grants.gov/v1/api/fetchOpportunity/{opportunity_id}"
        
        headers = {
            "Content-Type": "application/json"
        }
        
        # Try to get detailed info first using the fetchOpportunity endpoint
        logging.info(f'Fetching detailed information for grant: {opportunity_id}')
        detail_response = requests.get(detail_url, headers=headers)
        
        grant_data = None
        fetch_success = False
        
        # Process the detailed response if successful
        if detail_response.status_code == 200:
            try:
                detail_result = detail_response.json()
                if detail_result.get("errorcode", 1) == 0 and "data" in detail_result:
                    # Detail API response structure is different from search
                    grant_data = detail_result.get("data", {})
                    fetch_success = True
                    logging.info(f"Successfully fetched detailed data for grant {opportunity_id}")
            except Exception as detail_error:
                logging.error(f"Error processing detail response: {str(detail_error)}")
        
        # If detail API failed, fall back to search API
        if not fetch_success:
            logging.info(f'Detail API failed, falling back to search API for grant: {opportunity_id}')
            
            # Construct a search payload using the opportunity ID
            payload = {
                "oppNum": opportunity_id,  # Try using the ID as the opportunity number
                "rows": 1                  # We only need one result
            }
            
            response = requests.post(search_url, headers=headers, json=payload)
            
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
        
        # If we still don't have grant data, use a fallback with the ID
        if not grant_data:
            logging.warning(f"Creating fallback grant data for {opportunity_id}")
            grant_data = {
                "id": opportunity_id,
                "title": f"Grant {opportunity_id}",
                "agency": "Unknown",
                "description": "Details not available"
            }
        
        # Log what fields we have available for debugging
        logging.info(f"Grant data fields: {list(grant_data.keys())}")
        
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
        
        # Helper functions to clean and convert data
        def clean_text(text, max_len=32000):
            """Clean text for storage in Azure Table"""
            if text is None:
                return ""
            if isinstance(text, str):
                text = text.replace("\u0000", "")  # Remove null characters
                if len(text) > max_len:
                    return text[:max_len] + "... (truncated)"
                return text
            return str(text)
        
        def safe_int(value, default=0):
            """Convert value to integer safely"""
            if value is None:
                return default
            try:
                # Remove any non-numeric characters except negative sign
                if isinstance(value, str):
                    value = re.sub(r'[^0-9\-]', '', value)
                    if value == '' or value == '-':
                        return default
                return int(value)
            except (ValueError, TypeError):
                return default
        
        def safe_float(value, default=0.0):
            """Convert value to float safely"""
            if value is None:
                return default
            try:
                # Remove any non-numeric characters except decimal point and negative sign
                if isinstance(value, str):
                    value = re.sub(r'[^0-9\.\-]', '', value)
                    if value == '' or value == '.' or value == '-' or value == '-.':
                        return default
                return float(value)
            except (ValueError, TypeError):
                return default
        
        def format_date(date_str):
            """Format date string for better display"""
            if not date_str:
                return ""
            # Keep the original string format as it's easier to work with in Azure Storage
            return clean_text(date_str)
        
        # Extract fields with proper type conversion
        try:
            # Basic information (all as strings)
            title = clean_text(grant_data.get("title", ""))
            agency_name = clean_text(grant_data.get("agency", ""))
            agency_code = clean_text(grant_data.get("agencyCode", ""))
            description = clean_text(grant_data.get("description", ""))
            number = clean_text(grant_data.get("number", ""))
            status = clean_text(grant_data.get("status", grant_data.get("oppStatus", "")))
            doc_type = clean_text(grant_data.get("docType", ""))
            
            # Date fields (formatted as strings for Azure Table compatibility)
            open_date = format_date(grant_data.get("openDate", ""))
            close_date = format_date(grant_data.get("closeDate", ""))
            
            # Money fields (as integers or floats)
            award_floor = safe_float(grant_data.get("awardFloor", "0"))
            award_ceiling = safe_float(grant_data.get("awardCeiling", "0"))
            expected_awards = safe_int(grant_data.get("expectedNumOfAwards", "0"))
            
            # Category and funding information
            category = clean_text(grant_data.get("fundingCategory", ""))
            funding_type = clean_text(grant_data.get("fundingInstrument", ""))
            
            # CFDA numbers handling
            cfda_numbers = []
            cfda_string = clean_text(grant_data.get("cfda", ""))
            
            # Try different ways to extract CFDA numbers
            if "cfdaList" in grant_data and isinstance(grant_data["cfdaList"], list):
                cfda_numbers = grant_data["cfdaList"]
                cfda_string = ", ".join(str(cfda) for cfda in cfda_numbers)
            elif not cfda_string and "cfdaNumber" in grant_data:
                cfda_string = clean_text(grant_data.get("cfdaNumber", ""))
                
            # URL for the opportunity
            opportunity_url = f"https://www.grants.gov/search-results-detail/{opportunity_id}"
            
        except Exception as extract_error:
            logging.error(f'Error extracting fields: {str(extract_error)}')
            import traceback
            logging.error(traceback.format_exc())
            
            # Use default values if extraction fails
            title = f"Grant {opportunity_id}"
            agency_name = "Unknown"
            agency_code = ""
            description = "Error extracting details"
            open_date = ""
            close_date = ""
            status = ""
            award_floor = 0.0
            award_ceiling = 0.0
            expected_awards = 0
            category = ""
            funding_type = ""
            cfda_string = ""
            doc_type = ""
            number = opportunity_id
            opportunity_url = f"https://www.grants.gov/search-results-detail/{opportunity_id}"
        
        # Prepare entity with proper typing for Azure Table Storage
        # Note: Azure Table Storage supports string, bool, int, float, datetime, and bytes
        entity = {
            "PartitionKey": "Grant",
            "RowKey": opportunity_id,
            "Title": title,
            "AgencyName": agency_name,
            "AgencyCode": agency_code,
            "Description": description,
            "Number": number,
            "Status": status,
            "DocType": doc_type,
            
            # Date fields as strings (easier to work with in Azure Tables)
            "OpenDate": open_date,
            "CloseDate": close_date,
            
            # Money fields as floats
            "AwardFloor": award_floor,
            "AwardCeiling": award_ceiling,
            
            # Count as integer
            "ExpectedAwards": expected_awards,
            
            # Other fields as strings
            "Category": category,
            "FundingType": funding_type,
            "CFDANumbers": cfda_string,
            "OpportunityURL": opportunity_url,
            
            # Timestamp
            "LastUpdated": datetime.now().isoformat()
        }
        
        # Add to table with update mode set to merge to preserve any fields we don't set
        table_client.upsert_entity(entity, mode=UpdateMode.MERGE)
        logging.info(f"Successfully saved grant details for {opportunity_id}")
        
        # Log the actual saved entity for debugging
        logging.info(f"Saved entity with fields: {list(entity.keys())}")
        
    except Exception as e:
        logging.error(f'Error in GetGrantDetails: {str(e)}')
        import traceback
        logging.error(traceback.format_exc())