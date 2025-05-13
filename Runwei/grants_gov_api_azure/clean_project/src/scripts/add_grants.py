import os
import sys
import json
import requests
from azure.data.tables import TableServiceClient
from datetime import datetime

def main():
    # Get the connection string from environment variable or prompt the user
    connection_string = os.environ.get("STORAGE_CONNECTION")
    if not connection_string:
        print("Please set the STORAGE_CONNECTION environment variable first.")
        print("Example: export STORAGE_CONNECTION=\"your_connection_string_here\"")
        return
        
    # Initialize table client
    table_service = TableServiceClient.from_connection_string(connection_string)
    
    # Create table if it doesn't exist
    table_name = "GrantDetails"
    try:
        table_service.create_table(table_name)
        print(f"Table '{table_name}' created.")
    except Exception as e:
        print(f"Table likely exists already: {str(e)}")
    
    # Get table client
    table_client = table_service.get_table_client(table_name)
    
    # Use the search API to get some real grants
    search_url = "https://api.grants.gov/v1/api/search2"
    headers = {"Content-Type": "application/json"}
    
    # Try different search terms
    search_terms = ["health", "education", "research"]
    grants_added = 0
    
    for term in search_terms:
        print(f"Searching for grants with keyword: {term}")
        
        # Call the search API
        response = requests.post(
            search_url, 
            headers=headers, 
            json={"keyword": term, "rows": 10}
        )
        
        if response.status_code != 200:
            print(f"Error calling API: {response.status_code}")
            continue
            
        search_results = response.json()
        
        # Check for error code
        error_code = search_results.get("errorcode")
        if error_code and error_code != 0:
            print(f"API error: {search_results.get('msg', 'Unknown error')}")
            continue
            
        # Extract opportunities
        if "data" in search_results and "oppHits" in search_results["data"]:
            opportunities = search_results["data"]["oppHits"]
            print(f"Found {len(opportunities)} opportunities.")
            
            # Add each opportunity to the table
            for opportunity in opportunities:
                try:
                    # Extract fields
                    opp_id = opportunity.get("id", "")
                    
                    if not opp_id:
                        continue
                        
                    # Clean fields to ensure they are table-compatible
                    def clean_text(text):
                        if text is None:
                            return ""
                        if isinstance(text, str):
                            text = text.replace("\u0000", "")
                            if len(text) > 32000:
                                return text[:32000] + "... (truncated)"
                            return text
                        return str(text)
                    
                    # Get CFDA numbers
                    cfda_numbers = []
                    if "cfdaList" in opportunity and isinstance(opportunity["cfdaList"], list):
                        cfda_numbers = opportunity["cfdaList"]
                    cfda_string = ", ".join(str(cfda) for cfda in cfda_numbers)
                    
                    # Use the correct URL format
                    opportunity_url = f"https://www.grants.gov/search-results-detail/{opp_id}"
                    
                    # Prepare entity
                    entity = {
                        "PartitionKey": "Grant",
                        "RowKey": opp_id,
                        "Title": clean_text(opportunity.get("title", "")),
                        "AgencyName": clean_text(opportunity.get("agency", "")),
                        "AgencyCode": clean_text(opportunity.get("agencyCode", "")),
                        "OpenDate": clean_text(opportunity.get("openDate", "")),
                        "CloseDate": clean_text(opportunity.get("closeDate", "")),
                        "Status": clean_text(opportunity.get("oppStatus", "")),
                        "CFDANumbers": cfda_string,
                        "Number": clean_text(opportunity.get("number", "")),
                        "Description": "Retrieved from Grants.gov API",
                        "OpportunityURL": opportunity_url,
                        "LastUpdated": datetime.now().isoformat()
                    }
                    
                    # Add to table
                    table_client.upsert_entity(entity)
                    print(f"Added grant {opp_id}: {entity['Title']}")
                    print(f"  URL: {opportunity_url}")
                    grants_added += 1
                    
                except Exception as e:
                    print(f"Error adding grant {opportunity.get('id', 'unknown')}: {str(e)}")
    
    print(f"Total grants added: {grants_added}")
    
if __name__ == "__main__":
    main()
