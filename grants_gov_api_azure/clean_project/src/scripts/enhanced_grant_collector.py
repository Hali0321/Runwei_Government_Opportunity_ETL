import os
import requests
import json
import time
import random
import os
import requests
import json
from datetime import datetime, timedelta
from azure.data.tables import TableServiceClient, UpdateMode
import time
import logging
import sys
import random

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_text(text):
    """Clean text for storage in Azure Table"""
    if text is None:
        return ""
    return str(text).replace("\u0000", "").replace("\x00", "")

def generate_date_range_strategies():
    """Generate date-based search strategies with less overlap"""
    strategies = []
    
    # Generate non-overlapping date ranges covering the past few years
    # This will help get grants from different time periods
    end_date = datetime.now()
    
    # Create 30-day chunks going back 5 years
    for i in range(0, 60):  # 60 months = 5 years
        end = end_date - timedelta(days=i*30)
        start = end - timedelta(days=30)
        
        # Format dates in YYYY-MM-DD format for the API
        end_str = end.strftime("%Y-%m-%d")
        start_str = start.strftime("%Y-%m-%d")
        
        strategies.append({
            "startDate": start_str,
            "endDate": end_str,
            "rows": 1000
        })
    
    return strategies

def main():
    """Collect a large number of grants from Grants.gov API with enhanced strategies"""
    connection_string = os.environ.get("STORAGE_CONNECTION")
    
    if not connection_string:
        logger.error("Please set the STORAGE_CONNECTION environment variable first.")
        logger.error("Example: export STORAGE_CONNECTION=\"your_connection_string_here\"")
        return
        
    logger.info("Starting enhanced grant collection process")
    
    # Initialize table client
    try:
        logger.info("Connecting to Azure Storage...")
        table_service = TableServiceClient.from_connection_string(connection_string)
        
        # Create table if it doesn't exist
        table_name = "GrantDetails"
        try:
            table_service.create_table(table_name)
            logger.info(f"Table '{table_name}' created.")
        except Exception as e:
            logger.info(f"Table likely exists already: {str(e)}")
        
        # Get table client
        table_client = table_service.get_table_client(table_name)
        logger.info("Connected to Azure Table Storage successfully")
    except Exception as e:
        logger.error(f"Failed to connect to Azure Storage: {str(e)}")
        return
    
    # Configuration for grants collection
    search_url = "https://api.grants.gov/v1/api/search2"
    headers = {"Content-Type": "application/json"}
    
    # Create multiple search strategies with LESS OVERLAP to get as many unique grants as possible
    date_range_strategies = generate_date_range_strategies()
    
    search_strategies = [
        # Use very specific keyword combinations instead of single keywords
        {"keyword": "health research covid", "rows": 1000},
        {"keyword": "education technology", "rows": 1000},
        {"keyword": "cancer treatment", "rows": 1000},
        {"keyword": "renewable energy", "rows": 1000},
        {"keyword": "climate change", "rows": 1000},
        {"keyword": "artificial intelligence", "rows": 1000},
        {"keyword": "mental health services", "rows": 1000},
        {"keyword": "agriculture sustainability", "rows": 1000},
        {"keyword": "community development", "rows": 1000},
        {"keyword": "tribal assistance", "rows": 1000},
        {"keyword": "vaccine development", "rows": 1000},
        {"keyword": "quantum computing", "rows": 1000},
        {"keyword": "biotechnology innovation", "rows": 1000},
        {"keyword": "cybersecurity research", "rows": 1000},
        {"keyword": "environmental protection", "rows": 1000},
        
        # Target very specific agency sub-departments (more granular than before)
        {"agencyCode": "HHS-ACF", "rows": 1000},  # Admin for Children & Families
        {"agencyCode": "HHS-NIH-NCI", "rows": 1000},  # National Cancer Institute
        {"agencyCode": "DOD-ARMY", "rows": 1000},  # Army
        {"agencyCode": "DOT-FAA", "rows": 1000},  # Federal Aviation Admin
        {"agencyCode": "USDA-NIFA", "rows": 1000},  # National Inst of Food and Agriculture
        {"agencyCode": "DOE-EERE", "rows": 1000},  # Energy Efficiency & Renewable Energy
        {"agencyCode": "EPA-OGD", "rows": 1000},  # Office of Grants & Debarment
        {"agencyCode": "HHS-HRSA", "rows": 1000},  # Health Resources & Services Admin
        {"agencyCode": "HHS-NIH-NIAID", "rows": 1000},  # National Institute of Allergy and Infectious Diseases
        {"agencyCode": "DOC-NIST", "rows": 1000},  # National Institute of Standards and Technology
        
        # Use combinations of parameters to get more specific results
        {"keyword": "research", "agencyCode": "HHS-NIH", "rows": 1000},
        {"keyword": "education", "fundingCategories": "ED", "rows": 1000},
        {"keyword": "infrastructure", "fundingCategories": "ISS", "rows": 1000},
        {"keyword": "healthcare", "fundingCategories": "HL", "rows": 1000},
        {"keyword": "agriculture", "fundingCategories": "AG", "rows": 1000}
    ]
    
    # Combine strategies
    all_strategies = search_strategies + date_range_strategies
    
    # Shuffle strategies to improve unique grant discovery
    random.shuffle(all_strategies)
    
    # For the first run, we'll limit to 50 strategies to avoid taking too long
    # You can increase this number for more comprehensive collection
    active_strategies = all_strategies[:50]
    
    # Use paging to get even more results for each strategy
    pages_per_strategy = 5  # Reduce pages to focus on more strategies
    
    # Track stats
    total_grants_found = 0
    total_grants_added = 0
    processed_ids = set()
    
    # Get existing grant IDs to avoid duplicates
    try:
        logger.info("Checking for existing grants in the table...")
        entities = table_client.query_entities("PartitionKey eq 'Grant'")
        for entity in entities:
            processed_ids.add(entity.get("RowKey", ""))
        logger.info(f"Found {len(processed_ids)} existing grants in the table")
    except Exception as e:
        logger.error(f"Error querying existing grants: {str(e)}")
    
    start_time = time.time()
    
    # Process each search strategy
    for strategy_index, search_payload in enumerate(active_strategies):
        logger.info(f"Strategy {strategy_index+1}/{len(active_strategies)}: {search_payload}")
        
        # Use paging to get multiple pages of results
        for page in range(1, pages_per_strategy + 1):
            # Copy the search payload and add the start parameter for paging
            paged_payload = search_payload.copy()
            paged_payload["offset"] = (page - 1) * search_payload.get("rows", 1000)
            
            logger.info(f"  Page {page}/{pages_per_strategy}: offset={paged_payload['offset']}")
            
            try:
                # Add a delay to avoid rate limiting
                time.sleep(0.2)
                
                # Make the API call
                response = requests.post(search_url, headers=headers, json=paged_payload)
                
                # Check status code
                if response.status_code != 200:
                    logger.warning(f"API returned status code {response.status_code}")
                    continue
                
                # Parse the response
                try:
                    search_results = response.json()
                    
                    # Check for error code
                    if search_results.get("errorcode", 0) != 0:
                        logger.warning(f"API returned error: {search_results.get('msg', 'Unknown error')}")
                        continue
                    
                    # Process the data
                    if "data" in search_results and "oppHits" in search_results["data"]:
                        opportunities = search_results["data"]["oppHits"]
                        logger.info(f"  Found {len(opportunities)} opportunities on this page")
                        
                        # Track total found (including duplicates across strategies)
                        total_grants_found += len(opportunities)
                        
                        # Process each grant opportunity
                        for opportunity in opportunities:
                            try:
                                # Get the grant ID
                                opportunity_id = opportunity.get("id")
                                if not opportunity_id:
                                    continue
                                    
                                # Skip if we've already processed this ID (avoid duplicates)
                                if opportunity_id in processed_ids:
                                    continue
                                
                                # Add to processed set
                                processed_ids.add(opportunity_id)
                                
                                # Create entity for Azure Table Storage
                                entity = {
                                    "PartitionKey": "Grant",
                                    "RowKey": opportunity_id,
                                    "Title": clean_text(opportunity.get("title", "")),
                                    "Number": clean_text(opportunity.get("number", "")),
                                    "AgencyCode": clean_text(opportunity.get("agencyCode", "")),
                                    "AgencyName": clean_text(opportunity.get("agency", "")),
                                    "Category": clean_text(opportunity.get("fundingCategory", "")),
                                    "CFDANumbers": clean_text(opportunity.get("cfda", "")),
                                    "Description": clean_text(opportunity.get("description", "Retrieved from Grants.gov API")),
                                    "CloseDate": clean_text(opportunity.get("closeDate", "")),
                                    "OpenDate": clean_text(opportunity.get("openDate", "")),
                                    "AwardFloor": clean_text(opportunity.get("awardFloor", "")),
                                    "AwardCeiling": clean_text(opportunity.get("awardCeiling", "")),
                                    "ExpectedAwards": clean_text(opportunity.get("expectedNumOfAwards", "")),
                                    "DocType": clean_text(opportunity.get("docType", "")),
                                    "FundingType": clean_text(opportunity.get("fundingInstrument", "")),
                                    "LastUpdated": datetime.now().isoformat(),
                                    "OpportunityURL": f"https://www.grants.gov/search-results-detail/{opportunity_id}"
                                }
                                
                                # Add to table
                                table_client.upsert_entity(entity, mode=UpdateMode.MERGE)
                                
                                # Update counter
                                total_grants_added += 1
                                
                                # Provide periodic updates for large collections
                                if total_grants_added % 100 == 0:
                                    elapsed = time.time() - start_time
                                    logger.info(f"Progress: Added {total_grants_added} grants ({total_grants_added/elapsed:.2f} grants/sec)")
                                    
                            except Exception as e:
                                logger.error(f"Error processing grant {opportunity_id}: {str(e)}")
                                continue
                    else:
                        logger.warning("  No opportunities found in the response")
                
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse API response: {str(e)}")
                    continue
            
            except Exception as e:
                logger.error(f"Error making API request: {str(e)}")
                continue
            
            # Check if we've already found a significant number of new grants
            if total_grants_added > 5000:
                logger.info(f"Collected over 5000 new grants - consider this a success!")
    
    # Calculate time elapsed
    elapsed_time = time.time() - start_time
    
    # Print summary
    logger.info("\n\n=== COLLECTION SUMMARY ===")
    logger.info(f"Total grants found (including duplicates): {total_grants_found}")
    logger.info(f"Unique grants processed: {len(processed_ids) - (len(processed_ids) - total_grants_added)}")
    logger.info(f"New grants added to Azure Table: {total_grants_added}")
    logger.info(f"Time elapsed: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    logger.info("Collection complete")
    
    # Provide next steps
    logger.info("\nNext Steps:")
    logger.info("1. View your grants at: https://grantsgovfunc60542.azurewebsites.net/api/grantsviewer?format=html&limit=1000")
    logger.info("2. To get even more grants, run this script again with different parameters")
    
if __name__ == "__main__":
    main()
