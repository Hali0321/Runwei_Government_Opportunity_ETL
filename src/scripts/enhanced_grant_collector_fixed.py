#!/usr/bin/env python3
import os
import requests
import json
import time
import random
import re
from datetime import datetime, timedelta
from azure.data.tables import TableServiceClient, UpdateMode
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_text(text):
    """Clean text for storage in Azure Table"""
    if text is None:
        return ""
    return str(text).replace("\u0000", "").replace("\x00", "")

def get_grant_details(grant_id):
    """Fetch detailed information about a specific grant"""
    details_url = f"https://api.grants.gov/v1/api/search2/detail/{grant_id}"
    headers = {"Content-Type": "application/json"}
    
    try:
        # Add delay to avoid rate limiting
        time.sleep(0.5)
        
        response = requests.get(details_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Check for error code
            if data.get("errorcode", 0) != 0:
                logger.warning(f"API returned error: {data.get('msg', 'Unknown error')}")
                return None
                
            # Extract opportunity details
            if "data" in data and "opportunity" in data["data"]:
                return data["data"]["opportunity"]
        else:
            logger.warning(f"API returned status code {response.status_code} for grant {grant_id}")
            
    except Exception as e:
        logger.error(f"Error fetching grant details for {grant_id}: {str(e)}")
        
    return None

def get_connection_string():
    """Get Azure Storage connection string"""
    connection_string = os.environ.get("STORAGE_CONNECTION")
    
    if not connection_string:
        # Try to get it from Azure CLI
        import subprocess
        try:
            result = subprocess.run([
                'az', 'storage', 'account', 'show-connection-string',
                '--name', 'grantsgov58225',
                '--resource-group', 'GrantsGovAPI',
                '--query', 'connectionString',
                '-o', 'tsv'
            ], capture_output=True, text=True, check=False)
            
            if result.returncode == 0 and result.stdout.strip():
                connection_string = result.stdout.strip()
            else:
                logger.error("Failed to get connection string from Azure CLI")
                return None
        except Exception as e:
            logger.error(f"Error getting connection string: {str(e)}")
            return None
    
    return connection_string

def main():
    """Collect a large number of grants from Grants.gov API with enhanced strategies"""
    connection_string = get_connection_string()
    
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
    
    # Create search strategies to get different types of grants
    search_strategies = [
        {"keyword": "health research", "rows": 100},
        {"keyword": "education technology", "rows": 100},
        {"keyword": "renewable energy", "rows": 100},
        {"agencyCode": "HHS", "rows": 100},
        {"agencyCode": "NSF", "rows": 100},
        {"agencyCode": "DOE", "rows": 100},
        {"sortBy": "closeDate|desc", "rows": 100},
        {"sortBy": "openDate|desc", "rows": 100}
    ]
    
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
    for strategy_index, search_payload in enumerate(search_strategies):
        logger.info(f"Strategy {strategy_index+1}/{len(search_strategies)}: {search_payload}")
        
        try:
            # Add a delay to avoid rate limiting
            time.sleep(1)
            
            # Make the API call
            response = requests.post(search_url, headers=headers, json=search_payload)
            
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
                    logger.info(f"Found {len(opportunities)} opportunities")
                    
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

                            # Get detailed information for this grant
                            detailed_info = get_grant_details(opportunity_id)
                            if detailed_info:
                                # Merge the detailed info with the basic info we already have
                                for key, value in detailed_info.items():
                                    opportunity[key] = value
                            
                            # Create entity for Azure Table Storage
                            entity = {
                                "PartitionKey": "Grant",
                                "RowKey": opportunity_id,
                                "Title": clean_text(opportunity.get("title", "")),
                                "Number": clean_text(opportunity.get("number", "")),
                                "AgencyCode": clean_text(opportunity.get("agencyCode", "")),
                                "AgencyName": clean_text(opportunity.get("agency", "")),
                                "Category": clean_text(opportunity.get("fundingCategory", "")),
                                "CategoryExplanation": clean_text(opportunity.get("fundingCategoryExplanation", "")),
                                "OpportunityCategory": clean_text(opportunity.get("opportunityCategory", "")), 
                                "OpportunityCategoryExplanation": clean_text(opportunity.get("opportunityCategoryExplanation", "")),
                                "CFDANumbers": clean_text(opportunity.get("cfda", "")),
                                "AssistanceListings": clean_text(opportunity.get("cfda", "")),
                                "Description": clean_text(opportunity.get("description", "")),
                                "CloseDate": clean_text(opportunity.get("closeDate", "")),
                                "OpenDate": clean_text(opportunity.get("openDate", "")),
                                "OriginalCloseDate": clean_text(opportunity.get("originalCloseDate", "")),
                                "ArchiveDate": clean_text(opportunity.get("archiveDate", "")),
                                "AwardFloor": clean_text(opportunity.get("awardFloor", "")),
                                "AwardCeiling": clean_text(opportunity.get("awardCeiling", "")),
                                "EstimatedTotalProgramFunding": clean_text(opportunity.get("estimatedTotalProgramFunding", "")),
                                "ExpectedAwards": clean_text(opportunity.get("expectedNumOfAwards", "")),
                                "ExpectedNumberofAwards": clean_text(opportunity.get("expectedNumOfAwards", "")),
                                "DocType": clean_text(opportunity.get("docType", "")),
                                "FundingType": clean_text(opportunity.get("fundingInstrument", "")),
                                "CostSharing": clean_text(opportunity.get("costSharing", "No")),
                                "Version": clean_text(opportunity.get("version", "")),
                                "LastUpdated": datetime.now().isoformat(),
                                "EligibleApplicants": clean_text(opportunity.get("eligibleApplicants", "")),
                                "AdditionalEligibilityInfo": clean_text(opportunity.get("additionalEligibilityInfo", "")),
                                "AdditionalInfoLink": clean_text(opportunity.get("additionalInfoUrl", "")),
                                "GrantorContact": clean_text(opportunity.get("grantorContact", "")),
                                "OpportunityURL": f"https://www.grants.gov/search-results-detail/{opportunity_id}",
                                "DataTypesFixed": True
                            }
                            
                            # Convert numeric fields to proper types
                            numeric_fields = {
                                "AwardCeiling": float,
                                "AwardFloor": float,
                                "EstimatedTotalProgramFunding": float,
                                "ExpectedNumberofAwards": int,
                                "ExpectedAwards": int
                            }

                            # Process numeric fields
                            for field, converter in numeric_fields.items():
                                if field in entity and entity[field]:
                                    try:
                                        value = entity[field]
                                        if isinstance(value, str):
                                            value = value.replace('$', '').replace(',', '')
                                            value = re.sub(r'[^0-9\.\-]', '', value)
                                            if value:
                                                entity[field] = converter(value)
                                            else:
                                                entity[field] = 0
                                    except (ValueError, TypeError):
                                        entity[field] = 0
                            
                            # Add to table (just once)
                            table_client.upsert_entity(entity, mode=UpdateMode.MERGE)
                            
                            # Update counter
                            total_grants_added += 1
                            
                            # Provide periodic updates for large collections
                            if total_grants_added % 20 == 0:
                                elapsed = time.time() - start_time
                                logger.info(f"Progress: Added {total_grants_added} grants ({total_grants_added/elapsed:.2f} grants/sec)")
                                
                        except Exception as e:
                            logger.error(f"Error processing opportunity {opportunity.get('id', 'unknown')}: {str(e)}")
                else:
                    logger.warning("No opportunities found in the response")
            
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse API response: {str(e)}")
        
        except Exception as e:
            logger.error(f"Error making API request: {str(e)}")
    
    # Calculate time elapsed
    elapsed_time = time.time() - start_time
    
    # Print summary
    logger.info("\n\n=== COLLECTION SUMMARY ===")
    logger.info(f"Total grants found (including duplicates): {total_grants_found}")
    logger.info(f"Unique grants processed: {len(processed_ids)}")
    logger.info(f"New grants added to Azure Table: {total_grants_added}")
    logger.info(f"Time elapsed: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    logger.info("Collection complete")
    
    # Provide next steps
    logger.info("\nNext Steps:")
    logger.info("1. View your grants at: https://grantsgovfunc60542.azurewebsites.net/api/grantsviewer?format=html&limit=1000")
    logger.info("2. To get even more grants, run this script again")
    
if __name__ == "__main__":
    main()
