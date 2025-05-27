#!/usr/bin/env python3
import os
import requests
import json
import time
import re
import logging
import subprocess
import sys
from datetime import datetime, timedelta
from azure.data.tables import TableServiceClient, UpdateMode
import traceback
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"grant_collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# All field mappings from grants.gov to our Azure table
FIELD_MAPPINGS = {
    "Document Type": "DocType",
    "Funding Opportunity Number": "Number",
    "Funding Opportunity Title": "Title",
    "Opportunity Category": "OpportunityCategory",
    "Opportunity Category Explanation": "OpportunityCategoryExplanation",
    "Funding Instrument Type": "FundingType",
    "Category of Funding Activity": "Category",
    "Category Explanation": "CategoryExplanation",
    "Expected Number of Awards": "ExpectedNumberofAwards",
    "Assistance Listings": "AssistanceListings",
    "Cost Sharing or Matching Requirement": "CostSharing",
    "Version": "Version",
    "Posted Date": "OpenDate",
    "Last Updated Date": "LastUpdated",
    "Original Closing Date for Applications": "OriginalCloseDate",
    "Current Closing Date for Applications": "CloseDate",
    "Archive Date": "ArchiveDate",
    "Estimated Total Program Funding": "EstimatedTotalProgramFunding",
    "Award Ceiling": "AwardCeiling",
    "Award Floor": "AwardFloor",
    "Eligible Applicants": "EligibleApplicants",
    "Additional Information on Eligibility": "AdditionalEligibilityInfo",
    "Agency Name": "AgencyName",
    "Description": "Description",
    "Link to Additional Information": "AdditionalInfoLink",
    "Grantor Contact Information": "GrantorContact"
}

def ensure_azure_connection():
    """Make sure we're connected to Azure, attempt to login if needed"""
    try:
        # Try to get account information
        result = subprocess.run(['az', 'account', 'show'], 
                               capture_output=True, text=True, check=False)
        
        if result.returncode != 0:
            logger.warning("Not logged in to Azure. Attempting login...")
            
            # Try interactive login
            login_result = subprocess.run(['az', 'login'], 
                                         capture_output=True, text=True, check=False)
            
            if login_result.returncode != 0:
                logger.error(f"Failed to login to Azure: {login_result.stderr}")
                return False
            else:
                logger.info("Successfully logged in to Azure")
        else:
            account_info = json.loads(result.stdout)
            logger.info(f"Connected to Azure as: {account_info.get('user', {}).get('name', 'Unknown')}")
        
        return True
    except Exception as e:
        logger.error(f"Error checking Azure connection: {str(e)}")
        return False

def get_storage_connection_string():
    """Get the storage connection string from Azure CLI or environment"""
    # First try environment variable
    connection_string = os.environ.get("STORAGE_CONNECTION")
    if connection_string:
        return connection_string
    
    # Try to get it from Azure CLI
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
            # Save it to environment for future use
            os.environ["STORAGE_CONNECTION"] = connection_string
            logger.info("Retrieved connection string from Azure CLI")
            return connection_string
        else:
            logger.error(f"Failed to get connection string: {result.stderr}")
            return None
    except Exception as e:
        logger.error(f"Error getting connection string: {str(e)}")
        return None

def clean_text(text):
    """Clean text for storage in Azure Table"""
    if text is None:
        return ""
    return str(text).replace("\u0000", "").replace("\x00", "")

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
        return int(float(value))
    except (ValueError, TypeError):
        return default

def safe_float(value, default=0.0):
    """Convert value to float safely"""
    if value is None:
        return default
    try:
        # Remove any non-numeric characters except decimal point and negative sign
        if isinstance(value, str):
            # Handle dollar sign and commas
            value = value.replace('$', '').replace(',', '')
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
    
    # Try to identify the date format
    date_formats = [
        '%b %d, %Y',  # "Apr 13, 2023"
        '%B %d, %Y',  # "April 13, 2023"
        '%m/%d/%Y',   # "04/13/2023"
        '%Y-%m-%d',   # "2023-04-13"
    ]
    
    for fmt in date_formats:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
            
    return clean_text(date_str)

def get_grant_ids(strategy="recent", limit=1000):
    """Get grant IDs using various strategies"""
    grant_ids = set()
    
    # Base URL for Grants.gov API
    search_url = "https://api.grants.gov/v1/api/search2"
    headers = {"Content-Type": "application/json"}
    
    if strategy == "recent":
        # Get most recent grants first
        payload = {
            "startRecordNum": 0,
            "rows": min(limit, 1000),
            "sortBy": "closeDate|desc",
            "oppStatuses": "posted"
        }
    elif strategy == "all":
        # Get all grants (forecasted, posted, closed)
        payload = {
            "startRecordNum": 0,
            "rows": min(limit, 1000),
            "sortBy": "openDate|desc",
            "oppStatuses": "forecasted|posted|closed"
        }
    elif strategy == "closing_soon":
        # Get grants closing in the next 30 days
        today = datetime.now().strftime("%Y-%m-%d")
        thirty_days = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        payload = {
            "startRecordNum": 0,
            "rows": min(limit, 1000),
            "sortBy": "closeDate|asc",
            "dateRange": {
                "startDate": today,
                "endDate": thirty_days
            }
        }
    
    try:
        # Add delay to avoid rate limiting
        time.sleep(0.5)
        
        response = requests.post(search_url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Check for error code
            if data.get("errorcode", 0) != 0:
                logger.warning(f"API returned error: {data.get('msg', 'Unknown error')}")
                return grant_ids
                
            # Extract grant IDs
            if "data" in data and "oppHits" in data["data"]:
                opportunities = data["data"]["oppHits"]
                logger.info(f"Found {len(opportunities)} opportunities using {strategy} strategy")
                
                for opp in opportunities:
                    opportunity_id = opp.get("id")
                    if opportunity_id:
                        grant_ids.add(opportunity_id)
        else:
            logger.warning(f"API returned status code {response.status_code}")
    except Exception as e:
        logger.error(f"Error getting grant IDs: {str(e)}")
    
    return grant_ids

def scrape_grant_details(opportunity_id):
    """Scrape complete grant details from grants.gov website"""
    logger.debug(f"Scraping grant {opportunity_id} from Grants.gov website")
    
    url = f"https://www.grants.gov/search-results-detail/{opportunity_id}"
    
    try:
        # Add delay to avoid rate limiting
        time.sleep(0.5)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.grants.gov/',
            'Connection': 'keep-alive'
        }
        response = requests.get(url, headers=headers, timeout=30)
        
        # Debug output to see the response
        logger.debug(f"Response status: {response.status_code}")
        if response.status_code != 200:
            logger.warning(f"Failed to fetch grant {opportunity_id}: HTTP {response.status_code}")
            return None
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Save HTML for debugging if needed
        with open(f"grant_{opportunity_id}_debug.html", "w", encoding="utf-8") as f:
            f.write(response.text)
        
        # Initialize grant data with the ID and URL
        grant_data = {
            "PartitionKey": "Grant",
            "RowKey": opportunity_id,
            "OpportunityURL": url,
            "DataTypesFixed": True,
            "LastUpdated": datetime.now().isoformat()
        }
        
        # Locate the general information section - try different selectors
        general_info = None
        for selector in ['div.synopsis-section', 'div.section', 'div[data-testid="general-info"]']:
            general_info = soup.select_one(selector)
            if general_info:
                logger.debug(f"Found general info section using selector: {selector}")
                break
                
        if not general_info:
            # Try a more general approach - look for sections with tables
            sections = soup.find_all('div', class_=lambda c: c and ('section' in c.lower() or 'info' in c.lower()))
            for section in sections:
                if section.find('table'):
                    general_info = section
                    logger.debug("Found general info section by table detection")
                    break
        
        if not general_info:
            logger.warning(f"Could not find general information section for grant {opportunity_id}")
            
            # Try a different approach - get all tables on the page
            tables = soup.find_all('table')
            if tables:
                general_info = tables[0].parent
                logger.debug("Found general info by locating first table")
        
        # If we found the general info section, extract data from it
        if general_info:
            # Look for tables
            tables = general_info.find_all('table')
            if not tables:
                tables = soup.find_all('table')
                
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    header = row.find('th')
                    value = row.find('td')
                    
                    if header and value:
                        header_text = header.get_text().strip()
                        value_text = value.get_text().strip()
                        
                        # Map the field to our Azure table column names
                        if header_text in FIELD_MAPPINGS:
                            azure_field = FIELD_MAPPINGS[header_text]
                            grant_data[azure_field] = value_text
                            logger.debug(f"Found field: {header_text} = {value_text[:30]}...")
        
        # If we couldn't find tables, try another approach - look for labeled fields
        if len(grant_data) <= 5:  # If we only have the basic fields we started with
            logger.debug("Trying alternative extraction method for grant data")
            # Look for all divs that might contain field labels and values
            for field_label, azure_field in FIELD_MAPPINGS.items():
                # Try to find elements containing these labels
                label_elements = soup.find_all(string=lambda s: s and field_label in s)
                for label in label_elements:
                    # Try to get the value - it might be in the next sibling or parent's next sibling
                    parent = label.parent
                    if parent:
                        # Try next sibling
                        next_sibling = parent.next_sibling
                        if next_sibling and hasattr(next_sibling, 'get_text'):
                            value_text = next_sibling.get_text().strip()
                            if value_text:
                                grant_data[azure_field] = value_text
                                continue
                                
                        # Try parent's next sibling
                        parent_next = parent.parent.next_sibling if parent.parent else None
                        if parent_next and hasattr(parent_next, 'get_text'):
                            value_text = parent_next.get_text().strip()
                            if value_text:
                                grant_data[azure_field] = value_text
        
        # Try to find the title
        title_element = soup.find('h1', class_=lambda c: c and 'title' in c.lower())
        if title_element:
            grant_data["Title"] = title_element.get_text().strip()
        else:
            # Try different selectors for title
            for selector in ['h1', '.title', '.grant-title', '.opportunity-title']:
                elements = soup.select(selector)
                if elements:
                    grant_data["Title"] = elements[0].get_text().strip()
                    break
        
        # Extract description (which is in a different section)
        description_section = None
        for selector in ['div#description-id', 'div.description', 'div[data-testid="description"]', 
                        'div.synopsis-detail', 'div.opportunity-description']:
            description_section = soup.select_one(selector)
            if description_section:
                break
                
        if not description_section:
            # Try looking for headers with "Description" text
            headers = soup.find_all(['h2', 'h3'], string=lambda s: s and 'description' in s.lower())
            if headers:
                for header in headers:
                    description_section = header.find_next('div')
                    if description_section:
                        break
                        
        if description_section:
            description_text = description_section.get_text().strip()
            grant_data["Description"] = description_text
            
        # Extract eligibility info
        eligibility_section = None
        for selector in ['div#eligibility-id', 'div.eligibility', 'div[data-testid="eligibility"]']:
            eligibility_section = soup.select_one(selector)
            if eligibility_section:
                break
                
        if not eligibility_section:
            # Try looking for headers with "Eligibility" text
            headers = soup.find_all(['h2', 'h3'], string=lambda s: s and 'eligibility' in s.lower())
            if headers:
                for header in headers:
                    eligibility_section = header.find_next('div')
                    if eligibility_section:
                        break
        
        if eligibility_section:
            # Try different approaches to find eligible applicants section
            applicants_found = False
            
            # Approach 1: Look for a specific header with "Eligible Applicants"
            applicants_header = eligibility_section.find(string=lambda t: t and "Eligible Applicants:" in t)
            if applicants_header:
                parent = applicants_header.parent
                if parent:
                    next_elem = parent.find_next()
                    if next_elem:
                        grant_data["EligibleApplicants"] = next_elem.get_text().strip()
                        applicants_found = True
            
            # Approach 2: Try to find by structure
            if not applicants_found:
                # Look for patterns like label and value pairs
                for elem in eligibility_section.find_all(['div', 'p']):
                    text = elem.get_text().strip()
                    if "Eligible Applicants:" in text:
                        # Get the next element or the rest of this element
                        parts = text.split("Eligible Applicants:")
                        if len(parts) > 1 and parts[1].strip():
                            grant_data["EligibleApplicants"] = parts[1].strip()
                        elif elem.next_sibling:
                            grant_data["EligibleApplicants"] = elem.next_sibling.get_text().strip()
                            
            # Look for additional eligibility info
            additional_found = False
            additional_header = eligibility_section.find(string=lambda t: t and "Additional Information on Eligibility:" in t)
            if additional_header:
                parent = additional_header.parent
                if parent:
                    next_elem = parent.find_next()
                    if next_elem:
                        grant_data["AdditionalEligibilityInfo"] = next_elem.get_text().strip()
                        additional_found = True
                        
            # Second approach for additional eligibility
            if not additional_found:
                for elem in eligibility_section.find_all(['div', 'p']):
                    text = elem.get_text().strip()
                    if "Additional Information on Eligibility:" in text:
                        parts = text.split("Additional Information on Eligibility:")
                        if len(parts) > 1 and parts[1].strip():
                            grant_data["AdditionalEligibilityInfo"] = parts[1].strip()
                        elif elem.next_sibling:
                            grant_data["AdditionalEligibilityInfo"] = elem.next_sibling.get_text().strip()
        
        # Fix numeric types
        numeric_fields = {
            "AwardCeiling": safe_float, 
            "AwardFloor": safe_float, 
            "ExpectedNumberofAwards": safe_int,
            "EstimatedTotalProgramFunding": safe_float
        }
        
        # Process numeric fields
        for field, converter in numeric_fields.items():
            if field in grant_data:
                grant_data[field] = converter(grant_data[field])
                
                # For backwards compatibility
                if field == "ExpectedNumberofAwards":
                    grant_data["ExpectedAwards"] = grant_data[field]
        
        # Fix date fields
        date_fields = ["OpenDate", "CloseDate", "LastUpdated", "OriginalCloseDate", "ArchiveDate"]
        for field in date_fields:
            if field in grant_data:
                grant_data[field] = format_date(grant_data[field])
                
        # Check if we got at least some basic data
        if len(grant_data) <= 5:
            logger.warning(f"Very little data found for grant {opportunity_id} - may need manual inspection")
        else:
            logger.debug(f"Found {len(grant_data)} fields for grant {opportunity_id}")
            
        return grant_data
    except Exception as e:
        logger.error(f"Error scraping grant {opportunity_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def process_grant(grant_id, table_client, force_update=False):
    """Process a single grant: scrape data and update Azure table"""
    try:
        # Check if this grant is already in the Azure table (to avoid duplicate work)
        try:
            existing_grant = table_client.get_entity("Grant", grant_id)
            
            # Skip if we already have it and force_update is False
            if not force_update:
                logger.debug(f"Grant {grant_id} already in Azure table - skipping")
                return False
        except Exception as e:
            logger.debug(f"Grant {grant_id} not in table yet: {str(e)}")
            # Grant doesn't exist yet
            pass
        
        # Scrape grant data
        grant_data = scrape_grant_details(grant_id)
        
        if not grant_data:
            logger.warning(f"Failed to get data for grant {grant_id}")
            return False
        
        # Show what we're going to insert (for the first few grants)
        if grant_id in list(all_grant_ids)[:5]:  # Only for first 5 grants to avoid log spam
            logger.info(f"Sample data for grant {grant_id}:")
            for key, value in sorted(grant_data.items())[:10]:  # Show first 10 fields
                if key not in ["PartitionKey", "RowKey", "OpportunityURL"]:
                    logger.info(f"  {key}: {str(value)[:50]}...")
        
        # Insert or update the grant in Azure Table
        table_client.upsert_entity(grant_data)
        logger.info(f"Successfully processed grant {grant_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing grant {grant_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def main():
    """Main function to collect complete grant data"""
    logger.info("Starting complete grants collector")
    
    # Check for debug mode
    debug_mode = "--debug" in sys.argv
    if debug_mode:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug mode enabled")
    
    # First ensure we're connected to Azure
    if not ensure_azure_connection():
        logger.error("Failed to connect to Azure. Please run 'az login' manually and try again.")
        return
    
    # Get connection string
    connection_string = get_storage_connection_string()
    if not connection_string:
        logger.error("Failed to get storage connection string. Please set STORAGE_CONNECTION environment variable.")
        return
    
    # Initialize Azure Table Storage
    try:
        logger.info("Connecting to Azure Table Storage...")
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
    
    # Check if we have a specific grant ID to process
    specific_grant = None
    for arg in sys.argv:
        if arg.startswith("--grant="):
            specific_grant = arg.split("=")[1]
            logger.info(f"Processing specific grant: {specific_grant}")
    
    # Get grant IDs
    global all_grant_ids
    all_grant_ids = set()
    
    if specific_grant:
        all_grant_ids.add(specific_grant)
    else:
        # Get fewer grants when debugging
        limit = 10 if debug_mode else 500
        
        # Process a smaller number in test mode
        if "--test" in sys.argv:
            limit = 5
            logger.info(f"Test mode: processing {limit} grants")
        
        # Get recent grants
        recent_ids = get_grant_ids(strategy="recent", limit=limit)
        all_grant_ids.update(recent_ids)
        
        # Get grants closing soon
        closing_soon_ids = get_grant_ids(strategy="closing_soon", limit=limit)
        all_grant_ids.update(closing_soon_ids)
        
        # Get some from all grants
        all_ids = get_grant_ids(strategy="all", limit=limit)
        all_grant_ids.update(all_ids)
    
    logger.info(f"Total unique grant IDs to process: {len(all_grant_ids)}")
    
    # Process grants in parallel with a ThreadPoolExecutor
    start_time = time.time()
    successful = 0
    failed = 0
    
    # Use fewer threads when debugging
    max_threads = 1 if debug_mode else min(10, len(all_grant_ids))
    
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Submit all tasks
        future_to_grant = {
            executor.submit(process_grant, grant_id, table_client, force_update or debug_mode): grant_id 
            for grant_id in all_grant_ids
        }
        
        # Process results as they complete with a progress bar
        with tqdm(total=len(future_to_grant), desc="Processing grants") as pbar:
            for future in as_completed(future_to_grant):
                grant_id = future_to_grant[future]
                try:
                    if future.result():
                        successful += 1
                    else:
                        failed += 1
                except Exception as e:
                    logger.error(f"Grant {grant_id} generated an exception: {str(e)}")
                    failed += 1
                
                pbar.update(1)
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    
    # Print summary
    logger.info("\n\n=== COLLECTION SUMMARY ===")
    logger.info(f"Total grants processed: {len(all_grant_ids)}")
    logger.info(f"Successfully updated: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Time elapsed: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    logger.info("Collection complete")
    
    # Provide next steps
    logger.info("\nNext Steps:")
    logger.info("1. View your grants at: https://grantsgovfunc60542.azurewebsites.net/api/grantsviewer?format=html&limit=1000")
    logger.info("2. To get even more grants, run this script again")

if __name__ == "__main__":
    # Import necessary modules
    import re
    
    # Process any command line arguments
    force_update = "--force" in sys.argv
    if force_update:
        logger.info("Force update mode enabled - will update all grants even if they already exist")
    
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        logger.error(traceback.format_exc())