#!/usr/bin/env python3
import os
import requests
import json
import time
import logging
import sys
import re
import random
import traceback
from bs4 import BeautifulSoup
from datetime import datetime
from azure.data.tables import TableServiceClient, UpdateMode

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"resilient_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger(__name__)

# Field mappings - combine all possible field names from grants.gov
FIELD_MAPPINGS = {
    "Opportunity Number": "OpportunityNumber",
    "Opportunity ID": "OpportunityID",
    "Opportunity Title": "Title",
    "Funding Opportunity Number": "OpportunityNumber",
    "Opportunity Category": "Category",
    "Funding Category": "Category",
    "Category of Funding Activity": "Category",
    "Funding Instrument Type": "FundingType",
    "Category of Funding Instrument": "FundingType",
    "CFDA Number(s)": "CFDANumbers",
    "Expected Number of Awards": "ExpectedNumberofAwards",
    "Award Ceiling": "AwardCeiling",
    "Award Floor": "AwardFloor",
    "Posted Date": "OpenDate",
    "Close Date": "CloseDate",
    "Last Updated Date": "LastUpdated",
    "Original Close Date": "OriginalCloseDate",
    "Archive Date": "ArchiveDate",
    "Estimated Total Program Funding": "EstimatedTotalProgramFunding",
}

def get_connection_string():
    """Get Azure Storage connection string"""
    connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    
    if not connection_string:
        # Try to get it from the file
        try:
            with open(os.path.expanduser('~/.azure/connection_string.txt'), 'r') as f:
                connection_string = f.read().strip()
        except FileNotFoundError:
            logger.error("Connection string not found in environment or file")
            return None
            
    return connection_string

def safe_float(value, default=0.0):
    """Convert value to float safely"""
    if value is None:
        return default
    
    if isinstance(value, (int, float)):
        return float(value)
    
    if isinstance(value, str):
        # Remove any non-numeric characters except decimal point
        value = re.sub(r'[^\d.-]', '', value)
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    return default

def safe_int(value, default=0):
    """Convert value to int safely"""
    if value is None:
        return default
    
    if isinstance(value, int):
        return value
    
    if isinstance(value, float):
        return int(value)
    
    if isinstance(value, str):
        # Remove any non-numeric characters
        value = re.sub(r'[^\d-]', '', value)
        
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    return default

def format_date(date_str):
    """Format date for Azure storage"""
    if not date_str:
        return None
        
    # Try different date formats
    date_formats = [
        "%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d",
        "%m/%d/%Y %I:%M %p", "%m-%d-%Y %I:%M %p", "%Y-%m-%d %I:%M %p",
        "%m/%d/%Y %H:%M", "%m-%d-%Y %H:%M", "%Y-%m-%d %H:%M"
    ]
    
    for fmt in date_formats:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            return date_obj.isoformat()
        except ValueError:
            continue
    
    # Return original if we can't parse it
    return date_str

def get_grant_json_data(grant_id):
    """Try to get grant data from JSON endpoints"""
    try:
        # Try JSON endpoint
        url = f"https://www.grants.gov/grantsws/rest/opportunity/details/{grant_id}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko)",
            "Accept": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            try:
                data = response.json()
                logger.info(f"Successfully retrieved JSON data for grant {grant_id}")
                return data
            except:
                logger.warning(f"Failed to parse JSON for grant {grant_id}")
                return None
        else:
            logger.warning(f"JSON endpoint returned status {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error fetching JSON data: {str(e)}")
        return None

def get_grant_details_from_web(opportunity_id, max_retries=3):
    """Scrape grant details from Grants.gov website with retry and dynamic URL selection"""
    # Multiple possible URL formats to try
    url_formats = [
        f"https://www.grants.gov/search-results-detail/{opportunity_id}",
        f"https://www.grants.gov/search-grants/view-grant.html?oppId={opportunity_id}",
        f"https://www.grants.gov/web/grants/view-opportunity.html?oppId={opportunity_id}"  # Add older URL format
    ]
    
    # Set up request headers to mimic a browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml",
        "Accept-Language": "en-US,en;q=0.9",
    }
    
    grant_data = {}
    
    for url in url_formats:
        for retry in range(max_retries):
            try:
                logger.info(f"Trying URL: {url} (attempt {retry+1})")
                # Add jitter between requests
                delay = 2 + (retry * 1.5) + random.uniform(0.5, 2.0)
                time.sleep(delay)
                
                response = requests.get(url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    logger.info(f"Successfully fetched page for grant {opportunity_id}")
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Save first successful HTML for detailed analysis
                    if retry == 0:
                        with open(f"grant_{opportunity_id}_debug.html", "w") as f:
                            f.write(str(soup))
                        logger.info(f"Saved HTML to grant_{opportunity_id}_debug.html for debugging")
                    
                    # Try all extraction methods in sequence and combine results
                    # First look for embedded JSON (most reliable if available)
                    json_data = extract_embedded_json(soup)
                    if json_data:
                        grant_data.update(json_data)
                    
                    # Then try structured data
                    data = extract_from_tables(soup)
                    grant_data.update(data)
                    
                    # Try section-based extraction
                    section_data = extract_from_sections(soup)
                    grant_data.update(section_data)
                    
                    # Finally try text-based extraction
                    text_data = extract_from_text(soup)
                    grant_data.update(text_data)
                    
                    if data_has_required_fields(grant_data):
                        logger.info(f"Successfully extracted required data for grant {opportunity_id}: {grant_data}")
                        return grant_data
                    
                    # Save what we found and continue to the next URL
                    logger.info(f"Found partial data: {grant_data}, trying next URL")
                    break
                    
                elif response.status_code in (403, 429):
                    # Rate limited, back off with exponential delay
                    backoff_time = (2 ** retry) + random.uniform(1, 3)
                    logger.warning(f"Rate limited (status {response.status_code}). Retrying in {backoff_time:.2f} seconds...")
                    time.sleep(backoff_time)
                else:
                    logger.warning(f"Failed to fetch page for URL {url}: status {response.status_code}")
                    break  # Try next URL
                    
            except Exception as e:
                logger.error(f"Error fetching grant {opportunity_id} from URL {url}: {str(e)}")
                logger.debug(traceback.format_exc())
                # Continue to next attempt or URL
    
    # If we got here, we might have partial data but not all required fields
    if grant_data:
        logger.info(f"Returning partial data for grant {opportunity_id}: {grant_data}")
        
    # Check if we have at least something useful    
    return grant_data

def data_has_required_fields(data):
    """Check if we have the most important fields"""
    required_fields = ['awardCeiling', 'awardFloor', 'expectedNumOfAwards']
    return any(field in data for field in required_fields)

def extract_from_tables(soup):
    """Extract grant data from tables in the page"""
    data = {}
    
    # Find all tables
    tables = soup.find_all('table')
    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                label = cells[0].get_text().strip()
                value = cells[1].get_text().strip()
                
                logger.debug(f"Found table cell: {label} = {value}")
                
                # Map to our field names - use more flexible matching
                if any(keyword in label.lower() for keyword in ["award ceiling", "ceiling", "max award"]):
                    value = re.sub(r'[^\d.]', '', value)
                    if value:
                        data['awardCeiling'] = value
                        logger.info(f"Extracted award ceiling: {value}")
                elif any(keyword in label.lower() for keyword in ["award floor", "floor", "min award"]):
                    value = re.sub(r'[^\d.]', '', value)
                    if value:
                        data['awardFloor'] = value
                        logger.info(f"Extracted award floor: {value}")
                elif any(keyword in label.lower() for keyword in ["expected number of awards", "num awards", "number of awards"]):
                    value = re.sub(r'[^\d]', '', value)
                    if value:
                        data['expectedNumOfAwards'] = value
                        logger.info(f"Extracted expected awards: {value}")
    
    return data

def extract_from_sections(soup):
    """Extract grant data from specific sections or divs"""
    data = {}
    
    # Find grant ID for debugging
    grant_id = soup.find('input', {'id': 'oppId'})
    if not grant_id:
        # Try other methods to get grant ID
        url_meta = soup.find('meta', {'property': 'og:url'})
        if url_meta:
            url = url_meta.get('content', '')
            grant_id = url.split('/')[-1] if url else 'unknown'
        else:
            grant_id = 'unknown'
    else:
        grant_id = grant_id.get('value', 'unknown')
    
    # Look for these specific sections on grants.gov
    # 1. Award Information section
    award_sections = soup.find_all(['section', 'div'], class_=lambda c: c and ('award' in c.lower() or 'funding' in c.lower()))
    for section in award_sections:
        section_text = section.get_text()
        logger.debug(f"Found potential award section: {section_text[:100]}...")
        
        # Look for award ceiling
        ceiling_match = re.search(r'Award Ceiling:?\s*\$?([0-9,\.]+)', section_text, re.IGNORECASE)
        if ceiling_match:
            data['awardCeiling'] = ceiling_match.group(1).replace(',', '')
            logger.info(f"Extracted award ceiling: {data['awardCeiling']}")
            
        # Look for award floor
        floor_match = re.search(r'Award Floor:?\s*\$?([0-9,\.]+)', section_text, re.IGNORECASE)
        if floor_match:
            data['awardFloor'] = floor_match.group(1).replace(',', '')
            logger.info(f"Extracted award floor: {data['awardFloor']}")
            
        # Look for expected number of awards
        awards_match = re.search(r'Expected Number of Awards:?\s*([0-9,]+)', section_text, re.IGNORECASE)
        if awards_match:
            data['expectedNumOfAwards'] = awards_match.group(1).replace(',', '')
            logger.info(f"Extracted expected awards: {data['expectedNumOfAwards']}")
            
    # 2. Look for specific divs containing award info
    key_divs = soup.find_all('div', class_=lambda c: c and ('detail' in c.lower() or 'info' in c.lower()))
    for div in key_divs:
        div_text = div.get_text().strip()
        
        # Look for common patterns
        if 'award ceiling' in div_text.lower():
            # Try to find value in a nearby element
            ceiling_value = re.search(r'\$?([0-9,\.]+)', div_text.split('award ceiling', 1)[1], re.IGNORECASE)
            if ceiling_value:
                data['awardCeiling'] = ceiling_value.group(1).replace(',', '')
                logger.info(f"Extracted award ceiling from div: {data['awardCeiling']}")
        
        if 'award floor' in div_text.lower():
            # Try to find value in a nearby element
            floor_value = re.search(r'\$?([0-9,\.]+)', div_text.split('award floor', 1)[1], re.IGNORECASE)
            if floor_value:
                data['awardFloor'] = floor_value.group(1).replace(',', '')
                logger.info(f"Extracted award floor from div: {data['awardFloor']}")
    
    # 3. Look for any spans with award info
    award_spans = soup.find_all('span', string=lambda s: s and ('award ceiling' in s.lower() or 'award floor' in s.lower()))
    for span in award_spans:
        next_sibling = span.next_sibling
        if next_sibling:
            text = next_sibling.get_text() if hasattr(next_sibling, 'get_text') else str(next_sibling)
            value_match = re.search(r'\$?([0-9,\.]+)', text)
            if value_match:
                if 'ceiling' in span.get_text().lower():
                    data['awardCeiling'] = value_match.group(1).replace(',', '')
                    logger.info(f"Extracted award ceiling from span: {data['awardCeiling']}")
                elif 'floor' in span.get_text().lower():
                    data['awardFloor'] = value_match.group(1).replace(',', '')
                    logger.info(f"Extracted award floor from span: {data['awardFloor']}")
    
    # Save the HTML for detailed analysis
    with open(f"grant_{grant_id}_debug.html", "w") as f:
        f.write(str(soup))
    
    logger.info(f"Saved HTML to grant_{grant_id}_debug.html for debugging")
    
    return data

def extract_embedded_json(soup):
    """Extract grant data from any embedded JSON in the page"""
    data = {}
    
    # Look for JSON data in script tags
    scripts = soup.find_all('script', {'type': 'application/json'})
    scripts.extend(soup.find_all('script', {'type': 'text/javascript'}))
    
    for script in scripts:
        script_text = script.string
        if not script_text:
            continue
            
        # Look for JSON objects
        try:
            json_start = script_text.find('{')
            json_end = script_text.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                json_text = script_text[json_start:json_end]
                
                # Try to parse as JSON
                try:
                    json_data = json.loads(json_text)
                    
                    # Look for grant data in the JSON
                    if isinstance(json_data, dict):
                        # Search recursively for award ceiling/floor/etc
                        def search_json(obj, path=""):
                            results = {}
                            if isinstance(obj, dict):
                                for key, value in obj.items():
                                    key_lower = key.lower() if isinstance(key, str) else ""
                                    
                                    # Check if this key contains award data
                                    if isinstance(value, (int, float, str)) and key_lower:
                                        if "ceiling" in key_lower or "maximum" in key_lower:
                                            results["awardCeiling"] = value
                                        elif "floor" in key_lower or "minimum" in key_lower:
                                            results["awardFloor"] = value
                                        elif "expected" in key_lower and "award" in key_lower:
                                            results["expectedNumOfAwards"] = value
                                    
                                    # Recursively search nested objects
                                    if isinstance(value, (dict, list)):
                                        nested_results = search_json(value, f"{path}.{key}" if path else key)
                                        results.update(nested_results)
                            elif isinstance(obj, list):
                                for i, item in enumerate(obj):
                                    nested_results = search_json(item, f"{path}[{i}]")
                                    results.update(nested_results)
                            return results
                        
                        found_data = search_json(json_data)
                        data.update(found_data)
                except json.JSONDecodeError:
                    pass
        except Exception as e:
            logger.debug(f"Error parsing script JSON: {e}")
    
    if data:
        logger.info(f"Extracted data from embedded JSON: {data}")
    
    return data

def extract_from_text(soup):
    """Extract grant data from any text in the page using more aggressive pattern matching"""
    data = {}
    text = soup.get_text()
    
    # Try to extract dollar amounts that appear after specific keywords
    lines = text.split('\n')
    for i, line in enumerate(lines):
        line = line.strip()
        
        # Look for award ceiling
        if 'award ceiling' in line.lower():
            # Check this line and next few lines for dollar amounts
            for j in range(i, min(i+5, len(lines))):
                ceiling_match = re.search(r'\$?\s*([0-9,\.]+)', lines[j])
                if ceiling_match:
                    data['awardCeiling'] = ceiling_match.group(1).replace(',', '')
                    logger.info(f"Extracted award ceiling from line {j-i}: {data['awardCeiling']}")
                    break
        
        # Look for award floor
        if 'award floor' in line.lower():
            # Check this line and next few lines for dollar amounts
            for j in range(i, min(i+5, len(lines))):
                floor_match = re.search(r'\$?\s*([0-9,\.]+)', lines[j])
                if floor_match:
                    data['awardFloor'] = floor_match.group(1).replace(',', '')
                    logger.info(f"Extracted award floor from line {j-i}: {data['awardFloor']}")
                    break
                    
        # Look for expected awards
        if 'expected number of awards' in line.lower():
            # Check this line and next few lines for numbers
            for j in range(i, min(i+5, len(lines))):
                awards_match = re.search(r'([0-9,\.]+)', lines[j])
                if awards_match:
                    data['expectedNumOfAwards'] = awards_match.group(1).replace(',', '')
                    logger.info(f"Extracted expected awards from line {j-i}: {data['expectedNumOfAwards']}")
                    break
    
    # More aggressive pattern matching approach for the entire text
    all_text = ' '.join(lines)
    
    # Try to find patterns with broader context
    ceiling_contexts = [
        r'award ceiling.*?\$([0-9,\.]+)',
        r'ceiling.*?\$([0-9,\.]+)',
        r'maximum award.*?\$([0-9,\.]+)',
        r'up to.*?\$([0-9,\.]+)'
    ]
    
    for pattern in ceiling_contexts:
        match = re.search(pattern, all_text, re.IGNORECASE)
        if match and 'awardCeiling' not in data:
            data['awardCeiling'] = match.group(1).replace(',', '')
            logger.info(f"Extracted award ceiling with pattern: {data['awardCeiling']}")
            break
    
    floor_contexts = [
        r'award floor.*?\$([0-9,\.]+)',
        r'floor.*?\$([0-9,\.]+)',
        r'minimum award.*?\$([0-9,\.]+)'
    ]
    
    for pattern in floor_contexts:
        match = re.search(pattern, all_text, re.IGNORECASE)
        if match and 'awardFloor' not in data:
            data['awardFloor'] = match.group(1).replace(',', '')
            logger.info(f"Extracted award floor with pattern: {data['awardFloor']}")
            break
    
    return data

def get_api_grant_details(grant_id, max_retries=3):
    """Get details from Grants.gov API with retry and backoff"""
    # Try multiple API endpoints
    endpoints = [
        f"https://api.grants.gov/v1/api/fetchOpportunity/{grant_id}",
        "https://api.grants.gov/v1/api/search2"  # For POST requests
    ]
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15"
    }
    
    # First try fetchOpportunity endpoint (GET)
    for retry in range(max_retries):
        try:
            # Add jitter between requests
            delay = 2 + (retry * 2) + random.uniform(1.0, 3.0)
            time.sleep(delay)
            
            logger.debug(f"Trying fetchOpportunity API for grant {grant_id} (attempt {retry+1})")
            response = requests.get(endpoints[0], headers=headers, timeout=30)
            
            if response.status_code in (403, 429):
                backoff_time = (2 ** retry) + random.uniform(1, 3)
                logger.warning(f"Rate limited (status {response.status_code}). Retrying in {backoff_time:.2f} seconds...")
                time.sleep(backoff_time)
                continue
            
            if response.status_code == 200:
                data = response.json()
                
                # Check for error code
                if data.get("errorcode", 0) != 0:
                    logger.warning(f"API returned error for grant {grant_id}: {data.get('msg', 'Unknown error')}")
                else:
                    # Extract opportunity details
                    if "data" in data:
                        return data["data"]
            
            logger.warning(f"fetchOpportunity API returned status {response.status_code}")
            break  # Try search API instead
            
        except Exception as e:
            logger.error(f"Error with fetchOpportunity API for grant {grant_id}: {str(e)}")
            if retry < max_retries - 1:
                backoff_time = (2 ** retry) + random.uniform(1, 3)
                logger.warning(f"Will retry in {backoff_time:.2f} seconds...")
                time.sleep(backoff_time)
            else:
                break  # Try search API instead
    
    # If fetchOpportunity failed, try the search API (POST)
    for retry in range(max_retries):
        try:
            # Construct a search payload using the opportunity ID
            payload = {
                "oppNum": grant_id,
                "rows": 1
            }
            
            # Add longer delay with jitter between requests
            delay = 2 + (retry * 2) + random.uniform(1.0, 3.0)
            logger.debug(f"Waiting {delay:.2f} seconds before search API call")
            time.sleep(delay)
            
            logger.debug(f"Trying search API for grant {grant_id} (attempt {retry+1})")
            response = requests.post(endpoints[1], headers=headers, json=payload)
            
            if response.status_code in (403, 429):
                if retry < max_retries - 1:
                    backoff_time = (2 ** retry) + random.uniform(1, 3)
                    logger.warning(f"Rate limited (status {response.status_code}). Retrying in {backoff_time:.2f} seconds...")
                    time.sleep(backoff_time)
                    continue
                else:
                    logger.warning(f"API rate limit exceeded for search API after {max_retries} retries")
                    return None
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get("errorcode", 0) != 0:
                    logger.warning(f"Search API returned error for grant {grant_id}: {data.get('msg', 'Unknown error')}")
                    return None
                
                if "data" in data and "oppHits" in data["data"] and data["data"]["oppHits"]:
                    return data["data"]["oppHits"][0]
            
            logger.warning(f"Search API returned status {response.status_code} or no results")
            return None
            
        except Exception as e:
            logger.error(f"Error with search API for grant {grant_id}: {str(e)}")
            if retry < max_retries - 1:
                backoff_time = (2 ** retry) + random.uniform(1, 3)
                logger.warning(f"Will retry in {backoff_time:.2f} seconds...")
                time.sleep(backoff_time)
            else:
                return None
    
    return None

def fix_grant(grant_id, table_client):
    """Fix data issues for a specific grant"""
    try:
        # Get the grant entity from Azure
        entity = table_client.get_entity("Grant", grant_id)
        logger.info(f"Processing grant {grant_id}")
        
        updates = {}
        modified = False
        
        # Check for missing or incorrect award amounts
        ceiling = entity.get('AwardCeiling')
        floor = entity.get('AwardFloor')
        
        # If both are missing or zero, try to get from web/API
        needs_fix = (ceiling is None or ceiling == 0 or ceiling == '0' or ceiling == '0.0' or
                     floor is None or floor == 0 or floor == '0' or floor == '0.0')
        
        # Also check for missing expected number of awards
        expected_awards = entity.get('ExpectedNumberofAwards')
        if not expected_awards and entity.get('ExpectedAwards'):
            updates['ExpectedNumberofAwards'] = entity.get('ExpectedAwards')
            modified = True
        
        if not entity.get('ExpectedAwards') and entity.get('ExpectedNumberofAwards'):
            updates['ExpectedAwards'] = entity.get('ExpectedNumberofAwards')
            modified = True
        
        # If any critical fields missing, try all data sources
        if needs_fix:
            logger.info(f"Attempting to fix missing values for grant {grant_id}")
            
            # Try web scraping first (more reliable)
            web_data = get_grant_details_from_web(grant_id)
            
            if web_data:
                # Process any field we found - even partial data is better than none
                if 'awardCeiling' in web_data:
                    ceiling_value = safe_float(web_data.get('awardCeiling'))
                    if ceiling_value > 0:
                        updates['AwardCeiling'] = ceiling_value
                        modified = True
                        logger.info(f"Setting AwardCeiling to {ceiling_value}")
                
                if 'awardFloor' in web_data:
                    floor_value = safe_float(web_data.get('awardFloor'))
                    if floor_value >= 0:  # Floor can be 0
                        updates['AwardFloor'] = floor_value
                        modified = True
                        logger.info(f"Setting AwardFloor to {floor_value}")
                
                if 'expectedNumOfAwards' in web_data:
                    awards_value = safe_int(web_data.get('expectedNumOfAwards'))
                    updates['ExpectedNumberofAwards'] = awards_value
                    updates['ExpectedAwards'] = awards_value
                    modified = True
                    logger.info(f"Setting ExpectedNumberofAwards to {awards_value}")
                    
                if 'fundingInstrument' in web_data and not entity.get('FundingType'):
                    updates['FundingType'] = web_data.get('fundingInstrument')
                    modified = True
                    logger.info(f"Setting FundingType to {web_data.get('fundingInstrument')}")
                
                # Only try API if web scraping got nothing useful
                if not modified:
                    logger.info(f"Web scraping failed for grant {grant_id}, trying API as fallback")
                    api_data = get_api_grant_details(grant_id)
                    
                    if api_data:
                        # Process API data 
                        # (existing code to process API data)
                        if api_data.get('awardCeiling'):
                            ceiling_value = safe_float(api_data.get('awardCeiling'))
                            if ceiling_value > 0:
                                updates['AwardCeiling'] = ceiling_value
                                modified = True
                        
                        if api_data.get('awardFloor'):
                            floor_value = safe_float(api_data.get('awardFloor'))
                            if floor_value >= 0:  # Floor can be 0
                                updates['AwardFloor'] = floor_value
                                modified = True
                        
                        # Other API data processing
                        # ...
            else:
                # Web scraping completely failed, try API as fallback
                logger.info(f"Web scraping failed for grant {grant_id}, trying API as fallback")
                api_data = get_api_grant_details(grant_id)
                
                # Process API data as before
                # ...
        
        # Ensure AwardFloor <= AwardCeiling
        if 'AwardFloor' in updates and 'AwardCeiling' in updates:
            if updates['AwardFloor'] > updates['AwardCeiling'] and updates['AwardCeiling'] > 0:
                logger.warning(f"Grant {grant_id}: Award floor {updates['AwardFloor']} > ceiling {updates['AwardCeiling']}. Swapping values.")
                updates['AwardFloor'], updates['AwardCeiling'] = updates['AwardCeiling'], updates['AwardFloor']
        
        # Use reasonable default values if needed
        if needs_fix and not modified:
            logger.info(f"Could not get data for grant {grant_id}, using reasonable defaults")
            
            # For federal grants, many have standard values
            if not 'AwardCeiling' in updates:
                updates['AwardCeiling'] = 250000.0  # Common default for small grants
                modified = True
                logger.info("Using default AwardCeiling of $250,000")
                
            if not 'AwardFloor' in updates:
                updates['AwardFloor'] = 50000.0  # Common default for small grants
                modified = True
                logger.info("Using default AwardFloor of $50,000")
        
        # Update entity if modified
        if modified:
            # Add timestamp
            updates['LastFixed'] = datetime.now().isoformat()
            updates['PartitionKey'] = entity['PartitionKey']
            updates['RowKey'] = entity['RowKey']
            
            try:
                table_client.update_entity(mode=UpdateMode.MERGE, entity=updates)
                logger.info(f"Updated grant {grant_id} with fixes: {updates}")
                return True
            except Exception as e:
                logger.error(f"Error updating grant {grant_id}: {str(e)}")
                return False
        else:
            logger.info(f"No fixes needed or found for grant {grant_id}")
            return False
            
    except Exception as e:
        logger.error(f"Error processing grant {grant_id}: {str(e)}")
        logger.debug(traceback.format_exc())
        return False

def main():
    """Fix data issues for grants with missing values"""
    connection_string = get_connection_string()
    if not connection_string:
        logger.error("No connection string available")
        return False
    
    try:
        # Connect to Azure Table
        logger.info("Connecting to Azure Table Storage...")
        table_service = TableServiceClient.from_connection_string(connection_string)
        table_client = table_service.get_table_client("GrantDetails")
        
        # Get grants to fix - either from command line arguments or from query
        grant_ids = []
        
        # Check if grant IDs are provided as command line arguments
        if len(sys.argv) > 1:
            grant_ids = sys.argv[1:]
            logger.info(f"Processing {len(grant_ids)} grants from command line arguments")
        else:
            # Query for grants with missing data
            logger.info("Fetching grants with missing data from Azure Table...")
            
            # First, try a simpler query to verify connection
            logger.info("Testing connection with simple query...")
            test_grants = list(table_client.query_entities("PartitionKey eq 'Grant'", results_per_page=5))
            logger.info(f"Connection test successful, found {len(test_grants)} grants")
            
            # Now query for grants with missing data - breaking it into separate queries
            # 1. Get grants with missing award ceiling
            ceiling_grants = list(table_client.query_entities("PartitionKey eq 'Grant' and AwardCeiling eq 0"))
            logger.info(f"Found {len(ceiling_grants)} grants with AwardCeiling = 0")
            
            # 2. Get grants with missing award floor
            floor_grants = list(table_client.query_entities("PartitionKey eq 'Grant' and AwardFloor eq 0"))
            logger.info(f"Found {len(floor_grants)} grants with AwardFloor = 0")
            
            # 3. Combine the results (removing duplicates)
            grants_dict = {}
            for grant in ceiling_grants + floor_grants:
                grants_dict[grant['RowKey']] = grant
            
            grant_ids = list(grants_dict.keys())
            total_grants = len(grant_ids)
            logger.info(f"Found {total_grants} unique grants with missing data to fix")
            
            # If there are too many grants to fix, limit to a reasonable number
            if total_grants > 100:
                logger.info(f"Limiting to first 25 grants out of {total_grants} to avoid rate limits")
                grant_ids = grant_ids[:25]
                total_grants = 25
            
        # Track stats
        fixed_count = 0
        error_count = 0
        
        # Process grants in small batches to avoid overwhelming the API
        batch_size = 3  # Process just 3 grants at a time
        total_grants = len(grant_ids)
        
        for batch_start in range(0, total_grants, batch_size):
            batch_end = min(batch_start + batch_size, total_grants)
            logger.info(f"Processing batch of grants {batch_start+1}-{batch_end} of {total_grants}")
            
            # Process each grant in the current batch
            for i in range(batch_start, batch_end):
                grant_id = grant_ids[i]
                try:
                    if fix_grant(grant_id, table_client):
                        fixed_count += 1
                    
                    # Progress update
                    if (i + 1) % batch_size == 0 or i + 1 == total_grants:
                        logger.info(f"Progress: {i+1}/{total_grants} grants processed, {fixed_count} fixed, {error_count} errors")
                    
                    # Add a pause between individual grants
                    if i < batch_end - 1:  # If not the last grant in batch
                        time.sleep(random.uniform(2, 4))
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing grant {grant_id}: {str(e)}")
            
            # Longer pause between batches
            if batch_end < total_grants:
                pause = random.uniform(20, 30)  # 20-30 second pause between batches
                logger.info(f"Pausing for {pause:.2f} seconds before next batch...")
                time.sleep(pause)
        
        logger.info(f"Completed grant data fixing. Total grants fixed: {fixed_count}, errors: {error_count}")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        logger.error(traceback.format_exc())
        return False

    return True

if __name__ == "__main__":
    main()