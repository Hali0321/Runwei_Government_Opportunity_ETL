import azure.functions as func
import logging
import json
import os
from typing import Dict, List, Optional, Union
from azure.data.tables import TableServiceClient, TableEntity
from azure.storage.queue import QueueServiceClient
from datetime import datetime, timedelta
import pandas as pd
import re
import math

def main(msg: func.QueueMessage) -> None:
    """
    Process grants.gov data and store in Azure Table Storage
    This function is triggered by queue messages containing grant data processing requests
    """
    logging.info('GrantsGovDBProcessor function started processing a queue message.')
    
    try:
        # Parse the queue message with enhanced error handling
        try:
            message_body = msg.get_body().decode('utf-8')
            request_data = json.loads(message_body)
            logging.info(f"Processing request type: {request_data.get('type', 'unknown')}")
        except Exception as e:
            logging.error(f"Failed to parse queue message: {str(e)}")
            return  # Don't re-raise for malformed messages
        
        # Validate required environment variables
        connection_string = os.environ.get('STORAGE_CONNECTION_STRING') or os.environ.get('AzureWebJobsStorage')
        if not connection_string:
            logging.error("STORAGE_CONNECTION_STRING environment variable not set")
            return
        
        # Initialize Azure services with connection validation
        try:
            table_service = TableServiceClient.from_connection_string(connection_string)
            table_name = os.environ.get('GRANTS_TABLE_NAME', 'GrantsData')
            table_client = table_service.get_table_client(table_name)
            
            # Ensure table exists with proper error handling
            try:
                table_client.create_table()
                logging.info(f"Table {table_name} ensured to exist")
            except Exception as create_error:
                # Table likely already exists
                logging.debug(f"Table creation result: {str(create_error)}")
                
        except Exception as e:
            logging.error(f"Failed to initialize Azure Table service: {str(e)}")
            raise
        
        # Process based on request type with enhanced routing
        request_type = request_data.get('type', 'process_grants')
        
        if request_type == 'process_grants':
            result = process_grants_data(table_client, request_data)
            logging.info(f"Grants processing completed: {result}")
        elif request_type == 'update_grant':
            update_single_grant(table_client, request_data)
        elif request_type == 'cleanup_old_data':
            cleanup_old_grants(table_client, request_data)
        else:
            logging.warning(f"Unknown request type: {request_type}")
            return
            
        logging.info('GrantsGovDBProcessor completed successfully.')
        
    except Exception as e:
        logging.error(f"GrantsGovDBProcessor error: {str(e)}")
        # Re-raise to trigger retry mechanism
        raise

def process_grants_data(table_client, request_data) -> Dict[str, int]:
    """Process bulk grants data from CSV or API response with optimized batch processing"""
    try:
        # Get data source
        data_source = request_data.get('data_source', 'csv')
        
        if data_source == 'csv':
            csv_path = request_data.get('csv_path')
            if not csv_path:
                # ✅ Fixed: Use relative path that works in Azure
                csv_path = 'grants-gov-opp-search.csv'
                # Check if file exists in multiple locations
                possible_paths = [
                    csv_path,
                    f'/tmp/{csv_path}',
                    f'/home/site/wwwroot/{csv_path}',
                    'data/grants-gov-opp-search.csv'
                ]
                
                csv_path = None
                for path in possible_paths:
                    if os.path.exists(path):
                        csv_path = path
                        break
                
                if not csv_path:
                    logging.error("CSV file not found in any expected location")
                    return {'processed': 0, 'failed': 0, 'error': 'CSV file not found'}
            
            grants_data = load_grants_from_csv(csv_path)
        else:
            grants_data = request_data.get('grants_data', [])
        
        if not grants_data:
            logging.warning("No grants data to process")
            return {'processed': 0, 'failed': 0, 'message': 'No data to process'}
        
        # Process grants using the transformation function
        processed_count = 0
        failed_count = 0
        batch_size = 10
        
        total_grants = len(grants_data)
        logging.info(f"Starting to process {total_grants} grants in batches of {batch_size}")
        
        for i in range(0, len(grants_data), batch_size):
            batch = grants_data[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_grants + batch_size - 1) // batch_size
            
            logging.info(f"Processing batch {batch_num}/{total_batches}: {len(batch)} grants")
            
            for grant in batch:
                try:
                    # ✅ Fixed: Use the transform function properly
                    transformed_grant = transform_single_record(grant)
                    if transformed_grant:
                        entity = transform_grant_to_entity(transformed_grant)
                        
                        # Upsert to table with retry logic
                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                table_client.upsert_entity(entity)
                                processed_count += 1
                                break
                            except Exception as retry_e:
                                if attempt == max_retries - 1:
                                    logging.error(f"Failed after {max_retries} attempts: {grant.get('opportunity_id', 'unknown')}")
                                    failed_count += 1
                                else:
                                    logging.warning(f"Retry {attempt + 1}: {str(retry_e)}")
                    else:
                        failed_count += 1
                    
                except Exception as e:
                    logging.error(f"Failed to process grant {grant.get('opportunity_id', 'unknown')}: {str(e)}")
                    failed_count += 1
            
            # Small delay between batches
            if batch_num < total_batches:
                import time
                time.sleep(0.1)
        
        result = {
            'processed': processed_count,
            'failed': failed_count,
            'total': total_grants,
            'success_rate': round((processed_count / total_grants) * 100, 2) if total_grants > 0 else 0
        }
        
        logging.info(f"Batch processing completed: {result}")
        return result
        
    except Exception as e:
        logging.error(f"Error in process_grants_data: {str(e)}")
        raise

def load_grants_from_csv(csv_path) -> List[Dict]:
    """Load grants data from CSV file with robust encoding handling"""
    try:
        logging.info(f"Loading CSV from: {csv_path}")
        
        # Try multiple encodings
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(csv_path, encoding=encoding)
                logging.info(f"Successfully loaded CSV with {encoding} encoding")
                break
            except (UnicodeDecodeError, UnicodeError):
                continue
            except Exception as e:
                logging.warning(f"Failed to load CSV with {encoding}: {str(e)}")
                continue
        
        if df is None:
            raise Exception("Could not load CSV with any supported encoding")
        
        if df.empty:
            logging.warning("CSV file is empty")
            return []
        
        logging.info(f"CSV loaded successfully: {len(df)} rows")
        
        grants_data = []
        skipped_rows = 0
        
        for idx, row in df.iterrows():
            try:
                # ✅ Fixed: Proper opportunity ID extraction
                opportunity_id = extract_opportunity_id(str(row.get('OPPORTUNITY NUMBER', '')))
                
                if not opportunity_id:
                    skipped_rows += 1
                    continue
                
                # Create grant object with proper field mapping
                grant = {
                    'OPPORTUNITY NUMBER': opportunity_id,
                    'OPPORTUNITY TITLE': safe_str(row.get('OPPORTUNITY TITLE', '')),
                    'AGENCY CODE': safe_str(row.get('AGENCY CODE', '')),
                    'AGENCY NAME': safe_str(row.get('AGENCY NAME', '')),
                    'CATEGORY OF FUNDING ACTIVITY': safe_str(row.get('CATEGORY OF FUNDING ACTIVITY', '')),
                    'FUNDING INSTRUMENT TYPE': safe_str(row.get('FUNDING INSTRUMENT TYPE', '')),
                    'ESTIMATED TOTAL FUNDING': safe_str(row.get('ESTIMATED TOTAL FUNDING', '')),
                    'EXPECTED NUMBER OF AWARDS': safe_str(row.get('EXPECTED NUMBER OF AWARDS', '')),
                    'AWARD CEILING': safe_str(row.get('AWARD CEILING', '')),
                    'AWARD FLOOR': safe_str(row.get('AWARD FLOOR', '')),
                    'LINK TO ADDITIONAL INFORMATION': safe_str(row.get('LINK TO ADDITIONAL INFORMATION', '')),
                    'GRANTOR CONTACT EMAIL': safe_str(row.get('GRANTOR CONTACT EMAIL', '')),
                    'POSTED DATE': safe_str(row.get('POSTED DATE', '')),
                    'CLOSE DATE': safe_str(row.get('CLOSE DATE', '')),
                    'FUNDING DESCRIPTION': safe_str(row.get('FUNDING DESCRIPTION', ''))[:2000],
                    'ELIGIBLE APPLICANTS': safe_str(row.get('ELIGIBLE APPLICANTS', '')),
                    'ASSISTANCE LISTINGS': safe_str(row.get('ASSISTANCE LISTINGS', ''))
                }
                grants_data.append(grant)
                
            except Exception as e:
                logging.warning(f"Failed to process row {idx}: {str(e)}")
                skipped_rows += 1
                continue
        
        logging.info(f"Loaded {len(grants_data)} valid grants from CSV (skipped {skipped_rows} invalid rows)")
        return grants_data
        
    except Exception as e:
        logging.error(f"Error loading CSV: {str(e)}")
        raise

# ===== MISSING HELPER FUNCTIONS - NOW IMPLEMENTED =====

def extract_opportunity_id(opportunity_number: str) -> str:
    """Extract opportunity ID from various formats"""
    if not opportunity_number:
        return ''
    
    # Handle HYPERLINK formulas: =HYPERLINK("url","ID")
    if 'HYPERLINK' in opportunity_number and '","' in opportunity_number:
        try:
            parts = opportunity_number.split('","')
            if len(parts) >= 2:
                return parts[1].replace('")', '').strip()
        except:
            pass
    
    # Clean and validate the opportunity ID
    cleaned = str(opportunity_number).strip()
    
    # Remove common invalid values
    if cleaned.lower() in ['nan', 'none', '', 'null']:
        return ''
    
    return cleaned

def clean_text(text: str) -> str:
    """Clean text for display and storage"""
    if not text:
        return ''
    
    # Convert to string and strip whitespace
    cleaned = str(text).strip()
    
    # Remove excessive whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Remove null-like values
    if cleaned.lower() in ['nan', 'none', 'null']:
        return ''
    
    return cleaned

def transform_date(date_str: str) -> str:
    """Transform date string to ISO format"""
    if not date_str or str(date_str).lower() in ['nan', 'none', 'null', '']:
        return ''
    
    try:
        # Common date formats in grants data
        date_formats = [
            '%m/%d/%Y',    # 12/31/2024
            '%Y-%m-%d',    # 2024-12-31
            '%m-%d-%Y',    # 12-31-2024
            '%d/%m/%Y',    # 31/12/2024
            '%B %d, %Y',   # December 31, 2024
            '%b %d, %Y',   # Dec 31, 2024
            '%Y/%m/%d'     # 2024/12/31
        ]
        
        date_str = str(date_str).strip()
        
        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(date_str, fmt)
                return date_obj.isoformat()
            except ValueError:
                continue
        
        # If no format matched, return original
        return date_str
        
    except Exception as e:
        logging.warning(f"Date transformation failed for '{date_str}': {str(e)}")
        return str(date_str)

def safe_float(value) -> float:
    """Safely convert value to float"""
    if pd.isna(value) or value is None or value == '':
        return 0.0
    
    try:
        # Handle string values with currency symbols and commas
        if isinstance(value, str):
            # Remove currency symbols and commas
            cleaned = re.sub(r'[^\d.-]', '', value)
            if not cleaned:
                return 0.0
            return float(cleaned)
        
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def safe_str(value) -> str:
    """Safely convert value to string, handling NaN and None values"""
    if pd.isna(value) or value is None:
        return ''
    return str(value).strip()

# ===== MAPPING FUNCTIONS =====

def map_opportunity_gap(category: str) -> str:
    """Map funding category to opportunity gap"""
    if not category:
        return ''
    
    category_lower = str(category).lower()
    
    gap_mapping = {
        'research': 'Research & Development',
        'technology': 'Technology Innovation',
        'education': 'Education & Training',
        'health': 'Healthcare & Wellness',
        'environment': 'Environmental Sustainability',
        'agriculture': 'Agriculture & Food Security',
        'energy': 'Energy & Infrastructure',
        'economic': 'Economic Development',
        'social': 'Social Services',
        'transportation': 'Transportation & Infrastructure'
    }
    
    for key, gap in gap_mapping.items():
        if key in category_lower:
            return gap
    
    return 'General Funding'

def map_opportunity_type(instrument_type: str) -> str:
    """Map funding instrument to opportunity type"""
    if not instrument_type:
        return 'Grant'
    
    instrument_lower = str(instrument_type).lower()
    
    if 'grant' in instrument_lower:
        return 'Grant'
    elif 'loan' in instrument_lower:
        return 'Loan'
    elif 'cooperative' in instrument_lower or 'agreement' in instrument_lower:
        return 'Cooperative Agreement'
    elif 'contract' in instrument_lower:
        return 'Contract'
    else:
        return 'Grant'

def determine_global_eligibility(eligible_applicants: str) -> bool:
    """Determine if opportunity is globally available"""
    if not eligible_applicants:
        return False
    
    applicants_lower = str(eligible_applicants).lower()
    
    # Check for international eligibility indicators
    international_indicators = [
        'international', 'global', 'worldwide', 'foreign',
        'non-us', 'overseas', 'multinational'
    ]
    
    return any(indicator in applicants_lower for indicator in international_indicators)

def extract_target_community(eligible_applicants: str) -> str:
    """Extract target community from eligibility text"""
    if not eligible_applicants:
        return ''
    
    applicants_lower = str(eligible_applicants).lower()
    
    community_mapping = {
        'small business': 'Small Business',
        'minority': 'Minority-Owned Business',
        'women': 'Women-Owned Business',
        'veteran': 'Veteran-Owned Business',
        'university': 'Academic Institutions',
        'nonprofit': 'Nonprofit Organizations',
        'tribal': 'Tribal Organizations',
        'rural': 'Rural Communities',
        'urban': 'Urban Communities'
    }
    
    for key, community in community_mapping.items():
        if key in applicants_lower:
            return community
    
    return 'General Public'

def map_industry(category: str) -> str:
    """Map funding category to industry"""
    if not category:
        return ''
    
    category_lower = str(category).lower()
    
    industry_mapping = {
        'science': 'Science & Technology',
        'technology': 'Technology',
        'health': 'Healthcare',
        'education': 'Education',
        'environment': 'Environmental',
        'agriculture': 'Agriculture',
        'energy': 'Energy',
        'transportation': 'Transportation',
        'manufacturing': 'Manufacturing',
        'defense': 'Defense',
        'aerospace': 'Aerospace'
    }
    
    for key, industry in industry_mapping.items():
        if key in category_lower:
            return industry
    
    return 'General'

def transform_grant_to_entity(grant_data: Dict) -> TableEntity:
    """Transform grant data to Azure Table Storage entity"""
    try:
        # Use opportunity ID as both partition and row key for simplicity
        opportunity_id = str(grant_data.get('SourceID', grant_data.get('OpportunityURL', '').split('/')[-1]))
        
        entity = TableEntity()
        entity['PartitionKey'] = 'Grant'
        entity['RowKey'] = clean_table_key(opportunity_id)
        
        # Add all grant data fields with proper type conversion
        for key, value in grant_data.items():
            # Clean field names for Azure Table Storage
            clean_key = clean_table_key(key)
            if clean_key and clean_key not in ['PartitionKey', 'RowKey']:
                # Handle different data types
                if isinstance(value, (list, dict)):
                    entity[clean_key] = json.dumps(value)
                elif isinstance(value, (int, float)):
                    entity[clean_key] = value
                elif isinstance(value, bool):
                    entity[clean_key] = value
                else:
                    # Convert to string and truncate if too long
                    str_value = str(value)[:1000] if value else ''
                    entity[clean_key] = str_value
        
        # Add metadata
        entity['LastUpdated'] = datetime.utcnow()
        entity['ProcessedDate'] = datetime.utcnow()
        
        return entity
        
    except Exception as e:
        logging.error(f"Error transforming grant to entity: {str(e)}")
        raise

def clean_table_key(key: str) -> str:
    """Clean key for Azure Table Storage compatibility"""
    if not key:
        return ''
    
    # Remove invalid characters for Azure Table Storage
    cleaned = ''.join(c for c in str(key) if c.isalnum() or c in '-_.')
    
    # Ensure key doesn't start or end with invalid characters
    cleaned = cleaned.strip('-_.')
    
    return cleaned[:1024]  # Azure Table Storage key limit

# ===== KEEP EXISTING FUNCTIONS =====

def transform_single_record(source_record: Dict) -> Optional[Dict]:
    """Transform a single grant record to company schema with complete database design alignment"""
    
    try:
        # Extract opportunity ID
        opportunity_id = extract_opportunity_id(source_record.get('OPPORTUNITY NUMBER', ''))
        if not opportunity_id:
            return None
        
        # Map fields to new schema following DATABASE DESIGN ORDER + additional fields
        transformed = {
            # === DATABASE DESIGN PRIORITY ORDER ===
            
            # General Section (High Priority)
            "OpportunityURL": f"https://www.grants.gov/search-results-detail/{opportunity_id}",
            "Title": clean_text(source_record.get('OPPORTUNITY TITLE', '')),
            "Deadline": transform_date(source_record.get('CLOSE DATE', '')),
            "TimeZone": "EST",
            
            # Financial Information (High Priority)
            "AwardValue": safe_float(source_record.get('AWARD CEILING', 0)),
            "CashAward": safe_float(source_record.get('AWARD CEILING', 0)),
            
            # Application Details (High Priority)
            "DirectLinkToApplyURL": f"https://www.grants.gov/search-results-detail/{opportunity_id}",
            
            # Opportunity Details (High Priority)
            "OpportunityGap": map_opportunity_gap(source_record.get('CATEGORY OF FUNDING ACTIVITY', '')),
            "Type": map_opportunity_type(source_record.get('FUNDING INSTRUMENT TYPE', '')),
            
            # Geographic Eligibility (High Priority)
            "GlobalOpportunity": determine_global_eligibility(source_record.get('ELIGIBLE APPLICANTS', '')),
            "GlobalLocations": "North America",
            "CountriesEligible": "United States",
            "LocationDetails": "United States",
            
            # Detailed Information (High Priority)
            "ShortDescription": clean_text(source_record.get('FUNDING DESCRIPTION', ''))[:500],
            "Eligibility": clean_text(source_record.get('ELIGIBLE APPLICANTS', '')),
            "LongDescription": clean_text(source_record.get('FUNDING DESCRIPTION', '')),
            "TargetCommunity": extract_target_community(source_record.get('ELIGIBLE APPLICANTS', '')),
            "OpportunityLogoURL": "",
            "DatePosted": transform_date(source_record.get('POSTED DATE', '')),
            "Industry": map_industry(source_record.get('CATEGORY OF FUNDING ACTIVITY', '')),
            
            # === MISSING COLUMNS FROM DATABASE DESIGN ===
            "UNSDGAlignment": map_un_sdg_alignment(source_record.get('CATEGORY OF FUNDING ACTIVITY', ''), source_record.get('FUNDING DESCRIPTION', '')),
            
            # Additional Information (Medium Priority)
            "ServiceProviderESO": clean_text(source_record.get('AGENCY NAME', '')),
            "ESOWebsite": source_record.get('LINK TO ADDITIONAL INFORMATION', ''),
            "ContactEmailForOpportunity": source_record.get('GRANTOR CONTACT EMAIL', ''),
            
            # Internal Review (Medium Priority)
            "Cost": 0.0,
            "FinancialTermsOrCostList": "Grant",
            "FinancialTerms": f"Award Range: ${safe_float(source_record.get('AWARD FLOOR', 0)):,.0f} - ${safe_float(source_record.get('AWARD CEILING', 0)):,.0f}",
            "OpportunityRating": calculate_opportunity_rating(source_record),
            "ProviderRating": calculate_provider_rating(source_record.get('AGENCY NAME', '')),
            
            # === ADDITIONAL METADATA (Lower Priority) ===
            "SourceSystem": "Grants.gov",
            "SourceID": opportunity_id,
            "AgencyCode": source_record.get('AGENCY CODE', ''),
            "CFDANumbers": source_record.get('ASSISTANCE LISTINGS', ''),
            "ExpectedAwards": source_record.get('EXPECTED NUMBER OF AWARDS', ''),
            "TotalFunding": source_record.get('ESTIMATED TOTAL FUNDING', ''),
            "LastUpdated": datetime.utcnow().isoformat(),
            "ProcessingDate": datetime.utcnow().isoformat(),
            
            # === ENHANCED ANALYTICS FIELDS ===
            "CompetitionLevel": calculate_competition_level(source_record),
            "FundingAmountNormalized": normalize_funding_amount(source_record.get('AWARD CEILING', 0)),
            "DeadlineUrgency": calculate_deadline_urgency(source_record.get('CLOSE DATE', '')),
            "EligibilityMatchScore": calculate_eligibility_match_score(source_record.get('ELIGIBLE APPLICANTS', '')),
            "AgencyScore": calculate_agency_score(source_record.get('AGENCY NAME', '')),
            "KeywordMatches": extract_keyword_matches(source_record.get('FUNDING DESCRIPTION', ''))
        }
        
        return transformed
        
    except Exception as e:
        logging.warning(f"Error transforming record: {str(e)}")
        return None

# ===== KEEP ALL YOUR EXISTING FUNCTIONS =====
# map_un_sdg_alignment, calculate_opportunity_rating, calculate_provider_rating, etc.
# (All the functions you already have are kept as-is)

def map_un_sdg_alignment(category: str, description: str) -> List[str]:
    """Map grant content to UN Sustainable Development Goals"""
    
    sdg_mapping = {
        "1": ["poverty", "economic development", "income"],
        "2": ["agriculture", "food", "hunger", "nutrition"],
        "3": ["health", "medical", "wellness", "disease"],
        "4": ["education", "training", "learning", "school"],
        "5": ["gender", "women", "equality"],
        "6": ["water", "sanitation", "clean water"],
        "7": ["energy", "renewable", "clean energy"],
        "8": ["economic", "employment", "jobs", "growth"],
        "9": ["infrastructure", "innovation", "technology"],
        "10": ["inequality", "inclusion", "equity"],
        "11": ["cities", "urban", "communities", "sustainable"],
        "12": ["consumption", "production", "waste", "recycling"],
        "13": ["climate", "environmental", "carbon"],
        "14": ["ocean", "marine", "sea", "aquatic"],
        "15": ["biodiversity", "ecosystem", "forest", "land"],
        "16": ["peace", "justice", "institutions", "governance"],
        "17": ["partnership", "collaboration", "global"]
    }
    
    content = f"{category} {description}".lower()
    aligned_sdgs = []
    
    for sdg, keywords in sdg_mapping.items():
        if any(keyword in content for keyword in keywords):
            aligned_sdgs.append(f"SDG {sdg}")
    
    return aligned_sdgs[:3]

def calculate_opportunity_rating(source_record: Dict) -> float:
    """Calculate opportunity rating based on multiple factors"""
    
    rating = 5.0
    
    # Factor 1: Award amount
    award_amount = safe_float(source_record.get('AWARD CEILING', 0))
    if award_amount > 1000000:
        rating += 1.0
    elif award_amount > 500000:
        rating += 0.5
    elif award_amount < 50000:
        rating -= 0.5
    
    # Factor 2: Competition level
    expected_awards = safe_float(source_record.get('EXPECTED NUMBER OF AWARDS', 1))
    if expected_awards > 50:
        rating += 0.5
    elif expected_awards < 5:
        rating -= 0.5
    
    # Factor 3: Agency reputation
    agency = source_record.get('AGENCY NAME', '').lower()
    prestigious_agencies = ['nsf', 'nih', 'nasa', 'doe', 'darpa']
    if any(agency_name in agency for agency_name in prestigious_agencies):
        rating += 0.5
    
    # Factor 4: Deadline urgency
    deadline = source_record.get('CLOSE DATE', '')
    urgency = calculate_deadline_urgency(deadline)
    if urgency > 180:
        rating += 0.3
    elif urgency < 30:
        rating -= 0.5
    
    return min(max(rating, 1.0), 10.0)

def calculate_provider_rating(agency_name: str) -> float:
    """Calculate provider/agency rating"""
    
    agency = agency_name.lower()
    
    tier1_agencies = {
        'national science foundation': 9.5,
        'national institutes of health': 9.5,
        'nasa': 9.0,
        'department of energy': 8.5,
        'darpa': 9.0
    }
    
    tier2_agencies = {
        'department of commerce': 8.0,
        'department of defense': 8.5,
        'usda': 7.5,
        'epa': 8.0
    }
    
    for tier1_agency, rating in tier1_agencies.items():
        if tier1_agency in agency:
            return rating
    
    for tier2_agency, rating in tier2_agencies.items():
        if tier2_agency in agency:
            return rating
    
    return 7.0

def calculate_competition_level(source_record: Dict) -> str:
    """Calculate competition level (Low/Medium/High)"""
    
    expected_awards = safe_float(source_record.get('EXPECTED NUMBER OF AWARDS', 1))
    award_ceiling = safe_float(source_record.get('AWARD CEILING', 0))
    
    if expected_awards > 100:
        return "Low"
    elif expected_awards > 20:
        return "Medium"
    elif award_ceiling > 1000000:
        return "High"
    else:
        return "Medium"

def normalize_funding_amount(award_amount) -> float:
    """Normalize funding amount for ML features (0-1 scale)"""
    
    amount = safe_float(award_amount)
    
    if amount <= 0:
        return 0.0
    
    log_amount = math.log10(max(amount, 1000))
    log_min = math.log10(1000)
    log_max = math.log10(10000000)
    
    normalized = (log_amount - log_min) / (log_max - log_min)
    return min(max(normalized, 0.0), 1.0)

def calculate_deadline_urgency(deadline_str: str) -> int:
    """Calculate days until deadline"""
    
    if not deadline_str:
        return 365
    
    try:
        deadline_date = datetime.strptime(transform_date(deadline_str)[:10], '%Y-%m-%d')
        today = datetime.utcnow()
        days_until = (deadline_date - today).days
        return max(days_until, 0)
    except:
        return 365

def calculate_eligibility_match_score(eligibility: str) -> float:
    """Calculate how well eligibility matches common applicant types"""
    
    eligibility_lower = str(eligibility).lower()
    
    if 'unrestricted' in eligibility_lower or 'open to any' in eligibility_lower:
        return 1.0
    elif 'small business' in eligibility_lower:
        return 0.9
    elif 'nonprofit' in eligibility_lower:
        return 0.8
    elif 'university' in eligibility_lower or 'education' in eligibility_lower:
        return 0.7
    elif 'government' in eligibility_lower:
        return 0.6
    else:
        return 0.5

def calculate_agency_score(agency_name: str) -> float:
    """Calculate agency score for ML features"""
    
    provider_rating = calculate_provider_rating(agency_name)
    return provider_rating / 10.0

def extract_keyword_matches(description: str) -> List[str]:
    """Extract relevant keywords from description"""
    
    high_value_keywords = [
        'innovation', 'technology', 'research', 'development', 'startup',
        'entrepreneurship', 'small business', 'artificial intelligence',
        'machine learning', 'sustainability', 'clean energy', 'healthcare',
        'education', 'training', 'workforce development'
    ]
    
    description_lower = str(description).lower()
    matches = [keyword for keyword in high_value_keywords if keyword in description_lower]
    
    return matches[:5]

def update_single_grant(table_client, request_data):
    """Update a single grant record with enhanced validation"""
    try:
        grant_data = request_data.get('grant_data', {})
        if not grant_data:
            logging.warning("No grant data provided for update")
            return
        
        transformed_grant = transform_single_record(grant_data)
        if transformed_grant:
            entity = transform_grant_to_entity(transformed_grant)
            table_client.upsert_entity(entity)
            logging.info(f"Updated grant: {grant_data.get('OPPORTUNITY NUMBER', 'unknown')}")
        
    except Exception as e:
        logging.error(f"Error updating single grant: {str(e)}")
        raise

def cleanup_old_grants(table_client, request_data):
    """Clean up old grant records with batch deletion optimization"""
    try:
        cutoff_days = request_data.get('cutoff_days', 180)
        cutoff_date = datetime.utcnow() - timedelta(days=cutoff_days)
        
        logging.info(f"Cleaning up grants older than {cutoff_days} days")
        
        filter_query = f"LastUpdated lt datetime'{cutoff_date.isoformat()}'"
        old_entities = table_client.query_entities(filter_query)
        
        deleted_count = 0
        failed_count = 0
        
        for entity in old_entities:
            try:
                table_client.delete_entity(entity['PartitionKey'], entity['RowKey'])
                deleted_count += 1
                
                if deleted_count % 100 == 0:
                    logging.info(f"Deleted {deleted_count} old records so far...")
                    
            except Exception as e:
                logging.warning(f"Failed to delete entity {entity['RowKey']}: {str(e)}")
                failed_count += 1
        
        logging.info(f"Cleanup completed: {deleted_count} deleted, {failed_count} failed")
        
    except Exception as e:
        logging.error(f"Error in cleanup_old_grants: {str(e)}")
        raise