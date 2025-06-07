import azure.functions as func
import json
import logging
import os
import pandas as pd
from typing import Dict, List, Optional
from azure.data.tables import TableServiceClient
from datetime import datetime
import re
from io import StringIO

# Direct connection string for your storage account
STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=grantsgov225756;AccountKey=UXwW5dfy9MY9nh2BGmWhYUbzBve+6LUyT3F7+N3Cp0kWUoEk4AO3z5U6LrBYvo/VwO+Nduq2ay9E+AStKQb86Q==;EndpointSuffix=core.windows.net"

def main(req: func.HttpRequest) -> func.HttpResponse:
    """Transform grants data to company database schema"""
    
    logging.info('DataProcessor function processing transformation request')
    
    try:
        # Get processing parameters
        source_type = req.params.get('source', 'azure_table')
        target_format = req.params.get('format', 'json')
        limit = int(req.params.get('limit', 100))
        table_name = req.params.get('table', '')  # Allow specifying table name
        
        # Load and transform data
        transformed_data = process_grants_data_to_schema(source_type, limit, table_name)
        
        logging.info(f"Transformed {len(transformed_data)} records")
        
        if target_format == 'azure_table':
            # Store in Azure Table Storage
            result = store_in_azure_table(transformed_data)
            return func.HttpResponse(
                json.dumps({
                    "status": "success",
                    "message": f"Processed and stored {len(transformed_data)} records",
                    "details": result,
                    "source_tables_checked": get_available_grant_tables()
                }),
                mimetype="application/json",
                status_code=200
            )
        elif target_format == 'csv':
            # Return as CSV
            csv_data = convert_to_csv(transformed_data)
            return func.HttpResponse(
                csv_data,
                mimetype="text/csv",
                status_code=200,
                headers={"Content-Disposition": "attachment; filename=transformed_grants.csv"}
            )
        else:
            # Return as JSON
            return func.HttpResponse(
                json.dumps({
                    "transformed_data": transformed_data,
                    "metadata": {
                        "record_count": len(transformed_data),
                        "source_tables_checked": get_available_grant_tables(),
                        "processing_timestamp": datetime.utcnow().isoformat()
                    }
                }, default=str, indent=2),
                mimetype="application/json",
                status_code=200
            )
            
    except Exception as e:
        logging.error(f"Error in data processing: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "error": f"Processing failed: {str(e)}",
                "debug_info": get_debug_info()
            }),
            status_code=500,
            mimetype="application/json"
        )

def get_available_grant_tables() -> List[str]:
    """Get list of grant-related tables"""
    try:
        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        all_tables = list(table_service.list_tables())
        table_names = [table.name for table in all_tables]
        
        # Filter for grant-related tables
        grant_tables = [t for t in table_names 
                       if any(keyword in t.lower() for keyword in ['grant', 'company', 'transformed'])]
        return grant_tables
    except:
        return []

def get_debug_info() -> Dict:
    """Get debug information about available data sources"""
    
    debug = {
        "storage_account": "grantsgov225756",
        "connection_methods": {},
        "available_tables": [],
        "grant_tables": []
    }
    
    # Check direct connection
    try:
        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        tables = list(table_service.list_tables())
        debug["available_tables"] = [table.name for table in tables]
        debug["grant_tables"] = get_available_grant_tables()
        debug["connection_methods"]["direct"] = "SUCCESS"
    except Exception as e:
        debug["connection_methods"]["direct"] = f"FAILED: {str(e)}"
    
    return debug

def process_grants_data_to_schema(source_type: str, limit: int, table_name: str = "") -> List[Dict]:
    """Transform grants data to match company database schema"""
    
    # Load source data from your actual tables
    source_data = load_source_data(source_type, limit, table_name)
    logging.info(f"Loaded {len(source_data)} source records")
    
    if not source_data:
        logging.warning("No source data found, using sample data for demonstration")
        source_data = get_sample_data()
    
    transformed_records = []
    
    for record in source_data:
        try:
            # Transform each record to the new schema
            transformed_record = transform_single_record(record)
            if transformed_record:
                transformed_records.append(transformed_record)
        except Exception as e:
            logging.warning(f"Failed to transform record: {str(e)}")
            continue
    
    logging.info(f"Successfully transformed {len(transformed_records)} records")
    return transformed_records

def load_source_data(source_type: str, limit: int, table_name: str = "") -> List[Dict]:
    """Load data from your actual Azure Storage tables"""
    
    return load_from_azure_table_direct(limit, table_name)

def load_from_azure_table_direct(limit: int, specific_table: str = "") -> List[Dict]:
    """Load from Azure Storage tables using direct connection"""
    
    try:
        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        
        # Your actual table names from the diagnostic
        available_tables = [
            "GrantDetails", 
            "GrantsData", 
            "CompanyProcessed",
            "GrantsHistory", 
            "TransformedGrants"
        ]
        
        # If specific table requested, use it; otherwise try priority order
        if specific_table and specific_table in available_tables:
            tables_to_try = [specific_table]
        else:
            # Try tables in priority order (most likely to have raw grant data)
            tables_to_try = ["GrantsData", "GrantDetails", "CompanyProcessed", "GrantsHistory"]
        
        for table_name in tables_to_try:
            try:
                table_client = table_service.get_table_client(table_name)
                
                # Query for entities using list_entities (no filter required)
                entities = list(table_client.list_entities(results_per_page=limit))
                
                if entities:
                    logging.info(f"Found {len(entities)} records in table {table_name}")
                    
                    # Convert entities to standardized format
                    records = []
                    for entity in entities:
                        # Try to map entity fields to expected CSV format
                        record = map_entity_to_csv_format(entity, table_name)
                        if record:
                            records.append(record)
                    
                    if records:
                        logging.info(f"Successfully mapped {len(records)} records from {table_name}")
                        return records
                    
            except Exception as e:
                logging.warning(f"Failed to access table {table_name}: {str(e)}")
                continue
        
        logging.warning("No accessible grant tables found with data")
        return []
        
    except Exception as e:
        logging.error(f"Error connecting to Azure Storage: {str(e)}")
        return []

def map_entity_to_csv_format(entity: Dict, table_name: str) -> Optional[Dict]:
    """Map Azure Table entity to CSV-like format for processing"""
    
    try:
        # Create a mapping based on common field patterns and your actual data
        record = {}
        
        # Opportunity ID/Number - try multiple field names
        record['OPPORTUNITY NUMBER'] = (
            entity.get('OpportunityId') or 
            entity.get('OpportunityNumber') or 
            entity.get('ID') or 
            entity.get('RowKey') or
            entity.get('OpportunityID') or
            entity.get('GrantId', '')
        )
        
        # Title - try multiple field names
        record['OPPORTUNITY TITLE'] = (
            entity.get('Title') or 
            entity.get('OpportunityTitle') or 
            entity.get('Name') or
            entity.get('GrantTitle') or
            entity.get('OpportunityName', '')
        )
        
        # Agency information
        record['AGENCY NAME'] = (
            entity.get('AgencyName') or 
            entity.get('Agency') or 
            entity.get('ServiceProvider') or
            entity.get('GrantingAgency') or
            entity.get('OrganizationName', '')
        )
        
        record['AGENCY CODE'] = (
            entity.get('AgencyCode') or 
            entity.get('Code') or
            entity.get('AgencyAbbr', '')
        )
        
        # Description fields
        record['FUNDING DESCRIPTION'] = (
            entity.get('Description') or 
            entity.get('FundingDescription') or 
            entity.get('ShortDescription') or 
            entity.get('LongDescription') or
            entity.get('Synopsis') or
            entity.get('ProgramDescription', '')
        )
        
        # Date fields
        record['POSTED DATE'] = (
            entity.get('PostedDate') or 
            entity.get('DatePosted') or 
            entity.get('CreatedDate') or
            entity.get('PublishDate') or
            entity.get('AnnouncementDate', '')
        )
        
        record['CLOSE DATE'] = (
            entity.get('CloseDate') or 
            entity.get('Deadline') or 
            entity.get('DueDate') or
            entity.get('ApplicationDeadline') or
            entity.get('ClosingDate', '')
        )
        
        # Financial information
        record['AWARD CEILING'] = (
            entity.get('AwardCeiling') or 
            entity.get('AwardValue') or 
            entity.get('MaxAmount') or 
            entity.get('CashAward') or
            entity.get('MaxAward') or
            entity.get('CeilingAmount', 0)
        )
        
        record['AWARD FLOOR'] = (
            entity.get('AwardFloor') or 
            entity.get('MinAmount') or
            entity.get('MinAward') or
            entity.get('FloorAmount', 0)
        )
        
        # Category and type information
        record['CATEGORY OF FUNDING ACTIVITY'] = (
            entity.get('Category') or 
            entity.get('CategoryOfFundingActivity') or 
            entity.get('Industry') or 
            entity.get('Type') or
            entity.get('FundingCategory') or
            entity.get('ProgramType', '')
        )
        
        record['FUNDING INSTRUMENT TYPE'] = (
            entity.get('FundingInstrumentType') or 
            entity.get('InstrumentType') or
            entity.get('AwardType') or
            'Grant'  # Default
        )
        
        # Eligibility information
        record['ELIGIBLE APPLICANTS'] = (
            entity.get('EligibleApplicants') or 
            entity.get('Eligibility') or 
            entity.get('TargetCommunity') or
            entity.get('ApplicantEligibility') or
            entity.get('EligibilityRequirements', '')
        )
        
        # Links and contact information
        record['LINK TO ADDITIONAL INFORMATION'] = (
            entity.get('AdditionalInfoURL') or 
            entity.get('LinkToAdditionalInformation') or 
            entity.get('ESOWebsite') or 
            entity.get('Website') or
            entity.get('ProgramURL') or
            entity.get('MoreInfoURL', '')
        )
        
        # CFDA/Assistance Listings
        record['ASSISTANCE LISTINGS'] = (
            entity.get('CFDANumbers') or 
            entity.get('AssistanceListings') or 
            entity.get('ProgramNumbers') or
            entity.get('CFDANumber') or
            entity.get('AssistanceListingNumber', '')
        )
        
        # Additional fields
        record['EXPECTED NUMBER OF AWARDS'] = (
            entity.get('ExpectedNumberOfAwards') or 
            entity.get('ExpectedAwards') or
            entity.get('NumberOfAwards') or
            entity.get('AwardsExpected', '')
        )
        
        record['ESTIMATED TOTAL FUNDING'] = (
            entity.get('EstimatedTotalFunding') or 
            entity.get('TotalFunding') or
            entity.get('TotalProgramFunding') or
            entity.get('BudgetAmount', '')
        )
        
        record['GRANTOR CONTACT EMAIL'] = (
            entity.get('GrantorContactEmail') or 
            entity.get('ContactEmail') or 
            entity.get('ContactEmailForOpportunity') or
            entity.get('ProgramContactEmail') or
            entity.get('Email', '')
        )
        
        # Only return record if it has essential fields
        if record['OPPORTUNITY NUMBER'] and record['OPPORTUNITY TITLE']:
            logging.info(f"Successfully mapped record: {record['OPPORTUNITY NUMBER']} - {record['OPPORTUNITY TITLE'][:50]}...")
            return record
        else:
            logging.debug(f"Skipping entity without essential fields in table {table_name}")
            return None
            
    except Exception as e:
        logging.warning(f"Error mapping entity: {str(e)}")
        return None

def transform_single_record(source_record: Dict) -> Optional[Dict]:
    """Transform a single grant record to company schema"""
    
    try:
        # Extract opportunity ID
        opportunity_id = extract_opportunity_id(source_record.get('OPPORTUNITY NUMBER', ''))
        if not opportunity_id:
            return None
        
        # Map fields to new schema with Azure Table friendly names
        transformed = {
            # General Section
            "OpportunityURL": f"https://www.grants.gov/search-results-detail/{opportunity_id}",
            "Title": clean_text(source_record.get('OPPORTUNITY TITLE', '')),
            "Deadline": transform_date(source_record.get('CLOSE DATE', '')),
            "TimeZone": "EST",
            
            # Financial Information
            "AwardValue": safe_float(source_record.get('AWARD CEILING', 0)),
            "CashAward": safe_float(source_record.get('AWARD CEILING', 0)),
            
            # Application Details
            "DirectLinkToApplyURL": f"https://www.grants.gov/search-results-detail/{opportunity_id}",
            
            # Opportunity Details
            "OpportunityGap": map_opportunity_gap(source_record.get('CATEGORY OF FUNDING ACTIVITY', '')),
            "Type": map_opportunity_type(source_record.get('FUNDING INSTRUMENT TYPE', '')),
            
            # Geographic Eligibility
            "GlobalOpportunity": determine_global_eligibility(source_record.get('ELIGIBLE APPLICANTS', '')),
            "GlobalLocations": "North America",
            "CountriesEligible": "United States",
            "LocationDetails": "United States",
            
            # Detailed Information
            "ShortDescription": clean_text(source_record.get('FUNDING DESCRIPTION', ''))[:500],
            "Eligibility": clean_text(source_record.get('ELIGIBLE APPLICANTS', '')),
            "LongDescription": clean_text(source_record.get('FUNDING DESCRIPTION', '')),
            "TargetCommunity": extract_target_community(source_record.get('ELIGIBLE APPLICANTS', '')),
            "OpportunityLogoURL": "",
            "DatePosted": transform_date(source_record.get('POSTED DATE', '')),
            "Industry": map_industry(source_record.get('CATEGORY OF FUNDING ACTIVITY', '')),
            
            # Additional Information
            "ServiceProviderESO": clean_text(source_record.get('AGENCY NAME', '')),
            "ESOWebsite": source_record.get('LINK TO ADDITIONAL INFORMATION', ''),
            "ContactEmailForOpportunity": source_record.get('GRANTOR CONTACT EMAIL', ''),
            
            # Internal Review
            "Cost": 0.0,
            "FinancialTermsOrCostList": "Grant",
            "FinancialTerms": f"Award Range: ${safe_float(source_record.get('AWARD FLOOR', 0)):,.0f} - ${safe_float(source_record.get('AWARD CEILING', 0)):,.0f}",
            
            # Metadata
            "SourceSystem": "Grants.gov",
            "SourceID": opportunity_id,
            "AgencyCode": source_record.get('AGENCY CODE', ''),
            "CFDANumbers": source_record.get('ASSISTANCE LISTINGS', ''),
            "ExpectedAwards": source_record.get('EXPECTED NUMBER OF AWARDS', ''),
            "TotalFunding": source_record.get('ESTIMATED TOTAL FUNDING', ''),
            "LastUpdated": datetime.utcnow().isoformat(),
            "ProcessingDate": datetime.utcnow().isoformat()
        }
        
        return transformed
        
    except Exception as e:
        logging.warning(f"Error transforming record: {str(e)}")
        return None

# Include all your existing helper functions
def extract_opportunity_id(opportunity_number: str) -> str:
    """Extract clean opportunity ID from hyperlink format"""
    if not opportunity_number:
        return ""
    
    if 'HYPERLINK' in opportunity_number and '","' in opportunity_number:
        try:
            return opportunity_number.split('","')[1].replace('")', '').strip()
        except:
            return ""
    
    return str(opportunity_number).strip()

def clean_text(text: str) -> str:
    """Clean and normalize text fields"""
    if pd.isna(text) or text is None:
        return ""
    
    text = str(text).strip()
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'<[^>]+>', '', text)
    
    return text

def transform_date(date_str: str) -> str:
    """Transform date to ISO format"""
    if not date_str or pd.isna(date_str):
        return ""
    
    try:
        date_str = str(date_str).strip()
        formats = ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f']
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.isoformat()
            except:
                continue
        
        return date_str
        
    except:
        return ""

def safe_float(value) -> float:
    """Safely convert value to float"""
    try:
        if pd.isna(value) or value is None or value == '':
            return 0.0
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            cleaned = re.sub(r'[,$]', '', str(value))
            numbers = re.findall(r'\d+\.?\d*', cleaned)
            if numbers:
                return float(numbers[0])
        
        return float(value)
    except:
        return 0.0

def map_opportunity_gap(category: str) -> str:
    """Map funding category to opportunity gap"""
    category = str(category).lower()
    
    if any(word in category for word in ['business', 'commerce', 'economic']):
        return "Capital Access"
    elif any(word in category for word in ['education', 'training', 'development']):
        return "Capacity Building Access"
    elif any(word in category for word in ['network', 'partnership', 'collaboration']):
        return "Networks Access"
    else:
        return "Capacity Building Access"

def map_opportunity_type(funding_instrument: str) -> str:
    """Map funding instrument to opportunity type"""
    instrument = str(funding_instrument).lower()
    
    if 'grant' in instrument:
        return "Grants"
    elif 'loan' in instrument:
        return "Loans"
    elif 'cooperative' in instrument:
        return "Mentorship Programs"
    else:
        return "Grants"

def determine_global_eligibility(eligible_applicants: str) -> bool:
    """Determine if opportunity is globally available"""
    applicants = str(eligible_applicants).lower()
    
    if any(word in applicants for word in ['state', 'local', 'tribal', 'us', 'american']):
        return False
    
    return True

def extract_target_community(eligible_applicants: str) -> str:
    """Extract target community from eligibility requirements"""
    applicants = str(eligible_applicants).lower()
    
    if 'small business' in applicants:
        return "Small Businesses"
    elif 'nonprofit' in applicants:
        return "Nonprofit Organizations"
    elif 'university' in applicants or 'education' in applicants:
        return "Educational Institutions"
    elif 'tribal' in applicants:
        return "Tribal Communities"
    else:
        return "General Public"

def map_industry(category: str) -> str:
    """Map funding category to industry"""
    category = str(category).lower()
    
    industry_mapping = {
        'health': 'Healthcare',
        'education': 'Education',
        'science': 'Science & Technology',
        'technology': 'Science & Technology',
        'environment': 'Environmental',
        'energy': 'Energy',
        'agriculture': 'Agriculture',
        'arts': 'Arts & Culture',
        'business': 'Business & Commerce',
        'social': 'Social Services',
        'community': 'Community Development'
    }
    
    for keyword, industry in industry_mapping.items():
        if keyword in category:
            return industry
    
    return 'General'

def sanitize_property_name(name: str) -> str:
    """Sanitize property names for Azure Table Storage"""
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '', name)
    
    if sanitized and sanitized[0].isdigit():
        sanitized = f"Field_{sanitized}"
    
    if not sanitized:
        sanitized = "UnknownField"
    
    return sanitized

def store_in_azure_table(transformed_data: List[Dict]) -> Dict:
    """Store transformed data in Azure Table Storage with proper property names"""
    
    try:
        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        table_name = 'CompanyTransformed'  # Use a new table name for your company schema
        
        table_client = table_service.create_table_if_not_exists(table_name)
        
        stored_count = 0
        errors = []
        
        for i, record in enumerate(transformed_data):
            try:
                entity = {
                    'PartitionKey': 'Opportunity',
                    'RowKey': record.get('SourceID', f'record_{i}'),
                }
                
                for key, value in record.items():
                    if key not in ['PartitionKey', 'RowKey']:
                        sanitized_key = sanitize_property_name(key)
                        
                        if isinstance(value, bool):
                            entity[sanitized_key] = value
                        elif isinstance(value, (int, float)):
                            entity[sanitized_key] = value
                        elif isinstance(value, datetime):
                            entity[sanitized_key] = value
                        else:
                            entity[sanitized_key] = str(value) if value is not None else ""
                
                entity['OriginalFields'] = json.dumps({sanitize_property_name(k): k for k in record.keys()})
                
                table_client.upsert_entity(entity)
                stored_count += 1
                
            except Exception as e:
                error_msg = f"Failed to store record {i}: {str(e)}"
                errors.append(error_msg)
                logging.error(error_msg)
        
        return {
            "stored_count": stored_count,
            "total_records": len(transformed_data),
            "errors": errors[:10],
            "table_name": table_name
        }
        
    except Exception as e:
        return {"error": f"Storage failed: {str(e)}"}

def convert_to_csv(transformed_data: List[Dict]) -> str:
    """Convert transformed data to CSV format"""
    
    try:
        if not transformed_data:
            return ""
        
        df = pd.DataFrame(transformed_data)
        return df.to_csv(index=False)
        
    except Exception as e:
        logging.error(f"CSV conversion error: {str(e)}")
        return ""

def get_sample_data() -> List[Dict]:
    """Provide sample data only as last resort"""
    
    return [
        {
            'OPPORTUNITY NUMBER': 'SAMPLE-001',
            'OPPORTUNITY TITLE': 'Sample Research Grant for Technology Innovation',
            'AGENCY NAME': 'National Science Foundation',
            'AGENCY CODE': 'NSF',
            'FUNDING DESCRIPTION': 'This is a sample grant opportunity for technology research and innovation projects.',
            'POSTED DATE': '01/15/2024',
            'CLOSE DATE': '12/31/2024',
            'AWARD CEILING': 500000,
            'AWARD FLOOR': 100000,
            'CATEGORY OF FUNDING ACTIVITY': 'Science and Technology',
            'FUNDING INSTRUMENT TYPE': 'Grant',
            'ELIGIBLE APPLICANTS': 'Universities, Research Institutions',
            'LINK TO ADDITIONAL INFORMATION': 'https://nsf.gov/sample',
            'ASSISTANCE LISTINGS': '47.041',
            'EXPECTED NUMBER OF AWARDS': '5',
            'ESTIMATED TOTAL FUNDING': '2500000',
            'GRANTOR CONTACT EMAIL': 'grants@nsf.gov'
        }
    ]