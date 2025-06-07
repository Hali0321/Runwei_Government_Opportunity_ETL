import azure.functions as func
import logging
import json
import os
from typing import Dict, List, Optional
from azure.data.tables import TableServiceClient, TableEntity
from azure.storage.queue import QueueServiceClient
from datetime import datetime, timedelta
import pandas as pd

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
        connection_string = os.environ.get('STORAGE_CONNECTION_STRING')
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
                # Use absolute path for local development
                csv_path = '/Users/dinghali/Desktop/Runwei/grants_gov_api_azure/src/scripts/grants-gov-opp-search--20250530141151.csv'
            
            # Validate CSV exists before processing
            if not os.path.exists(csv_path):
                logging.error(f"CSV file not found: {csv_path}")
                return {'processed': 0, 'failed': 0, 'error': 'CSV file not found'}
            
            grants_data = load_grants_from_csv(csv_path)
        else:
            grants_data = request_data.get('grants_data', [])
        
        if not grants_data:
            logging.warning("No grants data to process")
            return {'processed': 0, 'failed': 0, 'message': 'No data to process'}
        
        # Optimized batch processing with Azure Table Storage best practices
        processed_count = 0
        failed_count = 0
        batch_size = 10  # Optimal batch size for Azure Table Storage
        
        total_grants = len(grants_data)
        logging.info(f"Starting to process {total_grants} grants in batches of {batch_size}")
        
        # Process in batches to optimize Azure Table Storage operations
        for i in range(0, len(grants_data), batch_size):
            batch = grants_data[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_grants + batch_size - 1) // batch_size
            
            logging.info(f"Processing batch {batch_num}/{total_batches}: {len(batch)} grants")
            
            # Process each grant in the batch
            for grant in batch:
                try:
                    # Transform grant data to table entity
                    entity = transform_grant_to_entity(grant)
                    
                    # Upsert to table with retry logic for resilience
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
                                logging.warning(f"Retry {attempt + 1} for grant {grant.get('opportunity_id', 'unknown')}: {str(retry_e)}")
                    
                except Exception as e:
                    logging.error(f"Failed to process grant {grant.get('opportunity_id', 'unknown')}: {str(e)}")
                    failed_count += 1
            
            # Small delay between batches to prevent throttling
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
        
        # Try multiple encodings for maximum compatibility
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
        
        # Convert to list of dictionaries with enhanced data validation
        grants_data = []
        skipped_rows = 0
        
        for idx, row in df.iterrows():
            try:
                # Extract opportunity ID from HYPERLINK formula if present
                opportunity_number = str(row.get('OPPORTUNITY NUMBER', ''))
                if 'HYPERLINK' in opportunity_number and '","' in opportunity_number:
                    # Extract the opportunity ID from =HYPERLINK("url","ID") format
                    opportunity_id = opportunity_number.split('","')[1].replace('")', '')
                else:
                    opportunity_id = opportunity_number.strip()
                
                # Skip rows without valid opportunity ID
                if not opportunity_id or opportunity_id.lower() in ['nan', 'none', '', 'null']:
                    skipped_rows += 1
                    continue
                
                # Create grant object with safe string conversion
                grant = {
                    'opportunity_id': opportunity_id,
                    'opportunity_title': safe_str(row.get('OPPORTUNITY TITLE', '')),
                    'agency_code': safe_str(row.get('AGENCY CODE', '')),
                    'agency_name': safe_str(row.get('AGENCY NAME', '')),
                    'category_of_funding_activity': safe_str(row.get('CATEGORY OF FUNDING ACTIVITY', '')),
                    'funding_instrument_type': safe_str(row.get('FUNDING INSTRUMENT TYPE', '')),
                    'estimated_total_funding': safe_str(row.get('ESTIMATED TOTAL FUNDING', '')),
                    'expected_number_of_awards': safe_str(row.get('EXPECTED NUMBER OF AWARDS', '')),
                    'award_ceiling': safe_str(row.get('AWARD CEILING', '')),
                    'award_floor': safe_str(row.get('AWARD FLOOR', '')),
                    'cost_sharing_match_requirement': safe_str(row.get('COST SHARING / MATCH REQUIREMENT', '')),
                    'link_to_additional_information': safe_str(row.get('LINK TO ADDITIONAL INFORMATION', '')),
                    'grantor_contact': safe_str(row.get('GRANTOR CONTACT', '')),
                    'grantor_contact_email': safe_str(row.get('GRANTOR CONTACT EMAIL', '')),
                    'posted_date': safe_str(row.get('POSTED DATE', '')),
                    'close_date': safe_str(row.get('CLOSE DATE', '')),
                    'opportunity_status': safe_str(row.get('OPPORTUNITY STATUS', '')),
                    'funding_description': safe_str(row.get('FUNDING DESCRIPTION', ''))[:2000],
                    'eligible_applicants': safe_str(row.get('ELIGIBLE APPLICANTS', ''))
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

def safe_str(value) -> str:
    """Safely convert value to string, handling NaN and None values"""
    if pd.isna(value) or value is None:
        return ''
    return str(value).strip()

def transform_grant_to_entity(grant) -> Dict:
    """Transform grant data to Azure Table entity with optimized field mapping"""
    try:
        # Create partition key (agency code) and row key (opportunity ID)
        partition_key = safe_str(grant.get('agency_code', 'UNKNOWN'))
        row_key = safe_str(grant.get('opportunity_id', f"GRANT_{datetime.utcnow().isoformat()}"))
        
        # Clean keys for Azure Table Storage compatibility
        partition_key = clean_table_key(partition_key)
        row_key = clean_table_key(row_key)
        
        # Ensure keys are not empty and within Azure limits
        if not partition_key:
            partition_key = 'UNKNOWN'
        if not row_key:
            row_key = f"GRANT_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        
        # Create entity with optimized field lengths for Azure Table Storage
        entity = {
            'PartitionKey': partition_key[:1024],
            'RowKey': row_key[:1024],
            'OpportunityId': safe_str(grant.get('opportunity_id', ''))[:1024],
            'Title': safe_str(grant.get('opportunity_title', ''))[:1024],
            'AgencyCode': safe_str(grant.get('agency_code', ''))[:256],
            'AgencyName': safe_str(grant.get('agency_name', ''))[:512],
            'Category': safe_str(grant.get('category_of_funding_activity', ''))[:256],
            'FundingInstrumentType': safe_str(grant.get('funding_instrument_type', ''))[:256],
            'EstimatedTotalFunding': safe_str(grant.get('estimated_total_funding', ''))[:256],
            'ExpectedNumberOfAwards': safe_str(grant.get('expected_number_of_awards', ''))[:256],
            'AwardCeiling': safe_str(grant.get('award_ceiling', ''))[:256],
            'AwardFloor': safe_str(grant.get('award_floor', ''))[:256],
            'CostSharingRequired': safe_str(grant.get('cost_sharing_match_requirement', ''))[:256],
            'AdditionalInfoLink': safe_str(grant.get('link_to_additional_information', ''))[:1024],
            'GrantorContact': safe_str(grant.get('grantor_contact', ''))[:512],
            'GrantorEmail': safe_str(grant.get('grantor_contact_email', ''))[:256],
            'PostedDate': safe_str(grant.get('posted_date', ''))[:256],
            'CloseDate': safe_str(grant.get('close_date', ''))[:256],
            'Status': safe_str(grant.get('opportunity_status', ''))[:256],
            'Description': safe_str(grant.get('funding_description', ''))[:30000],  # Azure Table limit
            'EligibleApplicants': safe_str(grant.get('eligible_applicants', ''))[:10000],
            'LastUpdated': datetime.utcnow(),
            'DataSource': 'grants.gov',
            'ProcessedAt': datetime.utcnow().isoformat()
        }
        
        return entity
        
    except Exception as e:
        logging.error(f"Error transforming grant to entity: {str(e)}")
        raise

def clean_table_key(key: str) -> str:
    """Clean key for Azure Table Storage compatibility"""
    if not key:
        return ''
    
    # Remove invalid characters for Azure Table Storage
    # Valid characters: letters, digits, and certain special characters
    cleaned = ''.join(c for c in key if c.isalnum() or c in '-_.')
    
    # Ensure key doesn't start or end with invalid characters
    cleaned = cleaned.strip('-_.')
    
    return cleaned[:1024]  # Azure Table Storage key limit

def update_single_grant(table_client, request_data):
    """Update a single grant record with enhanced validation"""
    try:
        grant_data = request_data.get('grant_data', {})
        if not grant_data:
            logging.warning("No grant data provided for update")
            return
        
        entity = transform_grant_to_entity(grant_data)
        table_client.upsert_entity(entity)
        
        logging.info(f"Updated grant: {grant_data.get('opportunity_id', 'unknown')}")
        
    except Exception as e:
        logging.error(f"Error updating single grant: {str(e)}")
        raise

def cleanup_old_grants(table_client, request_data):
    """Clean up old grant records with batch deletion optimization"""
    try:
        # Define cutoff date
        cutoff_days = request_data.get('cutoff_days', 180)
        cutoff_date = datetime.utcnow() - timedelta(days=cutoff_days)
        
        logging.info(f"Cleaning up grants older than {cutoff_days} days (before {cutoff_date.isoformat()})")
        
        # Query old entities
        filter_query = f"LastUpdated lt datetime'{cutoff_date.isoformat()}'"
        old_entities = table_client.query_entities(filter_query)
        
        # Delete old entities with batch processing
        deleted_count = 0
        failed_count = 0
        
        for entity in old_entities:
            try:
                table_client.delete_entity(entity['PartitionKey'], entity['RowKey'])
                deleted_count += 1
                
                # Log progress every 100 deletions
                if deleted_count % 100 == 0:
                    logging.info(f"Deleted {deleted_count} old records so far...")
                    
            except Exception as e:
                logging.warning(f"Failed to delete entity {entity['RowKey']}: {str(e)}")
                failed_count += 1
        
        logging.info(f"Cleanup completed: {deleted_count} deleted, {failed_count} failed")
        
    except Exception as e:
        logging.error(f"Error in cleanup_old_grants: {str(e)}")
        raise