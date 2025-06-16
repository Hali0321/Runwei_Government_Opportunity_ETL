#!/usr/bin/env python3
"""
Fixed Azure Data Synchronization Script
With proper connection string handling
"""

import os
import pyodbc
from azure.data.tables import TableServiceClient
from datetime import datetime
import logging
from typing import List, Dict, Any
import time

def setup_logging():
    """Setup enhanced logging for data sync operations"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('azure_data_sync_fixed.log', encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__)

def setup_azure_environment():
    """Setup Azure environment variables with fallbacks"""
    logger = logging.getLogger(__name__)
    
    # Known working connection string from bulk_update_grantdetails.py
    fallback_connection_string = "DefaultEndpointsProtocol=https;AccountName=grantsgov225756;AccountKey=UXwW5dfy9MY9nh2BGmWhYUbzBve+6LUyT3F7+N3Cp0kWUoEk4AO3z5U6LrBYvo/VwO+Nduq2ay9E+AStKQb86Q==;EndpointSuffix=core.windows.net"
    
    # Set environment variables if not already set
    if not os.environ.get('AzureWebJobsStorage'):
        os.environ['AzureWebJobsStorage'] = fallback_connection_string
        logger.info("✅ Set AzureWebJobsStorage environment variable")
    
    if not os.environ.get('STORAGE_CONNECTION_STRING'):
        os.environ['STORAGE_CONNECTION_STRING'] = fallback_connection_string
        logger.info("✅ Set STORAGE_CONNECTION_STRING environment variable")

def get_azure_table_data_batched():
    """Fetch data from Azure Table Storage with enhanced connection handling"""
    logger = logging.getLogger(__name__)
    
    try:
        # Multiple connection string sources
        connection_string = (
            os.environ.get('AzureWebJobsStorage') or 
            os.environ.get('STORAGE_CONNECTION_STRING') or
            os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        )
        
        if not connection_string:
            logger.error("❌ No Azure Storage connection string found in environment variables")
            logger.info("Available environment variables:")
            for key in os.environ.keys():
                if 'AZURE' in key.upper() or 'STORAGE' in key.upper():
                    logger.info(f"  {key}: {str(os.environ[key])[:50]}...")
            raise Exception("No Azure Storage connection string found")
        
        logger.info(f"✅ Using connection string: {connection_string[:60]}...")
        
        # Connect to Azure Table Storage
        table_service = TableServiceClient.from_connection_string(connection_string)
        table_client = table_service.get_table_client('GrantDetails')
        
        logger.info("🔗 Connected to Azure Table Storage - GrantDetails table")
        
        # Test connection with a small query first
        try:
            test_entities = list(table_client.query_entities(
                query_filter="PartitionKey eq 'Grant'",
                results_per_page=5
            ))
            logger.info(f"✅ Connection test successful - found {len(test_entities)} sample records")
        except Exception as test_error:
            logger.error(f"❌ Connection test failed: {test_error}")
            raise test_error
        
        # Query all grant records with pagination
        all_entities = []
        page_count = 0
        
        logger.info("📥 Starting to fetch all records...")
        
        # Use pagination to handle large datasets
        pages = table_client.query_entities(
            query_filter="PartitionKey eq 'Grant'",
            results_per_page=1000
        ).by_page()
        
        for page in pages:
            page_count += 1
            entities_in_page = list(page)
            all_entities.extend(entities_in_page)
            logger.info(f"📊 Processed page {page_count}: {len(entities_in_page)} records (Total: {len(all_entities)})")
            
            # Add small delay to avoid throttling
            time.sleep(0.1)
        
        logger.info(f"✅ Retrieved {len(all_entities)} total records from Azure Table Storage")
        return all_entities
        
    except Exception as e:
        logger.error(f"❌ Error fetching Azure Table Storage data: {e}")
        return []

def sync_to_sql_database_batched(entities: List[Dict[str, Any]]):
    """Enhanced sync with batch processing and better error handling"""
    logger = logging.getLogger(__name__)
    
    try:
        # Enhanced SQL Database connection string
        sql_conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=grants-gov-sql-server.database.windows.net;"
            "Database=GrantsGovDB;"
            "Uid=grantsadmin;"
            "Pwd=Grant$Admin2024!;"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=60;"
            "CommandTimeout=120;"
        )
        
        logger.info("🔗 Connecting to Azure SQL Database...")
        
        with pyodbc.connect(sql_conn_str) as conn:
            cursor = conn.cursor()
            
            # First, ensure RawGrantsLayer1 table exists
            logger.info("🔍 Checking RawGrantsLayer1 table structure...")
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RawGrantsLayer1')
                BEGIN
                    PRINT 'RawGrantsLayer1 table does not exist. Please run the schema update script first.'
                    RAISERROR('RawGrantsLayer1 table missing', 16, 1)
                END
            """)
            
            # Clear existing data for fresh sync
            logger.info("🗑️ Clearing existing RawGrantsLayer1 data...")
            cursor.execute("DELETE FROM RawGrantsLayer1")
            rows_deleted = cursor.rowcount
            conn.commit()
            logger.info(f"✅ Cleared {rows_deleted} existing records")
            
            # Enhanced insert query with error handling
            insert_query = """
                INSERT INTO RawGrantsLayer1 (
                    PartitionKey, RowKey, OpportunityNumber, Title, AgencyCode, AgencyName,
                    Category, CategoryExplanation, FundingType, CFDANumbers, EstimatedTotalFunding,
                    ExpectedAwards, AwardCeiling, AwardFloor, CostSharing, AdditionalInfoURL,
                    GrantorContact, GrantorPhone, GrantorEmail, EstimatedPostDate, EstimatedDueDate,
                    PostedDate, CloseDate, LastUpdatedOriginal, Version, Status, Package,
                    SynopsisArchived, Description, EligibleApplicants, ProcessedDate,
                    ProcessingTimestamp, SourceType, TotalColumns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Process in batches for better performance
            batch_size = 100
            synced_count = 0
            failed_count = 0
            
            logger.info(f"📤 Starting to sync {len(entities)} entities in batches of {batch_size}...")
            
            for i in range(0, len(entities), batch_size):
                batch = entities[i:i + batch_size]
                batch_success = 0
                
                for entity_index, entity in enumerate(batch):
                    try:
                        # Enhanced data mapping with null handling
                        values = (
                            str(entity.get('PartitionKey', 'Grant'))[:255],
                            str(entity.get('RowKey', ''))[:255],
                            str(entity.get('OpportunityNumber', ''))[:255],
                            str(entity.get('Title', ''))[:1000],
                            str(entity.get('AgencyCode', ''))[:100],
                            str(entity.get('AgencyName', ''))[:500],
                            str(entity.get('Category', ''))[:500],
                            str(entity.get('CategoryExplanation', ''))[:2000],
                            str(entity.get('FundingType', ''))[:255],
                            str(entity.get('CFDANumbers', ''))[:500],
                            _safe_decimal(entity.get('EstimatedTotalFunding')),
                            _safe_int(entity.get('ExpectedAwards')),
                            _safe_decimal(entity.get('AwardCeiling')),
                            _safe_decimal(entity.get('AwardFloor')),
                            str(entity.get('CostSharing', ''))[:500],
                            str(entity.get('AdditionalInfoURL', ''))[:2000],
                            str(entity.get('GrantorContact', ''))[:500],
                            str(entity.get('GrantorPhone', ''))[:100],
                            str(entity.get('GrantorEmail', ''))[:255],
                            _safe_datetime(entity.get('EstimatedPostDate')),
                            _safe_datetime(entity.get('EstimatedDueDate')),
                            _safe_datetime(entity.get('PostedDate')),
                            _safe_datetime(entity.get('CloseDate')),
                            _safe_datetime(entity.get('LastUpdatedOriginal')),
                            str(entity.get('Version', ''))[:50],
                            str(entity.get('Status', ''))[:100],
                            str(entity.get('Package', ''))[:500],
                            str(entity.get('SynopsisArchived', ''))[:50],
                            str(entity.get('Description', '')) if entity.get('Description') else None,
                            str(entity.get('EligibleApplicants', '')) if entity.get('EligibleApplicants') else None,
                            _safe_datetime(entity.get('ProcessedDate')),
                            str(entity.get('ProcessingTimestamp', ''))[:50],
                            str(entity.get('SourceType', 'Azure_Table_Storage'))[:50],
                            int(entity.get('TotalColumns', 28))
                        )
                        
                        cursor.execute(insert_query, values)
                        batch_success += 1
                        synced_count += 1
                        
                    except Exception as e:
                        failed_count += 1
                        entity_id = entity.get('RowKey', f'Index_{entity_index}')
                        logger.warning(f"⚠️ Failed to sync entity {entity_id}: {e}")
                        continue
                
                # Commit batch
                conn.commit()
                batch_number = i//batch_size + 1
                total_batches = (len(entities) + batch_size - 1) // batch_size
                logger.info(f"✅ Batch {batch_number}/{total_batches}: Synced {batch_success}/{len(batch)} records")
            
            logger.info(f"🎉 Sync completed: {synced_count} successful, {failed_count} failed")
            return synced_count
            
    except Exception as e:
        logger.error(f"❌ Error syncing to SQL Database: {e}")
        return 0

def _safe_decimal(value):
    """Safely convert to decimal"""
    if value is None or value == '':
        return None
    try:
        return float(str(value).replace(',', '').replace('$', ''))
    except (ValueError, TypeError):
        return None

def _safe_int(value):
    """Safely convert to integer"""
    if value is None or value == '':
        return None
    try:
        return int(float(str(value).replace(',', '')))
    except (ValueError, TypeError):
        return None

def _safe_datetime(value):
    """Safely convert to datetime"""
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value
    try:
        if isinstance(value, str):
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%m/%d/%Y', '%Y-%m-%dT%H:%M:%S']:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
    except Exception:
        pass
    return None

def verify_sync_results():
    """Verify the sync results and show summary"""
    logger = logging.getLogger(__name__)
    
    try:
        sql_conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=grants-gov-sql-server.database.windows.net;"
            "Database=GrantsGovDB;"
            "Uid=grantsadmin;"
            "Pwd=Grant$Admin2024!;"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        
        with pyodbc.connect(sql_conn_str) as conn:
            cursor = conn.cursor()
            
            # Get summary statistics
            cursor.execute("""
                SELECT 
                    COUNT(*) as TotalRecords,
                    COUNT(DISTINCT AgencyCode) as UniqueAgencies,
                    COUNT(DISTINCT Status) as UniqueStatuses,
                    MAX(ProcessedDate) as LatestProcessed,
                    COUNT(CASE WHEN AwardCeiling > 0 THEN 1 END) as RecordsWithFunding
                FROM RawGrantsLayer1
            """)
            
            results = cursor.fetchone()
            
            print(f"\n📊 RawGrantsLayer1 Sync Summary:")
            print(f"   Total Records: {results[0]:,}")
            print(f"   Unique Agencies: {results[1]}")
            print(f"   Unique Statuses: {results[2]}")
            print(f"   Latest Processed: {results[3]}")
            print(f"   Records with Funding: {results[4]:,}")
            
            return True
            
    except Exception as e:
        logger.error(f"Error verifying sync results: {e}")
        return False

def main():
    """Fixed main synchronization workflow"""
    logger = setup_logging()
    
    print("🔄 Starting Fixed Azure Data Synchronization")
    print("=" * 60)
    print(f"📅 Sync Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Source: Azure Table Storage (GrantDetails)")
    print("📍 Destination: Azure SQL Database (RawGrantsLayer1)")
    print("=" * 60)
    
    # Step 0: Setup Azure environment
    setup_azure_environment()
    
    # Step 1: Fetch data from Azure Table Storage
    print("\n📥 Fetching data from Azure Table Storage...")
    entities = get_azure_table_data_batched()
    
    if not entities:
        print("❌ No data found in Azure Table Storage")
        print("💡 Try running the data refresh script first:")
        print("   python src/scripts/bulk_update_grantdetails.py")
        return False
    
    print(f"✅ Found {len(entities):,} records in Azure Table Storage")
    
    # Step 2: Sync to SQL Database
    print(f"\n📤 Syncing {len(entities):,} records to RawGrantsLayer1...")
    synced_count = sync_to_sql_database_batched(entities)
    
    if synced_count == 0:
        print("❌ Failed to sync data to SQL Database")
        return False
    
    print(f"✅ Successfully synced {synced_count:,} records to RawGrantsLayer1")
    
    # Step 3: Verify results
    print("\n🔍 Verifying sync results...")
    if verify_sync_results():
        print("✅ Sync verification completed")
    
    print(f"\n🎉 Azure Data Synchronization Completed Successfully!")
    print(f"📊 Data Flow: Azure Table Storage ({len(entities):,}) → RawGrantsLayer1 ({synced_count:,})")
    print("\n🚀 RawGrantsLayer1 is now updated with your latest data!")
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)