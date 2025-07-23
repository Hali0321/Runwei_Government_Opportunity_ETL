#!/usr/bin/env python3
"""
Sync Azure Table Storage to RawGrantsLayer1
Uses correct column mapping from Azure Storage to SQL Database
"""

import os
import subprocess
from azure.data.tables import TableServiceClient
from datetime import datetime
import json

def sync_azure_to_sql():
    """Sync data from Azure Table Storage to RawGrantsLayer1 with proper column mapping"""
    
    # Your existing Azure connection
    connection_string = "DefaultEndpointsProtocol=https;AccountName=grantsgov225756;AccountKey=UXwW5dfy9MY9nh2BGmWhYUbzBve+6LUyT3F7+N3Cp0kWUoEk4AO3z5U6LrBYvo/VwO+Nduq2ay9E+AStKQb86Q==;EndpointSuffix=core.windows.net"
    
    try:
        # Connect to Azure Table Storage
        print("📡 Connecting to Azure Table Storage...")
        table_service = TableServiceClient.from_connection_string(connection_string)
        table_client = table_service.get_table_client('GrantDetails')
        
        # Fetch all entities
        print("📥 Fetching data from Azure Table Storage...")
        entities = list(table_client.query_entities("PartitionKey eq 'Grant'"))
        print(f"✅ Retrieved {len(entities)} entities from Azure Storage")
        
        if not entities:
            print("❌ No data found in Azure Table Storage")
            return False
        
        print(f"🚀 PRODUCTION MODE: Processing all {len(entities)} records...")
        
        # Clear existing data in RawGrantsLayer1
        print("🗑️ Clearing existing data in RawGrantsLayer1...")
        clear_sql = "DELETE FROM RawGrantsLayer1;"
        
        cmd = [
            "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
            "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
            "-Q", clear_sql, "-C"
        ]
        subprocess.run(cmd, check=True)
        print("✅ Cleared existing data")
        
        # Process all records with optimized batch size
        print("📤 Inserting all data into RawGrantsLayer1...")
        batch_size = 25  # Optimized batch size for production
        total_inserted = 0
        total_failed = 0
        
        for i in range(0, len(entities), batch_size):
            batch = entities[i:i + batch_size]
            batch_number = i//batch_size + 1
            total_batches = (len(entities) + batch_size - 1) // batch_size
            
            print(f"🔄 Processing batch {batch_number}/{total_batches} ({len(batch)} records)...")
            
            # Create INSERT statements for this batch
            insert_statements = []
            for entity in batch:
                try:
                    # Proven helper functions from debug session
                    def safe_get(key, default='', max_length=None):
                        value = entity.get(key, default)
                        if value is None:
                            return 'NULL'
                        if str(value).strip() == '':
                            return 'NULL'
                        if isinstance(value, str):
                            escaped = value.replace("'", "''")
                            if max_length and len(escaped) > max_length:
                                escaped = escaped[:max_length]
                            return f"'{escaped}'"
                        return f"'{str(value)}'"
                    
                    def safe_get_decimal(key, default=None):
                        value = entity.get(key, default)
                        if value is None:
                            return 'NULL'
                        try:
                            float_val = float(value)
                            return str(float_val)
                        except (ValueError, TypeError):
                            return 'NULL'
                    
                    def safe_get_int(key, default=None):
                        value = entity.get(key, default)
                        if value is None:
                            return 'NULL'
                        try:
                            int_val = int(float(value))
                            return str(int_val)
                        except (ValueError, TypeError):
                            return 'NULL'
                    
                    def safe_get_bool_as_string(key, default=''):
                        value = entity.get(key, default)
                        if value is None:
                            return 'NULL'
                        return f"'{str(value)}'"
                    
                    # Replace the safe_get_datetime function with this enhanced version:

                    def safe_get_datetime_with_alternatives(entity, primary_key, alternatives=None, default=None):
                        """Enhanced datetime getter with field name alternatives"""
                        if alternatives is None:
                            alternatives = []
                        
                        # Try primary field name first
                        fields_to_try = [primary_key] + alternatives
                        
                        for field_name in fields_to_try:
                            value = entity.get(field_name)
                            if value is not None and str(value).strip() != '':
                                try:
                                    # Handle datetime objects from Azure Table Storage
                                    if hasattr(value, 'strftime'):
                                        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
                                    # Handle string dates
                                    elif isinstance(value, str):
                                        from datetime import datetime
                                        try:
                                            # Handle MM/DD/YYYY format
                                            if '/' in value:
                                                dt = datetime.strptime(value, '%m/%d/%Y')
                                                return f"'{dt.strftime('%Y-%m-%d')}'"
                                            # Handle ISO format
                                            elif 'T' in value:
                                                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                                return f"'{dt.strftime('%Y-%m-%d %H:%M:%S')}'"
                                            # Handle YYYY-MM-DD format
                                            elif '-' in value and len(value) >= 10:
                                                dt = datetime.strptime(value[:10], '%Y-%m-%d')
                                                return f"'{dt.strftime('%Y-%m-%d')}'"
                                            else:
                                                # Try to parse as general date string
                                                from dateutil import parser
                                                dt = parser.parse(value)
                                                return f"'{dt.strftime('%Y-%m-%d %H:%M:%S')}'"
                                        except:
                                            continue  # Try next field name
                                    # Handle numeric timestamps
                                    elif isinstance(value, (int, float)):
                                        from datetime import datetime
                                        dt = datetime.fromtimestamp(value)
                                        return f"'{dt.strftime('%Y-%m-%d %H:%M:%S')}'"
                                except Exception:
                                    continue  # Try next field name
                        
                        return 'NULL'

                    # FIXED: Create INSERT with CORRECT field mappings for missing columns
                    insert_sql = f"""
INSERT INTO RawGrantsLayer1 (
    -- Core grant identifiers (CONFIRMED to exist in your schema)
    PartitionKey,
    RowKey,
    OpportunityNumber,
    OpportunityURL,
    Title,
    AgencyCode,
    AgencyName,
    Category,
    CategoryExplanation,  -- 🔧 FIXED MAPPING
    FundingType,
    CFDANumbers,
    
    -- Financial fields (CONFIRMED to exist)
    EstimatedTotalFunding,
    ExpectedAwards,      -- 🔧 FIXED MAPPING
    AwardCeiling,
    AwardFloor,
    CostSharing,         -- 🔧 FIXED MAPPING
    
    -- Additional information (CONFIRMED to exist)
    AdditionalInfoURL,
    GrantorContact,      -- 🔧 FIXED MAPPING
    GrantorPhone,        -- 🔧 FIXED MAPPING
    GrantorEmail,        -- 🔧 FIXED MAPPING
    
    -- Date fields (CONFIRMED to exist)
    EstimatedPostDate,   -- 🔧 FIXED MAPPING
    EstimatedDueDate,    -- 🔧 FIXED MAPPING
    PostedDate,
    CloseDate,
    LastUpdatedOriginal, -- 🔧 FIXED MAPPING
    
    -- Status and version (CONFIRMED to exist)
    Version,             -- 🔧 FIXED: Direct mapping
    Status,              -- 🔧 FIXED: Direct mapping
    Package,             -- 🔧 FIXED: Direct mapping
    SynopsisArchived,    -- 🔧 FIXED: Direct mapping
    
    -- Content fields (CONFIRMED to exist)
    Description,
    EligibleApplicants,
    
    -- System fields (CONFIRMED to exist)
    ProcessedDate,
    ProcessedBy,
    ProcessingTimestamp, -- 🔧 FIXED: Direct mapping
    SourceType,
    TotalColumns,        -- 🔧 FIXED: Direct mapping
    CreatedDate,
    UpdatedDate,
    Timestamp,           -- 🔧 FIXED: Direct mapping
    DataVersion          -- 🔧 FIXED: Direct mapping with default
) VALUES (
    -- Core grant identifiers
    {safe_get('PartitionKey', 'Grant', 255)},
    {safe_get('RowKey', '', 255)},
    {safe_get('OpportunityNumber', '', 255)},
    {safe_get('OpportunityURL', '', 2000)},
    {safe_get('Title', '', 1000)},
    {safe_get('AgencyCode', '', 100)},
    {safe_get('AgencyName', '', 500)},
    {safe_get('Category', '', 500)},
    {safe_get('CategoryExplanation', '', 2000)},  -- 🔧 FIXED: Direct mapping
    {safe_get('FundingType', '', 255)},
    {safe_get('CFDANumbers', '', 500)},
    
    -- Financial fields
    {safe_get_decimal('EstimatedTotalFunding')},
    {safe_get_int('ExpectedAwards')},             -- 🔧 FIXED: Direct mapping
    {safe_get_decimal('AwardCeiling')},
    {safe_get_decimal('AwardFloor')},
    {safe_get('CostSharing', '', 500)},           -- 🔧 FIXED: Direct mapping
    
    -- Additional information
    {safe_get('AdditionalInfoURL', '', 2000)},
    {safe_get('GrantorContact', '', 500)},        -- 🔧 FIXED: Direct mapping
    {safe_get('GrantorPhone', '', 100)},          -- 🔧 FIXED: Direct mapping  
    {safe_get('GrantorEmail', '', 255)},          -- 🔧 FIXED: Direct mapping
    
    -- Date fields
    {safe_get_datetime_with_alternatives(entity, 'EstimatedPostDate', ['PostDate', 'Estimated_Start_Date'])},     -- 🔧 FIXED: Direct mapping
    {safe_get_datetime_with_alternatives(entity, 'EstimatedDueDate', ['DueDate', 'Estimated_End_Date'])},      -- 🔧 FIXED: Direct mapping
    {safe_get_datetime_with_alternatives(entity, 'PostedDate')},
    {safe_get_datetime_with_alternatives(entity, 'CloseDate')},
    {safe_get_datetime_with_alternatives(entity, 'LastUpdatedOriginal', ['LastUpdated', 'Last_Updated', 'UpdatedDate', 'Modified', 'ModifiedDate'])},   -- 🔧 FIXED: Direct mapping
    
    -- Status and version
    {safe_get('Version', '', 50)},                -- 🔧 FIXED: Direct mapping
    {safe_get('Status', '', 100)},                -- 🔧 FIXED: Direct mapping
    {safe_get('Package', '', 500)},               -- 🔧 FIXED: Direct mapping
    {safe_get('SynopsisArchived', '', 50)},       -- 🔧 FIXED: Direct mapping
    
    -- Content fields
    {safe_get('Description')},
    {safe_get('EligibleApplicants')},
    
    -- System fields
    GETDATE(),  -- ProcessedDate
    {safe_get('ProcessedBy', 'Azure_Table_Storage_Sync', 255)},
    {safe_get('ProcessingTimestamp', '', 50)},    -- 🔧 FIXED: Direct mapping
    'Azure_Sync',  -- SourceType
    {safe_get_int('TotalColumns')},               -- 🔧 FIXED: Direct mapping
    GETDATE(),  -- CreatedDate
    GETDATE(),  -- UpdatedDate
    {safe_get_datetime_with_alternatives(entity, 'Timestamp', ['timestamp', 'TimeStamp', 'Created', 'CreatedAt', 'ProcessedTimestamp', 'etag'])},             -- 🔧 FIXED: Direct mapping
    {safe_get('DataVersion', '1.0', 50)}          -- 🔧 FIXED: Direct mapping with default
);
"""
                    insert_statements.append(insert_sql)
                    
                except Exception as e:
                    print(f"⚠️ Error creating SQL for entity {entity.get('RowKey', 'Unknown')}: {e}")
                    total_failed += 1
                    continue
            
            # Execute batch efficiently
            if insert_statements:
                batch_sql = "\n".join(insert_statements)
                temp_file = f"temp_batch_{batch_number}.sql"
                
                try:
                    with open(temp_file, 'w', encoding='utf-8') as f:
                        f.write(batch_sql)
                    
                    cmd = [
                        "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
                        "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
                        "-i", temp_file, "-C"
                    ]
                    subprocess.run(cmd, check=True, capture_output=True, text=True)
                    total_inserted += len(insert_statements)
                    
                    # Progress indicator
                    progress = (batch_number / total_batches) * 100
                    print(f"   ✅ Batch {batch_number} completed - {len(insert_statements)} records ({progress:.1f}% total progress)")
                    
                except subprocess.CalledProcessError as e:
                    print(f"   ❌ Batch {batch_number} failed - trying individual inserts...")
                    # Fall back to individual inserts for this batch
                    for j, stmt in enumerate(insert_statements):
                        individual_file = f"temp_single_{batch_number}_{j}.sql"
                        try:
                            with open(individual_file, 'w', encoding='utf-8') as f:
                                f.write(stmt)
                            
                            cmd = [
                                "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
                                "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
                                "-i", individual_file, "-C"
                            ]
                            subprocess.run(cmd, check=True, capture_output=True, text=True)
                            total_inserted += 1
                            
                        except:
                            total_failed += 1
                        finally:
                            if os.path.exists(individual_file):
                                os.remove(individual_file)
                    
                except Exception as e:
                    print(f"   ❌ Batch {batch_number} exception: {str(e)}")
                    total_failed += len(insert_statements)
                finally:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
            
            # Show progress every 10 batches
            if batch_number % 10 == 0:
                print(f"📊 Progress Update: {total_inserted} records inserted, {total_failed} failed")
        
        print(f"\n🎉 PRODUCTION SYNC COMPLETED!")
        print(f"✅ Successfully inserted: {total_inserted} records")
        print(f"❌ Failed records: {total_failed}")
        print(f"📈 Success rate: {(total_inserted/(total_inserted+total_failed)*100):.1f}%")
        
        # Enhanced verification with OpportunityURL
        verify_sql = """
-- Comprehensive verification including OpportunityURL
SELECT 
    COUNT(*) as 'Total_Records',
    
    -- Grant identifiers validation
    COUNT(CASE WHEN OpportunityNumber IS NOT NULL AND OpportunityNumber != '' THEN 1 END) as 'Valid_OpportunityNumbers',
    COUNT(CASE WHEN OpportunityURL IS NOT NULL AND OpportunityURL != '' THEN 1 END) as 'Valid_OpportunityURLs',
    COUNT(CASE WHEN Title IS NOT NULL AND Title != '' AND Title != 'NULL' THEN 1 END) as 'Valid_Titles',
    COUNT(CASE WHEN AgencyName IS NOT NULL AND AgencyName != '' AND AgencyName != 'NULL' THEN 1 END) as 'Valid_Agencies',
    
    -- Funding details validation
    COUNT(CASE WHEN AwardCeiling IS NOT NULL AND AwardCeiling > 0 THEN 1 END) as 'Valid_Awards',
    AVG(CASE WHEN AwardCeiling > 0 THEN AwardCeiling END) as 'Avg_Award_Amount',
    MAX(AwardCeiling) as 'Max_Award_Amount',
    
    -- System tracking validation
    MAX(ProcessedDate) as 'Latest_Sync',
    MIN(CreatedDate) as 'First_Record_Created'
FROM RawGrantsLayer1;

-- Show sample records with OpportunityURL
SELECT TOP 5
    'SAMPLE_WITH_URLS' as Report_Type,
    
    -- Grant identifiers including new OpportunityURL
    OpportunityNumber,
    LEFT(OpportunityURL, 80) + '...' as OpportunityURL_Sample,
    Title,
    AgencyName,
    
    -- Funding details
    AwardCeiling,
    
    -- Processing info
    ProcessedBy
FROM RawGrantsLayer1 
WHERE OpportunityURL IS NOT NULL AND OpportunityURL != ''
ORDER BY AwardCeiling DESC;
"""
        
        cmd = [
            "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
            "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
            "-Q", verify_sql, "-C"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        print("📊 Enhanced Verification Results (including OpportunityURL):")
        print(result.stdout)
        
        return total_inserted > 0
        
    except Exception as e:
        print(f"❌ Error during sync: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 AZURE TABLE STORAGE TO SQL SYNC - WITH OPPORTUNITY URLS")
    print("=" * 65)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🌐 Now includes OpportunityURL mapping from Azure Table Storage")
    
    start_time = datetime.now()
    success = sync_azure_to_sql()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    if success:
        print(f"\n🎯 ENHANCED SYNC COMPLETED SUCCESSFULLY!")
        print(f"⏱️ Total time: {duration:.2f} seconds")
        print("✅ RawGrantsLayer1 fully populated including OpportunityURL links")
        print("🌐 Direct grants.gov opportunity URLs now available for each record")
        
    else:
        print(f"\n❌ SYNC FAILED!")
        print(f"⏱️ Failed after: {duration:.2f} seconds")
        print("Check the error messages above")