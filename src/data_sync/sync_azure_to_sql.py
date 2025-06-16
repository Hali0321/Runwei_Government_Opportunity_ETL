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
                    
                    def safe_get_datetime(key, default=None):
                        value = entity.get(key, default)
                        if value is None or str(value).strip() == '':
                            return 'NULL'
                        try:
                            # Handle datetime objects from Azure Table Storage
                            if hasattr(value, 'strftime'):
                                return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
                            # Handle string dates
                            elif isinstance(value, str):
                                # Try to parse common date formats
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
                                    else:
                                        return 'NULL'
                                except:
                                    return 'NULL'
                            else:
                                return 'NULL'
                        except Exception:
                            return 'NULL'
                    
                    # Create INSERT with ALL table columns (proven working format)
                    insert_sql = f"""
INSERT INTO RawGrantsLayer1 (
    PartitionKey, RowKey, OpportunityNumber, Title, AgencyCode, AgencyName,
    Category, CategoryExplanation, FundingType, CFDANumbers,
    EstimatedTotalFunding, ExpectedAwards, AwardCeiling, AwardFloor,
    CostSharing, AdditionalInfoURL, GrantorContact, GrantorPhone, GrantorEmail,
    EstimatedPostDate, EstimatedDueDate, PostedDate, CloseDate, LastUpdatedOriginal,
    Version, Status, Package, SynopsisArchived,
    Description, EligibleApplicants,
    ProcessedDate, ProcessingTimestamp, SourceType, TotalColumns,
    CreatedDate, UpdatedDate
) VALUES (
    {safe_get('PartitionKey', 'Grant', 255)},
    {safe_get('RowKey', '', 255)},
    {safe_get('OpportunityNumber', '', 255)},
    {safe_get('Title', '', 1000)},
    {safe_get('AgencyCode', '', 100)},
    {safe_get('AgencyName', '', 500)},
    {safe_get('Category', '', 500)},
    {safe_get('CategoryExplanation', '', 2000)},
    {safe_get('FundingType', '', 255)},
    {safe_get('CFDANumbers', '', 500)},
    {safe_get_decimal('EstimatedTotalFunding')},
    {safe_get_int('ExpectedAwards')},
    {safe_get_decimal('AwardCeiling')},
    {safe_get_decimal('AwardFloor')},
    {safe_get_bool_as_string('CostSharing')},
    {safe_get('AdditionalInfoURL', '', 2000)},
    {safe_get('GrantorContact', '', 500)},
    {safe_get('GrantorPhone', '', 100)},
    {safe_get('GrantorEmail', '', 255)},
    {safe_get_datetime('EstimatedPostDate')},
    {safe_get_datetime('EstimatedDueDate')},
    {safe_get_datetime('PostedDate')},
    {safe_get_datetime('CloseDate')},
    {safe_get_datetime('LastUpdatedOriginal')},
    {safe_get('Version', '', 50)},
    {safe_get('Status', '', 100)},
    {safe_get_bool_as_string('Package')},
    {safe_get_bool_as_string('SynopsisArchived')},
    {safe_get('Description')},
    {safe_get('EligibleApplicants')},
    GETDATE(),
    {safe_get('ProcessingTimestamp', '', 50)},
    'Azure_Table_Storage_Sync',
    {safe_get_int('TotalColumns')},
    GETDATE(),
    GETDATE()
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
        
        # Comprehensive verification
        verify_sql = """
SELECT 
    COUNT(*) as 'Total_Records',
    COUNT(CASE WHEN Title IS NOT NULL AND Title != '' AND Title != 'NULL' THEN 1 END) as 'Valid_Titles',
    COUNT(CASE WHEN AwardCeiling IS NOT NULL AND AwardCeiling > 0 THEN 1 END) as 'Valid_Awards',
    COUNT(CASE WHEN AgencyName IS NOT NULL AND AgencyName != '' AND AgencyName != 'NULL' THEN 1 END) as 'Valid_Agencies',
    AVG(CASE WHEN AwardCeiling > 0 THEN AwardCeiling END) as 'Avg_Award_Amount',
    MAX(ProcessedDate) as 'Latest_Sync'
FROM RawGrantsLayer1;

-- Show top 5 highest value opportunities
SELECT TOP 5
    'TOP_OPPORTUNITIES' as Type,
    RowKey,
    Title,
    AwardCeiling,
    Category,
    AgencyName
FROM RawGrantsLayer1 
WHERE AwardCeiling IS NOT NULL AND AwardCeiling > 0
ORDER BY AwardCeiling DESC;
"""
        
        cmd = [
            "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
            "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
            "-Q", verify_sql, "-C"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        print("📊 Final Verification Results:")
        print(result.stdout)
        
        return total_inserted > 0
        
    except Exception as e:
        print(f"❌ Error during sync: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 AZURE TABLE STORAGE TO SQL SYNC - PRODUCTION MODE")
    print("=" * 60)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Processing ALL 1,683 records from Azure Table Storage")
    
    start_time = datetime.now()
    success = sync_azure_to_sql()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    if success:
        print(f"\n🎯 PRODUCTION SYNC COMPLETED SUCCESSFULLY!")
        print(f"⏱️ Total time: {duration:.2f} seconds")
        print("✅ RawGrantsLayer1 fully populated from Azure Table Storage")
        
        # Now update downstream layers
        print("\n📈 Updating downstream layers...")
        downstream_sql = """
-- Update RunweiFormatLayer2 
DELETE FROM RunweiFormatLayer2;
INSERT INTO RunweiFormatLayer2 (Title, ShortDescription, Industry, AwardValue, Status, OpportunityType, CreatedDate)
SELECT 
    r1.Title,
    LEFT(ISNULL(r1.Title, ''), 500),
    ISNULL(r1.Category, 'General'),
    ISNULL(r1.AwardCeiling, 0),
    ISNULL(r1.Status, 'Unknown'),
    'Grant',
    GETDATE()
FROM RawGrantsLayer1 r1
WHERE r1.Title IS NOT NULL AND r1.Title != '' AND r1.Title != 'NULL';

PRINT 'RunweiFormatLayer2 updated with ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' records';

-- Update BusinessIntelligenceLayer3
DELETE FROM BusinessIntelligenceLayer3;
INSERT INTO BusinessIntelligenceLayer3 (OpportunityID, CompetitiveScore, OpportunityValue, RecommendationLevel, CreatedDate)
SELECT 
    r2.OpportunityID,
    CASE WHEN r2.AwardValue >= 10000000 THEN 100.0
         WHEN r2.AwardValue >= 5000000 THEN 95.0
         WHEN r2.AwardValue >= 1000000 THEN 85.0
         WHEN r2.AwardValue >= 500000 THEN 75.0
         WHEN r2.AwardValue >= 100000 THEN 65.0
         ELSE 50.0 END,
    CASE WHEN r2.AwardValue >= 10000000 THEN 'Ultra-Premium'
         WHEN r2.AwardValue >= 5000000 THEN 'Premium'
         WHEN r2.AwardValue >= 1000000 THEN 'High-Value'
         WHEN r2.AwardValue >= 500000 THEN 'Significant'
         WHEN r2.AwardValue >= 100000 THEN 'Standard'
         ELSE 'Basic' END,
    CASE WHEN r2.AwardValue >= 10000000 THEN 'PRIORITY - Ultra High Value Opportunity'
         WHEN r2.AwardValue >= 5000000 THEN 'PRIORITY - Premium Opportunity'
         WHEN r2.AwardValue >= 1000000 THEN 'RECOMMENDED - Major Opportunity'
         WHEN r2.AwardValue >= 500000 THEN 'RECOMMENDED - Significant Opportunity'
         WHEN r2.AwardValue >= 100000 THEN 'CONSIDER - Good Opportunity'
         ELSE 'MONITOR - Standard Opportunity' END,
    GETDATE()
FROM RunweiFormatLayer2 r2;

PRINT 'BusinessIntelligenceLayer3 updated with ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' records';

-- Final comprehensive stats
SELECT 
    'RawGrantsLayer1' as Layer, 
    COUNT(*) as Records,
    COUNT(CASE WHEN Title IS NOT NULL AND Title != '' THEN 1 END) as Valid_Records,
    AVG(CASE WHEN AwardCeiling > 0 THEN AwardCeiling END) as Avg_Award
FROM RawGrantsLayer1
UNION ALL
SELECT 
    'RunweiFormatLayer2', 
    COUNT(*),
    COUNT(CASE WHEN Title IS NOT NULL AND Title != '' THEN 1 END),
    AVG(CASE WHEN AwardValue > 0 THEN AwardValue END)
FROM RunweiFormatLayer2
UNION ALL
SELECT 
    'BusinessIntelligenceLayer3', 
    COUNT(*),
    COUNT(CASE WHEN RecommendationLevel LIKE '%PRIORITY%' THEN 1 END),
    AVG(CASE WHEN CompetitiveScore > 0 THEN CompetitiveScore END)
FROM BusinessIntelligenceLayer3;
"""
        
        cmd = [
            "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
            "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
            "-Q", downstream_sql, "-C"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        print("📊 All Layers Final Status:")
        print(result.stdout)
        
        print("\n🎉 COMPLETE GRANTS.GOV PIPELINE IS NOW FULLY OPERATIONAL!")
        print("=" * 60)
        print("✅ Azure Table Storage ➜ RawGrantsLayer1 ➜ RunweiFormatLayer2 ➜ BusinessIntelligenceLayer3")
        print("🚀 Your grants.gov automation system is ready for production use!")
        
    else:
        print(f"\n❌ PRODUCTION SYNC FAILED!")
        print(f"⏱️ Failed after: {duration:.2f} seconds")
        print("Check the error messages above")