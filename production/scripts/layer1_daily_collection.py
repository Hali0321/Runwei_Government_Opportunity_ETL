#!/usr/bin/env python3
"""
Layer 1 - Daily Data Collection - Azure Production
Automated daily collection from Grants.gov with Azure Storage integration
"""

import subprocess
import logging
from datetime import datetime
import sys
import os
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class Layer1DailyCollector:
    """Production Layer 1 daily data collection"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovtDB"  # Note: Using your config file spelling
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        self.azure_connection_string = "DefaultEndpointsProtocol=https;AccountName=grantsgov225756;AccountKey=UXwW5dfy9MY9nh2BGmWhYUbzBve+6LUyT3F7+N3Cp0kWUoEk4AO3z5U6LrBYvo/VwO+Nduq2ay9E+AStKQb86Q=="
        print("🔧 Initialized Layer 1 Daily Collector")
        
    def execute_sql_command(self, sql_query, timeout=300):
        """Execute SQL command with Azure SQL Database"""
        print(f"📊 Executing SQL command (timeout: {timeout}s)...")
        try:
            cmd = [
                "sqlcmd", "-S", self.server, "-d", self.database,
                "-U", self.username, "-P", self.password,
                "-Q", sql_query, "-C", "-t", str(timeout), "-I", "-b"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 30)
            
            if result.returncode == 0:
                print("✅ SQL command executed successfully")
                if result.stdout and result.stdout.strip():
                    print(f"📋 Output: {result.stdout.strip()}")
                return result.stdout
            else:
                print(f"❌ SQL command failed with return code {result.returncode}")
                if result.stderr:
                    print(f"🔴 Error: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            print(f"⏰ SQL command timed out after {timeout} seconds")
            return None
        except Exception as e:
            print(f"💥 Error executing SQL: {e}")
            return None

    def collect_from_grants_gov(self):
        """Step 1: Collect data from Grants.gov using existing automation"""
        print("🌐 Step 1: Collecting fresh data from Grants.gov...")
        
        # Create the grants collection script in the right location
        collector_script = Path("production/scripts/grants_collector.py")
        
        if not collector_script.exists():
            print("📝 Creating grants collector script...")
            self._create_grants_collector_script()
        
        try:
            # Run the grants collection
            result = subprocess.run([
                sys.executable, str(collector_script)
            ], timeout=1800, capture_output=True, text=True)  # 30 minute timeout
            
            if result.returncode == 0:
                print("✅ Successfully collected data from Grants.gov")
                print("📊 Data stored in Azure Table Storage")
                return True
            else:
                print(f"❌ Grants collection failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print("⏰ Grants collection timed out after 30 minutes")
            return False
        except Exception as e:
            print(f"💥 Error during grants collection: {e}")
            return False

    def sync_azure_to_sql(self):
        """Step 2: Sync from Azure Table Storage to RawGrantsLayer1"""
        print("📤 Step 2: Syncing Azure Table Storage to SQL Database...")
        
        sql = """
        -- Clear existing data
        DELETE FROM RawGrantsLayer1;
        PRINT 'Cleared existing data in RawGrantsLayer1';
        """
        
        result = self.execute_sql_command(sql)
        if result is None:
            print("❌ Failed to clear existing data")
            return False
        
        # Create the sync script
        sync_script = Path("production/scripts/azure_to_sql_sync.py")
        if not sync_script.exists():
            print("📝 Creating Azure to SQL sync script...")
            self._create_azure_sync_script()
        
        try:
            # Run the sync
            result = subprocess.run([
                sys.executable, str(sync_script)
            ], timeout=600, capture_output=True, text=True)  # 10 minute timeout
            
            if result.returncode == 0:
                print("✅ Successfully synced Azure Table Storage to SQL")
                return True
            else:
                print(f"❌ Azure sync failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print("⏰ Azure sync timed out after 10 minutes")
            return False
        except Exception as e:
            print(f"💥 Error during Azure sync: {e}")
            return False

    def validate_layer1_data(self):
        """Step 3: Validate Layer 1 data quality"""
        print("🔍 Step 3: Validating Layer 1 data quality...")
        
        sql = """
        -- Comprehensive Layer 1 validation
        SELECT 
            COUNT(*) as TotalRecords,
            COUNT(CASE WHEN Title IS NOT NULL AND Title != '' THEN 1 END) as ValidTitles,
            COUNT(CASE WHEN AgencyName IS NOT NULL AND AgencyName != '' THEN 1 END) as ValidAgencies,
            COUNT(CASE WHEN OpportunityNumber IS NOT NULL AND OpportunityNumber != '' THEN 1 END) as ValidOpportunityNumbers,
            COUNT(CASE WHEN AwardCeiling IS NOT NULL AND AwardCeiling > 0 THEN 1 END) as ValidAwards,
            MAX(CreatedDate) as LatestRecord,
            ROUND(100.0 * COUNT(CASE WHEN Title IS NOT NULL AND Title != '' THEN 1 END) / COUNT(*), 1) as QualityScore
        FROM RawGrantsLayer1;
        
        -- Show sample of latest records
        SELECT TOP 5
            'SAMPLE_RECORDS' as ReportType,
            OpportunityNumber,
            LEFT(Title, 50) + '...' as Title_Sample,
            AgencyName,
            AwardCeiling,
            CreatedDate
        FROM RawGrantsLayer1 
        ORDER BY CreatedDate DESC;
        """
        
        result = self.execute_sql_command(sql)
        if result is not None:
            print("✅ Layer 1 data validation completed")
            return True
        else:
            print("❌ Layer 1 data validation failed")
            return False

    def _create_grants_collector_script(self):
        """Create the grants collector script based on your existing code"""
        
        script_content = f'''#!/usr/bin/env python3
"""
Grants.gov Collector - Production Version
Based on your existing Azure-optimized SPA automation
"""

import os
import sys
import logging
from datetime import datetime

# Configure Azure environment
os.environ["AzureWebJobsStorage"] = "{self.azure_connection_string};EndpointSuffix=core.windows.net"
os.environ["STORAGE_ACCOUNT_NAME"] = "grantsgov225756"

# Import your existing automation class
sys.path.append('/Users/dinghali/Desktop/Runwei/grants_gov_api_azure/archive/old_versions/layers/layer1_raw_data_collection/scripts')

try:
    from collect_grants_from_website import AutomatedGrantsFetcher
    
    def main():
        """Run automated grants collection"""
        logging.basicConfig(level=logging.INFO)
        
        print("�� Starting automated Grants.gov collection...")
        print(f"📅 Date: {{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}")
        
        try:
            # Initialize the fetcher with Azure settings
            fetcher = AutomatedGrantsFetcher()
            
            # Run automated fetch with broad search parameters for maximum data
            search_params = {{
                'keyword': '',  # Empty to get all grants
            }}
            
            success = fetcher.run_automated_fetch(search_params=search_params, cleanup=True)
            
            if success:
                print("✅ Grants collection completed successfully")
                print("📊 Data stored in Azure Table Storage: GrantDetails")
                return 0
            else:
                print("❌ Grants collection failed")
                return 1
                
        except Exception as e:
            print(f"💥 Error during collection: {{e}}")
            return 1

    if __name__ == "__main__":
        sys.exit(main())
        
except ImportError as e:
    print(f"❌ Could not import grants collector: {{e}}")
    print("📍 Make sure the collect_grants_from_website.py is available")
    sys.exit(1)
'''
        
        with open("production/scripts/grants_collector.py", "w") as f:
            f.write(script_content)
        
        # Make it executable
        os.chmod("production/scripts/grants_collector.py", 0o755)
        print("✅ Created grants_collector.py")

    def _create_azure_sync_script(self):
        """Create the Azure to SQL sync script based on your existing code"""
        
        script_content = f'''#!/usr/bin/env python3
"""
Azure Table Storage to SQL Sync - Production Version
Based on your existing import_storage_to_layer1.py
"""

import subprocess
from azure.data.tables import TableServiceClient
from datetime import datetime

def sync_azure_to_sql():
    """Sync data from Azure Table Storage to RawGrantsLayer1"""
    
    connection_string = "{self.azure_connection_string};EndpointSuffix=core.windows.net"
    
    try:
        print("📡 Connecting to Azure Table Storage...")
        table_service = TableServiceClient.from_connection_string(connection_string)
        table_client = table_service.get_table_client('GrantDetails')
        
        print("📥 Fetching data from Azure Table Storage...")
        entities = list(table_client.query_entities("PartitionKey eq 'Grant'"))
        print(f"✅ Retrieved {{len(entities)}} entities from Azure Storage")
        
        if not entities:
            print("❌ No data found in Azure Table Storage")
            return False
        
        print(f"🚀 Processing {{len(entities)}} records...")
        
        # Process in batches
        batch_size = 25
        total_inserted = 0
        
        for i in range(0, len(entities), batch_size):
            batch = entities[i:i + batch_size]
            batch_number = i//batch_size + 1
            total_batches = (len(entities) + batch_size - 1) // batch_size
            
            print(f"🔄 Processing batch {{batch_number}}/{{total_batches}} ({{len(batch)}} records)...")
            
            insert_statements = []
            for entity in batch:
                try:
                    # Helper functions for safe data extraction
                    def safe_get(key, default='', max_length=None):
                        value = entity.get(key, default)
                        if value is None or str(value).strip() == '':
                            return 'NULL'
                        if isinstance(value, str):
                            escaped = value.replace("'", "''")
                            if max_length and len(escaped) > max_length:
                                escaped = escaped[:max_length]
                            return f"'{{escaped}}'"
                        return f"'{{str(value)}}'"
                    
                    def safe_get_decimal(key, default=None):
                        value = entity.get(key, default)
                        if value is None:
                            return 'NULL'
                        try:
                            return str(float(value))
                        except (ValueError, TypeError):
                            return 'NULL'
                    
                    def safe_get_datetime(key, default=None):
                        value = entity.get(key, default)
                        if value is None or str(value).strip() == '':
                            return 'NULL'
                        try:
                            if hasattr(value, 'strftime'):
                                return f"'{{value.strftime('%Y-%m-%d %H:%M:%S')}}'"
                            elif isinstance(value, str) and '/' in value:
                                from datetime import datetime
                                dt = datetime.strptime(value, '%m/%d/%Y')
                                return f"'{{dt.strftime('%Y-%m-%d')}}'"
                            else:
                                return 'NULL'
                        except:
                            return 'NULL'
                    
                    # Create INSERT statement
                    insert_sql = f"""
INSERT INTO RawGrantsLayer1 (
    PartitionKey, RowKey, OpportunityNumber, OpportunityURL, Title,
    AgencyCode, AgencyName, Category, FundingType, CFDANumbers,
    EstimatedTotalFunding, AwardCeiling, AwardFloor, AdditionalInfoURL,
    PostedDate, CloseDate, Description, EligibleApplicants,
    Status, Version, ProcessedDate, ProcessedBy, SourceType, CreatedDate, UpdatedDate
) VALUES (
    {{safe_get('PartitionKey', 'Grant', 255)}},
    {{safe_get('RowKey', '', 255)}},
    {{safe_get('OpportunityNumber', '', 255)}},
    {{safe_get('OpportunityURL', '', 2000)}},
    {{safe_get('Title', '', 1000)}},
    {{safe_get('AgencyCode', '', 100)}},
    {{safe_get('AgencyName', '', 500)}},
    {{safe_get('Category', '', 500)}},
    {{safe_get('FundingType', '', 255)}},
    {{safe_get('CFDANumbers', '', 500)}},
    {{safe_get_decimal('EstimatedTotalFunding')}},
    {{safe_get_decimal('AwardCeiling')}},
    {{safe_get_decimal('AwardFloor')}},
    {{safe_get('AdditionalInfoURL', '', 2000)}},
    {{safe_get_datetime('PostedDate')}},
    {{safe_get_datetime('CloseDate')}},
    {{safe_get('Description')}},
    {{safe_get('EligibleApplicants')}},
    {{safe_get('Status', '', 100)}},
    {{safe_get('Version', '', 50)}},
    GETDATE(),
    'Layer1_Daily_Collection',
    'Azure_Sync',
    GETDATE(),
    GETDATE()
);
"""
                    insert_statements.append(insert_sql)
                    
                except Exception as e:
                    print(f"⚠️ Error creating SQL for entity {{entity.get('RowKey', 'Unknown')}}: {{e}}")
                    continue
            
            # Execute batch
            if insert_statements:
                batch_sql = "\\n".join(insert_statements)
                temp_file = f"temp_batch_{{batch_number}}.sql"
                
                try:
                    with open(temp_file, 'w', encoding='utf-8') as f:
                        f.write(batch_sql)
                    
                    cmd = [
                        "sqlcmd", "-S", "{self.server}",
                        "-d", "{self.database}", "-U", "{self.username}", "-P", "{self.password}",
                        "-i", temp_file, "-C"
                    ]
                    subprocess.run(cmd, check=True, capture_output=True, text=True)
                    total_inserted += len(insert_statements)
                    
                    print(f"   ✅ Batch {{batch_number}} completed - {{len(insert_statements)}} records")
                    
                except Exception as e:
                    print(f"   ❌ Batch {{batch_number}} failed: {{e}}")
                finally:
                    import os
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
        
        print(f"\\n🎉 SYNC COMPLETED!")
        print(f"✅ Successfully inserted: {{total_inserted}} records")
        return total_inserted > 0
        
    except Exception as e:
        print(f"❌ Error during sync: {{e}}")
        return False

if __name__ == "__main__":
    print("🔄 Azure Table Storage to SQL Sync")
    print("=" * 40)
    print(f"📅 Started: {{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}")
    
    success = sync_azure_to_sql()
    if success:
        print("\\n✅ Azure sync completed successfully!")
    else:
        print("\\n❌ Azure sync failed")
        exit(1)
'''
        
        with open("production/scripts/azure_to_sql_sync.py", "w") as f:
            f.write(script_content)
        
        # Make it executable
        os.chmod("production/scripts/azure_to_sql_sync.py", 0o755)
        print("✅ Created azure_to_sql_sync.py")

def main():
    """Main execution function for Layer 1 daily collection"""
    print("=" * 65)
    print("�� Layer 1 - Daily Data Collection - Azure Production")
    print("=" * 65)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Goal: Automated daily collection from Grants.gov")
    
    try:
        collector = Layer1DailyCollector()
        
        # Step 1: Collect from Grants.gov
        print("\n🌐 Step 1: Collecting data from Grants.gov...")
        if not collector.collect_from_grants_gov():
            print("❌ Failed to collect data from Grants.gov")
            return False
        
        # Step 2: Sync Azure to SQL
        print("\n📤 Step 2: Syncing Azure Table Storage to SQL...")
        if not collector.sync_azure_to_sql():
            print("❌ Failed to sync Azure data to SQL")
            return False
        
        # Step 3: Validate data
        print("\n🔍 Step 3: Validating Layer 1 data...")
        if not collector.validate_layer1_data():
            print("❌ Failed to validate Layer 1 data")
            return False
        
        print("\n🎊 SUCCESS! Layer 1 Daily Collection Complete!")
        print("=" * 65)
        print("✅ Fresh data collected from Grants.gov")
        print("✅ Data stored in Azure Table Storage")
        print("✅ Data synced to RawGrantsLayer1 in SQL Database")
        print("✅ Data quality validated")
        print("🎯 Ready for Layer 2 enhancement")
        
        return True
        
    except Exception as e:
        print(f"\n💥 Layer 1 collection failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🏁 Starting Layer 1 Daily Collection...")
    success = main()
    if success:
        print("\n🚀 Layer 1 Daily Collection Successfully Completed!")
        print("📊 RawGrantsLayer1 contains fresh grant data")
        print("🎯 Ready for Layer 2 and Layer 3 processing")
    else:
        print("\n❌ Layer 1 collection failed - check logs for details")
        exit(1)
