#!/usr/bin/env python3
"""
Layer 1 - Import-Based Collection - Azure Production
Uses existing scripts: collect_grants_from_website.py + import_storage_to_layer1.py
Clean, efficient orchestration of proven components
"""

import subprocess
import logging
import sys
import os
from datetime import datetime
from pathlib import Path

# Import your existing modules
sys.path.append(str(Path(__file__).parent))

try:
    from collect_grants_from_website import AutomatedGrantsFetcher, setup_azure_environment
    from import_storage_to_layer1 import sync_azure_to_sql
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("📍 Ensure collect_grants_from_website.py and import_storage_to_layer1.py are in the same directory")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class ImportBasedLayer1Collector:
    """Layer 1 collector that imports and orchestrates existing scripts"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        print("🔧 Initialized Import-Based Layer 1 Collector")
        
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

    def collect_from_grants_gov_using_existing_script(self):
        """Step 1: Use existing collect_grants_from_website.py"""
        print("🌐 Step 1: Collecting data using existing AutomatedGrantsFetcher...")
        
        try:
            # Setup Azure environment using existing function
            setup_azure_environment()
            
            # Initialize your existing fetcher
            fetcher = AutomatedGrantsFetcher()
            
            # Run the existing automation with optimized parameters
            search_params = {
                'keyword': '',  # Empty for all grants - maximum data collection
            }
            
            print("🚀 Starting automated grants collection...")
            print("📊 Using existing proven SPA automation")
            print("☁️ Target: Azure Table Storage (GrantDetails)")
            
            success = fetcher.run_automated_fetch(
                search_params=search_params, 
                cleanup=True
            )
            
            if success:
                print("✅ Successfully collected data using existing AutomatedGrantsFetcher")
                print("📊 Data stored in Azure Table Storage via proven automation")
                print("🎯 Ready for sync to SQL Database")
                return True
            else:
                print("❌ Existing grants collection script failed")
                print("🔍 Check the AutomatedGrantsFetcher logs above for details")
                return False
                
        except Exception as e:
            print(f"❌ Error using existing grants collection script: {e}")
            logger.exception("Grants collection error")
            return False

    def sync_azure_to_sql_using_existing_script(self):
        """Step 2: Use existing import_storage_to_layer1.py"""
        print("📤 Step 2: Syncing data using existing sync_azure_to_sql function...")
        
        try:
            print("🚀 Starting Azure Table Storage to SQL sync...")
            print("📊 Using existing proven batch processing")
            print("🗄️ Target: RawGrantsLayer1 SQL table")
            
            # Call your existing sync function
            success = sync_azure_to_sql()
            
            if success:
                print("✅ Successfully synced data using existing sync function")
                print("📊 RawGrantsLayer1 populated via proven sync logic")
                print("🎯 Data ready for Layer 2 processing")
                return True
            else:
                print("❌ Existing sync script failed")
                print("🔍 Check the sync_azure_to_sql logs above for details")
                return False
                
        except Exception as e:
            print(f"❌ Error using existing sync script: {e}")
            logger.exception("Sync error")
            return False

    def validate_layer1_data(self):
        """Step 3: Validate Layer 1 data quality"""
        print("🔍 Step 3: Validating Layer 1 data quality...")
        
        sql = """
        SELECT 
            COUNT(*) as TotalRecords,
            COUNT(CASE WHEN Title IS NOT NULL AND Title != '' THEN 1 END) as ValidTitles,
            COUNT(CASE WHEN AgencyName IS NOT NULL AND AgencyName != '' THEN 1 END) as ValidAgencies,
            COUNT(CASE WHEN OpportunityNumber IS NOT NULL AND OpportunityNumber != '' THEN 1 END) as ValidOpportunityNumbers,
            COUNT(CASE WHEN AwardCeiling IS NOT NULL AND AwardCeiling > 0 THEN 1 END) as ValidAwards,
            MAX(CreatedDate) as LatestRecord,
            ROUND(100.0 * COUNT(CASE WHEN Title IS NOT NULL AND Title != '' THEN 1 END) / COUNT(*), 1) as QualityScore
        FROM RawGrantsLayer1;
        
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
            print("📊 Data quality metrics generated")
            print("🎯 RawGrantsLayer1 validated and ready")
            return True
        else:
            print("❌ Layer 1 data validation failed")
            return False

    def run_import_based_collection(self):
        """Execute collection using existing imported scripts"""
        print("🔄 Starting import-based Layer 1 collection process...")
        start_time = datetime.now()
        
        try:
            # Step 1: Use existing grants collection script
            print("\n" + "="*60)
            print("📥 STEP 1: GRANTS.GOV DATA COLLECTION")
            print("="*60)
            if not self.collect_from_grants_gov_using_existing_script():
                print("❌ Failed at Step 1: Existing grants collection")
                return False
            
            # Step 2: Use existing sync script
            print("\n" + "="*60)
            print("📤 STEP 2: AZURE TO SQL SYNC")
            print("="*60)
            if not self.sync_azure_to_sql_using_existing_script():
                print("❌ Failed at Step 2: Existing Azure sync")
                return False
            
            # Step 3: Validate data
            print("\n" + "="*60)
            print("🔍 STEP 3: DATA VALIDATION")
            print("="*60)
            if not self.validate_layer1_data():
                print("❌ Failed at Step 3: Data validation")
                return False
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print("\n" + "="*60)
            print("🎉 IMPORT-BASED COLLECTION COMPLETED!")
            print("="*60)
            print(f"⏱️ Total time: {duration:.2f} seconds")
            print("📊 Pipeline: Grants.gov → Azure Table Storage → SQL Database")
            print("✅ All existing scripts executed successfully")
            return True
            
        except Exception as e:
            print(f"❌ Unexpected error in import-based collection: {e}")
            logger.exception("Import-based collection error")
            return False

def main():
    """Main execution function for import-based Layer 1 collection"""
    print("=" * 70)
    print("🎯 Import-Based Layer 1 Collection - Azure Production")
    print("=" * 70)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Goal: Use existing scripts for complete pipeline")
    print("📁 Components:")
    print("   • AutomatedGrantsFetcher (collect_grants_from_website.py)")
    print("   • sync_azure_to_sql (import_storage_to_layer1.py)")
    print("   • Data validation and quality checks")
    print("🔄 Import-based collection process:")
    print("   1. Import and run AutomatedGrantsFetcher")
    print("   2. Import and run sync_azure_to_sql") 
    print("   3. Validate data quality and generate metrics")
    print("✨ Advantages:")
    print("   • Uses your proven, tested code")
    print("   • No code duplication")
    print("   • Clean orchestration layer")
    print("   • Easy maintenance and updates")
    
    try:
        collector = ImportBasedLayer1Collector()
        success = collector.run_import_based_collection()
        
        if success:
            print(f"\n🎉 Import-based Layer 1 collection completed successfully!")
            print(f"📊 Fresh data collected via existing AutomatedGrantsFetcher")
            print(f"☁️ Data synced via existing sync_azure_to_sql function")
            print(f"🗄️ RawGrantsLayer1 populated and validated")
            print(f"✅ Ready for Layer 2 processing")
            print(f"🚀 Next step: Run Layer 2 enhancement script")
            return True
        else:
            print(f"\n❌ Import-based Layer 1 collection failed")
            print(f"🔍 Check the error messages above for details")
            print(f"💡 Verify that both existing scripts are working independently")
            return False
        
    except Exception as e:
        print(f"💥 Unexpected error in import-based collection: {e}")
        logger.exception("Import-based collection error")
        return False

if __name__ == "__main__":
    print("🏁 Starting Import-Based Layer 1 Collection...")
    success = main()
    if success:
        print("✅ Import-based collection completed successfully")
        sys.exit(0)
    else:
        print("❌ Import-based collection failed - check logs for details")
        sys.exit(1)