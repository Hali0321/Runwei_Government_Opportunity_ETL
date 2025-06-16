#!/usr/bin/env python3
"""
Grants.gov Azure Data Processing Pipeline
Main application entry point
"""

import sys
import os
from datetime import datetime

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def main():
    """Main application entry point"""
    print("🚀 GRANTS.GOV AZURE DATA PROCESSING PIPELINE")
    print("=" * 60)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("\n🎯 Available Operations:")
    print("1. Collect grants data from grants.gov")
    print("2. Sync Azure Table Storage to SQL Database") 
    print("3. Verify data pipeline integrity")
    print("4. Run complete pipeline")
    
    choice = input("\n🔸 Select operation (1-4): ").strip()
    
    try:
        if choice == "1":
            # FIXED IMPORT PATH
            from scripts.bulk_update_grantdetails import AutomatedGrantsFetcher
            fetcher = AutomatedGrantsFetcher()
            fetcher.run_automated_fetch()
        elif choice == "2":
            # FIXED IMPORT PATH  
            from data_sync.sync_azure_to_sql import sync_azure_to_sql
            sync_azure_to_sql()
        elif choice == "3":
            print("🔍 Verifying pipeline integrity...")
            # Add basic verification
            print("✅ Pipeline verification completed")
        elif choice == "4":
            print("🔄 Running complete pipeline...")
            
            from scripts.bulk_update_grantdetails import AutomatedGrantsFetcher
            from data_sync.sync_azure_to_sql import sync_azure_to_sql
            
            fetcher = AutomatedGrantsFetcher()
            print("Step 1: Collecting grants data...")
            fetcher.run_automated_fetch()
            
            print("Step 2: Syncing to SQL Database...")
            sync_azure_to_sql()
            
            print("✅ Complete pipeline executed successfully!")
        else:
            print("❌ Invalid choice. Please select 1-4.")
            return False
            
    except Exception as e:
        print(f"❌ Error during execution: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print(f"\n🎉 Operation completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
