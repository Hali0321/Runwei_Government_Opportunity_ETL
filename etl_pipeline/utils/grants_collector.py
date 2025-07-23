#!/usr/bin/env python3
"""
Grants.gov Collector - Production Version
Based on your existing Azure-optimized SPA automation
"""

import os
import sys
import logging
from datetime import datetime

# Configure Azure environment
os.environ["AzureWebJobsStorage"] = "DefaultEndpointsProtocol=https;AccountName=grantsgov225756;AccountKey=UXwW5dfy9MY9nh2BGmWhYUbzBve+6LUyT3F7+N3Cp0kWUoEk4AO3z5U6LrBYvo/VwO+Nduq2ay9E+AStKQb86Q==;EndpointSuffix=core.windows.net"
os.environ["STORAGE_ACCOUNT_NAME"] = "grantsgov225756"

# Import your existing automation class
sys.path.append('/Users/dinghali/Desktop/Runwei/grants_gov_api_azure/archive/old_versions/layers/layer1_raw_data_collection/scripts')

try:
    from collect_grants_from_website import AutomatedGrantsFetcher
    
    def main():
        """Run automated grants collection"""
        logging.basicConfig(level=logging.INFO)
        
        print("�� Starting automated Grants.gov collection...")
        print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Initialize the fetcher with Azure settings
            fetcher = AutomatedGrantsFetcher()
            
            # Run automated fetch with broad search parameters for maximum data
            search_params = {
                'keyword': '',  # Empty to get all grants
            }
            
            success = fetcher.run_automated_fetch(search_params=search_params, cleanup=True)
            
            if success:
                print("✅ Grants collection completed successfully")
                print("📊 Data stored in Azure Table Storage: GrantDetails")
                return 0
            else:
                print("❌ Grants collection failed")
                return 1
                
        except Exception as e:
            print(f"💥 Error during collection: {e}")
            return 1

    if __name__ == "__main__":
        sys.exit(main())
        
except ImportError as e:
    print(f"❌ Could not import grants collector: {e}")
    print("📍 Make sure the collect_grants_from_website.py is available")
    sys.exit(1)
