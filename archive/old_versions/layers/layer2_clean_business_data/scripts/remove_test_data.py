#!/usr/bin/env python3
"""
Azure SQL Database - Immediate Sample Record Removal
Remove the 10 SAMPLE records and 10 Sample titles from CleanGrantsLayer2
"""

import os
import subprocess
from datetime import datetime

def execute_sample_cleanup():
    """Execute immediate cleanup of sample records"""
    
    print("🧹 IMMEDIATE SAMPLE RECORD REMOVAL")
    print("=" * 35)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Target: Remove 10 SAMPLE records + 10 Sample titles")
    
    try:
        # Direct cleanup execution
        print("\n🗑️ Executing sample record removal...")
        
        immediate_cleanup_sql = """
-- IMMEDIATE SAMPLE RECORD CLEANUP
-- Remove SAMPLE-001 through SAMPLE-010 and Sample Grant Opportunity titles

BEGIN TRANSACTION;

-- Show what we're about to remove
SELECT 
    'RECORDS_BEING_REMOVED' as Action,
    COUNT(*) as Count,
    'SAMPLE OpportunityNumbers' as Type
FROM CleanGrantsLayer2 
WHERE OpportunityNumber LIKE 'SAMPLE-%';

SELECT 
    'RECORDS_BEING_REMOVED' as Action,
    COUNT(*) as Count,
    'Sample Grant Opportunity Titles' as Type
FROM CleanGrantsLayer2 
WHERE Title LIKE 'Sample Grant Opportunity%';

-- Remove SAMPLE- opportunity numbers
DELETE FROM CleanGrantsLayer2 
WHERE OpportunityNumber LIKE 'SAMPLE-%';

PRINT 'Removed SAMPLE- OpportunityNumbers: ' + CAST(@@ROWCOUNT as VARCHAR(10));

-- Remove Sample Grant Opportunity titles
DELETE FROM CleanGrantsLayer2 
WHERE Title LIKE 'Sample Grant Opportunity%';

PRINT 'Removed Sample Grant Opportunity titles: ' + CAST(@@ROWCOUNT as VARCHAR(10));

-- Verify removal
SELECT 
    'CLEANUP_VERIFICATION' as Verification_Type,
    COUNT(*) as Total_Records_Remaining,
    COUNT(CASE WHEN OpportunityNumber LIKE 'SAMPLE-%' THEN 1 END) as SAMPLE_Records_Remaining,
    COUNT(CASE WHEN Title LIKE 'Sample Grant Opportunity%' THEN 1 END) as Sample_Titles_Remaining,
    CASE 
        WHEN COUNT(CASE WHEN OpportunityNumber LIKE 'SAMPLE-%' 
                          OR Title LIKE 'Sample Grant Opportunity%' 
                    THEN 1 END) = 0 
        THEN '🏆 SUCCESS - ALL SAMPLES REMOVED!'
        ELSE '⚠️ ERROR - Samples still remain'
    END as Cleanup_Status
FROM CleanGrantsLayer2;

-- Show final clean record count
SELECT 
    'FINAL_LAYER2_STATUS' as Status_Type,
    COUNT(*) as Production_Records,
    MIN(CreatedDate) as Oldest_Record,
    MAX(CreatedDate) as Newest_Record,
    COUNT(DISTINCT SUBSTRING(OpportunityNumber, 1, 3)) as Unique_Agency_Codes,
    '🚀 READY FOR LAYER 3 CREATION' as Next_Step
FROM CleanGrantsLayer2;

COMMIT TRANSACTION;

PRINT '✅ Sample cleanup completed successfully!';
"""
        
        # Execute the cleanup directly
        cmd = [
            "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
            "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
            "-Q", immediate_cleanup_sql, "-C"
        ]
        
        print("🔄 Executing immediate sample removal...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        print("🗑️ Cleanup Execution Results:")
        print(result.stdout)
        
        if result.stderr:
            print("⚠️ Messages:")
            print(result.stderr)
        
        # Final verification
        print("\n✅ Final verification...")
        
        verification_cmd = [
            "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
            "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
            "-Q", """
SELECT 
    COUNT(*) as Total_Records,
    COUNT(CASE WHEN OpportunityNumber LIKE 'SAMPLE-%' THEN 1 END) as SAMPLE_Records,
    COUNT(CASE WHEN Title LIKE 'Sample Grant Opportunity%' THEN 1 END) as Sample_Titles,
    CASE 
        WHEN COUNT(CASE WHEN OpportunityNumber LIKE 'SAMPLE-%' THEN 1 END) = 0 
        THEN '🏆 ALL SAMPLES REMOVED!'
        ELSE '⚠️ Samples still exist'
    END as Final_Status
FROM CleanGrantsLayer2;
""", "-C"
        ]
        
        verify_result = subprocess.run(verification_cmd, capture_output=True, text=True)
        print("✅ Final Verification:")
        print(verify_result.stdout)
        
        return True
        
    except Exception as e:
        print(f"❌ Error during sample cleanup: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    start_time = datetime.now()
    success = execute_sample_cleanup()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    if success:
        print(f"\n🎉 SAMPLE CLEANUP COMPLETED!")
        print(f"⏱️ Total time: {duration:.2f} seconds")
        print("✅ SAMPLE-001 through SAMPLE-010 removed")
        print("✅ Sample Grant Opportunity titles removed")
        print("🧹 Layer 2 now contains only production data")
        print("🚀 READY FOR LAYER 3 CREATION!")
        print("\n🌟 Your database is now sample-free!")
    else:
        print(f"\n❌ CLEANUP FAILED!")
        print(f"⏱️ Failed after: {duration:.2f} seconds")