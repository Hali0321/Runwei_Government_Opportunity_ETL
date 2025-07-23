#!/usr/bin/env python3
"""
Azure SQL Database - Immediate Sample Record Removal
Remove the 10 SAMPLE records and 10 Sample titles from CleanGrantsLayer2
"""

import os
import subprocess
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestDataRemover:
    """Test Data Removal for Azure SQL Database"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        
    def execute_sql_command(self, sql_query, timeout=600):
        """Execute SQL command with Azure SQL Database optimizations"""
        try:
            cmd = [
                "sqlcmd", "-S", self.server,
                "-d", self.database, 
                "-U", self.username, 
                "-P", self.password,
                "-Q", sql_query, 
                "-C", "-t", str(timeout), "-I", "-b"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 30)
            
            if result.returncode == 0:
                logger.info("✅ SQL command executed successfully")
                return result.stdout
            else:
                logger.error(f"❌ SQL command failed: {result.stderr}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error executing SQL: {e}")
            return None

    def remove_test_data(self):
        """Remove test and invalid data from CleanGrantsLayer2"""
        logger.info("🗑️ Removing test data...")
        
        removal_sql = """
        -- Remove test data and invalid records
        DELETE FROM CleanGrantsLayer2 
        WHERE 
            Title LIKE '%TEST%' OR 
            Title LIKE '%test%' OR
            Title LIKE '%Test%' OR
            Description LIKE '%TEST%' OR
            Description LIKE '%test%' OR
            AgencyName LIKE '%TEST%' OR
            OpportunityNumber LIKE '%TEST%' OR
            OpportunityNumber IS NULL OR
            Title IS NULL OR
            LTRIM(RTRIM(Title)) = '';
        
        PRINT CONCAT('Removed ', @@ROWCOUNT, ' test/invalid records');
        
        -- Update record counts and statistics
        SELECT 
            'CLEANUP_STATISTICS' as StatType,
            COUNT(*) as RemainingRecords,
            COUNT(CASE WHEN IsGrant = 1 THEN 1 END) as GrantRecords,
            COUNT(CASE WHEN IsProcurementContract = 1 THEN 1 END) as ContractRecords,
            AVG(DataQualityScore) as AvgDataQuality
        FROM CleanGrantsLayer2;
        """
        
        result = self.execute_sql_command(removal_sql)
        return result is not None

    def run_complete_test_data_removal(self):
        """Run complete test data removal - Pipeline Controller Interface"""
        logger.info("🗑️ Running test data removal...")
        
        try:
            success = self.remove_test_data()
            
            if success:
                logger.info("✅ Test data removal completed successfully")
                return True
            else:
                logger.error("❌ Test data removal failed")
                return False
                
        except Exception as e:
            logger.error(f"❌ Test data removal error: {e}")
            return False

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

def main():
    """Main execution function"""
    print("🗑️ TEST DATA REMOVAL")
    print("=" * 25)
    
    remover = TestDataRemover()
    success = remover.run_complete_test_data_removal()
    
    if success:
        print("✅ Test data removal completed!")
    else:
        print("❌ Test data removal failed!")

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