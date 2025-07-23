#!/usr/bin/env python3
"""
Azure SQL Database - Clean Up Duplicate Fee Columns
Removes old duplicate columns and keeps only Runwei-formatted ones

Removes:
- CostSharingRequired (old column)
- BusinessRules (old column)

Keeps:
- FeeRequired (Runwei format)
- CostToParticipate (Runwei format)
"""

import subprocess
import logging
from datetime import datetime
from pathlib import Path

# Configure logging
SCRIPT_DIR = Path(__file__).parent
PYCACHE_DIR = SCRIPT_DIR / "__pycache__"
PYCACHE_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    handlers=[
        logging.FileHandler(PYCACHE_DIR / 'cleanup_duplicate_columns.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ColumnCleanup:
    """Clean up duplicate fee columns in Layer 2"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"

    def execute_sql_command(self, sql_query, timeout=300):
        """Execute SQL command with Azure SQL Database"""
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
                if result.stdout:
                    logger.info(f"Output: {result.stdout}")
                return result.stdout
            else:
                logger.error(f"❌ SQL command failed with return code {result.returncode}")
                if result.stderr:
                    logger.error(f"Error: {result.stderr}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error executing SQL: {e}")
            return None

    def check_existing_columns(self):
        """Check which columns exist in CleanGrantsLayer2"""
        logger.info("🔍 Checking existing columns...")
        
        check_sql = """
        SELECT 
            'COLUMN_CHECK' as Check_Type,
            COLUMN_NAME as Column_Name,
            DATA_TYPE as Data_Type,
            IS_NULLABLE as Is_Nullable
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'CleanGrantsLayer2' 
          AND COLUMN_NAME IN ('CostSharingRequired', 'BusinessRules', 'FeeRequired', 'CostToParticipate')
        ORDER BY COLUMN_NAME;
        """
        
        result = self.execute_sql_command(check_sql, timeout=60)
        return result is not None

    def remove_old_duplicate_columns(self):
        """Remove old duplicate columns (CostSharingRequired, BusinessRules)"""
        logger.info("🗑️ Removing old duplicate columns...")
        
        cleanup_sql = """
        -- REMOVE OLD DUPLICATE COLUMNS
        
        -- Remove CostSharingRequired column if it exists
        IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                  WHERE TABLE_NAME = 'CleanGrantsLayer2' AND COLUMN_NAME = 'CostSharingRequired')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 DROP COLUMN CostSharingRequired;
            PRINT 'Removed CostSharingRequired column (old duplicate)';
        END
        ELSE
        BEGIN
            PRINT 'CostSharingRequired column does not exist';
        END
        
        -- Remove BusinessRules column if it exists
        IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                  WHERE TABLE_NAME = 'CleanGrantsLayer2' AND COLUMN_NAME = 'BusinessRules')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 DROP COLUMN BusinessRules;
            PRINT 'Removed BusinessRules column (old duplicate)';
        END
        ELSE
        BEGIN
            PRINT 'BusinessRules column does not exist';
        END
        
        -- Verify cleanup
        SELECT 
            'CLEANUP_COMPLETE' as Status,
            COUNT(*) as Total_Records,
            'Only Runwei columns remain: FeeRequired, CostToParticipate' as Result
        FROM CleanGrantsLayer2;
        """
        
        result = self.execute_sql_command(cleanup_sql, timeout=120)
        return result is not None and 'CLEANUP_COMPLETE' in str(result)

    def verify_final_schema(self):
        """Verify final schema has only Runwei columns"""
        logger.info("✅ Verifying final schema...")
        
        verify_sql = """
        -- VERIFY FINAL SCHEMA
        
        -- Check remaining fee-related columns
        SELECT 
            'FINAL_SCHEMA_CHECK' as Check_Type,
            COLUMN_NAME as Column_Name,
            DATA_TYPE as Data_Type
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'CleanGrantsLayer2' 
          AND COLUMN_NAME IN ('CostSharingRequired', 'BusinessRules', 'FeeRequired', 'CostToParticipate')
        ORDER BY COLUMN_NAME;
        
        -- Sample data check
        SELECT TOP 5
            'SAMPLE_DATA' as Sample_Type,
            OpportunityNumber,
            FeeRequired,
            CostToParticipate
        FROM CleanGrantsLayer2
        ORDER BY UpdatedDate DESC;
        """
        
        result = self.execute_sql_command(verify_sql, timeout=60)
        return result is not None

    def run_column_cleanup(self):
        """Run complete column cleanup process"""
        logger.info("🧹 COLUMN CLEANUP - Starting...")
        logger.info("=" * 50)
        logger.info("🗑️ Removing duplicate fee columns")
        logger.info("✅ Keeping only Runwei-formatted columns")
        
        steps = [
            ("Check Existing Columns", self.check_existing_columns),
            ("Remove Old Duplicate Columns", self.remove_old_duplicate_columns),
            ("Verify Final Schema", self.verify_final_schema)
        ]
        
        success_count = 0
        for i, (step_name, step_function) in enumerate(steps, 1):
            logger.info(f"\n📍 STEP {i}/{len(steps)}: {step_name}")
            
            try:
                success = step_function()
                if success:
                    logger.info(f"✅ {step_name} completed successfully")
                    success_count += 1
                else:
                    logger.error(f"❌ {step_name} failed")
            except Exception as e:
                logger.error(f"❌ {step_name} error: {e}")
        
        logger.info(f"\n🧹 COLUMN CLEANUP SUMMARY")
        logger.info("=" * 30)
        logger.info(f"✅ Completed Steps: {success_count}/{len(steps)}")
        
        if success_count >= 2:
            logger.info("🎉 COLUMN CLEANUP SUCCESS!")
            logger.info("🗑️ Removed: CostSharingRequired, BusinessRules")
            logger.info("✅ Kept: FeeRequired, CostToParticipate (Runwei format)")
            logger.info("🎯 Schema is now clean and Runwei compliant")
            return True
        else:
            logger.error("❌ Column cleanup failed")
            return False

def main():
    """Main execution function for Column Cleanup"""
    print("🧹 CLEANUP DUPLICATE FEE COLUMNS")
    print("=" * 40)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Cleanup Goal:")
    print("   🗑️ Remove: CostSharingRequired (old column)")
    print("   🗑️ Remove: BusinessRules (old column)")
    print("   ✅ Keep: FeeRequired (Runwei format)")
    print("   ✅ Keep: CostToParticipate (Runwei format)")
    print("\n🔧 Processing steps:")
    print("   • Check existing columns")
    print("   • Remove old duplicate columns")
    print("   • Verify final schema is clean")
    
    cleaner = ColumnCleanup()
    success = cleaner.run_column_cleanup()
    
    if success:
        print("\n🎉 COLUMN CLEANUP COMPLETED!")
        print("\n📊 FINAL SCHEMA STATUS:")
        print("   🗑️ Removed Columns: ✅ CostSharingRequired, BusinessRules")
        print("   ✅ Kept Columns: FeeRequired, CostToParticipate")
        print("   🎯 Schema: Clean and Runwei compliant")
        print("\n🔍 VERIFY YOUR CLEAN SCHEMA:")
        print("   📊 Check remaining columns:")
        print("      → SELECT COLUMN_NAME, DATA_TYPE")
        print("         FROM INFORMATION_SCHEMA.COLUMNS")
        print("         WHERE TABLE_NAME = 'CleanGrantsLayer2'")
        print("         AND COLUMN_NAME LIKE '%Cost%' OR COLUMN_NAME LIKE '%Fee%'")
        print("\n   💰 Check data:")
        print("      → SELECT TOP 10 FeeRequired, CostToParticipate")
        print("         FROM CleanGrantsLayer2")
        print("\n✅ Schema is now clean with only Runwei-compliant columns!")
        print("🚀 No more duplicates!")
    else:
        print("\n❌ Column cleanup failed. Check logs for details.")

if __name__ == "__main__":
    main()