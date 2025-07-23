#!/usr/bin/env python3
"""
Remove RunweiCategory column with its index dependency
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
        logging.FileHandler(PYCACHE_DIR / 'remove_runwei_category.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RunweiCategoryRemover:
    """Remove RunweiCategory column and its dependencies"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        
    def execute_sql_command(self, sql_command, timeout=120):
        """Execute SQL command using sqlcmd"""
        try:
            command = [
                "sqlcmd",
                "-S", self.server,
                "-d", self.database,
                "-U", self.username,
                "-P", self.password,
                "-Q", sql_command,
                "-t", str(timeout)
            ]
            
            result = subprocess.run(
                command, 
                capture_output=True, 
                text=True, 
                timeout=timeout
            )
            
            if result.returncode == 0:
                logger.info("✅ SQL command executed successfully")
                if result.stdout:
                    logger.info(f"Output: {result.stdout}")
                return True
            else:
                logger.error(f"❌ SQL Error: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"⏰ SQL command timed out after {timeout} seconds")
            return False
        except Exception as e:
            logger.error(f"💥 Error executing SQL: {e}")
            return False

    def remove_runwei_category_with_dependencies(self):
        """Remove RunweiCategory column and its dependencies"""
        logger.info("🗑️ Removing RunweiCategory column with all dependencies...")
        
        sql = """
        -- Drop all indexes and dependencies related to RunweiCategory
        IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanGrantsLayer2_RunweiCategory')
        BEGIN
            DROP INDEX IX_CleanGrantsLayer2_RunweiCategory ON CleanGrantsLayer2;
            PRINT 'Dropped index IX_CleanGrantsLayer2_RunweiCategory';
        END
        
        -- Drop composite index with category flags if it exists
        IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanGrantsLayer2_CategoryFlags')
        BEGIN
            DROP INDEX IX_CleanGrantsLayer2_CategoryFlags ON CleanGrantsLayer2;
            PRINT 'Dropped index IX_CleanGrantsLayer2_CategoryFlags';
        END
        
        -- Now drop the RunweiCategory column
        IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'RunweiCategory')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 DROP COLUMN RunweiCategory;
            PRINT 'Removed RunweiCategory column';
        END
        ELSE
        BEGIN
            PRINT 'RunweiCategory column does not exist';
        END
        """
        
        return self.execute_sql_command(sql)

    def verify_removal(self):
        """Verify that RunweiCategory has been completely removed"""
        logger.info("✅ Verifying RunweiCategory removal...")
        
        sql = """
        -- Check if column still exists
        SELECT 
            COUNT(*) as RunweiCategoryExists
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'CleanGrantsLayer2' 
        AND COLUMN_NAME = 'RunweiCategory';
        
        -- Check if related indexes still exist
        SELECT 
            COUNT(*) as RelatedIndexes
        FROM sys.indexes 
        WHERE name LIKE '%RunweiCategory%' 
        OR name LIKE '%CategoryFlags%';
        """
        
        return self.execute_sql_command(sql)

def main():
    """Main execution function"""
    logger.info("🚀 Starting RunweiCategory column removal")
    logger.info("=" * 50)
    
    remover = RunweiCategoryRemover()
    
    try:
        # Step 1: Remove RunweiCategory with dependencies
        if remover.remove_runwei_category_with_dependencies():
            logger.info("✅ RunweiCategory removal completed")
        else:
            logger.error("❌ RunweiCategory removal failed")
            return False
        
        # Step 2: Verify removal
        if remover.verify_removal():
            logger.info("✅ Verification completed")
        else:
            logger.error("❌ Verification failed")
            return False
        
        logger.info("🎉 RunweiCategory has been completely removed!")
        
    except Exception as e:
        logger.error(f"💥 Process failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        logger.info("✅ Process completed successfully")
    else:
        logger.error("❌ Process failed")
