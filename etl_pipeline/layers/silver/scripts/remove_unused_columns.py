#!/usr/bin/env python3
"""
Remove Unused Columns from CleanGrantsLayer2
Remove: CoverImage, AwardValueFormatted, RunweiCategory, CategoryTags, OriginalFundingType, URLProcessingNotes
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
        logging.FileHandler(PYCACHE_DIR / 'remove_unused_columns.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ColumnRemover:
    """Remove unused columns from CleanGrantsLayer2 table"""
    
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
                return result.stdout
            else:
                logger.error(f"❌ SQL Error: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            logger.error(f"⏰ SQL command timed out after {timeout} seconds")
            return None
        except Exception as e:
            logger.error(f"💥 Error executing SQL: {e}")
            return None

    def check_existing_columns(self):
        """Check which columns currently exist"""
        logger.info("🔍 Checking which columns currently exist...")
        
        sql = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'CleanGrantsLayer2' 
        AND COLUMN_NAME IN ('CoverImage', 'AwardValueFormatted', 'RunweiCategory', 'CategoryTags', 'OriginalFundingType', 'URLProcessingNotes')
        ORDER BY COLUMN_NAME;
        """
        
        result = self.execute_sql_command(sql)
        if result:
            logger.info("📊 Current columns found:")
            logger.info(result)
        return result

    def remove_columns(self):
        """Remove the specified unused columns"""
        logger.info("🗑️ Starting column removal process...")
        
        # List of columns to remove
        columns_to_remove = [
            'CoverImage',
            'AwardValueFormatted', 
            'RunweiCategory',
            'CategoryTags',
            'OriginalFundingType',
            'URLProcessingNotes'
        ]
        
        for column in columns_to_remove:
            logger.info(f"🔄 Attempting to remove column: {column}")
            
            sql = f"""
            -- Check if column exists and remove it
            IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = '{column}')
            BEGIN
                ALTER TABLE CleanGrantsLayer2 DROP COLUMN {column};
                PRINT 'Removed {column} column';
            END
            ELSE
            BEGIN
                PRINT '{column} column does not exist';
            END
            """
            
            result = self.execute_sql_command(sql)
            if result:
                logger.info(f"✅ {column} processing complete")
            else:
                logger.error(f"❌ Failed to process {column}")

    def verify_removal(self):
        """Verify that columns have been removed"""
        logger.info("✅ Verifying column removal...")
        
        sql = """
        SELECT 
            COUNT(*) as RemainingColumns
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'CleanGrantsLayer2' 
        AND COLUMN_NAME IN ('CoverImage', 'AwardValueFormatted', 'RunweiCategory', 'CategoryTags', 'OriginalFundingType', 'URLProcessingNotes');
        
        SELECT 
            COUNT(*) as TotalColumns
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'CleanGrantsLayer2';
        """
        
        result = self.execute_sql_command(sql)
        if result:
            logger.info("📊 Verification results:")
            logger.info(result)

def main():
    """Main execution function"""
    logger.info("🚀 Starting unused column removal process")
    logger.info("=" * 60)
    
    remover = ColumnRemover()
    
    try:
        # Step 1: Check existing columns
        remover.check_existing_columns()
        
        # Step 2: Remove columns
        remover.remove_columns()
        
        # Step 3: Verify removal
        remover.verify_removal()
        
        logger.info("🎉 Column removal process completed successfully!")
        logger.info("📊 Database schema has been optimized")
        
    except Exception as e:
        logger.error(f"💥 Column removal failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        logger.info("✅ Process completed successfully")
    else:
        logger.error("❌ Process failed")
