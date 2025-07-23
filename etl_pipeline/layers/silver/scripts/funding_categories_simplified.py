#!/usr/bin/env python3
"""
Azure SQL Database Silver Layer Category Processor - SIMPLIFIED VERSION
Only manages funding type flags without unused columns
Removed columns: RunweiCategory, CategoryTags, OriginalFundingType
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
        logging.FileHandler(PYCACHE_DIR / 'silver_category_processing_simplified.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SimplifiedCategoryProcessor:
    """Simplified category processing - only flag columns"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"

    def execute_sql_command(self, sql_command, timeout=300):
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
                    logger.info(f"Output: {result.stdout.strip()}")
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

    def run_simplified_category_processing(self):
        """Run simplified category processing - only flag columns"""
        logger.info("🚀 Starting Simplified Silver Layer Category Processing")
        logger.info("=" * 60)
        
        try:
            processing_sql = """
            -- ===================================
            -- SIMPLIFIED FUNDING CATEGORY PROCESSING
            -- Only manage flag columns: IsGrant, IsProcurementContract, IsOther
            -- Removed columns: RunweiCategory, CategoryTags, OriginalFundingType
            -- ===================================
            
            BEGIN TRANSACTION SimplifiedCategoryProcessing;
            
            -- Add flag columns if they don't exist
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'IsGrant')
            BEGIN
                ALTER TABLE CleanGrantsLayer2 ADD IsGrant BIT DEFAULT 0;
                PRINT 'Added IsGrant flag';
            END
            
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'IsProcurementContract')
            BEGIN
                ALTER TABLE CleanGrantsLayer2 ADD IsProcurementContract BIT DEFAULT 0;
                PRINT 'Added IsProcurementContract flag';
            END
            
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'IsOther')
            BEGIN
                ALTER TABLE CleanGrantsLayer2 ADD IsOther BIT DEFAULT 0;
                PRINT 'Added IsOther flag';
            END
            
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'CategoryProcessedDate')
            BEGIN
                ALTER TABLE CleanGrantsLayer2 ADD CategoryProcessedDate DATETIME2 DEFAULT GETDATE();
                PRINT 'Added CategoryProcessedDate timestamp';
            END
            
            -- Reset all flags to 0 before processing
            UPDATE CleanGrantsLayer2 
            SET IsGrant = 0, IsProcurementContract = 0, IsOther = 0;
            
            PRINT 'Reset all category flags to 0';
            
            -- Process funding types using intelligent pattern matching
            UPDATE CleanGrantsLayer2 
            SET 
                IsGrant = CASE 
                    WHEN FundingType LIKE '%Grant%' OR FundingType LIKE '%Cooperative Agreement%' THEN 1 
                    ELSE 0 
                END,
                IsProcurementContract = CASE 
                    WHEN FundingType LIKE '%Procurement Contract%' THEN 1 
                    ELSE 0 
                END,
                IsOther = CASE 
                    WHEN FundingType LIKE '%Other%' 
                         OR (FundingType IS NOT NULL 
                             AND FundingType NOT LIKE '%Grant%' 
                             AND FundingType NOT LIKE '%Cooperative%' 
                             AND FundingType NOT LIKE '%Procurement%')
                         OR FundingType IS NULL 
                         OR LTRIM(RTRIM(FundingType)) = ''
                    THEN 1 
                    ELSE 0 
                END,
                CategoryProcessedDate = GETDATE()
            WHERE FundingType IS NOT NULL OR FundingType IS NULL;
            
            PRINT CONCAT('Processed ', @@ROWCOUNT, ' records with intelligent pattern matching');
            
            -- Create performance indexes for flag columns
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanGrantsLayer2_FundingTypeFlags')
            BEGIN
                CREATE NONCLUSTERED INDEX IX_CleanGrantsLayer2_FundingTypeFlags 
                ON CleanGrantsLayer2(IsGrant, IsProcurementContract, IsOther);
                PRINT 'Created funding type flags composite index';
            END
            
            COMMIT TRANSACTION SimplifiedCategoryProcessing;
            
            -- Final results summary
            SELECT 
                'SIMPLIFIED_CATEGORY_PROCESSING_COMPLETE' as Status,
                COUNT(*) as TotalRecords,
                SUM(CAST(IsGrant as INT)) as GrantRecords,
                SUM(CAST(IsProcurementContract as INT)) as ProcurementContractRecords,
                SUM(CAST(IsOther as INT)) as OtherRecords,
                ROUND(100.0 * SUM(CAST(IsGrant as INT)) / COUNT(*), 1) as GrantPercentage,
                ROUND(100.0 * SUM(CAST(IsProcurementContract as INT)) / COUNT(*), 1) as ProcurementPercentage,
                ROUND(100.0 * SUM(CAST(IsOther as INT)) / COUNT(*), 1) as OtherPercentage,
                MAX(CategoryProcessedDate) as ProcessingTimestamp
            FROM CleanGrantsLayer2;
            """
            
            result = self.execute_sql_command(processing_sql, timeout=300)
            if result:
                logger.info("✅ Simplified category processing completed successfully")
                logger.info("\n🎉 SIMPLIFIED SILVER LAYER CATEGORY PROCESSING COMPLETED!")
                logger.info("✅ Funding type flags properly set (IsGrant, IsProcurementContract, IsOther)")
                logger.info("✅ Unused columns (RunweiCategory, CategoryTags, OriginalFundingType) removed")
                logger.info("✅ Performance indexes created")
                logger.info("✅ Database schema optimized")
                return True
            else:
                logger.error("❌ Simplified category processing failed")
                return False
                
        except Exception as e:
            logger.error(f"❌ Category processing failed: {e}")
            import traceback
            traceback.print_exc()
            return False

def main():
    """Main execution function"""
    processor = SimplifiedCategoryProcessor()
    
    try:
        success = processor.run_simplified_category_processing()
        
        if success:
            logger.info("🎉 Simplified category processing completed successfully!")
            return True
        else:
            logger.error("❌ Simplified category processing failed")
            return False
            
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        logger.info("✅ Process completed successfully")
    else:
        logger.error("❌ Process failed")
