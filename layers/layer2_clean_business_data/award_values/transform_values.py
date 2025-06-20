#!/usr/bin/env python3
"""
Layer 2 Award Values Processing
Comprehensive award value transformation for BusinessIntelligenceLayer2
"""

import os
import subprocess
from datetime import datetime
import logging

# Setup Azure-optimized logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('layer2_award_values.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Layer2AwardValuesProcessor:
    """Layer 2 Award Values Processing for BusinessIntelligenceLayer2"""
    
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
                "-C", "-t", str(timeout), "-I"
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
    
    def process_award_values(self):
        """Process award values for Layer 2"""
        logger.info("💰 Processing award values for Layer 2...")
        
        award_sql = """
        -- ===================================
        -- LAYER 2: AWARD VALUES PROCESSING
        -- Clean and standardize award values in BusinessIntelligenceLayer2
        -- ===================================
        
        BEGIN TRANSACTION Layer2AwardProcessing;
        
        -- Add award processing columns if they don't exist
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('BusinessIntelligenceLayer2') AND name = 'AwardValue_Clean')
        BEGIN
            ALTER TABLE BusinessIntelligenceLayer2 ADD 
                AwardValue_Clean DECIMAL(18,2),
                EstimatedTotalFunding_Clean DECIMAL(18,2),
                ExpectedAwards_Clean INT,
                AwardValuePerAward DECIMAL(18,2),
                AwardCategory NVARCHAR(50),
                AwardProcessingDate DATETIME2 DEFAULT GETDATE();
        END
        
        -- Process award values
        UPDATE BusinessIntelligenceLayer2
        SET 
            -- Clean AwardValue
            AwardValue_Clean = CASE 
                WHEN AwardValue IS NULL OR LTRIM(RTRIM(AwardValue)) = '' THEN NULL
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardValue, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL 
                THEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardValue, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2))
                ELSE NULL
            END,
            
            -- Clean EstimatedTotalFunding
            EstimatedTotalFunding_Clean = CASE 
                WHEN EstimatedTotalFunding IS NULL OR LTRIM(RTRIM(EstimatedTotalFunding)) = '' THEN NULL
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL 
                THEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2))
                ELSE NULL
            END,
            
            -- Clean ExpectedAwards
            ExpectedAwards_Clean = CASE 
                WHEN ExpectedAwards IS NULL OR LTRIM(RTRIM(ExpectedAwards)) = '' THEN NULL
                WHEN TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT) IS NOT NULL 
                THEN TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT)
                ELSE NULL
            END,
            
            AwardProcessingDate = GETDATE();
        
        -- Calculate derived values
        UPDATE BusinessIntelligenceLayer2
        SET 
            AwardValuePerAward = CASE 
                WHEN EstimatedTotalFunding_Clean IS NOT NULL 
                 AND ExpectedAwards_Clean IS NOT NULL 
                 AND ExpectedAwards_Clean > 0
                THEN EstimatedTotalFunding_Clean / ExpectedAwards_Clean
                ELSE NULL
            END,
            
            AwardCategory = CASE 
                WHEN ISNULL(EstimatedTotalFunding_Clean, AwardValue_Clean) >= 100000000 THEN 'MEGA_FUNDING'
                WHEN ISNULL(EstimatedTotalFunding_Clean, AwardValue_Clean) >= 10000000 THEN 'LARGE_FUNDING'
                WHEN ISNULL(EstimatedTotalFunding_Clean, AwardValue_Clean) >= 1000000 THEN 'MEDIUM_FUNDING'
                WHEN ISNULL(EstimatedTotalFunding_Clean, AwardValue_Clean) >= 100000 THEN 'SMALL_FUNDING'
                WHEN ISNULL(EstimatedTotalFunding_Clean, AwardValue_Clean) > 0 THEN 'MICRO_FUNDING'
                ELSE 'UNSPECIFIED'
            END;
        
        COMMIT TRANSACTION Layer2AwardProcessing;
        
        SELECT 
            'LAYER2_AWARD_PROCESSING_COMPLETE' as Status,
            COUNT(*) as TotalRecords,
            COUNT(AwardValue_Clean) as RecordsWithCleanAwardValue,
            COUNT(EstimatedTotalFunding_Clean) as RecordsWithCleanFunding,
            GETDATE() as ProcessingTimestamp
        FROM BusinessIntelligenceLayer2;
        """
        
        result = self.execute_sql_command(award_sql)
        if result:
            logger.info("✅ Layer 2 award values processed successfully")
            logger.info(result)
            return True
        else:
            logger.error("❌ Layer 2 award values processing failed")
            return False

def main():
    processor = Layer2AwardValuesProcessor()
    processor.process_award_values()

if __name__ == "__main__":
    main()