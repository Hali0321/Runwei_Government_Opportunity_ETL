#!/usr/bin/env python3
"""
Value Transformer for Azure SQL Database Layer 2 Processing
Final value standardization and quality optimization
"""

import subprocess
import logging
from datetime import datetime
from pathlib import Path

# FIXED: Configure logging to __pycache__ folder
SCRIPT_DIR = Path(__file__).parent
PYCACHE_DIR = SCRIPT_DIR / "__pycache__"
PYCACHE_DIR.mkdir(exist_ok=True)  # Ensure __pycache__ exists

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    handlers=[
        logging.FileHandler(PYCACHE_DIR / 'transform_values.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ValueTransformer:
    """Final value transformation for CleanGrantsLayer2"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        
    def execute_sql_command(self, sql_query, timeout=300):
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

    def transform_values(self):
        """Transform and standardize various field values - FIXED for Azure SQL"""
        logger.info("🔄 Transforming values...")
        
        # FIXED: Split into smaller operations to avoid data type conflicts
        transformation_sql = """
        -- Step 1: Standardize Status values
        UPDATE CleanGrantsLayer2 
        SET Status = 
            CASE 
                WHEN Status LIKE '%Posted%' OR Status LIKE '%posted%' THEN 'Posted'
                WHEN Status LIKE '%Closed%' OR Status LIKE '%closed%' THEN 'Closed'
                WHEN Status LIKE '%Archived%' OR Status LIKE '%archived%' THEN 'Archived'
                WHEN Status LIKE '%Forecast%' OR Status LIKE '%forecast%' THEN 'Forecast'
                WHEN Status IS NULL OR LTRIM(RTRIM(Status)) = '' THEN 'Unknown'
                ELSE Status
            END;
        
        PRINT CONCAT('Status standardization: ', @@ROWCOUNT, ' records updated');
        
        -- Step 2: Clean and standardize Version field
        UPDATE CleanGrantsLayer2 
        SET Version = 
            CASE 
                WHEN Version IS NULL OR LTRIM(RTRIM(Version)) = '' THEN '1.0'
                WHEN TRY_CAST(Version AS FLOAT) IS NOT NULL THEN Version
                ELSE '1.0'
            END;
        
        PRINT CONCAT('Version standardization: ', @@ROWCOUNT, ' records updated');
        
        -- Step 3: Update final processing timestamp
        UPDATE CleanGrantsLayer2 
        SET UpdatedDate = GETDATE()
        WHERE UpdatedDate IS NULL;
        
        PRINT CONCAT('Timestamp update: ', @@ROWCOUNT, ' records updated');
        
        SELECT 'STEP_1_SUCCESS' as Status, COUNT(*) as Records_Updated FROM CleanGrantsLayer2;
        """
        
        result1 = self.execute_sql_command(transformation_sql, timeout=300)
        if result1 is None or 'STEP_1_SUCCESS' not in str(result1):
            logger.error("❌ Step 1 transformation failed")
            return False
        
        # FIXED: Separate quality score calculation with proper data type handling
        quality_score_sql = """
        -- Step 4: FIXED Data Quality Score calculation with proper type handling
        UPDATE CleanGrantsLayer2 
        SET DataQualityScore = 
            CAST(
                -- Title component (20 points max)
                (CASE WHEN Title IS NOT NULL AND LEN(LTRIM(RTRIM(Title))) > 0 THEN 20.0 ELSE 0.0 END) +
                -- Description component (20 points max)
                (CASE WHEN Description IS NOT NULL AND LEN(LTRIM(RTRIM(Description))) > 50 THEN 20.0 ELSE 10.0 END) +
                -- Agency component (15 points max)
                (CASE WHEN AgencyName IS NOT NULL AND LEN(LTRIM(RTRIM(AgencyName))) > 0 THEN 15.0 ELSE 0.0 END) +
                -- Category component (15 points max)
                (CASE WHEN RunweiCategory IS NOT NULL AND RunweiCategory != 'Other' THEN 15.0 ELSE 5.0 END) +
                -- Funding component (15 points max)
                (CASE WHEN EstimatedTotalFunding IS NOT NULL AND LTRIM(RTRIM(EstimatedTotalFunding)) != '' THEN 15.0 ELSE 0.0 END) +
                -- Deadline component (10 points max)
                (CASE WHEN Deadline IS NOT NULL THEN 10.0 ELSE 0.0 END) +
                -- URL bonus (5 points max)
                (CASE WHEN OpportunityURL IS NOT NULL AND OpportunityURL LIKE 'http%' THEN 5.0 ELSE 0.0 END)
            AS DECIMAL(10,2)
            );
        
        PRINT CONCAT('Quality score recalculation: ', @@ROWCOUNT, ' records updated');
        
        SELECT 'STEP_2_SUCCESS' as Status, COUNT(*) as Records_Updated FROM CleanGrantsLayer2;
        """
        
        result2 = self.execute_sql_command(quality_score_sql, timeout=300)
        if result2 is None or 'STEP_2_SUCCESS' not in str(result2):
            logger.error("❌ Step 2 quality score calculation failed")
            return False
        
        # Step 5: Final verification and statistics
        verification_sql = """
        SELECT 
            'VALUE_TRANSFORMATION_SUCCESS' as Status, 
            COUNT(*) as Records_Processed,
            CAST(AVG(CAST(DataQualityScore AS FLOAT)) AS DECIMAL(10,2)) as Avg_Quality_Score,
            COUNT(CASE WHEN CAST(DataQualityScore AS FLOAT) >= 90.0 THEN 1 END) as High_Quality_Records,
            COUNT(CASE WHEN CAST(DataQualityScore AS FLOAT) >= 95.0 THEN 1 END) as Premium_Quality_Records,
            COUNT(CASE WHEN Status = 'Posted' THEN 1 END) as Posted_Opportunities,
            COUNT(CASE WHEN RunweiCategory = 'Grant' THEN 1 END) as Grant_Opportunities
        FROM CleanGrantsLayer2;
        """
        
        result3 = self.execute_sql_command(verification_sql, timeout=300)
        return result3 is not None and 'VALUE_TRANSFORMATION_SUCCESS' in str(result3)

    def run_complete_value_transformation(self):
        """Run complete value transformation - Pipeline Controller Interface"""
        logger.info("🔄 Running value transformation...")
        logger.info("🔄 Transforming values...")
        
        try:
            success = self.transform_values()
            
            if success:
                logger.info("✅ Value transformation completed successfully")
                return True
            else:
                logger.error("❌ Value transformation failed")
                return False
                
        except Exception as e:
            logger.error(f"❌ Value transformation error: {e}")
            return False

def main():
    """Main execution"""
    print("🔄 Value Transformer - Final Value Standardization and Quality Optimization")
    
    transformer = ValueTransformer()
    success = transformer.run_complete_value_transformation()
    
    if success:
        print("✅ Value transformation completed successfully. Check logs for details.")
    else:
        print("❌ Value transformation failed. Check logs for details.")

if __name__ == "__main__":
    main()