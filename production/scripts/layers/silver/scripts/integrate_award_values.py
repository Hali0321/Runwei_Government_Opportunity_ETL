#!/usr/bin/env python3
"""
Azure SQL Database Award Value Integrator - Layer 2 Processing
Integrates and formats award values according to Runwei standards
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
        logging.FileHandler(PYCACHE_DIR / 'layer2_award_values.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AwardValueIntegrator:
    """Award value integration with Runwei formatting standards"""
    
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

    def run_complete_award_integration(self):
        """Run complete award value integration - Pipeline Controller Interface"""
        logger.info("💰 Running award values integration...")
        
        award_sql = """
        -- Runwei award formatting standards
        UPDATE CleanGrantsLayer2 
        SET EstimatedTotalFunding = 
            CASE 
                WHEN EstimatedTotalFunding LIKE '$%USD' THEN EstimatedTotalFunding
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, ',', ''), '$', ''), ' ', '') AS BIGINT) IS NOT NULL
                AND EstimatedTotalFunding IS NOT NULL 
                AND LTRIM(RTRIM(EstimatedTotalFunding)) != ''
                THEN '$' + FORMAT(TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, ',', ''), '$', ''), ' ', '') AS BIGINT), 'N0') + ' USD'
                ELSE EstimatedTotalFunding
            END
        WHERE EstimatedTotalFunding IS NOT NULL;
        
        UPDATE CleanGrantsLayer2 
        SET ProcessedBy = 'runwei_award_integration_master'
        WHERE EstimatedTotalFunding LIKE '$%USD';
        
        SELECT 'AWARD_INTEGRATION_SUCCESS' as Status, COUNT(*) as Records_Processed FROM CleanGrantsLayer2;
        """
        
        result = self.execute_sql_command(award_sql)
        return result is not None and 'AWARD_INTEGRATION_SUCCESS' in str(result)

def main():
    """Main execution"""
    print("💰 Award Value Integrator - Runwei Standards")
    
    integrator = AwardValueIntegrator()
    success = integrator.run_complete_award_integration()
    
    if success:
        print("✅ Award integration completed successfully. Check logs for details.")
    else:
        print("❌ Award integration failed. Check logs for details.")

if __name__ == "__main__":
    main()