#!/usr/bin/env python3
"""
Layer 2 Status Check - Verify your current progress
"""

import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_layer2_status():
    """Check current Layer 2 implementation status"""
    
    server = "grants-gov-sql-server.database.windows.net"
    database = "GrantsGovDB"
    username = "grantsadmin"
    password = "Grant$Admin2024!"
    
    # Check what tables exist in Layer 2
    check_sql = """
    -- Check Layer 2 tables and data
    SELECT 
        'EXISTING_TABLES' as CheckType,
        TABLE_NAME,
        (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = t.TABLE_NAME) as ColumnCount
    FROM INFORMATION_SCHEMA.TABLES t
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME;
    
    -- Check BusinessIntelligenceLayer3 (your current analytics table)
    SELECT 
        'LAYER2_DATA_STATUS' as CheckType,
        COUNT(*) as TotalRecords,
        COUNT(CASE WHEN IsActive = 1 THEN 1 END) as ActiveRecords,
        COUNT(CASE WHEN CreatedDate >= DATEADD(day, -7, GETDATE()) THEN 1 END) as RecentRecords
    FROM BusinessIntelligenceLayer3;
    """
    
    try:
        cmd = [
            "sqlcmd", "-S", server, "-d", database, 
            "-U", username, "-P", password,
            "-Q", check_sql, "-C", "-I"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            logger.info("✅ Layer 2 Status Check Results:")
            logger.info(result.stdout)
            return True
        else:
            logger.error(f"❌ Status check failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error checking status: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Checking Layer 2 Status...")
    check_layer2_status()