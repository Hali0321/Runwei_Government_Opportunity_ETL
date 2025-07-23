#!/usr/bin/env python3
"""
🔍 LOGO URL STATUS CHECKER
Quick script to verify LogoUrl column status in CleanGrantsLayer2
"""

import subprocess
from datetime import datetime

def execute_sql(sql):
    """Execute SQL command using sqlcmd."""
    try:
        cmd = [
            "sqlcmd",
            "-S", "grants-gov-sql-server.database.windows.net",
            "-d", "GrantsGovDB", 
            "-U", "grantsadmin",
            "-P", "Grant$Admin2024!",
            "-Q", sql,
            "-b", "-C"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode != 0:
            return None
        
        return result.stdout.strip()
        
    except Exception as e:
        print(f"Error: {e}")
        return None

def check_logo_status():
    """Check the current status of LogoUrl column."""
    print("🔍 LOGO URL STATUS CHECK")
    print("=" * 50)
    print(f"📅 Checked: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Overall stats
    print("📊 OVERALL STATISTICS:")
    stats_sql = """
    SELECT 
        COUNT(*) as Total_Records,
        COUNT(LogoUrl) as Records_With_Logo,
        COUNT(DISTINCT AgencyName) as Total_Agencies,
        COUNT(DISTINCT LogoUrl) as Unique_Logo_URLs,
        ROUND((COUNT(LogoUrl) * 100.0) / COUNT(*), 1) as Coverage_Percent
    FROM CleanGrantsLayer2;
    """
    
    result = execute_sql(stats_sql)
    if result:
        print(result)
    else:
        print("❌ Failed to get statistics")
        return False
    
    print()
    print("🏛️ TOP 10 AGENCIES BY GRANT COUNT:")
    top_agencies_sql = """
    SELECT TOP 10
        AgencyName,
        COUNT(*) as Grant_Count,
        LogoUrl
    FROM CleanGrantsLayer2
    WHERE LogoUrl IS NOT NULL
    GROUP BY AgencyName, LogoUrl
    ORDER BY Grant_Count DESC;
    """
    
    result = execute_sql(top_agencies_sql)
    if result:
        print(result)
    
    print()
    print("🖼️ SAMPLE LOGO URLs:")
    sample_sql = """
    SELECT TOP 5
        AgencyName,
        LogoUrl
    FROM CleanGrantsLayer2 
    WHERE LogoUrl IS NOT NULL
    ORDER BY AgencyName;
    """
    
    result = execute_sql(sample_sql)
    if result:
        print(result)
    
    print()
    print("=" * 50)
    print("✅ STATUS CHECK COMPLETED!")
    print("🎯 If you see data above, LogoUrl is working correctly!")
    return True

def main():
    """Main function."""
    return check_logo_status()

if __name__ == "__main__":
    main()
