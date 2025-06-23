#!/usr/bin/env python3
"""
Layer 2 - Comprehensive Data Enhancement - Azure SQL Database
Fixed version with ACTUAL column names from CleanGrantsLayer2
"""

import subprocess
import logging
from datetime import datetime
import time
import sys

# Configure logging to show output immediately
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class Layer2ComprehensiveEnhancer:
    """Comprehensive Layer 2 data enhancement - FIXED with actual column names"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        print("🔧 Initialized Layer 2 Enhancer (FIXED VERSION)")
        
    def execute_sql_command(self, sql_query, timeout=600):
        """Execute SQL command with Azure SQL Database optimizations"""
        print(f"📊 Executing SQL command (timeout: {timeout}s)...")
        try:
            cmd = [
                "sqlcmd", "-S", self.server, "-d", self.database, 
                "-U", self.username, "-P", self.password,
                "-Q", sql_query, "-C", "-t", str(timeout), "-I", "-b"
            ]
            
            print("🔄 Running sqlcmd...")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 30)
            
            if result.returncode == 0:
                print("✅ SQL command executed successfully")
                if result.stdout and result.stdout.strip():
                    print(f"📋 Output: {result.stdout.strip()}")
                return result.stdout
            else:
                print(f"❌ SQL command failed with return code {result.returncode}")
                if result.stderr:
                    print(f"🔴 Error: {result.stderr}")
                if result.stdout:
                    print(f"📋 Output: {result.stdout}")
                return None
                
        except subprocess.TimeoutExpired:
            print(f"⏰ SQL command timed out after {timeout} seconds")
            return None
        except Exception as e:
            print(f"💥 Error executing SQL: {e}")
            return None

    def comprehensive_enhancement(self):
        """Run comprehensive data enhancement using ACTUAL column names"""
        print("🚀 Running comprehensive data enhancement with FIXED column names...")
        
        sql = """
        BEGIN TRANSACTION ComprehensiveEnhancement;
        
        -- Step 1: Generate LogoUrl and visual assets using ACTUAL column names
        UPDATE CleanGrantsLayer2
        SET LogoUrl = CASE
            WHEN LogoUrl IS NOT NULL AND LogoUrl != '' THEN LogoUrl
            WHEN AgencyName LIKE '%Department%' OR AgencyName LIKE '%Agency%' THEN
                'https://www.grants.gov/assets/img/logo.png'
            WHEN AgencyName LIKE '%University%' OR AgencyName LIKE '%Educational%' THEN
                'https://via.placeholder.com/150x150/1f4e79/ffffff?text=EDU'
            WHEN AgencyName LIKE '%Foundation%' OR AgencyName LIKE '%Non-Profit%' THEN
                'https://via.placeholder.com/150x150/2d5a87/ffffff?text=NPO'
            WHEN Category LIKE '%Health%' OR Category LIKE '%Medical%' THEN
                'https://via.placeholder.com/150x150/dc2626/ffffff?text=HEALTH'
            ELSE 'https://via.placeholder.com/150x150/4a90e2/ffffff?text=GRANT'
        END,
        CoverImage = CASE
            WHEN CoverImage IS NOT NULL AND CoverImage != '' THEN CoverImage
            WHEN Category LIKE '%Research%' THEN
                'https://via.placeholder.com/800x400/1e3a8a/ffffff?text=Research+Grant'
            WHEN Category LIKE '%Innovation%' THEN
                'https://via.placeholder.com/800x400/7c3aed/ffffff?text=Innovation+Grant'
            WHEN Category LIKE '%Health%' THEN
                'https://via.placeholder.com/800x400/dc2626/ffffff?text=Health+Grant'
            WHEN Category LIKE '%Education%' THEN
                'https://via.placeholder.com/800x400/059669/ffffff?text=Education+Grant'
            WHEN Category LIKE '%Environment%' THEN
                'https://via.placeholder.com/800x400/16a34a/ffffff?text=Environment+Grant'
            WHEN Category LIKE '%Technology%' THEN
                'https://via.placeholder.com/800x400/6366f1/ffffff?text=Tech+Grant'
            ELSE 'https://via.placeholder.com/800x400/6b7280/ffffff?text=Grant+Opportunity'
        END;
        
        PRINT CONCAT('Enhanced visual assets for ', @@ROWCOUNT, ' records');
        
        -- Step 2: Generate Summary from Description (ACTUAL column name)
        UPDATE CleanGrantsLayer2
        SET Summary = CASE
            WHEN Description IS NOT NULL AND LEN(Description) BETWEEN 50 AND 300 THEN
                LEFT(Description, 250) + CASE WHEN LEN(Description) > 250 THEN '...' ELSE '' END
            WHEN Description IS NOT NULL AND LEN(Description) > 20 THEN
                LEFT(Description, 250) + CASE WHEN LEN(Description) > 250 THEN '...' ELSE '' END
            WHEN Title IS NOT NULL THEN
                Title + ' - ' + ISNULL(Category, 'Grant opportunity') + ' providing federal funding.'
            ELSE 'Federal grant opportunity providing funding and support for eligible applicants.'
        END;
        
        PRINT CONCAT('Generated Summary for ', @@ROWCOUNT, ' records');
        
        -- Step 3: Format AwardValue using ACTUAL column names
        UPDATE CleanGrantsLayer2
        SET AwardValueFormatted = CASE
            WHEN AwardCeiling IS NOT NULL AND AwardCeiling > 0 THEN
                '$' + FORMAT(AwardCeiling, 'N0') + ' USD'
            WHEN AwardFloor IS NOT NULL AND AwardFloor > 0 THEN
                'From $' + FORMAT(AwardFloor, 'N0') + ' USD'
            WHEN EstimatedTotalFunding IS NOT NULL AND EstimatedTotalFunding > 0 THEN
                'Total: $' + FORMAT(EstimatedTotalFunding, 'N0') + ' USD'
            WHEN AwardValue IS NOT NULL AND AwardValue > 0 THEN
                '$' + FORMAT(AwardValue, 'N0') + ' USD'
            ELSE 'Amount varies'
        END;
        
        PRINT CONCAT('Formatted AwardValue for ', @@ROWCOUNT, ' records');
        
        -- Step 4: Calculate quality scores using ACTUAL column names
        UPDATE CleanGrantsLayer2
        SET DataQualityScore = (
            CASE WHEN Title IS NOT NULL AND LEN(Title) > 10 THEN 2.0 ELSE 0 END +
            CASE WHEN Description IS NOT NULL AND LEN(Description) > 50 THEN 2.0 ELSE 0 END +
            CASE WHEN Summary IS NOT NULL AND LEN(Summary) > 20 THEN 1.0 ELSE 0 END +
            CASE WHEN AgencyName IS NOT NULL AND AgencyName != '' THEN 1.0 ELSE 0 END +
            CASE WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN 2.0 ELSE 0 END +
            CASE WHEN Eligibility IS NOT NULL AND LEN(Eligibility) > 20 THEN 1.0 ELSE 0 END +
            CASE WHEN Category IS NOT NULL AND Category != '' THEN 1.0 ELSE 0 END
        ),
        EnhancementStatus = CASE
            WHEN (CASE WHEN Title IS NOT NULL AND LEN(Title) > 10 THEN 2.0 ELSE 0 END +
                  CASE WHEN Description IS NOT NULL AND LEN(Description) > 50 THEN 2.0 ELSE 0 END +
                  CASE WHEN Summary IS NOT NULL AND LEN(Summary) > 20 THEN 1.0 ELSE 0 END +
                  CASE WHEN AgencyName IS NOT NULL AND AgencyName != '' THEN 1.0 ELSE 0 END +
                  CASE WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN 2.0 ELSE 0 END +
                  CASE WHEN Eligibility IS NOT NULL AND LEN(Eligibility) > 20 THEN 1.0 ELSE 0 END +
                  CASE WHEN Category IS NOT NULL AND Category != '' THEN 1.0 ELSE 0 END) >= 8.0 
                THEN 'Excellent - Production Ready'
            WHEN (CASE WHEN Title IS NOT NULL AND LEN(Title) > 10 THEN 2.0 ELSE 0 END +
                  CASE WHEN Description IS NOT NULL AND LEN(Description) > 50 THEN 2.0 ELSE 0 END +
                  CASE WHEN Summary IS NOT NULL AND LEN(Summary) > 20 THEN 1.0 ELSE 0 END +
                  CASE WHEN AgencyName IS NOT NULL AND AgencyName != '' THEN 1.0 ELSE 0 END +
                  CASE WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN 2.0 ELSE 0 END +
                  CASE WHEN Eligibility IS NOT NULL AND LEN(Eligibility) > 20 THEN 1.0 ELSE 0 END +
                  CASE WHEN Category IS NOT NULL AND Category != '' THEN 1.0 ELSE 0 END) >= 6.0 
                THEN 'Good - Enhanced and ready'
            ELSE 'Needs improvement'
        END,
        ReadyForLayer3 = CASE
            WHEN Title IS NOT NULL 
                AND Title != ''
                AND Summary IS NOT NULL
                AND AwardValueFormatted IS NOT NULL
                AND (CASE WHEN Title IS NOT NULL AND LEN(Title) > 10 THEN 2.0 ELSE 0 END +
                     CASE WHEN Description IS NOT NULL AND LEN(Description) > 50 THEN 2.0 ELSE 0 END +
                     CASE WHEN Summary IS NOT NULL AND LEN(Summary) > 20 THEN 1.0 ELSE 0 END +
                     CASE WHEN AgencyName IS NOT NULL AND AgencyName != '' THEN 1.0 ELSE 0 END +
                     CASE WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN 2.0 ELSE 0 END +
                     CASE WHEN Eligibility IS NOT NULL AND LEN(Eligibility) > 20 THEN 1.0 ELSE 0 END +
                     CASE WHEN Category IS NOT NULL AND Category != '' THEN 1.0 ELSE 0 END) >= 6.0
                THEN 1
            ELSE 0
        END,
        EnhancementDate = GETDATE();
        
        PRINT CONCAT('Calculated quality scores for ', @@ROWCOUNT, ' records');
        
        COMMIT TRANSACTION ComprehensiveEnhancement;
        
        -- Get final statistics
        SELECT 
            COUNT(*) as TotalRecords,
            AVG(DataQualityScore) as AvgQuality,
            SUM(CASE WHEN ReadyForLayer3 = 1 THEN 1 ELSE 0 END) as ReadyForLayer3Count,
            ROUND(100.0 * SUM(CASE WHEN ReadyForLayer3 = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) as Layer3ReadyPercentage
        FROM CleanGrantsLayer2;
        """
        
        result = self.execute_sql_command(sql, timeout=600)
        if result is not None:
            print("✅ Comprehensive enhancement completed successfully")
            return True
        else:
            print("❌ Failed to run comprehensive enhancement")
            return False

def main():
    """Main execution function"""
    print("=" * 70)
    print("🚀 Layer 2 - FIXED Enhancement - Azure SQL Database")
    print("=" * 70)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Goal: Comprehensive data enhancement using ACTUAL column names")
    
    try:
        enhancer = Layer2ComprehensiveEnhancer()
        
        # Run comprehensive enhancement with correct column names
        print("\n🚀 Running comprehensive data enhancement with FIXED column names...")
        if not enhancer.comprehensive_enhancement():
            print("❌ Failed to enhance data")
            return False
        
        print("\n🎊 SUCCESS! Layer 2 FIXED Enhancement Complete!")
        print("=" * 70)
        print("✅ Visual Assets: LogoUrl and CoverImage generated")
        print("✅ Content: Summary field generated from Description")
        print("✅ Financial: AwardValueFormatted from actual award columns")
        print("✅ Quality: Comprehensive scoring using real column names")
        print("✅ Readiness: Records marked ready for Layer 3 selection")
        
        return True
        
    except Exception as e:
        print(f"\n💥 Layer 2 enhancement failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🏁 Starting Layer 2 FIXED Enhancement...")
    success = main()
    if success:
        print("\n🚀 Layer 2 FIXED Enhancement Successfully Completed!")
        print("📊 Your CleanGrantsLayer2 contains fully enhanced data")
        print("🎯 Ready for Layer 3 simple selection")
    else:
        print("\n❌ Layer 2 enhancement failed - check logs for details")
        exit(1)
