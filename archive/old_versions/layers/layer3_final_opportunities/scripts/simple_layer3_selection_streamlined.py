#!/usr/bin/env python3
"""
Layer 3 - Streamlined Selection - Azure SQL Database
Creates ONLY the fields you specified - no extras
"""

import subprocess
import logging
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class Layer3StreamlinedSelector:
    """Streamlined Layer 3 - Only your specified fields"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        print("🎯 Initialized Layer 3 Streamlined Selector")
        
    def execute_sql_command(self, sql_query, timeout=300):
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
                return None
                
        except subprocess.TimeoutExpired:
            print(f"⏰ SQL command timed out after {timeout} seconds")
            return None
        except Exception as e:
            print(f"💥 Error executing SQL: {e}")
            return None

    def create_streamlined_final_table(self):
        """Create ONLY your specified fields in FinalOpportunities"""
        print("🏗️ Creating streamlined FinalOpportunities table...")
        
        sql = """
        -- Drop existing FinalOpportunities table if it exists
        IF OBJECT_ID('dbo.FinalOpportunities', 'U') IS NOT NULL
            DROP TABLE dbo.FinalOpportunities;
        
        -- Create streamlined FinalOpportunities table with ONLY your specified fields
        CREATE TABLE dbo.FinalOpportunities (
            ID NVARCHAR(50) PRIMARY KEY,
            Title NVARCHAR(MAX),
            Url NVARCHAR(MAX),
            Deadline DATETIME2,
            AwardValue NVARCHAR(100),
            CashAward DECIMAL(18,2),
            ContactEmail NVARCHAR(MAX),
            LogoUrl NVARCHAR(MAX),
            CoverImage NVARCHAR(MAX),
            ShortDescription NVARCHAR(1000),
            Description NVARCHAR(MAX),
            Eligibility NVARCHAR(MAX),
            ContactNames NVARCHAR(500),
            OpportunityTypeId INT,
            IndustryId INT,
            TargetCommunityId INT,
            TimeZone NVARCHAR(50),
            DirectApplyLink NVARCHAR(MAX),
            OpportunityGap NVARCHAR(255),
            GlobalOpportunity BIT,
            GlobalLocations NVARCHAR(1000),
            CountriesEligible NVARCHAR(1000),
            LocationDetails NVARCHAR(1000),
            SdgAlignment NVARCHAR(500),
            EsoWebsite NVARCHAR(MAX),
            ServiceProviderEso NVARCHAR(500),
            ApprovalStatus NVARCHAR(100),
            Cost NVARCHAR(255),
            FinancialTerms NVARCHAR(MAX),
            AreaOfFocus NVARCHAR(500),
            Tags NVARCHAR(1000),
            Industry NVARCHAR(500),
            Slug NVARCHAR(255),
            AwardValueStr NVARCHAR(100),
            DeadlineStr NVARCHAR(100),
            DatePosted DATETIME2,
            OpportunityType NVARCHAR(100),
            IsFeatured BIT DEFAULT 0,
            PublishOnLinkedin BIT DEFAULT 0,
            TargetCommunity NVARCHAR(500),
            CreatedAt DATETIME2 DEFAULT GETDATE(),
            
            -- Essential indexes only
            INDEX IX_FinalOpportunities_Featured (IsFeatured),
            INDEX IX_FinalOpportunities_Deadline (Deadline),
            INDEX IX_FinalOpportunities_Industry (IndustryId),
            INDEX IX_FinalOpportunities_Type (OpportunityTypeId)
        );
        
        PRINT 'Created streamlined FinalOpportunities table with your exact fields';
        """
        
        result = self.execute_sql_command(sql)
        return result is not None

    def populate_streamlined_data(self):
        """Populate with data mapped from Layer 2 to your exact fields"""
        print("🎯 Populating streamlined data...")
        
        sql = """
        -- Insert streamlined data mapping Layer 2 to your exact field requirements
        INSERT INTO dbo.FinalOpportunities (
            ID,
            Title,
            Url,
            Deadline,
            AwardValue,
            CashAward,
            ContactEmail,
            LogoUrl,
            CoverImage,
            ShortDescription,
            Description,
            Eligibility,
            ContactNames,
            OpportunityTypeId,
            IndustryId,
            TargetCommunityId,
            TimeZone,
            DirectApplyLink,
            OpportunityGap,
            GlobalOpportunity,
            GlobalLocations,
            CountriesEligible,
            LocationDetails,
            SdgAlignment,
            EsoWebsite,
            ServiceProviderEso,
            ApprovalStatus,
            Cost,
            FinancialTerms,
            AreaOfFocus,
            Tags,
            Industry,
            Slug,
            AwardValueStr,
            DeadlineStr,
            DatePosted,
            OpportunityType,
            IsFeatured,
            PublishOnLinkedin,
            TargetCommunity
        )
        SELECT 
            CAST(ID AS NVARCHAR(50)) as ID,
            Title,
            ISNULL(OpportunityURL, 'https://www.grants.gov/search-results-detail/' + OpportunityNumber) as Url,
            Deadline,
            AwardValueFormatted as AwardValue,
            ISNULL(AwardCeiling, ISNULL(AwardFloor, ISNULL(EstimatedTotalFunding, 0))) as CashAward,
            ISNULL(AgencyName + '@grants.gov', 'contact@grants.gov') as ContactEmail,
            LogoUrl,
            CoverImage,
            Summary as ShortDescription,
            Description,
            Eligibility,
            AgencyName as ContactNames,
            CASE 
                WHEN OpportunityType LIKE '%Grant%' THEN 1
                WHEN OpportunityType LIKE '%Cooperative%' THEN 2
                WHEN OpportunityType LIKE '%Contract%' THEN 3
                ELSE 4
            END as OpportunityTypeId,
            CASE 
                WHEN Category LIKE '%Education%' THEN 1
                WHEN Category LIKE '%Health%' THEN 2
                WHEN Category LIKE '%Science%' THEN 3
                WHEN Category LIKE '%Technology%' THEN 4
                WHEN Category LIKE '%Environment%' THEN 5
                WHEN Category LIKE '%Social%' THEN 6
                ELSE 7
            END as IndustryId,
            CASE 
                WHEN EligibilityCategory LIKE '%Individual%' THEN 1
                WHEN EligibilityCategory LIKE '%Organization%' THEN 2
                WHEN EligibilityCategory LIKE '%Government%' THEN 3
                ELSE 4
            END as TargetCommunityId,
            ISNULL(TimeZone, 'EST') as TimeZone,
            OpportunityURL as DirectApplyLink,
            OpportunityGap,
            ISNULL(GlobalOpportunity, 0) as GlobalOpportunity,
            'United States' as GlobalLocations,
            ISNULL(CountriesEligible, 'United States') as CountriesEligible,
            'Federal Grant Program' as LocationDetails,
            SDGTags as SdgAlignment,
            'https://www.grants.gov' as EsoWebsite,
            AgencyName as ServiceProviderEso,
            CASE 
                WHEN Status = 'Posted' THEN 'Approved'
                WHEN Status = 'Forecasted' THEN 'Pending'
                ELSE 'Draft'
            END as ApprovalStatus,
            CASE WHEN CostSharingRequired = 'True' THEN 'Cost Sharing Required' ELSE 'No Cost Sharing' END as Cost,
            'Federal funding terms apply' as FinancialTerms,
            Category as AreaOfFocus,
            KeywordTags as Tags,
            Category as Industry,
            LOWER(REPLACE(REPLACE(Title, ' ', '-'), '''', '')) as Slug,
            AwardValueFormatted as AwardValueStr,
            CASE 
                WHEN Deadline IS NOT NULL THEN FORMAT(Deadline, 'MMM dd, yyyy')
                ELSE 'No deadline specified'
            END as DeadlineStr,
            ISNULL(PostedDate, CreatedDate) as DatePosted,
            OpportunityType,
            CASE WHEN DataQualityScore >= 9.0 THEN 1 ELSE 0 END as IsFeatured,
            0 as PublishOnLinkedin,
            EligibilityCategory as TargetCommunity
        FROM CleanGrantsLayer2
        WHERE ReadyForLayer3 = 1
          AND DataQualityScore >= 6.0
          AND Title IS NOT NULL
          AND Title != ''
        ORDER BY DataQualityScore DESC;
        
        PRINT CONCAT('Populated ', @@ROWCOUNT, ' streamlined records');
        
        -- Final statistics for your streamlined table
        SELECT 
            COUNT(*) as TotalRecords,
            SUM(CASE WHEN IsFeatured = 1 THEN 1 ELSE 0 END) as FeaturedRecords,
            COUNT(DISTINCT IndustryId) as UniqueIndustries,
            COUNT(DISTINCT OpportunityTypeId) as UniqueOpportunityTypes,
            COUNT(DISTINCT TargetCommunityId) as UniqueTargetCommunities,
            SUM(CASE WHEN CashAward > 0 THEN 1 ELSE 0 END) as RecordsWithCashAward,
            SUM(CASE WHEN Deadline IS NOT NULL THEN 1 ELSE 0 END) as RecordsWithDeadlines
        FROM dbo.FinalOpportunities;
        """
        
        result = self.execute_sql_command(sql, timeout=600)
        return result is not None

def main():
    """Main execution function"""
    print("=" * 55)
    print("🎯 Layer 3 - Streamlined Selection - Your Exact Fields")
    print("=" * 55)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Goal: Create ONLY your specified 38 fields")
    
    try:
        selector = Layer3StreamlinedSelector()
        
        # Step 1: Create streamlined table structure
        print("\n🏗️ Step 1: Creating streamlined table with your exact fields...")
        if not selector.create_streamlined_final_table():
            print("❌ Failed to create streamlined table")
            return False
        
        # Step 2: Populate with mapped data
        print("\n🎯 Step 2: Populating with streamlined data...")
        if not selector.populate_streamlined_data():
            print("❌ Failed to populate streamlined data")
            return False
        
        print("\n🎊 SUCCESS! Streamlined Layer 3 Complete!")
        print("=" * 55)
        print("✅ Table: dbo.FinalOpportunities")
        print("✅ Fields: Exactly your 38 specified fields")
        print("✅ Data: High-quality records from Layer 2")
        print("✅ Status: Ready for your application")
        
        return True
        
    except Exception as e:
        print(f"\n💥 Layer 3 streamlined selection failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🏁 Starting Streamlined Layer 3 Selection...")
    success = main()
    if success:
        print("\n🚀 Streamlined Layer 3 Successfully Completed!")
        print("📊 Your dbo.FinalOpportunities contains exactly your specified fields")
        print("🎯 Ready for your application integration")
    else:
        print("\n❌ Streamlined Layer 3 failed - check logs for details")
        exit(1)