#!/usr/bin/env python3
"""
Layer 3 - FinalOpportunities Selection - Azure SQL Database
Updates your existing FinalOpportunities table with fresh Layer 2 data including Runwei award values
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

class Layer3AdaptiveSelector:
    """Update existing FinalOpportunities with fresh Layer 2 data"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        print("🎯 Initialized Layer 3 FinalOpportunities Updater")
        
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

    def update_final_opportunities_with_fresh_data(self):
        """Update existing FinalOpportunities with fresh Layer 2 data including Runwei awards"""
        print("🔄 Updating FinalOpportunities with fresh Layer 2 data including Runwei award formatting...")
        
        sql = """
        -- Clear existing data and repopulate with fresh Layer 2 data
        DELETE FROM dbo.FinalOpportunities;
        
        -- Insert fresh data from Layer 2 with Runwei-formatted awards AND CostSharing filter
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
            
            -- Use the NEW Runwei-formatted award values from Layer 2 award integration
            CASE 
                WHEN EstimatedTotalFunding LIKE '$%USD' THEN EstimatedTotalFunding
                WHEN AwardCeiling LIKE '$%USD' THEN AwardCeiling
                WHEN AwardFloor LIKE '$%USD' THEN AwardFloor
                WHEN EstimatedTotalFunding IS NOT NULL AND EstimatedTotalFunding != '' THEN EstimatedTotalFunding
                ELSE 'Amount varies'
            END as AwardValue,
            
            -- Extract numeric value for CashAward from Runwei-formatted strings
            CASE 
                WHEN EstimatedTotalFunding LIKE '$%USD' THEN 
                    TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' USD', '') AS DECIMAL(18,2))
                WHEN AwardCeiling LIKE '$%USD' THEN 
                    TRY_CAST(REPLACE(REPLACE(REPLACE(AwardCeiling, '$', ''), ',', ''), ' USD', '') AS DECIMAL(18,2))
                WHEN AwardFloor LIKE '$%USD' THEN 
                    TRY_CAST(REPLACE(REPLACE(REPLACE(AwardFloor, '$', ''), ',', ''), ' USD', '') AS DECIMAL(18,2))
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(ISNULL(EstimatedTotalFunding, '0'), '$', ''), ',', ''), ' ', '') AS DECIMAL(18,2)) IS NOT NULL
                THEN TRY_CAST(REPLACE(REPLACE(REPLACE(ISNULL(EstimatedTotalFunding, '0'), '$', ''), ',', ''), ' ', '') AS DECIMAL(18,2))
                ELSE 0
            END as CashAward,
            
            ISNULL(AgencyName + '@grants.gov', 'contact@grants.gov') as ContactEmail,
            ISNULL(LogoUrl, 'https://www.grants.gov/assets/img/logo.png') as LogoUrl,
            ISNULL(CoverImage, 'https://via.placeholder.com/800x400/4a90e2/ffffff?text=Grant+Opportunity') as CoverImage,
            Summary as ShortDescription,
            Description,
            Eligibility,
            AgencyName as ContactNames,
            CASE 
                WHEN OpportunityType LIKE '%Grant%' THEN 1
                WHEN OpportunityType LIKE '%Cooperative%' THEN 2
                WHEN OpportunityType LIKE '%Contract%' THEN 3
                WHEN OpportunityType LIKE '%Fellowship%' THEN 4
                ELSE 5
            END as OpportunityTypeId,
            CASE 
                WHEN Category LIKE '%Education%' THEN 1
                WHEN Category LIKE '%Health%' THEN 2
                WHEN Category LIKE '%Science%' THEN 3
                WHEN Category LIKE '%Technology%' THEN 4
                WHEN Category LIKE '%Environment%' THEN 5
                WHEN Category LIKE '%Social%' THEN 6
                WHEN Category LIKE '%Arts%' THEN 7
                ELSE 8
            END as IndustryId,
            CASE 
                WHEN EligibilityCategory LIKE '%Individual%' THEN 1
                WHEN EligibilityCategory LIKE '%Organization%' THEN 2
                WHEN EligibilityCategory LIKE '%Government%' THEN 3
                WHEN EligibilityCategory LIKE '%Non-Profit%' THEN 4
                ELSE 5
            END as TargetCommunityId,
            ISNULL(TimeZone, 'EST') as TimeZone,
            OpportunityURL as DirectApplyLink,
            OpportunityGap,
            ISNULL(CAST(GlobalOpportunity AS BIT), 0) as GlobalOpportunity,
            'United States' as GlobalLocations,
            ISNULL(CountriesEligible, 'United States') as CountriesEligible,
            'Federal Grant Program' as LocationDetails,
            SDGTags as SdgAlignment,
            'https://www.grants.gov' as EsoWebsite,
            AgencyName as ServiceProviderEso,
            CASE 
                WHEN Status = 'Posted' THEN 'Approved'
                WHEN Status = 'Forecasted' THEN 'Pending'
                WHEN Status = 'Closed' THEN 'Closed'
                ELSE 'Approved'
            END as ApprovalStatus,
            'No Cost Sharing Required' as Cost,  -- All records are CostSharing = false
            'Federal funding terms apply' as FinancialTerms,
            Category as AreaOfFocus,
            KeywordTags as Tags,
            Category as Industry,
            LOWER(REPLACE(REPLACE(Title, ' ', '-'), '''', '')) as Slug,
            
            -- Use Runwei-formatted award values for display
            CASE 
                WHEN EstimatedTotalFunding LIKE '$%USD' THEN EstimatedTotalFunding
                WHEN AwardCeiling LIKE '$%USD' THEN AwardCeiling
                WHEN AwardFloor LIKE '$%USD' THEN AwardFloor
                WHEN EstimatedTotalFunding IS NOT NULL AND EstimatedTotalFunding != '' THEN EstimatedTotalFunding
                ELSE 'Amount varies'
            END as AwardValueStr,
            
            CASE 
                WHEN Deadline IS NOT NULL THEN FORMAT(Deadline, 'MMM dd, yyyy')
                ELSE 'No deadline specified'
            END as DeadlineStr,
            ISNULL(PostedDate, CreatedDate) as DatePosted,
            OpportunityType,
            CASE WHEN DataQualityScore >= 9.0 THEN 1 ELSE 0 END as IsFeatured,
            0 as PublishOnLinkedin,
            EligibilityCategory as TargetCommunity
        FROM dbo.EligibleGrantsLayer2  -- <-- CHANGED: Now uses filtered view
        WHERE ReadyForLayer3 = 1
          AND DataQualityScore >= 6.0
          AND Title IS NOT NULL
          AND Title != ''
          -- Prioritize records with Runwei award formatting
        ORDER BY 
            CASE WHEN ProcessedBy = 'runwei_award_integration_ultra_safe' THEN 0 ELSE 1 END,
            DataQualityScore DESC;
        
        PRINT CONCAT('Updated FinalOpportunities with ', @@ROWCOUNT, ' fresh CostSharing-filtered records including Runwei award formatting');
        
        -- Final statistics for your FinalOpportunities table
        SELECT 
            'FINALOPPORTUNITIES_UPDATE_SUCCESS' as Status,
            COUNT(*) as TotalRecords,
            SUM(CASE WHEN IsFeatured = 1 THEN 1 ELSE 0 END) as FeaturedRecords,
            COUNT(DISTINCT IndustryId) as UniqueIndustries,
            COUNT(DISTINCT OpportunityTypeId) as UniqueOpportunityTypes,
            COUNT(DISTINCT TargetCommunityId) as UniqueTargetCommunities,
            SUM(CASE WHEN CashAward > 0 THEN 1 ELSE 0 END) as RecordsWithCashAward,
            SUM(CASE WHEN Deadline IS NOT NULL THEN 1 ELSE 0 END) as RecordsWithDeadlines,
            SUM(CASE WHEN AwardValue LIKE '$%USD' THEN 1 ELSE 0 END) as RunweiFormattedAwards,
            AVG(CashAward) as AverageCashAward,
            MAX(CashAward) as MaxCashAward,
            'CostSharing=false filter applied' as BusinessRuleApplied,
            GETDATE() as UpdateTimestamp
        FROM dbo.FinalOpportunities;
        
        -- Show sample of Runwei-formatted awards
        SELECT TOP 10
            'RUNWEI_AWARD_SAMPLES' as ReportType,
            Title,
            AwardValue,
            CashAward,
            AwardValueStr
        FROM dbo.FinalOpportunities
        WHERE AwardValue LIKE '$%USD'
        ORDER BY CashAward DESC;
        """
        
        result = self.execute_sql_command(sql, timeout=600)
        return result is not None and 'FINALOPPORTUNITIES_UPDATE_SUCCESS' in str(result)

def main():
    """Main execution function"""
    print("=" * 70)
    print("🎯 FinalOpportunities Update with Runwei Award Values")
    print("=" * 70)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Goal: Update FinalOpportunities with fresh Layer 2 award formatting")
    print("🔄 Source: CleanGrantsLayer2 with Runwei award integration")
    
    try:
        selector = Layer3AdaptiveSelector()
        
        # Update FinalOpportunities with fresh data including Runwei awards
        print("\n🔄 Updating FinalOpportunities with fresh Layer 2 data + Runwei awards...")
        if not selector.update_final_opportunities_with_fresh_data():
            print("❌ Failed to update FinalOpportunities with fresh data")
            return False
        
        print("\n🎊 SUCCESS! FinalOpportunities Updated!")
        print("=" * 70)
        print("✅ Table: dbo.FinalOpportunities")
        print("✅ Data: Fresh from Layer 2 with Runwei award formatting")
        print("✅ Awards: $X,XXX USD format applied where possible")
        print("✅ Quality: Only records with DataQualityScore >= 6.0")
        print("✅ Prioritized: Records with Runwei award formatting first")
        
        print("\n📊 Your FinalOpportunities now includes:")
        print("   💰 Runwei-formatted award values ($X,XXX USD)")
        print("   📈 Quality-scored opportunities")
        print("   🎯 All your exact 38 fields")
        print("   🔄 Fresh data from Layer 2")
        
        print("\n📊 Quick Verification Queries:")
        print("   SELECT COUNT(*) FROM dbo.FinalOpportunities;")
        print("   SELECT * FROM dbo.FinalOpportunities WHERE AwardValue LIKE '$%USD' ORDER BY CashAward DESC;")
        print("   SELECT * FROM dbo.FinalOpportunities WHERE IsFeatured = 1;")
        
        return True
        
    except Exception as e:
        print(f"\n💥 FinalOpportunities update failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🏁 Starting FinalOpportunities Update...")
    success = main()
    if success:
        print("\n🚀 FinalOpportunities Successfully Updated!")
        print("📊 Your FinalOpportunities table now has fresh Layer 2 data with Runwei awards")
        print("🎯 Ready for your application!")
    else:
        print("\n❌ FinalOpportunities update failed - check logs for details")
        exit(1)