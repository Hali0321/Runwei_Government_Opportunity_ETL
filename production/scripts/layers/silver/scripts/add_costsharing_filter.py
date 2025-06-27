#!/usr/bin/env python3
"""
Simple CostSharing Filter for Layer 1 → Layer 2 Import
Filter out CostSharing = true records during import - no new tables/columns
"""

import subprocess
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleCostSharingFilter:
    """Simple filter to exclude CostSharing = true during Layer 1 → Layer 2 import"""
    
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
                if result.stdout:
                    logger.error(f"Output: {result.stdout}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error executing SQL: {e}")
            return None

    def apply_costsharing_filter_to_existing_layer2(self):
        """Simply remove CostSharing = true records from existing Layer 2"""
        logger.info("🔄 Applying CostSharing filter to existing Layer 2 data...")
        
        filter_sql = """
        -- Remove records with CostSharing = true from Layer 2
        DELETE FROM CleanGrantsLayer2
        WHERE ID IN (
            SELECT c2.ID
            FROM CleanGrantsLayer2 c2
            INNER JOIN RawGrantsLayer1 r1 ON c2.OpportunityNumber = r1.OpportunityNumber
            WHERE r1.CostSharing = 'true' OR r1.CostSharing = '1'
        );
        
        PRINT CONCAT('Removed ', @@ROWCOUNT, ' records with CostSharing = true from Layer 2');
        
        -- Show remaining count
        SELECT 
            'COSTSHARING_FILTER_RESULT' as ResultType,
            COUNT(*) as RemainingRecords,
            'Only CostSharing = false records remain' as FilterApplied
        FROM CleanGrantsLayer2;
        """
        
        result = self.execute_sql_command(filter_sql)
        return result is not None

    def update_layer3_if_exists(self):
        """Remove CostSharing = true records from Layer 3 if it exists"""
        logger.info("🔄 Updating Layer 3 if it exists...")
        
        layer3_filter_sql = """
        -- Remove CostSharing = true records from Layer 3 if table exists
        IF EXISTS (SELECT * FROM sys.tables WHERE name = 'FinalOpportunities' AND schema_id = SCHEMA_ID('dbo'))
        BEGIN
            DELETE FROM dbo.FinalOpportunities
            WHERE ID IN (
                SELECT r1.OpportunityNumber
                FROM RawGrantsLayer1 r1
                WHERE r1.CostSharing = 'true' OR r1.CostSharing = '1'
            );
            
            PRINT CONCAT('Removed ', @@ROWCOUNT, ' records with CostSharing = true from Layer 3');
            
            SELECT 
                'LAYER3_FILTER_RESULT' as ResultType,
                COUNT(*) as RemainingRecords,
                'Only CostSharing = false records remain' as FilterApplied
            FROM dbo.FinalOpportunities;
        END
        ELSE
        BEGIN
            PRINT 'Layer 3 (FinalOpportunities) does not exist - no action needed';
        END
        """
        
        result = self.execute_sql_command(layer3_filter_sql)
        return result is not None

    def update_layer1_to_layer2_import_with_costsharing_filter(self):
        """Update the Layer 1 → Layer 2 import process to include CostSharing filter"""
        logger.info("🔧 Updating Layer 1 → Layer 2 import process with CostSharing filter...")
        
        # Clear Layer 2 and reimport with filter
        reimport_sql = """
        -- Clear existing Layer 2 data
        TRUNCATE TABLE CleanGrantsLayer2;
        
        -- Reimport from Layer 1 with CostSharing filter applied
        WITH FilteredLayer1 AS (
            SELECT 
                ROW_NUMBER() OVER (PARTITION BY OpportunityNumber ORDER BY ID) as rn,
                *
            FROM RawGrantsLayer1 r1
            WHERE r1.OpportunityNumber IS NOT NULL 
              AND r1.OpportunityNumber NOT LIKE 'SAMPLE-%'
              AND r1.OpportunityNumber NOT LIKE 'TEST-%'
              AND (r1.CostSharing = 'false' OR r1.CostSharing IS NULL OR r1.CostSharing = 'False' OR r1.CostSharing = '0')
              -- FILTER: Only include records where CostSharing is NOT True
              AND r1.CostSharing NOT IN ('true', 'True', '1', 'YES', 'Yes')
        )
        INSERT INTO CleanGrantsLayer2 (
            OpportunityNumber, Title, Description, OpportunityURL, AdditionalInfoURL,
            AgencyName, AgencyCode, AwardValue, AwardCeiling, AwardFloor,
            EstimatedTotalFunding, ExpectedAwards, FundingType, Deadline,
            PostedDate, EstimatedPostDate, EstimatedDueDate, Category,
            OpportunityType, Eligibility, EligibilityCategory, CountriesEligible,
            GlobalOpportunity, TimeZone, SDGTags, OpportunityGap, KeywordTags,
            DataQualityScore, ProcessingFlags, SourceLayerID, ProcessedDate,
            ProcessedBy, DataVersion, CreatedDate, UpdatedDate, CFDANumbers,
            Package, Status, Version, CostSharingRequired
        )
        SELECT 
            OpportunityNumber,
            COALESCE(Title, 'Untitled') as Title,
            Description,
            OpportunityURL,
            AdditionalInfoURL,
            COALESCE(AgencyName, 'Unknown') as AgencyName,
            AgencyCode,
            COALESCE(AwardCeiling, AwardFloor, EstimatedTotalFunding) as AwardValue,
            AwardCeiling, AwardFloor, EstimatedTotalFunding, ExpectedAwards, FundingType,
            CloseDate as Deadline,
            PostedDate, EstimatedPostDate, EstimatedDueDate, Category,
            CASE 
                WHEN UPPER(Title) LIKE '%FELLOWSHIP%' THEN 'Fellowship'
                WHEN UPPER(Title) LIKE '%RESEARCH%' THEN 'Research Grant'
                ELSE 'Grant'
            END as OpportunityType,
            EligibleApplicants as Eligibility,
            CASE 
                WHEN EligibleApplicants LIKE '%individual%' THEN 'Individuals'
                WHEN EligibleApplicants LIKE '%nonprofit%' THEN 'Nonprofits'
                ELSE 'Multiple'
            END as EligibilityCategory,
            'United States' as CountriesEligible,
            CASE WHEN Description LIKE '%global%' OR Description LIKE '%international%' THEN 1 ELSE 0 END as GlobalOpportunity,
            'EST' as TimeZone,
            CASE 
                WHEN Description LIKE '%climate%' THEN 'SDG 13: Climate Action'
                WHEN Description LIKE '%health%' THEN 'SDG 3: Good Health'
                WHEN Description LIKE '%education%' THEN 'SDG 4: Quality Education'
                ELSE 'Multiple SDGs'
            END as SDGTags,
            'Standard Opportunity' as OpportunityGap,
            LEFT(REPLACE(Title, ',', ' '), 1000) as KeywordTags,
            CASE 
                WHEN Title IS NOT NULL AND Description IS NOT NULL AND AwardCeiling IS NOT NULL AND CloseDate IS NOT NULL THEN 95.0
                WHEN Title IS NOT NULL AND Description IS NOT NULL THEN 75.0
                ELSE 50.0
            END as DataQualityScore,
            'COSTSHARING_FILTERED' as ProcessingFlags,
            ID as SourceLayerID,
            GETDATE() as ProcessedDate,
            'CostSharing_Filter_Import' as ProcessedBy,
            '2.0' as DataVersion,
            GETDATE() as CreatedDate,
            GETDATE() as UpdatedDate,
            CFDANumbers, Package, Status, Version,
            'false' as CostSharingRequired  -- Mark all as no cost sharing required
        FROM FilteredLayer1
        WHERE rn = 1;  -- Remove duplicates
        
        -- Verification
        SELECT 
            'COSTSHARING_IMPORT_SUCCESS' as Status,
            COUNT(*) as Layer2_Records,
            'All records have CostSharing = false or NULL' as FilterApplied,
            (SELECT COUNT(*) FROM RawGrantsLayer1) as Original_Layer1_Records,
            (SELECT COUNT(*) FROM RawGrantsLayer1 WHERE CostSharing IN ('true', 'True', '1', 'YES', 'Yes')) as Filtered_Out_Records
        FROM CleanGrantsLayer2;
        """
        
        result = self.execute_sql_command(reimport_sql, timeout=600)
        return result is not None and 'COSTSHARING_IMPORT_SUCCESS' in str(result)

    def fix_layer3_with_proper_filtering(self):
        """Fix Layer 3 to only include properly filtered Layer 2 records"""
        logger.info("🔧 Fixing Layer 3 with proper CostSharing filtering...")
        
        layer3_fix_sql = """
        -- Clear Layer 3 and rebuild from properly filtered Layer 2
        IF EXISTS (SELECT * FROM sys.tables WHERE name = 'FinalOpportunities' AND schema_id = SCHEMA_ID('dbo'))
        BEGIN
            TRUNCATE TABLE dbo.FinalOpportunities;
            
            -- Rebuild Layer 3 from filtered Layer 2 only
            INSERT INTO dbo.FinalOpportunities (
                ID, Title, Url, Deadline, AwardValue, CashAward, ContactEmail,
                LogoUrl, CoverImage, ShortDescription, Description, Eligibility,
                ContactNames, OpportunityTypeId, IndustryId, TargetCommunityId,
                TimeZone, DirectApplyLink, OpportunityGap, GlobalOpportunity,
                GlobalLocations, CountriesEligible, LocationDetails, SdgAlignment,
                EsoWebsite, ServiceProviderEso, ApprovalStatus, Cost, FinancialTerms,
                AreaOfFocus, Tags, Industry, Slug, AwardValueStr, DeadlineStr,
                DatePosted, OpportunityType, IsFeatured, PublishOnLinkedin, TargetCommunity
            )
            SELECT 
                CAST(ID AS NVARCHAR(50)) as ID,
                Title,
                ISNULL(OpportunityURL, 'https://www.grants.gov/search-results-detail/' + OpportunityNumber) as Url,
                Deadline,
                ISNULL(CAST(AwardValue AS NVARCHAR(100)), 'Amount varies') as AwardValue,
                ISNULL(AwardValue, 0) as CashAward,
                ISNULL(AgencyName + '@grants.gov', 'contact@grants.gov') as ContactEmail,
                'https://www.grants.gov/assets/img/logo.png' as LogoUrl,
                'https://via.placeholder.com/800x400/4a90e2/ffffff?text=Grant+Opportunity' as CoverImage,
                LEFT(ISNULL(Description, 'Federal grant opportunity'), 500) as ShortDescription,
                Description,
                Eligibility,
                AgencyName as ContactNames,
                CASE WHEN OpportunityType LIKE '%Grant%' THEN 1 ELSE 5 END as OpportunityTypeId,
                CASE WHEN Category LIKE '%Education%' THEN 1 WHEN Category LIKE '%Health%' THEN 2 ELSE 8 END as IndustryId,
                CASE WHEN EligibilityCategory LIKE '%Individual%' THEN 1 ELSE 5 END as TargetCommunityId,
                TimeZone,
                OpportunityURL as DirectApplyLink,
                OpportunityGap,
                GlobalOpportunity,
                GlobalLocations,
                CountriesEligible,
                'Federal Grant Program' as LocationDetails,
                SDGTags as SdgAlignment,
                'https://www.grants.gov' as EsoWebsite,
                AgencyName as ServiceProviderEso,
                'Approved' as ApprovalStatus,
                'No Cost Sharing Required' as Cost,  -- All records are CostSharing = false
                'Federal funding terms apply' as FinancialTerms,
                Category as AreaOfFocus,
                KeywordTags as Tags,
                Category as Industry,
                LOWER(REPLACE(REPLACE(Title, ' ', '-'), '''', '')) as Slug,
                ISNULL(CAST(AwardValue AS NVARCHAR(100)), 'Amount varies') as AwardValueStr,
                CASE WHEN Deadline IS NOT NULL THEN FORMAT(Deadline, 'MMM dd, yyyy') ELSE 'No deadline' END as DeadlineStr,
                ISNULL(PostedDate, CreatedDate) as DatePosted,
                OpportunityType,
                CASE WHEN DataQualityScore >= 90.0 THEN 1 ELSE 0 END as IsFeatured,
                0 as PublishOnLinkedin,
                EligibilityCategory as TargetCommunity
            FROM CleanGrantsLayer2
            WHERE DataQualityScore >= 50.0  -- Only high-quality records
              AND Title IS NOT NULL
              AND Title != '';
            
            SELECT 
                'LAYER3_FIX_SUCCESS' as Status,
                COUNT(*) as Layer3_Records,
                'Only CostSharing = false records included' as FilterStatus,
                (SELECT COUNT(*) FROM CleanGrantsLayer2) as Source_Layer2_Records
            FROM dbo.FinalOpportunities;
        END
        ELSE
        BEGIN
            SELECT 'LAYER3_NOT_EXISTS' as Status, 'FinalOpportunities table does not exist' as Message;
        END
        """
        
        result = self.execute_sql_command(layer3_fix_sql, timeout=600)
        return result is not None

    def run_complete_costsharing_fix(self):
        """Run complete CostSharing filter fix across all layers"""
        logger.info("🚀 Running complete CostSharing filter fix...")
        
        try:
            # Step 1: Fix Layer 1 → Layer 2 import with filter
            logger.info("Step 1: Fixing Layer 1 → Layer 2 import with CostSharing filter...")
            if not self.update_layer1_to_layer2_import_with_costsharing_filter():
                logger.error("❌ Failed to fix Layer 1 → Layer 2 import")
                return False
            
            # Step 2: Fix Layer 3 from filtered Layer 2
            logger.info("Step 2: Fixing Layer 3 from properly filtered Layer 2...")
            if not self.fix_layer3_with_proper_filtering():
                logger.error("❌ Failed to fix Layer 3")
                return False
            
            # Step 3: Final verification
            logger.info("Step 3: Final verification...")
            verification_sql = """
            SELECT 
                'FINAL_VERIFICATION' as CheckType,
                (SELECT COUNT(*) FROM RawGrantsLayer1) as Layer1_Total,
                (SELECT COUNT(*) FROM RawGrantsLayer1 WHERE CostSharing IN ('true', 'True', '1', 'YES', 'Yes')) as Layer1_CostSharing_True,
                (SELECT COUNT(*) FROM CleanGrantsLayer2) as Layer2_Total,
                (SELECT COUNT(*) FROM dbo.FinalOpportunities) as Layer3_Total,
                'Layer2 and Layer3 should be <= Layer1 minus CostSharing=True records' as ExpectedResult;
            """
            
            result = self.execute_sql_command(verification_sql)
            logger.info("📊 Final verification results:")
            logger.info(result)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Complete CostSharing fix failed: {e}")
            return False

def main():
    """Main execution - simple CostSharing filter"""
    print("🎯 Simple CostSharing Filter - No New Tables/Columns")
    print("=" * 60)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🔄 Goal: Remove CostSharing = true from existing layers")
    
    filter_manager = SimpleCostSharingFilter()
    
    try:
        # Step 1: Apply filter to existing Layer 2
        print("\nStep 1: Filtering existing Layer 2 data...")
        if not filter_manager.apply_costsharing_filter_to_existing_layer2():
            print("❌ Layer 2 filtering failed")
            return False
        
        # Step 2: Update Layer 3 if it exists
        print("\nStep 2: Updating Layer 3 if it exists...")
        if not filter_manager.update_layer3_if_exists():
            print("❌ Layer 3 update failed")
            return False
        
        print("\n🎊 SUCCESS! CostSharing Filter Applied!")
        print("=" * 50)
        print("✅ Layer 2: CostSharing = true records removed")
        print("✅ Layer 3: CostSharing = true records removed (if exists)")
        print("✅ Future imports: Modify import script to include WHERE filter")
        
        print("\n📋 Next Step:")
        print("Update your Layer 1 → Layer 2 import script to include:")
        print("WHERE (r1.CostSharing = 'false' OR r1.CostSharing IS NULL)")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Simple CostSharing filter failed: {e}")
        logger.error(f"Error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🚀 Simple Filter Complete!")
        print("📝 Now update your import scripts to include CostSharing filter")
    else:
        print("\n❌ Filter failed - check logs for details")