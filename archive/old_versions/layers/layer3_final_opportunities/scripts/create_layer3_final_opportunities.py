#!/usr/bin/env python3
"""
Create Layer 3 Final Opportunities Table - AZURE SQL DATABASE FINAL FIX
Transform Layer 2 cleaned grants data into final opportunities format
Fixed for all Azure SQL Database specific constraints and limitations
"""

import subprocess
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Layer3AzureSQLFinal:
    """Create final opportunities table from Layer 2 - Azure SQL Database fully optimized"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        
    def execute_sql_command(self, sql_query, timeout=600):
        """Execute SQL command with proper Azure SQL Database error handling"""
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
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ SQL command timed out after {timeout} seconds")
            return None
        except Exception as e:
            logger.error(f"❌ Error executing SQL: {e}")
            return None

    def check_table_exists(self, table_name):
        """Check if table exists in Azure SQL Database"""
        check_sql = f"""
        SELECT 
            CASE WHEN EXISTS (
                SELECT * FROM sys.tables 
                WHERE name = '{table_name}' AND schema_id = SCHEMA_ID('dbo')
            ) THEN 'EXISTS' ELSE 'NOT_EXISTS' END AS TableStatus;
        """
        
        result = self.execute_sql_command(check_sql)
        if result and 'EXISTS' in result:
            return 'EXISTS' in result and 'NOT_EXISTS' not in result
        return False

    def drop_table_if_exists(self, table_name):
        """Safely drop table if it exists"""
        logger.info(f"🗑️ Checking and dropping table {table_name} if exists...")
        
        drop_sql = f"""
        IF EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}' AND schema_id = SCHEMA_ID('dbo'))
        BEGIN
            DROP TABLE dbo.{table_name};
            PRINT 'Table {table_name} dropped successfully';
        END
        ELSE
        BEGIN
            PRINT 'Table {table_name} does not exist';
        END
        """
        
        result = self.execute_sql_command(drop_sql)
        return result is not None

    def create_final_opportunities_structure(self):
        """Create the table structure with Azure SQL Database compatible data types"""
        logger.info("📋 Creating FinalOpportunities table structure...")
        
        create_sql = """
        CREATE TABLE dbo.FinalOpportunities (
            ID NVARCHAR(200) NOT NULL,
            Title NVARCHAR(500),           -- Changed from MAX to allow indexing
            Url NVARCHAR(2000),            -- Changed from MAX for better performance
            Deadline NVARCHAR(100),
            AwardValue NVARCHAR(100),
            CashAward NVARCHAR(100),
            ContactEmail NVARCHAR(500),
            LogoUrl NVARCHAR(2000),        -- Changed from MAX
            CoverImage NVARCHAR(2000),     -- Changed from MAX
            ShortDescription NVARCHAR(1000), -- Changed from MAX
            Description NVARCHAR(4000),    -- Changed from MAX but kept larger
            Eligibility NVARCHAR(4000),    -- Changed from MAX but kept larger
            ContactNames NVARCHAR(1000),
            OpportunityTypeId NVARCHAR(50),
            IndustryId NVARCHAR(50),
            TargetCommunityId NVARCHAR(50),
            TimeZone NVARCHAR(100),
            DirectApplyLink NVARCHAR(2000), -- Changed from MAX
            OpportunityGap NVARCHAR(2000),  -- Changed from MAX
            GlobalOpportunity NVARCHAR(10),
            GlobalLocations NVARCHAR(2000), -- Changed from MAX
            CountriesEligible NVARCHAR(2000), -- Changed from MAX
            LocationDetails NVARCHAR(2000), -- Changed from MAX
            SdgAlignment NVARCHAR(2000),    -- Changed from MAX
            EsoWebsite NVARCHAR(2000),      -- Changed from MAX
            ServiceProviderEso NVARCHAR(500),
            ApprovalStatus NVARCHAR(50),
            Cost NVARCHAR(100),
            FinancialTerms NVARCHAR(2000),  -- Changed from MAX
            AreaOfFocus NVARCHAR(500),      -- Changed from MAX
            Tags NVARCHAR(2000),            -- Changed from MAX
            Industry NVARCHAR(200),         -- Changed from MAX to allow indexing
            Slug NVARCHAR(500),
            AwardValueStr NVARCHAR(100),
            DeadlineStr NVARCHAR(100),
            DatePosted NVARCHAR(100),
            OpportunityType NVARCHAR(100),
            IsFeatured NVARCHAR(10),
            PublishOnLinkedin NVARCHAR(10),
            TargetCommunity NVARCHAR(500),
            CreatedAt NVARCHAR(100),
            
            CONSTRAINT PK_FinalOpportunities PRIMARY KEY (ID)
        );
        
        PRINT 'FinalOpportunities table created successfully';
        """
        
        result = self.execute_sql_command(create_sql)
        if result:
            time.sleep(2)
            if self.check_table_exists('FinalOpportunities'):
                logger.info("✅ Table structure verified as created")
                return True
            else:
                logger.error("❌ Table creation verification failed")
                return False
        return False

    def create_indexes(self):
        """Create indexes with proper Azure SQL Database syntax"""
        logger.info("📊 Creating indexes...")
        
        indexes = [
            # Index on Title (now that it's NVARCHAR(500))
            "CREATE NONCLUSTERED INDEX IX_FinalOpportunities_Title ON dbo.FinalOpportunities (Title) WHERE Title IS NOT NULL;",
            # Index on Industry (now that it's NVARCHAR(200))
            "CREATE NONCLUSTERED INDEX IX_FinalOpportunities_Industry ON dbo.FinalOpportunities (Industry) WHERE Industry IS NOT NULL;",
            # Index on OpportunityType 
            "CREATE NONCLUSTERED INDEX IX_FinalOpportunities_OpportunityType ON dbo.FinalOpportunities (OpportunityType) WHERE OpportunityType IS NOT NULL;",
            # Index on Deadline
            "CREATE NONCLUSTERED INDEX IX_FinalOpportunities_Deadline ON dbo.FinalOpportunities (Deadline) WHERE Deadline IS NOT NULL;",
            # Index on IsFeatured for filtering
            "CREATE NONCLUSTERED INDEX IX_FinalOpportunities_IsFeatured ON dbo.FinalOpportunities (IsFeatured) WHERE IsFeatured IS NOT NULL;",
            # Index on ApprovalStatus
            "CREATE NONCLUSTERED INDEX IX_FinalOpportunities_ApprovalStatus ON dbo.FinalOpportunities (ApprovalStatus) WHERE ApprovalStatus IS NOT NULL;"
        ]
        
        for idx, index_sql in enumerate(indexes, 1):
            logger.info(f"Creating index {idx}/{len(indexes)}...")
            result = self.execute_sql_command(index_sql)
            if not result:
                logger.warning(f"Index {idx} creation may have failed, continuing...")
            time.sleep(1)
        
        logger.info("✅ Index creation completed")
        return True

    def populate_all_data(self):
        """Populate table with all data at once - fixed Azure SQL syntax"""
        logger.info("📊 Populating FinalOpportunities with all data...")
        
        # Use ROW_NUMBER() instead of TOP with OFFSET for Azure SQL compatibility
        populate_sql = """
        WITH SourceData AS (
            SELECT 
                OpportunityNumber,
                [Title],
                OpportunityURL,
                [Deadline],
                EstimatedTotalFunding,
                AwardValue,
                [Description],
                Eligibility,
                AgencyName,
                Category,
                EligibilityCategory,
                TimeZone,
                AdditionalInfoURL,
                OpportunityGap,
                GlobalOpportunity,
                CountriesEligible,
                SDGTags,
                [Status],
                KeywordTags,
                FundingType,
                PostedDate,
                CreatedDate,
                ROW_NUMBER() OVER (ORDER BY OpportunityNumber) as RowNum
            FROM CleanGrantsLayer2
            WHERE OpportunityNumber IS NOT NULL
              AND [Title] IS NOT NULL
        )
        INSERT INTO dbo.FinalOpportunities (
            ID, Title, Url, Deadline, AwardValue, CashAward, ContactEmail, LogoUrl, CoverImage,
            ShortDescription, Description, Eligibility, ContactNames, OpportunityTypeId, IndustryId,
            TargetCommunityId, TimeZone, DirectApplyLink, OpportunityGap, GlobalOpportunity,
            GlobalLocations, CountriesEligible, LocationDetails, SdgAlignment, EsoWebsite,
            ServiceProviderEso, ApprovalStatus, Cost, FinancialTerms, AreaOfFocus, Tags, Industry,
            Slug, AwardValueStr, DeadlineStr, DatePosted, OpportunityType, IsFeatured,
            PublishOnLinkedin, TargetCommunity, CreatedAt
        )
        SELECT 
            -- Use OpportunityNumber as unique ID
            LTRIM(RTRIM(OpportunityNumber)) AS ID,
            
            -- Title (truncated to fit NVARCHAR(500))
            LEFT(LTRIM(RTRIM([Title])), 500) AS Title,
            
            -- URL (truncated to fit NVARCHAR(2000))
            CASE 
                WHEN OpportunityURL IS NOT NULL AND LTRIM(RTRIM(OpportunityURL)) != '' 
                THEN LEFT(LTRIM(RTRIM(OpportunityURL)), 2000)
                ELSE NULL
            END AS Url,
            
            -- Deadline
            CASE 
                WHEN [Deadline] IS NOT NULL AND TRY_CAST([Deadline] AS DATETIME2) IS NOT NULL
                THEN FORMAT(TRY_CAST([Deadline] AS DATETIME2), 'yyyy-MM-dd HH:mm:ss')
                ELSE NULL 
            END AS Deadline,
            
            -- Award Value
            CASE 
                WHEN EstimatedTotalFunding IS NOT NULL AND LTRIM(RTRIM(EstimatedTotalFunding)) != ''
                THEN LEFT(LTRIM(RTRIM(EstimatedTotalFunding)), 100)
                WHEN AwardValue IS NOT NULL AND LTRIM(RTRIM(AwardValue)) != ''
                THEN LEFT(LTRIM(RTRIM(AwardValue)), 100)
                ELSE NULL
            END AS AwardValue,
            
            -- Cash Award (same as Award Value)
            CASE 
                WHEN EstimatedTotalFunding IS NOT NULL AND LTRIM(RTRIM(EstimatedTotalFunding)) != ''
                THEN LEFT(LTRIM(RTRIM(EstimatedTotalFunding)), 100)
                WHEN AwardValue IS NOT NULL AND LTRIM(RTRIM(AwardValue)) != ''
                THEN LEFT(LTRIM(RTRIM(AwardValue)), 100)
                ELSE NULL
            END AS CashAward,
            
            NULL AS ContactEmail,  -- Not available in Layer 2
            NULL AS LogoUrl,       -- Not available in Layer 2
            NULL AS CoverImage,    -- Not available in Layer 2
            
            -- Short Description (truncated to fit NVARCHAR(1000))
            CASE 
                WHEN [Title] IS NOT NULL AND AgencyName IS NOT NULL
                THEN LEFT(LTRIM(RTRIM([Title])) + ' - ' + LTRIM(RTRIM(AgencyName)), 1000)
                WHEN [Title] IS NOT NULL
                THEN LEFT(LTRIM(RTRIM([Title])), 1000)
                ELSE 'Grant Opportunity'
            END AS ShortDescription,
            
            -- Full Description (truncated to fit NVARCHAR(4000))
            CASE 
                WHEN [Description] IS NOT NULL AND LTRIM(RTRIM([Description])) != ''
                THEN LEFT(LTRIM(RTRIM([Description])), 4000)
                ELSE LEFT('Grant opportunity from ' + ISNULL(LTRIM(RTRIM(AgencyName)), 'Government Agency'), 4000)
            END AS Description,
            
            -- Eligibility (truncated to fit NVARCHAR(4000))
            CASE 
                WHEN Eligibility IS NOT NULL AND LTRIM(RTRIM(Eligibility)) != ''
                THEN LEFT(LTRIM(RTRIM(Eligibility)), 4000)
                ELSE 'See opportunity details for eligibility requirements'
            END AS Eligibility,
            
            -- Contact Names (from Agency, truncated)
            LEFT(LTRIM(RTRIM(ISNULL(AgencyName, 'Government Agency'))), 1000) AS ContactNames,
            
            -- OpportunityTypeId (mapped from Category)
            CASE 
                WHEN Category LIKE '%Research%' THEN '1'
                WHEN Category LIKE '%Education%' THEN '2'
                WHEN Category LIKE '%Health%' THEN '3'
                WHEN Category LIKE '%Technology%' OR Category LIKE '%Science%' THEN '4'
                WHEN Category LIKE '%Arts%' OR Category LIKE '%Humanities%' THEN '5'
                WHEN Category LIKE '%Environment%' THEN '6'
                WHEN Category LIKE '%Business%' THEN '7'
                ELSE '8'
            END AS OpportunityTypeId,
            
            -- IndustryId (mapped from AgencyName)
            CASE 
                WHEN AgencyName LIKE '%Health%' OR AgencyName LIKE '%NIH%' OR AgencyName LIKE '%CDC%' THEN '1'
                WHEN AgencyName LIKE '%Education%' OR AgencyName LIKE '%NSF%' THEN '2'
                WHEN AgencyName LIKE '%Defense%' OR AgencyName LIKE '%DOD%' THEN '3'
                WHEN AgencyName LIKE '%Energy%' OR AgencyName LIKE '%EPA%' THEN '4'
                WHEN AgencyName LIKE '%Commerce%' OR AgencyName LIKE '%SBA%' THEN '5'
                WHEN AgencyName LIKE '%Agriculture%' OR AgencyName LIKE '%USDA%' THEN '6'
                WHEN AgencyName LIKE '%Transportation%' THEN '7'
                WHEN AgencyName LIKE '%Arts%' OR AgencyName LIKE '%Humanities%' THEN '8'
                ELSE '9'
            END AS IndustryId,
            
            '1' AS TargetCommunityId,  -- Default
            
            -- TimeZone
            ISNULL(NULLIF(LTRIM(RTRIM(TimeZone)), ''), 'EST') AS TimeZone,
            
            -- Direct Apply Link (truncated)
            CASE 
                WHEN OpportunityURL IS NOT NULL AND LTRIM(RTRIM(OpportunityURL)) != ''
                THEN LEFT(LTRIM(RTRIM(OpportunityURL)), 2000)
                WHEN AdditionalInfoURL IS NOT NULL AND LTRIM(RTRIM(AdditionalInfoURL)) != ''
                THEN LEFT(LTRIM(RTRIM(AdditionalInfoURL)), 2000)
                ELSE NULL
            END AS DirectApplyLink,
            
            -- Opportunity Gap (truncated)
            CASE 
                WHEN OpportunityGap IS NOT NULL AND LTRIM(RTRIM(OpportunityGap)) != ''
                THEN LEFT(LTRIM(RTRIM(OpportunityGap)), 2000)
                ELSE NULL
            END AS OpportunityGap,
            
            -- Global Opportunity
            ISNULL(NULLIF(LTRIM(RTRIM(GlobalOpportunity)), ''), 'Yes') AS GlobalOpportunity,
            
            'United States' AS GlobalLocations,
            
            -- Countries Eligible (truncated)
            LEFT(ISNULL(NULLIF(LTRIM(RTRIM(CountriesEligible)), ''), 'United States'), 2000) AS CountriesEligible,
            
            -- Location Details (truncated)
            LEFT(LTRIM(RTRIM(ISNULL(AgencyName, 'Government Agency') + ' - ' + ISNULL(Category, 'General'))), 2000) AS LocationDetails,
            
            -- SDG Alignment (truncated)
            CASE 
                WHEN SDGTags IS NOT NULL AND LTRIM(RTRIM(SDGTags)) != ''
                THEN LEFT(LTRIM(RTRIM(SDGTags)), 2000)
                ELSE NULL
            END AS SdgAlignment,
            
            'https://grants.gov' AS EsoWebsite,
            
            -- Service Provider (truncated)
            LEFT(ISNULL(LTRIM(RTRIM(AgencyName)), 'U.S. Government'), 500) AS ServiceProviderEso,
            
            -- Approval Status
            CASE 
                WHEN [Status] = 'Posted' THEN 'Approved'
                WHEN [Status] = 'Closed' THEN 'Closed'
                WHEN [Status] = 'Cancelled' THEN 'Cancelled'
                ELSE 'Active'
            END AS ApprovalStatus,
            
            '0' AS Cost,  -- Free for grants
            
            -- Financial Terms (truncated)
            LEFT('Award: ' + ISNULL(LTRIM(RTRIM(EstimatedTotalFunding)), 'Not specified'), 2000) AS FinancialTerms,
            
            -- Area of Focus (truncated)
            LEFT(ISNULL(NULLIF(LTRIM(RTRIM(Category)), ''), 'General'), 500) AS AreaOfFocus,
            
            -- Tags (truncated)
            LEFT(LTRIM(RTRIM(ISNULL(KeywordTags, '') + 
                CASE WHEN Category IS NOT NULL AND KeywordTags IS NOT NULL THEN ', ' + Category
                     WHEN Category IS NOT NULL THEN Category
                     ELSE ''
                END)), 2000) AS Tags,
            
            -- Industry (truncated to fit indexable column)
            LEFT(CASE 
                WHEN AgencyName LIKE '%Health%' OR AgencyName LIKE '%NIH%' OR AgencyName LIKE '%CDC%' THEN 'Healthcare'
                WHEN AgencyName LIKE '%Education%' OR AgencyName LIKE '%NSF%' THEN 'Education'
                WHEN AgencyName LIKE '%Defense%' OR AgencyName LIKE '%DOD%' THEN 'Defense'
                WHEN AgencyName LIKE '%Energy%' OR AgencyName LIKE '%EPA%' THEN 'Energy & Environment'
                WHEN AgencyName LIKE '%Commerce%' OR AgencyName LIKE '%SBA%' THEN 'Business & Commerce'
                WHEN AgencyName LIKE '%Agriculture%' OR AgencyName LIKE '%USDA%' THEN 'Agriculture'
                WHEN AgencyName LIKE '%Transportation%' THEN 'Transportation'
                WHEN AgencyName LIKE '%Arts%' OR AgencyName LIKE '%Humanities%' THEN 'Arts & Humanities'
                ELSE 'Government'
            END, 200) AS Industry,
            
            -- Slug (truncated)
            LEFT(LOWER(REPLACE(REPLACE(REPLACE(ISNULL(OpportunityNumber, 'opp') + '-' + LEFT(ISNULL([Title], 'grant'), 30), ' ', '-'), '&', 'and'), '/', '-')), 500) AS Slug,
            
            -- Award Value String
            LEFT(ISNULL(LTRIM(RTRIM(EstimatedTotalFunding)), LTRIM(RTRIM(AwardValue))), 100) AS AwardValueStr,
            
            -- Deadline String
            CASE 
                WHEN [Deadline] IS NOT NULL AND TRY_CAST([Deadline] AS DATETIME2) IS NOT NULL
                THEN FORMAT(TRY_CAST([Deadline] AS DATETIME2), 'MMM dd, yyyy')
                ELSE 'See details'
            END AS DeadlineStr,
            
            -- Date Posted
            CASE 
                WHEN PostedDate IS NOT NULL AND TRY_CAST(PostedDate AS DATETIME2) IS NOT NULL
                THEN FORMAT(TRY_CAST(PostedDate AS DATETIME2), 'yyyy-MM-dd')
                ELSE FORMAT(GETDATE(), 'yyyy-MM-dd')
            END AS DatePosted,
            
            -- Opportunity Type
            LEFT(ISNULL(NULLIF(LTRIM(RTRIM(FundingType)), ''), 'Grant'), 100) AS OpportunityType,
            
            -- Is Featured
            CASE 
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(ISNULL(EstimatedTotalFunding, '0'), '$', ''), ',', ''), ' ', '') AS DECIMAL) >= 1000000 
                THEN 'Yes'
                ELSE 'No'
            END AS IsFeatured,
            
            'Yes' AS PublishOnLinkedin,
            
            -- Target Community (truncated)
            LEFT(CASE 
                WHEN EligibilityCategory IS NOT NULL AND LTRIM(RTRIM(EligibilityCategory)) != ''
                THEN LTRIM(RTRIM(EligibilityCategory))
                WHEN Category LIKE '%Research%' THEN 'Researchers'
                WHEN Category LIKE '%Education%' THEN 'Educational Institutions'
                WHEN Category LIKE '%Business%' THEN 'Businesses'
                ELSE 'General Public'
            END, 500) AS TargetCommunity,
            
            -- Created At
            FORMAT(ISNULL(CreatedDate, GETDATE()), 'yyyy-MM-ddTHH:mm:ss') AS CreatedAt
            
        FROM SourceData;
        
        SELECT @@ROWCOUNT as RecordsInserted;
        """
        
        result = self.execute_sql_command(populate_sql, timeout=900)
        if result:
            logger.info("✅ All data populated successfully")
            logger.info(result)
            return True
        else:
            logger.error("❌ Failed to populate data")
            return False

    def verify_final_table(self):
        """Final verification of the created table"""
        logger.info("🔍 Final verification...")
        
        verify_sql = """
        SELECT 
            'FINAL_VERIFICATION' as CheckType,
            COUNT(*) as TotalRecords,
            COUNT(CASE WHEN Title IS NOT NULL THEN 1 END) as WithTitle,
            COUNT(CASE WHEN Industry IS NOT NULL THEN 1 END) as WithIndustry,
            COUNT(CASE WHEN IsFeatured = 'Yes' THEN 1 END) as FeaturedCount
        FROM dbo.FinalOpportunities;
        
        SELECT TOP 5
            ID, Title, Industry, AwardValue, OpportunityType
        FROM dbo.FinalOpportunities
        ORDER BY ID;
        
        -- Industry distribution
        SELECT 
            Industry,
            COUNT(*) as OpportunityCount
        FROM dbo.FinalOpportunities
        GROUP BY Industry
        ORDER BY OpportunityCount DESC;
        """
        
        result = self.execute_sql_command(verify_sql)
        if result:
            logger.info("✅ Final verification completed")
            logger.info(result)
            return True
        return False

if __name__ == "__main__":
    print("🚀 Creating Final Opportunities Table - Azure SQL Database Final Fix...")
    creator = Layer3AzureSQLFinal()
    
    try:
        # Step 1: Drop existing table
        print("Step 1: Cleaning up existing table...")
        if not creator.drop_table_if_exists('FinalOpportunities'):
            print("❌ Table cleanup failed")
            exit(1)
        
        # Step 2: Create table structure
        print("Step 2: Creating table structure...")
        if not creator.create_final_opportunities_structure():
            print("❌ Table structure creation failed")
            exit(1)
        
        # Step 3: Create indexes
        print("Step 3: Creating indexes...")
        creator.create_indexes()
        
        # Step 4: Populate all data
        print("Step 4: Populating all data...")
        if not creator.populate_all_data():
            print("❌ Data population failed")
            exit(1)
        
        # Step 5: Final verification
        print("Step 5: Final verification...")
        if creator.verify_final_table():
            print("\n🎯 Layer 3 Final Opportunities Created Successfully!")
            print("📊 Your table is ready at: dbo.FinalOpportunities")
            print("🔍 Use Azure Data Studio or SQL Server Management Studio to view the data")
            print("\n📋 Sample Queries:")
            print("   SELECT COUNT(*) FROM dbo.FinalOpportunities;")
            print("   SELECT * FROM dbo.FinalOpportunities WHERE IsFeatured = 'Yes';")
            print("   SELECT Industry, COUNT(*) FROM dbo.FinalOpportunities GROUP BY Industry;")
        else:
            print("\n❌ Final verification failed")
        
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        logger.error(f"Unexpected error: {e}")