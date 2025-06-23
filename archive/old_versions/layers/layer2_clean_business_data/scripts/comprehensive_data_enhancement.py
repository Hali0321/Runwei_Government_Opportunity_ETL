#!/usr/bin/env python3
"""
Layer 2 - Comprehensive Data Enhancement - Azure SQL Database
Complete all data cleaning, enhancement, formatting, and quality scoring
This is the MAIN PROCESSING LAYER - does all the heavy lifting
"""

import subprocess
import logging
from datetime import datetime
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Layer2ComprehensiveEnhancer:
    """Comprehensive data enhancement for Layer 2 - Azure SQL Database optimized"""
    
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

    def analyze_layer2_current_state(self):
        """Analyze current Layer 2 data state"""
        logger.info("🔍 Analyzing Layer 2 current data state...")
        
        analysis_sql = """
        -- Layer 2 Current State Analysis
        SELECT 
            'LAYER2_CURRENT_STATE' as AnalysisType,
            COUNT(*) as TotalRecords,
            
            -- Check existing enhancement columns
            COUNT(CASE WHEN ProcessedBy IS NOT NULL THEN 1 END) as ProcessedRecords,
            COUNT(CASE WHEN BusinessRules IS NOT NULL THEN 1 END) as RecordsWithBusinessRules,
            COUNT(CASE WHEN UpdatedDate IS NOT NULL THEN 1 END) as RecordsWithUpdateDate,
            
            -- Check for missing data that needs enhancement
            SUM(CASE WHEN ServiceProviderName IS NULL OR ServiceProviderName = '' THEN 1 ELSE 0 END) as MissingServiceProvider,
            SUM(CASE WHEN OpportunityTitle IS NULL OR OpportunityTitle = '' THEN 1 ELSE 0 END) as MissingTitle,
            SUM(CASE WHEN AwardFloor IS NULL OR AwardFloor = '' THEN 1 ELSE 0 END) as MissingAwardFloor,
            SUM(CASE WHEN AwardCeiling IS NULL OR AwardCeiling = '' THEN 1 ELSE 0 END) as MissingAwardCeiling
            
        FROM CleanGrantsLayer2;
        
        -- Show sample records for quality assessment
        SELECT TOP 5
            'LAYER2_SAMPLE_DATA' as SampleType,
            OpportunityNumber,
            LEFT(ISNULL(OpportunityTitle, 'NO_TITLE'), 40) + '...' as Title_Preview,
            LEFT(ISNULL(ServiceProviderName, 'NO_PROVIDER'), 25) + '...' as Provider_Preview,
            AwardFloor,
            AwardCeiling,
            CloseDate,
            ProcessedBy
        FROM CleanGrantsLayer2
        ORDER BY OpportunityNumber;
        """
        
        return self.execute_sql_command(analysis_sql)

    def add_comprehensive_enhancement_columns(self):
        """Add all enhancement columns to Layer 2"""
        logger.info("🔧 Adding comprehensive enhancement columns to Layer 2...")
        
        add_columns_sql = """
        -- Add comprehensive enhancement columns to CleanGrantsLayer2
        
        -- Visual and branding columns
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'LogoUrl')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD LogoUrl NVARCHAR(2000);
            PRINT 'Added LogoUrl column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'CoverImage')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD CoverImage NVARCHAR(2000);
            PRINT 'Added CoverImage column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'EsoWebsite')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD EsoWebsite NVARCHAR(2000);
            PRINT 'Added EsoWebsite column';
        END
        
        -- Enhanced content columns
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'Summary')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD Summary NVARCHAR(500);
            PRINT 'Added Summary column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'Tags')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD Tags NVARCHAR(MAX);
            PRINT 'Added Tags column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'SdgAlignment')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD SdgAlignment NVARCHAR(2000);
            PRINT 'Added SdgAlignment column';
        END
        
        -- Financial formatting columns
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'AwardValueFormatted')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD AwardValueFormatted NVARCHAR(100);
            PRINT 'Added AwardValueFormatted column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'AwardRange')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD AwardRange NVARCHAR(100);
            PRINT 'Added AwardRange column';
        END
        
        -- Date formatting columns
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'DeadlineFormatted')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD DeadlineFormatted NVARCHAR(100);
            PRINT 'Added DeadlineFormatted column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'DatePostedFormatted')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD DatePostedFormatted NVARCHAR(100);
            PRINT 'Added DatePostedFormatted column';
        END
        
        -- Business logic columns
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'IsRollingDeadline')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD IsRollingDeadline NVARCHAR(10) DEFAULT 'No';
            PRINT 'Added IsRollingDeadline column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'FeeRequired')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD FeeRequired NVARCHAR(50) DEFAULT 'No';
            PRINT 'Added FeeRequired column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'EquityRequired')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD EquityRequired NVARCHAR(50) DEFAULT 'No - Grant based';
            PRINT 'Added EquityRequired column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'IsNonDilutive')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD IsNonDilutive NVARCHAR(10) DEFAULT 'Yes';
            PRINT 'Added IsNonDilutive column';
        END
        
        -- Quality and metadata columns
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'DataQualityScore')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD DataQualityScore DECIMAL(3,1) DEFAULT 0.0;
            PRINT 'Added DataQualityScore column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'EnhancementStatus')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD EnhancementStatus NVARCHAR(100) DEFAULT 'Pending';
            PRINT 'Added EnhancementStatus column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'ReadyForLayer3')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD ReadyForLayer3 BIT DEFAULT 0;
            PRINT 'Added ReadyForLayer3 column';
        END
        
        PRINT 'All comprehensive enhancement columns added successfully';
        """
        
        result = self.execute_sql_command(add_columns_sql)
        if result:
            logger.info("✅ Enhancement columns added")
            time.sleep(2)  # Allow schema changes to propagate
            return True
        else:
            logger.error("❌ Failed to add enhancement columns")
            return False

    def enhance_visual_and_branding_assets(self):
        """Enhance visual assets and branding information"""
        logger.info("🎨 Enhancing visual assets and branding...")
        
        visual_enhancement_sql = """
        BEGIN TRANSACTION VisualEnhancement;
        
        -- Step 1: Generate LogoUrl based on service provider and agency type
        UPDATE CleanGrantsLayer2
        SET LogoUrl = CASE
            WHEN LogoUrl IS NOT NULL AND LogoUrl != '' THEN LogoUrl
            WHEN ServiceProviderName LIKE '%Department of%' OR ServiceProviderName LIKE '%DOD%' OR ServiceProviderName LIKE '%Defense%' THEN
                'https://www.defense.gov/Portals/1/Images/dod_seal.png'
            WHEN ServiceProviderName LIKE '%Health%' OR ServiceProviderName LIKE '%NIH%' OR ServiceProviderName LIKE '%CDC%' THEN
                'https://www.nih.gov/sites/default/files/about-nih/2012-logo.png'
            WHEN ServiceProviderName LIKE '%Energy%' OR ServiceProviderName LIKE '%DOE%' THEN
                'https://www.energy.gov/sites/default/files/doe-logo.png'
            WHEN ServiceProviderName LIKE '%Agriculture%' OR ServiceProviderName LIKE '%USDA%' THEN
                'https://www.usda.gov/sites/default/files/usda-logo.png'
            WHEN ServiceProviderName LIKE '%Education%' OR ServiceProviderName LIKE '%Department of Education%' THEN
                'https://www2.ed.gov/about/overview/focus/brand/ed-logo.png'
            WHEN ServiceProviderName LIKE '%NASA%' OR ServiceProviderName LIKE '%Space%' THEN
                'https://www.nasa.gov/sites/default/files/thumbnails/image/nasa-logo-web-rgb.png'
            WHEN ServiceProviderName LIKE '%Science%' OR ServiceProviderName LIKE '%NSF%' THEN
                'https://www.nsf.gov/images/logos/NSF_4-Color_bitmap_Logo.png'
            WHEN ServiceProviderName LIKE '%Commerce%' OR ServiceProviderName LIKE '%NIST%' THEN
                'https://www.commerce.gov/sites/commerce.gov/files/commerce_logo.png'
            WHEN ServiceProviderName LIKE '%Environmental%' OR ServiceProviderName LIKE '%EPA%' THEN
                'https://www.epa.gov/sites/default/files/2016-09/epa-logo.png'
            WHEN ServiceProviderName LIKE '%University%' OR ServiceProviderName LIKE '%College%' THEN
                'https://via.placeholder.com/150x150/1f4e79/ffffff?text=EDU'
            WHEN ServiceProviderName LIKE '%Foundation%' THEN
                'https://via.placeholder.com/150x150/2d5a87/ffffff?text=FOUNDATION'
            ELSE 'https://www.grants.gov/assets/img/logo.png'
        END;
        
        PRINT CONCAT('Updated LogoUrl for ', @@ROWCOUNT, ' records');
        
        -- Step 2: Generate EsoWebsite based on service provider
        UPDATE CleanGrantsLayer2
        SET EsoWebsite = CASE
            WHEN EsoWebsite IS NOT NULL AND EsoWebsite != '' THEN EsoWebsite
            WHEN ServiceProviderName LIKE '%Department of Defense%' OR ServiceProviderName LIKE '%DOD%' THEN 'https://www.defense.gov'
            WHEN ServiceProviderName LIKE '%Health%' OR ServiceProviderName LIKE '%NIH%' THEN 'https://www.nih.gov'
            WHEN ServiceProviderName LIKE '%Energy%' OR ServiceProviderName LIKE '%DOE%' THEN 'https://www.energy.gov'
            WHEN ServiceProviderName LIKE '%Agriculture%' OR ServiceProviderName LIKE '%USDA%' THEN 'https://www.usda.gov'
            WHEN ServiceProviderName LIKE '%Education%' THEN 'https://www.ed.gov'
            WHEN ServiceProviderName LIKE '%NASA%' THEN 'https://www.nasa.gov'
            WHEN ServiceProviderName LIKE '%Science%' OR ServiceProviderName LIKE '%NSF%' THEN 'https://www.nsf.gov'
            WHEN ServiceProviderName LIKE '%Commerce%' THEN 'https://www.commerce.gov'
            WHEN ServiceProviderName LIKE '%Environmental%' OR ServiceProviderName LIKE '%EPA%' THEN 'https://www.epa.gov'
            ELSE 'https://www.grants.gov'
        END;
        
        PRINT CONCAT('Updated EsoWebsite for ', @@ROWCOUNT, ' records');
        
        -- Step 3: Generate CoverImage based on category and content
        UPDATE CleanGrantsLayer2
        SET CoverImage = CASE
            WHEN CoverImage IS NOT NULL AND CoverImage != '' THEN CoverImage
            WHEN CategoryOfFundingActivity LIKE '%Research%' OR OpportunityTitle LIKE '%Research%' THEN
                'https://via.placeholder.com/800x400/1e3a8a/ffffff?text=Research+Grant'
            WHEN CategoryOfFundingActivity LIKE '%Education%' OR OpportunityTitle LIKE '%Education%' THEN
                'https://via.placeholder.com/800x400/059669/ffffff?text=Education+Grant'
            WHEN CategoryOfFundingActivity LIKE '%Health%' OR OpportunityTitle LIKE '%Health%' THEN
                'https://via.placeholder.com/800x400/dc2626/ffffff?text=Health+Grant'
            WHEN CategoryOfFundingActivity LIKE '%Environment%' OR OpportunityTitle LIKE '%Environment%' THEN
                'https://via.placeholder.com/800x400/16a34a/ffffff?text=Environment+Grant'
            WHEN CategoryOfFundingActivity LIKE '%Technology%' OR OpportunityTitle LIKE '%Technology%' THEN
                'https://via.placeholder.com/800x400/6366f1/ffffff?text=Technology+Grant'
            WHEN CategoryOfFundingActivity LIKE '%Energy%' OR OpportunityTitle LIKE '%Energy%' THEN
                'https://via.placeholder.com/800x400/f59e0b/ffffff?text=Energy+Grant'
            WHEN CategoryOfFundingActivity LIKE '%Defense%' OR ServiceProviderName LIKE '%Defense%' THEN
                'https://via.placeholder.com/800x400/374151/ffffff?text=Defense+Grant'
            ELSE 'https://via.placeholder.com/800x400/6b7280/ffffff?text=Grant+Opportunity'
        END;
        
        PRINT CONCAT('Updated CoverImage for ', @@ROWCOUNT, ' records');
        
        COMMIT TRANSACTION VisualEnhancement;
        """
        
        return self.execute_sql_command(visual_enhancement_sql, timeout=300)

    def enhance_financial_formatting(self):
        """Format financial fields properly"""
        logger.info("💰 Enhancing financial field formatting...")
        
        financial_enhancement_sql = """
        BEGIN TRANSACTION FinancialEnhancement;
        
        -- Step 1: Format AwardValueFormatted from AwardFloor and AwardCeiling
        UPDATE CleanGrantsLayer2
        SET AwardValueFormatted = CASE
            WHEN AwardFloor IS NOT NULL AND AwardCeiling IS NOT NULL 
                AND ISNUMERIC(AwardFloor) = 1 AND ISNUMERIC(AwardCeiling) = 1 THEN
                CASE
                    WHEN CAST(AwardFloor AS BIGINT) = CAST(AwardCeiling AS BIGINT) THEN
                        '$' + FORMAT(CAST(AwardFloor AS MONEY), 'N0') + ' USD'
                    ELSE
                        '$' + FORMAT(CAST(AwardFloor AS MONEY), 'N0') + ' - $' + FORMAT(CAST(AwardCeiling AS MONEY), 'N0') + ' USD'
                END
            WHEN AwardCeiling IS NOT NULL AND ISNUMERIC(AwardCeiling) = 1 THEN
                'Up to $' + FORMAT(CAST(AwardCeiling AS MONEY), 'N0') + ' USD'
            WHEN AwardFloor IS NOT NULL AND ISNUMERIC(AwardFloor) = 1 THEN
                'From $' + FORMAT(CAST(AwardFloor AS MONEY), 'N0') + ' USD'
            ELSE 'Amount varies'
        END;
        
        PRINT CONCAT('Formatted AwardValueFormatted for ', @@ROWCOUNT, ' records');
        
        -- Step 2: Create simplified AwardRange for easy display
        UPDATE CleanGrantsLayer2
        SET AwardRange = CASE
            WHEN AwardFloor IS NOT NULL AND AwardCeiling IS NOT NULL 
                AND ISNUMERIC(AwardFloor) = 1 AND ISNUMERIC(AwardCeiling) = 1 THEN
                CASE
                    WHEN CAST(AwardFloor AS BIGINT) = CAST(AwardCeiling AS BIGINT) THEN 'Fixed Amount'
                    WHEN CAST(AwardCeiling AS BIGINT) - CAST(AwardFloor AS BIGINT) > 1000000 THEN 'Large Range (>$1M)'
                    WHEN CAST(AwardCeiling AS BIGINT) - CAST(AwardFloor AS BIGINT) > 100000 THEN 'Medium Range ($100K-$1M)'
                    ELSE 'Small Range (<$100K)'
                END
            WHEN AwardCeiling IS NOT NULL AND ISNUMERIC(AwardCeiling) = 1 THEN
                CASE
                    WHEN CAST(AwardCeiling AS BIGINT) > 10000000 THEN 'Large Grant (>$10M)'
                    WHEN CAST(AwardCeiling AS BIGINT) > 1000000 THEN 'Medium Grant ($1M-$10M)'
                    WHEN CAST(AwardCeiling AS BIGINT) > 100000 THEN 'Small Grant ($100K-$1M)'
                    ELSE 'Micro Grant (<$100K)'
                END
            ELSE 'Variable'
        END;
        
        PRINT CONCAT('Set AwardRange for ', @@ROWCOUNT, ' records');
        
        COMMIT TRANSACTION FinancialEnhancement;
        """
        
        return self.execute_sql_command(financial_enhancement_sql, timeout=300)

    def enhance_date_formatting(self):
        """Format date fields properly"""
        logger.info("📅 Enhancing date field formatting...")
        
        date_enhancement_sql = """
        BEGIN TRANSACTION DateEnhancement;
        
        -- Step 1: Format DeadlineFormatted from CloseDate
        UPDATE CleanGrantsLayer2
        SET DeadlineFormatted = CASE
            WHEN CloseDate IS NULL OR CloseDate = '' THEN 'Rolling basis'
            WHEN CloseDate LIKE '%rolling%' OR CloseDate LIKE '%ongoing%' THEN 'Rolling basis'
            WHEN ISDATE(CloseDate) = 1 THEN
                FORMAT(CAST(CloseDate AS DATE), 'dd MMM yyyy')
            WHEN LEN(CloseDate) = 10 AND CloseDate LIKE '%-%-%' THEN
                CASE
                    WHEN ISDATE(CloseDate) = 1 THEN FORMAT(CAST(CloseDate AS DATE), 'dd MMM yyyy')
                    ELSE CloseDate
                END
            ELSE ISNULL(CloseDate, 'Rolling basis')
        END,
        IsRollingDeadline = CASE
            WHEN CloseDate IS NULL OR CloseDate = '' THEN 'Yes'
            WHEN CloseDate LIKE '%rolling%' OR CloseDate LIKE '%ongoing%' THEN 'Yes'
            ELSE 'No'
        END;
        
        PRINT CONCAT('Formatted DeadlineFormatted for ', @@ROWCOUNT, ' records');
        
        -- Step 2: Format DatePostedFormatted from PostDate
        UPDATE CleanGrantsLayer2
        SET DatePostedFormatted = CASE
            WHEN PostDate IS NOT NULL AND ISDATE(PostDate) = 1 THEN
                FORMAT(CAST(PostDate AS DATE), 'dd MMM yyyy')
            WHEN PostDate IS NOT NULL AND LEN(PostDate) = 10 AND PostDate LIKE '%-%-%' THEN
                CASE
                    WHEN ISDATE(PostDate) = 1 THEN FORMAT(CAST(PostDate AS DATE), 'dd MMM yyyy')
                    ELSE PostDate
                END
            ELSE FORMAT(GETDATE(), 'dd MMM yyyy')
        END;
        
        PRINT CONCAT('Formatted DatePostedFormatted for ', @@ROWCOUNT, ' records');
        
        COMMIT TRANSACTION DateEnhancement;
        """
        
        return self.execute_sql_command(date_enhancement_sql, timeout=300)

    def enhance_content_and_tags(self):
        """Enhance content quality and generate tags"""
        logger.info("📝 Enhancing content and generating tags...")
        
        content_enhancement_sql = """
        BEGIN TRANSACTION ContentEnhancement;
        
        -- Step 1: Generate Summary from OpportunityDescription
        UPDATE CleanGrantsLayer2
        SET Summary = CASE
            WHEN OpportunityDescription IS NOT NULL AND LEN(OpportunityDescription) > 20 THEN
                CASE
                    WHEN LEN(OpportunityDescription) <= 300 THEN OpportunityDescription
                    ELSE LEFT(OpportunityDescription, 297) + '...'
                END
            WHEN OpportunityTitle IS NOT NULL THEN
                OpportunityTitle + ' - ' + 
                CASE
                    WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN
                        'Grant opportunity with funding ' + AwardValueFormatted + '.'
                    ELSE 'Grant opportunity providing funding and support.'
                END
            ELSE 'Federal grant opportunity available for eligible applicants.'
        END;
        
        PRINT CONCAT('Generated Summary for ', @@ROWCOUNT, ' records');
        
        -- Step 2: Generate Tags based on content analysis
        UPDATE CleanGrantsLayer2
        SET Tags = CASE
            -- Research-focused tags
            WHEN OpportunityTitle LIKE '%Research%' OR OpportunityDescription LIKE '%research%' OR CategoryOfFundingActivity LIKE '%Research%' THEN
                'research, grants, funding, academic, innovation'
            
            -- Health-focused tags
            WHEN ServiceProviderName LIKE '%Health%' OR OpportunityTitle LIKE '%Health%' OR OpportunityDescription LIKE '%health%' OR CategoryOfFundingActivity LIKE '%Health%' THEN
                'healthcare, medical, health, grants, funding, research'
            
            -- Education-focused tags
            WHEN ServiceProviderName LIKE '%Education%' OR OpportunityTitle LIKE '%Education%' OR OpportunityDescription LIKE '%education%' OR CategoryOfFundingActivity LIKE '%Education%' THEN
                'education, academic, learning, grants, funding, students'
            
            -- Environment-focused tags
            WHEN OpportunityTitle LIKE '%Environment%' OR OpportunityDescription LIKE '%environment%' OR OpportunityDescription LIKE '%climate%' OR CategoryOfFundingActivity LIKE '%Environment%' THEN
                'environment, sustainability, climate, green, grants, funding'
            
            -- Technology-focused tags
            WHEN OpportunityTitle LIKE '%Technology%' OR OpportunityDescription LIKE '%technology%' OR OpportunityDescription LIKE '%innovation%' OR CategoryOfFundingActivity LIKE '%Technology%' THEN
                'technology, innovation, tech, digital, grants, funding'
            
            -- Energy-focused tags
            WHEN ServiceProviderName LIKE '%Energy%' OR OpportunityTitle LIKE '%Energy%' OR OpportunityDescription LIKE '%energy%' OR CategoryOfFundingActivity LIKE '%Energy%' THEN
                'energy, renewable, clean energy, grants, funding, sustainability'
            
            -- Defense-focused tags
            WHEN ServiceProviderName LIKE '%Defense%' OR OpportunityTitle LIKE '%Defense%' OR OpportunityDescription LIKE '%defense%' OR CategoryOfFundingActivity LIKE '%Defense%' THEN
                'defense, security, military, grants, funding, research'
            
            -- Agriculture-focused tags
            WHEN ServiceProviderName LIKE '%Agriculture%' OR OpportunityTitle LIKE '%Agriculture%' OR OpportunityDescription LIKE '%agriculture%' OR CategoryOfFundingActivity LIKE '%Agriculture%' THEN
                'agriculture, farming, food, rural, grants, funding'
            
            -- General tags
            ELSE 'grants, funding, federal, opportunities'
        END;
        
        PRINT CONCAT('Generated Tags for ', @@ROWCOUNT, ' records');
        
        -- Step 3: Generate SDG Alignment based on content
        UPDATE CleanGrantsLayer2
        SET SdgAlignment = CASE
            WHEN Tags LIKE '%education%' OR CategoryOfFundingActivity LIKE '%Education%' THEN 
                'SDG 4: Quality Education'
            WHEN Tags LIKE '%health%' OR CategoryOfFundingActivity LIKE '%Health%' THEN 
                'SDG 3: Good Health and Well-being'
            WHEN Tags LIKE '%environment%' OR Tags LIKE '%climate%' OR CategoryOfFundingActivity LIKE '%Environment%' THEN 
                'SDG 13: Climate Action, SDG 15: Life on Land'
            WHEN Tags LIKE '%energy%' OR CategoryOfFundingActivity LIKE '%Energy%' THEN 
                'SDG 7: Affordable and Clean Energy'
            WHEN Tags LIKE '%technology%' OR Tags LIKE '%innovation%' OR CategoryOfFundingActivity LIKE '%Technology%' THEN 
                'SDG 9: Industry, Innovation and Infrastructure'
            WHEN Tags LIKE '%agriculture%' OR CategoryOfFundingActivity LIKE '%Agriculture%' THEN 
                'SDG 2: Zero Hunger, SDG 15: Life on Land'
            WHEN Tags LIKE '%research%' THEN 
                'SDG 9: Industry, Innovation and Infrastructure'
            ELSE 'Multiple SDGs applicable'
        END;
        
        PRINT CONCAT('Generated SDG Alignment for ', @@ROWCOUNT, ' records');
        
        COMMIT TRANSACTION ContentEnhancement;
        """
        
        return self.execute_sql_command(content_enhancement_sql, timeout=300)

    def apply_business_logic_flags(self):
        """Apply business logic and set binary flags"""
        logger.info("🔘 Applying business logic and binary flags...")
        
        business_logic_sql = """
        BEGIN TRANSACTION BusinessLogic;
        
        -- Set FeeRequired based on cost sharing and other indicators
        UPDATE CleanGrantsLayer2
        SET FeeRequired = CASE
            WHEN CostSharingRequired = 'true' THEN 'Yes - Cost sharing required'
            WHEN OpportunityDescription LIKE '%fee%' OR OpportunityDescription LIKE '%cost%' THEN 'Possible - See details'
            ELSE 'No'
        END;
        
        PRINT CONCAT('Set FeeRequired for ', @@ROWCOUNT, ' records');
        
        -- Set EquityRequired (mostly N/A for government grants)
        UPDATE CleanGrantsLayer2
        SET EquityRequired = CASE
            WHEN OpportunityDescription LIKE '%equity%' OR OpportunityDescription LIKE '%ownership%' THEN 'Possible - See terms'
            WHEN CategoryOfFundingActivity LIKE '%Venture%' OR CategoryOfFundingActivity LIKE '%Investment%' THEN 'Possible - Investment program'
            ELSE 'No - Grant based'
        END;
        
        PRINT CONCAT('Set EquityRequired for ', @@ROWCOUNT, ' records');
        
        -- Set IsNonDilutive (government grants are typically non-dilutive)
        UPDATE CleanGrantsLayer2
        SET IsNonDilutive = CASE
            WHEN EquityRequired LIKE '%Possible%' THEN 'No'
            WHEN OpportunityDescription LIKE '%equity%' OR OpportunityDescription LIKE '%dilut%' THEN 'No'
            ELSE 'Yes'
        END;
        
        PRINT CONCAT('Set IsNonDilutive for ', @@ROWCOUNT, ' records');
        
        COMMIT TRANSACTION BusinessLogic;
        """
        
        return self.execute_sql_command(business_logic_sql, timeout=300)

    def calculate_comprehensive_quality_scores(self):
        """Calculate comprehensive data quality scores"""
        logger.info("📊 Calculating comprehensive data quality scores...")
        
        quality_scoring_sql = """
        BEGIN TRANSACTION QualityScoring;
        
        -- Calculate comprehensive quality scores (0-10 scale)
        UPDATE CleanGrantsLayer2
        SET DataQualityScore = (
            -- Core content (3.0 points)
            CASE WHEN OpportunityTitle IS NOT NULL AND LEN(OpportunityTitle) > 10 THEN 1.0 ELSE 0 END +
            CASE WHEN OpportunityDescription IS NOT NULL AND LEN(OpportunityDescription) > 50 THEN 1.0 ELSE 0 END +
            CASE WHEN Summary IS NOT NULL AND LEN(Summary) > 20 THEN 1.0 ELSE 0 END +
            
            -- Provider and contact (1.5 points)
            CASE WHEN ServiceProviderName IS NOT NULL AND ServiceProviderName != '' THEN 0.5 ELSE 0 END +
            CASE WHEN EsoWebsite IS NOT NULL AND EsoWebsite != '' THEN 0.5 ELSE 0 END +
            CASE WHEN AgencyContactInfo IS NOT NULL AND AgencyContactInfo != '' THEN 0.5 ELSE 0 END +
            
            -- Financial information (2.0 points)
            CASE WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN 1.0 ELSE 0 END +
            CASE WHEN AwardRange IS NOT NULL AND AwardRange != 'Variable' THEN 0.5 ELSE 0 END +
            CASE WHEN (AwardFloor IS NOT NULL AND ISNUMERIC(AwardFloor) = 1) OR (AwardCeiling IS NOT NULL AND ISNUMERIC(AwardCeiling) = 1) THEN 0.5 ELSE 0 END +
            
            -- Application details (1.5 points)
            CASE WHEN OpportunityNumber IS NOT NULL AND OpportunityNumber != '' THEN 0.5 ELSE 0 END +
            CASE WHEN DeadlineFormatted IS NOT NULL AND DeadlineFormatted != '' THEN 0.5 ELSE 0 END +
            CASE WHEN EligibilityDescription IS NOT NULL AND LEN(EligibilityDescription) > 20 THEN 0.5 ELSE 0 END +
            
            -- Enhanced content and metadata (2.0 points)
            CASE WHEN LogoUrl IS NOT NULL AND LogoUrl != '' THEN 0.3 ELSE 0 END +
            CASE WHEN CoverImage IS NOT NULL AND CoverImage != '' THEN 0.2 ELSE 0 END +
            CASE WHEN Tags IS NOT NULL AND Tags LIKE '%,%' THEN 0.5 ELSE 0 END +
            CASE WHEN SdgAlignment IS NOT NULL AND SdgAlignment != '' THEN 0.5 ELSE 0 END +
            CASE WHEN CategoryOfFundingActivity IS NOT NULL AND CategoryOfFundingActivity != '' THEN 0.5 ELSE 0 END
        );
        
        PRINT CONCAT('Calculated quality scores for ', @@ROWCOUNT, ' records');
        
        -- Set enhancement status and readiness for Layer 3
        UPDATE CleanGrantsLayer2
        SET EnhancementStatus = CASE
            WHEN DataQualityScore >= 9.0 THEN 'Excellent - Ready for Production'
            WHEN DataQualityScore >= 7.5 THEN 'Very Good - Minor polish needed'
            WHEN DataQualityScore >= 6.0 THEN 'Good - Acceptable quality'
            WHEN DataQualityScore >= 4.0 THEN 'Fair - Needs improvement'
            ELSE 'Poor - Major issues'
        END,
        ReadyForLayer3 = CASE
            WHEN DataQualityScore >= 6.0 THEN 1  -- Quality threshold for Layer 3
            ELSE 0
        END;
        
        PRINT CONCAT('Set enhancement status and Layer 3 readiness for ', @@ROWCOUNT, ' records');
        
        -- Update processing metadata
        UPDATE CleanGrantsLayer2
        SET ProcessedBy = 'Comprehensive_Enhancement_Complete',
            UpdatedDate = GETDATE(),
            BusinessRules = ISNULL(BusinessRules, '') + '; Comprehensive enhancement applied; Quality scored; Layer 3 readiness assessed'
        WHERE DataQualityScore > 0;
        
        PRINT CONCAT('Updated processing metadata for ', @@ROWCOUNT, ' records');
        
        COMMIT TRANSACTION QualityScoring;
        """
        
        return self.execute_sql_command(quality_scoring_sql, timeout=300)

    def create_comprehensive_enhancement_report(self):
        """Create comprehensive enhancement report"""
        logger.info("📋 Creating comprehensive enhancement report...")
        
        report_sql = """
        -- Comprehensive Layer 2 Enhancement Report
        SELECT 
            'COMPREHENSIVE_ENHANCEMENT_SUMMARY' as ReportType,
            COUNT(*) as TotalRecords,
            AVG(DataQualityScore) as AverageQualityScore,
            MIN(DataQualityScore) as MinQualityScore,
            MAX(DataQualityScore) as MaxQualityScore,
            
            -- Enhancement status distribution
            SUM(CASE WHEN EnhancementStatus LIKE '%Excellent%' THEN 1 ELSE 0 END) as ExcellentRecords,
            SUM(CASE WHEN EnhancementStatus LIKE '%Very Good%' THEN 1 ELSE 0 END) as VeryGoodRecords,
            SUM(CASE WHEN EnhancementStatus LIKE '%Good%' THEN 1 ELSE 0 END) as GoodRecords,
            SUM(CASE WHEN EnhancementStatus LIKE '%Fair%' THEN 1 ELSE 0 END) as FairRecords,
            SUM(CASE WHEN EnhancementStatus LIKE '%Poor%' THEN 1 ELSE 0 END) as PoorRecords,
            
            -- Layer 3 readiness
            SUM(CASE WHEN ReadyForLayer3 = 1 THEN 1 ELSE 0 END) as ReadyForLayer3Count,
            ROUND(100.0 * SUM(CASE WHEN ReadyForLayer3 = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) as Layer3ReadyPercentage,
            
            GETDATE() as ReportGeneratedAt
        FROM CleanGrantsLayer2;
        
        -- Field completion analysis
        SELECT 
            'FIELD_COMPLETION_ANALYSIS' as AnalysisType,
            COUNT(*) as TotalRecords,
            
            -- Visual assets
            ROUND(100.0 * SUM(CASE WHEN LogoUrl IS NOT NULL AND LogoUrl != '' THEN 1 ELSE 0 END) / COUNT(*), 1) as LogoUrl_Completion,
            ROUND(100.0 * SUM(CASE WHEN CoverImage IS NOT NULL AND CoverImage != '' THEN 1 ELSE 0 END) / COUNT(*), 1) as CoverImage_Completion,
            ROUND(100.0 * SUM(CASE WHEN EsoWebsite IS NOT NULL AND EsoWebsite != '' THEN 1 ELSE 0 END) / COUNT(*), 1) as EsoWebsite_Completion,
            
            -- Content fields
            ROUND(100.0 * SUM(CASE WHEN Summary IS NOT NULL AND Summary != '' THEN 1 ELSE 0 END) / COUNT(*), 1) as Summary_Completion,
            ROUND(100.0 * SUM(CASE WHEN Tags IS NOT NULL AND Tags LIKE '%,%' THEN 1 ELSE 0 END) / COUNT(*), 1) as Tags_Generated,
            ROUND(100.0 * SUM(CASE WHEN SdgAlignment IS NOT NULL AND SdgAlignment != '' THEN 1 ELSE 0 END) / COUNT(*), 1) as SDG_Completion,
            
            -- Financial formatting
            ROUND(100.0 * SUM(CASE WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN 1 ELSE 0 END) / COUNT(*), 1) as Financial_Formatted,
            ROUND(100.0 * SUM(CASE WHEN AwardRange IS NOT NULL AND AwardRange != 'Variable' THEN 1 ELSE 0 END) / COUNT(*), 1) as AwardRange_Set,
            
            -- Date formatting
            ROUND(100.0 * SUM(CASE WHEN DeadlineFormatted IS NOT NULL AND DeadlineFormatted != '' THEN 1 ELSE 0 END) / COUNT(*), 1) as Deadline_Formatted,
            ROUND(100.0 * SUM(CASE WHEN IsRollingDeadline IN ('Yes', 'No') THEN 1 ELSE 0 END) / COUNT(*), 1) as Rolling_Flag_Set
            
        FROM CleanGrantsLayer2;
        
        -- Quality distribution by funding category
        SELECT 
            'QUALITY_BY_CATEGORY' as AnalysisType,
            ISNULL(CategoryOfFundingActivity, 'Unknown') as FundingCategory,
            COUNT(*) as RecordCount,
            AVG(DataQualityScore) as AvgQualityScore,
            SUM(CASE WHEN ReadyForLayer3 = 1 THEN 1 ELSE 0 END) as ReadyForLayer3Count
        FROM CleanGrantsLayer2
        GROUP BY CategoryOfFundingActivity
        ORDER BY COUNT(*) DESC;
        
        -- Top quality records sample
        SELECT TOP 10
            'HIGH_QUALITY_SAMPLE' as SampleType,
            OpportunityNumber,
            LEFT(OpportunityTitle, 50) + '...' as Title_Preview,
            LEFT(ServiceProviderName, 30) + '...' as Provider_Preview,
            AwardValueFormatted,
            DataQualityScore,
            EnhancementStatus,
            ReadyForLayer3
        FROM CleanGrantsLayer2
        ORDER BY DataQualityScore DESC;
        """
        
        return self.execute_sql_command(report_sql)

def main():
    """Main execution function"""
    print("🚀 Layer 2 - Comprehensive Data Enhancement - Azure SQL Database")
    print("=" * 70)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Goal: Complete comprehensive data enhancement for Layer 2")
    print("📊 Scope: Visual assets, financial formatting, content enhancement, quality scoring")
    
    enhancer = Layer2ComprehensiveEnhancer()
    
    try:
        # Step 1: Analyze current state
        print("\n🔍 Step 1: Analyzing Layer 2 current state...")
        if not enhancer.analyze_layer2_current_state():
            print("⚠️ Analysis had issues but continuing...")
        
        # Step 2: Add enhancement columns
        print("\n🔧 Step 2: Adding comprehensive enhancement columns...")
        if not enhancer.add_comprehensive_enhancement_columns():
            print("❌ Failed to add enhancement columns")
            return False
        
        # Step 3: Enhance visual and branding assets
        print("\n🎨 Step 3: Enhancing visual and branding assets...")
        if not enhancer.enhance_visual_and_branding_assets():
            print("❌ Failed to enhance visual assets")
            return False
        
        # Step 4: Enhance financial formatting
        print("\n💰 Step 4: Enhancing financial field formatting...")
        if not enhancer.enhance_financial_formatting():
            print("❌ Failed to enhance financial formatting")
            return False
        
        # Step 5: Enhance date formatting
        print("\n📅 Step 5: Enhancing date field formatting...")
        if not enhancer.enhance_date_formatting():
            print("❌ Failed to enhance date formatting")
            return False
        
        # Step 6: Enhance content and tags
        print("\n📝 Step 6: Enhancing content and generating tags...")
        if not enhancer.enhance_content_and_tags():
            print("❌ Failed to enhance content and tags")
            return False
        
        # Step 7: Apply business logic flags
        print("\n🔘 Step 7: Applying business logic and binary flags...")
        if not enhancer.apply_business_logic_flags():
            print("❌ Failed to apply business logic")
            return False
        
        # Step 8: Calculate quality scores
        print("\n📊 Step 8: Calculating comprehensive quality scores...")
        if not enhancer.calculate_comprehensive_quality_scores():
            print("❌ Failed to calculate quality scores")
            return False
        
        # Step 9: Create enhancement report
        print("\n📋 Step 9: Creating comprehensive enhancement report...")
        if not enhancer.create_comprehensive_enhancement_report():
            print("⚠️ Report generation had issues but enhancement is complete")
        
        print("\n🎊 SUCCESS! Layer 2 Comprehensive Enhancement Complete!")
        print("=" * 70)
        print("✅ Visual Assets: Logos, cover images, ESO websites generated")
        print("✅ Financial Fields: Formatted with proper currency and ranges")
        print("✅ Date Fields: Formatted to DD Mmm YYYY standard")
        print("✅ Content Enhancement: Summaries, tags, SDG alignment generated")
        print("✅ Business Logic: Fee requirements, equity, non-dilutive flags set")
        print("✅ Quality Scoring: Comprehensive 0-10 scale scoring applied")
        print("✅ Layer 3 Readiness: Records assessed and flagged for Layer 3")
        
        print("\n💡 Usage Examples:")
        print("-- View Layer 3 ready records")
        print("SELECT * FROM CleanGrantsLayer2 WHERE ReadyForLayer3 = 1;")
        print("")
        print("-- Check enhancement status")
        print("SELECT EnhancementStatus, COUNT(*) FROM CleanGrantsLayer2 GROUP BY EnhancementStatus;")
        print("")
        print("-- High-quality records")
        print("SELECT * FROM CleanGrantsLayer2 WHERE DataQualityScore >= 8.0 ORDER BY DataQualityScore DESC;")
        
        print("\n🎯 Next Steps:")
        print("1. ✅ Review enhancement results and quality scores")
        print("2. 🔄 Run Layer 3 simple selection script")
        print("3. 📤 Layer 2 provides all enhanced data for Layer 3 selection")
        print("4. 🚀 Your comprehensive data enhancement is complete!")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Layer 2 comprehensive enhancement failed: {e}")
        logger.error(f"Error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🚀 Layer 2 Comprehensive Enhancement Successfully Completed!")
        print("📊 Your CleanGrantsLayer2 table now contains all enhanced data")
        print("🎯 Ready for Layer 3 simple selection")
    else:
        print("\n❌ Enhancement failed - check logs for details")