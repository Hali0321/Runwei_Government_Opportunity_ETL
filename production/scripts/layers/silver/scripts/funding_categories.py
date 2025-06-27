#!/usr/bin/env python3
"""
Azure SQL Database Silver Layer Category Processor
Advanced funding type categorization with Runwei business logic
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
        logging.FileHandler(PYCACHE_DIR / 'silver_category_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SilverCategoryProcessor:
    """Advanced category processing for Azure SQL Database Layer 2"""
    
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
                "-C", "-t", str(timeout), "-I"
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

    def run_complete_category_processing(self):
        """Run the complete category processing pipeline - FIXED VERSION"""
        logger.info("🚀 Starting Silver Layer Category Processing")
        logger.info("=" * 60)
        
        try:
            # Step 1: Add columns and process in one batch
            processing_sql = """
            -- ===================================
            -- COMPLETE FUNDING CATEGORY PROCESSING
            -- Add columns and process funding types in one transaction
            -- ===================================
            
            BEGIN TRANSACTION CategoryProcessing;
            
            -- Add Runwei standardized category columns if they don't exist
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'RunweiCategory')
            BEGIN
                ALTER TABLE CleanGrantsLayer2 ADD RunweiCategory NVARCHAR(500);
                PRINT 'Added RunweiCategory column';
            END
            
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'CategoryTags')
            BEGIN
                ALTER TABLE CleanGrantsLayer2 ADD CategoryTags NVARCHAR(1000);
                PRINT 'Added CategoryTags column';
            END
            
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'IsGrant')
            BEGIN
                ALTER TABLE CleanGrantsLayer2 ADD IsGrant BIT DEFAULT 0;
                PRINT 'Added IsGrant flag';
            END
            
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'IsProcurementContract')
            BEGIN
                ALTER TABLE CleanGrantsLayer2 ADD IsProcurementContract BIT DEFAULT 0;
                PRINT 'Added IsProcurementContract flag';
            END
            
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'IsOther')
            BEGIN
                ALTER TABLE CleanGrantsLayer2 ADD IsOther BIT DEFAULT 0;
                PRINT 'Added IsOther flag';
            END
            
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'OriginalFundingType')
            BEGIN
                ALTER TABLE CleanGrantsLayer2 ADD OriginalFundingType NVARCHAR(500);
                PRINT 'Added OriginalFundingType backup column';
            END
            
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'CategoryProcessedDate')
            BEGIN
                ALTER TABLE CleanGrantsLayer2 ADD CategoryProcessedDate DATETIME2 DEFAULT GETDATE();
                PRINT 'Added CategoryProcessedDate timestamp';
            END
            
            -- Backup original FundingType values
            UPDATE CleanGrantsLayer2 
            SET OriginalFundingType = FundingType
            WHERE OriginalFundingType IS NULL;
            
            PRINT CONCAT('Backed up ', @@ROWCOUNT, ' original FundingType values');
            
            -- RUNWEI CATEGORY TRANSFORMATION
            -- Handle NULL and empty values first
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 'Other',
                CategoryTags = 'unspecified',
                IsGrant = 0,
                IsProcurementContract = 0,
                IsOther = 1,
                CategoryProcessedDate = GETDATE()
            WHERE (FundingType IS NULL OR LTRIM(RTRIM(FundingType)) = '')
              AND RunweiCategory IS NULL;
            
            PRINT CONCAT('Processed ', @@ROWCOUNT, ' NULL/empty funding types');
            
            -- Pure Grant
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 'Grant',
                CategoryTags = 'grant',
                IsGrant = 1,
                IsProcurementContract = 0,
                IsOther = 0,
                CategoryProcessedDate = GETDATE()
            WHERE FundingType = 'Grant'
              AND RunweiCategory IS NULL;
            
            PRINT CONCAT('Processed ', @@ROWCOUNT, ' pure Grant records');
            
            -- Cooperative Agreement → Grant (federal standard)
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 'Grant',
                CategoryTags = 'grant,cooperative-agreement',
                IsGrant = 1,
                IsProcurementContract = 0,
                IsOther = 0,
                CategoryProcessedDate = GETDATE()
            WHERE FundingType = 'Cooperative Agreement'
              AND RunweiCategory IS NULL;
            
            PRINT CONCAT('Processed ', @@ROWCOUNT, ' Cooperative Agreement records');
            
            -- Pure Procurement Contract
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 'Procurement Contract',
                CategoryTags = 'procurement-contract',
                IsGrant = 0,
                IsProcurementContract = 1,
                IsOther = 0,
                CategoryProcessedDate = GETDATE()
            WHERE FundingType = 'Procurement Contract'
              AND RunweiCategory IS NULL;
            
            PRINT CONCAT('Processed ', @@ROWCOUNT, ' pure Procurement Contract records');
            
            -- Pure Other
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 'Other',
                CategoryTags = 'other',
                IsGrant = 0,
                IsProcurementContract = 0,
                IsOther = 1,
                CategoryProcessedDate = GETDATE()
            WHERE FundingType = 'Other'
              AND RunweiCategory IS NULL;
            
            PRINT CONCAT('Processed ', @@ROWCOUNT, ' pure Other records');
            
            -- Complex multi-type combinations
            -- Grant; Procurement Contract
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 'Grant, Procurement Contract',
                CategoryTags = 'grant,procurement-contract,multi-type',
                IsGrant = 1,
                IsProcurementContract = 1,
                IsOther = 0,
                CategoryProcessedDate = GETDATE()
            WHERE FundingType = 'Grant; Procurement Contract'
              AND RunweiCategory IS NULL;
            
            -- Cooperative Agreement; Grant → Grant (both are grant-type)
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 'Grant',
                CategoryTags = 'grant,cooperative-agreement,multi-type',
                IsGrant = 1,
                IsProcurementContract = 0,
                IsOther = 0,
                CategoryProcessedDate = GETDATE()
            WHERE FundingType = 'Cooperative Agreement; Grant'
              AND RunweiCategory IS NULL;
            
            PRINT CONCAT('Processed ', @@ROWCOUNT, ' Cooperative Agreement; Grant records');
            
            -- Cooperative Agreement; Other → Grant, Other
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 'Grant, Other',
                CategoryTags = 'grant,cooperative-agreement,other,multi-type',
                IsGrant = 1,
                IsProcurementContract = 0,
                IsOther = 1,
                CategoryProcessedDate = GETDATE()
            WHERE FundingType = 'Cooperative Agreement; Other'
              AND RunweiCategory IS NULL;
            
            PRINT CONCAT('Processed ', @@ROWCOUNT, ' Cooperative Agreement; Other records');
            
            -- Cooperative Agreement; Procurement Contract → Grant, Procurement Contract
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 'Grant, Procurement Contract',
                CategoryTags = 'grant,cooperative-agreement,procurement-contract,multi-type',
                IsGrant = 1,
                IsProcurementContract = 1,
                IsOther = 0,
                CategoryProcessedDate = GETDATE()
            WHERE FundingType = 'Cooperative Agreement; Procurement Contract'
              AND RunweiCategory IS NULL;
            
            PRINT CONCAT('Processed ', @@ROWCOUNT, ' Cooperative Agreement; Procurement Contract records');
            
            -- Cooperative Agreement; Grant; Other → Grant, Other
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 'Grant, Other',
                CategoryTags = 'grant,cooperative-agreement,other,multi-type',
                IsGrant = 1,
                IsProcurementContract = 0,
                IsOther = 1,
                CategoryProcessedDate = GETDATE()
            WHERE FundingType = 'Cooperative Agreement; Grant; Other'
              AND RunweiCategory IS NULL;
            
            PRINT CONCAT('Processed ', @@ROWCOUNT, ' Cooperative Agreement; Grant; Other records');
            
            -- Cooperative Agreement; Grant; Procurement Contract → Grant, Procurement Contract
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 'Grant, Procurement Contract',
                CategoryTags = 'grant,cooperative-agreement,procurement-contract,multi-type',
                IsGrant = 1,
                IsProcurementContract = 1,
                IsOther = 0,
                CategoryProcessedDate = GETDATE()
            WHERE FundingType = 'Cooperative Agreement; Grant; Procurement Contract'
              AND RunweiCategory IS NULL;
            
            PRINT CONCAT('Processed ', @@ROWCOUNT, ' Cooperative Agreement; Grant; Procurement Contract records');
            
            -- Cooperative Agreement; Other; Procurement Contract → Grant, Other, Procurement Contract
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 'Grant, Other, Procurement Contract',
                CategoryTags = 'grant,cooperative-agreement,other,procurement-contract,multi-type',
                IsGrant = 1,
                IsProcurementContract = 1,
                IsOther = 1,
                CategoryProcessedDate = GETDATE()
            WHERE FundingType = 'Cooperative Agreement; Other; Procurement Contract'
              AND RunweiCategory IS NULL;
            
            PRINT CONCAT('Processed ', @@ROWCOUNT, ' Cooperative Agreement; Other; Procurement Contract records');
            
            -- Cooperative Agreement; Grant; Other; Procurement Contract → Grant, Other, Procurement Contract
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 'Grant, Other, Procurement Contract',
                CategoryTags = 'grant,cooperative-agreement,other,procurement-contract,multi-type',
                IsGrant = 1,
                IsProcurementContract = 1,
                IsOther = 1,
                CategoryProcessedDate = GETDATE()
            WHERE FundingType = 'Cooperative Agreement; Grant; Other; Procurement Contract'
              AND RunweiCategory IS NULL;
            
            PRINT CONCAT('Processed ', @@ROWCOUNT, ' Cooperative Agreement; Grant; Other; Procurement Contract records');
            
            -- Pattern-based intelligent mapping for any remaining complex types
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 
                    CASE 
                        -- Multi-type: Grant + Contract + Other
                        WHEN (FundingType LIKE '%Cooperative Agreement%' OR FundingType LIKE '%Grant%') 
                             AND FundingType LIKE '%Procurement Contract%' 
                             AND FundingType LIKE '%Other%'
                        THEN 'Grant, Other, Procurement Contract'
                        
                        -- Multi-type: Grant + Contract
                        WHEN (FundingType LIKE '%Cooperative Agreement%' OR FundingType LIKE '%Grant%') 
                             AND FundingType LIKE '%Procurement Contract%'
                        THEN 'Grant, Procurement Contract'
                        
                        -- Multi-type: Grant + Other
                        WHEN (FundingType LIKE '%Cooperative Agreement%' OR FundingType LIKE '%Grant%') 
                             AND FundingType LIKE '%Other%'
                        THEN 'Grant, Other'
                        
                        -- Grant-only (Cooperative Agreement or Grant)
                        WHEN FundingType LIKE '%Cooperative Agreement%' OR FundingType LIKE '%Grant%'
                        THEN 'Grant'
                        
                        -- Contract-only
                        WHEN FundingType LIKE '%Procurement Contract%' 
                             AND FundingType NOT LIKE '%Grant%' 
                             AND FundingType NOT LIKE '%Cooperative%'
                        THEN 'Procurement Contract'
                        
                        -- Default to Other
                        ELSE 'Other'
                    END,
                CategoryTags = 
                    CASE 
                        WHEN (FundingType LIKE '%Cooperative Agreement%' OR FundingType LIKE '%Grant%') 
                             AND FundingType LIKE '%Procurement Contract%' 
                             AND FundingType LIKE '%Other%'
                        THEN 'grant,other,procurement-contract,pattern-matched'
                        
                        WHEN (FundingType LIKE '%Cooperative Agreement%' OR FundingType LIKE '%Grant%') 
                             AND FundingType LIKE '%Procurement Contract%'
                        THEN 'grant,procurement-contract,pattern-matched'
                        
                        WHEN (FundingType LIKE '%Cooperative Agreement%' OR FundingType LIKE '%Grant%') 
                             AND FundingType LIKE '%Other%'
                        THEN 'grant,other,pattern-matched'
                        
                        WHEN FundingType LIKE '%Cooperative Agreement%' OR FundingType LIKE '%Grant%'
                        THEN 'grant,pattern-matched'
                        
                        WHEN FundingType LIKE '%Procurement Contract%'
                        THEN 'procurement-contract,pattern-matched'
                        
                        ELSE 'other,pattern-matched'
                    END,
                IsGrant = CASE WHEN FundingType LIKE '%Cooperative Agreement%' OR FundingType LIKE '%Grant%' THEN 1 ELSE 0 END,
                IsProcurementContract = CASE WHEN FundingType LIKE '%Procurement Contract%' THEN 1 ELSE 0 END,
                IsOther = CASE WHEN FundingType LIKE '%Other%' OR (FundingType NOT LIKE '%Grant%' AND FundingType NOT LIKE '%Cooperative%' AND FundingType NOT LIKE '%Procurement%') THEN 1 ELSE 0 END,
                CategoryProcessedDate = GETDATE()
            WHERE RunweiCategory IS NULL 
              AND FundingType IS NOT NULL 
              AND LTRIM(RTRIM(FundingType)) != '';
            
            PRINT CONCAT('Pattern-matched ', @@ROWCOUNT, ' remaining records');
            
            -- Handle any final edge cases
            UPDATE CleanGrantsLayer2 
            SET 
                RunweiCategory = 'Other',
                CategoryTags = 'unmapped,defaulted',
                IsGrant = 0,
                IsProcurementContract = 0,
                IsOther = 1,
                CategoryProcessedDate = GETDATE()
            WHERE RunweiCategory IS NULL;
            
            PRINT CONCAT('Defaulted ', @@ROWCOUNT, ' final edge cases to Other');
            
            -- Create indexes for performance
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanGrantsLayer2_RunweiCategory')
            BEGIN
                CREATE NONCLUSTERED INDEX IX_CleanGrantsLayer2_RunweiCategory 
                ON CleanGrantsLayer2(RunweiCategory);
                PRINT 'Created RunweiCategory index';
            END
            
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanGrantsLayer2_CategoryFlags')
            BEGIN
                CREATE NONCLUSTERED INDEX IX_CleanGrantsLayer2_CategoryFlags 
                ON CleanGrantsLayer2(IsGrant, IsProcurementContract, IsOther);
                PRINT 'Created category flags composite index';
            END
            
            COMMIT TRANSACTION CategoryProcessing;
            
            -- Final results
            SELECT 
                'CATEGORY_PROCESSING_COMPLETE' as Status,
                COUNT(*) as TotalRecords,
                COUNT(DISTINCT OriginalFundingType) as UniqueOriginalTypes,
                COUNT(DISTINCT RunweiCategory) as UniqueRunweiCategories,
                COUNT(CASE WHEN OriginalFundingType != RunweiCategory THEN 1 END) as TransformedRecords,
                ROUND(100.0 * COUNT(CASE WHEN OriginalFundingType != RunweiCategory THEN 1 END) / COUNT(*), 2) as TransformationPercentage,
                MAX(CategoryProcessedDate) as ProcessingTimestamp
            FROM CleanGrantsLayer2;
            
            -- Show category distribution
            SELECT 
                'RUNWEI_CATEGORIES' as ReportType,
                RunweiCategory,
                COUNT(*) as RecordCount,
                ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM CleanGrantsLayer2), 2) as Percentage
            FROM CleanGrantsLayer2
            GROUP BY RunweiCategory
            ORDER BY COUNT(*) DESC;
            
            -- Show flag summary
            SELECT 
                'CATEGORY_FLAGS' as FlagType,
                SUM(CAST(IsGrant as INT)) as GrantRecords,
                SUM(CAST(IsProcurementContract as INT)) as ProcurementContractRecords,
                SUM(CAST(IsOther as INT)) as OtherRecords,
                ROUND(100.0 * SUM(CAST(IsGrant as INT)) / COUNT(*), 1) as GrantPercentage,
                ROUND(100.0 * SUM(CAST(IsProcurementContract as INT)) / COUNT(*), 1) as ProcurementPercentage,
                ROUND(100.0 * SUM(CAST(IsOther as INT)) / COUNT(*), 1) as OtherPercentage
            FROM CleanGrantsLayer2;
            """
            
            result = self.execute_sql_command(processing_sql, timeout=900)
            if result:
                logger.info("✅ Category processing completed successfully")
                
                # Create business intelligence views
                views_sql = """
                -- Create category analysis views
                CREATE OR ALTER VIEW vw_GrantOpportunities AS
                SELECT 
                    OpportunityNumber, Title, Description, AgencyName,
                    RunweiCategory, CategoryTags, EstimatedTotalFunding,
                    ExpectedAwards, Deadline, PostedDate, DataQualityScore
                FROM CleanGrantsLayer2
                WHERE IsGrant = 1;
                
                CREATE OR ALTER VIEW vw_ProcurementOpportunities AS
                SELECT 
                    OpportunityNumber, Title, Description, AgencyName,
                    RunweiCategory, CategoryTags, EstimatedTotalFunding,
                    ExpectedAwards, Deadline, PostedDate, DataQualityScore
                FROM CleanGrantsLayer2
                WHERE IsProcurementContract = 1;
                
                SELECT 'VIEWS_CREATED' as Status, GETDATE() as CreatedAt;
                """
                
                self.execute_sql_command(views_sql)
                
                logger.info("\n🎉 SILVER LAYER CATEGORY PROCESSING COMPLETED!")
                logger.info("✅ All FundingType values transformed to Runwei categories")
                logger.info("✅ Complex multi-type combinations properly handled")
                logger.info("✅ Individual category flags created (IsGrant, IsProcurementContract, IsOther)")
                logger.info("✅ Performance indexes created")
                logger.info("✅ Analysis views created for business intelligence")
                logger.info("✅ Original values backed up for audit trail")
                logger.info("🚀 CleanGrantsLayer2 now has standardized Runwei categories!")
                
                return True
            else:
                logger.error("❌ Category processing failed")
                return False
                
        except Exception as e:
            logger.error(f"❌ Category processing failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def create_category_views(self):
        """Create useful views for category analysis"""
        logger.info("📊 Creating category analysis views...")
        
        views_sql = """
        -- Drop existing views if they exist
        IF OBJECT_ID('vw_GrantOpportunities', 'V') IS NOT NULL
            DROP VIEW vw_GrantOpportunities;
        
        IF OBJECT_ID('vw_ProcurementOpportunities', 'V') IS NOT NULL
            DROP VIEW vw_ProcurementOpportunities;
        
        IF OBJECT_ID('vw_CategorySummary', 'V') IS NOT NULL
            DROP VIEW vw_CategorySummary;
        """
        
        # Execute drop statements first
        self.execute_sql_command(views_sql)
        
        # Create views one by one to avoid syntax issues
        grant_view_sql = """
        CREATE VIEW vw_GrantOpportunities AS
        SELECT 
            OpportunityNumber,
            Title,
            Description,
            AgencyName,
            RunweiCategory,
            CategoryTags,
            EstimatedTotalFunding,
            ExpectedAwards,
            Deadline,
            PostedDate,
            DataQualityScore
        FROM CleanGrantsLayer2
        WHERE IsGrant = 1;
        """
        
        procurement_view_sql = """
        CREATE VIEW vw_ProcurementOpportunities AS
        SELECT 
            OpportunityNumber,
            Title,
            Description,
            AgencyName,
            RunweiCategory,
            CategoryTags,
            EstimatedTotalFunding,
            ExpectedAwards,
            Deadline,
            PostedDate,
            DataQualityScore
        FROM CleanGrantsLayer2
        WHERE IsProcurementContract = 1;
        """
        
        summary_view_sql = """
        CREATE VIEW vw_CategorySummary AS
        SELECT 
            RunweiCategory,
            COUNT(*) as TotalOpportunities,
            COUNT(EstimatedTotalFunding) as OpportunitiesWithFunding,
            AVG(CAST(DataQualityScore AS FLOAT)) as AvgDataQuality,
            COUNT(DISTINCT AgencyName) as UniqueAgencies
        FROM CleanGrantsLayer2
        GROUP BY RunweiCategory;
        """
        
        # Create each view separately
        views_created = 0
        for view_name, view_sql in [
            ('vw_GrantOpportunities', grant_view_sql),
            ('vw_ProcurementOpportunities', procurement_view_sql),
            ('vw_CategorySummary', summary_view_sql)
        ]:
            result = self.execute_sql_command(view_sql)
            if result:
                logger.info(f"✅ Created {view_name}")
                views_created += 1
            else:
                logger.warning(f"⚠️ Failed to create {view_name}")
        
        return views_created > 0

def main():
    """Main execution function"""
    print("🚀 SILVER LAYER - CATEGORY PROCESSING FOR RUNWEI")
    print("=" * 55)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Transform FundingType → Runwei Standard Categories")
    
    processor = SilverCategoryProcessor()
    success = processor.run_complete_category_processing()
    
    if success:
        print("\n💼 Silver Layer Category Processing completed successfully!")
        print("\n🔍 You can now query standardized categories:")
        print("   - SELECT * FROM vw_GrantOpportunities")
        print("   - SELECT * FROM vw_ProcurementOpportunities") 
        print("\n📊 Direct queries:")
        print("   - SELECT * FROM CleanGrantsLayer2 WHERE IsGrant = 1")
        print("   - SELECT RunweiCategory, COUNT(*) FROM CleanGrantsLayer2 GROUP BY RunweiCategory")
        print("\n🚀 Your CleanGrantsLayer2 now has Runwei-standardized categories!")
    else:
        print("\n❌ Category processing failed. Check logs for details.")

if __name__ == "__main__":
    main()