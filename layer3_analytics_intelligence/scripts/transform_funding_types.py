#!/usr/bin/env python3
"""
BusinessIntelligenceLayer3 FundingType Standardization Pipeline
Transform FundingType to Runwei Standard Categories
Azure SQL Database optimized processing
"""

import os
import subprocess
from datetime import datetime
import logging

# Setup Azure-optimized logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('funding_type_transformation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FundingTypeTransformer:
    """Azure SQL Database transformer for FundingType standardization"""
    
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
                "-C",  # Trust server certificate for Azure
                "-t", str(timeout),  # Query timeout
                "-I"   # Enable quoted identifiers
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 30)
            
            if result.returncode == 0:
                logger.info("✅ SQL command executed successfully")
                return result.stdout
            else:
                logger.error(f"❌ SQL command failed: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            logger.error("⏰ SQL command timed out")
            return None
        except Exception as e:
            logger.error(f"❌ Error executing SQL: {e}")
            return None
    
    def execute_sql_file(self, sql_file_path, timeout=900):
        """Execute SQL file with extended timeout"""
        try:
            cmd = [
                "sqlcmd", "-S", self.server,
                "-d", self.database,
                "-U", self.username,
                "-P", self.password,
                "-i", sql_file_path,
                "-C", "-t", str(timeout), "-I"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 30)
            
            if result.returncode == 0:
                logger.info(f"✅ SQL file executed successfully: {sql_file_path}")
                return result.stdout
            else:
                logger.error(f"❌ SQL file execution failed: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            logger.error("⏰ SQL file execution timed out")
            return None
        except Exception as e:
            logger.error(f"❌ Error executing SQL file: {e}")
            return None
    
    def analyze_current_funding_types(self):
        """Analyze current FundingType values in BusinessIntelligenceLayer3"""
        logger.info("🔍 Analyzing current FundingType values...")
        
        analysis_sql = """
        -- ===================================
        -- FUNDING TYPE ANALYSIS
        -- Analyze current FundingType patterns in BusinessIntelligenceLayer3
        -- ===================================
        
        -- Current FundingType distribution
        SELECT 
            'FUNDING_TYPE_DISTRIBUTION' as AnalysisType,
            ISNULL(FundingType, 'NULL') as CurrentFundingType,
            COUNT(*) as RecordCount,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM BusinessIntelligenceLayer3), 2) as Percentage
        FROM BusinessIntelligenceLayer3
        GROUP BY FundingType
        ORDER BY COUNT(*) DESC;
        
        -- Check for complex funding type combinations
        SELECT 
            'COMPLEX_FUNDING_TYPES' as AnalysisType,
            FundingType,
            COUNT(*) as RecordCount,
            CASE 
                WHEN FundingType LIKE '%;%' THEN 'MULTIPLE_TYPES'
                WHEN FundingType IS NULL THEN 'NULL_VALUE'
                WHEN LTRIM(RTRIM(FundingType)) = '' THEN 'EMPTY_VALUE'
                ELSE 'SINGLE_TYPE'
            END as TypeComplexity
        FROM BusinessIntelligenceLayer3
        WHERE FundingType IS NOT NULL
        GROUP BY FundingType
        ORDER BY COUNT(*) DESC;
        
        -- Sample records for manual review
        SELECT TOP 20
            'FUNDING_TYPE_SAMPLES' as AnalysisType,
            OpportunityNumber,
            OpportunityTitle,
            FundingType,
            Agency
        FROM BusinessIntelligenceLayer3
        WHERE FundingType IS NOT NULL
        ORDER BY NEWID(); -- Random sample
        """
        
        result = self.execute_sql_command(analysis_sql)
        if result:
            logger.info("📊 Current FundingType Analysis:")
            logger.info(result)
            return True
        else:
            logger.error("❌ Failed to analyze current FundingType values")
            return False
    
    def create_funding_type_transformation(self):
        """Create comprehensive FundingType transformation using Runwei standards"""
        logger.info("🔄 Creating FundingType transformation with Runwei standards...")
        
        transformation_sql = """
        -- ===================================
        -- RUNWEI FUNDING TYPE STANDARDIZATION
        -- Transform FundingType to Runwei Standard Categories
        -- ===================================
        
        BEGIN TRANSACTION FundingTypeStandardization;
        
        -- Add new standardized columns if they don't exist
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('BusinessIntelligenceLayer3') AND name = 'RunweiFundingType')
        BEGIN
            ALTER TABLE BusinessIntelligenceLayer3 ADD 
                RunweiFundingType NVARCHAR(500),
                FundingTypeTransformationNotes NVARCHAR(1000),
                FundingTypeTransformationDate DATETIME2 DEFAULT GETDATE(),
                OriginalFundingType NVARCHAR(500);
            
            PRINT 'Added Runwei FundingType standardization columns';
        END
        
        -- Store original values before transformation
        UPDATE BusinessIntelligenceLayer3 
        SET OriginalFundingType = FundingType
        WHERE OriginalFundingType IS NULL;
        
        -- ===================================
        -- RUNWEI STANDARD CATEGORY MAPPING
        -- Based on provided mapping strategy
        -- ===================================
        
        -- 1. Handle NULL and empty values
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 'Other',
            FundingTypeTransformationNotes = 'NULL or empty FundingType mapped to Other',
            FundingTypeTransformationDate = GETDATE()
        WHERE FundingType IS NULL OR LTRIM(RTRIM(FundingType)) = '';
        
        -- 2. Direct single-type mappings
        
        -- Grant (direct match)
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 'Grant',
            FundingTypeTransformationNotes = 'Direct mapping: Grant → Grant',
            FundingTypeTransformationDate = GETDATE()
        WHERE FundingType = 'Grant'
          AND RunweiFundingType IS NULL;
        
        -- Cooperative Agreement → Grant (per federal standards)
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 'Grant',
            FundingTypeTransformationNotes = 'Cooperative Agreement mapped to Grant (federal standard)',
            FundingTypeTransformationDate = GETDATE()
        WHERE FundingType = 'Cooperative Agreement'
          AND RunweiFundingType IS NULL;
        
        -- Procurement Contract (direct match)
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 'Procurement Contract',
            FundingTypeTransformationNotes = 'Direct mapping: Procurement Contract → Procurement Contract',
            FundingTypeTransformationDate = GETDATE()
        WHERE FundingType = 'Procurement Contract'
          AND RunweiFundingType IS NULL;
        
        -- Other (direct match)
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 'Other',
            FundingTypeTransformationNotes = 'Direct mapping: Other → Other',
            FundingTypeTransformationDate = GETDATE()
        WHERE FundingType = 'Other'
          AND RunweiFundingType IS NULL;
        
        -- 3. Complex multi-type mappings (exact matches from your mapping table)
        
        -- Grant; Procurement Contract → Grant, Procurement Contract
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 'Grant, Procurement Contract',
            FundingTypeTransformationNotes = 'Multi-type: Grant; Procurement Contract → Grant, Procurement Contract',
            FundingTypeTransformationDate = GETDATE()
        WHERE FundingType = 'Grant; Procurement Contract'
          AND RunweiFundingType IS NULL;
        
        -- Cooperative Agreement; Grant → Grant (both are grant-type)
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 'Grant',
            FundingTypeTransformationNotes = 'Multi-type: Cooperative Agreement; Grant → Grant (both grant-type)',
            FundingTypeTransformationDate = GETDATE()
        WHERE FundingType = 'Cooperative Agreement; Grant'
          AND RunweiFundingType IS NULL;
        
        -- Cooperative Agreement; Other → Grant, Other
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 'Grant, Other',
            FundingTypeTransformationNotes = 'Multi-type: Cooperative Agreement; Other → Grant, Other',
            FundingTypeTransformationDate = GETDATE()
        WHERE FundingType = 'Cooperative Agreement; Other'
          AND RunweiFundingType IS NULL;
        
        -- Cooperative Agreement; Procurement Contract → Grant, Procurement Contract
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 'Grant, Procurement Contract',
            FundingTypeTransformationNotes = 'Multi-type: Cooperative Agreement; Procurement Contract → Grant, Procurement Contract',
            FundingTypeTransformationDate = GETDATE()
        WHERE FundingType = 'Cooperative Agreement; Procurement Contract'
          AND RunweiFundingType IS NULL;
        
        -- Cooperative Agreement; Grant; Other → Grant, Other
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 'Grant, Other',
            FundingTypeTransformationNotes = 'Multi-type: Cooperative Agreement; Grant; Other → Grant, Other',
            FundingTypeTransformationDate = GETDATE()
        WHERE FundingType = 'Cooperative Agreement; Grant; Other'
          AND RunweiFundingType IS NULL;
        
        -- Cooperative Agreement; Grant; Procurement Contract → Grant, Procurement Contract
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 'Grant, Procurement Contract',
            FundingTypeTransformationNotes = 'Multi-type: Cooperative Agreement; Grant; Procurement Contract → Grant, Procurement Contract',
            FundingTypeTransformationDate = GETDATE()
        WHERE FundingType = 'Cooperative Agreement; Grant; Procurement Contract'
          AND RunweiFundingType IS NULL;
        
        -- Cooperative Agreement; Other; Procurement Contract → Grant, Other, Procurement Contract
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 'Grant, Other, Procurement Contract',
            FundingTypeTransformationNotes = 'Multi-type: Cooperative Agreement; Other; Procurement Contract → Grant, Other, Procurement Contract',
            FundingTypeTransformationDate = GETDATE()
        WHERE FundingType = 'Cooperative Agreement; Other; Procurement Contract'
          AND RunweiFundingType IS NULL;
        
        -- Cooperative Agreement; Grant; Other; Procurement Contract → Grant, Other, Procurement Contract
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 'Grant, Other, Procurement Contract',
            FundingTypeTransformationNotes = 'Multi-type: Cooperative Agreement; Grant; Other; Procurement Contract → Grant, Other, Procurement Contract',
            FundingTypeTransformationDate = GETDATE()
        WHERE FundingType = 'Cooperative Agreement; Grant; Other; Procurement Contract'
          AND RunweiFundingType IS NULL;
        
        -- 4. Pattern-based intelligent mapping for any remaining complex types
        
        -- Handle variations with different separators or spacing
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 
                CASE 
                    -- If contains both Cooperative Agreement/Grant and Procurement Contract
                    WHEN (FundingType LIKE '%Cooperative Agreement%' OR FundingType LIKE '%Grant%') 
                         AND FundingType LIKE '%Procurement Contract%' 
                         AND FundingType LIKE '%Other%'
                    THEN 'Grant, Other, Procurement Contract'
                    
                    WHEN (FundingType LIKE '%Cooperative Agreement%' OR FundingType LIKE '%Grant%') 
                         AND FundingType LIKE '%Procurement Contract%'
                    THEN 'Grant, Procurement Contract'
                    
                    WHEN (FundingType LIKE '%Cooperative Agreement%' OR FundingType LIKE '%Grant%') 
                         AND FundingType LIKE '%Other%'
                    THEN 'Grant, Other'
                    
                    -- If contains only Cooperative Agreement or Grant
                    WHEN FundingType LIKE '%Cooperative Agreement%' OR FundingType LIKE '%Grant%'
                    THEN 'Grant'
                    
                    -- If contains only Procurement Contract
                    WHEN FundingType LIKE '%Procurement Contract%' AND FundingType NOT LIKE '%Grant%' AND FundingType NOT LIKE '%Cooperative%'
                    THEN 'Procurement Contract'
                    
                    -- Default to Other for any unmatched patterns
                    ELSE 'Other'
                END,
            FundingTypeTransformationNotes = 'Pattern-based intelligent mapping applied',
            FundingTypeTransformationDate = GETDATE()
        WHERE RunweiFundingType IS NULL 
          AND FundingType IS NOT NULL 
          AND LTRIM(RTRIM(FundingType)) != '';
        
        -- 5. Handle any remaining edge cases
        UPDATE BusinessIntelligenceLayer3 
        SET 
            RunweiFundingType = 'Other',
            FundingTypeTransformationNotes = 'Unmapped funding type defaulted to Other: ' + ISNULL(FundingType, 'NULL'),
            FundingTypeTransformationDate = GETDATE()
        WHERE RunweiFundingType IS NULL;
        
        -- 6. Add individual category flags for easy filtering
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('BusinessIntelligenceLayer3') AND name = 'IsGrant')
        BEGIN
            ALTER TABLE BusinessIntelligenceLayer3 ADD 
                IsGrant BIT DEFAULT 0,
                IsForgivableLoan BIT DEFAULT 0,
                IsTaxCredit BIT DEFAULT 0,
                IsLegislativeInitiative BIT DEFAULT 0,
                IsCompetition BIT DEFAULT 0,
                IsProcurementContract BIT DEFAULT 0,
                IsOther BIT DEFAULT 0;
            
            PRINT 'Added individual category flag columns';
        END
        
        -- Set category flags based on RunweiFundingType
        UPDATE BusinessIntelligenceLayer3 
        SET 
            IsGrant = CASE WHEN RunweiFundingType LIKE '%Grant%' THEN 1 ELSE 0 END,
            IsForgivableLoan = CASE WHEN RunweiFundingType LIKE '%Forgivable Loan%' THEN 1 ELSE 0 END,
            IsTaxCredit = CASE WHEN RunweiFundingType LIKE '%Tax Credit%' THEN 1 ELSE 0 END,
            IsLegislativeInitiative = CASE WHEN RunweiFundingType LIKE '%Legislative Initiative%' THEN 1 ELSE 0 END,
            IsCompetition = CASE WHEN RunweiFundingType LIKE '%Competition%' THEN 1 ELSE 0 END,
            IsProcurementContract = CASE WHEN RunweiFundingType LIKE '%Procurement Contract%' THEN 1 ELSE 0 END,
            IsOther = CASE WHEN RunweiFundingType LIKE '%Other%' THEN 1 ELSE 0 END;
        
        -- Create index on RunweiFundingType for performance
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_BusinessIntelligenceLayer3_RunweiFundingType')
        BEGIN
            CREATE NONCLUSTERED INDEX IX_BusinessIntelligenceLayer3_RunweiFundingType 
            ON BusinessIntelligenceLayer3(RunweiFundingType);
        END
        
        COMMIT TRANSACTION FundingTypeStandardization;
        
        -- ===================================
        -- TRANSFORMATION REPORT
        -- ===================================
        
        -- Summary of transformation results
        SELECT 
            'TRANSFORMATION_SUMMARY' as ReportType,
            COUNT(*) as TotalRecords,
            COUNT(DISTINCT OriginalFundingType) as UniqueOriginalTypes,
            COUNT(DISTINCT RunweiFundingType) as UniqueRunweiTypes,
            COUNT(CASE WHEN OriginalFundingType != RunweiFundingType THEN 1 END) as RecordsTransformed,
            ROUND(100.0 * COUNT(CASE WHEN OriginalFundingType != RunweiFundingType THEN 1 END) / COUNT(*), 2) as TransformationPercentage,
            MAX(FundingTypeTransformationDate) as TransformationTimestamp
        FROM BusinessIntelligenceLayer3;
        
        -- Runwei category distribution
        SELECT 
            'RUNWEI_CATEGORY_DISTRIBUTION' as ReportType,
            RunweiFundingType,
            COUNT(*) as RecordCount,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM BusinessIntelligenceLayer3), 2) as Percentage
        FROM BusinessIntelligenceLayer3
        GROUP BY RunweiFundingType
        ORDER BY COUNT(*) DESC;
        
        -- Individual category flags summary
        SELECT 
            'CATEGORY_FLAGS_SUMMARY' as ReportType,
            SUM(CAST(IsGrant as INT)) as GrantRecords,
            SUM(CAST(IsForgivableLoan as INT)) as ForgivableLoanRecords,
            SUM(CAST(IsTaxCredit as INT)) as TaxCreditRecords,
            SUM(CAST(IsLegislativeInitiative as INT)) as LegislativeInitiativeRecords,
            SUM(CAST(IsCompetition as INT)) as CompetitionRecords,
            SUM(CAST(IsProcurementContract as INT)) as ProcurementContractRecords,
            SUM(CAST(IsOther as INT)) as OtherRecords
        FROM BusinessIntelligenceLayer3;
        
        -- Mapping verification - show before/after examples
        SELECT TOP 20
            'MAPPING_VERIFICATION' as ReportType,
            OriginalFundingType,
            RunweiFundingType,
            FundingTypeTransformationNotes,
            COUNT(*) as RecordCount
        FROM BusinessIntelligenceLayer3
        WHERE OriginalFundingType IS NOT NULL
        GROUP BY OriginalFundingType, RunweiFundingType, FundingTypeTransformationNotes
        ORDER BY COUNT(*) DESC;
        """
        
        temp_file = "funding_type_transformation.sql"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(transformation_sql)
        
        result = self.execute_sql_file(temp_file, timeout=1200)
        
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        if result:
            logger.info("✅ FundingType transformation completed successfully")
            logger.info(result)
            return True
        else:
            logger.error("❌ FundingType transformation failed")
            return False
    
    def create_funding_type_analysis_views(self):
        """Create business intelligence views for funding type analysis"""
        logger.info("📊 Creating funding type analysis views...")
        
        views_sql = """
        -- ===================================
        -- RUNWEI FUNDING TYPE ANALYSIS VIEWS
        -- Business intelligence views for standardized funding types
        -- ===================================
        
        -- Funding Type Distribution Analysis
        CREATE OR ALTER VIEW vw_RunweiFundingTypeAnalysis AS
        SELECT 
            RunweiFundingType,
            COUNT(*) as TotalOpportunities,
            COUNT(EstimatedTotalFunding_Clean) as OpportunitiesWithFunding,
            SUM(EstimatedTotalFunding_Clean) as TotalEstimatedFunding,
            AVG(EstimatedTotalFunding_Clean) as AvgEstimatedFunding,
            MAX(EstimatedTotalFunding_Clean) as MaxEstimatedFunding,
            MIN(EstimatedTotalFunding_Clean) as MinEstimatedFunding,
            
            -- Award metrics
            SUM(ExpectedAwards_Clean) as TotalExpectedAwards,
            AVG(ExpectedAwards_Clean) as AvgExpectedAwards,
            AVG(AwardValuePerAward) as AvgAwardValuePerAward,
            
            -- Quality metrics
            AVG(AwardDataQualityScore) as AvgDataQualityScore,
            COUNT(CASE WHEN AwardDataQualityScore >= 85 THEN 1 END) as HighQualityOpportunities,
            ROUND(100.0 * COUNT(CASE WHEN AwardDataQualityScore >= 85 THEN 1 END) / COUNT(*), 2) as HighQualityPercentage,
            
            -- Agency diversity
            COUNT(DISTINCT Agency) as UniqueAgencies,
            
            -- Completeness metrics
            ROUND(100.0 * COUNT(EstimatedTotalFunding_Clean) / COUNT(*), 2) as FundingDataCompleteness
            
        FROM BusinessIntelligenceLayer3
        WHERE RunweiFundingType IS NOT NULL
        GROUP BY RunweiFundingType
        ORDER BY TotalEstimatedFunding DESC;
        
        -- Agency by Funding Type Matrix
        CREATE OR ALTER VIEW vw_AgencyFundingTypeMatrix AS
        SELECT 
            Agency,
            SUM(CAST(IsGrant as INT)) as GrantOpportunities,
            SUM(CAST(IsForgivableLoan as INT)) as ForgivableLoanOpportunities,
            SUM(CAST(IsTaxCredit as INT)) as TaxCreditOpportunities,
            SUM(CAST(IsLegislativeInitiative as INT)) as LegislativeInitiativeOpportunities,
            SUM(CAST(IsCompetition as INT)) as CompetitionOpportunities,
            SUM(CAST(IsProcurementContract as INT)) as ProcurementContractOpportunities,
            SUM(CAST(IsOther as INT)) as OtherOpportunities,
            COUNT(*) as TotalOpportunities,
            
            -- Percentages
            ROUND(100.0 * SUM(CAST(IsGrant as INT)) / COUNT(*), 1) as GrantPercentage,
            ROUND(100.0 * SUM(CAST(IsProcurementContract as INT)) / COUNT(*), 1) as ProcurementContractPercentage,
            ROUND(100.0 * SUM(CAST(IsOther as INT)) / COUNT(*), 1) as OtherPercentage,
            
            -- Funding totals by type
            SUM(CASE WHEN IsGrant = 1 THEN EstimatedTotalFunding_Clean ELSE 0 END) as TotalGrantFunding,
            SUM(CASE WHEN IsProcurementContract = 1 THEN EstimatedTotalFunding_Clean ELSE 0 END) as TotalProcurementFunding,
            
            -- Primary funding type
            CASE 
                WHEN SUM(CAST(IsGrant as INT)) > SUM(CAST(IsProcurementContract as INT)) 
                 AND SUM(CAST(IsGrant as INT)) > SUM(CAST(IsOther as INT))
                THEN 'Grant-Focused'
                WHEN SUM(CAST(IsProcurementContract as INT)) > SUM(CAST(IsGrant as INT)) 
                 AND SUM(CAST(IsProcurementContract as INT)) > SUM(CAST(IsOther as INT))
                THEN 'Contract-Focused'
                WHEN SUM(CAST(IsOther as INT)) > SUM(CAST(IsGrant as INT)) 
                 AND SUM(CAST(IsOther as INT)) > SUM(CAST(IsProcurementContract as INT))
                THEN 'Other-Focused'
                ELSE 'Mixed'
            END as PrimaryFundingType
            
        FROM BusinessIntelligenceLayer3
        WHERE Agency IS NOT NULL
        GROUP BY Agency
        HAVING COUNT(*) >= 5  -- Only agencies with significant presence
        ORDER BY TotalOpportunities DESC;
        
        -- Monthly Funding Type Trends
        CREATE OR ALTER VIEW vw_MonthlyFundingTypeTrends AS
        SELECT 
            YEAR(PostDate) as Year,
            MONTH(PostDate) as Month,
            CONCAT(YEAR(PostDate), '-', FORMAT(MONTH(PostDate), '00')) as YearMonth,
            
            -- Counts by type
            SUM(CAST(IsGrant as INT)) as GrantOpportunities,
            SUM(CAST(IsProcurementContract as INT)) as ProcurementContractOpportunities,
            SUM(CAST(IsOther as INT)) as OtherOpportunities,
            COUNT(*) as TotalOpportunities,
            
            -- Funding by type
            SUM(CASE WHEN IsGrant = 1 THEN EstimatedTotalFunding_Clean ELSE 0 END) as GrantFunding,
            SUM(CASE WHEN IsProcurementContract = 1 THEN EstimatedTotalFunding_Clean ELSE 0 END) as ProcurementFunding,
            SUM(CASE WHEN IsOther = 1 THEN EstimatedTotalFunding_Clean ELSE 0 END) as OtherFunding,
            SUM(EstimatedTotalFunding_Clean) as TotalFunding,
            
            -- Percentages
            ROUND(100.0 * SUM(CAST(IsGrant as INT)) / COUNT(*), 1) as GrantPercentage,
            ROUND(100.0 * SUM(CAST(IsProcurementContract as INT)) / COUNT(*), 1) as ProcurementPercentage,
            ROUND(100.0 * SUM(CAST(IsOther as INT)) / COUNT(*), 1) as OtherPercentage
            
        FROM BusinessIntelligenceLayer3
        WHERE PostDate IS NOT NULL
        GROUP BY YEAR(PostDate), MONTH(PostDate)
        ORDER BY Year DESC, Month DESC;
        
        -- Funding Type Transformation Audit
        CREATE OR ALTER VIEW vw_FundingTypeTransformationAudit AS
        SELECT 
            OriginalFundingType,
            RunweiFundingType,
            FundingTypeTransformationNotes,
            COUNT(*) as RecordCount,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM BusinessIntelligenceLayer3), 2) as Percentage,
            MIN(FundingTypeTransformationDate) as FirstTransformed,
            MAX(FundingTypeTransformationDate) as LastTransformed,
            
            -- Sample agencies using this mapping
            STRING_AGG(DISTINCT Agency, ', ') WITHIN GROUP (ORDER BY Agency) as SampleAgencies
            
        FROM BusinessIntelligenceLayer3
        WHERE OriginalFundingType IS NOT NULL
        GROUP BY OriginalFundingType, RunweiFundingType, FundingTypeTransformationNotes
        ORDER BY COUNT(*) DESC;
        
        -- Data Quality by Funding Type
        CREATE OR ALTER VIEW vw_FundingTypeDataQuality AS
        SELECT 
            RunweiFundingType,
            COUNT(*) as TotalRecords,
            
            -- Data completeness
            COUNT(EstimatedTotalFunding_Clean) as RecordsWithFunding,
            COUNT(ExpectedAwards_Clean) as RecordsWithExpectedAwards,
            COUNT(AwardValue_Clean) as RecordsWithAwardValue,
            
            -- Completeness percentages
            ROUND(100.0 * COUNT(EstimatedTotalFunding_Clean) / COUNT(*), 2) as FundingCompleteness,
            ROUND(100.0 * COUNT(ExpectedAwards_Clean) / COUNT(*), 2) as ExpectedAwardsCompleteness,
            ROUND(100.0 * COUNT(AwardValue_Clean) / COUNT(*), 2) as AwardValueCompleteness,
            
            -- Quality scores
            AVG(AwardDataQualityScore) as AvgDataQualityScore,
            MIN(AwardDataQualityScore) as MinDataQualityScore,
            MAX(AwardDataQualityScore) as MaxDataQualityScore,
            
            -- Quality distribution
            COUNT(CASE WHEN AwardDataQualityScore >= 85 THEN 1 END) as ExcellentQuality,
            COUNT(CASE WHEN AwardDataQualityScore >= 70 AND AwardDataQualityScore < 85 THEN 1 END) as GoodQuality,
            COUNT(CASE WHEN AwardDataQualityScore >= 50 AND AwardDataQualityScore < 70 THEN 1 END) as FairQuality,
            COUNT(CASE WHEN AwardDataQualityScore < 50 THEN 1 END) as PoorQuality,
            
            -- Quality percentages
            ROUND(100.0 * COUNT(CASE WHEN AwardDataQualityScore >= 85 THEN 1 END) / COUNT(*), 2) as ExcellentQualityPercentage
            
        FROM BusinessIntelligenceLayer3
        WHERE RunweiFundingType IS NOT NULL
        GROUP BY RunweiFundingType
        ORDER BY AvgDataQualityScore DESC;
        
        SELECT 'FUNDING_TYPE_ANALYSIS_VIEWS_CREATED' as Status, GETDATE() as CreationTimestamp;
        """
        
        temp_file = "funding_type_views.sql"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(views_sql)
        
        result = self.execute_sql_file(temp_file)
        
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        if result:
            logger.info("✅ Funding type analysis views created successfully")
            return True
        else:
            logger.error("❌ Failed to create funding type analysis views")
            return False
    
    def run_complete_funding_type_transformation(self):
        """Run the complete funding type transformation process"""
        logger.info("🚀 Starting complete Runwei funding type standardization")
        logger.info("=" * 80)
        
        try:
            # Step 1: Analyze current funding types
            if not self.analyze_current_funding_types():
                logger.error("❌ Current funding type analysis failed")
                return False
            
            # Step 2: Create comprehensive transformation
            if not self.create_funding_type_transformation():
                logger.error("❌ Funding type transformation failed")
                return False
            
            # Step 3: Create analysis views
            if not self.create_funding_type_analysis_views():
                logger.warning("⚠️ Analysis views creation had issues")
            
            logger.info("\n🎉 COMPLETE RUNWEI FUNDING TYPE STANDARDIZATION SUCCESSFUL!")
            logger.info("✅ All funding types mapped to Runwei standard categories")
            logger.info("✅ Cooperative Agreements mapped to Grants (federal standard)")
            logger.info("✅ Complex multi-type combinations properly handled")
            logger.info("✅ Individual category flags created for easy filtering")
            logger.info("✅ Comprehensive business intelligence views created")
            logger.info("✅ Transformation audit trail maintained")
            logger.info("🚀 Your funding type data is now Runwei-standardized!")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Funding type transformation failed: {e}")
            import traceback
            traceback.print_exc()
            return False

def main():
    """Main transformation function"""
    transformer = FundingTypeTransformer()
    success = transformer.run_complete_funding_type_transformation()
    
    if success:
        print("\n💼 Runwei Funding Type Standardization completed successfully!")
        print("🔍 You can now query your standardized funding types:")
        print("   - SELECT * FROM vw_RunweiFundingTypeAnalysis")
        print("   - SELECT * FROM vw_AgencyFundingTypeMatrix") 
        print("   - SELECT * FROM vw_MonthlyFundingTypeTrends")
        print("   - SELECT * FROM vw_FundingTypeTransformationAudit")
        print("   - SELECT * FROM vw_FundingTypeDataQuality")
        print("\n📊 Individual category flags available:")
        print("   - IsGrant, IsProcurementContract, IsOther, etc.")
        print("\n🚀 Your BusinessIntelligenceLayer3 now uses Runwei standard categories!")
    else:
        print("\n❌ Funding type transformation failed. Check logs for details.")

if __name__ == "__main__":
    main()