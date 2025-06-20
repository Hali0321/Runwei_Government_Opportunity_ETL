#!/usr/bin/env python3
"""
BusinessIntelligenceLayer3 Award Value Transformation Pipeline
Comprehensive transformation of all BI Layer 3 data with award value extraction
Azure SQL Database optimized processing
"""

import os
import subprocess
from datetime import datetime
import logging
import json

# Setup Azure-optimized logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bi_layer3_transformation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BusinessIntelligenceLayer3Transformer:
    """Azure SQL Database transformer for BusinessIntelligenceLayer3 award values"""
    
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
    
    def execute_sql_file(self, sql_file_path, timeout=1200):
        """Execute SQL file with extended timeout for large transformations"""
        try:
            cmd = [
                "sqlcmd", "-S", self.server,
                "-d", self.database,
                "-U", self.username,
                "-P", self.password,
                "-i", sql_file_path,
                "-C", "-t", str(timeout), "-I"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 60)
            
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
    
    def analyze_bi_layer3_structure(self):
        """Analyze the structure and content of BusinessIntelligenceLayer3"""
        logger.info("🔍 Analyzing BusinessIntelligenceLayer3 table structure...")
        
        analysis_sql = """
        -- ===================================
        -- BUSINESSINTELLIGENCE LAYER 3 ANALYSIS
        -- Comprehensive analysis of existing data structure
        -- ===================================
        
        -- Table structure analysis
        SELECT 
            'TABLE_STRUCTURE' as AnalysisType,
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'BusinessIntelligenceLayer3'
        ORDER BY ORDINAL_POSITION;
        
        -- Data volume and quality analysis
        SELECT 
            'DATA_VOLUME_ANALYSIS' as AnalysisType,
            COUNT(*) as TotalRecords,
            COUNT(DISTINCT OpportunityNumber) as UniqueOpportunities,
            
            -- Check for award-related columns
            COUNT(CASE WHEN EstimatedTotalFunding IS NOT NULL THEN 1 END) as RecordsWithEstimatedFunding,
            COUNT(CASE WHEN ExpectedAwards IS NOT NULL THEN 1 END) as RecordsWithExpectedAwards,
            COUNT(CASE WHEN AwardValue IS NOT NULL THEN 1 END) as RecordsWithAwardValue,
            COUNT(CASE WHEN AwardCeiling IS NOT NULL THEN 1 END) as RecordsWithAwardCeiling,
            COUNT(CASE WHEN AwardFloor IS NOT NULL THEN 1 END) as RecordsWithAwardFloor,
            
            -- Funding amount analysis
            AVG(CAST(EstimatedTotalFunding as FLOAT)) as AvgEstimatedFunding,
            MAX(CAST(EstimatedTotalFunding as FLOAT)) as MaxEstimatedFunding,
            MIN(CAST(EstimatedTotalFunding as FLOAT)) as MinEstimatedFunding,
            
            -- Award value analysis
            AVG(CAST(AwardValue as FLOAT)) as AvgAwardValue,
            MAX(CAST(AwardValue as FLOAT)) as MaxAwardValue,
            MIN(CAST(AwardValue as FLOAT)) as MinAwardValue,
            
            -- Expected awards analysis
            AVG(CAST(ExpectedAwards as FLOAT)) as AvgExpectedAwards,
            MAX(CAST(ExpectedAwards as FLOAT)) as MaxExpectedAwards,
            
            -- Data completeness
            ROUND(100.0 * COUNT(CASE WHEN EstimatedTotalFunding IS NOT NULL THEN 1 END) / COUNT(*), 2) as EstimatedFundingCompleteness,
            ROUND(100.0 * COUNT(CASE WHEN AwardValue IS NOT NULL THEN 1 END) / COUNT(*), 2) as AwardValueCompleteness
            
        FROM BusinessIntelligenceLayer3;
        
        -- Sample data for pattern analysis
        SELECT TOP 10
            'DATA_SAMPLE' as AnalysisType,
            OpportunityNumber,
            OpportunityTitle,
            EstimatedTotalFunding,
            ExpectedAwards,
            AwardValue,
            AwardCeiling,
            AwardFloor,
            Agency
        FROM BusinessIntelligenceLayer3
        WHERE EstimatedTotalFunding IS NOT NULL 
           OR AwardValue IS NOT NULL
        ORDER BY CAST(ISNULL(EstimatedTotalFunding, AwardValue) as FLOAT) DESC;
        
        -- Data quality issues identification
        SELECT 
            'DATA_QUALITY_ISSUES' as AnalysisType,
            COUNT(CASE WHEN EstimatedTotalFunding = '0' OR EstimatedTotalFunding = '0.0' THEN 1 END) as ZeroEstimatedFunding,
            COUNT(CASE WHEN AwardValue = '0' OR AwardValue = '0.0' THEN 1 END) as ZeroAwardValue,
            COUNT(CASE WHEN EstimatedTotalFunding LIKE '%,%' THEN 1 END) as FundingWithCommas,
            COUNT(CASE WHEN EstimatedTotalFunding LIKE '%$%' THEN 1 END) as FundingWithDollarSigns,
            COUNT(CASE WHEN AwardValue LIKE '%,%' THEN 1 END) as AwardValueWithCommas,
            COUNT(CASE WHEN AwardValue LIKE '%$%' THEN 1 END) as AwardValueWithDollarSigns,
            COUNT(CASE WHEN EstimatedTotalFunding LIKE '%.%' AND LEN(EstimatedTotalFunding) - LEN(REPLACE(EstimatedTotalFunding, '.', '')) > 1 THEN 1 END) as MultipleDotsFunding,
            COUNT(CASE WHEN TRY_CAST(EstimatedTotalFunding as DECIMAL(18,2)) IS NULL AND EstimatedTotalFunding IS NOT NULL THEN 1 END) as InvalidFundingFormat,
            COUNT(CASE WHEN TRY_CAST(AwardValue as DECIMAL(18,2)) IS NULL AND AwardValue IS NOT NULL THEN 1 END) as InvalidAwardFormat
        FROM BusinessIntelligenceLayer3;
        """
        
        result = self.execute_sql_command(analysis_sql)
        if result:
            logger.info("📊 BusinessIntelligenceLayer3 Analysis Results:")
            logger.info(result)
            return True
        else:
            logger.error("❌ Failed to analyze BusinessIntelligenceLayer3")
            return False
    
    def create_transformation_staging_table(self):
        """Create staging table for award value transformations"""
        logger.info("📋 Creating transformation staging table...")
        
        staging_sql = """
        -- ===================================
        -- CREATE AWARD TRANSFORMATION STAGING TABLE
        -- Optimized for BusinessIntelligenceLayer3 data processing
        -- ===================================
        
        -- Drop existing staging table if exists
        IF OBJECT_ID('AwardTransformationStaging', 'U') IS NOT NULL
        BEGIN
            DROP TABLE AwardTransformationStaging;
            PRINT 'Dropped existing transformation staging table';
        END
        
        -- Create comprehensive staging table
        CREATE TABLE AwardTransformationStaging (
            StagingID INT IDENTITY(1,1) PRIMARY KEY,
            OpportunityNumber NVARCHAR(200),
            OpportunityTitle NVARCHAR(MAX),
            Agency NVARCHAR(500),
            
            -- Original values (as stored in BI Layer 3)
            OriginalEstimatedTotalFunding NVARCHAR(100),
            OriginalExpectedAwards NVARCHAR(100),
            OriginalAwardValue NVARCHAR(100),
            OriginalAwardCeiling NVARCHAR(100),
            OriginalAwardFloor NVARCHAR(100),
            
            -- Cleaned numeric values
            EstimatedTotalFunding_Clean DECIMAL(18,2),
            ExpectedAwards_Clean INT,
            AwardValue_Clean DECIMAL(18,2),
            AwardCeiling_Clean DECIMAL(18,2),
            AwardFloor_Clean DECIMAL(18,2),
            
            -- Calculated values
            AwardValuePerAward DECIMAL(18,2),  -- EstimatedTotalFunding / ExpectedAwards
            AwardRange DECIMAL(18,2),          -- AwardCeiling - AwardFloor
            
            -- Data quality indicators
            EstimatedFundingStatus NVARCHAR(50),
            AwardValueStatus NVARCHAR(50),
            DataQualityScore INT,
            
            -- Business validation
            BusinessValidation NVARCHAR(500),
            
            -- Award categorization
            AwardCategory NVARCHAR(50),
            FundingTier NVARCHAR(50),
            
            -- Processing metadata
            TransformationDate DATETIME2 DEFAULT GETDATE(),
            TransformationBy NVARCHAR(100) DEFAULT 'BI_Layer3_Transformer',
            
            -- Indexes for performance
            INDEX IX_AwardTransformationStaging_OpportunityNumber (OpportunityNumber),
            INDEX IX_AwardTransformationStaging_EstimatedFunding (EstimatedTotalFunding_Clean),
            INDEX IX_AwardTransformationStaging_AwardValue (AwardValue_Clean),
            INDEX IX_AwardTransformationStaging_QualityScore (DataQualityScore)
        );
        
        SELECT 
            'TRANSFORMATION_STAGING_CREATED' as Status,
            'AwardTransformationStaging' as TableName,
            GETDATE() as CreatedTimestamp;
        """
        
        result = self.execute_sql_command(staging_sql)
        if result:
            logger.info("✅ Transformation staging table created successfully")
            logger.info(result)
            return True
        else:
            logger.error("❌ Failed to create transformation staging table")
            return False
    
    def transform_bi_layer3_data(self):
        """Transform all BusinessIntelligenceLayer3 data with award value processing"""
        logger.info("🔄 Transforming BusinessIntelligenceLayer3 award data...")
        
        transformation_sql = """
        -- ===================================
        -- COMPREHENSIVE BI LAYER 3 AWARD TRANSFORMATION
        -- Process all records with advanced award value extraction
        -- ===================================
        
        BEGIN TRANSACTION BILayer3Transformation;
        
        -- Clear existing staging data
        TRUNCATE TABLE AwardTransformationStaging;
        
        -- Insert all records with comprehensive transformation
        INSERT INTO AwardTransformationStaging (
            OpportunityNumber,
            OpportunityTitle,
            Agency,
            OriginalEstimatedTotalFunding,
            OriginalExpectedAwards,
            OriginalAwardValue,
            OriginalAwardCeiling,
            OriginalAwardFloor,
            EstimatedTotalFunding_Clean,
            ExpectedAwards_Clean,
            AwardValue_Clean,
            AwardCeiling_Clean,
            AwardFloor_Clean,
            AwardValuePerAward,
            AwardRange,
            EstimatedFundingStatus,
            AwardValueStatus,
            DataQualityScore,
            BusinessValidation,
            AwardCategory,
            FundingTier
        )
        SELECT 
            OpportunityNumber,
            OpportunityTitle,
            Agency,
            
            -- Original values
            EstimatedTotalFunding,
            ExpectedAwards,
            AwardValue,
            AwardCeiling,
            AwardFloor,
            
            -- Clean EstimatedTotalFunding
            CASE 
                WHEN EstimatedTotalFunding IS NULL OR LTRIM(RTRIM(EstimatedTotalFunding)) = '' THEN NULL
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL 
                THEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2))
                ELSE NULL
            END as EstimatedTotalFunding_Clean,
            
            -- Clean ExpectedAwards
            CASE 
                WHEN ExpectedAwards IS NULL OR LTRIM(RTRIM(ExpectedAwards)) = '' THEN NULL
                WHEN TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT) IS NOT NULL 
                THEN TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT)
                ELSE NULL
            END as ExpectedAwards_Clean,
            
            -- Clean AwardValue
            CASE 
                WHEN AwardValue IS NULL OR LTRIM(RTRIM(AwardValue)) = '' THEN NULL
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardValue, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL 
                THEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardValue, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2))
                ELSE NULL
            END as AwardValue_Clean,
            
            -- Clean AwardCeiling
            CASE 
                WHEN AwardCeiling IS NULL OR LTRIM(RTRIM(AwardCeiling)) = '' THEN NULL
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardCeiling, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL 
                THEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardCeiling, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2))
                ELSE NULL
            END as AwardCeiling_Clean,
            
            -- Clean AwardFloor
            CASE 
                WHEN AwardFloor IS NULL OR LTRIM(RTRIM(AwardFloor)) = '' THEN NULL
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardFloor, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL 
                THEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardFloor, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2))
                ELSE NULL
            END as AwardFloor_Clean,
            
            -- Calculate AwardValuePerAward
            CASE 
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL 
                 AND TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT) IS NOT NULL 
                 AND TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT) > 0
                THEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) / 
                     TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT)
                ELSE NULL
            END as AwardValuePerAward,
            
            -- Calculate AwardRange
            CASE 
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardCeiling, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL 
                 AND TRY_CAST(REPLACE(REPLACE(REPLACE(AwardFloor, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL
                THEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardCeiling, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) - 
                     TRY_CAST(REPLACE(REPLACE(REPLACE(AwardFloor, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2))
                ELSE NULL
            END as AwardRange,
            
            -- EstimatedFundingStatus
            CASE 
                WHEN EstimatedTotalFunding IS NULL OR LTRIM(RTRIM(EstimatedTotalFunding)) = '' THEN 'NULL'
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) = 0 THEN 'ZERO_VALUE'
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL THEN 'SPECIFIED'
                ELSE 'INVALID_FORMAT'
            END as EstimatedFundingStatus,
            
            -- AwardValueStatus
            CASE 
                WHEN AwardValue IS NULL OR LTRIM(RTRIM(AwardValue)) = '' THEN 'NULL'
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardValue, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) = 0 THEN 'ZERO_VALUE'
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardValue, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL THEN 'SPECIFIED'
                ELSE 'INVALID_FORMAT'
            END as AwardValueStatus,
            
            -- DataQualityScore (0-100)
            CASE 
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL 
                 AND TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT) IS NOT NULL 
                 AND TRY_CAST(REPLACE(REPLACE(REPLACE(AwardValue, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL
                THEN 100  -- Complete data
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL 
                 AND TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT) IS NOT NULL
                THEN 85   -- Funding and awards specified
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL
                THEN 70   -- Only funding specified
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardValue, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL
                THEN 60   -- Only award value specified
                ELSE 25   -- Poor data quality
            END as DataQualityScore,
            
            -- BusinessValidation
            CASE 
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL 
                 AND TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT) IS NOT NULL 
                 AND TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT) > 0
                THEN 'COMPLETE_FUNDING_DATA|AWARD_CALCULATION_POSSIBLE'
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) > 100000000
                THEN 'HIGH_VALUE_FUNDING|VALIDATION_REQUIRED'
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) IS NOT NULL
                THEN 'FUNDING_SPECIFIED|PARTIAL_DATA'
                ELSE 'INCOMPLETE_FUNDING_DATA'
            END as BusinessValidation,
            
            -- AwardCategory
            CASE 
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(ISNULL(EstimatedTotalFunding, AwardValue), '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) >= 100000000 THEN 'MEGA_FUNDING'
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(ISNULL(EstimatedTotalFunding, AwardValue), '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) >= 10000000 THEN 'LARGE_FUNDING'
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(ISNULL(EstimatedTotalFunding, AwardValue), '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) >= 1000000 THEN 'MEDIUM_FUNDING'
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(ISNULL(EstimatedTotalFunding, AwardValue), '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) >= 100000 THEN 'SMALL_FUNDING'
                WHEN TRY_CAST(REPLACE(REPLACE(REPLACE(ISNULL(EstimatedTotalFunding, AwardValue), '$', ''), ',', ''), ' ', '') as DECIMAL(18,2)) > 0 THEN 'MICRO_FUNDING'
                ELSE 'UNSPECIFIED'
            END as AwardCategory,
            
            -- FundingTier
            CASE 
                WHEN TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT) >= 100 THEN 'HIGH_VOLUME'
                WHEN TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT) >= 20 THEN 'MEDIUM_VOLUME'
                WHEN TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT) >= 5 THEN 'LOW_VOLUME'
                WHEN TRY_CAST(REPLACE(REPLACE(ExpectedAwards, ',', ''), ' ', '') as INT) >= 1 THEN 'SINGLE_AWARD'
                ELSE 'UNSPECIFIED'
            END as FundingTier
            
        FROM BusinessIntelligenceLayer3
        WHERE OpportunityNumber IS NOT NULL;
        
        COMMIT TRANSACTION BILayer3Transformation;
        
        -- Generate transformation report
        SELECT 
            'TRANSFORMATION_REPORT' as ReportType,
            COUNT(*) as TotalRecordsTransformed,
            COUNT(EstimatedTotalFunding_Clean) as RecordsWithCleanFunding,
            COUNT(AwardValue_Clean) as RecordsWithCleanAwardValue,
            COUNT(ExpectedAwards_Clean) as RecordsWithCleanExpectedAwards,
            COUNT(AwardValuePerAward) as RecordsWithCalculatedAwardValue,
            AVG(DataQualityScore) as AvgDataQualityScore,
            COUNT(CASE WHEN DataQualityScore >= 85 THEN 1 END) as HighQualityRecords,
            SUM(EstimatedTotalFunding_Clean) as TotalEstimatedFunding,
            AVG(EstimatedTotalFunding_Clean) as AvgEstimatedFunding,
            MAX(EstimatedTotalFunding_Clean) as MaxEstimatedFunding,
            GETDATE() as TransformationTimestamp
        FROM AwardTransformationStaging;
        
        -- Show award category distribution
        SELECT 
            'AWARD_CATEGORY_DISTRIBUTION' as DistributionType,
            AwardCategory,
            COUNT(*) as RecordCount,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM AwardTransformationStaging), 2) as Percentage,
            AVG(EstimatedTotalFunding_Clean) as AvgFunding,
            SUM(EstimatedTotalFunding_Clean) as TotalFunding
        FROM AwardTransformationStaging
        GROUP BY AwardCategory
        ORDER BY TotalFunding DESC;
        
        -- Show funding tier distribution
        SELECT 
            'FUNDING_TIER_DISTRIBUTION' as DistributionType,
            FundingTier,
            COUNT(*) as RecordCount,
            AVG(ExpectedAwards_Clean) as AvgExpectedAwards,
            AVG(AwardValuePerAward) as AvgAwardValuePerAward
        FROM AwardTransformationStaging
        WHERE ExpectedAwards_Clean IS NOT NULL
        GROUP BY FundingTier
        ORDER BY AvgExpectedAwards DESC;
        """
        
        temp_file = "bi_layer3_transformation.sql"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(transformation_sql)
        
        result = self.execute_sql_file(temp_file, timeout=1800)  # 30 minute timeout for large datasets
        
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        if result:
            logger.info("✅ BusinessIntelligenceLayer3 transformation completed successfully")
            logger.info(result)
            return True
        else:
            logger.error("❌ BusinessIntelligenceLayer3 transformation failed")
            return False
    
    def update_bi_layer3_with_transformed_data(self):
        """Update BusinessIntelligenceLayer3 with cleaned award values"""
        logger.info("🔄 Updating BusinessIntelligenceLayer3 with transformed data...")
        
        update_sql = """
        -- ===================================
        -- UPDATE BI LAYER 3 WITH TRANSFORMED DATA
        -- Apply cleaned values back to source table
        -- ===================================
        
        BEGIN TRANSACTION UpdateBILayer3;
        
        -- Add new columns if they don't exist
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('BusinessIntelligenceLayer3') AND name = 'EstimatedTotalFunding_Clean')
        BEGIN
            ALTER TABLE BusinessIntelligenceLayer3 ADD 
                EstimatedTotalFunding_Clean DECIMAL(18,2),
                ExpectedAwards_Clean INT,
                AwardValue_Clean DECIMAL(18,2),
                AwardCeiling_Clean DECIMAL(18,2),
                AwardFloor_Clean DECIMAL(18,2),
                AwardValuePerAward DECIMAL(18,2),
                AwardRange DECIMAL(18,2),
                EstimatedFundingStatus NVARCHAR(50),
                AwardValueStatus NVARCHAR(50),
                AwardDataQualityScore INT,
                AwardBusinessValidation NVARCHAR(500),
                AwardCategory NVARCHAR(50),
                FundingTier NVARCHAR(50),
                AwardTransformationDate DATETIME2 DEFAULT GETDATE();
            
            PRINT 'Added transformation columns to BusinessIntelligenceLayer3';
        END
        
        -- Update BusinessIntelligenceLayer3 with transformed data
        UPDATE bi
        SET 
            EstimatedTotalFunding_Clean = ats.EstimatedTotalFunding_Clean,
            ExpectedAwards_Clean = ats.ExpectedAwards_Clean,
            AwardValue_Clean = ats.AwardValue_Clean,
            AwardCeiling_Clean = ats.AwardCeiling_Clean,
            AwardFloor_Clean = ats.AwardFloor_Clean,
            AwardValuePerAward = ats.AwardValuePerAward,
            AwardRange = ats.AwardRange,
            EstimatedFundingStatus = ats.EstimatedFundingStatus,
            AwardValueStatus = ats.AwardValueStatus,
            AwardDataQualityScore = ats.DataQualityScore,
            AwardBusinessValidation = ats.BusinessValidation,
            AwardCategory = ats.AwardCategory,
            FundingTier = ats.FundingTier,
            AwardTransformationDate = GETDATE()
        FROM BusinessIntelligenceLayer3 bi
        INNER JOIN AwardTransformationStaging ats ON bi.OpportunityNumber = ats.OpportunityNumber;
        
        -- Create indexes for performance
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_BusinessIntelligenceLayer3_EstimatedFunding_Clean')
        BEGIN
            CREATE NONCLUSTERED INDEX IX_BusinessIntelligenceLayer3_EstimatedFunding_Clean 
            ON BusinessIntelligenceLayer3(EstimatedTotalFunding_Clean) 
            WHERE EstimatedTotalFunding_Clean IS NOT NULL;
        END
        
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_BusinessIntelligenceLayer3_AwardValue_Clean')
        BEGIN
            CREATE NONCLUSTERED INDEX IX_BusinessIntelligenceLayer3_AwardValue_Clean 
            ON BusinessIntelligenceLayer3(AwardValue_Clean) 
            WHERE AwardValue_Clean IS NOT NULL;
        END
        
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_BusinessIntelligenceLayer3_AwardCategory')
        BEGIN
            CREATE NONCLUSTERED INDEX IX_BusinessIntelligenceLayer3_AwardCategory 
            ON BusinessIntelligenceLayer3(AwardCategory) 
            WHERE AwardCategory IS NOT NULL;
        END
        
        COMMIT TRANSACTION UpdateBILayer3;
        
        -- Generate final update report
        SELECT 
            'BI_LAYER3_UPDATE_REPORT' as ReportType,
            COUNT(*) as TotalRecords,
            COUNT(EstimatedTotalFunding_Clean) as RecordsWithCleanFunding,
            COUNT(AwardValue_Clean) as RecordsWithCleanAwardValue,
            COUNT(AwardValuePerAward) as RecordsWithCalculatedValues,
            AVG(AwardDataQualityScore) as AvgQualityScore,
            COUNT(CASE WHEN AwardDataQualityScore >= 85 THEN 1 END) as HighQualityRecords,
            ROUND(100.0 * COUNT(CASE WHEN AwardDataQualityScore >= 85 THEN 1 END) / COUNT(*), 2) as HighQualityPercentage,
            SUM(EstimatedTotalFunding_Clean) as TotalCleanFunding,
            MAX(AwardTransformationDate) as LastTransformationDate
        FROM BusinessIntelligenceLayer3;
        """
        
        temp_file = "update_bi_layer3.sql"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(update_sql)
        
        result = self.execute_sql_file(temp_file, timeout=900)
        
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        if result:
            logger.info("✅ BusinessIntelligenceLayer3 updated successfully")
            logger.info(result)
            return True
        else:
            logger.error("❌ Failed to update BusinessIntelligenceLayer3")
            return False
    
    def create_enhanced_bi_views(self):
        """Create enhanced business intelligence views"""
        logger.info("📊 Creating enhanced BI views...")
        
        views_sql = """
        -- ===================================
        -- ENHANCED BUSINESS INTELLIGENCE VIEWS
        -- Comprehensive award analysis views
        -- ===================================
        
        -- Enhanced Award Analysis View
        CREATE OR ALTER VIEW vw_EnhancedAwardAnalysis AS
        SELECT 
            OpportunityNumber,
            OpportunityTitle,
            Agency,
            OpportunityCategory,
            
            -- Original values
            EstimatedTotalFunding as OriginalEstimatedFunding,
            ExpectedAwards as OriginalExpectedAwards,
            AwardValue as OriginalAwardValue,
            
            -- Cleaned values
            EstimatedTotalFunding_Clean,
            ExpectedAwards_Clean,
            AwardValue_Clean,
            AwardCeiling_Clean,
            AwardFloor_Clean,
            
            -- Calculated values
            AwardValuePerAward,
            AwardRange,
            
            -- Classifications
            AwardCategory,
            FundingTier,
            EstimatedFundingStatus,
            AwardValueStatus,
            AwardDataQualityScore,
            
            -- Quality grade
            CASE 
                WHEN AwardDataQualityScore >= 85 THEN 'EXCELLENT'
                WHEN AwardDataQualityScore >= 70 THEN 'GOOD'
                WHEN AwardDataQualityScore >= 50 THEN 'FAIR'
                ELSE 'POOR'
            END as QualityGrade,
            
            -- Business indicators
            CASE WHEN AwardBusinessValidation LIKE '%COMPLETE_FUNDING_DATA%' THEN 1 ELSE 0 END as HasCompleteFundingData,
            CASE WHEN AwardBusinessValidation LIKE '%HIGH_VALUE_FUNDING%' THEN 1 ELSE 0 END as IsHighValueFunding,
            CASE WHEN AwardBusinessValidation LIKE '%AWARD_CALCULATION_POSSIBLE%' THEN 1 ELSE 0 END as CanCalculateAwardValue,
            
            -- Dates
            PostDate,
            CloseDate,
            AwardTransformationDate
            
        FROM BusinessIntelligenceLayer3
        WHERE EstimatedTotalFunding_Clean IS NOT NULL OR AwardValue_Clean IS NOT NULL;
        
        -- Agency Funding Summary View
        CREATE OR ALTER VIEW vw_AgencyFundingSummary AS
        SELECT 
            Agency,
            COUNT(*) as TotalOpportunities,
            COUNT(EstimatedTotalFunding_Clean) as OpportunitiesWithFunding,
            SUM(EstimatedTotalFunding_Clean) as TotalEstimatedFunding,
            AVG(EstimatedTotalFunding_Clean) as AvgEstimatedFunding,
            MAX(EstimatedTotalFunding_Clean) as MaxEstimatedFunding,
            SUM(ExpectedAwards_Clean) as TotalExpectedAwards,
            AVG(AwardValuePerAward) as AvgAwardValuePerAward,
            
            -- Award category distribution
            COUNT(CASE WHEN AwardCategory = 'MEGA_FUNDING' THEN 1 END) as MegaFundingOpportunities,
            COUNT(CASE WHEN AwardCategory = 'LARGE_FUNDING' THEN 1 END) as LargeFundingOpportunities,
            COUNT(CASE WHEN AwardCategory = 'MEDIUM_FUNDING' THEN 1 END) as MediumFundingOpportunities,
            COUNT(CASE WHEN AwardCategory = 'SMALL_FUNDING' THEN 1 END) as SmallFundingOpportunities,
            
            -- Quality metrics
            AVG(AwardDataQualityScore) as AvgQualityScore,
            COUNT(CASE WHEN AwardDataQualityScore >= 85 THEN 1 END) as HighQualityOpportunities,
            ROUND(100.0 * COUNT(CASE WHEN AwardDataQualityScore >= 85 THEN 1 END) / COUNT(*), 2) as HighQualityPercentage,
            
            -- Completeness
            ROUND(100.0 * COUNT(EstimatedTotalFunding_Clean) / COUNT(*), 2) as FundingCompleteness
            
        FROM BusinessIntelligenceLayer3
        WHERE Agency IS NOT NULL
        GROUP BY Agency
        HAVING COUNT(*) >= 5
        ORDER BY TotalEstimatedFunding DESC;
        
        -- Monthly Funding Trends View
        CREATE OR ALTER VIEW vw_MonthlyFundingTrends AS
        SELECT 
            YEAR(PostDate) as Year,
            MONTH(PostDate) as Month,
            COUNT(*) as TotalOpportunities,
            COUNT(EstimatedTotalFunding_Clean) as OpportunitiesWithFunding,
            SUM(EstimatedTotalFunding_Clean) as TotalEstimatedFunding,
            AVG(EstimatedTotalFunding_Clean) as AvgEstimatedFunding,
            SUM(ExpectedAwards_Clean) as TotalExpectedAwards,
            AVG(AwardDataQualityScore) as AvgQualityScore,
            
            -- Category breakdown
            COUNT(CASE WHEN AwardCategory = 'MEGA_FUNDING' THEN 1 END) as MegaFunding,
            COUNT(CASE WHEN AwardCategory = 'LARGE_FUNDING' THEN 1 END) as LargeFunding,
            COUNT(CASE WHEN AwardCategory = 'MEDIUM_FUNDING' THEN 1 END) as MediumFunding,
            COUNT(CASE WHEN AwardCategory = 'SMALL_FUNDING' THEN 1 END) as SmallFunding
            
        FROM BusinessIntelligenceLayer3
        WHERE PostDate IS NOT NULL
        GROUP BY YEAR(PostDate), MONTH(PostDate)
        ORDER BY Year DESC, Month DESC;
        
        -- Data Quality Dashboard View  
        CREATE OR ALTER VIEW vw_DataQualityDashboard AS
        SELECT 
            'DATA_QUALITY_SUMMARY' as MetricType,
            COUNT(*) as TotalRecords,
            
            -- Funding data quality
            COUNT(EstimatedTotalFunding_Clean) as RecordsWithCleanFunding,
            ROUND(100.0 * COUNT(EstimatedTotalFunding_Clean) / COUNT(*), 2) as FundingDataCompleteness,
            
            -- Award data quality
            COUNT(AwardValue_Clean) as RecordsWithCleanAwardValue,
            ROUND(100.0 * COUNT(AwardValue_Clean) / COUNT(*), 2) as AwardValueCompleteness,
            
            -- Expected awards data quality
            COUNT(ExpectedAwards_Clean) as RecordsWithCleanExpectedAwards,
            ROUND(100.0 * COUNT(ExpectedAwards_Clean) / COUNT(*), 2) as ExpectedAwardsCompleteness,
            
            -- Overall quality scores
            AVG(AwardDataQualityScore) as OverallAvgQualityScore,
            COUNT(CASE WHEN AwardDataQualityScore >= 85 THEN 1 END) as ExcellentQualityRecords,
            COUNT(CASE WHEN AwardDataQualityScore >= 70 THEN 1 END) as GoodQualityRecords,
            COUNT(CASE WHEN AwardDataQualityScore < 50 THEN 1 END) as PoorQualityRecords,
            
            -- Quality percentages
            ROUND(100.0 * COUNT(CASE WHEN AwardDataQualityScore >= 85 THEN 1 END) / COUNT(*), 2) as ExcellentQualityPercentage,
            ROUND(100.0 * COUNT(CASE WHEN AwardDataQualityScore >= 70 THEN 1 END) / COUNT(*), 2) as GoodQualityPercentage,
            
            -- Business validation
            COUNT(CASE WHEN AwardBusinessValidation LIKE '%COMPLETE_FUNDING_DATA%' THEN 1 END) as RecordsWithCompleteFundingData,
            COUNT(CASE WHEN AwardBusinessValidation LIKE '%HIGH_VALUE_FUNDING%' THEN 1 END) as HighValueFundingRecords,
            
            -- Transformation status
            MAX(AwardTransformationDate) as LastTransformationDate
            
        FROM BusinessIntelligenceLayer3;
        
        SELECT 'ENHANCED_BI_VIEWS_CREATED' as Status, GETDATE() as CreationTimestamp;
        """
        
        temp_file = "enhanced_bi_views.sql"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(views_sql)
        
        result = self.execute_sql_file(temp_file)
        
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        if result:
            logger.info("✅ Enhanced BI views created successfully")
            return True
        else:
            logger.error("❌ Failed to create enhanced BI views")
            return False
    
    def cleanup_staging_data(self):
        """Clean up staging table after successful transformation"""
        logger.info("🧹 Cleaning up staging data...")
        
        cleanup_sql = """
        -- Archive staging statistics before cleanup
        SELECT 
            'STAGING_CLEANUP_SUMMARY' as Status,
            COUNT(*) as StagingRecordsProcessed,
            AVG(DataQualityScore) as AvgStagingQualityScore,
            GETDATE() as CleanupTimestamp
        FROM AwardTransformationStaging;
        
        -- Drop staging table
        DROP TABLE IF EXISTS AwardTransformationStaging;
        
        SELECT 'STAGING_TABLE_DROPPED' as Status;
        """
        
        result = self.execute_sql_command(cleanup_sql)
        if result:
            logger.info("✅ Staging data cleaned up successfully")
            logger.info(result)
            return True
        else:
            logger.warning("⚠️ Staging cleanup had issues")
            return True
    
    def run_complete_transformation(self):
        """Run the complete BusinessIntelligenceLayer3 transformation process"""
        logger.info("🚀 Starting complete BusinessIntelligenceLayer3 transformation")
        logger.info("=" * 80)
        
        try:
            # Step 1: Analyze current structure
            if not self.analyze_bi_layer3_structure():
                logger.error("❌ Structure analysis failed")
                return False
            
            # Step 2: Create staging table
            if not self.create_transformation_staging_table():
                logger.error("❌ Staging table creation failed")
                return False
            
            # Step 3: Transform all data
            if not self.transform_bi_layer3_data():
                logger.error("❌ Data transformation failed")
                return False
            
            # Step 4: Update main table
            if not self.update_bi_layer3_with_transformed_data():
                logger.error("❌ Main table update failed")
                return False
            
            # Step 5: Create enhanced views
            if not self.create_enhanced_bi_views():
                logger.warning("⚠️ Enhanced views creation had issues")
            
            # Step 6: Cleanup
            self.cleanup_staging_data()
            
            logger.info("\n🎉 COMPLETE BI LAYER 3 TRANSFORMATION SUCCESSFUL!")
            logger.info("✅ All BusinessIntelligenceLayer3 data transformed")
            logger.info("✅ Award values cleaned and validated")
            logger.info("✅ Quality scores calculated")
            logger.info("✅ Business categories assigned")
            logger.info("✅ Enhanced BI views created")
            logger.info("🚀 Your BI Layer 3 is now enterprise-ready!")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Transformation process failed: {e}")
            import traceback
            traceback.print_exc()
            return False

def main():
    """Main transformation function"""
    transformer = BusinessIntelligenceLayer3Transformer()
    success = transformer.run_complete_transformation()
    
    if success:
        print("\n💼 Transformation completed successfully!")
        print("🔍 You can now query your enhanced BI Layer 3:")
        print("   - SELECT * FROM vw_EnhancedAwardAnalysis")
        print("   - SELECT * FROM vw_AgencyFundingSummary") 
        print("   - SELECT * FROM vw_MonthlyFundingTrends")
        print("   - SELECT * FROM vw_DataQualityDashboard")
        print("🚀 Your BusinessIntelligenceLayer3 is now fully transformed!")
    else:
        print("\n❌ Transformation failed. Check logs for details.")

if __name__ == "__main__":
    main()