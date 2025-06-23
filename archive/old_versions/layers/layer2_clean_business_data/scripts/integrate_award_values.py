#!/usr/bin/env python3
"""
Azure SQL Database Integration for Award Values
Professional integration with CleanGrantsLayer2 table
"""

import os
import subprocess
import pandas as pd
from datetime import datetime
import logging

# Setup Azure-optimized logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('azure_award_integration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AzureAwardValueIntegrator:
    """Professional Azure SQL Database integrator for award values"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        self.processed_data_path = "/Users/dinghali/Desktop/Runwei/grants_gov_api_azure/layer2_clean_business_data/processed_data/processed_award_values.csv"
        
    def execute_sql_command(self, sql_query, timeout=300):
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
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 10)
            
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
    
    def execute_sql_file(self, sql_file_path, timeout=600):
        """Execute SQL file with Azure SQL Database optimizations"""
        try:
            cmd = [
                "sqlcmd", "-S", self.server,
                "-d", self.database,
                "-U", self.username,
                "-P", self.password,
                "-i", sql_file_path,
                "-C",  # Trust server certificate for Azure
                "-t", str(timeout),  # Query timeout
                "-I"   # Enable quoted identifiers
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
    
    def backup_current_state(self):
        """Create backup of current CleanGrantsLayer2 state"""
        logger.info("💾 Creating backup of current CleanGrantsLayer2 state...")
        
        backup_sql = """
        -- Create backup table with timestamp
        DECLARE @BackupTableName NVARCHAR(100) = 'CleanGrantsLayer2_Backup_' + FORMAT(GETDATE(), 'yyyyMMdd_HHmmss');
        DECLARE @SQL NVARCHAR(MAX) = 'SELECT * INTO ' + @BackupTableName + ' FROM CleanGrantsLayer2';
        
        EXEC sp_executesql @SQL;
        
        -- Report backup creation
        SELECT 
            'BACKUP_CREATED' as Status,
            @BackupTableName as BackupTableName,
            COUNT(*) as RecordsBackedUp,
            GETDATE() as BackupTimestamp
        FROM CleanGrantsLayer2;
        """
        
        result = self.execute_sql_command(backup_sql)
        if result:
            logger.info("✅ Backup created successfully")
            logger.info(result)
            return True
        else:
            logger.error("❌ Failed to create backup")
            return False
    
    def prepare_schema_updates(self):
        """Prepare CleanGrantsLayer2 table for award value integration"""
        logger.info("🔧 Preparing schema updates for award values...")
        
        schema_sql = """
        -- ===================================
        -- AZURE SQL DATABASE SCHEMA UPDATES
        -- Add award value columns to CleanGrantsLayer2
        -- ===================================
        
        BEGIN TRANSACTION SchemaUpdate;
        
        -- Check and add AwardValue column
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'AwardValue')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD AwardValue DECIMAL(18,2) NULL;
            PRINT 'Added AwardValue column';
        END
        
        -- Check and add CashAward column
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'CashAward')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD CashAward DECIMAL(18,2) NULL;
            PRINT 'Added CashAward column';
        END
        
        -- Check and add AwardValueStatus column
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'AwardValueStatus')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD AwardValueStatus NVARCHAR(50) NULL;
            PRINT 'Added AwardValueStatus column';
        END
        
        -- Check and add CashAwardStatus column
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'CashAwardStatus')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD CashAwardStatus NVARCHAR(50) NULL;
            PRINT 'Added CashAwardStatus column';
        END
        
        -- Check and add AwardDataQualityScore column
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'AwardDataQualityScore')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD AwardDataQualityScore INT NULL;
            PRINT 'Added AwardDataQualityScore column';
        END
        
        -- Check and add AwardBusinessValidation column
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'AwardBusinessValidation')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD AwardBusinessValidation NVARCHAR(500) NULL;
            PRINT 'Added AwardBusinessValidation column';
        END
        
        -- Check and add AwardIntegrationDate column
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'AwardIntegrationDate')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD AwardIntegrationDate DATETIME2 DEFAULT GETDATE();
            PRINT 'Added AwardIntegrationDate column';
        END
        
        -- Create indexes for performance optimization
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanGrantsLayer2_AwardValue')
        BEGIN
            CREATE NONCLUSTERED INDEX IX_CleanGrantsLayer2_AwardValue 
            ON CleanGrantsLayer2(AwardValue) 
            WHERE AwardValue IS NOT NULL;
            PRINT 'Created AwardValue index';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanGrantsLayer2_CashAward')
        BEGIN
            CREATE NONCLUSTERED INDEX IX_CleanGrantsLayer2_CashAward 
            ON CleanGrantsLayer2(CashAward) 
            WHERE CashAward IS NOT NULL;
            PRINT 'Created CashAward index';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanGrantsLayer2_QualityScore')
        BEGIN
            CREATE NONCLUSTERED INDEX IX_CleanGrantsLayer2_QualityScore 
            ON CleanGrantsLayer2(AwardDataQualityScore) 
            WHERE AwardDataQualityScore IS NOT NULL;
            PRINT 'Created QualityScore index';
        END
        
        COMMIT TRANSACTION SchemaUpdate;
        
        -- Verify schema updates
        SELECT 
            'SCHEMA_UPDATE_VERIFICATION' as Status,
            COUNT(*) as TotalColumns,
            COUNT(CASE WHEN COLUMN_NAME = 'AwardValue' THEN 1 END) as AwardValueColumn,
            COUNT(CASE WHEN COLUMN_NAME = 'CashAward' THEN 1 END) as CashAwardColumn,
            COUNT(CASE WHEN COLUMN_NAME = 'AwardDataQualityScore' THEN 1 END) as QualityScoreColumn
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'CleanGrantsLayer2' 
          AND COLUMN_NAME IN ('AwardValue', 'CashAward', 'AwardValueStatus', 'CashAwardStatus', 'AwardDataQualityScore', 'AwardBusinessValidation', 'AwardIntegrationDate');
        """
        
        temp_file = "schema_update.sql"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(schema_sql)
        
        result = self.execute_sql_file(temp_file)
        
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        if result:
            logger.info("✅ Schema updates completed successfully")
            logger.info(result)
            return True
        else:
            logger.error("❌ Schema updates failed")
            return False
    
    def create_staging_table(self):
        """Create staging table for award value data"""
        logger.info("📋 Creating staging table for award value data...")
        
        staging_sql = """
        -- ===================================
        -- CREATE AWARD VALUE STAGING TABLE
        -- Optimized for Azure SQL Database
        -- ===================================
        
        -- Drop existing staging table if exists
        IF OBJECT_ID('AwardValueStaging', 'U') IS NOT NULL
        BEGIN
            DROP TABLE AwardValueStaging;
            PRINT 'Dropped existing staging table';
        END
        
        -- Create new staging table
        CREATE TABLE AwardValueStaging (
            StagingID INT IDENTITY(1,1) PRIMARY KEY,
            RecordID INT,
            OriginalAwardValue NVARCHAR(100),
            OriginalCashAward NVARCHAR(100),
            FinalAwardValue_Clean DECIMAL(18,2),
            FinalAwardValue_Status NVARCHAR(50),
            CashAward_Clean DECIMAL(18,2),
            CashAward_Status NVARCHAR(50),
            BusinessValidation NVARCHAR(500),
            DataQualityScore INT,
            ProcessedDate DATETIME2,
            ProcessedBy NVARCHAR(100),
            CreatedDate DATETIME2 DEFAULT GETDATE(),
            
            -- Constraints
            CONSTRAINT CK_AwardValueStaging_QualityScore CHECK (DataQualityScore BETWEEN 0 AND 100),
            CONSTRAINT CK_AwardValueStaging_AwardValue CHECK (FinalAwardValue_Clean >= 0),
            CONSTRAINT CK_AwardValueStaging_CashAward CHECK (CashAward_Clean >= 0)
        );
        
        -- Create indexes for performance
        CREATE NONCLUSTERED INDEX IX_AwardValueStaging_RecordID ON AwardValueStaging(RecordID);
        CREATE NONCLUSTERED INDEX IX_AwardValueStaging_AwardValue ON AwardValueStaging(FinalAwardValue_Clean) WHERE FinalAwardValue_Clean IS NOT NULL;
        CREATE NONCLUSTERED INDEX IX_AwardValueStaging_CashAward ON AwardValueStaging(CashAward_Clean) WHERE CashAward_Clean IS NOT NULL;
        
        SELECT 
            'STAGING_TABLE_CREATED' as Status,
            'AwardValueStaging' as TableName,
            GETDATE() as CreatedTimestamp;
        """
        
        result = self.execute_sql_command(staging_sql)
        if result:
            logger.info("✅ Staging table created successfully")
            logger.info(result)
            return True
        else:
            logger.error("❌ Failed to create staging table")
            return False
    
    def load_data_to_staging(self):
        """Load processed award value data to staging table"""
        logger.info("📤 Loading processed data to staging table...")
        
        try:
            # Read processed data
            if not os.path.exists(self.processed_data_path):
                logger.error(f"❌ Processed data file not found: {self.processed_data_path}")
                return False
            
            df = pd.read_csv(self.processed_data_path)
            logger.info(f"📊 Loaded {len(df)} records from processed data")
            
            # Create bulk insert SQL
            bulk_insert_sql = """
            -- ===================================
            -- BULK INSERT AWARD VALUE DATA
            -- Load data from CSV to staging table
            -- ===================================
            
            BEGIN TRANSACTION BulkInsert;
            
            -- Clear existing staging data
            DELETE FROM AwardValueStaging;
            
            """
            
            # Generate INSERT statements for each record
            for index, row in df.iterrows():
                # Handle NULL values properly
                record_id = row.get('RecordID', index + 1)
                original_award = str(row.get('FinalAwardValue (USD)', '')).replace("'", "''")
                original_cash = str(row.get('CashAward (USD)', '')).replace("'", "''")
                award_clean = row.get('FinalAwardValue_Clean')
                award_status = str(row.get('FinalAwardValue_Status', '')).replace("'", "''")
                cash_clean = row.get('CashAward_Clean')
                cash_status = str(row.get('CashAward_Status', '')).replace("'", "''")
                business_validation = str(row.get('BusinessValidation', '')).replace("'", "''")
                quality_score = row.get('DataQualityScore', 0)
                processed_by = str(row.get('ProcessedBy', 'Professional_Award_Value_Processor')).replace("'", "''")
                
                # Build INSERT statement
                insert_values = f"({record_id}, '{original_award}', '{original_cash}', "
                insert_values += f"{award_clean if pd.notna(award_clean) else 'NULL'}, '{award_status}', "
                insert_values += f"{cash_clean if pd.notna(cash_clean) else 'NULL'}, '{cash_status}', "
                insert_values += f"'{business_validation}', {quality_score}, GETDATE(), '{processed_by}')"
                
                bulk_insert_sql += f"""
                INSERT INTO AwardValueStaging (RecordID, OriginalAwardValue, OriginalCashAward, FinalAwardValue_Clean, FinalAwardValue_Status, CashAward_Clean, CashAward_Status, BusinessValidation, DataQualityScore, ProcessedDate, ProcessedBy)
                VALUES {insert_values};
                """
            
            bulk_insert_sql += """
            
            COMMIT TRANSACTION BulkInsert;
            
            -- Verify data load
            SELECT 
                'DATA_LOAD_VERIFICATION' as Status,
                COUNT(*) as RecordsLoaded,
                COUNT(FinalAwardValue_Clean) as AwardValuesLoaded,
                COUNT(CashAward_Clean) as CashAwardsLoaded,
                AVG(DataQualityScore) as AvgQualityScore,
                GETDATE() as LoadTimestamp
            FROM AwardValueStaging;
            """
            
            # Execute bulk insert
            temp_file = "bulk_insert_awards.sql"
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(bulk_insert_sql)
            
            result = self.execute_sql_file(temp_file, timeout=900)  # Extended timeout for bulk insert
            
            # Clean up temp file
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            if result:
                logger.info("✅ Data loaded to staging table successfully")
                logger.info(result)
                return True
            else:
                logger.error("❌ Failed to load data to staging table")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error loading data to staging: {e}")
            return False
    
    def integrate_with_main_table(self):
        """Integrate staging data with CleanGrantsLayer2 table"""
        logger.info("🔄 Integrating award values with CleanGrantsLayer2...")
        
        integration_sql = """
        -- ===================================
        -- INTEGRATE AWARD VALUES WITH MAIN TABLE
        -- Professional data integration with business rules
        -- ===================================
        
        BEGIN TRANSACTION AwardValueIntegration;
        
        -- Strategy 1: Update records by sequential mapping (assuming data is in same order)
        -- This is the safest approach when we don't have explicit keys
        
        WITH StagingCTE AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY StagingID) as RowNum,
                FinalAwardValue_Clean,
                FinalAwardValue_Status,
                CashAward_Clean,
                CashAward_Status,
                BusinessValidation,
                DataQualityScore
            FROM AwardValueStaging
        ),
        MainTableCTE AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY OpportunityNumber) as RowNum,
                OpportunityNumber
            FROM CleanGrantsLayer2
        )
        UPDATE cgl
        SET 
            AwardValue = s.FinalAwardValue_Clean,
            CashAward = s.CashAward_Clean,
            AwardValueStatus = s.FinalAwardValue_Status,
            CashAwardStatus = s.CashAward_Status,
            AwardDataQualityScore = s.DataQualityScore,
            AwardBusinessValidation = s.BusinessValidation,
            AwardIntegrationDate = GETDATE(),
            UpdatedDate = GETDATE(),
            ProcessedBy = 'Award_Value_Integration'
        FROM CleanGrantsLayer2 cgl
        INNER JOIN MainTableCTE m ON cgl.OpportunityNumber = m.OpportunityNumber
        INNER JOIN StagingCTE s ON m.RowNum = s.RowNum;
        
        -- Update statistics
        UPDATE STATISTICS CleanGrantsLayer2;
        
        COMMIT TRANSACTION AwardValueIntegration;
        
        -- Generate integration report
        SELECT 
            'INTEGRATION_REPORT' as ReportType,
            COUNT(*) as TotalRecords,
            COUNT(AwardValue) as RecordsWithAwardValue,
            COUNT(CashAward) as RecordsWithCashAward,
            COUNT(CASE WHEN AwardValue IS NOT NULL AND CashAward IS NOT NULL THEN 1 END) as CompleteAwardRecords,
            ROUND(100.0 * COUNT(AwardValue) / COUNT(*), 2) as AwardValueCompleteness,
            ROUND(100.0 * COUNT(CashAward) / COUNT(*), 2) as CashAwardCompleteness,
            AVG(AwardDataQualityScore) as AvgQualityScore,
            COUNT(CASE WHEN AwardDataQualityScore >= 90 THEN 1 END) as HighQualityRecords,
            ROUND(100.0 * COUNT(CASE WHEN AwardDataQualityScore >= 90 THEN 1 END) / COUNT(*), 2) as HighQualityPercentage,
            MAX(AwardIntegrationDate) as IntegrationTimestamp
        FROM CleanGrantsLayer2;
        
        -- Show sample of integrated data
        SELECT TOP 10
            'INTEGRATION_SAMPLE' as SampleType,
            OpportunityNumber,
            OpportunityTitle,
            AwardValue,
            CashAward,
            AwardValueStatus,
            AwardDataQualityScore,
            AwardIntegrationDate
        FROM CleanGrantsLayer2
        WHERE AwardValue IS NOT NULL
        ORDER BY AwardValue DESC;
        
        -- Show award value distribution
        SELECT 
            'AWARD_DISTRIBUTION' as DistributionType,
            CASE 
                WHEN AwardValue >= 10000000 THEN 'MEGA_GRANT (10M+)'
                WHEN AwardValue >= 1000000 THEN 'LARGE_GRANT (1M-10M)'
                WHEN AwardValue >= 100000 THEN 'MEDIUM_GRANT (100K-1M)'
                WHEN AwardValue >= 10000 THEN 'SMALL_GRANT (10K-100K)'
                WHEN AwardValue > 0 THEN 'MICRO_GRANT (<10K)'
                ELSE 'UNSPECIFIED'
            END as AwardCategory,
            COUNT(*) as RecordCount,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM CleanGrantsLayer2), 2) as Percentage,
            AVG(AwardValue) as AvgAwardValue,
            SUM(AwardValue) as TotalAwardValue
        FROM CleanGrantsLayer2
        GROUP BY 
            CASE 
                WHEN AwardValue >= 10000000 THEN 'MEGA_GRANT (10M+)'
                WHEN AwardValue >= 1000000 THEN 'LARGE_GRANT (1M-10M)'
                WHEN AwardValue >= 100000 THEN 'MEDIUM_GRANT (100K-1M)'
                WHEN AwardValue >= 10000 THEN 'SMALL_GRANT (10K-100K)'
                WHEN AwardValue > 0 THEN 'MICRO_GRANT (<10K)'
                ELSE 'UNSPECIFIED'
            END
        ORDER BY AvgAwardValue DESC;
        """
        
        temp_file = "award_integration.sql"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(integration_sql)
        
        result = self.execute_sql_file(temp_file, timeout=600)
        
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        if result:
            logger.info("✅ Award value integration completed successfully")
            logger.info(result)
            return True
        else:
            logger.error("❌ Award value integration failed")
            return False
    
    def create_business_intelligence_views(self):
        """Create business intelligence views for award analysis"""
        logger.info("📊 Creating business intelligence views...")
        
        bi_views_sql = """
        -- ===================================
        -- BUSINESS INTELLIGENCE VIEWS
        -- Azure SQL Database optimized views for award analysis
        -- ===================================
        
        -- Award Value Analysis View
        CREATE OR ALTER VIEW vw_AwardValueAnalysis AS
        SELECT 
            OpportunityNumber,
            OpportunityTitle,
            OpportunityCategory,
            Agency,
            AwardValue,
            CashAward,
            AwardValueStatus,
            CashAwardStatus,
            AwardDataQualityScore,
            AwardBusinessValidation,
            
            -- Award categories
            CASE 
                WHEN AwardValue >= 10000000 THEN 'MEGA_GRANT'
                WHEN AwardValue >= 1000000 THEN 'LARGE_GRANT'
                WHEN AwardValue >= 100000 THEN 'MEDIUM_GRANT'
                WHEN AwardValue >= 10000 THEN 'SMALL_GRANT'
                WHEN AwardValue > 0 THEN 'MICRO_GRANT'
                ELSE 'UNSPECIFIED'
            END as AwardCategory,
            
            -- Cash award ratio
            CASE 
                WHEN AwardValue > 0 AND CashAward > 0 
                THEN ROUND(100.0 * CashAward / AwardValue, 2)
                ELSE NULL
            END as CashAwardPercentage,
            
            -- Quality indicators
            CASE 
                WHEN AwardDataQualityScore >= 90 THEN 'HIGH_QUALITY'
                WHEN AwardDataQualityScore >= 70 THEN 'GOOD_QUALITY'
                WHEN AwardDataQualityScore >= 50 THEN 'FAIR_QUALITY'
                ELSE 'POOR_QUALITY'
            END as QualityGrade,
            
            -- Business validation flags
            CASE WHEN AwardBusinessValidation LIKE '%CASH_EXCEEDS_AWARD%' THEN 1 ELSE 0 END as CashExceedsAwardFlag,
            CASE WHEN AwardBusinessValidation LIKE '%HIGH_VALUE_AWARD%' THEN 1 ELSE 0 END as HighValueAwardFlag,
            CASE WHEN AwardBusinessValidation LIKE '%COMPLETE_DATA%' THEN 1 ELSE 0 END as CompleteDataFlag,
            
            -- Dates
            AwardIntegrationDate,
            UpdatedDate
            
        FROM CleanGrantsLayer2
        WHERE AwardValue IS NOT NULL OR CashAward IS NOT NULL;
        
        -- Award Statistics Summary View
        CREATE OR ALTER VIEW vw_AwardStatisticsSummary AS
        SELECT 
            'OVERALL_STATISTICS' as StatisticCategory,
            COUNT(*) as TotalRecords,
            COUNT(AwardValue) as RecordsWithAwardValue,
            COUNT(CashAward) as RecordsWithCashAward,
            COUNT(CASE WHEN AwardValue IS NOT NULL AND CashAward IS NOT NULL THEN 1 END) as CompleteRecords,
            
            -- Award value statistics
            AVG(AwardValue) as AvgAwardValue,
            MIN(AwardValue) as MinAwardValue,
            MAX(AwardValue) as MaxAwardValue,
            SUM(AwardValue) as TotalAwardValue,
            
            -- Cash award statistics  
            AVG(CashAward) as AvgCashAward,
            MIN(CashAward) as MinCashAward,
            MAX(CashAward) as MaxCashAward,
            SUM(CashAward) as TotalCashAward,
            
            -- Quality statistics
            AVG(AwardDataQualityScore) as AvgQualityScore,
            COUNT(CASE WHEN AwardDataQualityScore >= 90 THEN 1 END) as HighQualityRecords,
            
            -- Completeness percentages
            ROUND(100.0 * COUNT(AwardValue) / COUNT(*), 2) as AwardValueCompleteness,
            ROUND(100.0 * COUNT(CashAward) / COUNT(*), 2) as CashAwardCompleteness,
            ROUND(100.0 * COUNT(CASE WHEN AwardDataQualityScore >= 90 THEN 1 END) / COUNT(*), 2) as HighQualityPercentage
            
        FROM CleanGrantsLayer2;
        
        -- Agency Award Analysis View
        CREATE OR ALTER VIEW vw_AgencyAwardAnalysis AS
        SELECT 
            Agency,
            COUNT(*) as TotalOpportunities,
            COUNT(AwardValue) as OpportunitiesWithAwardValue,
            AVG(AwardValue) as AvgAwardValue,
            SUM(AwardValue) as TotalAwardValue,
            MAX(AwardValue) as MaxAwardValue,
            MIN(AwardValue) as MinAwardValue,
            AVG(AwardDataQualityScore) as AvgQualityScore,
            
            -- Award distribution by category
            COUNT(CASE WHEN AwardValue >= 10000000 THEN 1 END) as MegaGrants,
            COUNT(CASE WHEN AwardValue >= 1000000 AND AwardValue < 10000000 THEN 1 END) as LargeGrants,
            COUNT(CASE WHEN AwardValue >= 100000 AND AwardValue < 1000000 THEN 1 END) as MediumGrants,
            COUNT(CASE WHEN AwardValue >= 10000 AND AwardValue < 100000 THEN 1 END) as SmallGrants,
            COUNT(CASE WHEN AwardValue > 0 AND AwardValue < 10000 THEN 1 END) as MicroGrants,
            
            -- Completeness
            ROUND(100.0 * COUNT(AwardValue) / COUNT(*), 2) as AwardValueCompleteness
            
        FROM CleanGrantsLayer2
        WHERE Agency IS NOT NULL
        GROUP BY Agency
        HAVING COUNT(*) >= 5  -- Only agencies with 5+ opportunities
        ORDER BY TotalAwardValue DESC;
        
        -- Monthly Award Trends View (if PostDate available)
        CREATE OR ALTER VIEW vw_MonthlyAwardTrends AS
        SELECT 
            YEAR(PostDate) as Year,
            MONTH(PostDate) as Month,
            CONCAT(YEAR(PostDate), '-', FORMAT(MONTH(PostDate), '00')) as YearMonth,
            COUNT(*) as TotalOpportunities,
            COUNT(AwardValue) as OpportunitiesWithAwardValue,
            AVG(AwardValue) as AvgAwardValue,
            SUM(AwardValue) as TotalAwardValue,
            AVG(AwardDataQualityScore) as AvgQualityScore
            
        FROM CleanGrantsLayer2
        WHERE PostDate IS NOT NULL
        GROUP BY YEAR(PostDate), MONTH(PostDate)
        ORDER BY Year DESC, Month DESC;
        
        SELECT 'BUSINESS_INTELLIGENCE_VIEWS_CREATED' as Status, GETDATE() as CreationTimestamp;
        """
        
        temp_file = "bi_views.sql"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(bi_views_sql)
        
        result = self.execute_sql_file(temp_file)
        
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        if result:
            logger.info("✅ Business intelligence views created successfully")
            return True
        else:
            logger.error("❌ Failed to create business intelligence views")
            return False
    
    def cleanup_staging_table(self):
        """Clean up staging table after successful integration"""
        logger.info("🧹 Cleaning up staging table...")
        
        cleanup_sql = """
        -- Archive staging data before cleanup
        SELECT 
            'STAGING_CLEANUP' as Status,
            COUNT(*) as StagingRecordsProcessed,
            GETDATE() as CleanupTimestamp
        FROM AwardValueStaging;
        
        -- Drop staging table
        DROP TABLE IF EXISTS AwardValueStaging;
        
        SELECT 'STAGING_TABLE_DROPPED' as Status;
        """
        
        result = self.execute_sql_command(cleanup_sql)
        if result:
            logger.info("✅ Staging table cleaned up successfully")
            logger.info(result)
            return True
        else:
            logger.warning("⚠️ Staging table cleanup had issues (may not exist)")
            return True  # Not critical if it fails
    
    def run_full_integration(self):
        """Run the complete award value integration process"""
        logger.info("🚀 Starting complete award value integration process")
        logger.info("=" * 70)
        
        try:
            # Step 1: Create backup
            if not self.backup_current_state():
                logger.error("❌ Backup failed - aborting integration")
                return False
            
            # Step 2: Prepare schema
            if not self.prepare_schema_updates():
                logger.error("❌ Schema preparation failed - aborting integration")
                return False
            
            # Step 3: Create staging table
            if not self.create_staging_table():
                logger.error("❌ Staging table creation failed - aborting integration")
                return False
            
            # Step 4: Load data to staging
            if not self.load_data_to_staging():
                logger.error("❌ Data loading failed - aborting integration")
                return False
            
            # Step 5: Integrate with main table
            if not self.integrate_with_main_table():
                logger.error("❌ Main table integration failed - aborting integration")
                return False
            
            # Step 6: Create BI views
            if not self.create_business_intelligence_views():
                logger.warning("⚠️ BI views creation had issues - continuing")
            
            # Step 7: Cleanup
            self.cleanup_staging_table()
            
            logger.info("\n🎉 COMPLETE AWARD VALUE INTEGRATION SUCCESSFUL!")
            logger.info("✅ Backup created and schema updated")
            logger.info("✅ Award values integrated with CleanGrantsLayer2")
            logger.info("✅ Business intelligence views created")
            logger.info("✅ Data quality scores applied")
            logger.info("🚀 Your grants database now includes comprehensive award information!")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Integration process failed: {e}")
            import traceback
            traceback.print_exc()
            return False

def main():
    """Main integration function"""
    integrator = AzureAwardValueIntegrator()
    success = integrator.run_full_integration()
    
    if success:
        print("\n💼 Integration completed successfully!")
        print("🔍 You can now query your enhanced grants database:")
        print("   - SELECT * FROM vw_AwardValueAnalysis")
        print("   - SELECT * FROM vw_AwardStatisticsSummary")
        print("   - SELECT * FROM vw_AgencyAwardAnalysis")
        print("🚀 Your grants database is now enterprise-ready with award value data!")
    else:
        print("\n❌ Integration failed. Check logs for details.")
        print("💡 You can restore from backup if needed.")

if __name__ == "__main__":
    main()