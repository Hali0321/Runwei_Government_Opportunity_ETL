-- ===================================
-- GRANTS.GOV API AZURE - ROLLBACK SCRIPT
-- ===================================

SET NOCOUNT ON;
GO

PRINT '===============================================';
PRINT 'Starting Grants.gov API Azure rollback';
PRINT 'Rollback started at ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '===============================================';
GO

-- Create rollback log table if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'RollbackLog') AND type = 'U')
BEGIN
    CREATE TABLE RollbackLog (
        LogID INT IDENTITY(1,1) PRIMARY KEY,
        StepName NVARCHAR(100) NOT NULL,
        StepStatus NVARCHAR(20) NOT NULL,
        Message NVARCHAR(MAX) NULL,
        RollbackTime DATETIME DEFAULT GETDATE()
    );
END
GO

-- Helper stored procedure for rollback logging
CREATE OR ALTER PROCEDURE LogRollbackStep
    @StepName NVARCHAR(100),
    @Status NVARCHAR(20),
    @Message NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO RollbackLog (StepName, StepStatus, Message)
    VALUES (@StepName, @Status, @Message);
    
    PRINT @StepName + ' - ' + @Status + ISNULL(': ' + @Message, '');
END;
GO

-- Define error handling
BEGIN TRY
    -- First verify if we should proceed with rollback
    DECLARE @ConfirmRollback BIT = 1; -- Set to 0 to prevent accidental execution
    
    IF @ConfirmRollback = 0
    BEGIN
        RAISERROR('Rollback protection is enabled. To execute rollback, set @ConfirmRollback = 1', 16, 1);
        RETURN;
    END
    
    -- Step 1: Drop Views (reverse order of creation)
    EXEC LogRollbackStep 'Dropping Views', 'Starting';
    
    -- API Views
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_DashboardSummary' AND schema_id = SCHEMA_ID('api'))
        DROP VIEW api.vw_DashboardSummary;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_EligibilityList' AND schema_id = SCHEMA_ID('api'))
        DROP VIEW api.vw_EligibilityList;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_CategoryList' AND schema_id = SCHEMA_ID('api'))
        DROP VIEW api.vw_CategoryList;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_AgencyList' AND schema_id = SCHEMA_ID('api'))
        DROP VIEW api.vw_AgencyList;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_GrantDetail' AND schema_id = SCHEMA_ID('api'))
        DROP VIEW api.vw_GrantDetail;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_GrantSearch' AND schema_id = SCHEMA_ID('api'))
        DROP VIEW api.vw_GrantSearch;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_GrantSummary' AND schema_id = SCHEMA_ID('api'))
        DROP VIEW api.vw_GrantSummary;
    
    -- Reporting Views
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_GeographicDistribution')
        DROP VIEW vw_GeographicDistribution;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_SuccessFactors')
        DROP VIEW vw_SuccessFactors;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_EligibilityAnalysis')
        DROP VIEW vw_EligibilityAnalysis;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_CompetitionAnalysis')
        DROP VIEW vw_CompetitionAnalysis;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_GrantDeadlineCalendar')
        DROP VIEW vw_GrantDeadlineCalendar;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_FundingTrends')
        DROP VIEW vw_FundingTrends;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_CategoryAnalysis')
        DROP VIEW vw_CategoryAnalysis;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_AgencyPerformance')
        DROP VIEW vw_AgencyPerformance;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_GrantsSummary')
        DROP VIEW vw_GrantsSummary;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_HighValueGrants')
        DROP VIEW vw_HighValueGrants;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_ActiveGrants')
        DROP VIEW vw_ActiveGrants;
    
    -- Monitoring Views
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_AzureSQLResourceStats' AND schema_id = SCHEMA_ID('monitoring'))
        DROP VIEW monitoring.vw_AzureSQLResourceStats;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_DatabaseGrowthTrend' AND schema_id = SCHEMA_ID('monitoring'))
        DROP VIEW monitoring.vw_DatabaseGrowthTrend;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_ApplicationErrorSummary' AND schema_id = SCHEMA_ID('monitoring'))
        DROP VIEW monitoring.vw_ApplicationErrorSummary;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_APIRequestSummary' AND schema_id = SCHEMA_ID('monitoring'))
        DROP VIEW monitoring.vw_APIRequestSummary;
        
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_PerformanceOverview' AND schema_id = SCHEMA_ID('monitoring'))
        DROP VIEW monitoring.vw_PerformanceOverview;
        
    EXEC LogRollbackStep 'Dropping Views', 'Completed', 'Successfully dropped all views';
    
    -- Step 2: Drop Stored Procedures
    EXEC LogRollbackStep 'Dropping Stored Procedures', 'Starting';
    
    -- Maintenance Procedures
    IF EXISTS (SELECT * FROM sys.objects WHERE name = 'PurgeOldLogs' AND schema_id = SCHEMA_ID('maintenance') AND type = 'P')
        DROP PROCEDURE maintenance.PurgeOldLogs;
        
    IF EXISTS (SELECT * FROM sys.objects WHERE name = 'DatabaseHealthCheck' AND schema_id = SCHEMA_ID('maintenance') AND type = 'P')
        DROP PROCEDURE maintenance.DatabaseHealthCheck;
        
    IF EXISTS (SELECT * FROM sys.objects WHERE name = 'ArchiveOldGrants' AND schema_id = SCHEMA_ID('maintenance') AND type = 'P')
        DROP PROCEDURE maintenance.ArchiveOldGrants;
        
    IF EXISTS (SELECT * FROM sys.objects WHERE name = 'RebuildIndexes' AND schema_id = SCHEMA_ID('maintenance') AND type = 'P')
        DROP PROCEDURE maintenance.RebuildIndexes;
    
    -- Monitoring Procedures
    IF EXISTS (SELECT * FROM sys.objects WHERE name = 'CollectDatabaseMetrics' AND schema_id = SCHEMA_ID('monitoring') AND type = 'P')
        DROP PROCEDURE monitoring.CollectDatabaseMetrics;
    
    -- Rollback Logging Procedure
    IF EXISTS (SELECT * FROM sys.objects WHERE name = 'LogRollbackStep' AND type = 'P')
        DROP PROCEDURE LogRollbackStep;
    
    EXEC LogRollbackStep 'Dropping Stored Procedures', 'Completed', 'Successfully dropped all procedures';
    
    -- Step 3: Drop Tables (reverse order of creation)
    EXEC LogRollbackStep 'Dropping Tables', 'Starting';
    
    -- Monitoring Tables
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'DatabaseGrowth' AND schema_id = SCHEMA_ID('monitoring'))
        DROP TABLE monitoring.DatabaseGrowth;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'PerformanceMetrics' AND schema_id = SCHEMA_ID('monitoring'))
        DROP TABLE monitoring.PerformanceMetrics;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'HealthCheckLog' AND schema_id = SCHEMA_ID('monitoring'))
        DROP TABLE monitoring.HealthCheckLog;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'MaintenanceLog' AND schema_id = SCHEMA_ID('monitoring'))
        DROP TABLE monitoring.MaintenanceLog;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'APIRequestLog' AND schema_id = SCHEMA_ID('monitoring'))
        DROP TABLE monitoring.APIRequestLog;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'ApplicationLog' AND schema_id = SCHEMA_ID('monitoring'))
        DROP TABLE monitoring.ApplicationLog;
    
    -- Business Layer Tables
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'TrendAnalysisLayer3')
        DROP TABLE TrendAnalysisLayer3;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'CategoryStatsLayer3')
        DROP TABLE CategoryStatsLayer3;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'AgencyStatsLayer3')
        DROP TABLE AgencyStatsLayer3;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'SuccessFactorsLayer3')
        DROP TABLE SuccessFactorsLayer3;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'GrantBusinessViewLayer3')
        DROP TABLE GrantBusinessViewLayer3;
    
    -- Cleaned Layer Tables
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'GrantGeographicCoverageLayer2')
        DROP TABLE GrantGeographicCoverageLayer2;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'GrantEligibilityLayer2')
        DROP TABLE GrantEligibilityLayer2;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'GeographicCoverageMasterLayer2')
        DROP TABLE GeographicCoverageMasterLayer2;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'EligibilityMasterLayer2')
        DROP TABLE EligibilityMasterLayer2;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'CategoryMasterLayer2')
        DROP TABLE CategoryMasterLayer2;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'AgencyMasterLayer2')
        DROP TABLE AgencyMasterLayer2;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'ArchivedGrantsLayer2')
        DROP TABLE ArchivedGrantsLayer2;
        
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'CleanedGrantsLayer2')
        DROP TABLE CleanedGrantsLayer2;
    
    -- Raw Layer Tables
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'RawGrantsLayer1')
        DROP TABLE RawGrantsLayer1;
    
    -- Rollback Log Table (drop last)
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'RollbackLog')
        DROP TABLE RollbackLog;
    
    EXEC LogRollbackStep 'Dropping Tables', 'Completed', 'Successfully dropped all tables';
    
    -- Step 4: Drop Schemas
    EXEC LogRollbackStep 'Dropping Schemas', 'Starting';
    
    IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'api')
        DROP SCHEMA api;
        
    IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'monitoring')
        DROP SCHEMA monitoring;
        
    IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'maintenance')
        DROP SCHEMA maintenance;
    
    EXEC LogRollbackStep 'Dropping Schemas', 'Completed', 'Successfully dropped all schemas';
    
    -- Log successful completion
    EXEC LogRollbackStep 'Rollback', 'Completed', 'Successfully completed full database rollback';

END TRY
BEGIN CATCH
    -- Capture error details
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorState INT = ERROR_STATE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorProcedure NVARCHAR(200) = ISNULL(ERROR_PROCEDURE(), '-');
    
    -- Format full error message
    DECLARE @FullErrorMessage NVARCHAR(4000) = 
        'Error ' + CAST(@ErrorSeverity AS NVARCHAR) + 
        ', State ' + CAST(@ErrorState AS NVARCHAR) + 
        ' occurred at line ' + CAST(@ErrorLine AS NVARCHAR) + 
        ' in procedure ' + @ErrorProcedure + 
        '. Message: ' + @ErrorMessage;
    
    -- Log error to rollback log
    INSERT INTO RollbackLog (StepName, StepStatus, Message)
    VALUES ('Rollback', 'ERROR', @FullErrorMessage);
    
    -- Re-throw with original severity
    RAISERROR(@FullErrorMessage, @ErrorSeverity, 1);
END CATCH;

PRINT '===============================================';
PRINT 'Rollback completed at ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '===============================================';
GO