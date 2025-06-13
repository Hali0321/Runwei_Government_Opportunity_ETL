-- ===================================
-- GRANTS.GOV API AZURE - MASTER DEPLOYMENT SCRIPT
-- ===================================

-- Enable error reporting
SET NOCOUNT ON;
GO

PRINT '===============================================';
PRINT 'Starting Grants.gov API Azure deployment';
PRINT 'Deployment started at ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '===============================================';
GO

-- Create deployment log table if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'DeploymentLog') AND type = 'U')
BEGIN
    CREATE TABLE DeploymentLog (
        LogID INT IDENTITY(1,1) PRIMARY KEY,
        StepName NVARCHAR(100) NOT NULL,
        StepStatus NVARCHAR(20) NOT NULL,
        Message NVARCHAR(MAX) NULL,
        DeploymentTime DATETIME DEFAULT GETDATE()
    );
END
GO

-- Log deployment start
INSERT INTO DeploymentLog (StepName, StepStatus, Message)
VALUES ('Deployment', 'Started', 'Starting full database deployment');
GO

-- Helper stored procedure for deployment logging
CREATE OR ALTER PROCEDURE LogDeploymentStep
    @StepName NVARCHAR(100),
    @Status NVARCHAR(20),
    @Message NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO DeploymentLog (StepName, StepStatus, Message)
    VALUES (@StepName, @Status, @Message);
    
    PRINT @StepName + ' - ' + @Status + ISNULL(': ' + @Message, '');
END;
GO

-- Define error handling
BEGIN TRY
    -- Step 1: Database Setup
    EXEC LogDeploymentStep 'Database Setup', 'Starting';
    
    :r ..\01_database_setup\01_create_database.sql
    :r ..\01_database_setup\02_security_setup.sql
    :r ..\01_database_setup\03_database_settings.sql
    
    EXEC LogDeploymentStep 'Database Setup', 'Completed';
    
    -- Step 2: Schema Creation
    EXEC LogDeploymentStep 'Schema Creation', 'Starting';
    
    :r ..\02_schema\01_layer1_raw_tables.sql
    :r ..\02_schema\02_layer2_cleaned_tables.sql
    :r ..\02_schema\03_layer3_business_tables.sql
    :r ..\02_schema\04_constraints_indexes.sql
    
    EXEC LogDeploymentStep 'Schema Creation', 'Completed';
    
    -- Step 3: Functions and Procedures
    EXEC LogDeploymentStep 'Functions and Procedures', 'Starting';
    
    :r ..\03_functions_procedures\01_functions.sql
    :r ..\03_functions_procedures\02_stored_procedures.sql
    :r ..\03_functions_procedures\03_triggers.sql
    
    EXEC LogDeploymentStep 'Functions and Procedures', 'Completed';
    
    -- Step 4: Views
    EXEC LogDeploymentStep 'Views', 'Starting';
    
    :r ..\04_views\01_reporting_views.sql
    :r ..\04_views\02_api_views.sql
    
    EXEC LogDeploymentStep 'Views', 'Completed';
    
    -- Step 5: Sample Data (optional based on environment)
    DECLARE @Environment VARCHAR(10);
    SELECT @Environment = CASE 
        WHEN @@SERVERNAME LIKE '%dev%' THEN 'DEV'
        WHEN @@SERVERNAME LIKE '%test%' THEN 'TEST'
        ELSE 'PROD'
    END;
    
    IF @Environment IN ('DEV', 'TEST')
    BEGIN
        EXEC LogDeploymentStep 'Sample Data', 'Starting', 'Loading in ' + @Environment + ' environment';
        
        :r ..\05_sample_data\01_reference_data.sql
        :r ..\05_sample_data\02_test_data.sql
        
        EXEC LogDeploymentStep 'Sample Data', 'Completed';
    END
    ELSE
    BEGIN
        -- In production, only load reference data
        EXEC LogDeploymentStep 'Reference Data', 'Starting', 'Loading in PROD environment';
        
        :r ..\05_sample_data\01_reference_data.sql
        
        EXEC LogDeploymentStep 'Reference Data', 'Completed';
    END
    
    -- Step 6: Maintenance and Monitoring Setup
    EXEC LogDeploymentStep 'Maintenance Setup', 'Starting';
    
    :r ..\06_maintenance\01_maintenance_procedures.sql
    :r ..\06_maintenance\02_monitoring_setup.sql
    
    EXEC LogDeploymentStep 'Maintenance Setup', 'Completed';
    
    -- Step 7: Initial Maintenance Tasks
    EXEC LogDeploymentStep 'Initial Maintenance', 'Starting';
    
    -- Rebuild all indexes to ensure optimal performance at start
    EXEC maintenance.RebuildIndexes;
    
    -- Collect initial database metrics
    EXEC monitoring.CollectDatabaseMetrics;
    
    EXEC LogDeploymentStep 'Initial Maintenance', 'Completed';
    
    -- Log successful deployment
    EXEC LogDeploymentStep 'Deployment', 'Completed', 'Successfully deployed all components';

END TRY
BEGIN CATCH
    -- Log error
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorState INT = ERROR_STATE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    DECLARE @ErrorProcedure NVARCHAR(200) = ISNULL(ERROR_PROCEDURE(), 'deploy_all.sql');
    
    DECLARE @FullErrorMessage NVARCHAR(4000) = 
        'Error ' + CAST(@ErrorNumber AS VARCHAR) + 
        ', Level ' + CAST(@ErrorSeverity AS VARCHAR) + 
        ', State ' + CAST(@ErrorState AS VARCHAR) + 
        ', Procedure ' + @ErrorProcedure +
        ', Line ' + CAST(@ErrorLine AS VARCHAR) + 
        ': ' + @ErrorMessage;
    
    EXEC LogDeploymentStep 'Deployment', 'Failed', @FullErrorMessage;
    
    -- Re-throw error to calling process
    RAISERROR(@FullErrorMessage, @ErrorSeverity, 1);
END CATCH;

PRINT '===============================================';
PRINT 'Deployment completed at ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '===============================================';
GO