-- ===================================
-- GRANTS.GOV API AZURE - STANDALONE DEPLOYMENT SCRIPT
-- ===================================

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
    -- Step 1: Create Schemas
    EXEC LogDeploymentStep 'Schema Creation', 'Starting';
    
    -- Create API schema
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'api')
        EXEC('CREATE SCHEMA api');
    
    -- Create monitoring schema
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'monitoring')
        EXEC('CREATE SCHEMA monitoring');
    
    -- Create maintenance schema
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'maintenance')
        EXEC('CREATE SCHEMA maintenance');
    
    EXEC LogDeploymentStep 'Schema Creation', 'Completed';
    
    -- Step 2: Create Core Tables (Layer 1 - Raw Data)
    EXEC LogDeploymentStep 'Layer 1 Tables', 'Starting';
    
    -- Create RawGrantsLayer1 table (if it doesn't exist)
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'RawGrantsLayer1') AND type = 'U')
    BEGIN
        CREATE TABLE RawGrantsLayer1 (
            RawID INT IDENTITY(1,1) PRIMARY KEY,
            RowKey NVARCHAR(255) NOT NULL,
            PartitionKey NVARCHAR(255) NOT NULL,
            OpportunityID NVARCHAR(255),
            Title NVARCHAR(MAX),
            AgencyName NVARCHAR(500),
            Description NVARCHAR(MAX),
            RawData NVARCHAR(MAX),
            ImportDate DATETIME DEFAULT GETDATE(),
            DataSource NVARCHAR(100) DEFAULT 'Azure Table Storage',
            INDEX IX_RawGrants_OpportunityID NONCLUSTERED (OpportunityID),
            INDEX IX_RawGrants_PartitionKey NONCLUSTERED (PartitionKey),
            INDEX IX_RawGrants_ImportDate NONCLUSTERED (ImportDate)
        );
    END
    
    EXEC LogDeploymentStep 'Layer 1 Tables', 'Completed';
    
    -- Step 3: Create Master Tables (Layer 2)
    EXEC LogDeploymentStep 'Layer 2 Master Tables', 'Starting';
    
    -- Create AgencyMasterLayer2 table
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'AgencyMasterLayer2') AND type = 'U')
    BEGIN
        CREATE TABLE AgencyMasterLayer2 (
            AgencyID INT IDENTITY(1,1) PRIMARY KEY,
            AgencyCode NVARCHAR(20) NOT NULL UNIQUE,
            AgencyName NVARCHAR(500) NOT NULL,
            ParentAgency NVARCHAR(500),
            WebsiteURL NVARCHAR(1000),
            ContactEmail NVARCHAR(255),
            ContactPhone NVARCHAR(50),
            IsActive BIT DEFAULT 1,
            CreatedDate DATETIME DEFAULT GETDATE(),
            LastUpdated DATETIME DEFAULT GETDATE(),
            DataQualityScore INT DEFAULT 0,
            INDEX IX_Agency_Code NONCLUSTERED (AgencyCode),
            INDEX IX_Agency_Active NONCLUSTERED (IsActive)
        );
    END
    
    -- Create CategoryMasterLayer2 table
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'CategoryMasterLayer2') AND type = 'U')
    BEGIN
        CREATE TABLE CategoryMasterLayer2 (
            CategoryID INT IDENTITY(1,1) PRIMARY KEY,
            CategoryName NVARCHAR(255) NOT NULL,
            CategoryGroup NVARCHAR(255),
            Description NVARCHAR(MAX),
            Keywords NVARCHAR(MAX),
            IsActive BIT DEFAULT 1,
            CreatedDate DATETIME DEFAULT GETDATE(),
            LastUpdated DATETIME DEFAULT GETDATE(),
            INDEX IX_Category_Name NONCLUSTERED (CategoryName),
            INDEX IX_Category_Active NONCLUSTERED (IsActive)
        );
    END
    
    -- Create EligibilityMasterLayer2 table
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'EligibilityMasterLayer2') AND type = 'U')
    BEGIN
        CREATE TABLE EligibilityMasterLayer2 (
            EligibilityID INT IDENTITY(1,1) PRIMARY KEY,
            EligibilityType NVARCHAR(255) NOT NULL,
            EligibilityDescription NVARCHAR(MAX),
            IsActive BIT DEFAULT 1,
            CreatedDate DATETIME DEFAULT GETDATE(),
            INDEX IX_Eligibility_Type NONCLUSTERED (EligibilityType),
            INDEX IX_Eligibility_Active NONCLUSTERED (IsActive)
        );
    END
    
    -- Create GeographicCoverageMasterLayer2 table
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'GeographicCoverageMasterLayer2') AND type = 'U')
    BEGIN
        CREATE TABLE GeographicCoverageMasterLayer2 (
            GeographicID INT IDENTITY(1,1) PRIMARY KEY,
            GeographicLevel NVARCHAR(50) NOT NULL,
            RegionName NVARCHAR(255) NOT NULL,
            StateCode NVARCHAR(5),
            CountryCode NVARCHAR(5) DEFAULT 'US',
            IsActive BIT DEFAULT 1,
            INDEX IX_Geographic_Level NONCLUSTERED (GeographicLevel),
            INDEX IX_Geographic_State NONCLUSTERED (StateCode)
        );
    END
    
    EXEC LogDeploymentStep 'Layer 2 Master Tables', 'Completed';
    
    -- Step 4: Create Cleaned Grants Table (Layer 2)
    EXEC LogDeploymentStep 'Layer 2 Cleaned Tables', 'Starting';
    
    -- Create CleanedGrantsLayer2 table
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'CleanedGrantsLayer2') AND type = 'U')
    BEGIN
        CREATE TABLE CleanedGrantsLayer2 (
            GrantID INT IDENTITY(1,1) PRIMARY KEY,
            OpportunityID NVARCHAR(255) NOT NULL UNIQUE,
            Title NVARCHAR(MAX) NOT NULL,
            AgencyID INT,
            CategoryID INT,
            Description NVARCHAR(MAX),
            AwardCeiling DECIMAL(18,2),
            AwardFloor DECIMAL(18,2),
            EstimatedTotalFunding DECIMAL(18,2),
            ExpectedAwards INT,
            CFDANumbers NVARCHAR(500),
            PostedDate DATETIME,
            CloseDate DATETIME,
            DaysUntilDeadline AS DATEDIFF(DAY, GETDATE(), CloseDate),
            Status NVARCHAR(50),
            EligibilityFullText NVARCHAR(MAX),
            Keywords NVARCHAR(MAX),
            GrantsGovURL NVARCHAR(1000),
            AdditionalInfoURL NVARCHAR(1000),
            ContactEmail NVARCHAR(255),
            ContactPhone NVARCHAR(50),
            IsActive BIT DEFAULT 1,
            CreatedDate DATETIME DEFAULT GETDATE(),
            LastUpdated DATETIME DEFAULT GETDATE(),
            DataQualityScore INT DEFAULT 0,
            FOREIGN KEY (AgencyID) REFERENCES AgencyMasterLayer2(AgencyID),
            FOREIGN KEY (CategoryID) REFERENCES CategoryMasterLayer2(CategoryID),
            INDEX IX_Grants_OpportunityID NONCLUSTERED (OpportunityID),
            INDEX IX_Grants_CloseDate NONCLUSTERED (CloseDate),
            INDEX IX_Grants_AgencyID NONCLUSTERED (AgencyID),
            INDEX IX_Grants_CategoryID NONCLUSTERED (CategoryID),
            INDEX IX_Grants_Status NONCLUSTERED (Status),
            INDEX IX_Grants_Active NONCLUSTERED (IsActive)
        );
    END
    
    -- Create junction tables
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'GrantEligibilityLayer2') AND type = 'U')
    BEGIN
        CREATE TABLE GrantEligibilityLayer2 (
            GrantEligibilityID INT IDENTITY(1,1) PRIMARY KEY,
            GrantID INT NOT NULL,
            EligibilityID INT NOT NULL,
            FOREIGN KEY (GrantID) REFERENCES CleanedGrantsLayer2(GrantID),
            FOREIGN KEY (EligibilityID) REFERENCES EligibilityMasterLayer2(EligibilityID),
            UNIQUE(GrantID, EligibilityID)
        );
    END
    
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'GrantGeographicCoverageLayer2') AND type = 'U')
    BEGIN
        CREATE TABLE GrantGeographicCoverageLayer2 (
            GrantGeographicID INT IDENTITY(1,1) PRIMARY KEY,
            GrantID INT NOT NULL,
            GeographicID INT NOT NULL,
            FOREIGN KEY (GrantID) REFERENCES CleanedGrantsLayer2(GrantID),
            FOREIGN KEY (GeographicID) REFERENCES GeographicCoverageMasterLayer2(GeographicID),
            UNIQUE(GrantID, GeographicID)
        );
    END
    
    EXEC LogDeploymentStep 'Layer 2 Cleaned Tables', 'Completed';
    
    -- Step 5: Create Business Layer Tables (Layer 3)
    EXEC LogDeploymentStep 'Layer 3 Business Tables', 'Starting';
    
    -- Create GrantBusinessViewLayer3 table
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'GrantBusinessViewLayer3') AND type = 'U')
    BEGIN
        CREATE TABLE GrantBusinessViewLayer3 (
            BusinessViewID INT IDENTITY(1,1) PRIMARY KEY,
            GrantID INT NOT NULL,
            ApplicantCount INT,
            CompetitionLevel NVARCHAR(50),
            FundingTier NVARCHAR(50),
            PastCompetitionLevel NVARCHAR(50),
            RecommendedFocus NVARCHAR(MAX),
            CreatedDate DATETIME DEFAULT GETDATE(),
            LastUpdated DATETIME DEFAULT GETDATE(),
            FOREIGN KEY (GrantID) REFERENCES CleanedGrantsLayer2(GrantID),
            INDEX IX_BusinessView_GrantID NONCLUSTERED (GrantID),
            INDEX IX_BusinessView_CompetitionLevel NONCLUSTERED (CompetitionLevel)
        );
    END
    
    -- Create SuccessFactorsLayer3 table
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'SuccessFactorsLayer3') AND type = 'U')
    BEGIN
        CREATE TABLE SuccessFactorsLayer3 (
            SuccessFactorID INT IDENTITY(1,1) PRIMARY KEY,
            GrantID INT NOT NULL,
            SuccessMetric NVARCHAR(255),
            SuccessScore INT,
            KeyCompetitiveFactors NVARCHAR(MAX),
            RecommendedApproach NVARCHAR(MAX),
            PastSuccessTemplates NVARCHAR(MAX),
            RequiredCapabilities NVARCHAR(MAX),
            CreatedDate DATETIME DEFAULT GETDATE(),
            FOREIGN KEY (GrantID) REFERENCES CleanedGrantsLayer2(GrantID),
            INDEX IX_SuccessFactors_GrantID NONCLUSTERED (GrantID),
            INDEX IX_SuccessFactors_Score NONCLUSTERED (SuccessScore)
        );
    END
    
    -- Create AgencyStatsLayer3 table
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'AgencyStatsLayer3') AND type = 'U')
    BEGIN
        CREATE TABLE AgencyStatsLayer3 (
            AgencyStatsID INT IDENTITY(1,1) PRIMARY KEY,
            AgencyID INT NOT NULL,
            TotalGrants INT,
            ActiveGrants INT,
            ClosedGrants INT,
            TotalFunding DECIMAL(18,2),
            AvgFunding DECIMAL(18,2),
            CategoryCount INT,
            AvgApplicationPeriod INT,
            LastCalculated DATETIME DEFAULT GETDATE(),
            FOREIGN KEY (AgencyID) REFERENCES AgencyMasterLayer2(AgencyID),
            INDEX IX_AgencyStats_AgencyID NONCLUSTERED (AgencyID),
            INDEX IX_AgencyStats_LastCalculated NONCLUSTERED (LastCalculated)
        );
    END
    
    EXEC LogDeploymentStep 'Layer 3 Business Tables', 'Completed';
    
    -- Step 6: Create Monitoring Tables
    EXEC LogDeploymentStep 'Monitoring Tables', 'Starting';
    
    -- Create Application Log Table
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'monitoring.ApplicationLog') AND type = 'U')
    BEGIN
        CREATE TABLE monitoring.ApplicationLog (
            LogID INT IDENTITY(1,1) PRIMARY KEY,
            LogLevel VARCHAR(10) NOT NULL,
            LogComponent VARCHAR(50) NOT NULL,
            LogMessage NVARCHAR(MAX) NOT NULL,
            LogDate DATETIME NOT NULL DEFAULT GETDATE(),
            Username VARCHAR(50),
            SessionID VARCHAR(100),
            ExceptionDetails NVARCHAR(MAX),
            ClientIP VARCHAR(50),
            INDEX IX_AppLog_Date NONCLUSTERED (LogDate),
            INDEX IX_AppLog_Level NONCLUSTERED (LogLevel),
            INDEX IX_AppLog_Component NONCLUSTERED (LogComponent)
        );
    END
    
    -- Create API Request Log Table
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'monitoring.APIRequestLog') AND type = 'U')
    BEGIN
        CREATE TABLE monitoring.APIRequestLog (
            RequestID INT IDENTITY(1,1) PRIMARY KEY,
            Endpoint VARCHAR(100) NOT NULL,
            Method VARCHAR(10) NOT NULL,
            StatusCode INT NOT NULL,
            RequestTime DATETIME NOT NULL DEFAULT GETDATE(),
            ResponseTime INT NOT NULL,
            ClientIP VARCHAR(50),
            UserAgent VARCHAR(255),
            QueryParams NVARCHAR(MAX),
            AuthUser VARCHAR(50),
            ErrorDetails NVARCHAR(MAX),
            RequestSize INT,
            ResponseSize INT,
            INDEX IX_APILog_RequestTime NONCLUSTERED (RequestTime),
            INDEX IX_APILog_Endpoint NONCLUSTERED (Endpoint),
            INDEX IX_APILog_StatusCode NONCLUSTERED (StatusCode)
        );
    END
    
    EXEC LogDeploymentStep 'Monitoring Tables', 'Completed';
    
    -- Step 7: Create Essential Views
    EXEC LogDeploymentStep 'Views', 'Starting';
    
    -- Create Active Grants View
    EXEC('CREATE OR ALTER VIEW vw_ActiveGrants AS
    SELECT 
        g.GrantID,
        g.OpportunityID,
        g.Title,
        a.AgencyName,
        c.CategoryName,
        g.AwardCeiling,
        g.CloseDate,
        g.DaysUntilDeadline,
        g.Status,
        g.GrantsGovURL
    FROM CleanedGrantsLayer2 g
    LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
    LEFT JOIN CategoryMasterLayer2 c ON g.CategoryID = c.CategoryID
    WHERE g.IsActive = 1 AND g.CloseDate >= GETDATE()');
    
    -- Create API Grant Summary View
    EXEC('CREATE OR ALTER VIEW api.vw_GrantSummary AS
    SELECT 
        g.GrantID,
        g.OpportunityID,
        g.Title,
        a.AgencyName,
        a.AgencyCode,
        c.CategoryName,
        g.AwardCeiling,
        g.EstimatedTotalFunding,
        g.PostedDate,
        g.CloseDate,
        g.DaysUntilDeadline,
        g.Status,
        g.GrantsGovURL,
        g.Description
    FROM CleanedGrantsLayer2 g
    LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
    LEFT JOIN CategoryMasterLayer2 c ON g.CategoryID = c.CategoryID
    WHERE g.IsActive = 1');
    
    -- Create High Value Grants View
    EXEC('CREATE OR ALTER VIEW vw_HighValueGrants AS
    SELECT 
        g.GrantID,
        g.OpportunityID,
        g.Title,
        a.AgencyName,
        g.AwardCeiling,
        g.EstimatedTotalFunding,
        g.CloseDate,
        g.DaysUntilDeadline
    FROM CleanedGrantsLayer2 g
    LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
    WHERE g.IsActive = 1 
    AND (g.AwardCeiling > 1000000 OR g.EstimatedTotalFunding > 5000000)
    AND g.CloseDate >= GETDATE()');
    
    EXEC LogDeploymentStep 'Views', 'Completed';
    
    -- Step 8: Insert Reference Data
    EXEC LogDeploymentStep 'Reference Data', 'Starting';
    
    -- Insert Agency Reference Data
    MERGE AgencyMasterLayer2 AS target
    USING (VALUES 
        ('HHS', 'Department of Health and Human Services', NULL, 'https://www.hhs.gov', 95),
        ('NIH', 'National Institutes of Health', 'HHS', 'https://www.nih.gov', 97),
        ('NSF', 'National Science Foundation', NULL, 'https://www.nsf.gov', 96),
        ('ED', 'Department of Education', NULL, 'https://www.ed.gov', 92),
        ('DOE', 'Department of Energy', NULL, 'https://www.energy.gov', 94),
        ('USDA', 'Department of Agriculture', NULL, 'https://www.usda.gov', 93),
        ('NASA', 'National Aeronautics and Space Administration', NULL, 'https://www.nasa.gov', 95),
        ('EPA', 'Environmental Protection Agency', NULL, 'https://www.epa.gov', 90)
    ) AS source (AgencyCode, AgencyName, ParentAgency, WebsiteURL, DataQualityScore)
    ON target.AgencyCode = source.AgencyCode
    WHEN NOT MATCHED THEN
        INSERT (AgencyCode, AgencyName, ParentAgency, WebsiteURL, DataQualityScore, IsActive)
        VALUES (source.AgencyCode, source.AgencyName, source.ParentAgency, source.WebsiteURL, source.DataQualityScore, 1);
    
    -- Insert Category Reference Data
    MERGE CategoryMasterLayer2 AS target
    USING (VALUES 
        ('Health', 'Medical and Health Sciences', 'healthcare, medicine, wellness, medical research, public health'),
        ('Science and Technology', 'STEM', 'research, innovation, engineering, computing, biology'),
        ('Education', 'Education and Training', 'schools, universities, teaching, learning, curriculum'),
        ('Agriculture', 'Food and Agriculture', 'farming, agriculture, food production, rural development'),
        ('Energy', 'Energy and Environment', 'renewable energy, sustainability, conservation, climate'),
        ('Environment', 'Environmental Sciences', 'environmental protection, sustainability, climate change'),
        ('Transportation', 'Transportation and Infrastructure', 'transportation, infrastructure, roads, transit'),
        ('Housing', 'Housing and Community Development', 'housing, community development, urban planning')
    ) AS source (CategoryName, CategoryGroup, Keywords)
    ON target.CategoryName = source.CategoryName
    WHEN NOT MATCHED THEN
        INSERT (CategoryName, CategoryGroup, Keywords, IsActive)
        VALUES (source.CategoryName, source.CategoryGroup, source.Keywords, 1);
    
    -- Insert Eligibility Reference Data
    MERGE EligibilityMasterLayer2 AS target
    USING (VALUES 
        ('Public/State Institutions', 'Public and State controlled institutions of higher education'),
        ('Private Institutions', 'Private institutions of higher education'),
        ('Nonprofit Organizations', '501(c)(3) nonprofit organizations with valid tax-exempt status'),
        ('For-profit Organizations', 'Small businesses and for-profit organizations'),
        ('State Governments', 'State governments and state agencies'),
        ('Local Governments', 'City or township governments, county governments'),
        ('Tribal Governments', 'Federally recognized tribal governments'),
        ('Individual Researchers', 'Individual researchers and scholars')
    ) AS source (EligibilityType, EligibilityDescription)
    ON target.EligibilityType = source.EligibilityType
    WHEN NOT MATCHED THEN
        INSERT (EligibilityType, EligibilityDescription, IsActive)
        VALUES (source.EligibilityType, source.EligibilityDescription, 1);
    
    EXEC LogDeploymentStep 'Reference Data', 'Completed';
    
    -- Step 9: Create Essential Stored Procedures
    EXEC LogDeploymentStep 'Stored Procedures', 'Starting';
    
    -- Create maintenance procedures in maintenance schema
    EXEC('CREATE OR ALTER PROCEDURE maintenance.RebuildIndexes AS
    BEGIN
        SET NOCOUNT ON;
        DECLARE @sql NVARCHAR(MAX) = '''';
        
        SELECT @sql = @sql + ''ALTER INDEX '' + i.name + '' ON '' + s.name + ''.'' + t.name + '' REBUILD;'' + CHAR(13)
        FROM sys.indexes i
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE i.index_id > 0
        AND i.is_disabled = 0;
        
        IF @sql <> ''''
            EXEC sp_executesql @sql;
    END');
    
    -- Create monitoring procedure
    EXEC('CREATE OR ALTER PROCEDURE monitoring.CollectDatabaseMetrics AS
    BEGIN
        SET NOCOUNT ON;
        
        INSERT INTO monitoring.ApplicationLog (LogLevel, LogComponent, LogMessage)
        VALUES (''INFO'', ''Monitoring'', ''Database metrics collected at '' + CONVERT(VARCHAR, GETDATE(), 120));
        
        -- Log database size
        DECLARE @dbSize VARCHAR(50);
        SELECT @dbSize = CAST((SUM(size) * 8 / 1024) AS VARCHAR) + '' MB''
        FROM sys.database_files;
        
        INSERT INTO monitoring.ApplicationLog (LogLevel, LogComponent, LogMessage)
        VALUES (''INFO'', ''Database'', ''Current database size: '' + @dbSize);
    END');
    
    EXEC LogDeploymentStep 'Stored Procedures', 'Completed';
    
    -- Step 10: Run Initial Maintenance
    EXEC LogDeploymentStep 'Initial Maintenance', 'Starting';
    
    -- Collect initial database metrics
    EXEC monitoring.CollectDatabaseMetrics;
    
    EXEC LogDeploymentStep 'Initial Maintenance', 'Completed';
    
    -- Log successful deployment
    EXEC LogDeploymentStep 'Deployment', 'Completed', 'Successfully deployed all components to Azure SQL Database';

END TRY
BEGIN CATCH
    -- Log error
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorState INT = ERROR_STATE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    DECLARE @ErrorProcedure NVARCHAR(200) = ISNULL(ERROR_PROCEDURE(), 'azure_standalone_deploy.sql');
    
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
PRINT 'Azure SQL Database deployment completed at ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '===============================================';
GO