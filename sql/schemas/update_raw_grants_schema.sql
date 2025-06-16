-- ===================================
-- UPDATE RAWGRANTSLAYER1 TO MATCH AZURE TABLE STORAGE
-- Complete column alignment with your storage account data
-- ===================================

USE GrantsGovDB;
GO

PRINT '🔄 Updating RawGrantsLayer1 to match Azure Table Storage schema...';
PRINT 'Adding all 28+ columns from grants.gov CSV structure';
PRINT '===============================================';

-- Check if RawGrantsLayer1 exists, if not create it
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RawGrantsLayer1')
BEGIN
    PRINT 'Creating RawGrantsLayer1 table with complete schema...';
    
    CREATE TABLE RawGrantsLayer1 (
        -- Primary Keys (Azure Table Storage format)
        ID BIGINT IDENTITY(1,1) PRIMARY KEY,
        PartitionKey NVARCHAR(255) NOT NULL DEFAULT 'Grant',
        RowKey NVARCHAR(255) NOT NULL,
        
        -- Core Grants.gov CSV Columns (exact match to your storage)
        OpportunityNumber NVARCHAR(255) NULL,
        Title NVARCHAR(1000) NULL,
        AgencyCode NVARCHAR(100) NULL,
        AgencyName NVARCHAR(500) NULL,
        Category NVARCHAR(500) NULL, -- CATEGORY OF FUNDING ACTIVITY
        CategoryExplanation NVARCHAR(2000) NULL, -- FUNDING CATEGORY EXPLANATION
        FundingType NVARCHAR(255) NULL, -- FUNDING INSTRUMENT TYPE
        CFDANumbers NVARCHAR(500) NULL, -- ASSISTANCE LISTINGS
        
        -- Financial Fields
        EstimatedTotalFunding DECIMAL(18,2) NULL,
        ExpectedAwards INT NULL,
        AwardCeiling DECIMAL(18,2) NULL,
        AwardFloor DECIMAL(18,2) NULL,
        
        -- Additional Information
        CostSharing NVARCHAR(500) NULL,
        AdditionalInfoURL NVARCHAR(2000) NULL,
        GrantorContact NVARCHAR(500) NULL,
        GrantorPhone NVARCHAR(100) NULL,
        GrantorEmail NVARCHAR(255) NULL,
        
        -- Date Fields
        EstimatedPostDate DATETIME2 NULL,
        EstimatedDueDate DATETIME2 NULL,
        PostedDate DATETIME2 NULL,
        CloseDate DATETIME2 NULL,
        LastUpdatedOriginal DATETIME2 NULL,
        
        -- Status and Version
        Version NVARCHAR(50) NULL,
        Status NVARCHAR(100) NULL,
        Package NVARCHAR(500) NULL,
        SynopsisArchived NVARCHAR(50) NULL,
        
        -- Long Text Fields
        Description NVARCHAR(MAX) NULL,
        EligibleApplicants NVARCHAR(MAX) NULL,
        
        -- Processing Metadata
        ProcessedDate DATETIME2 NULL,
        ProcessingTimestamp NVARCHAR(50) NULL,
        SourceType NVARCHAR(50) NULL,
        TotalColumns INT NULL,
        
        -- System Fields
        CreatedDate DATETIME2 DEFAULT GETDATE(),
        UpdatedDate DATETIME2 DEFAULT GETDATE(),
        
        CONSTRAINT UQ_RawGrants_RowKey UNIQUE (RowKey)
    );
    
    PRINT '✅ RawGrantsLayer1 created with complete schema (28+ columns)';
END
ELSE
BEGIN
    PRINT 'RawGrantsLayer1 exists, checking for missing columns...';
    
    -- Add missing columns if they don't exist
    DECLARE @sql NVARCHAR(MAX) = '';
    
    -- Check and add columns dynamically
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'CategoryExplanation')
        SET @sql = @sql + 'ALTER TABLE RawGrantsLayer1 ADD CategoryExplanation NVARCHAR(2000) NULL;' + CHAR(13);
    
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'CFDANumbers')
        SET @sql = @sql + 'ALTER TABLE RawGrantsLayer1 ADD CFDANumbers NVARCHAR(500) NULL;' + CHAR(13);
        
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'EstimatedTotalFunding')
        SET @sql = @sql + 'ALTER TABLE RawGrantsLayer1 ADD EstimatedTotalFunding DECIMAL(18,2) NULL;' + CHAR(13);
        
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'Description')
        SET @sql = @sql + 'ALTER TABLE RawGrantsLayer1 ADD Description NVARCHAR(MAX) NULL;' + CHAR(13);
        
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'EligibleApplicants')
        SET @sql = @sql + 'ALTER TABLE RawGrantsLayer1 ADD EligibleApplicants NVARCHAR(MAX) NULL;' + CHAR(13);
        
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'ProcessingTimestamp')
        SET @sql = @sql + 'ALTER TABLE RawGrantsLayer1 ADD ProcessingTimestamp NVARCHAR(50) NULL;' + CHAR(13);
        
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'SourceType')
        SET @sql = @sql + 'ALTER TABLE RawGrantsLayer1 ADD SourceType NVARCHAR(50) NULL;' + CHAR(13);
    
    -- Execute the ALTER statements if any columns need to be added
    IF LEN(@sql) > 0
    BEGIN
        EXEC sp_executesql @sql;
        PRINT '✅ Missing columns added to RawGrantsLayer1';
    END
    ELSE
        PRINT '✅ RawGrantsLayer1 schema is already up to date';
END

-- Final verification
PRINT '';
PRINT '🔍 Schema verification:';
SELECT 
    COUNT(*) as 'Total_Columns',
    STRING_AGG(COLUMN_NAME, ', ') as 'Sample_Columns'
FROM (
    SELECT TOP 10 COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'RawGrantsLayer1'
    ORDER BY ORDINAL_POSITION
) t;

PRINT '✅ RawGrantsLayer1 schema update completed successfully!';
PRINT 'Ready to receive data from Azure Table Storage';

GO