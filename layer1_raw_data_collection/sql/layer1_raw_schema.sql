-- ===================================
-- UPDATE RAWGRANTSLAYER1 TO MATCH AZURE TABLE STORAGE
-- Complete column alignment with your storage account data
-- ===================================

USE GrantsGovDB;
GO

PRINT '🔄 Updating RawGrantsLayer1 to match Azure Table Storage schema...';
PRINT 'Adding OpportunityURL and other missing columns from storage account';
PRINT '===============================================';

-- First, let's check what exists
PRINT 'Current table status:';
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RawGrantsLayer1')
    PRINT '✅ RawGrantsLayer1 table exists - will add missing columns'
ELSE
    PRINT '❌ RawGrantsLayer1 table does not exist - will create it'

-- Create table if it doesn't exist
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RawGrantsLayer1')
BEGIN
    PRINT 'Creating RawGrantsLayer1 table with complete schema...';
    
    CREATE TABLE RawGrantsLayer1 (
        -- Primary Keys (Azure Table Storage format)
        ID BIGINT IDENTITY(1,1) PRIMARY KEY,
        PartitionKey NVARCHAR(255) NOT NULL DEFAULT 'Grant',
        RowKey NVARCHAR(255) NOT NULL,
        
        -- Core Grants.gov columns matching your Azure Storage
        OpportunityNumber NVARCHAR(255) NULL,
        OpportunityURL NVARCHAR(2000) NULL,  -- 🆕 From your storage account
        Title NVARCHAR(1000) NULL,
        AgencyCode NVARCHAR(100) NULL,
        AgencyName NVARCHAR(500) NULL,
        Category NVARCHAR(500) NULL,
        CategoryExplanation NVARCHAR(2000) NULL,
        FundingType NVARCHAR(255) NULL,
        CFDANumbers NVARCHAR(500) NULL,
        
        -- Financial Fields
        EstimatedTotalFunding DECIMAL(18,2) NULL,
        ExpectedAwards INT NULL,
        AwardCeiling DECIMAL(18,2) NULL,
        AwardFloor DECIMAL(18,2) NULL,
        CostSharing NVARCHAR(500) NULL,
        
        -- Additional Information
        AdditionalInfoURL NVARCHAR(2000) NULL,
        GrantorContact NVARCHAR(500) NULL,
        GrantorPhone NVARCHAR(100) NULL,
        GrantorEmail NVARCHAR(255) NULL,
        
        -- Date Fields
        EstimatedPostDate DATETIME2 NULL,
        EstimatedDueDate DATETIME2 NULL,
        PostedDate DATETIME2 NULL,
        CloseDate DATETIME2 NULL,
        LastUpdated DATETIME2 NULL,
        LastUpdatedOriginal DATETIME2 NULL,
        
        -- Status and Version Fields
        Version NVARCHAR(50) NULL,
        Status NVARCHAR(100) NULL,
        Package NVARCHAR(500) NULL,
        SynopsisArchived NVARCHAR(50) NULL,
        DataVersion NVARCHAR(50) NULL,
        
        -- Content Fields
        Description NVARCHAR(MAX) NULL,
        EligibleApplicants NVARCHAR(MAX) NULL,
        
        -- Processing Metadata (from your storage)
        ProcessedDate DATETIME2 NULL,
        ProcessedBy NVARCHAR(255) NULL,
        ProcessingTimestamp NVARCHAR(50) NULL,
        SourceType NVARCHAR(50) NULL,
        TotalColumns INT NULL,
        Timestamp DATETIME2 NULL,
        
        -- System Fields
        CreatedDate DATETIME2 DEFAULT GETDATE(),
        UpdatedDate DATETIME2 DEFAULT GETDATE(),
        
        CONSTRAINT UQ_RawGrants_RowKey UNIQUE (RowKey)
    );
    
    PRINT '✅ RawGrantsLayer1 created with complete schema including OpportunityURL';
END
ELSE
BEGIN
    PRINT 'Table exists - adding missing columns individually...';
    
    -- Add OpportunityURL column if it doesn't exist
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'OpportunityURL')
    BEGIN
        ALTER TABLE RawGrantsLayer1 ADD OpportunityURL NVARCHAR(2000) NULL;
        PRINT '✅ Added OpportunityURL column';
    END
    ELSE
        PRINT 'OpportunityURL column already exists';
    
    -- Add other missing columns
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'ProcessedBy')
    BEGIN
        ALTER TABLE RawGrantsLayer1 ADD ProcessedBy NVARCHAR(255) NULL;
        PRINT '✅ Added ProcessedBy column';
    END
    
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'ProcessingTimestamp')
    BEGIN
        ALTER TABLE RawGrantsLayer1 ADD ProcessingTimestamp NVARCHAR(50) NULL;
        PRINT '✅ Added ProcessingTimestamp column';
    END
    
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'SourceType')
    BEGIN
        ALTER TABLE RawGrantsLayer1 ADD SourceType NVARCHAR(50) NULL;
        PRINT '✅ Added SourceType column';
    END
    
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'TotalColumns')
    BEGIN
        ALTER TABLE RawGrantsLayer1 ADD TotalColumns INT NULL;
        PRINT '✅ Added TotalColumns column';
    END
    
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'Timestamp')
    BEGIN
        ALTER TABLE RawGrantsLayer1 ADD Timestamp DATETIME2 NULL;
        PRINT '✅ Added Timestamp column';
    END
    
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'DataVersion')
    BEGIN
        ALTER TABLE RawGrantsLayer1 ADD DataVersion NVARCHAR(50) NULL;
        PRINT '✅ Added DataVersion column';
    END
    
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'CategoryExplanation')
    BEGIN
        ALTER TABLE RawGrantsLayer1 ADD CategoryExplanation NVARCHAR(2000) NULL;
        PRINT '✅ Added CategoryExplanation column';
    END
    
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'CFDANumbers')
    BEGIN
        ALTER TABLE RawGrantsLayer1 ADD CFDANumbers NVARCHAR(500) NULL;
        PRINT '✅ Added CFDANumbers column';
    END
    
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'EstimatedTotalFunding')
    BEGIN
        ALTER TABLE RawGrantsLayer1 ADD EstimatedTotalFunding DECIMAL(18,2) NULL;
        PRINT '✅ Added EstimatedTotalFunding column';
    END
    
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'Description')
    BEGIN
        ALTER TABLE RawGrantsLayer1 ADD Description NVARCHAR(MAX) NULL;
        PRINT '✅ Added Description column';
    END
    
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RawGrantsLayer1' AND COLUMN_NAME = 'EligibleApplicants')
    BEGIN
        ALTER TABLE RawGrantsLayer1 ADD EligibleApplicants NVARCHAR(MAX) NULL;
        PRINT '✅ Added EligibleApplicants column';
    END
END

-- Create index on OpportunityURL for performance
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_RawGrants_OpportunityURL' AND object_id = OBJECT_ID('RawGrantsLayer1'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_RawGrants_OpportunityURL 
    ON RawGrantsLayer1 (OpportunityURL)
    WHERE OpportunityURL IS NOT NULL;
    PRINT '✅ Created index on OpportunityURL column';
END

-- Verification - show current schema
PRINT '';
PRINT '🔍 Current schema verification:';
SELECT 
    COUNT(*) as Total_Columns
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'RawGrantsLayer1';

-- Show OpportunityURL column specifically
SELECT 
    'OpportunityURL_Column' as Column_Info,
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'RawGrantsLayer1' 
AND COLUMN_NAME = 'OpportunityURL';

PRINT '✅ Schema update completed successfully!';
PRINT '🌐 OpportunityURL column is ready for Azure Storage data';

GO