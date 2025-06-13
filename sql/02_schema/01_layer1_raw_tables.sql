-- ===================================
-- GRANTS.GOV API AZURE - LAYER 1: RAW DATA TABLES
-- ===================================

USE GrantsGovDB;
GO

-- Raw Grants Data (exact replica of source data)
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='RawGrantsLayer1' AND xtype='U')
CREATE TABLE RawGrantsLayer1 (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    OpportunityNumber NVARCHAR(100) NOT NULL,
    OpportunityTitle NVARCHAR(500),
    AgencyName NVARCHAR(200),
    AgencyCode NVARCHAR(50),
    FundingDescription NTEXT,
    PostedDate NVARCHAR(50),
    CloseDate NVARCHAR(50),
    AwardCeiling NVARCHAR(50),
    AwardFloor NVARCHAR(50),
    CategoryOfFundingActivity NVARCHAR(200),
    FundingInstrumentType NVARCHAR(100),
    EligibleApplicants NTEXT,
    LinkToAdditionalInformation NVARCHAR(500),
    AssistanceListings NVARCHAR(200),
    ExpectedNumberOfAwards NVARCHAR(50),
    EstimatedTotalFunding NVARCHAR(50),
    GrantorContactEmail NVARCHAR(200),
    DataQualityScore DECIMAL(3,2) DEFAULT 5.0,
    ImportDate DATETIME2 DEFAULT GETUTCDATE(),
    SourceFile NVARCHAR(200) DEFAULT 'Azure_Table_Storage',
    ProcessingStatus NVARCHAR(50) DEFAULT 'Raw',
    ValidationErrors NVARCHAR(1000)
);

-- Create indexes on raw grants table
IF EXISTS (SELECT * FROM sysobjects WHERE name='RawGrantsLayer1' AND xtype='U')
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_RawGrants_OpportunityNumber')
        CREATE INDEX IX_RawGrants_OpportunityNumber ON RawGrantsLayer1(OpportunityNumber);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_RawGrants_AgencyName')
        CREATE INDEX IX_RawGrants_AgencyName ON RawGrantsLayer1(AgencyName);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_RawGrants_ImportDate')
        CREATE INDEX IX_RawGrants_ImportDate ON RawGrantsLayer1(ImportDate);
END

-- Raw Agencies Data
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='RawAgenciesLayer1' AND xtype='U')
CREATE TABLE RawAgenciesLayer1 (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    AgencyName NVARCHAR(200) NOT NULL,
    AgencyCode NVARCHAR(50),
    AgencyType NVARCHAR(100),
    Department NVARCHAR(200),
    Website NVARCHAR(500),
    ContactInfo NVARCHAR(500),
    SourceSystem NVARCHAR(100),
    ImportDate DATETIME2 DEFAULT GETUTCDATE(),
    IsActive BIT DEFAULT 1
);

-- Raw Categories Data
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='RawCategoriesLayer1' AND xtype='U')
CREATE TABLE RawCategoriesLayer1 (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    CategoryName NVARCHAR(200) NOT NULL,
    CategoryDescription NVARCHAR(500),
    ParentCategory NVARCHAR(200),
    CategoryLevel INT DEFAULT 1,
    SourceSystem NVARCHAR(100),
    ImportDate DATETIME2 DEFAULT GETUTCDATE(),
    IsActive BIT DEFAULT 1
);

PRINT '✅ Layer 1 (Raw Data Tables) created successfully';
GO
