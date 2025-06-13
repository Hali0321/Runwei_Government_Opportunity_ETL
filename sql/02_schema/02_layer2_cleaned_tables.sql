-- ===================================
-- GRANTS.GOV API AZURE - LAYER 2: CLEANED & NORMALIZED DATA
-- ===================================

USE GrantsGovDB;
GO

-- Agency Master Table
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='AgencyMasterLayer2' AND xtype='U')
CREATE TABLE AgencyMasterLayer2 (
    AgencyID INT IDENTITY(1,1) PRIMARY KEY,
    AgencyName NVARCHAR(200) NOT NULL UNIQUE,
    AgencyCode NVARCHAR(50),
    AgencyType NVARCHAR(100) DEFAULT 'Federal',
    Department NVARCHAR(200),
    ParentAgency NVARCHAR(200),
    Website NVARCHAR(500),
    ContactEmail NVARCHAR(200),
    PhoneNumber NVARCHAR(50),
    Address NVARCHAR(500),
    
    -- Calculated fields
    GrantCount INT DEFAULT 0,
    TotalFunding MONEY DEFAULT 0,
    AvgFundingAmount MONEY DEFAULT 0,
    ActiveGrantsCount INT DEFAULT 0,
    
    -- Data quality and audit
    DataQualityScore DECIMAL(3,2) DEFAULT 5.0,
    ValidationStatus NVARCHAR(50) DEFAULT 'Validated',
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETUTCDATE(),
    LastUpdated DATETIME2 DEFAULT GETUTCDATE(),
    LastValidated DATETIME2
);

-- Create agency indexes
IF EXISTS (SELECT * FROM sysobjects WHERE name='AgencyMasterLayer2' AND xtype='U')
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Agency_Name')
        CREATE INDEX IX_Agency_Name ON AgencyMasterLayer2(AgencyName);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Agency_Code')
        CREATE INDEX IX_Agency_Code ON AgencyMasterLayer2(AgencyCode);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Agency_Type')
        CREATE INDEX IX_Agency_Type ON AgencyMasterLayer2(AgencyType);
END

-- Category Master Table
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='CategoryMasterLayer2' AND xtype='U')
CREATE TABLE CategoryMasterLayer2 (
    CategoryID INT IDENTITY(1,1) PRIMARY KEY,
    CategoryName NVARCHAR(200) NOT NULL,
    CategoryCode NVARCHAR(50),
    CategoryGroup NVARCHAR(100),
    ParentCategoryID INT NULL,
    CategoryLevel INT DEFAULT 1,
    CategoryDescription NVARCHAR(500),
    Keywords NVARCHAR(500),
    
    -- Calculated fields
    GrantCount INT DEFAULT 0,
    TotalFunding MONEY DEFAULT 0,
    AvgFunding MONEY DEFAULT 0,
    ActiveGrantsCount INT DEFAULT 0,
    
    -- Data management
    IsActive BIT DEFAULT 1,
    SortOrder INT DEFAULT 0,
    CreatedDate DATETIME2 DEFAULT GETUTCDATE(),
    LastUpdated DATETIME2 DEFAULT GETUTCDATE()
);

-- Create category indexes
IF EXISTS (SELECT * FROM sysobjects WHERE name='CategoryMasterLayer2' AND xtype='U')
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Category_Name')
        CREATE INDEX IX_Category_Name ON CategoryMasterLayer2(CategoryName);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Category_Group')
        CREATE INDEX IX_Category_Group ON CategoryMasterLayer2(CategoryGroup);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Category_Level')
        CREATE INDEX IX_Category_Level ON CategoryMasterLayer2(CategoryLevel);
END

-- Self-referencing foreign key will be added in constraints file

-- Cleaned Grants Table
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='CleanedGrantsLayer2' AND xtype='U')
CREATE TABLE CleanedGrantsLayer2 (
    GrantID INT IDENTITY(1,1) PRIMARY KEY,
    OpportunityID NVARCHAR(100) NOT NULL UNIQUE,
    Title NVARCHAR(500) NOT NULL,
    ShortTitle NVARCHAR(100),
    AgencyID INT,
    CategoryID INT,
    SubcategoryID INT,
    
    -- Description and details
    Description NTEXT,
    ShortDescription NVARCHAR(500),
    EligibilityRequirements NTEXT,
    ApplicationRequirements NTEXT,
    
    -- Dates
    PostedDate DATE,
    CloseDate DATE,
    OpenDate DATE,
    LastUpdatedDate DATE,
    
    -- Funding information
    AwardCeiling MONEY,
    AwardFloor MONEY,
    EstimatedTotalFunding MONEY,
    ExpectedAwards INT,
    AverageFundingAmount AS (
        CASE 
            WHEN ExpectedAwards > 0 AND EstimatedTotalFunding > 0 
            THEN EstimatedTotalFunding / ExpectedAwards 
            ELSE AwardCeiling 
        END
    ),
    
    -- Grant characteristics
    InstrumentType NVARCHAR(100),
    FundingActivity NVARCHAR(200),
    CFDANumbers NVARCHAR(200),
    EligibilityCode NVARCHAR(100),
    
    -- Contact and links
    ContactEmail NVARCHAR(200),
    ContactPhone NVARCHAR(50),
    AdditionalInfoURL NVARCHAR(500),
    ApplicationURL NVARCHAR(500),
    
    -- Calculated fields
    GrantsGovURL AS ('https://www.grants.gov/search-results-detail/' + OpportunityID),
    DaysUntilDeadline AS (DATEDIFF(day, GETDATE(), CloseDate)),
    Status AS (
        CASE 
            WHEN CloseDate < GETDATE() THEN 'Closed'
            WHEN DATEDIFF(day, GETDATE(), CloseDate) <= 7 AND CloseDate >= GETDATE() THEN 'Closing Soon'
            WHEN DATEDIFF(day, GETDATE(), CloseDate) <= 30 AND CloseDate >= GETDATE() THEN 'Closing This Month'
            WHEN PostedDate > GETDATE() THEN 'Upcoming'
            ELSE 'Open'
        END
    ),
    IsUrgent AS (
        CASE 
            WHEN DATEDIFF(day, GETDATE(), CloseDate) <= 7 AND CloseDate >= GETDATE() THEN 1
            ELSE 0
        END
    ),
    
    -- Data quality and processing
    DataQualityScore DECIMAL(3,2) DEFAULT 5.0,
    ValidationStatus NVARCHAR(50) DEFAULT 'Pending',
    ProcessingNotes NVARCHAR(500),
    QualityFlags NVARCHAR(200),
    
    -- Audit fields
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETUTCDATE(),
    LastUpdated DATETIME2 DEFAULT GETUTCDATE(),
    ProcessedBy NVARCHAR(100) DEFAULT 'DataMigrator_Function',
    OriginalRowID INT
);

-- Foreign keys will be added in constraints file

-- Create grants indexes
IF EXISTS (SELECT * FROM sysobjects WHERE name='CleanedGrantsLayer2' AND xtype='U')
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanedGrants_OpportunityID')
        CREATE INDEX IX_CleanedGrants_OpportunityID ON CleanedGrantsLayer2(OpportunityID);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanedGrants_AgencyID')
        CREATE INDEX IX_CleanedGrants_AgencyID ON CleanedGrantsLayer2(AgencyID);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanedGrants_CategoryID')
        CREATE INDEX IX_CleanedGrants_CategoryID ON CleanedGrantsLayer2(CategoryID);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanedGrants_PostedDate')
        CREATE INDEX IX_CleanedGrants_PostedDate ON CleanedGrantsLayer2(PostedDate);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanedGrants_CloseDate')
        CREATE INDEX IX_CleanedGrants_CloseDate ON CleanedGrantsLayer2(CloseDate);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanedGrants_Status')
        CREATE INDEX IX_CleanedGrants_Status ON CleanedGrantsLayer2(Status);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanedGrants_FundingAmount')
        CREATE INDEX IX_CleanedGrants_FundingAmount ON CleanedGrantsLayer2(AwardCeiling);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CleanedGrants_Active')
        CREATE INDEX IX_CleanedGrants_Active ON CleanedGrantsLayer2(IsActive, CloseDate);
END

PRINT '✅ Layer 2 (Cleaned & Normalized Data) created successfully';
GO
