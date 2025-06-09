-- ===================================
-- GRANTS.GOV API AZURE - COMPLETE DATABASE SCHEMA
-- Three-Layer Architecture for Grant Data Management
-- ===================================

-- ===================================
-- LAYER 1: RAW DATA TABLES
-- ===================================

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
    ValidationErrors NVARCHAR(1000),
    
    -- Indexes for performance
    INDEX IX_RawGrants_OpportunityNumber (OpportunityNumber),
    INDEX IX_RawGrants_AgencyName (AgencyName),
    INDEX IX_RawGrants_ImportDate (ImportDate)
);

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

-- ===================================
-- LAYER 2: CLEANED & NORMALIZED DATA
-- ===================================

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
    LastValidated DATETIME2,
    
    -- Indexes
    INDEX IX_Agency_Name (AgencyName),
    INDEX IX_Agency_Code (AgencyCode),
    INDEX IX_Agency_Type (AgencyType)
);

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
    LastUpdated DATETIME2 DEFAULT GETUTCDATE(),
    
    FOREIGN KEY (ParentCategoryID) REFERENCES CategoryMasterLayer2(CategoryID),
    
    -- Indexes
    INDEX IX_Category_Name (CategoryName),
    INDEX IX_Category_Group (CategoryGroup),
    INDEX IX_Category_Level (CategoryLevel)
);

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
    OriginalRowID INT,
    
    FOREIGN KEY (AgencyID) REFERENCES AgencyMasterLayer2(AgencyID),
    FOREIGN KEY (CategoryID) REFERENCES CategoryMasterLayer2(CategoryID),
    
    -- Performance indexes
    INDEX IX_CleanedGrants_OpportunityID (OpportunityID),
    INDEX IX_CleanedGrants_AgencyID (AgencyID),
    INDEX IX_CleanedGrants_CategoryID (CategoryID),
    INDEX IX_CleanedGrants_PostedDate (PostedDate),
    INDEX IX_CleanedGrants_CloseDate (CloseDate),
    INDEX IX_CleanedGrants_Status (Status),
    INDEX IX_CleanedGrants_FundingAmount (AwardCeiling),
    INDEX IX_CleanedGrants_Active (IsActive, CloseDate)
);

-- ===================================
-- LAYER 3: BUSINESS VIEWS & ANALYTICS
-- ===================================

-- Comprehensive Grant Business View
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='GrantBusinessViewLayer3' AND xtype='U')
CREATE TABLE GrantBusinessViewLayer3 (
    ViewID INT IDENTITY(1,1) PRIMARY KEY,
    GrantID INT NOT NULL,
    OpportunityID NVARCHAR(100) NOT NULL,
    Title NVARCHAR(500),
    AgencyName NVARCHAR(200),
    CategoryName NVARCHAR(200),
    Status NVARCHAR(50),
    UrgencyLevel NVARCHAR(20),
    FundingTier NVARCHAR(20),
    CompetitionLevel NVARCHAR(20),
    
    -- Enhanced calculations
    DaysUntilDeadline INT,
    FundingAmountTier AS (
        CASE 
            WHEN AwardCeiling >= 1000000 THEN 'Large ($1M+)'
            WHEN AwardCeiling >= 500000 THEN 'Medium ($500K-$1M)'
            WHEN AwardCeiling >= 100000 THEN 'Small ($100K-$500K)'
            WHEN AwardCeiling > 0 THEN 'Micro (<$100K)'
            ELSE 'Unknown'
        END
    ),
    ApplicationComplexity AS (
        CASE 
            WHEN AwardCeiling >= 1000000 THEN 'High'
            WHEN AwardCeiling >= 250000 THEN 'Medium'
            ELSE 'Low'
        END
    ),
    
    -- All financial fields
    AwardCeiling MONEY,
    AwardFloor MONEY,
    EstimatedTotalFunding MONEY,
    ExpectedAwards INT,
    
    -- All date fields
    PostedDate DATE,
    CloseDate DATE,
    
    -- URLs and contact
    GrantsGovURL NVARCHAR(600),
    AdditionalInfoURL NVARCHAR(500),
    ContactEmail NVARCHAR(200),
    
    -- Refresh tracking
    RefreshDate DATETIME2 DEFAULT GETUTCDATE(),
    DataAsOfDate DATETIME2 DEFAULT GETUTCDATE(),
    
    FOREIGN KEY (GrantID) REFERENCES CleanedGrantsLayer2(GrantID),
    
    -- Optimized indexes for common queries
    INDEX IX_BusinessView_Status (Status),
    INDEX IX_BusinessView_Agency (AgencyName),
    INDEX IX_BusinessView_Category (CategoryName),
    INDEX IX_BusinessView_Urgency (UrgencyLevel, CloseDate),
    INDEX IX_BusinessView_Funding (FundingTier, AwardCeiling)
);

-- Agency Statistics & Performance
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='AgencyStatsLayer3' AND xtype='U')
CREATE TABLE AgencyStatsLayer3 (
    StatsID INT IDENTITY(1,1) PRIMARY KEY,
    AgencyID INT NOT NULL,
    AgencyName NVARCHAR(200),
    
    -- Grant counts
    TotalGrants INT DEFAULT 0,
    ActiveGrants INT DEFAULT 0,
    ClosedGrants INT DEFAULT 0,
    UpcomingGrants INT DEFAULT 0,
    
    -- Funding statistics
    TotalFunding MONEY DEFAULT 0,
    AvgFunding MONEY DEFAULT 0,
    MinFunding MONEY DEFAULT 0,
    MaxFunding MONEY DEFAULT 0,
    MedianFunding MONEY DEFAULT 0,
    
    -- Performance metrics
    AvgApplicationPeriod INT DEFAULT 0,
    GrantsPerMonth DECIMAL(10,2) DEFAULT 0,
    FundingGrowthRate DECIMAL(5,2) DEFAULT 0,
    
    -- Diversity metrics
    CategoryCount INT DEFAULT 0,
    PrimaryCategory NVARCHAR(200),
    CategoryDistribution NVARCHAR(500),
    
    -- Refresh tracking
    LastUpdated DATETIME2 DEFAULT GETUTCDATE(),
    StatsAsOfDate DATETIME2 DEFAULT GETUTCDATE(),
    
    FOREIGN KEY (AgencyID) REFERENCES AgencyMasterLayer2(AgencyID),
    
    INDEX IX_AgencyStats_AgencyID (AgencyID),
    INDEX IX_AgencyStats_TotalFunding (TotalFunding DESC),
    INDEX IX_AgencyStats_TotalGrants (TotalGrants DESC)
);

-- Category Statistics & Insights
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='CategoryStatsLayer3' AND xtype='U')
CREATE TABLE CategoryStatsLayer3 (
    StatsID INT IDENTITY(1,1) PRIMARY KEY,
    CategoryID INT NOT NULL,
    CategoryName NVARCHAR(200),
    CategoryGroup NVARCHAR(100),
    
    -- Grant metrics
    TotalGrants INT DEFAULT 0,
    ActiveGrants INT DEFAULT 0,
    CompletedGrants INT DEFAULT 0,
    
    -- Funding analysis
    TotalFunding MONEY DEFAULT 0,
    AvgFunding MONEY DEFAULT 0,
    FundingVariance DECIMAL(15,2) DEFAULT 0,
    
    -- Agency diversity
    AgencyCount INT DEFAULT 0,
    TopAgency NVARCHAR(200),
    AgencyConcentration DECIMAL(5,2) DEFAULT 0,
    
    -- Timing patterns
    AvgApplicationWindow INT DEFAULT 0,
    PeakPostingMonth INT DEFAULT 0,
    SeasonalityScore DECIMAL(3,2) DEFAULT 0,
    
    -- Competition metrics
    AvgCompetitorsPerGrant DECIMAL(10,2) DEFAULT 0,
    SuccessRate DECIMAL(5,2) DEFAULT 0,
    
    -- Refresh tracking
    LastUpdated DATETIME2 DEFAULT GETUTCDATE(),
    StatsAsOfDate DATETIME2 DEFAULT GETUTCDATE(),
    
    FOREIGN KEY (CategoryID) REFERENCES CategoryMasterLayer2(CategoryID),
    
    INDEX IX_CategoryStats_CategoryID (CategoryID),
    INDEX IX_CategoryStats_TotalFunding (TotalFunding DESC),
    INDEX IX_CategoryStats_Group (CategoryGroup)
);

-- Funding Analytics & Trends
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='FundingAnalyticsLayer3' AND xtype='U')
CREATE TABLE FundingAnalyticsLayer3 (
    AnalyticsID INT IDENTITY(1,1) PRIMARY KEY,
    AnalysisDate DATE NOT NULL,
    AnalysisType NVARCHAR(50) NOT NULL, -- Daily, Weekly, Monthly, Quarterly
    
    -- Opportunity metrics
    TotalOpportunities INT DEFAULT 0,
    NewOpportunities INT DEFAULT 0,
    ClosingOpportunities INT DEFAULT 0,
    
    -- Funding metrics
    TotalFunding MONEY DEFAULT 0,
    NewFunding MONEY DEFAULT 0,
    AvgFunding MONEY DEFAULT 0,
    MedianFunding MONEY DEFAULT 0,
    
    -- Distribution analysis
    LargeFundingCount INT DEFAULT 0, -- >$1M
    MediumFundingCount INT DEFAULT 0, -- $100K-$1M
    SmallFundingCount INT DEFAULT 0, -- <$100K
    
    -- Trend indicators
    FundingTrend NVARCHAR(20), -- Increasing, Decreasing, Stable
    OpportunityTrend NVARCHAR(20),
    CompetitionIndex DECIMAL(5,2) DEFAULT 0,
    
    -- Agency and category insights
    TopFundingAgency NVARCHAR(200),
    TopFundingCategory NVARCHAR(200),
    DiversityIndex DECIMAL(3,2) DEFAULT 0,
    
    CreatedDate DATETIME2 DEFAULT GETUTCDATE(),
    
    -- Unique constraint for analysis periods
    UNIQUE (AnalysisDate, AnalysisType),
    
    INDEX IX_Analytics_Date (AnalysisDate DESC),
    INDEX IX_Analytics_Type (AnalysisType),
    INDEX IX_Analytics_Funding (TotalFunding DESC)
);

-- ===================================
-- STORED PROCEDURES & FUNCTIONS
-- ===================================

-- Refresh Business Views
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'RefreshBusinessViews')
    DROP PROCEDURE RefreshBusinessViews;
GO

CREATE PROCEDURE RefreshBusinessViews
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Refresh Grant Business View
    MERGE GrantBusinessViewLayer3 AS target
    USING (
        SELECT 
            g.GrantID,
            g.OpportunityID,
            g.Title,
            a.AgencyName,
            c.CategoryName,
            g.Status,
            CASE 
                WHEN g.DaysUntilDeadline <= 7 AND g.DaysUntilDeadline >= 0 THEN 'Critical'
                WHEN g.DaysUntilDeadline <= 30 AND g.DaysUntilDeadline >= 0 THEN 'High'
                WHEN g.DaysUntilDeadline <= 60 AND g.DaysUntilDeadline >= 0 THEN 'Medium'
                ELSE 'Low'
            END as UrgencyLevel,
            CASE 
                WHEN g.AwardCeiling >= 1000000 THEN 'Large'
                WHEN g.AwardCeiling >= 500000 THEN 'Medium'
                WHEN g.AwardCeiling >= 100000 THEN 'Small'
                ELSE 'Micro'
            END as FundingTier,
            CASE 
                WHEN g.ExpectedAwards <= 5 AND g.AwardCeiling >= 500000 THEN 'High'
                WHEN g.ExpectedAwards <= 20 THEN 'Medium'
                ELSE 'Low'
            END as CompetitionLevel,
            g.DaysUntilDeadline,
            g.AwardCeiling,
            g.AwardFloor,
            g.EstimatedTotalFunding,
            g.ExpectedAwards,
            g.PostedDate,
            g.CloseDate,
            g.GrantsGovURL,
            g.AdditionalInfoURL,
            g.ContactEmail
        FROM CleanedGrantsLayer2 g
        LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
        LEFT JOIN CategoryMasterLayer2 c ON g.CategoryID = c.CategoryID
        WHERE g.IsActive = 1
    ) AS source ON target.GrantID = source.GrantID
    
    WHEN MATCHED THEN
        UPDATE SET
            OpportunityID = source.OpportunityID,
            Title = source.Title,
            AgencyName = source.AgencyName,
            CategoryName = source.CategoryName,
            Status = source.Status,
            UrgencyLevel = source.UrgencyLevel,
            FundingTier = source.FundingTier,
            CompetitionLevel = source.CompetitionLevel,
            DaysUntilDeadline = source.DaysUntilDeadline,
            AwardCeiling = source.AwardCeiling,
            AwardFloor = source.AwardFloor,
            EstimatedTotalFunding = source.EstimatedTotalFunding,
            ExpectedAwards = source.ExpectedAwards,
            PostedDate = source.PostedDate,
            CloseDate = source.CloseDate,
            GrantsGovURL = source.GrantsGovURL,
            AdditionalInfoURL = source.AdditionalInfoURL,
            ContactEmail = source.ContactEmail,
            RefreshDate = GETUTCDATE()
    
    WHEN NOT MATCHED THEN
        INSERT (GrantID, OpportunityID, Title, AgencyName, CategoryName, Status, 
                UrgencyLevel, FundingTier, CompetitionLevel, DaysUntilDeadline,
                AwardCeiling, AwardFloor, EstimatedTotalFunding, ExpectedAwards,
                PostedDate, CloseDate, GrantsGovURL, AdditionalInfoURL, ContactEmail)
        VALUES (source.GrantID, source.OpportunityID, source.Title, source.AgencyName, 
                source.CategoryName, source.Status, source.UrgencyLevel, source.FundingTier,
                source.CompetitionLevel, source.DaysUntilDeadline, source.AwardCeiling,
                source.AwardFloor, source.EstimatedTotalFunding, source.ExpectedAwards,
                source.PostedDate, source.CloseDate, source.GrantsGovURL, 
                source.AdditionalInfoURL, source.ContactEmail);
    
    -- Refresh Agency Stats
    MERGE AgencyStatsLayer3 AS target
    USING (
        SELECT 
            a.AgencyID,
            a.AgencyName,
            COUNT(g.GrantID) as TotalGrants,
            SUM(CASE WHEN g.CloseDate >= GETDATE() THEN 1 ELSE 0 END) as ActiveGrants,
            SUM(CASE WHEN g.CloseDate < GETDATE() THEN 1 ELSE 0 END) as ClosedGrants,
            SUM(CASE WHEN g.PostedDate > GETDATE() THEN 1 ELSE 0 END) as UpcomingGrants,
            ISNULL(SUM(g.AwardCeiling), 0) as TotalFunding,
            ISNULL(AVG(g.AwardCeiling), 0) as AvgFunding,
            ISNULL(MIN(g.AwardCeiling), 0) as MinFunding,
            ISNULL(MAX(g.AwardCeiling), 0) as MaxFunding,
            COUNT(DISTINCT g.CategoryID) as CategoryCount,
            ISNULL(AVG(DATEDIFF(day, g.PostedDate, g.CloseDate)), 0) as AvgApplicationPeriod
        FROM AgencyMasterLayer2 a
        LEFT JOIN CleanedGrantsLayer2 g ON a.AgencyID = g.AgencyID AND g.IsActive = 1
        GROUP BY a.AgencyID, a.AgencyName
    ) AS source ON target.AgencyID = source.AgencyID
    
    WHEN MATCHED THEN
        UPDATE SET
            AgencyName = source.AgencyName,
            TotalGrants = source.TotalGrants,
            ActiveGrants = source.ActiveGrants,
            ClosedGrants = source.ClosedGrants,
            UpcomingGrants = source.UpcomingGrants,
            TotalFunding = source.TotalFunding,
            AvgFunding = source.AvgFunding,
            MinFunding = source.MinFunding,
            MaxFunding = source.MaxFunding,
            CategoryCount = source.CategoryCount,
            AvgApplicationPeriod = source.AvgApplicationPeriod,
            LastUpdated = GETUTCDATE()
    
    WHEN NOT MATCHED THEN
        INSERT (AgencyID, AgencyName, TotalGrants, ActiveGrants, ClosedGrants, UpcomingGrants,
                TotalFunding, AvgFunding, MinFunding, MaxFunding, CategoryCount, AvgApplicationPeriod)
        VALUES (source.AgencyID, source.AgencyName, source.TotalGrants, source.ActiveGrants,
                source.ClosedGrants, source.UpcomingGrants, source.TotalFunding, source.AvgFunding,
                source.MinFunding, source.MaxFunding, source.CategoryCount, source.AvgApplicationPeriod);
    
    -- Refresh Category Stats
    MERGE CategoryStatsLayer3 AS target
    USING (
        SELECT 
            c.CategoryID,
            c.CategoryName,
            c.CategoryGroup,
            COUNT(g.GrantID) as TotalGrants,
            SUM(CASE WHEN g.CloseDate >= GETDATE() THEN 1 ELSE 0 END) as ActiveGrants,
            ISNULL(AVG(g.AwardCeiling), 0) as AvgFunding,
            ISNULL(SUM(g.AwardCeiling), 0) as TotalFunding,
            COUNT(DISTINCT g.AgencyID) as AgencyCount
        FROM CategoryMasterLayer2 c
        LEFT JOIN CleanedGrantsLayer2 g ON c.CategoryID = g.CategoryID AND g.IsActive = 1
        GROUP BY c.CategoryID, c.CategoryName, c.CategoryGroup
    ) AS source ON target.CategoryID = source.CategoryID
    
    WHEN MATCHED THEN
        UPDATE SET
            CategoryName = source.CategoryName,
            CategoryGroup = source.CategoryGroup,
            TotalGrants = source.TotalGrants,
            ActiveGrants = source.ActiveGrants,
            AvgFunding = source.AvgFunding,
            TotalFunding = source.TotalFunding,
            AgencyCount = source.AgencyCount,
            LastUpdated = GETUTCDATE()
    
    WHEN NOT MATCHED THEN
        INSERT (CategoryID, CategoryName, CategoryGroup, TotalGrants, ActiveGrants, 
                AvgFunding, TotalFunding, AgencyCount)
        VALUES (source.CategoryID, source.CategoryName, source.CategoryGroup, source.TotalGrants, 
                source.ActiveGrants, source.AvgFunding, source.TotalFunding,
                source.AgencyCount);
END;
GO

-- Data Quality Assessment Function
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'FN' AND name = 'CalculateDataQuality')
    DROP FUNCTION CalculateDataQuality;
GO

CREATE FUNCTION CalculateDataQuality(@GrantID INT)
RETURNS DECIMAL(3,2)
AS
BEGIN
    DECLARE @Score DECIMAL(3,2) = 0;
    DECLARE @MaxScore DECIMAL(3,2) = 10;
    
    -- Check if grant exists
    IF NOT EXISTS (SELECT 1 FROM CleanedGrantsLayer2 WHERE GrantID = @GrantID)
        RETURN 0;
    
    -- Score based on completeness
    SELECT @Score = @Score +
        CASE WHEN Title IS NOT NULL AND LEN(Title) > 10 THEN 1.5 ELSE 0 END +
        CASE WHEN Description IS NOT NULL AND LEN(Description) > 50 THEN 1.5 ELSE 0 END +
        CASE WHEN AgencyID IS NOT NULL THEN 1.0 ELSE 0 END +
        CASE WHEN CategoryID IS NOT NULL THEN 1.0 ELSE 0 END +
        CASE WHEN PostedDate IS NOT NULL THEN 1.0 ELSE 0 END +
        CASE WHEN CloseDate IS NOT NULL THEN 1.0 ELSE 0 END +
        CASE WHEN AwardCeiling > 0 THEN 1.5 ELSE 0 END +
        CASE WHEN ContactEmail IS NOT NULL AND ContactEmail LIKE '%@%' THEN 1.0 ELSE 0 END +
        CASE WHEN CloseDate > PostedDate THEN 0.5 ELSE 0 END
    FROM CleanedGrantsLayer2
    WHERE GrantID = @GrantID;
    
    RETURN @Score;
END;
GO

-- ===================================
-- INITIAL DATA SETUP & CONSTRAINTS
-- ===================================

-- Add foreign key constraints if they don't exist
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_CleanedGrants_Agency')
    ALTER TABLE CleanedGrantsLayer2 
    ADD CONSTRAINT FK_CleanedGrants_Agency 
    FOREIGN KEY (AgencyID) REFERENCES AgencyMasterLayer2(AgencyID);

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_CleanedGrants_Category')
    ALTER TABLE CleanedGrantsLayer2 
    ADD CONSTRAINT FK_CleanedGrants_Category 
    FOREIGN KEY (CategoryID) REFERENCES CategoryMasterLayer2(CategoryID);

-- Create database views for common queries
CREATE OR ALTER VIEW vw_ActiveGrants AS
SELECT 
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
WHERE g.IsActive = 1 
AND g.CloseDate >= GETDATE()
AND g.Status IN ('Open', 'Closing Soon', 'Closing This Month');

CREATE OR ALTER VIEW vw_HighValueGrants AS
SELECT 
    g.OpportunityID,
    g.Title,
    a.AgencyName,
    c.CategoryName,
    g.AwardCeiling,
    g.EstimatedTotalFunding,
    g.ExpectedAwards,
    g.CloseDate,
    g.DaysUntilDeadline
FROM CleanedGrantsLayer2 g
LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
LEFT JOIN CategoryMasterLayer2 c ON g.CategoryID = c.CategoryID
WHERE g.IsActive = 1 
AND g.AwardCeiling >= 500000
AND g.CloseDate >= GETDATE();

CREATE OR ALTER VIEW vw_GrantsSummary AS
SELECT 
    COUNT(*) as TotalGrants,
    COUNT(CASE WHEN CloseDate >= GETDATE() THEN 1 END) as ActiveGrants,
    COUNT(CASE WHEN DaysUntilDeadline <= 7 AND CloseDate >= GETDATE() THEN 1 END) as ClosingSoon,
    COUNT(CASE WHEN AwardCeiling >= 1000000 THEN 1 END) as LargeFundingGrants,
    SUM(AwardCeiling) as TotalFundingAvailable,
    AVG(AwardCeiling) as AvgFundingAmount,
    COUNT(DISTINCT AgencyID) as UniqueAgencies,
    COUNT(DISTINCT CategoryID) as UniqueCategories
FROM CleanedGrantsLayer2
WHERE IsActive = 1;

-- ===================================
-- SAMPLE DATA INSERTION (FOR TESTING)
-- ===================================

-- Insert sample agencies if none exist
IF NOT EXISTS (SELECT 1 FROM AgencyMasterLayer2)
BEGIN
    INSERT INTO AgencyMasterLayer2 (AgencyName, AgencyCode, AgencyType, Department)
    VALUES 
    ('National Science Foundation', 'NSF', 'Federal', 'National Science Foundation'),
    ('Department of Health and Human Services', 'HHS', 'Federal', 'Health and Human Services'),
    ('Department of Education', 'ED', 'Federal', 'Education'),
    ('Environmental Protection Agency', 'EPA', 'Federal', 'Environmental Protection'),
    ('National Institutes of Health', 'NIH', 'Federal', 'Health and Human Services'),
    ('Department of Defense', 'DOD', 'Federal', 'Defense'),
    ('Department of Energy', 'DOE', 'Federal', 'Energy'),
    ('National Aeronautics and Space Administration', 'NASA', 'Federal', 'Space & Technology');
END

-- Insert sample categories if none exist
IF NOT EXISTS (SELECT 1 FROM CategoryMasterLayer2)
BEGIN
    INSERT INTO CategoryMasterLayer2 (CategoryName, CategoryCode, CategoryGroup, CategoryDescription)
    VALUES 
    ('Science and Technology', 'SCI_TECH', 'Science & Technology', 'Research and development in science and technology'),
    ('Health Research', 'HEALTH', 'Health', 'Medical and health-related research'),
    ('Education Programs', 'EDU', 'Education', 'Educational initiatives and programs'),
    ('Environmental Protection', 'ENV', 'Environment', 'Environmental conservation and protection'),
    ('Community Development', 'COMM_DEV', 'Community', 'Community development and social programs'),
    ('Arts and Culture', 'ARTS', 'Arts & Culture', 'Arts, culture, and humanities programs'),
    ('Infrastructure', 'INFRA', 'Infrastructure', 'Infrastructure development and maintenance'),
    ('Economic Development', 'ECON_DEV', 'Economic', 'Economic development and business programs');
END

-- ===================================
-- MAINTENANCE & MONITORING
-- ===================================

-- Create maintenance procedure
CREATE OR ALTER PROCEDURE sp_DatabaseMaintenance
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update statistics
    UPDATE STATISTICS AgencyMasterLayer2;
    UPDATE STATISTICS CategoryMasterLayer2;
    UPDATE STATISTICS CleanedGrantsLayer2;
    
    -- Clean up old analytics data (keep last 2 years)
    DELETE FROM FundingAnalyticsLayer3 
    WHERE AnalysisDate < DATEADD(YEAR, -2, GETDATE());
    
    -- Refresh all business views
    EXEC RefreshBusinessViews;
    
    -- Update agency grant counts
    UPDATE a 
    SET GrantCount = (
        SELECT COUNT(*) 
        FROM CleanedGrantsLayer2 g 
        WHERE g.AgencyID = a.AgencyID AND g.IsActive = 1
    ),
    TotalFunding = (
        SELECT ISNULL(SUM(AwardCeiling), 0) 
        FROM CleanedGrantsLayer2 g 
        WHERE g.AgencyID = a.AgencyID AND g.IsActive = 1
    ),
    LastUpdated = GETUTCDATE()
    FROM AgencyMasterLayer2 a;
    
    -- Update category grant counts
    UPDATE c 
    SET GrantCount = (
        SELECT COUNT(*) 
        FROM CleanedGrantsLayer2 g 
        WHERE g.CategoryID = c.CategoryID AND g.IsActive = 1
    ),
    TotalFunding = (
        SELECT ISNULL(SUM(AwardCeiling), 0) 
        FROM CleanedGrantsLayer2 g 
        WHERE g.CategoryID = c.CategoryID AND g.IsActive = 1
    ),
    LastUpdated = GETUTCDATE()
    FROM CategoryMasterLayer2 c;
    
    PRINT 'Database maintenance completed successfully';
END;
GO

-- ===================================
-- SECURITY & PERMISSIONS
-- ===================================

-- Create read-only user for reporting
-- Note: This should be run by database administrator
/*
CREATE USER [grants_reader] WITH PASSWORD = 'ReadOnlyPassword123!';
GRANT SELECT ON SCHEMA::dbo TO [grants_reader];

-- Create application user with limited permissions
CREATE USER [grants_app] WITH PASSWORD = 'AppPassword123!';
GRANT SELECT, INSERT, UPDATE ON AgencyMasterLayer2 TO [grants_app];
GRANT SELECT, INSERT, UPDATE ON CategoryMasterLayer2 TO [grants_app];
GRANT SELECT, INSERT, UPDATE ON CleanedGrantsLayer2 TO [grants_app];
GRANT SELECT ON GrantBusinessViewLayer3 TO [grants_app];
GRANT EXECUTE ON RefreshBusinessViews TO [grants_app];
*/

PRINT 'Database schema creation completed successfully!';
PRINT 'Ready for data migration from Azure Table Storage.';