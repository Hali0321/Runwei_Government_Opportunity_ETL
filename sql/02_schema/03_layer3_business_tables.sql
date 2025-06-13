-- ===================================
-- GRANTS.GOV API AZURE - LAYER 3: BUSINESS VIEWS & ANALYTICS
-- ===================================

USE GrantsGovDB;
GO

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
    DataAsOfDate DATETIME2 DEFAULT GETUTCDATE()
);

-- Create business view indexes
IF EXISTS (SELECT * FROM sysobjects WHERE name='GrantBusinessViewLayer3' AND xtype='U')
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_BusinessView_Status')
        CREATE INDEX IX_BusinessView_Status ON GrantBusinessViewLayer3(Status);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_BusinessView_Agency')
        CREATE INDEX IX_BusinessView_Agency ON GrantBusinessViewLayer3(AgencyName);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_BusinessView_Category')
        CREATE INDEX IX_BusinessView_Category ON GrantBusinessViewLayer3(CategoryName);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_BusinessView_Urgency')
        CREATE INDEX IX_BusinessView_Urgency ON GrantBusinessViewLayer3(UrgencyLevel, CloseDate);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_BusinessView_Funding')
        CREATE INDEX IX_BusinessView_Funding ON GrantBusinessViewLayer3(FundingTier, AwardCeiling);
END

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
    StatsAsOfDate DATETIME2 DEFAULT GETUTCDATE()
);

-- Create agency stats indexes
IF EXISTS (SELECT * FROM sysobjects WHERE name='AgencyStatsLayer3' AND xtype='U')
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_AgencyStats_AgencyID')
        CREATE INDEX IX_AgencyStats_AgencyID ON AgencyStatsLayer3(AgencyID);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_AgencyStats_TotalFunding')
        CREATE INDEX IX_AgencyStats_TotalFunding ON AgencyStatsLayer3(TotalFunding DESC);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_AgencyStats_TotalGrants')
        CREATE INDEX IX_AgencyStats_TotalGrants ON AgencyStatsLayer3(TotalGrants DESC);
END

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
    StatsAsOfDate DATETIME2 DEFAULT GETUTCDATE()
);

-- Create category stats indexes
IF EXISTS (SELECT * FROM sysobjects WHERE name='CategoryStatsLayer3' AND xtype='U')
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CategoryStats_CategoryID')
        CREATE INDEX IX_CategoryStats_CategoryID ON CategoryStatsLayer3(CategoryID);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CategoryStats_TotalFunding')
        CREATE INDEX IX_CategoryStats_TotalFunding ON CategoryStatsLayer3(TotalFunding DESC);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CategoryStats_Group')
        CREATE INDEX IX_CategoryStats_Group ON CategoryStatsLayer3(CategoryGroup);
END

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
    UNIQUE (AnalysisDate, AnalysisType)
);

-- Create funding analytics indexes
IF EXISTS (SELECT * FROM sysobjects WHERE name='FundingAnalyticsLayer3' AND xtype='U')
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Analytics_Date')
        CREATE INDEX IX_Analytics_Date ON FundingAnalyticsLayer3(AnalysisDate DESC);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Analytics_Type')
        CREATE INDEX IX_Analytics_Type ON FundingAnalyticsLayer3(AnalysisType);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Analytics_Funding')
        CREATE INDEX IX_Analytics_Funding ON FundingAnalyticsLayer3(TotalFunding DESC);
END

PRINT '✅ Layer 3 (Business Views & Analytics) created successfully';
GO
