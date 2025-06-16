-- ===================================
-- CLEAN DATABASE ARCHITECTURE - 3-LAYER DESIGN
-- Remove all unnecessary tables and create clean structure
-- ===================================

USE GrantsGovDB;
GO

PRINT '===============================================';
PRINT 'Starting database cleanup and restructuring...';
PRINT 'Creating clean 3-layer architecture';
PRINT '===============================================';

-- Drop all unnecessary tables and objects
PRINT 'Dropping unnecessary tables...';

-- Drop all Layer2 tables (except the ones we want to keep)
IF OBJECT_ID('AgencyMasterLayer2', 'U') IS NOT NULL DROP TABLE AgencyMasterLayer2;
IF OBJECT_ID('CategoryMasterLayer2', 'U') IS NOT NULL DROP TABLE CategoryMasterLayer2;
IF OBJECT_ID('EligibilityMasterLayer2', 'U') IS NOT NULL DROP TABLE EligibilityMasterLayer2;
IF OBJECT_ID('GeographicCoverageMasterLayer2', 'U') IS NOT NULL DROP TABLE GeographicCoverageMasterLayer2;
IF OBJECT_ID('CleanedGrantsLayer2', 'U') IS NOT NULL DROP TABLE CleanedGrantsLayer2;
IF OBJECT_ID('GrantEligibilityLayer2', 'U') IS NOT NULL DROP TABLE GrantEligibilityLayer2;
IF OBJECT_ID('GrantGeographicCoverageLayer2', 'U') IS NOT NULL DROP TABLE GrantGeographicCoverageLayer2;
GO

-- Drop all Layer3 tables (we'll recreate our own)
IF OBJECT_ID('AgencyStatsLayer3', 'U') IS NOT NULL DROP TABLE AgencyStatsLayer3;
IF OBJECT_ID('GrantBusinessViewLayer3', 'U') IS NOT NULL DROP TABLE GrantBusinessViewLayer3;
IF OBJECT_ID('SuccessFactorsLayer3', 'U') IS NOT NULL DROP TABLE SuccessFactorsLayer3;
GO

-- Drop lookup tables we don't need
IF OBJECT_ID('OpportunityTypesMaster', 'U') IS NOT NULL DROP TABLE OpportunityTypesMaster;
IF OBJECT_ID('IndustriesMaster', 'U') IS NOT NULL DROP TABLE IndustriesMaster;
IF OBJECT_ID('UNSDGMaster', 'U') IS NOT NULL DROP TABLE UNSDGMaster;
GO

-- Drop raw layer tables (except Grants which we'll rename)
IF OBJECT_ID('RawGrantsLayer1', 'U') IS NOT NULL DROP TABLE RawGrantsLayer1;
GO

-- Drop monitoring tables
IF OBJECT_ID('monitoring.APIRequestLog', 'U') IS NOT NULL DROP TABLE monitoring.APIRequestLog;
IF OBJECT_ID('monitoring.ApplicationLog', 'U') IS NOT NULL DROP TABLE monitoring.ApplicationLog;
GO

-- Drop all views
DECLARE @sql NVARCHAR(MAX) = '';
SELECT @sql = @sql + 'DROP VIEW ' + QUOTENAME(SCHEMA_NAME(schema_id)) + '.' + QUOTENAME(name) + ';' + CHAR(13)
FROM sys.views 
WHERE name NOT IN ('DeploymentLog'); -- Keep deployment log for tracking

EXEC sp_executesql @sql;
GO

-- Drop all stored procedures except system ones
DECLARE @sql2 NVARCHAR(MAX) = '';
SELECT @sql2 = @sql2 + 'DROP PROCEDURE ' + QUOTENAME(SCHEMA_NAME(schema_id)) + '.' + QUOTENAME(name) + ';' + CHAR(13)
FROM sys.procedures 
WHERE name NOT LIKE 'sp_%' AND name NOT LIKE 'LogDeploymentStep';

EXEC sp_executesql @sql2;
GO

PRINT 'Cleanup completed - unnecessary objects removed';
GO

-- Step 1: Rename existing tables to new architecture
PRINT 'Renaming tables to new architecture...';

-- Rename Grants to RawGrantsLayer1
IF OBJECT_ID('Grants', 'U') IS NOT NULL
BEGIN
    EXEC sp_rename 'Grants', 'RawGrantsLayer1';
    PRINT 'Renamed Grants to RawGrantsLayer1';
END
GO

-- Rename OpportunitiesLayer1 to RunweiFormatLayer2
IF OBJECT_ID('OpportunitiesLayer1', 'U') IS NOT NULL
BEGIN
    EXEC sp_rename 'OpportunitiesLayer1', 'RunweiFormatLayer2';
    PRINT 'Renamed OpportunitiesLayer1 to RunweiFormatLayer2';
END
GO

PRINT 'Table renaming completed';
GO

-- Step 2: Create the third layer - BusinessIntelligenceLayer3
PRINT 'Creating BusinessIntelligenceLayer3...';

CREATE TABLE BusinessIntelligenceLayer3 (
    -- Primary Key
    AnalyticsID BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Reference to source data
    OpportunityID BIGINT NOT NULL,
    
    -- Business Intelligence Metrics
    CompetitiveScore DECIMAL(5,2) NULL, -- 0-100 score based on funding amount and deadline
    OpportunityValue NVARCHAR(50) NULL, -- 'High', 'Medium', 'Low'
    UrgencyRating NVARCHAR(50) NULL, -- 'Critical', 'High', 'Medium', 'Low'
    RecommendationLevel NVARCHAR(100) NULL, -- Strategic recommendation
    
    -- Market Analysis
    IndustryTrend NVARCHAR(100) NULL, -- 'Growing', 'Stable', 'Declining'
    FundingTrend NVARCHAR(100) NULL, -- 'Increasing', 'Stable', 'Decreasing'
    CompetitionLevel NVARCHAR(50) NULL, -- 'High', 'Medium', 'Low'
    
    -- Strategic Insights
    StrategicFit DECIMAL(3,2) NULL, -- 1-5 rating for strategic alignment
    ROIProjection DECIMAL(10,2) NULL, -- Projected return on investment
    SuccessProbability DECIMAL(5,2) NULL, -- 0-100% chance of success
    
    -- Engagement Metrics
    ViewCount INT DEFAULT 0,
    LastViewedDate DATETIME2(7) NULL,
    BookmarkedCount INT DEFAULT 0,
    ApplicationSubmissions INT DEFAULT 0,
    
    -- AI-Powered Insights
    AIRecommendationScore DECIMAL(5,2) NULL, -- 0-100 AI-generated recommendation
    SimilarOpportunities NVARCHAR(MAX) NULL, -- JSON array of similar opportunity IDs
    KeywordRelevance NVARCHAR(MAX) NULL, -- JSON object with keyword matching scores
    
    -- Performance Tracking
    EngagementScore DECIMAL(5,2) NULL, -- Overall engagement metric
    ConversionRate DECIMAL(5,4) NULL, -- Views to applications ratio
    
    -- System Fields
    CreatedDate DATETIME2(7) NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2(7) NOT NULL DEFAULT GETDATE(),
    IsActive BIT NOT NULL DEFAULT 1,
    
    -- Foreign Key Constraint
    CONSTRAINT FK_BusinessIntelligence_Opportunity 
        FOREIGN KEY (OpportunityID) REFERENCES RunweiFormatLayer2(OpportunityID)
        ON DELETE CASCADE
);

-- Create indexes for BusinessIntelligenceLayer3
CREATE NONCLUSTERED INDEX IX_BusinessIntelligence_OpportunityID ON BusinessIntelligenceLayer3 (OpportunityID);
CREATE NONCLUSTERED INDEX IX_BusinessIntelligence_Value ON BusinessIntelligenceLayer3 (OpportunityValue);
CREATE NONCLUSTERED INDEX IX_BusinessIntelligence_Urgency ON BusinessIntelligenceLayer3 (UrgencyRating);
CREATE NONCLUSTERED INDEX IX_BusinessIntelligence_Score ON BusinessIntelligenceLayer3 (CompetitiveScore);
CREATE NONCLUSTERED INDEX IX_BusinessIntelligence_Active ON BusinessIntelligenceLayer3 (IsActive) WHERE IsActive = 1;

PRINT 'BusinessIntelligenceLayer3 created successfully';
GO

-- Create clean API schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'api')
BEGIN
    EXEC('CREATE SCHEMA api');
    PRINT 'Created api schema';
END
GO

-- Step 3: Create essential views for the 3-layer architecture
PRINT 'Creating essential views...';

-- Main API View combining all layers
CREATE VIEW api.vw_ComprehensiveOpportunities AS
SELECT 
    -- Layer 2: Core Opportunity Data
    r2.OpportunityID,
    r2.OpportunityURL,
    r2.Title,
    r2.ShortDescription,
    r2.LongDescription,
    r2.Deadline,
    r2.Status,
    r2.UrgencyLevel,
    r2.DaysUntilDeadline,
    r2.AwardValue,
    r2.CashAward,
    r2.FundingTier,
    r2.OpportunityType,
    r2.Industry,
    r2.GlobalOpportunity,
    r2.DirectLinkToApplyURL,
    r2.ContactEmailForOpportunity,
    r2.OpportunityRating,
    r2.DataQualityScore,
    r2.CreatedDate as OpportunityCreated,
    
    -- Layer 3: Business Intelligence
    bi.CompetitiveScore,
    bi.OpportunityValue,
    bi.UrgencyRating,
    bi.RecommendationLevel,
    bi.IndustryTrend,
    bi.FundingTrend,
    bi.CompetitionLevel,
    bi.StrategicFit,
    bi.ROIProjection,
    bi.SuccessProbability,
    bi.ViewCount,
    bi.BookmarkedCount,
    bi.ApplicationSubmissions,
    bi.AIRecommendationScore,
    bi.EngagementScore,
    bi.ConversionRate
    
FROM RunweiFormatLayer2 r2
LEFT JOIN BusinessIntelligenceLayer3 bi ON r2.OpportunityID = bi.OpportunityID
WHERE r2.IsActive = 1 AND (bi.IsActive = 1 OR bi.IsActive IS NULL);
GO

-- Active High-Value Opportunities
CREATE VIEW api.vw_PriorityOpportunities AS
SELECT *
FROM api.vw_ComprehensiveOpportunities
WHERE Status IN ('Open', 'Closing Soon', 'Closing This Month')
    AND (AwardValue >= 100000 OR OpportunityValue = 'High')
    AND (UrgencyRating IN ('Critical', 'High') OR UrgencyLevel IN ('Critical', 'High'));
GO

-- Analytics Dashboard View
CREATE VIEW api.vw_AnalyticsDashboard AS
SELECT 
    COUNT(*) as TotalOpportunities,
    COUNT(CASE WHEN Status IN ('Open', 'Closing Soon', 'Closing This Month') THEN 1 END) as ActiveOpportunities,
    COUNT(CASE WHEN AwardValue >= 500000 THEN 1 END) as HighValueOpportunities,
    COUNT(CASE WHEN UrgencyLevel = 'Critical' THEN 1 END) as CriticalOpportunities,
    COUNT(CASE WHEN GlobalOpportunity = 1 THEN 1 END) as GlobalOpportunities,
    
    FORMAT(SUM(AwardValue), 'C0') as TotalFundingAvailable,
    FORMAT(AVG(AwardValue), 'C0') as AverageFundingAmount,
    
    AVG(OpportunityRating) as AverageRating,
    AVG(DataQualityScore) as AverageDataQuality,
    AVG(CompetitiveScore) as AverageCompetitiveScore,
    AVG(SuccessProbability) as AverageSuccessProbability,
    
    SUM(ViewCount) as TotalViews,
    SUM(BookmarkedCount) as TotalBookmarks,
    SUM(ApplicationSubmissions) as TotalApplications
FROM api.vw_ComprehensiveOpportunities;
GO

PRINT 'Essential views created successfully';
GO

-- Step 4: Create essential stored procedures
PRINT 'Creating essential stored procedures...';

-- Procedure to add business intelligence data
CREATE PROCEDURE sp_UpdateBusinessIntelligence
    @OpportunityID BIGINT,
    @CompetitiveScore DECIMAL(5,2) = NULL,
    @OpportunityValue NVARCHAR(50) = NULL,
    @UrgencyRating NVARCHAR(50) = NULL,
    @RecommendationLevel NVARCHAR(100) = NULL,
    @StrategicFit DECIMAL(3,2) = NULL,
    @SuccessProbability DECIMAL(5,2) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Upsert business intelligence data
    IF EXISTS (SELECT 1 FROM BusinessIntelligenceLayer3 WHERE OpportunityID = @OpportunityID)
    BEGIN
        UPDATE BusinessIntelligenceLayer3 
        SET 
            CompetitiveScore = ISNULL(@CompetitiveScore, CompetitiveScore),
            OpportunityValue = ISNULL(@OpportunityValue, OpportunityValue),
            UrgencyRating = ISNULL(@UrgencyRating, UrgencyRating),
            RecommendationLevel = ISNULL(@RecommendationLevel, RecommendationLevel),
            StrategicFit = ISNULL(@StrategicFit, StrategicFit),
            SuccessProbability = ISNULL(@SuccessProbability, SuccessProbability),
            ModifiedDate = GETDATE()
        WHERE OpportunityID = @OpportunityID;
    END
    ELSE
    BEGIN
        INSERT INTO BusinessIntelligenceLayer3 (
            OpportunityID, CompetitiveScore, OpportunityValue, UrgencyRating,
            RecommendationLevel, StrategicFit, SuccessProbability
        )
        VALUES (
            @OpportunityID, @CompetitiveScore, @OpportunityValue, @UrgencyRating,
            @RecommendationLevel, @StrategicFit, @SuccessProbability
        );
    END
END;
GO

-- Procedure to track engagement
CREATE PROCEDURE sp_TrackEngagement
    @OpportunityID BIGINT,
    @ActionType NVARCHAR(50) -- 'View', 'Bookmark', 'Application'
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Ensure business intelligence record exists
    IF NOT EXISTS (SELECT 1 FROM BusinessIntelligenceLayer3 WHERE OpportunityID = @OpportunityID)
    BEGIN
        INSERT INTO BusinessIntelligenceLayer3 (OpportunityID) VALUES (@OpportunityID);
    END
    
    -- Update engagement metrics
    IF @ActionType = 'View'
    BEGIN
        UPDATE BusinessIntelligenceLayer3 
        SET ViewCount = ViewCount + 1, 
            LastViewedDate = GETDATE(),
            ModifiedDate = GETDATE()
        WHERE OpportunityID = @OpportunityID;
    END
    ELSE IF @ActionType = 'Bookmark'
    BEGIN
        UPDATE BusinessIntelligenceLayer3 
        SET BookmarkedCount = BookmarkedCount + 1,
            ModifiedDate = GETDATE()
        WHERE OpportunityID = @OpportunityID;
    END
    ELSE IF @ActionType = 'Application'
    BEGIN
        UPDATE BusinessIntelligenceLayer3 
        SET ApplicationSubmissions = ApplicationSubmissions + 1,
            ModifiedDate = GETDATE()
        WHERE OpportunityID = @OpportunityID;
    END
    
    -- Recalculate engagement score
    UPDATE BusinessIntelligenceLayer3 
    SET EngagementScore = 
        (ViewCount * 1.0) + 
        (BookmarkedCount * 5.0) + 
        (ApplicationSubmissions * 20.0),
    ConversionRate = 
        CASE WHEN ViewCount > 0 
             THEN CAST(ApplicationSubmissions AS DECIMAL(10,4)) / ViewCount 
             ELSE 0 
        END
    WHERE OpportunityID = @OpportunityID;
END;
GO

PRINT 'Essential stored procedures created successfully';
GO

PRINT '===============================================';
PRINT 'Database restructuring completed successfully!';
PRINT '';
PRINT 'CLEAN 3-LAYER ARCHITECTURE:';
PRINT '1. RawGrantsLayer1 - Original grants data';
PRINT '2. RunweiFormatLayer2 - Your opportunity format';
PRINT '3. BusinessIntelligenceLayer3 - Analytics and insights';
PRINT '';
PRINT 'API ENDPOINTS AVAILABLE:';
PRINT '- api.vw_ComprehensiveOpportunities';
PRINT '- api.vw_PriorityOpportunities';
PRINT '- api.vw_AnalyticsDashboard';
PRINT '';
PRINT 'Ready for production use!';
PRINT '===============================================';
GO