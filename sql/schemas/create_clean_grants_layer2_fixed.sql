-- ===================================
-- CREATE CLEANGRANTSLAYER2 TABLE (FIXED VERSION)
-- Clean, standardized, and enriched grants data
-- ===================================

USE GrantsGovDB;
GO

-- Drop existing table if it exists (for development)
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'CleanGrantsLayer2')
BEGIN
    DROP TABLE CleanGrantsLayer2;
    PRINT '🗑️ Dropped existing CleanGrantsLayer2 table';
END

-- Create the clean and enriched Layer 2 table
CREATE TABLE CleanGrantsLayer2 (
    -- Primary Keys
    ID BIGINT IDENTITY(1,1) PRIMARY KEY,
    OpportunityNumber NVARCHAR(255) NOT NULL UNIQUE,
    
    -- Core Grant Information (Cleaned)
    Title NVARCHAR(1000) NOT NULL,
    Description NVARCHAR(MAX) NULL,
    OpportunityURL NVARCHAR(2000) NULL,
    
    -- Agency Information (Standardized)
    AgencyName NVARCHAR(500) NULL,
    AgencyCode NVARCHAR(100) NULL,
    
    -- Funding Information (Cleaned & Standardized)
    AwardValue DECIMAL(18,2) NULL,  -- Primary award amount (from AwardCeiling or AwardFloor)
    AwardCeiling DECIMAL(18,2) NULL,
    AwardFloor DECIMAL(18,2) NULL,
    EstimatedTotalFunding DECIMAL(18,2) NULL,
    ExpectedAwards INT NULL,
    FundingType NVARCHAR(255) NULL,
    
    -- Dates (Standardized to DATETIME2)
    Deadline DATETIME2 NULL,        -- Cleaned CloseDate
    PostedDate DATETIME2 NULL,      -- Cleaned PostedDate
    EstimatedPostDate DATETIME2 NULL,
    EstimatedDueDate DATETIME2 NULL,
    
    -- Categorization (Enhanced)
    Category NVARCHAR(500) NULL,
    OpportunityType NVARCHAR(100) NULL,  -- "Grant", "Fellowship", "Accelerator", etc.
    
    -- Eligibility (Cleaned & Standardized)
    Eligibility NVARCHAR(MAX) NULL,
    EligibilityCategory NVARCHAR(255) NULL,  -- "Individuals", "Nonprofits", "Startups", etc.
    
    -- Geographic Scope (Derived)
    CountriesEligible NVARCHAR(1000) NULL,
    GlobalOpportunity BIT DEFAULT 0,  -- Boolean for global/international opportunities
    TimeZone NVARCHAR(50) NULL,      -- Derived from agency location
    
    -- Enrichment Fields (AI/NLP Generated)
    SDGTags NVARCHAR(500) NULL,      -- UN SDG alignment keywords
    OpportunityGap NVARCHAR(255) NULL,  -- Equity/disadvantage focus
    KeywordTags NVARCHAR(1000) NULL,    -- Extracted keywords
    
    -- Quality & Audit Fields
    DataQualityScore DECIMAL(3,2) NULL,  -- 0.0 to 1.0 quality score
    ProcessingFlags NVARCHAR(500) NULL,  -- Any processing notes or warnings
    
    -- System Fields
    SourceLayerID BIGINT NULL,       -- Reference to Layer 1 record
    ProcessedDate DATETIME2 DEFAULT GETDATE(),
    ProcessedBy NVARCHAR(255) DEFAULT 'Layer2_ETL_Pipeline',
    DataVersion NVARCHAR(50) DEFAULT '2.0',
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    UpdatedDate DATETIME2 DEFAULT GETDATE(),
    
    -- Additional Reference Fields
    CFDANumbers NVARCHAR(500) NULL,
    Package NVARCHAR(500) NULL,
    Status NVARCHAR(100) NULL,
    Version NVARCHAR(50) NULL
);

-- Create simple indexes (no filtered indexes to avoid QUOTED_IDENTIFIER issues)
CREATE NONCLUSTERED INDEX IX_CleanGrants_OpportunityNumber ON CleanGrantsLayer2 (OpportunityNumber);
CREATE NONCLUSTERED INDEX IX_CleanGrants_Deadline ON CleanGrantsLayer2 (Deadline DESC);
CREATE NONCLUSTERED INDEX IX_CleanGrants_AwardValue ON CleanGrantsLayer2 (AwardValue DESC);
CREATE NONCLUSTERED INDEX IX_CleanGrants_AgencyName ON CleanGrantsLayer2 (AgencyName);
CREATE NONCLUSTERED INDEX IX_CleanGrants_ProcessedDate ON CleanGrantsLayer2 (ProcessedDate DESC);

PRINT '✅ CleanGrantsLayer2 table created successfully';
PRINT '📊 Table includes data quality, enrichment, and audit fields';
PRINT '🔍 Simple performance indexes created (no filtered indexes)';

GO