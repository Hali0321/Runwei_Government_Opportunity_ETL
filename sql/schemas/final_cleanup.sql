-- ===================================
-- FINAL AZURE DATABASE CLEANUP
-- Remove all tables except the 3 essential layers
-- ===================================

USE GrantsGovDB;
GO

PRINT '🧹 Starting final Azure database cleanup...';
PRINT 'Keeping only: RawGrantsLayer1, RunweiFormatLayer2, BusinessIntelligenceLayer3';
PRINT '===============================================';

-- Drop all unnecessary tables that are still remaining
PRINT 'Removing remaining unnecessary tables...';

-- Drop Agency and Category tables
IF OBJECT_ID('AgencyMasterLayer2', 'U') IS NOT NULL
BEGIN
    DROP TABLE AgencyMasterLayer2;
    PRINT '✅ Removed AgencyMasterLayer2';
END

IF OBJECT_ID('CategoryMasterLayer2', 'U') IS NOT NULL
BEGIN
    DROP TABLE CategoryMasterLayer2;
    PRINT '✅ Removed CategoryMasterLayer2';
END

-- Drop any other Layer2 tables that might still exist
IF OBJECT_ID('EligibilityMasterLayer2', 'U') IS NOT NULL
BEGIN
    DROP TABLE EligibilityMasterLayer2;
    PRINT '✅ Removed EligibilityMasterLayer2';
END

IF OBJECT_ID('GeographicCoverageMasterLayer2', 'U') IS NOT NULL
BEGIN
    DROP TABLE GeographicCoverageMasterLayer2;
    PRINT '✅ Removed GeographicCoverageMasterLayer2';
END

IF OBJECT_ID('CleanedGrantsLayer2', 'U') IS NOT NULL
BEGIN
    DROP TABLE CleanedGrantsLayer2;
    PRINT '✅ Removed CleanedGrantsLayer2';
END

IF OBJECT_ID('GrantEligibilityLayer2', 'U') IS NOT NULL
BEGIN
    DROP TABLE GrantEligibilityLayer2;
    PRINT '✅ Removed GrantEligibilityLayer2';
END

IF OBJECT_ID('GrantGeographicCoverageLayer2', 'U') IS NOT NULL
BEGIN
    DROP TABLE GrantGeographicCoverageLayer2;
    PRINT '✅ Removed GrantGeographicCoverageLayer2';
END

-- Drop any remaining Layer3 tables (except BusinessIntelligenceLayer3)
IF OBJECT_ID('AgencyStatsLayer3', 'U') IS NOT NULL
BEGIN
    DROP TABLE AgencyStatsLayer3;
    PRINT '✅ Removed AgencyStatsLayer3';
END

IF OBJECT_ID('GrantBusinessViewLayer3', 'U') IS NOT NULL
BEGIN
    DROP TABLE GrantBusinessViewLayer3;
    PRINT '✅ Removed GrantBusinessViewLayer3';
END

IF OBJECT_ID('SuccessFactorsLayer3', 'U') IS NOT NULL
BEGIN
    DROP TABLE SuccessFactorsLayer3;
    PRINT '✅ Removed SuccessFactorsLayer3';
END

-- Drop any lookup tables that might still exist
IF OBJECT_ID('OpportunityTypesMaster', 'U') IS NOT NULL
BEGIN
    DROP TABLE OpportunityTypesMaster;
    PRINT '✅ Removed OpportunityTypesMaster';
END

IF OBJECT_ID('IndustriesMaster', 'U') IS NOT NULL
BEGIN
    DROP TABLE IndustriesMaster;
    PRINT '✅ Removed IndustriesMaster';
END

IF OBJECT_ID('UNSDGMaster', 'U') IS NOT NULL
BEGIN
    DROP TABLE UNSDGMaster;
    PRINT '✅ Removed UNSDGMaster';
END

-- Drop monitoring tables
IF OBJECT_ID('monitoring.APIRequestLog', 'U') IS NOT NULL
BEGIN
    DROP TABLE monitoring.APIRequestLog;
    PRINT '✅ Removed monitoring.APIRequestLog';
END

IF OBJECT_ID('monitoring.ApplicationLog', 'U') IS NOT NULL
BEGIN
    DROP TABLE monitoring.ApplicationLog;
    PRINT '✅ Removed monitoring.ApplicationLog';
END

-- Drop any other tables that shouldn't be there
DECLARE @TableName NVARCHAR(128);
DECLARE @SQL NVARCHAR(MAX);

DECLARE table_cursor CURSOR FOR
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE'
  AND TABLE_NAME NOT IN (
    'RawGrantsLayer1', 
    'RunweiFormatLayer2', 
    'BusinessIntelligenceLayer3',
    'DeploymentLog'  -- Keep this for tracking
  )
  AND TABLE_SCHEMA = 'dbo';

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL = 'DROP TABLE [dbo].[' + @TableName + ']';
    EXEC sp_executesql @SQL;
    PRINT '✅ Removed additional table: ' + @TableName;
    
    FETCH NEXT FROM table_cursor INTO @TableName;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

PRINT '';
PRINT '🧹 Cleanup completed successfully!';
PRINT '';
PRINT '📊 Final Clean Database Structure:';

-- Show the final clean structure
SELECT 
    TABLE_NAME as 'Essential Tables Remaining',
    CASE 
        WHEN TABLE_NAME = 'RawGrantsLayer1' THEN 'Layer 1: Raw grants data'
        WHEN TABLE_NAME = 'RunweiFormatLayer2' THEN 'Layer 2: Your opportunity format'
        WHEN TABLE_NAME = 'BusinessIntelligenceLayer3' THEN 'Layer 3: Analytics & insights'
        WHEN TABLE_NAME = 'DeploymentLog' THEN 'System: Deployment tracking'
        ELSE 'Other'
    END as 'Purpose',
    CASE 
        WHEN TABLE_NAME = 'RawGrantsLayer1' THEN (SELECT COUNT(*) FROM RawGrantsLayer1)
        WHEN TABLE_NAME = 'RunweiFormatLayer2' THEN (SELECT COUNT(*) FROM RunweiFormatLayer2)
        WHEN TABLE_NAME = 'BusinessIntelligenceLayer3' THEN (SELECT COUNT(*) FROM BusinessIntelligenceLayer3)
        ELSE 0
    END as 'Record Count'
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE'
  AND TABLE_NAME IN (
    'RawGrantsLayer1', 
    'RunweiFormatLayer2', 
    'BusinessIntelligenceLayer3',
    'DeploymentLog'
  )
ORDER BY 
    CASE 
        WHEN TABLE_NAME = 'RawGrantsLayer1' THEN 1
        WHEN TABLE_NAME = 'RunweiFormatLayer2' THEN 2
        WHEN TABLE_NAME = 'BusinessIntelligenceLayer3' THEN 3
        ELSE 4
    END;

PRINT '';
PRINT '✅ Azure database is now clean with only 3 essential tables!';
PRINT '🚀 Ready for production use!';
GO