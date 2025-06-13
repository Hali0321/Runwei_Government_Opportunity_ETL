-- ===================================
-- GRANTS.GOV API AZURE - DATABASE SETTINGS
-- ===================================

USE GrantsGovDB;
GO

-- Set recovery model
ALTER DATABASE [GrantsGovDB] SET RECOVERY SIMPLE;

-- Enable auto create statistics
ALTER DATABASE [GrantsGovDB] SET AUTO_CREATE_STATISTICS ON;

-- Enable auto update statistics
ALTER DATABASE [GrantsGovDB] SET AUTO_UPDATE_STATISTICS ON;

-- Set compatibility level to latest
DECLARE @CompatLevel INT;
SELECT @CompatLevel = compatibility_level FROM sys.databases WHERE name = 'GrantsGovDB';

PRINT 'Current compatibility level: ' + CAST(@CompatLevel AS VARCHAR(10));
PRINT '✅ Database settings configured';
GO
