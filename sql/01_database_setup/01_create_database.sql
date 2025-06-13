-- ===================================
-- GRANTS.GOV API AZURE - DATABASE CREATION
-- ===================================

-- Create the database if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'GrantsGovDB')
BEGIN
    CREATE DATABASE GrantsGovDB;
END
GO

USE GrantsGovDB;
GO

-- Verify the current database context
SELECT DB_NAME() AS CurrentDatabase;
GO

PRINT '✅ Database created or already exists';
