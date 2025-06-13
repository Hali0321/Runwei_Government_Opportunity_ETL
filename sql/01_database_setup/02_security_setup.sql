-- ===================================
-- GRANTS.GOV API AZURE - SECURITY SETUP
-- ===================================

USE GrantsGovDB;
GO

-- Create application login (if deploying to production, use more secure methods)
-- Create read-only user for reporting
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'grants_reader')
BEGIN
    CREATE USER [grants_reader] WITH PASSWORD = 'ReadOnlyPassword123!';
    GRANT SELECT ON SCHEMA::dbo TO [grants_reader];
    PRINT '✅ Created grants_reader user with read permissions';
END
ELSE
    PRINT '⚠️ grants_reader user already exists';

-- Create application user with limited permissions
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'grants_app')
BEGIN
    CREATE USER [grants_app] WITH PASSWORD = 'AppPassword123!';
    PRINT '✅ Created grants_app user';
END
ELSE
    PRINT '⚠️ grants_app user already exists';

-- NOTE: Specific permissions will be granted after tables are created
PRINT '✅ Security setup completed';
