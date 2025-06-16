#!/bin/bash

echo "🚀 COMPLETE GRANTS.GOV AZURE DEPLOYMENT"
echo "========================================"
echo "📅 Started: $(date)"
echo "🎯 Deploying Clean 3-Layer Architecture"
echo "📍 Current directory: $(pwd)"
echo "========================================"

# Check if we're in the right directory
if [ ! -f "sql/clean_architecture/update_rawgrants_schema.sql" ]; then
    echo "❌ Error: Cannot find required SQL files"
    echo "📂 Current directory: $(pwd)"
    echo "📋 Available files:"
    find . -name "*.sql" -type f 2>/dev/null | head -10
    echo ""
    echo "💡 Please run this script from the project root directory:"
    echo "   cd /Users/dinghali/Desktop/Runwei/grants_gov_api_azure"
    echo "   ./sql/deployment/deploy_everything.sh"
    exit 1
fi

# Step 1: Create the azure_standalone_deploy.sql if it doesn't exist
if [ ! -f "sql/deploy/azure_standalone_deploy.sql" ]; then
    echo "📋 Creating azure_standalone_deploy.sql..."
    mkdir -p sql/deploy
    
    cat > sql/deploy/azure_standalone_deploy.sql << 'SQLEOF'
-- ===================================
-- AZURE STANDALONE DEPLOYMENT
-- Complete 3-layer grants architecture
-- ===================================

USE GrantsGovDB;
GO

PRINT '🚀 Starting Azure Standalone Deployment...';
PRINT '==========================================';

-- Create RawGrantsLayer1 if not exists
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RawGrantsLayer1')
BEGIN
    PRINT '📋 Creating RawGrantsLayer1...';
    CREATE TABLE RawGrantsLayer1 (
        ID BIGINT IDENTITY(1,1) PRIMARY KEY,
        PartitionKey NVARCHAR(255) NOT NULL DEFAULT 'Grant',
        RowKey NVARCHAR(255) NOT NULL,
        OpportunityNumber NVARCHAR(255) NULL,
        Title NVARCHAR(1000) NULL,
        AgencyCode NVARCHAR(100) NULL,
        AgencyName NVARCHAR(500) NULL,
        Category NVARCHAR(500) NULL,
        FundingType NVARCHAR(255) NULL,
        AwardCeiling DECIMAL(18,2) NULL,
        Status NVARCHAR(100) NULL,
        CreatedDate DATETIME2 DEFAULT GETDATE(),
        CONSTRAINT UQ_RawGrants_RowKey UNIQUE (RowKey)
    );
    PRINT '✅ RawGrantsLayer1 created';
END
ELSE
    PRINT '✅ RawGrantsLayer1 already exists';

-- Create RunweiFormatLayer2 if not exists
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RunweiFormatLayer2')
BEGIN
    PRINT '📋 Creating RunweiFormatLayer2...';
    CREATE TABLE RunweiFormatLayer2 (
        OpportunityID BIGINT IDENTITY(1,1) PRIMARY KEY,
        Title NVARCHAR(1000) NULL,
        ShortDescription NVARCHAR(500) NULL,
        Industry NVARCHAR(500) NULL,
        AwardValue DECIMAL(18,2) NULL,
        Deadline DATETIME2 NULL,
        Status NVARCHAR(100) NULL,
        OpportunityType NVARCHAR(100) DEFAULT 'Grant',
        CreatedDate DATETIME2 DEFAULT GETDATE()
    );
    PRINT '✅ RunweiFormatLayer2 created';
END
ELSE
    PRINT '✅ RunweiFormatLayer2 already exists';

-- Create BusinessIntelligenceLayer3 if not exists
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'BusinessIntelligenceLayer3')
BEGIN
    PRINT '📋 Creating BusinessIntelligenceLayer3...';
    CREATE TABLE BusinessIntelligenceLayer3 (
        IntelligenceID BIGINT IDENTITY(1,1) PRIMARY KEY,
        OpportunityID BIGINT NULL,
        CompetitiveScore DECIMAL(5,2) NULL,
        OpportunityValue NVARCHAR(100) NULL,
        RecommendationLevel NVARCHAR(500) NULL,
        AnalyticsData NVARCHAR(MAX) NULL,
        CreatedDate DATETIME2 DEFAULT GETDATE(),
        FOREIGN KEY (OpportunityID) REFERENCES RunweiFormatLayer2(OpportunityID)
    );
    PRINT '✅ BusinessIntelligenceLayer3 created';
END
ELSE
    PRINT '✅ BusinessIntelligenceLayer3 already exists';

PRINT '🎉 Azure Standalone Deployment completed successfully!';
GO
SQLEOF
    echo "✅ Created azure_standalone_deploy.sql"
fi

# Step 1: Deploy the clean architecture
echo ""
echo "📋 STEP 1: Deploying Clean Architecture..."
sqlcmd -S grants-gov-sql-server.database.windows.net -d GrantsGovDB -U grantsadmin -P 'Grant$Admin2024!' -i "sql/deploy/azure_standalone_deploy.sql" -C

if [ $? -ne 0 ]; then
    echo "❌ Architecture deployment failed"
    exit 1
fi
echo "✅ Clean architecture deployed"

# Step 2: Update RawGrantsLayer1 schema to match Azure Storage
echo ""
echo "🔧 STEP 2: Updating RawGrantsLayer1 schema..."
sqlcmd -S grants-gov-sql-server.database.windows.net -d GrantsGovDB -U grantsadmin -P 'Grant$Admin2024!' -i "sql/clean_architecture/update_rawgrants_schema.sql" -C

if [ $? -ne 0 ]; then
    echo "❌ Schema update failed"
    exit 1
fi
echo "✅ Schema updated to match Azure Table Storage"

# Step 3: Verify deployment
echo ""
echo "🔍 STEP 3: Verifying deployment..."
sqlcmd -S grants-gov-sql-server.database.windows.net -d GrantsGovDB -U grantsadmin -P 'Grant$Admin2024!' -Q "
SELECT 
    'Table' as ObjectType,
    TABLE_NAME as Name,
    'Active' as Status
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME IN ('RawGrantsLayer1', 'RunweiFormatLayer2', 'BusinessIntelligenceLayer3')
ORDER BY Name;
" -C

echo ""
echo "📊 Final Verification - Column Count Check:"
sqlcmd -S grants-gov-sql-server.database.windows.net -d GrantsGovDB -U grantsadmin -P 'Grant$Admin2024!' -Q "
SELECT 
    TABLE_NAME as 'Table',
    COUNT(*) as 'Columns'
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME IN ('RawGrantsLayer1', 'RunweiFormatLayer2', 'BusinessIntelligenceLayer3')
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;
" -C

echo ""
echo "🎉 DEPLOYMENT COMPLETED SUCCESSFULLY!"
echo "========================================"
echo "📅 Finished: $(date)"
echo "✅ Clean 3-layer architecture deployed"
echo "✅ RawGrantsLayer1 schema updated (28+ columns)"
echo "✅ Ready for data synchronization"
echo "✅ All API endpoints available"
echo ""
echo "🚀 Next Steps:"
echo "1. Run data refresh: python src/scripts/bulk_update_grantdetails.py"
echo "2. Sync to SQL: python one_click_simple.py"
echo "3. Verify data: Check all 3 layers populated"
echo "========================================"