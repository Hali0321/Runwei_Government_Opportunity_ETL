-- filepath: /Users/dinghali/Desktop/Runwei/grants_gov_api_azure/deploy_clean_architecture.sh
#!/bin/bash

# ===================================
# SINGLE COMMAND DEPLOYMENT
# Deploy clean 3-layer architecture to Azure
# ===================================

set -e  # Exit on any error

echo "🚀 Deploying Clean 3-Layer Architecture to Azure SQL Database"
echo "=============================================================="

# Check if we're in the right directory
if [ ! -f "sql/clean_architecture/clean_database_architecture.sql" ]; then
    echo "❌ Error: Cannot find clean_database_architecture.sql"
    echo "Make sure you're in the project root directory"
    exit 1
fi

echo "📋 Step 1: Deploying clean database architecture..."
sqlcmd -S grants-gov-sql-server.database.windows.net -d GrantsGovDB -U grantsadmin -P 'Grant$Admin2024!' -i "./sql/clean_architecture/clean_database_architecture.sql" -C

echo ""
echo "🧹 Step 2: Running final cleanup to remove any remaining unnecessary tables..."
sqlcmd -S grants-gov-sql-server.database.windows.net -d GrantsGovDB -U grantsadmin -P 'Grant$Admin2024!' -i "./sql/clean_architecture/final_cleanup.sql" -C

echo ""
echo "✅ Step 3: Verifying clean architecture deployment..."
sqlcmd -S grants-gov-sql-server.database.windows.net -d GrantsGovDB -U grantsadmin -P 'Grant$Admin2024!' -Q "
SELECT 
    COUNT(*) as 'Total Tables',
    STRING_AGG(TABLE_NAME, ', ') as 'Table Names'
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE'
  AND TABLE_NAME IN ('RawGrantsLayer1', 'RunweiFormatLayer2', 'BusinessIntelligenceLayer3');" -C

echo ""
echo "🎉 DEPLOYMENT COMPLETED SUCCESSFULLY!"
echo "Your clean 3-layer architecture is now live on Azure!"
echo ""
echo "📊 Available API Endpoints:"
echo "- api.vw_ComprehensiveOpportunities"
echo "- api.vw_PriorityOpportunities" 
echo "- api.vw_AnalyticsDashboard"
echo ""
echo "🚀 Ready for production use!"