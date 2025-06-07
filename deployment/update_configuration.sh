#!/bin/bash
# Update configuration for final infrastructure

echo "⚙️ Updating GrantsGov configuration for production..."

# Final production configuration
RESOURCE_GROUP="GrantsGov"
STORAGE_ACCOUNT="grantsgov225756"
KEYVAULT_NAME="grantsgov-vault-5756"
FUNCTION_APP="grantsgov-func-py312"  # Python 3.12 Function App
LOCATION="eastus"
SUBSCRIPTION_ID="2361614a-5170-4ddf-abba-11cc0ec2b900"

echo "📋 Production Configuration:"
echo "Storage Account: $STORAGE_ACCOUNT"
echo "Key Vault: $KEYVAULT_NAME"
echo "Function App: $FUNCTION_APP (Python 3.12)"

# Get storage connection string
echo "🔗 Getting storage connection string..."
STORAGE_CONNECTION=$(az storage account show-connection-string \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --output tsv)

# Create final configuration file
cat > grantsgov_config.env << ENVEOF
# GrantsGov Azure Configuration - Production $(date)
export RESOURCE_GROUP="$RESOURCE_GROUP"
export STORAGE_ACCOUNT_NAME="$STORAGE_ACCOUNT"
export FUNCTION_APP_NAME="$FUNCTION_APP"
export KEYVAULT_NAME="$KEYVAULT_NAME"
export STORAGE_CONNECTION_STRING="$STORAGE_CONNECTION"
export AZURE_KEY_VAULT_URL="https://$KEYVAULT_NAME.vault.azure.net/"
export FUNCTION_APP_URL="https://$FUNCTION_APP.azurewebsites.net"
export AZURE_LOCATION="$LOCATION"
export SUBSCRIPTION_ID="$SUBSCRIPTION_ID"

# Table names
export GRANTS_TABLE_NAME="GrantDetails"
export GRANTS_HISTORY_TABLE="GrantsHistory"
export PROCESSING_LOG_TABLE="ProcessingLog"
export COMPANY_PROCESSED_TABLE="CompanyProcessed"

# Queue names
export GRANTS_PROCESSING_QUEUE="grants-processing"
export GRANTS_RETRY_QUEUE="grants-retry"
export GRANTS_FAILED_QUEUE="grants-failed"
export DATA_PROCESSING_QUEUE="data-processing"

# Container names
export GRANTS_EXPORTS_CONTAINER="grants-exports"
export GRANTS_BACKUPS_CONTAINER="grants-backups"
export GRANTS_LOGS_CONTAINER="grants-logs"
ENVEOF

echo "✅ Production configuration updated: grantsgov_config.env"
echo "🎯 Function App URL: https://$FUNCTION_APP.azurewebsites.net"
