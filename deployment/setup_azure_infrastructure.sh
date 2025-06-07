#!/bin/bash
# Complete Azure infrastructure setup for GrantsGov data processing

# Enable strict error handling
set -euo pipefail

echo "🔍 Starting GrantsGov infrastructure setup..."
echo "Current directory: $(pwd)"
echo "Script path: $0"
echo "User: $(whoami)"
echo "Date: $(date)"

# Configuration
RESOURCE_GROUP="GrantsGov"
LOCATION="eastus"
TIMESTAMP=$(date +%s)
STORAGE_ACCOUNT="grantsgov225141"
FUNCTION_APP="grantsgov-func-5141"
KEYVAULT_NAME="grantsgov-vault-5141"

echo ""
echo "🚀 Setting up GrantsGov infrastructure..."
echo "📍 Location: $LOCATION"
echo "📦 Resource Group: $RESOURCE_GROUP"
echo "💾 Storage Account: $STORAGE_ACCOUNT"
echo "⚡ Function App: $FUNCTION_APP"
echo "🔐 Key Vault: $KEYVAULT_NAME"

# Check Azure login
echo ""
echo "🔐 Step 1: Checking Azure login status..."
if ! az account show &> /dev/null; then
    echo "❌ Not logged in to Azure. Please run 'az login' first."
    exit 1
fi

echo "✅ Azure login verified"

# Show current context
SUBSCRIPTION_ID=2361614a-5170-4ddf-abba-11cc0ec2b900
USER_NAME=$(az account show --query user.name -o tsv)
echo "Subscription: $SUBSCRIPTION_ID"
echo "User: $USER_NAME"

# Check/Create Resource Group
echo ""
echo "📦 Step 2: Checking/Creating Resource Group..."
if az group show --name "$RESOURCE_GROUP" &> /dev/null; then
    echo "✅ Resource group '$RESOURCE_GROUP' already exists"
else
    echo "Creating new resource group '$RESOURCE_GROUP'..."
    az group create \
      --name "$RESOURCE_GROUP" \
      --location "$LOCATION" \
      --tags "Project=GrantsGov" "Environment=Development" "Owner=Hali"
    
    if [ $? -eq 0 ]; then
        echo "✅ Resource group '$RESOURCE_GROUP' created successfully"
    else
        echo "❌ Failed to create resource group '$RESOURCE_GROUP'"
        exit 1
    fi
fi

# Create Storage Account
echo ""
echo "💾 Step 3: Creating Storage Account..."
echo "Creating storage account '$STORAGE_ACCOUNT'..."

az storage account create \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku "Standard_LRS" \
  --kind "StorageV2" \
  --access-tier "Hot" \
  --allow-blob-public-access false \
  --min-tls-version "TLS1_2" \
  --tags "Purpose=GrantsData" "DataType=Government"

if [ $? -ne 0 ]; then
    echo "❌ Failed to create storage account"
    exit 1
fi

echo "✅ Storage account '$STORAGE_ACCOUNT' created"

# Get storage connection string
echo ""
echo "🔗 Step 4: Getting storage connection string..."
STORAGE_CONNECTION=$(az storage account show-connection-string \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --query "connectionString" \
  --output tsv)

if [ -z "$STORAGE_CONNECTION" ]; then
    echo "❌ Failed to get storage connection string"
    exit 1
fi

echo "✅ Storage connection string obtained (length: ${#STORAGE_CONNECTION})"

# Create tables
echo ""
echo "📊 Step 5: Creating storage tables..."

TABLES=("GrantDetails" "GrantsHistory" "ProcessingLog" "CompanyProcessed")

for table in "${TABLES[@]}"; do
    echo "Creating table: $table"
    az storage table create \
      --name "$table" \
      --connection-string "$STORAGE_CONNECTION"
    
    if [ $? -eq 0 ]; then
        echo "✅ Table '$table' created"
    else
        echo "⚠️ Table '$table' may already exist or failed to create"
    fi
done

# Create queues
echo ""
echo "📬 Step 6: Creating storage queues..."

QUEUES=("grants-processing" "grants-retry" "grants-failed" "data-processing")

for queue in "${QUEUES[@]}"; do
    echo "Creating queue: $queue"
    az storage queue create \
      --name "$queue" \
      --connection-string "$STORAGE_CONNECTION"
    
    if [ $? -eq 0 ]; then
        echo "✅ Queue '$queue' created"
    else
        echo "⚠️ Queue '$queue' may already exist or failed to create"
    fi
done

# Create blob containers
echo ""
echo "📦 Step 7: Creating blob containers..."

CONTAINERS=("grants-exports" "grants-backups" "grants-logs")

for container in "${CONTAINERS[@]}"; do
    echo "Creating container: $container"
    az storage container create \
      --name "$container" \
      --connection-string "$STORAGE_CONNECTION" \
      --public-access off
    
    if [ $? -eq 0 ]; then
        echo "✅ Container '$container' created"
    else
        echo "⚠️ Container '$container' may already exist or failed to create"
    fi
done

# Create Key Vault - FIXED: Removed problematic parameters
echo ""
echo "🔐 Step 8: Creating Key Vault..."
echo "Creating Key Vault '$KEYVAULT_NAME'..."

az keyvault create \
  --name "$KEYVAULT_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku "standard" \
  --tags "Purpose=GrantsSecrets"

if [ $? -ne 0 ]; then
    echo "❌ Failed to create Key Vault"
    exit 1
fi

echo "✅ Key Vault '$KEYVAULT_NAME' created"

# Store secrets
echo ""
echo "🗝️ Step 9: Storing secrets in Key Vault..."

echo "Storing storage connection string..."
az keyvault secret set \
  --vault-name "$KEYVAULT_NAME" \
  --name "StorageConnectionString" \
  --value "$STORAGE_CONNECTION"

if [ $? -eq 0 ]; then
    echo "✅ Storage connection string stored"
else
    echo "❌ Failed to store storage connection string"
fi

echo "Storing API endpoint..."
az keyvault secret set \
  --vault-name "$KEYVAULT_NAME" \
  --name "GrantsGovApiEndpoint" \
  --value "https://www.grants.gov/grantsws/rest/opportunities/search/"

if [ $? -eq 0 ]; then
    echo "✅ API endpoint stored"
else
    echo "❌ Failed to store API endpoint"
fi

# Create Function App
echo ""
echo "⚡ Step 10: Creating Function App..."
echo "Creating Function App '$FUNCTION_APP'..."

az functionapp create \
  --resource-group "$RESOURCE_GROUP" \
  --consumption-plan-location "$LOCATION" \
  --runtime "python" \
  --runtime-version "3.11" \
  --functions-version "4" \
  --name "$FUNCTION_APP" \
  --storage-account "$STORAGE_ACCOUNT" \
  --os-type "Linux" \
  --tags "Purpose=GrantsProcessing"

if [ $? -ne 0 ]; then
    echo "❌ Failed to create Function App"
    exit 1
fi

echo "✅ Function App '$FUNCTION_APP' created"

# Configure managed identity
echo ""
echo "🎭 Step 11: Setting up managed identity..."

FUNCTION_PRINCIPAL_ID=$(az functionapp identity assign \
  --name "$FUNCTION_APP" \
  --resource-group "$RESOURCE_GROUP" \
  --query "principalId" \
  --output tsv)

if [ -z "$FUNCTION_PRINCIPAL_ID" ]; then
    echo "❌ Failed to assign managed identity"
    exit 1
fi

echo "✅ Managed identity assigned: $FUNCTION_PRINCIPAL_ID"

# Set permissions
echo ""
echo "🔒 Step 12: Setting up permissions..."

echo "Granting Key Vault access..."
az keyvault set-policy \
  --name "$KEYVAULT_NAME" \
  --object-id "$FUNCTION_PRINCIPAL_ID" \
  --secret-permissions get list

if [ $? -eq 0 ]; then
    echo "✅ Key Vault permissions granted"
else
    echo "⚠️ Key Vault permissions may have failed"
fi

echo "Granting Storage access..."
az role assignment create \
  --assignee "$FUNCTION_PRINCIPAL_ID" \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT"

if [ $? -eq 0 ]; then
    echo "✅ Storage permissions granted"
else
    echo "⚠️ Storage permissions may have failed"
fi

# Configure Function App settings
echo ""
echo "⚙️ Step 13: Configuring Function App..."

FUNCTION_URL="https://$FUNCTION_APP.azurewebsites.net"

az functionapp config appsettings set \
  --name "$FUNCTION_APP" \
  --resource-group "$RESOURCE_GROUP" \
  --settings \
    "AzureWebJobsStorage=$STORAGE_CONNECTION" \
    "STORAGE_ACCOUNT_NAME=$STORAGE_ACCOUNT" \
    "AZURE_KEY_VAULT_URL=https://$KEYVAULT_NAME.vault.azure.net/" \
    "GRANTS_TABLE_NAME=GrantDetails" \
    "GRANTS_QUEUE_NAME=grants-processing" \
    "WEBSITE_RUN_FROM_PACKAGE=1" \
    "ENABLE_ORYX_BUILD=true" \
    "SCM_DO_BUILD_DURING_DEPLOYMENT=1" \
    "FUNCTIONS_EXTENSION_VERSION=~4" \
    "PYTHON_ISOLATE_WORKER_DEPENDENCIES=1"

if [ $? -eq 0 ]; then
    echo "✅ Function App configured"
else
    echo "⚠️ Function App configuration may have failed"
fi

# Test the setup
echo ""
echo "🧪 Step 14: Testing infrastructure..."
echo "Waiting for Function App to be ready..."
sleep 15

echo "Testing storage tables..."
az storage table list --connection-string "$STORAGE_CONNECTION" --output table

echo "Testing storage queues..."
az storage queue list --connection-string "$STORAGE_CONNECTION" --output table

# Save configuration
echo ""
echo "💾 Step 15: Saving configuration..."

cat > grantsgov_config.env << ENVEOF
# GrantsGov Azure Configuration - Generated $(date)
# Load with: source grantsgov_config.env

export RESOURCE_GROUP="$RESOURCE_GROUP"
export STORAGE_ACCOUNT_NAME="$STORAGE_ACCOUNT"
export FUNCTION_APP_NAME="$FUNCTION_APP"
export KEYVAULT_NAME="$KEYVAULT_NAME"
export STORAGE_CONNECTION_STRING="$STORAGE_CONNECTION"
export AZURE_KEY_VAULT_URL="https://$KEYVAULT_NAME.vault.azure.net/"
export FUNCTION_APP_URL="$FUNCTION_URL"
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

echo "✅ Configuration saved to grantsgov_config.env"

# Create deployment script with correct path
cat > deploy_grantsgov_functions.sh << 'DEPLOYEOF'
#!/bin/bash
# Deployment script for GrantsGov functions

echo "🔄 Loading GrantsGov configuration..."
if [ ! -f "grantsgov_config.env" ]; then
    echo "❌ Configuration file grantsgov_config.env not found!"
    echo "Please run ./deployment/setup_azure_infrastructure.sh first"
    exit 1
fi

source grantsgov_config.env

echo "🚀 Deploying GrantsGov functions..."
echo "Target: $FUNCTION_APP_NAME"
echo "URL: $FUNCTION_APP_URL"

# Navigate to functions directory - CORRECTED PATH
FUNCTIONS_DIR="src/azure_functions"
if [ ! -d "$FUNCTIONS_DIR" ]; then
    echo "❌ Functions directory not found: $FUNCTIONS_DIR"
    echo "Available directories:"
    find . -name "*functions*" -type d 2>/dev/null || echo "No functions directories found"
    exit 1
fi

cd "$FUNCTIONS_DIR"

echo "📂 Current directory: $(pwd)"
echo "📁 Functions found:"
ls -la

# Check for required files and create if missing
if [ ! -f "host.json" ]; then
    echo "⚠️ host.json not found, creating default..."
    cat > host.json << 'HOSTEOF'
{
  "version": "2.0",
  "logging": {
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": true,
        "excludedTypes": "Request"
      }
    }
  },
  "extensionBundle": {
    "id": "Microsoft.Azure.Functions.ExtensionBundle",
    "version": "[3.*, 4.0.0)"
  },
  "functionTimeout": "00:10:00"
}
HOSTEOF
fi

if [ ! -f "requirements.txt" ]; then
    echo "⚠️ requirements.txt not found, creating..."
    cat > requirements.txt << 'REQEOF'
azure-functions>=1.18.0
azure-data-tables>=12.4.0
azure-storage-queue>=12.8.0
azure-storage-blob>=12.19.0
azure-identity>=1.15.0
requests>=2.31.0
python-dateutil>=2.8.2
python-dotenv>=1.0.0
REQEOF
fi

# Deploy functions
echo "🚀 Starting deployment..."
func azure functionapp publish $FUNCTION_APP_NAME --python --build remote

if [ $? -eq 0 ]; then
    echo "✅ Deployment complete!"
    echo "🌐 Health check: $FUNCTION_APP_URL/api/healthcheck"
    echo "🔗 Azure Portal: https://portal.azure.com/#resource/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Web/sites/$FUNCTION_APP_NAME"
    echo ""
    echo "🎯 Next steps:"
    echo "1. Test health endpoint: curl $FUNCTION_APP_URL/api/healthcheck"
    echo "2. Monitor logs: az functionapp log tail --name $FUNCTION_APP_NAME --resource-group $RESOURCE_GROUP"
    echo "3. View in Azure Portal to verify deployment"
else
    echo "❌ Deployment failed!"
    echo "💡 Troubleshooting steps:"
    echo "1. Check Azure CLI login: az account show"
    echo "2. Verify Function App exists: az functionapp show --name $FUNCTION_APP_NAME --resource-group $RESOURCE_GROUP"
    echo "3. Check functions directory structure: ls -la"
    echo "4. Verify Azure Functions Core Tools: func --version"
fi
DEPLOYEOF

chmod +x deploy_grantsgov_functions.sh

echo ""
echo "🎉 GrantsGov infrastructure setup completed successfully!"
echo "========================================================================"
echo "📦 Resource Group: $RESOURCE_GROUP"
echo "💾 Storage Account: $STORAGE_ACCOUNT"
echo "⚡ Function App: $FUNCTION_APP"
echo "🔐 Key Vault: $KEYVAULT_NAME"
echo "🌐 Function App URL: $FUNCTION_URL"
echo "📍 Location: $LOCATION"
echo "========================================================================"
echo ""
echo "📊 Created Resources:"
echo "✅ Tables: GrantDetails, GrantsHistory, ProcessingLog, CompanyProcessed"
echo "✅ Queues: grants-processing, grants-retry, grants-failed, data-processing"
echo "✅ Containers: grants-exports, grants-backups, grants-logs"
echo "✅ Key Vault with secrets: StorageConnectionString, GrantsGovApiEndpoint"
echo "✅ Function App with managed identity and permissions"
echo ""
echo "💾 Configuration saved to: grantsgov_config.env"
echo "🚀 Deployment script created: deploy_grantsgov_functions.sh"
echo ""
echo "🎯 Next steps:"
echo "1. Load configuration: source grantsgov_config.env"
echo "2. Deploy functions: ./deploy_grantsgov_functions.sh"
echo "3. Test deployment: curl \$FUNCTION_APP_URL/api/healthcheck"
echo "4. View in Azure Portal:"
echo "   https://portal.azure.com/#resource/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP"
echo ""
echo "🔍 Troubleshooting:"
echo "- Check logs: az functionapp log tail --name $FUNCTION_APP --resource-group $RESOURCE_GROUP"
echo "- Monitor functions: az functionapp list --resource-group $RESOURCE_GROUP --output table"
echo "- Verify storage: az storage account show --name $STORAGE_ACCOUNT --resource-group $RESOURCE_GROUP"
