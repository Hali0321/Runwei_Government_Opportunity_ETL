#!/bin/bash

# Azure Data Factory ETL Pipeline Deployment Script
# Deploys the complete Grants.gov ETL infrastructure

set -e

# Default values for deployment
DEFAULT_RESOURCE_GROUP="GrantsGov"
DEFAULT_LOCATION="East US"
DEFAULT_SUBSCRIPTION="2361614a-5170-4ddf-abba-11cc0ec2b900"
DEFAULT_SQL_SERVER="grants-gov-sql-server"
DEFAULT_SQL_DATABASE="GrantsGovDB"
DEFAULT_STORAGE_ACCOUNT="grantsgov225756"
DEFAULT_KEY_VAULT="kv-grants-gov"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Functions
print_header() {
    echo -e "${CYAN}🚀 Azure Data Factory ETL Pipeline Deployment${NC}"
    echo -e "${CYAN}================================================${NC}"
}

print_step() {
    echo -e "${BLUE}📋 $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --resource-group)
            RESOURCE_GROUP="$2"
            shift 2
            ;;
        --data-factory-name)
            DATA_FACTORY_NAME="$2"
            shift 2
            ;;
        --sql-server-name)
            SQL_SERVER_NAME="$2"
            shift 2
            ;;
        --sql-database-name)
            SQL_DATABASE_NAME="$2"
            shift 2
            ;;
        --storage-account-name)
            STORAGE_ACCOUNT_NAME="$2"
            shift 2
            ;;
        --key-vault-name)
            KEY_VAULT_NAME="$2"
            shift 2
            ;;
        --environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        --location)
            LOCATION="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --resource-group NAME     Azure resource group name (default: grants-gov-rg)"
            echo "  --data-factory-name NAME  Data Factory name (default: df-grants-gov-etl)"
            echo "  --sql-server-name NAME    SQL Server name (default: grants-gov-sql-server)"
            echo "  --sql-database-name NAME  SQL Database name (default: GrantsGovDB)"
            echo "  --storage-account-name NAME Storage Account name (default: grantsgov225756)"
            echo "  --key-vault-name NAME     Key Vault name (default: kv-grants-gov)"
            echo "  --environment ENV         Environment (default: test)"
            echo "  --location LOCATION       Azure region (default: eastus)"
            echo "  -h, --help               Show this help message"
            echo ""
            echo "Example:"
            echo "  $0 --resource-group my-rg --environment prod"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Initialize variables with defaults if not set
RESOURCE_GROUP=${RESOURCE_GROUP:-$DEFAULT_RESOURCE_GROUP}
DATA_FACTORY_NAME=${DATA_FACTORY_NAME:-"df-grants-gov-etl"}
SQL_SERVER_NAME=${SQL_SERVER_NAME:-$DEFAULT_SQL_SERVER}
SQL_DATABASE_NAME=${SQL_DATABASE_NAME:-$DEFAULT_SQL_DATABASE}
STORAGE_ACCOUNT_NAME=${STORAGE_ACCOUNT_NAME:-$DEFAULT_STORAGE_ACCOUNT}
KEY_VAULT_NAME=${KEY_VAULT_NAME:-$DEFAULT_KEY_VAULT}
ENVIRONMENT=${ENVIRONMENT:-"test"}
LOCATION=${LOCATION:-$DEFAULT_LOCATION}

print_header

print_step "Configuration Summary"
echo "  📍 Resource Group: $RESOURCE_GROUP"
echo "  🏭 Data Factory: $DATA_FACTORY_NAME"
echo "  🗄️  SQL Server: $SQL_SERVER_NAME"
echo "  💾 SQL Database: $SQL_DATABASE_NAME"
echo "  � Storage Account: $STORAGE_ACCOUNT_NAME"
echo "  �🔐 Key Vault: $KEY_VAULT_NAME"
echo "  🏷️  Environment: $ENVIRONMENT"
echo "  🌍 Location: $LOCATION"
echo ""

# Check if Azure CLI is installed and logged in
print_step "Checking Azure CLI setup..."
if ! command -v az &> /dev/null; then
    print_error "Azure CLI is not installed. Please install it first."
    exit 1
fi

# Check if logged in
if ! az account show &> /dev/null; then
    print_error "Not logged in to Azure. Please run 'az login' first."
    exit 1
fi

print_success "Azure CLI is configured"

# Get current subscription
SUBSCRIPTION_ID=$(az account show --query id -o tsv)
SUBSCRIPTION_NAME=$(az account show --query name -o tsv)
echo "  📊 Subscription: $SUBSCRIPTION_NAME ($SUBSCRIPTION_ID)"
echo ""

# Check if resource group exists
print_step "Checking resource group..."
if az group show --name "$RESOURCE_GROUP" &> /dev/null; then
    print_success "Resource group '$RESOURCE_GROUP' exists"
else
    print_warning "Resource group '$RESOURCE_GROUP' does not exist"
    read -p "Create resource group? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_step "Creating resource group..."
        az group create --name "$RESOURCE_GROUP" --location "$LOCATION"
        print_success "Resource group created"
    else
        print_error "Resource group is required. Exiting."
        exit 1
    fi
fi
echo ""

# Validate prerequisites
print_step "Validating prerequisites..."

# Check if SQL Server exists
if az sql server show --name "$SQL_SERVER_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    print_success "SQL Server '$SQL_SERVER_NAME' exists"
else
    print_error "SQL Server '$SQL_SERVER_NAME' not found in resource group '$RESOURCE_GROUP'"
    echo "Please create the SQL Server first or update the configuration."
    exit 1
fi

# Check if SQL Database exists
if az sql db show --name "$SQL_DATABASE_NAME" --server "$SQL_SERVER_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    print_success "SQL Database '$SQL_DATABASE_NAME' exists"
else
    print_error "SQL Database '$SQL_DATABASE_NAME' not found"
    echo "Please create the database first or update the configuration."
    exit 1
fi

# Check if Key Vault exists
if az keyvault show --name "$KEY_VAULT_NAME" &> /dev/null; then
    print_success "Key Vault '$KEY_VAULT_NAME' exists"
    
    # Check if database password secret exists
    if az keyvault secret show --vault-name "$KEY_VAULT_NAME" --name "GrantsGovDB-Password" &> /dev/null; then
        print_success "Database password secret exists in Key Vault"
    else
        print_error "Database password secret 'GrantsGovDB-Password' not found in Key Vault"
        echo "Please store the database password in Key Vault with the name 'GrantsGovDB-Password'"
        exit 1
    fi
else
    print_error "Key Vault '$KEY_VAULT_NAME' not found"
    echo "Please create the Key Vault first or update the configuration."
    exit 1
fi

print_success "All prerequisites validated"
echo ""

# Deploy Data Factory
print_step "Deploying Azure Data Factory infrastructure..."
echo "  🔧 Template: main.bicep"
echo "  ⏱️  This may take 5-10 minutes..."
echo ""

DEPLOYMENT_NAME="df-etl-deployment-$(date +%Y%m%d-%H%M%S)"

az deployment group create \
    --resource-group "$RESOURCE_GROUP" \
    --template-file main.bicep \
    --name "$DEPLOYMENT_NAME" \
    --parameters \
        dataFactoryName="$DATA_FACTORY_NAME" \
        sqlServerName="$SQL_SERVER_NAME" \
        sqlDatabaseName="$SQL_DATABASE_NAME" \
        storageAccountName="$STORAGE_ACCOUNT_NAME" \
        keyVaultName="$KEY_VAULT_NAME" \
        environment="$ENVIRONMENT" \
        location="$LOCATION" \
    --output table

if [ $? -eq 0 ]; then
    print_success "Data Factory infrastructure deployed successfully!"
else
    print_error "Data Factory deployment failed"
    exit 1
fi

echo ""

# Get deployment outputs
print_step "Retrieving deployment information..."
DEPLOYMENT_OUTPUTS=$(az deployment group show \
    --resource-group "$RESOURCE_GROUP" \
    --name "$DEPLOYMENT_NAME" \
    --query properties.outputs)

if [ "$DEPLOYMENT_OUTPUTS" != "null" ]; then
    DATA_FACTORY_ID=$(echo "$DEPLOYMENT_OUTPUTS" | jq -r '.dataFactoryId.value // empty')
    TEST_TRIGGER_NAME=$(echo "$DEPLOYMENT_OUTPUTS" | jq -r '.testTriggerName.value // empty')
    DAILY_TRIGGER_NAME=$(echo "$DEPLOYMENT_OUTPUTS" | jq -r '.dailyTriggerName.value // empty')
    MAIN_PIPELINE_NAME=$(echo "$DEPLOYMENT_OUTPUTS" | jq -r '.mainPipelineName.value // empty')
    
    echo "  🏭 Data Factory ID: $DATA_FACTORY_ID"
    echo "  🧪 Test Trigger: $TEST_TRIGGER_NAME"
    echo "  📅 Daily Trigger: $DAILY_TRIGGER_NAME"
    echo "  🔄 Main Pipeline: $MAIN_PIPELINE_NAME"
fi

echo ""

# Verify deployment
print_step "Verifying deployment..."

# Check Data Factory
if az datafactory show --resource-group "$RESOURCE_GROUP" --factory-name "$DATA_FACTORY_NAME" &> /dev/null; then
    print_success "Data Factory is accessible"
else
    print_error "Data Factory verification failed"
    exit 1
fi

# Check pipelines
PIPELINE_COUNT=$(az datafactory pipeline list --resource-group "$RESOURCE_GROUP" --factory-name "$DATA_FACTORY_NAME" --query "length(@)")
echo "  📋 Pipelines deployed: $PIPELINE_COUNT"

# Check triggers
TRIGGER_COUNT=$(az datafactory trigger list --resource-group "$RESOURCE_GROUP" --factory-name "$DATA_FACTORY_NAME" --query "length(@)")
echo "  ⏰ Triggers deployed: $TRIGGER_COUNT"

# Check linked services
LINKEDSERVICE_COUNT=$(az datafactory linked-service list --resource-group "$RESOURCE_GROUP" --factory-name "$DATA_FACTORY_NAME" --query "length(@)")
echo "  🔗 Linked services deployed: $LINKEDSERVICE_COUNT"

# Check datasets
DATASET_COUNT=$(az datafactory dataset list --resource-group "$RESOURCE_GROUP" --factory-name "$DATA_FACTORY_NAME" --query "length(@)")
echo "  📊 Datasets deployed: $DATASET_COUNT"

print_success "Deployment verification completed"
echo ""

# Final summary and next steps
print_step "🎉 DEPLOYMENT COMPLETED SUCCESSFULLY!"
echo ""
echo "📋 DEPLOYMENT SUMMARY:"
echo "  ✅ Data Factory: $DATA_FACTORY_NAME"
echo "  ✅ Pipelines: $PIPELINE_COUNT deployed"
echo "  ✅ Triggers: $TRIGGER_COUNT deployed (both stopped)"
echo "  ✅ Linked Services: $LINKEDSERVICE_COUNT deployed"
echo "  ✅ Datasets: $DATASET_COUNT deployed"
echo ""

print_step "🎯 NEXT STEPS:"
echo ""
echo "1. 🧪 START TEST TRIGGER (10-minute intervals):"
echo "   ./Manage-ETL-Triggers.ps1 -Action start-test"
echo "   OR"
echo "   az datafactory trigger start --resource-group $RESOURCE_GROUP --factory-name $DATA_FACTORY_NAME --name ETL_Test_10Min_Trigger"
echo ""

echo "2. 📊 MONITOR PIPELINE RUNS:"
echo "   # Azure Portal"
echo "   https://portal.azure.com/#@/resource/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.DataFactory/factories/$DATA_FACTORY_NAME"
echo ""
echo "   # Azure CLI"
echo "   az datafactory pipeline-run query-by-factory --resource-group $RESOURCE_GROUP --factory-name $DATA_FACTORY_NAME --last-updated-after '$(date -u -d '1 hour ago' +'%Y-%m-%dT%H:%M:%SZ')'"
echo ""

echo "3. 🛑 STOP TEST TRIGGER WHEN DONE:"
echo "   ./Manage-ETL-Triggers.ps1 -Action stop-test"
echo ""

echo "4. 📅 SWITCH TO PRODUCTION (Daily 8:00 AM EST):"
echo "   ./Manage-ETL-Triggers.ps1 -Action switch-to-daily"
echo ""

print_warning "IMPORTANT REMINDERS:"
echo "  • Test trigger runs every 10 minutes - monitor costs!"
echo "  • Both triggers start in STOPPED state"
echo "  • Only activate one trigger at a time to avoid overlaps"
echo "  • Monitor Azure Container Instance costs"
echo "  • Ensure Python scripts are accessible in container environment"
echo ""

print_step "📖 For detailed usage instructions, see:"
echo "   📄 infra/data_factory/README.md"
echo ""

print_success "Azure Data Factory ETL Pipeline is ready for use! 🚀"
