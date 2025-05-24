#!/bin/bash
echo "=== Grants.gov API Connection-Resilient Collector ==="

# Check if az CLI is installed
if ! command -v az &> /dev/null; then
    echo "Error: Azure CLI is not installed."
    echo "Please install it using one of these methods:"
    echo "  • macOS: brew install azure-cli"
    echo "  • Visit: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
    exit 1
fi

# Try to get current Azure account
echo "Checking Azure connection..."
if ! az account show &> /dev/null; then
    echo "You're not logged in to Azure. Starting login process..."
    if ! az login; then
        echo "Login failed. Please try again manually with 'az login'"
        exit 1
    fi
fi

echo "Getting storage connection string..."
export STORAGE_CONNECTION=$(az storage account show-connection-string \
  --name grantsgov58225 \
  --resource-group GrantsGovAPI \
  --query connectionString -o tsv)

if [ -z "$STORAGE_CONNECTION" ]; then
    echo "Failed to get storage connection string."
    echo "Please check if the storage account 'grantsgov58225' exists and you have access to it."
    exit 1
fi

echo "Running connection-resilient grant collector..."
python /Users/dinghali/Desktop/Runwei/grants_gov_api_azure/clean_project/src/scripts/connection_resilient_collector.py

echo "Collection process completed."
