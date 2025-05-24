#!/bin/bash
echo "Setting up environment for Grants.gov API Azure project"

# Ensure Azure CLI is logged in
echo "Checking Azure login status..."
az account show > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Please login to Azure first:"
    az login
fi

# Set environment variables
echo "Setting up environment variables..."
export STORAGE_CONNECTION=$(az storage account show-connection-string \
  --name grantsgov58225 \
  --resource-group GrantsGovAPI \
  --query connectionString -o tsv)

# Verify connection string was set properly
if [ -z "$STORAGE_CONNECTION" ]; then
    echo "Error: Failed to retrieve storage connection string"
    exit 1
fi

echo "Storage connection string successfully retrieved."
echo "Length of connection string: ${#STORAGE_CONNECTION}"
echo "Environment is ready for development."

# Output help
echo ""
echo "To use these environment variables in other terminal sessions:"
echo "source ./deploy/setup_env.sh"
