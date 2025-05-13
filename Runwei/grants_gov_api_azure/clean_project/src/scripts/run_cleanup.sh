#!/bin/bash
echo "Setting connection string..."
export STORAGE_CONNECTION=$(az storage account show-connection-string \
  --name grantsgov58225 \
  --resource-group GrantsGovAPI \
  --query connectionString -o tsv)

echo "Running cleanup script..."
python /Users/dinghali/Desktop/Runwei/grants_gov_api_azure/clean_project/src/scripts/cleanup_old_grants.py
