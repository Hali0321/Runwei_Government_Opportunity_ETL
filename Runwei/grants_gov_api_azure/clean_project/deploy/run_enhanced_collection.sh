#!/bin/bash
echo "Setting up environment for enhanced grants collection..."

# Get connection string from Azure CLI
if [ -z "$STORAGE_CONNECTION" ]; then
    echo "Getting storage connection string from Azure..."
    export STORAGE_CONNECTION=$(az storage account show-connection-string \
      --name grantsgov58225 \
      --resource-group GrantsGovAPI \
      --query connectionString -o tsv)
      
    if [ -z "$STORAGE_CONNECTION" ]; then
        echo "Failed to get connection string. Please set it manually:"
        echo "export STORAGE_CONNECTION=\"your_connection_string_here\""
        exit 1
    else
        echo "Connection string retrieved successfully."
    fi
fi

# Run the enhanced collection script
echo "Starting enhanced grant collection..."
python src/scripts/enhanced_grant_collector.py

echo "Enhanced collection process completed."
