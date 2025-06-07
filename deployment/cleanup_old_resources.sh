#!/bin/bash
# Clean up old Azure resources

echo "🧹 Cleaning up old Azure resources..."

# Load current configuration
source grantsgov_config.env

echo "Current Function App: $FUNCTION_APP_NAME"
echo "Keeping: grantsgov-func-py312, grantsgov225756, grantsgov-vault-5756"

# Resources to remove
OLD_FUNCTION_APP="grantsgov-func-5756"
OLD_STORAGE="grantsgov225141"
OLD_KEYVAULT="grantsgov-vault-5141"

echo ""
echo "🗑️ Removing old resources..."

# Remove old Function App (5756)
echo "Removing old Function App: $OLD_FUNCTION_APP"
if az functionapp show --name "$OLD_FUNCTION_APP" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    az functionapp delete --name "$OLD_FUNCTION_APP" --resource-group "$RESOURCE_GROUP" --yes
    echo "✅ Removed $OLD_FUNCTION_APP"
else
    echo "ℹ️ $OLD_FUNCTION_APP already removed or doesn't exist"
fi

# Remove old storage account (225141)
echo "Removing old storage account: $OLD_STORAGE"
if az storage account show --name "$OLD_STORAGE" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    az storage account delete --name "$OLD_STORAGE" --resource-group "$RESOURCE_GROUP" --yes
    echo "✅ Removed $OLD_STORAGE"
else
    echo "ℹ️ $OLD_STORAGE already removed or doesn't exist"
fi

# Remove old Key Vault (5141)
echo "Removing old Key Vault: $OLD_KEYVAULT"
if az keyvault show --name "$OLD_KEYVAULT" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    az keyvault delete --name "$OLD_KEYVAULT" --resource-group "$RESOURCE_GROUP"
    echo "✅ Removed $OLD_KEYVAULT"
else
    echo "ℹ️ $OLD_KEYVAULT already removed or doesn't exist"
fi

echo ""
echo "🎉 Cleanup completed!"
echo "📋 Remaining resources:"
az resource list --resource-group "$RESOURCE_GROUP" --output table
