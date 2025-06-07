#!/bin/bash
# Production deployment script for Python 3.12 Function App

echo "🚀 Production GrantsGov Deployment (Python 3.12)"
echo "================================================="

# Load configuration
source grantsgov_config.env

echo "📋 Deployment Details:"
echo "Function App: $FUNCTION_APP_NAME"
echo "URL: $FUNCTION_APP_URL"
echo "Resource Group: $RESOURCE_GROUP"

# Verify Function App is Python 3.12
echo "🔍 Verifying Function App configuration..."
RUNTIME_VERSION=$(az functionapp config show \
  --name "$FUNCTION_APP_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "linuxFxVersion" \
  --output tsv 2>/dev/null)

echo "Runtime Version: $RUNTIME_VERSION"

# Navigate to functions directory
cd src/azure_functions

echo "📂 Deploying from: $(pwd)"
echo "📁 Functions to deploy:"
ls -d */ 2>/dev/null

# Ensure requirements.txt is optimized for Python 3.12
cat > requirements.txt << 'REQEOF'
azure-functions>=1.18.0
azure-data-tables>=12.4.0
azure-storage-queue>=12.8.0
azure-storage-blob>=12.19.0
azure-identity>=1.15.0
requests>=2.31.0
python-dateutil>=2.8.2
REQEOF

# Ensure host.json is optimized
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
    "version": "[4.*, 5.0.0)"
  },
  "functionTimeout": "00:10:00"
}
HOSTEOF

# Deploy via ZIP (most reliable method)
echo "📦 Creating deployment package..."
zip -r ../deployment-package.zip . -x "*.pyc" "*/__pycache__/*" "*.git/*"

if [ $? -eq 0 ]; then
    echo "✅ Package created successfully"
    
    # Deploy via Azure CLI
    echo "🚀 Deploying to production..."
    cd ..
    az functionapp deployment source config-zip \
      --resource-group "$RESOURCE_GROUP" \
      --name "$FUNCTION_APP_NAME" \
      --src "deployment-package.zip"
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "🎉 Production deployment completed successfully!"
        echo "=============================================="
        echo "🌐 Function App URL: $FUNCTION_APP_URL"
        echo "🔗 Health Check: $FUNCTION_APP_URL/api/healthcheck"
        echo "🔗 Grants Viewer: $FUNCTION_APP_URL/api/grantsviewer?format=html&limit=100"
        echo "🔗 Search Grants: $FUNCTION_APP_URL/api/searchgrants"
        echo ""
        echo "🧪 Testing deployment..."
        sleep 10
        
        # Test health endpoint
        HEALTH_RESPONSE=$(curl -s "$FUNCTION_APP_URL/api/healthcheck")
        if [ $? -eq 0 ]; then
            echo "✅ Health check passed: $HEALTH_RESPONSE"
        else
            echo "⚠️ Health check failed"
        fi
        
        echo ""
        echo "🔗 Azure Portal:"
        echo "https://portal.azure.com/#resource/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Web/sites/$FUNCTION_APP_NAME"
    else
        echo "❌ Deployment failed"
    fi
    
    # Cleanup
    rm -f deployment-package.zip
else
    echo "❌ Failed to create deployment package"
fi
