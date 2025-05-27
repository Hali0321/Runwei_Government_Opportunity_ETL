#!/bin/bash

# Generate unique values
TIMESTAMP=$(date +%s | cut -c 6-14)
FUNCTION_APP="GrantsGovFunc${TIMESTAMP}"

# Get storage account name from previous deployment
STORAGE_NAME=$(cat storage-deployment-*.txt | grep "Storage Account:" | cut -d' ' -f3)
echo "Using storage account: ${STORAGE_NAME}"
echo "New Function App: ${FUNCTION_APP}"

# Deploy function app - make sure --os-type Linux is included
echo "Deploying function app..."
az functionapp create \
  --resource-group GrantsGovAPI \
  --consumption-plan-location eastus \
  --name ${FUNCTION_APP} \
  --storage-account ${STORAGE_NAME} \
  --runtime python \
  --os-type Linux \
  --runtime-version 3.9 \
  --functions-version 4

if [ $? -ne 0 ]; then
  echo "Function app creation failed!"
  exit 1
fi

echo "Function App deployed successfully: ${FUNCTION_APP}"
echo "Function App: ${FUNCTION_APP}" >> function-deployment-${TIMESTAMP}.txt
echo "Storage Account: ${STORAGE_NAME}" >> function-deployment-${TIMESTAMP}.txt