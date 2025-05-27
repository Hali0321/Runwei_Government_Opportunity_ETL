#!/bin/bash

echo "=== Grants.gov API Azure Integration - Deployment Script ==="
echo "This script deploys all Azure Functions to your Function App"

# Get Function App name from parameters or prompt
FUNCTION_APP=$1
if [ -z "$FUNCTION_APP" ]; then
  # Try to get from environment
  if [ -z "$FUNCTION_APP" ]; then
    # Check if there's a deployment file to read from
    if [ -f "../function-deployment.txt" ]; then
      FUNCTION_APP=$(grep "Function App:" ../function-deployment.txt | cut -d' ' -f3)
    fi
  fi
  
  # If still not set, prompt the user
  if [ -z "$FUNCTION_APP" ]; then
    echo "Please enter your Function App name:"
    read FUNCTION_APP
  fi
fi

echo "Deploying to Function App: $FUNCTION_APP"

# Navigate to the functions directory
cd "$(dirname "$0")/../src/functions/GrantsGovFunctions"

# Deploy all functions
echo "Starting deployment..."
func azure functionapp publish "$FUNCTION_APP" --force

echo "Deployment complete!"
echo ""
echo "You can view your functions at: https://$FUNCTION_APP.azurewebsites.net/"
echo "Try the GrantsViewer at: https://$FUNCTION_APP.azurewebsites.net/api/grantsviewer?format=html"
