#!/bin/bash
cd /Users/dinghali/Desktop/Runwei/grants_gov_api_azure/clean_project/src/functions/GrantsGovFunctions
echo "Deploying updated GrantsViewer function..."
func azure functionapp publish GrantsGovFunc60542 --force
echo "Deployment complete. View your grants at:"
echo "https://grantsgovfunc60542.azurewebsites.net/api/grantsviewer?format=html&limit=1000"
