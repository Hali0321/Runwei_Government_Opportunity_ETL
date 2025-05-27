#!/bin/bash
cd /Users/dinghali/Desktop/Runwei/grants_gov_api_azure/clean_project/src/functions/GrantsGovFunctions
echo "Deploying functions to Azure..."
func azure functionapp publish GrantsGovFunc60542 --force
echo "Deployment complete."
