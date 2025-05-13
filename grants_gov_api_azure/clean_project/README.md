# Grants.gov API Azure Integration

This project integrates with the Grants.gov API to retrieve grant information and store it in Azure Table Storage using Azure Functions.

## Components

- **Function App**: GrantsGovFunc60542
- **Storage Account**: grantsgov58225
- **Table Storage**: GrantDetails
- **Queue**: grants-processing

## Functions

1. **SearchGrants** (Timer Trigger): Searches for grants and adds IDs to the queue
2. **GetGrantDetails** (Queue Trigger): Retrieves grant details from Grants.gov API and stores in Table Storage
3. **ProcessGrantData** (Queue Trigger): Processes stored grant data for further analysis
4. **GrantsViewer** (HTTP Trigger): View and search grant data in HTML or JSON format
5. **ApiTester** (HTTP Trigger): Diagnostic function to test Grants.gov API endpoints directly
6. **HealthCheck** (HTTP Trigger): Monitors the health of the system and its components
7. **StorageTest** (HTTP Trigger): Tests Azure Storage connectivity and adds a test entry

## Local Testing Setup

Before running any scripts, set your Azure Storage connection string:

```bash
# Set up environment variables
./deploy/setup_env.sh