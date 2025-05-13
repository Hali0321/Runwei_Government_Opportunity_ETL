# Grants.gov API Azure Integration

This project integrates the Grants.gov API with Azure services to extract, process, and store grant data for analytics and consumption.

## Architecture

- **Azure Functions**: Handle API calls, data processing, and storage
- **Azure Logic Apps**: Orchestrate the workflow
- **Azure Cosmos DB**: Store detailed grant information
- **Azure SQL Database**: Store structured grant data for analytics
- **Azure Blob Storage**: Store raw JSON data from API calls

## Setup

1. **Deploy Infrastructure**:

   ```bash
   az login
   az group create --name GrantsGovAPI --location eastus
   az deployment group create --resource-group GrantsGovAPI --template-file infrastructure/main.bicep --parameters infrastructure/parameters.json
   ```

2. **Deploy Functions**:

   ```bash
   cd src/GrantsGovFunctions
   func azure functionapp publish GrantsGovFunctions0
   ```

3. **Deploy SQL Schema**:

   ```bash
   sqlcmd -S grantssql0.database.windows.net -d GrantsDB -U sqladmin -P "P@ssw0rd!" -i src/database/schema.sql
   ```

## Testing

Run tests using pytest:

```bash
pip install -r src/GrantsGovFunctions/requirements.txt
pip install pytest pytest-cov
pytest tests/
```

## Monitoring

Monitor the application using:
- Azure Application Insights
- Azure Monitor
- Power BI for data visualization

## Security

- All Azure resources use managed identities where possible
- RBAC is implemented for resource access
- SQL Server firewall is configured to only allow Azure services
