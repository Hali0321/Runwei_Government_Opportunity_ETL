# Azure Data Factory ETL Pipeline - Grants.gov Data Processing

This directory contains the complete Azure Data Factory configuration for automating the Grants.gov ETL pipeline with Bronze → Silver → Gold layer processing.

## 🎯 Overview

The ETL pipeline automates the complete data processing workflow:

1. **Bronze Layer** - Extract raw data from Grants.gov using `run_layer1.py`
2. **Silver Layer** - Transform and enhance data using `run_layer2_complete.py`
3. **Gold Layer** - Create curated data using `run_layer3.py`

## 📁 File Structure

```
infra/data_factory/
├── main.bicep                           # Main Bicep template for Data Factory deployment
├── Manage-ETL-Triggers.ps1             # PowerShell script for trigger management
├── pipeline_grants_etl.json            # Main ETL pipeline definition
├── pipeline_bronze_layer.json          # Bronze layer pipeline
├── pipeline_silver_layer.json          # Silver layer pipeline
├── pipeline_gold_layer.json            # Gold layer pipeline
├── trigger_test_10min.json             # Test trigger (10-minute intervals)
├── trigger_daily_8am.json              # Production trigger (daily 8:00 AM EST)
├── dataset_bronze_layer.json           # Bronze layer dataset
├── dataset_silver_layer.json           # Silver layer dataset
├── dataset_gold_layer.json             # Gold layer dataset
├── linkedservice_azuresql.json         # Azure SQL Database linked service
├── linkedservice_function.json         # Azure Function App linked service
└── README.md                           # This file
```

## 🚀 Deployment Instructions

### Prerequisites

1. **Azure CLI** installed and logged in
2. **Azure SQL Database** with GrantsGovDB database
3. **Azure Key Vault** with database password stored as `GrantsGovDB-Password`
4. **Azure Function App** for running ETL scripts (grants-etl-functions.azurewebsites.net)
5. **Appropriate Azure permissions** for creating Data Factory resources

### Step 1: Deploy Data Factory Infrastructure

```bash
# Navigate to the data factory directory
cd infra/data_factory

# Deploy using Azure CLI
az deployment group create \
  --resource-group grants-gov-rg \
  --template-file main.bicep \
  --parameters \
    dataFactoryName="df-grants-gov-etl" \
    sqlServerName="grants-gov-sql-server" \
    sqlDatabaseName="GrantsGovDB" \
    keyVaultName="kv-grants-gov" \
    environment="test"
```

### Step 2: Verify Deployment

```bash
# Check Data Factory status
az datafactory show \
  --resource-group grants-gov-rg \
  --factory-name df-grants-gov-etl

# List all pipelines
az datafactory pipeline list \
  --resource-group grants-gov-rg \
  --factory-name df-grants-gov-etl \
  --output table

# List all triggers
az datafactory trigger list \
  --resource-group grants-gov-rg \
  --factory-name df-grants-gov-etl \
  --output table
```

## ⚙️ Trigger Management

### Using PowerShell Script (Recommended)

The `Manage-ETL-Triggers.ps1` script provides easy trigger management:

```powershell
# Start 10-minute test trigger
.\Manage-ETL-Triggers.ps1 -Action start-test

# Check trigger status
.\Manage-ETL-Triggers.ps1 -Action status

# Stop test trigger
.\Manage-ETL-Triggers.ps1 -Action stop-test

# Switch to daily production schedule
.\Manage-ETL-Triggers.ps1 -Action switch-to-daily

# Start daily trigger
.\Manage-ETL-Triggers.ps1 -Action start-daily

# Stop daily trigger
.\Manage-ETL-Triggers.ps1 -Action stop-daily
```

### Using Azure CLI

#### Test Trigger (10-minute intervals)

```bash
# Start test trigger
az datafactory trigger start \
  --resource-group grants-gov-rg \
  --factory-name df-grants-gov-etl \
  --name ETL_Test_10Min_Trigger

# Stop test trigger
az datafactory trigger stop \
  --resource-group grants-gov-rg \
  --factory-name df-grants-gov-etl \
  --name ETL_Test_10Min_Trigger
```

#### Daily Production Trigger (8:00 AM EST)

```bash
# Start daily trigger
az datafactory trigger start \
  --resource-group grants-gov-rg \
  --factory-name df-grants-gov-etl \
  --name ETL_Daily_8AM_Trigger

# Stop daily trigger
az datafactory trigger stop \
  --resource-group grants-gov-rg \
  --factory-name df-grants-gov-etl \
  --name ETL_Daily_8AM_Trigger
```

## 📊 Pipeline Configuration

### Main ETL Pipeline: `GrantsGov_ETL_Pipeline`

The main pipeline orchestrates the complete ETL process with dependency management:

```
Bronze Layer Extract
       ↓ (on success)
Silver Layer Transform  
       ↓ (on success)
Gold Layer Load
       ↓ (on success)
Success Notification
```

### Individual Layer Pipelines

1. **Bronze_Layer_Pipeline**
   - Executes `run_layer1.py` script
   - Validates data in `RawGrantsLayer1` table
   - Timeout: 1 hour

2. **Silver_Layer_Pipeline**
   - Executes `run_layer2_complete.py` script
   - Validates data in `CleanGrantsLayer2` table
   - Timeout: 2 hours

3. **Gold_Layer_Pipeline**
   - Executes `run_layer3.py` script
   - Validates data in `GoldGrantsOpportunities` table
   - Timeout: 1.5 hours

### Trigger Configurations

#### Test Trigger: `ETL_Test_10Min_Trigger`
- **Frequency**: Every 10 minutes
- **Timezone**: Eastern Standard Time
- **Purpose**: Testing and development
- **Parameters**: `executionMode: "test"`

#### Production Trigger: `ETL_Daily_8AM_Trigger`
- **Frequency**: Daily at 8:00 AM
- **Timezone**: Eastern Standard Time
- **Purpose**: Production data processing
- **Parameters**: `executionMode: "production"`

## 🔧 Configuration Details

### Dependencies and Error Handling

- **Sequential Execution**: Each layer waits for the previous layer to complete successfully
- **Retry Logic**: Each activity retries up to 2 times with 30-60 second intervals
- **Timeout Protection**: Appropriate timeouts for each layer
- **Validation**: Data validation queries after each layer

### Security Configuration

- **Managed Identity**: Data Factory uses system-assigned managed identity
- **Key Vault Integration**: Database passwords stored securely in Azure Key Vault
- **Encrypted Connections**: All SQL connections use mandatory encryption
- **RBAC**: Least-privilege access for Data Factory to Key Vault

### Container Configuration

The pipeline uses Azure Container Instances to run Python scripts:

- **Image**: `python:3.11-slim`
- **Resources**: 2 CPU cores, 4 GB memory
- **Dependencies**: Automatic installation of required packages (pyodbc, sqlalchemy, etc.)
- **Environment**: Azure SQL connection details injected securely

## 📈 Monitoring and Troubleshooting

### Monitor Pipeline Runs

```bash
# List recent pipeline runs
az datafactory pipeline-run query-by-factory \
  --resource-group grants-gov-rg \
  --factory-name df-grants-gov-etl \
  --last-updated-after "2025-07-21T00:00:00Z" \
  --output table

# Get specific pipeline run details
az datafactory pipeline-run show \
  --resource-group grants-gov-rg \
  --factory-name df-grants-gov-etl \
  --run-id <pipeline-run-id>
```

### Monitor Trigger Runs

```bash
# List trigger runs
az datafactory trigger-run query-by-factory \
  --resource-group grants-gov-rg \
  --factory-name df-grants-gov-etl \
  --last-updated-after "2025-07-21T00:00:00Z" \
  --output table
```

### Common Issues and Solutions

1. **Pipeline Timeout**
   - Check individual script execution times
   - Increase timeout values if necessary
   - Monitor Azure Container Instance performance

2. **Authentication Errors**
   - Verify Key Vault access permissions
   - Check Data Factory managed identity
   - Ensure database credentials are correct

3. **Script Execution Failures**
   - Check script logs in Azure Container Instance
   - Verify script dependencies and environment
   - Ensure database connectivity

## 🎛️ Customization Options

### Changing Trigger Frequency

To modify trigger schedules, update the `recurrence` section in the trigger configuration:

```json
{
  "recurrence": {
    "frequency": "Hour",    // or "Day", "Week", "Month"
    "interval": 2,          // every 2 hours
    "timeZone": "Eastern Standard Time",
    "schedule": {
      "hours": [8, 14, 20], // specific hours
      "minutes": [0]        // specific minutes
    }
  }
}
```

### Adding Additional Validation

Extend the validation queries in each pipeline to include additional data quality checks:

```json
{
  "sqlReaderQuery": "SELECT COUNT(*) as RecordCount, AVG(DataQualityScore) as AvgQuality, COUNT(CASE WHEN Title IS NULL THEN 1 END) as MissingTitles FROM CleanGrantsLayer2"
}
```

### Environment-Specific Configuration

Use Bicep parameters to deploy different configurations for different environments:

```bash
# Development environment
az deployment group create \
  --template-file main.bicep \
  --parameters environment="dev" dataFactoryName="df-grants-gov-dev"

# Production environment  
az deployment group create \
  --template-file main.bicep \
  --parameters environment="prod" dataFactoryName="df-grants-gov-prod"
```

## 🚨 Important Notes

### Cost Management

- **Test Trigger**: Running every 10 minutes can generate significant costs
- **Always disable test trigger** when not actively testing
- **Monitor Azure Container Instance usage** and associated costs
- **Use daily trigger for production** to minimize unnecessary runs

### Avoiding Pipeline Overlaps

- **Dependency Conditions**: Each layer waits for the previous layer to succeed
- **Timeout Settings**: Prevent long-running pipelines from overlapping
- **Trigger State Management**: Only one trigger should be active at a time

### Data Quality Assurance

- **Validation Queries**: Each layer includes data validation
- **Quality Thresholds**: Pipeline parameters include quality score thresholds
- **Error Notifications**: Failed pipelines can trigger alerts

## 🔄 Migration from Test to Production

When ready to move from testing to production:

1. **Stop test trigger**:
   ```powershell
   .\Manage-ETL-Triggers.ps1 -Action stop-test
   ```

2. **Verify data quality** from test runs

3. **Switch to daily schedule**:
   ```powershell
   .\Manage-ETL-Triggers.ps1 -Action switch-to-daily
   ```

4. **Monitor first few production runs**

5. **Set up alerting** for production pipeline failures

## 📞 Support and Maintenance

- **Pipeline Logs**: Available in Azure Data Factory monitoring
- **Container Logs**: Available in Azure Container Instance logs
- **Database Monitoring**: Monitor Azure SQL Database performance
- **Cost Monitoring**: Set up cost alerts for Azure Data Factory usage

---

For questions or issues, refer to the Azure Data Factory documentation or contact the development team.
