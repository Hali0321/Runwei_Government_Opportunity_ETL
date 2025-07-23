@description('Azure Data Factory for Grants.gov ETL Pipeline')
@minLength(3)
@maxLength(63)
param dataFactoryName string = 'df-grants-gov-etl'

@description('Location for all resources')
param location string = resourceGroup().location

@description('Azure SQL Server name')
param sqlServerName string = 'grants-gov-sql-server'

@description('Azure SQL Database name')
param sqlDatabaseName string = 'GrantsGovDB'

@description('Storage Account name')
param storageAccountName string = 'grantsgov225756'

@description('Key Vault name for storing secrets')
param keyVaultName string = 'kv-grants-gov'

@description('Environment tag')
@allowed(['dev', 'test', 'prod'])
param environment string = 'test'

// Data Factory
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  tags: {
    Environment: environment
    Project: 'GrantsGov-ETL'
    Purpose: 'Bronze-Silver-Gold-ETL'
  }
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
    globalParameters: {
      environment: {
        type: 'string'
        value: environment
      }
      sqlServerName: {
        type: 'string'
        value: sqlServerName
      }
      sqlDatabaseName: {
        type: 'string'
        value: sqlDatabaseName
      }
      storageAccountName: {
        type: 'string'
        value: storageAccountName
      }
    }
  }
}

// Key Vault Linked Service
resource keyVaultLinkedService 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  name: 'KeyVault_LinkedService'
  parent: dataFactory
  properties: {
    type: 'AzureKeyVault'
    typeProperties: {
      baseUrl: 'https://${keyVaultName}${az.environment().suffixes.keyvaultDns}/'
    }
  }
}

// Azure SQL Database Linked Service
resource azureSqlLinkedService 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  name: 'AzureSQL_LinkedService'
  parent: dataFactory
  properties: {
    type: 'AzureSqlDatabase'
    typeProperties: {
      server: '${sqlServerName}${az.environment().suffixes.sqlServerHostname}'
      database: sqlDatabaseName
      encrypt: 'mandatory'
      trustServerCertificate: false
      authenticationType: 'SQL'
      userName: 'grantsadmin'
      password: {
        type: 'AzureKeyVaultSecret'
        store: {
          referenceName: keyVaultLinkedService.name
          type: 'LinkedServiceReference'
        }
        secretName: 'GrantsGovDB-Password'
      }
    }
  }
}

// Azure Function App Linked Service (for ETL script execution)
resource functionLinkedService 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  name: 'AzureFunction_LinkedService'
  parent: dataFactory
  properties: {
    type: 'AzureFunction'
    typeProperties: {
      functionAppUrl: 'https://grants-etl-functions.azurewebsites.net'
      authentication: 'MSI'
    }
  }
}

// Datasets
resource bronzeDataset 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: 'AzureSQL_RawGrantsLayer1'
  parent: dataFactory
  properties: {
    type: 'AzureSqlTable'
    linkedServiceName: {
      referenceName: azureSqlLinkedService.name
      type: 'LinkedServiceReference'
    }
    schema: []
    typeProperties: {
      schema: 'bronze'
      table: 'raw_grants'
    }
  }
}

resource silverDataset 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: 'AzureSQL_ProcessedGrantsLayer2'
  parent: dataFactory
  properties: {
    type: 'AzureSqlTable'
    linkedServiceName: {
      referenceName: azureSqlLinkedService.name
      type: 'LinkedServiceReference'
    }
    schema: []
    typeProperties: {
      schema: 'silver'
      table: 'processed_grants'
    }
  }
}

resource goldDataset 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: 'AzureSQL_AnalyticsLayer3'
  parent: dataFactory
  properties: {
    type: 'AzureSqlTable'
    linkedServiceName: {
      referenceName: azureSqlLinkedService.name
      type: 'LinkedServiceReference'
    }
    schema: []
    typeProperties: {
      schema: 'gold'
      table: 'analytics_grants'
    }
  }
}

// Pipelines
resource bronzePipeline 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: 'Bronze_Layer_Pipeline'
  parent: dataFactory
  properties: {
    description: 'Bronze Layer - Raw data collection from Grants.gov website'
    activities: [
      {
        name: 'RunBronzeETL'
        type: 'AzureFunctionActivity'
        typeProperties: {
          functionName: 'run_layer1'
          method: 'POST'
          headers: {
            'Content-Type': 'application/json'
          }
          body: {
            environment: '@pipeline().globalParameters.environment'
            sqlServer: '@pipeline().globalParameters.sqlServerName'
            sqlDatabase: '@pipeline().globalParameters.sqlDatabaseName'
          }
        }
        linkedServiceName: {
          referenceName: functionLinkedService.name
          type: 'LinkedServiceReference'
        }
      }
    ]
    parameters: {}
    variables: {}
    folder: {
      name: 'ETL_Pipelines'
    }
  }
}

resource silverPipeline 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: 'Silver_Layer_Pipeline'
  parent: dataFactory
  properties: {
    description: 'Silver Layer - Data processing and standardization'
    activities: [
      {
        name: 'RunSilverETL'
        type: 'AzureFunctionActivity'
        typeProperties: {
          functionName: 'run_layer2_complete'
          method: 'POST'
          headers: {
            'Content-Type': 'application/json'
          }
          body: {
            environment: '@pipeline().globalParameters.environment'
            sqlServer: '@pipeline().globalParameters.sqlServerName'
            sqlDatabase: '@pipeline().globalParameters.sqlDatabaseName'
          }
        }
        linkedServiceName: {
          referenceName: functionLinkedService.name
          type: 'LinkedServiceReference'
        }
      }
    ]
    parameters: {}
    variables: {}
    folder: {
      name: 'ETL_Pipelines'
    }
  }
}

resource goldPipeline 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: 'Gold_Layer_Pipeline'
  parent: dataFactory
  properties: {
    description: 'Gold Layer - Analytics and aggregation processing'
    activities: [
      {
        name: 'RunGoldETL'
        type: 'AzureFunctionActivity'
        typeProperties: {
          functionName: 'run_layer3'
          method: 'POST'
          headers: {
            'Content-Type': 'application/json'
          }
          body: {
            environment: '@pipeline().globalParameters.environment'
            sqlServer: '@pipeline().globalParameters.sqlServerName'
            sqlDatabase: '@pipeline().globalParameters.sqlDatabaseName'
          }
        }
        linkedServiceName: {
          referenceName: functionLinkedService.name
          type: 'LinkedServiceReference'
        }
      }
    ]
    parameters: {}
    variables: {}
    folder: {
      name: 'ETL_Pipelines'
    }
  }
}

// Main ETL Pipeline - Orchestrates Bronze → Silver → Gold
resource mainEtlPipeline 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: 'Main_ETL_Pipeline'
  parent: dataFactory
  properties: {
    description: 'Main ETL Pipeline - Orchestrates Bronze → Silver → Gold layer processing'
    activities: [
      {
        name: 'ExecuteBronzeLayer'
        type: 'ExecutePipeline'
        typeProperties: {
          pipeline: {
            referenceName: bronzePipeline.name
            type: 'PipelineReference'
          }
          waitOnCompletion: true
        }
      }
      {
        name: 'ExecuteSilverLayer'
        type: 'ExecutePipeline'
        dependsOn: [
          {
            activity: 'ExecuteBronzeLayer'
            dependencyConditions: ['Succeeded']
          }
        ]
        typeProperties: {
          pipeline: {
            referenceName: silverPipeline.name
            type: 'PipelineReference'
          }
          waitOnCompletion: true
        }
      }
      {
        name: 'ExecuteGoldLayer'
        type: 'ExecutePipeline'
        dependsOn: [
          {
            activity: 'ExecuteSilverLayer'
            dependencyConditions: ['Succeeded']
          }
        ]
        typeProperties: {
          pipeline: {
            referenceName: goldPipeline.name
            type: 'PipelineReference'
          }
          waitOnCompletion: true
        }
      }
    ]
    parameters: {}
    variables: {}
    folder: {
      name: 'ETL_Orchestration'
    }
  }
}

// Triggers
resource testTrigger 'Microsoft.DataFactory/factories/triggers@2018-06-01' = {
  name: 'TestTrigger_Every10Minutes'
  parent: dataFactory
  properties: {
    type: 'ScheduleTrigger'
    typeProperties: {
      recurrence: {
        frequency: 'Minute'
        interval: 10
        timeZone: 'Eastern Standard Time'
        startTime: '2024-01-01T00:00:00Z'
      }
    }
    pipelines: [
      {
        pipelineReference: {
          referenceName: mainEtlPipeline.name
          type: 'PipelineReference'
        }
        parameters: {}
      }
    ]
  }
}

resource dailyTrigger 'Microsoft.DataFactory/factories/triggers@2018-06-01' = {
  name: 'DailyTrigger_8AM_EST'
  parent: dataFactory
  properties: {
    type: 'ScheduleTrigger'
    typeProperties: {
      recurrence: {
        frequency: 'Day'
        interval: 1
        timeZone: 'Eastern Standard Time'
        startTime: '2024-01-01T08:00:00Z'
        schedule: {
          hours: [8]
          minutes: [0]
        }
      }
    }
    pipelines: [
      {
        pipelineReference: {
          referenceName: mainEtlPipeline.name
          type: 'PipelineReference'
        }
        parameters: {}
      }
    ]
  }
}

// Outputs
output dataFactoryName string = dataFactory.name
output dataFactoryId string = dataFactory.id
output dataFactoryPrincipalId string = dataFactory.identity.principalId
output testTriggerName string = testTrigger.name
output dailyTriggerName string = dailyTrigger.name
output bronzePipelineName string = bronzePipeline.name
output silverPipelineName string = silverPipeline.name
output goldPipelineName string = goldPipeline.name
output mainEtlPipelineName string = mainEtlPipeline.name
