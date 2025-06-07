# Runwei Government Opportunity ETL

A comprehensive Azure-based ETL solution for processing federal grant opportunities from Grants.gov into a standardized company database schema.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Grants.gov    │───▶│  Azure Functions │───▶│ Azure Storage   │
│      API        │    │   ETL Pipeline   │    │   Tables        │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │ Company Database │
                       │     Schema       │
                       └──────────────────┘
```

## ✨ Features

### **Data Collection**
- 🔄 Automated grant opportunity collection from Grants.gov API
- 📊 Real-time data ingestion with pagination support
- 🛡️ Error handling and retry mechanisms
- 📅 Scheduled data collection via Azure Functions

### **Data Processing** 
- 🔧 Transform raw grant data to company database schema
- 🏷️ Standardized field mapping and data validation
- 💰 Financial data parsing and normalization
- 🎯 Opportunity categorization and tagging

### **Data Storage**
- ☁️ Azure Table Storage for scalable data persistence
- 📋 Multiple output formats (JSON, CSV, Azure Tables)
- 🔍 Searchable and queryable data structure
- 📈 Historical data tracking and versioning

### **Monitoring & Diagnostics**
- 🩺 Storage connectivity diagnostics
- 📊 Data quality monitoring
- 🚨 Error tracking and alerting
- 📋 Processing statistics and reporting

## 🚀 Quick Start

### Prerequisites
- Azure subscription
- Azure Functions Core Tools
- Python 3.9+
- Git

### Deployment

```bash
# Clone the repository
git clone https://github.com/Hali0321/Runwei_Government_Opportunity_ETL.git
cd Runwei_Government_Opportunity_ETL

# Install dependencies
pip install -r requirements.txt

# Deploy to Azure Functions
cd src/azure_functions
func azure functionapp publish your-function-app-name --python
```

## 📡 API Endpoints

### **GrantsCollector** 
```
GET /api/grantscollector?limit=100&agency=NSF
```
- Collects grant opportunities from Grants.gov API
- Stores raw data in Azure Storage

### **DataProcessing**
```
GET /api/dataprocessor?source=azure_table&format=json&limit=50
```
- Transforms grant data to company schema
- Supports multiple output formats

### **StorageDiagnostic**
```
GET /api/storagediagnostic
```
- Provides storage connectivity and table information
- Useful for troubleshooting and monitoring

## 🗂️ Data Schema

### **Input Schema (Grants.gov)**
- Opportunity Number, Title, Agency Information
- Funding Details, Deadlines, Eligibility Requirements
- CFDA Numbers, Contact Information

### **Output Schema (Company Database)**
- Standardized opportunity fields
- Financial information (Award Value, Cash Award)
- Geographic and eligibility data
- Application URLs and contact details

## 🛠️ Configuration

Environment variables required:
```bash
STORAGE_CONNECTION_STRING=your_azure_storage_connection_string
GRANTS_GOV_API_KEY=your_grants_gov_api_key (optional)
```

## 📊 Monitoring

The solution includes comprehensive monitoring:
- Real-time processing statistics
- Data quality metrics
- Error tracking and alerting
- Storage utilization monitoring

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support and questions:
- Create an issue in this repository
- Check the documentation in the `/docs` folder
- Review the diagnostic endpoints for troubleshooting