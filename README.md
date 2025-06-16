# 🚀 Grants.gov Azure Data Processing Pipeline

[![Azure](https://img.shields.io/badge/Azure-Cloud-blue)](https://azure.microsoft.com/)
[![Python](https://img.shields.io/badge/Python-3.8+-green)](https://python.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

Enterprise-grade automated grants.gov data collection and processing system built on Microsoft Azure cloud infrastructure.

## 🎯 Overview

This system provides a complete end-to-end solution for automatically fetching, processing, and analyzing grants.gov opportunities using Azure cloud services. The system implements a robust 3-layer data architecture for optimal data processing and business intelligence.

## 🏗️ Architecture

\`\`\`
grants.gov → Selenium Automation → Azure Table Storage → Azure SQL Database
                                            ↓
                                3-Layer Data Pipeline:
                                ┌─ RawGrantsLayer1 (Source Data)
                                ├─ RunweiFormatLayer2 (Business Logic) 
                                └─ BusinessIntelligenceLayer3 (Analytics)
\`\`\`

## ✨ Features

- **🤖 Automated Data Collection**: Selenium-powered automation for grants.gov
- **☁️ Azure Table Storage**: Scalable NoSQL storage for raw grant data  
- **🗄️ Azure SQL Database**: Structured data with 3-layer architecture
- **⚡ Batch Processing**: Efficiently handles 1,600+ records with error recovery
- **📊 Business Intelligence**: Competitive scoring and opportunity recommendations
- **🔍 Data Validation**: Comprehensive type checking and sanitization
- **📈 Progress Tracking**: Real-time processing status and performance metrics

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Azure subscription with Table Storage and SQL Database
- Chrome/Firefox browser for Selenium

### Installation
\`\`\`bash
git clone https://github.com/your-username/grants_gov_api_azure.git
cd grants_gov_api_azure
pip install -r requirements.txt
\`\`\`

### Configuration
1. Copy \`.env.template\` to \`.env\`
2. Copy \`config/azure_config.template\` to \`config/azure_config.env\`
3. Update with your Azure connection strings and database credentials

### Usage
\`\`\`bash
# Run the main application
python main.py

# Or run specific components
python -m src.data_sync.sync_azure_to_sql
python -m src.data_collection.bulk_update_grantdetails
\`\`\`

## 📊 Data Pipeline

### Layer 1: RawGrantsLayer1
- **Purpose**: Raw data from grants.gov via Azure Table Storage
- **Volume**: 1,683+ grant opportunities  
- **Schema**: 37 fields including titles, amounts, agencies, dates

### Layer 2: RunweiFormatLayer2  
- **Purpose**: Business-ready formatted data
- **Processing**: Data transformation, standardization, categorization

### Layer 3: BusinessIntelligenceLayer3
- **Purpose**: Analytics and intelligent recommendations
- **Features**: Competitive scoring (0-100), opportunity ranking, priority classification

## 🛠️ Development

### Project Structure
\`\`\`
grants_gov_api_azure/
├── src/
│   ├── data_collection/     # Grants.gov data collection
│   ├── data_sync/          # Azure to SQL synchronization
│   ├── data_processing/    # Data transformation
│   ├── azure_functions/    # Azure Functions
│   └── utils/              # Utility functions
├── sql/
│   ├── schemas/            # Database schemas
│   └── deployment/         # Deployment scripts
├── config/                 # Configuration templates
├── deployment/             # Azure infrastructure scripts
├── docs/                   # Documentation
└── tests/                  # Unit tests
\`\`\`

## 📈 Performance Metrics

- **Processing Speed**: ~11 records/second
- **Success Rate**: >95% with error recovery
- **Data Volume**: 1,683+ records processed efficiently  
- **Batch Size**: 25 records per batch (optimized)

## 🔧 Configuration

Key configuration options in \`.env\`:

\`\`\`env
AZURE_STORAGE_CONNECTION_STRING=your_connection_string
SQL_SERVER=your-sql-server.database.windows.net
BATCH_SIZE=25
DEBUG_MODE=False
\`\`\`

## 🤝 Contributing

1. Fork the repository  
2. Create a feature branch (\`git checkout -b feature/amazing-feature\`)  
3. Commit your changes (\`git commit -m 'Add amazing feature'\`)  
4. Push to the branch (\`git push origin feature/amazing-feature'\`)  
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🔗 Links

- [grants.gov Official Site](https://www.grants.gov)
- [Azure Documentation](https://docs.microsoft.com/azure/)

---

**Built with ❤️ for the grants community using Azure Cloud**

## 📚 Complete Documentation

This README provides a quick start guide. For comprehensive documentation:

### **📖 Core Documentation**
- **[Database Architecture](sql/DATABASE_ARCHITECTURE.md)** - 3-layer database design and schema
- **[ETL Pipeline](docs/ETL_PIPELINE.md)** - Data collection and transformation processes
- **[API Reference](docs/api/API_REFERENCE.md)** - Complete endpoint documentation

### **🚀 Setup & Deployment**  
- **[Deployment Guide](docs/deployment/DEPLOYMENT_GUIDE.md)** - Step-by-step Azure setup
- **[System Architecture](docs/architecture/SYSTEM_DESIGN.md)** - Technical architecture overview

### **🔧 Development**
- **[Contributing Guidelines](CONTRIBUTING.md)** - Development workflow and standards
- **[Change Log](CHANGELOG.md)** - Version history and updates
