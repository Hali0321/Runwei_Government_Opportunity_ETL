# 🚀 Setup Guide for New Team Members

## 📋 Prerequisites

### Required Software
- **Python 3.9+** with pip
- **Git** for version control
- **Azure CLI** (optional, for cloud deployment)
- **SQL Server Management Studio** or **Azure Data Studio** (for database access)

### Required Access
- Access to Azure SQL Database: `grants-gov-sql-server.database.windows.net`
- Database: `GrantsGovDB`
- Credentials: Contact team lead for database credentials

## 🔧 Initial Setup

### 1. Clone Repository
```bash
git clone https://github.com/Hali0321/Runwei_Government_Opportunity_ETL.git
cd Runwei_Government_Opportunity_ETL
```

### 2. Create Python Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment
```bash
# Copy environment template
cp .env.template .env

# Edit .env file with actual credentials (contact team lead)
# You'll need:
# - Azure SQL Database credentials
# - Azure Storage connection strings (if using)
```

## 🏗️ Project Structure

```
grants_gov_api_azure/
├── etl_pipeline/           # Main ETL processing pipeline
│   ├── main.py            # Entry point for complete pipeline
│   ├── scheduler.py       # Daily scheduler (runs at 8AM EST)
│   └── layers/
│       ├── bronze/        # Layer 1: Raw data collection
│       ├── silver/        # Layer 2: Data cleaning & enrichment
│       └── gold/          # Layer 3: Production-ready data
├── azure_functions/       # Azure Functions for cloud deployment
├── infrastructure/        # Infrastructure as Code (Bicep)
├── config/               # Configuration files
├── docs/                 # Documentation
└── logs/                 # Application logs
```

## 🔄 Running the Pipeline

### Full Pipeline Execution
```bash
# Run complete ETL pipeline (all 3 layers)
cd etl_pipeline
python main.py
```

### Individual Layer Execution
```bash
# Layer 1: Collect raw data from Grants.gov
cd etl_pipeline/layers/bronze/scripts
python run_layer1.py

# Layer 2: Clean and enrich data
cd etl_pipeline/layers/silver/scripts
python run_layer2.py

# Layer 3: Create production-ready tables
cd etl_pipeline/layers/gold/scripts
python run_layer3.py
```

### Scheduled Execution
```bash
# Run daily scheduler (8AM EST execution)
cd etl_pipeline
python scheduler.py
```

## 📊 Database Tables

### Layer 1 (Bronze): Raw Data
- **`RawGrantsLayer1`**: Raw grants data from Grants.gov

### Layer 2 (Silver): Clean & Enriched Data
- **`CleanGrantsLayer2`**: Cleaned and enriched grants with:
  - ✅ AI-powered data quality scoring
  - ✅ SDG alignment tagging
  - ✅ Opportunity gap analysis
  - ✅ Rolling deadline detection
  - ✅ Agency website integration
  - ✅ Award value standardization

### Layer 3 (Gold): Production Ready
- **`GoldGrantsOpportunities`**: Production API-ready data with:
  - ✅ Top 2000 highest quality opportunities
  - ✅ Optimized schema for API consumption
  - ✅ Performance indexes
  - ✅ Website URLs for all sponsors

## 🧪 Verification & Testing

### Database Verification
```sql
-- Check data counts
SELECT 'Layer 1' as Layer, COUNT(*) as RecordCount FROM RawGrantsLayer1
UNION ALL
SELECT 'Layer 2' as Layer, COUNT(*) as RecordCount FROM CleanGrantsLayer2
UNION ALL
SELECT 'Layer 3' as Layer, COUNT(*) as RecordCount FROM GoldGrantsOpportunities;

-- Check website integration
SELECT COUNT(*) as Total, COUNT(SponsorESOWebsite) as WithWebsites 
FROM GoldGrantsOpportunities;
```

### Integration Verification
```bash
# Run Layer 2 to Layer 3 integration verification
cd etl_pipeline/layers/gold/scripts
python verify_layer2_layer3_integration.py
```

## 🚀 Key Features

### AI-Powered Enhancements
- **Quality Scoring**: 0-10 scale data quality assessment
- **SDG Alignment**: UN Sustainable Development Goals mapping
- **Gap Analysis**: Opportunity gap resource detection
- **Rolling Detection**: Smart deadline analysis (103 rolling opportunities detected)
- **Website Integration**: 181 agencies mapped to official websites

### Performance Features
- **Optimized Queries**: Strategic database indexing
- **Error Handling**: Robust retry logic
- **Batch Processing**: Efficient data processing
- **Progress Tracking**: Detailed logging and monitoring

## 📈 Current Status

- **✅ 1,670 grants** successfully processed
- **✅ 100% website coverage** (all sponsors have official website URLs)
- **✅ 181 unique agencies** with complete profiles
- **✅ 103 rolling opportunities** identified (6.17% of total)
- **✅ Production ready** with comprehensive testing

## 🔧 Troubleshooting

### Common Issues

1. **Database Connection Timeout**
   ```bash
   # Check network connectivity
   sqlcmd -S grants-gov-sql-server.database.windows.net -d GrantsGovDB -U username -P password -Q "SELECT 1"
   ```

2. **Missing Dependencies**
   ```bash
   pip install -r requirements.txt --upgrade
   ```

3. **Permission Issues**
   - Contact team lead for database credentials
   - Ensure Azure SQL firewall allows your IP

### Logs Location
- Application logs: `logs/etl_pipeline.log`
- Individual script logs: `etl_pipeline/layers/*/scripts/__pycache__/*.log`

## 📞 Support

- **Technical Issues**: Contact development team lead
- **Database Access**: Contact database administrator
- **Azure Resources**: Contact cloud infrastructure team

## 🔗 Related Documentation

- [Database Architecture](docs/DATABASE_ARCHITECTURE.md)
- [ETL Pipeline Details](docs/ETL_PIPELINE.md)
- [Azure Infrastructure](infrastructure/README.md)

---

**Last Updated**: July 23, 2025  
**Version**: 2.0 (Post-reorganization)  
**Status**: Production Ready ✅
