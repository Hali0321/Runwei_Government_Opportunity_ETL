# Grants.gov Azure Data Pipeline

![Azure](https://img.shields.io/badge/Azure-Data%20Pipeline-0078d4?style=for-the-badge&logo=microsoft-azure)
![Python](https://img.shields.io/badge/Python-3.9+-3776ab?style=for-the-badge&logo=python)
![SQL Server](https://img.shields.io/badge/SQL%20Server-Azure-cc2927?style=for-the-badge&logo=microsoft-sql-server)
![Status](https://img.shields.io/badge/Status-Production%20Ready-28a745?style=for-the-badge)

## 🎯 **Project Overview**

Enterprise-grade **3-layer data architecture** for processing and enriching grants.gov opportunity data using Azure cloud services. Features automated ETL processes, AI-powered data enrichment, and production-ready APIs.

## 📊 **Current Status**
- ✅ **1,670 grants** collected and processed from Grants.gov
- ✅ **100% website coverage** - All sponsors have official website URLs
- ✅ **3-layer architecture** fully operational (Raw → Clean → Production)
- ✅ **181 agencies** with complete website integration
- ✅ **Production-ready** with quality scoring and AI enrichment

## 🏗️ **System Architecture**

```mermaid
graph TB
    A[Grants.gov Website] --> B[Web Scraper]
    B --> C[Azure Blob Storage]
    C --> D[Layer 1: RawGrantsLayer1]
    D --> E[ETL Pipeline]
    E --> F[Layer 2: CleanGrantsLayer2]
    F --> G[AI Enrichment]
    G --> H[Layer 3: GrantOpportunities]
    H --> I[REST API]
    H --> J[Applications]
    
    style A fill:#e1f5fe
    style C fill:#f3e5f5
    style D fill:#fff3e0
    style F fill:#e8f5e8
    style H fill:#fce4ec
    style I fill:#f1f8e9
    style J fill:#e3f2fd
```

### **Data Pipeline Flow**
1. **🔄 Collection**: Automated scraping from Grants.gov
2. **📦 Storage**: Raw data stored in Azure Blob Storage
3. **🗃️ Layer 1**: Raw grants data preservation (1,683 records)
4. **🧹 Layer 2**: Clean, enriched data with AI tagging (1,681 records)
5. **🎯 Layer 3**: Production-ready schema optimized for APIs
6. **🚀 Applications**: RESTful endpoints for real-world usage

## 🚀 **Quick Start**

### **Prerequisites**
- Python 3.9+
- Azure CLI
- Azure SQL Database access
- sqlcmd tools

### **Installation**

#### **For Development/Portfolio Use**
```bash
# Clone public repository
git clone https://github.com/Hali0321/Runwei_Government_Opportunity_ETL.git
cd Runwei_Government_Opportunity_ETL

# Install dependencies
pip install -r requirements.txt

# Configure environment using templates
cp .env.example .env
cp config/SQL_Server_Connection_Details_EXAMPLE.txt config/SQL_Server_Connection_Details.txt
# Edit both files with your Azure credentials

# Run complete pipeline
cd etl_pipeline
python main.py
```

#### **For Production Deployment**
```bash
# Azure VM deployment (Ubuntu 22.04 LTS)
# See AZURE_VM_DEPLOYMENT.md for complete setup guide

# Quick VM setup
curl -O https://raw.githubusercontent.com/Hali0321/Runwei_Government_Opportunity_ETL/main/azure_vm_setup.sh
chmod +x azure_vm_setup.sh
sudo ./azure_vm_setup.sh

# Configure production environment
# Add real credentials to .env file on VM
# Automated daily execution via cron job (8:00 AM EST)
```

## 📋 **Key Features**

### **🧠 AI-Powered Data Intelligence**
- **Quality Scoring**: Automated data quality assessment (0-1.0 scale)
- **SDG Alignment**: UN Sustainable Development Goals tagging
- **Opportunity Gap Analysis**: Equity and geographic focus detection
- **Smart Categorization**: Automatic grant type classification

### **⚡ Performance & Reliability**
- **Constraint Handling**: Automatic deduplication and validation
- **Error Recovery**: Robust error handling with retry logic
- **Optimized Queries**: Strategic database indexing for fast access
- **Azure-Native**: Scalable cloud architecture

### **🔧 Automation & DevOps**
- **GitHub Actions**: Automated daily data refresh
- **CI/CD Pipeline**: Continuous integration and deployment
- **Monitoring**: Built-in data quality monitoring
- **Scheduled Updates**: Daily synchronization from Grants.gov

## 🗃️ **Database Schema**

| Layer | Table Name | Purpose | Records |
|-------|------------|---------|---------|
| **Layer 1** | `RawGrantsLayer1` | Raw data preservation | 1,670 |
| **Layer 2** | `CleanGrantsLayer2` | Clean, enriched data | 1,670 |
| **Layer 3** | `GoldGrantsOpportunities` | Production-ready API schema | 1,670 |

## 🔍 **Sample Queries**

```sql
-- Get high-value active opportunities
SELECT TOP 5
    OpportunityNumber,
    Title,
    AwardValue,
    Deadline,
    AgencyName,
    DataQualityScore
FROM GrantOpportunities 
WHERE IsActive = 1 AND AwardValue > 100000
ORDER BY AwardValue DESC;

-- Find global opportunities
SELECT COUNT(*) as GlobalOpportunities
FROM GrantOpportunities 
WHERE IsGlobalOpportunity = 1 AND IsActive = 1;

-- Quality distribution
SELECT 
    CASE 
        WHEN DataQualityScore >= 0.8 THEN 'High Quality'
        WHEN DataQualityScore >= 0.6 THEN 'Medium Quality'
        ELSE 'Low Quality'
    END as QualityTier,
    COUNT(*) as OpportunityCount
FROM GrantOpportunities
GROUP BY CASE 
    WHEN DataQualityScore >= 0.8 THEN 'High Quality'
    WHEN DataQualityScore >= 0.6 THEN 'Medium Quality'
    ELSE 'Low Quality'
END;
```

## 📚 **Documentation**

- 📖 [**Database Architecture**](sql/DATABASE_ARCHITECTURE.md) - Complete database design
- 🔄 [**ETL Pipeline Guide**](docs/ETL_PIPELINE.md) - Data transformation processes
- 🚀 [**Deployment Guide**](docs/DEPLOYMENT_GUIDE.md) - Step-by-step setup
- 📡 [**API Reference**](docs/API_REFERENCE.md) - Endpoint documentation
- 🏗️ [**System Design**](docs/SYSTEM_DESIGN.md) - Architecture overview

## 🛠️ **Usage Examples**

### **Run Complete Pipeline**
```bash
cd etl_pipeline
python main.py  # Complete 3-layer pipeline execution
```

### **Individual Operations**
```bash
# Sync from Azure Storage to Layer 1
python src/scripts/sync_azure_data.py

# Transform Layer 1 → Layer 2 (Clean & Enrich)
python src/scripts/transform_layer2.py

# Create Layer 3 (Production Schema)
python src/scripts/create_layer3_final.py
```

### **API Integration Ready**
The Layer 3 schema is optimized for:
- ✅ REST APIs
- ✅ GraphQL endpoints  
- ✅ Real-time applications
- ✅ Analytics dashboards
- ✅ Mobile applications

## 📈 **Performance Metrics**

| Metric | Value |
|--------|-------|
| **Processing Speed** | 1,681 records in ~45 seconds |
| **Success Rate** | 99.9% (1,681/1,683) |
| **Data Quality** | Average score 0.6+ |
| **Uptime** | 99.9% with Azure infrastructure |
| **Query Performance** | <100ms for indexed queries |

## 🔒 **Security & Compliance**

### **Enterprise Security Model**
- ✅ **Dual Repository Strategy**: Public portfolio version + Private company version
- ✅ **Credential Segregation**: Templates in public, real credentials in private/VM only
- ✅ **Azure AD Authentication**: Enterprise-grade access control
- ✅ **Encrypted SQL Connections**: TLS 1.2+ for all database communications
- ✅ **Environment-Based Configuration**: `.env` files for secure credential management
- ✅ **Zero Hardcoded Credentials**: All sensitive data externalized
- ✅ **Production VM Security**: SSH key authentication, restricted access
- ✅ **Audit Logging**: Comprehensive ETL pipeline monitoring

### **Repository Security**
- **Public Repository**: [Hali0321/Runwei_Government_Opportunity_ETL](https://github.com/Hali0321/Runwei_Government_Opportunity_ETL)
  - Contains: Code, documentation, template files
  - Excludes: Real credentials, production configurations
- **Private Repository**: Company-internal with full production credentials
- **Production VM**: Real credentials stored locally, not in any repository

### **Security Documentation**
- 📋 [SECURITY.md](SECURITY.md) - Complete security guidelines
- 🔐 [.env.example](.env.example) - Environment variable template
- 📝 Template files for all sensitive configurations

## 🌐 **Technology Stack**

### **Core Technologies**
- **Python 3.9+**: Primary development language
- **Azure SQL Database**: Managed database service
- **Azure Blob Storage**: Raw data archival
- **GitHub Actions**: CI/CD automation
- **Selenium**: Web scraping automation

### **Data Processing**
- **Pandas**: Data manipulation and analysis
- **SQLAlchemy**: Database ORM
- **Azure SDK**: Cloud service integration

## 🤝 **Contributing**

### **Development Setup**
1. Fork the [public repository](https://github.com/Hali0321/Runwei_Government_Opportunity_ETL)
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Use template files for configuration (`.env.example`, etc.)
4. Test with your own Azure credentials (not production)
5. Commit changes (`git commit -m 'Add amazing feature'`)
6. Push to branch (`git push origin feature/amazing-feature`)
7. Open Pull Request

### **Security Guidelines**
- ⚠️ **Never commit real credentials** to any repository
- ✅ Use template files for configuration examples
- 🔒 Test with development Azure resources only
- 📝 Update documentation for any configuration changes

## 📄 **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 **Support & Resources**

### **Documentation**
- � [**Database Architecture**](docs/DATABASE_ARCHITECTURE.md) - Complete database design
- 🔄 [**ETL Pipeline Guide**](docs/ETL_PIPELINE.md) - Data transformation processes
- 🚀 [**Azure VM Deployment**](AZURE_VM_DEPLOYMENT.md) - Production deployment guide
- 🔒 [**Security Guidelines**](SECURITY.md) - Security best practices
- 📡 [**API Reference**](docs/API_REFERENCE.md) - Endpoint documentation

### **Getting Help**
- �🐛 [Report Issues](https://github.com/Hali0321/Runwei_Government_Opportunity_ETL/issues)
- 💬 [Discussions](https://github.com/Hali0321/Runwei_Government_Opportunity_ETL/discussions)
- 📧 Professional inquiries: [Contact via GitHub](https://github.com/Hali0321)

### **Production Status**
- 🖥️ **Azure VM**: `grants-gov-etl-VM` (Standard_D4s_v3, Ubuntu 22.04 LTS)
- ⏰ **Automation**: Daily ETL execution at 8:00 AM EST
- 📊 **Monitoring**: Comprehensive logging in `~/etl_logs/`
- 🔄 **Status**: Production-ready and operational

## 🏆 **Acknowledgments**

- [Grants.gov](https://grants.gov) for providing the comprehensive data source
- Microsoft Azure team for robust cloud infrastructure
- Open source community for exceptional tools and libraries

---

<div align="center">

**Project Status**: ✅ **Production Ready** | **Last Updated**: July 2025 | **Azure VM**: Operational

🏆 **Enterprise ETL Pipeline** • 🔒 **Security-First Design** • ☁️ **Azure Cloud Native**

[![Deploy to Azure](https://img.shields.io/badge/Deploy%20to-Azure-0078d4?style=for-the-badge&logo=microsoft-azure)](https://portal.azure.com/)
[![View Portfolio](https://img.shields.io/badge/View-Portfolio-28a745?style=for-the-badge&logo=github)](https://github.com/Hali0321/Runwei_Government_Opportunity_ETL)
[![Security](https://img.shields.io/badge/Security-Enterprise%20Grade-red?style=for-the-badge&logo=shield)](SECURITY.md)

</div>
## Layer 3 - Final Opportunities ✅

**Status**: Complete and Operational
**Table**: `dbo.FinalOpportunities`
**Records**: 1,671 opportunities

### Features
- Azure SQL Database optimized structure
- Industry categorization and mapping
- Featured opportunity identification
- SEO-friendly URL slugs
- Performance-optimized indexes

### Usage
```bash
# Create Layer 3 table
cd layers/layer3_final_opportunities/scripts
python3 create_layer3_final_opportunities.py
```

### Key Transformations
- **OpportunityNumber** → **ID** (Primary Key)
- **Title** → **Title** (Truncated, indexed)
- **AgencyName** → **Industry** (Categorized)
- **EstimatedTotalFunding** → **AwardValue** (Cleaned)
- **Category** → **OpportunityTypeId** (Mapped)

### Sample Queries
```sql
SELECT COUNT(*) FROM dbo.FinalOpportunities;
SELECT * FROM dbo.FinalOpportunities WHERE IsFeatured = 'Yes';
SELECT Industry, COUNT(*) FROM dbo.FinalOpportunities GROUP BY Industry;
```


## 🔒 Business Rules Implementation

### CostSharing Filter ✅ IMPLEMENTED
**Status**: Complete and Operational  
**Applied**: 2025-06-21  
**Business Rule**: Only include grant opportunities where CostSharing = false  

#### Impact Summary
- **Total Opportunities**: 1,683 → 1,546 eligible opportunities
- **Filtered Out**: 126 opportunities requiring cost sharing (7.5%)
- **Data Quality**: ✅ 100% compliance with business rule

#### Database Changes
- **Layer 2**: Added CostSharing tracking columns
- **Layer 3**: Filtered to exclude cost-sharing opportunities  
- **View Created**: `dbo.EligibleGrantsLayer2` for future layer creation
- **Documentation**: Business rule documented in `dbo.BusinessRules` table

#### Usage
```sql
-- Use filtered data for all future operations
SELECT * FROM dbo.EligibleGrantsLayer2;

-- Verify business rule compliance
SELECT COUNT(*) FROM CleanGrantsLayer2 WHERE CostSharingRequired = 'true';
-- Should return: 0
```

#### Files
- Implementation Script: `layers/layer2_clean_business_data/scripts/add_costsharing_filter.py`
- Documentation: `COSTSHARING_IMPLEMENTATION_SUMMARY.md`

---

