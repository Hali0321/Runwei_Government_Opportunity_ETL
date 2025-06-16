# 📚 Documentation Hub

Welcome to the comprehensive documentation for the **Grants.gov Azure Data Processing Pipeline**.

## 🎯 **Quick Navigation**

### **🆕 New to the Project?**
- 👈 **Start Here**: [Main README](../README.md) - Project overview and quick start
- 🏗️ **Architecture**: [System Design](SYSTEM_DESIGN.md) - How everything works together
- 🗃️ **Database**: [Database Architecture](../sql/DATABASE_ARCHITECTURE.md) - Data layer design

### **🚀 Getting Started**
- 📖 **Setup Guide**: [Deployment Guide](DEPLOYMENT_GUIDE.md) - Step-by-step installation
- 🔧 **Configuration**: [Configuration Guide](CONFIGURATION.md) - Environment setup
- ✅ **Verification**: [Testing Guide](TESTING.md) - Verify your setup

### **👩‍💻 Developer Resources**
- 🔄 **ETL Pipeline**: [Pipeline Documentation](ETL_PIPELINE.md) - Data transformation processes
- 📡 **API Reference**: [API Documentation](API_REFERENCE.md) - Endpoint specifications
- 🧪 **Examples**: [Code Examples](EXAMPLES.md) - Sample implementations

### **🎯 Specific Use Cases**
- 📊 **Analytics**: Query examples and dashboard setup
- 🌐 **API Integration**: REST API usage patterns
- 🔍 **Data Quality**: Understanding quality scores and enrichment
- 🚨 **Troubleshooting**: Common issues and solutions

## 📑 **Documentation Map**

```
docs/
├── README.md                    # This navigation file
├── SYSTEM_DESIGN.md            # System architecture overview
├── ETL_PIPELINE.md             # Data transformation processes
├── API_REFERENCE.md            # Complete API documentation
├── DEPLOYMENT_GUIDE.md         # Step-by-step deployment
├── CONFIGURATION.md            # Environment and settings
├── TESTING.md                  # Verification and testing
├── EXAMPLES.md                 # Code samples and use cases
└── TROUBLESHOOTING.md          # Common issues and solutions
```

## 🏗️ **Architecture at a Glance**

Our **3-layer data architecture** processes grants.gov data through:

1. **🗃️ Layer 1**: Raw data preservation (1,683 records)
2. **🧹 Layer 2**: Clean, enriched data (1,681 records, 99.9% success)
3. **🎯 Layer 3**: Production-ready API schema

## 📊 **Current Status**

| Component | Status | Records |
|-----------|--------|---------|
| **Data Collection** | ✅ Active | 1,683 collected |
| **Layer 1 (Raw)** | ✅ Complete | 1,683 stored |
| **Layer 2 (Clean)** | ✅ Complete | 1,681 processed |
| **Layer 3 (Production)** | ✅ Ready | Active grants |
| **API Endpoints** | ✅ Available | REST ready |

## 🎯 **Most Common Tasks**

### **For Developers**
1. [Set up development environment](DEPLOYMENT_GUIDE.md#development-setup)
2. [Run the ETL pipeline](ETL_PIPELINE.md#running-the-pipeline)
3. [Query the database](API_REFERENCE.md#database-queries)
4. [Integrate with APIs](API_REFERENCE.md#rest-endpoints)

### **For Data Analysts**
1. [Understanding data quality scores](ETL_PIPELINE.md#data-quality)
2. [Working with enriched data](ETL_PIPELINE.md#ai-enrichment)
3. [Sample analytical queries](EXAMPLES.md#analytical-queries)
4. [Export data for analysis](API_REFERENCE.md#data-export)

### **For System Administrators**
1. [Deploy to production](DEPLOYMENT_GUIDE.md#production-deployment)
2. [Monitor pipeline health](TROUBLESHOOTING.md#monitoring)
3. [Backup and recovery](DEPLOYMENT_GUIDE.md#backup-strategy)
4. [Scale for higher volume](SYSTEM_DESIGN.md#scalability)

---

**Need help?** Check [Troubleshooting](TROUBLESHOOTING.md) or [create an issue](https://github.com/yourusername/grants_gov_api_azure/issues).
