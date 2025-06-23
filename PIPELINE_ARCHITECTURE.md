# Azure Data Pipeline Architecture

## 🏗️ Three-Layer Architecture

### Layer 1: Raw Data Ingestion
- **Table**: `RawGrantsLayer1`
- **Purpose**: Raw API data from Grants.gov

### Layer 2: Comprehensive Enhancement  
- **Table**: `CleanGrantsLayer2`
- **Purpose**: Complete data enhancement and quality scoring
- **Features**: Visual assets, summaries, formatted values, quality scores

### Layer 3: Simple Selection
- **Table**: `FinalOpportunities`
- **Purpose**: Selection of high-quality records for production
- **Criteria**: DataQualityScore >= 6.0, ReadyForLayer3 = 1

## 🔄 Execution Flow
```
Raw Data → Enhanced Data → Selected Output
Layer 1  →    Layer 2    →    Layer 3
```

## 🎯 Quality Scoring (0-10 scale)
- **8.0+**: Excellent - Production Ready
- **6.0-7.9**: Good - Enhanced and ready
- **< 6.0**: Needs improvement

## 🚀 Azure SQL Database Integration
- Optimized for Azure SQL Database
- Uses sqlcmd for reliable connections
- Proper error handling and timeouts
- Transaction-based operations

## 📊 Data Flow
1. **Layer 1**: Raw data ingestion from Grants.gov API
2. **Layer 2**: Comprehensive enhancement with quality scoring
3. **Layer 3**: Simple selection of production-ready records

## 🎯 Benefits
- Clean separation of concerns
- Quality-driven data processing
- Azure-optimized performance
- Flexible selection criteria
