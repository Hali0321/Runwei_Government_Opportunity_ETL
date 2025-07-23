# Layer 3 - Final Opportunities

## Overview
Layer 3 transforms the cleaned grants data from Layer 2 into a final opportunities format suitable for applications and APIs.

## Architecture
```
CleanGrantsLayer2 → FinalOpportunities (Layer 3)
```

## Table Structure
- **Table Name**: `dbo.FinalOpportunities`
- **Records**: ~1,671 opportunities
- **Primary Key**: `ID` (OpportunityNumber)

## Key Features
- ✅ Azure SQL Database optimized
- ✅ Proper indexing for performance
- ✅ String length limits for Azure compatibility
- ✅ Industry categorization
- ✅ Featured opportunity flagging
- ✅ SEO-friendly slugs

## Column Mappings
| Layer 2 Column | Layer 3 Column | Transformation |
|---------------|----------------|----------------|
| OpportunityNumber | ID | Primary key |
| Title | Title | Truncated to 500 chars |
| EstimatedTotalFunding | AwardValue | Cleaned and formatted |
| AgencyName | Industry | Mapped to industry categories |
| Category | OpportunityTypeId | Numeric mapping |
| Deadline | Deadline | Formatted datetime |

## Usage

### Run Layer 3 Creation
```bash
cd /Users/dinghali/Desktop/Runwei/grants_gov_api_azure/layers/layer3_final_opportunities/scripts
python3 create_layer3_final_opportunities.py
```

### Query Final Opportunities
```sql
-- Count total opportunities
SELECT COUNT(*) FROM dbo.FinalOpportunities;

-- Get featured opportunities
SELECT * FROM dbo.FinalOpportunities WHERE IsFeatured = 'Yes';

-- Industry distribution
SELECT Industry, COUNT(*) FROM dbo.FinalOpportunities GROUP BY Industry;

-- High-value opportunities
SELECT TOP 10 ID, Title, AwardValue, Industry 
FROM dbo.FinalOpportunities 
WHERE AwardValue IS NOT NULL 
ORDER BY TRY_CAST(REPLACE(REPLACE(AwardValue, '$', ''), ',', '') AS DECIMAL) DESC;
```

## Performance Features
- **Filtered Indexes**: Only index non-NULL values
- **Optimized Data Types**: NVARCHAR with appropriate lengths
- **Batch Processing**: Handles large datasets efficiently
- **Azure SQL Compatibility**: Uses Azure-specific optimizations

## Industry Categories
1. Healthcare
2. Education  
3. Defense
4. Energy & Environment
5. Business & Commerce
6. Agriculture
7. Transportation
8. Arts & Humanities
9. Government (General)

## Next Steps
- Create API endpoints for FinalOpportunities
- Build search and filtering capabilities
- Implement recommendation engine
- Create analytics dashboards