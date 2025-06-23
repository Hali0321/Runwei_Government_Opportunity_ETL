# Azure Grants.gov Data Pipeline - Production

## 🚀 Overview
3-layer data pipeline for processing Grants.gov data in Azure SQL Database:
- **Layer 1**: Raw data collection (handled separately)
- **Layer 2**: Data enhancement and quality scoring
- **Layer 3**: Streamlined selection for production use

## 📊 Database Details
- **Server**: grants-gov-sql-server.database.windows.net
- **Database**: GrantsGovDB
- **Source Table**: CleanGrantsLayer2 
- **Target Table**: dbo.FinalOpportunities

## 🎯 Production Scripts

### Layer 2 Enhancement
```bash
python3 production/scripts/layer2_enhancement.py
```
- Adds visual assets (LogoUrl, CoverImage)
- Generates summaries from descriptions
- Formats award values
- Calculates quality scores (0-10 scale)
- Marks records ready for Layer 3

### Layer 3 Selection  
```bash
python3 production/scripts/layer3_selection.py
```
- Creates streamlined FinalOpportunities table
- 38 specific fields for application use
- Selects high-quality records (score >= 6.0)
- Production-ready data structure

## 📈 Expected Results
- **Source**: ~1,500+ enhanced records in CleanGrantsLayer2
- **Output**: All high-quality records in dbo.FinalOpportunities
- **Quality**: Average score 9+ with 100% Layer 3 readiness

## 🔧 Maintenance
- Run Layer 2 when source data is updated
- Run Layer 3 to refresh production table
- Monitor quality scores and adjust thresholds as needed

## 📋 Final Table Fields
ID, Title, Url, Deadline, AwardValue, CashAward, ContactEmail, LogoUrl, CoverImage, ShortDescription, Description, Eligibility, ContactNames, OpportunityTypeId, IndustryId, TargetCommunityId, TimeZone, DirectApplyLink, OpportunityGap, GlobalOpportunity, GlobalLocations, CountriesEligible, LocationDetails, SdgAlignment, EsoWebsite, ServiceProviderEso, ApprovalStatus, Cost, FinancialTerms, AreaOfFocus, Tags, Industry, Slug, AwardValueStr, DeadlineStr, DatePosted, OpportunityType, IsFeatured, PublishOnLinkedin, TargetCommunity, CreatedAt
