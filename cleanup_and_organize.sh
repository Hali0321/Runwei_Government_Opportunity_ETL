#!/bin/bash

echo "🧹 Azure Grants.gov Project Cleanup & Organization"
echo "=================================================="
echo "📅 Started: $(date)"
echo "🎯 Goal: Keep only essential files for future use"

# Set base directory
BASE_DIR="/Users/dinghali/Desktop/Runwei/grants_gov_api_azure"
cd "$BASE_DIR"

echo ""
echo "🗂️  Current project structure analysis..."
find . -type f -name "*.py" | wc -l | xargs echo "Python files:"
find . -type f -name "*.sh" | wc -l | xargs echo "Shell scripts:"
find . -type f -name "*.md" | wc -l | xargs echo "Documentation files:"

echo ""
echo "🧹 Step 1: Remove debug and test files..."

# Remove debug files
rm -f debug_pipeline_setup.sh 2>/dev/null && echo "✅ Removed debug_pipeline_setup.sh"
rm -f check_table_structure.py 2>/dev/null && echo "✅ Removed check_table_structure.py"

# Remove old/broken versions
rm -f layers/layer2_clean_business_data/scripts/comprehensive_layer2_enhancement.py 2>/dev/null && echo "✅ Removed old Layer 2 script"
rm -f layers/layer3_final_opportunities/scripts/simple_layer3_selection_fixed.py 2>/dev/null && echo "✅ Removed old Layer 3 script"
rm -f comprehensive_layer2_enhancement_fixed.py 2>/dev/null && echo "✅ Removed misplaced Layer 2 script"

# Remove log files
rm -f daily_collection_*.log 2>/dev/null && echo "✅ Removed old log files"

echo ""
echo "🗂️  Step 2: Organize essential files..."

# Create clean directory structure
mkdir -p production/{scripts,docs,config}
mkdir -p archive/old_versions

# Move essential scripts to production folder
echo "📁 Moving essential scripts to production folder..."

# Layer 2 - Keep the working version
cp layers/layer2_clean_business_data/scripts/comprehensive_layer2_enhancement_fixed.py production/scripts/layer2_enhancement.py 2>/dev/null && echo "✅ Saved Layer 2 enhancement script"

# Layer 3 - Keep the streamlined version  
cp layers/layer3_final_opportunities/scripts/simple_layer3_selection_streamlined.py production/scripts/layer3_selection.py 2>/dev/null && echo "✅ Saved Layer 3 selection script"

# Keep the pipeline controller
cp run_complete_pipeline.py production/scripts/pipeline_controller.py 2>/dev/null && echo "✅ Saved pipeline controller"

echo ""
echo "📋 Step 3: Create production documentation..."

cat > production/docs/README.md << 'DOC_EOF'
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
DOC_EOF

echo "✅ Created production documentation"

# Create configuration template
cat > production/config/database_config.template << 'CONFIG_EOF'
# Azure SQL Database Configuration Template
# Copy to database_config.py and update with actual values

SERVER = "grants-gov-sql-server.database.windows.net"
DATABASE = "GrantsGovDB" 
USERNAME = "grantsadmin"
PASSWORD = "your_password_here"

# Quality thresholds
MIN_QUALITY_SCORE = 6.0
FEATURED_THRESHOLD = 9.0
CONFIG_EOF

echo "✅ Created configuration template"

echo ""
echo "��️  Step 4: Archive old structure..."

# Move old layer structure to archive
if [ -d "layers" ]; then
    mv layers archive/old_versions/ 2>/dev/null && echo "✅ Archived old layers directory"
fi

# Archive other development files
mv complete_pipeline_setup.sh archive/old_versions/ 2>/dev/null && echo "✅ Archived setup script"
mv layer2_check_status.py archive/old_versions/ 2>/dev/null && echo "✅ Archived status check script"
mv layer2_data_flow_check.py archive/old_versions/ 2>/dev/null && echo "✅ Archived data flow check"

echo ""
echo "🧹 Step 5: Final cleanup..."

# Remove empty directories
find . -type d -empty -delete 2>/dev/null

# Create quick start script
cat > run_production_pipeline.sh << 'PIPELINE_EOF'
#!/bin/bash

echo "🚀 Azure Grants.gov Production Pipeline"
echo "======================================"
echo "📅 Started: $(date)"

# Run Layer 2 Enhancement
echo ""
echo "🧹 Step 1: Running Layer 2 Enhancement..."
python3 production/scripts/layer2_enhancement.py

if [ $? -eq 0 ]; then
    echo "✅ Layer 2 completed successfully"
    
    # Run Layer 3 Selection
    echo ""
    echo "🎯 Step 2: Running Layer 3 Selection..."
    python3 production/scripts/layer3_selection.py
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "🎊 SUCCESS! Production pipeline completed!"
        echo "📊 Check dbo.FinalOpportunities for results"
    else
        echo "❌ Layer 3 failed"
        exit 1
    fi
else
    echo "❌ Layer 2 failed"
    exit 1
fi
PIPELINE_EOF

chmod +x run_production_pipeline.sh
echo "✅ Created run_production_pipeline.sh"

echo ""
echo "🎊 CLEANUP COMPLETE!"
echo "==================="
echo ""
echo "�� KEPT ESSENTIAL FILES:"
echo "├── production/"
echo "│   ├── scripts/"
echo "│   │   ├── layer2_enhancement.py      # Layer 2 data enhancement"
echo "│   │   ├── layer3_selection.py        # Layer 3 streamlined selection"
echo "│   │   └── pipeline_controller.py     # Original pipeline controller"
echo "│   ├── docs/"
echo "│   │   └── README.md                  # Production documentation"
echo "│   └── config/"
echo "│       └── database_config.template   # Configuration template"
echo "├── run_production_pipeline.sh         # Quick start script"
echo "└── archive/                           # Old files safely stored"
echo ""
echo "🚀 TO RUN PRODUCTION PIPELINE:"
echo "   ./run_production_pipeline.sh"
echo ""
echo "📋 TO VIEW RESULTS:"
echo "   sqlcmd -S grants-gov-sql-server.database.windows.net -d GrantsGovDB -U grantsadmin -P 'Grant\$Admin2024!' -Q 'SELECT COUNT(*) FROM dbo.FinalOpportunities;'"
echo ""
echo "✅ Project is now clean and organized for future use!"

