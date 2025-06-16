#!/usr/bin/env python3
"""
Transform Layer 1 (Raw) to Layer 2 (Clean & Enriched) - FIXED VERSION
Includes data cleaning, standardization, and AI-powered enrichment
"""

import os
import subprocess
from datetime import datetime

def transform_layer1_to_layer2_fixed():
    """Transform raw grants data to clean, enriched Layer 2 - Fixed version"""
    
    print("🚀 LAYER 2 TRANSFORMATION PIPELINE (FIXED)")
    print("=" * 55)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # First, check if Layer 1 has data
        print("\n🔍 Checking Layer 1 data availability...")
        check_sql = "SELECT COUNT(*) as Layer1_Count FROM RawGrantsLayer1;"
        
        cmd = [
            "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
            "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
            "-Q", check_sql, "-C"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        print("📊 Layer 1 Status:")
        print(result.stdout)
        
        # Clear existing Layer 2 data
        print("\n🗑️ Clearing existing Layer 2 data...")
        clear_sql = "TRUNCATE TABLE CleanGrantsLayer2;"
        
        cmd = [
            "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
            "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
            "-Q", clear_sql, "-C"
        ]
        subprocess.run(cmd, check=True)
        print("✅ Layer 2 cleared successfully")
        
        # CORRECTED transformation SQL - removed problematic WHERE clause
        print("\n🔄 Executing Layer 1 → Layer 2 transformation (corrected)...")
        
        transform_sql = """
-- ===================================
-- LAYER 1 → LAYER 2 TRANSFORMATION (CORRECTED)
-- Fixed WHERE clause and column mapping issues
-- ===================================

INSERT INTO CleanGrantsLayer2 (
    OpportunityNumber,
    Title,
    Description,
    OpportunityURL,
    AgencyName,
    AgencyCode,
    AwardValue,
    AwardCeiling,
    AwardFloor,
    EstimatedTotalFunding,
    ExpectedAwards,
    FundingType,
    Deadline,
    PostedDate,
    EstimatedPostDate,
    EstimatedDueDate,
    Category,
    OpportunityType,
    Eligibility,
    EligibilityCategory,
    CountriesEligible,
    GlobalOpportunity,
    TimeZone,
    SDGTags,
    OpportunityGap,
    KeywordTags,
    DataQualityScore,
    ProcessingFlags,
    SourceLayerID,
    CFDANumbers,
    Package,
    Status,
    Version,
    ProcessedDate,
    ProcessedBy,
    DataVersion,
    CreatedDate,
    UpdatedDate
)
SELECT 
    -- Core identifiers (ensure OpportunityNumber is never null)
    CASE 
        WHEN OpportunityNumber IS NOT NULL AND LEN(LTRIM(RTRIM(OpportunityNumber))) > 0 
        THEN LTRIM(RTRIM(OpportunityNumber))
        ELSE 'UNKNOWN_' + CAST(ID as NVARCHAR(50))
    END as OpportunityNumber,
    
    CASE 
        WHEN Title IS NOT NULL AND LEN(LTRIM(RTRIM(Title))) > 0 
        THEN LTRIM(RTRIM(Title))
        ELSE 'Untitled Opportunity'
    END as Title,
    
    CASE 
        WHEN Description IS NOT NULL AND LEN(LTRIM(RTRIM(Description))) > 0 
        THEN LTRIM(RTRIM(Description))
        ELSE NULL
    END as Description,
    
    CASE 
        WHEN OpportunityURL IS NOT NULL AND LEN(LTRIM(RTRIM(OpportunityURL))) > 0 
        THEN LTRIM(RTRIM(OpportunityURL))
        ELSE NULL
    END as OpportunityURL,
    
    -- Agency information (with fallbacks)
    CASE 
        WHEN AgencyName IS NOT NULL AND LEN(LTRIM(RTRIM(AgencyName))) > 0 
        THEN LTRIM(RTRIM(AgencyName))
        ELSE 'Unknown Agency'
    END as AgencyName,
    
    CASE 
        WHEN AgencyCode IS NOT NULL AND LEN(LTRIM(RTRIM(AgencyCode))) > 0 
        THEN LTRIM(RTRIM(AgencyCode))
        ELSE NULL
    END as AgencyCode,
    
    -- Funding information (smart fallbacks)
    CASE 
        WHEN AwardCeiling IS NOT NULL AND AwardCeiling > 0 THEN AwardCeiling
        WHEN AwardFloor IS NOT NULL AND AwardFloor > 0 THEN AwardFloor
        WHEN EstimatedTotalFunding IS NOT NULL AND EstimatedTotalFunding > 0 THEN EstimatedTotalFunding
        ELSE NULL
    END as AwardValue,
    AwardCeiling,
    AwardFloor,
    EstimatedTotalFunding,
    ExpectedAwards,
    CASE 
        WHEN FundingType IS NOT NULL AND LEN(LTRIM(RTRIM(FundingType))) > 0 
        THEN LTRIM(RTRIM(FundingType))
        ELSE NULL
    END as FundingType,
    
    -- Dates (with validation)
    CASE WHEN CloseDate IS NOT NULL AND CloseDate > '1900-01-01' THEN CloseDate ELSE NULL END as Deadline,
    CASE WHEN PostedDate IS NOT NULL AND PostedDate > '1900-01-01' THEN PostedDate ELSE NULL END as PostedDate,
    CASE WHEN EstimatedPostDate IS NOT NULL AND EstimatedPostDate > '1900-01-01' THEN EstimatedPostDate ELSE NULL END as EstimatedPostDate,
    CASE WHEN EstimatedDueDate IS NOT NULL AND EstimatedDueDate > '1900-01-01' THEN EstimatedDueDate ELSE NULL END as EstimatedDueDate,
    
    -- Categorization (enhanced)
    CASE 
        WHEN Category IS NOT NULL AND LEN(LTRIM(RTRIM(Category))) > 0 
        THEN LTRIM(RTRIM(Category))
        ELSE NULL
    END as Category,
    
    CASE 
        WHEN UPPER(Title) LIKE '%FELLOWSHIP%' OR UPPER(Title) LIKE '%SCHOLAR%' THEN 'Fellowship'
        WHEN UPPER(Title) LIKE '%RESEARCH%' OR UPPER(Title) LIKE '%R&D%' THEN 'Research Grant'
        WHEN UPPER(Title) LIKE '%STARTUP%' OR UPPER(Title) LIKE '%ENTREPRENEUR%' THEN 'Startup Grant'
        WHEN UPPER(Title) LIKE '%ACCELERATOR%' OR UPPER(Title) LIKE '%INCUBATOR%' THEN 'Accelerator'
        ELSE 'Grant'
    END as OpportunityType,
    
    -- Eligibility (cleaned)
    CASE 
        WHEN EligibleApplicants IS NOT NULL AND LEN(LTRIM(RTRIM(EligibleApplicants))) > 0 
        THEN LTRIM(RTRIM(EligibleApplicants))
        ELSE NULL
    END as Eligibility,
    
    CASE 
        WHEN EligibleApplicants LIKE '%individual%' OR EligibleApplicants LIKE '%person%' THEN 'Individuals'
        WHEN EligibleApplicants LIKE '%nonprofit%' OR EligibleApplicants LIKE '%non-profit%' THEN 'Nonprofits'
        WHEN EligibleApplicants LIKE '%startup%' OR EligibleApplicants LIKE '%small business%' THEN 'Startups'
        WHEN EligibleApplicants LIKE '%university%' OR EligibleApplicants LIKE '%academic%' THEN 'Academic'
        WHEN EligibleApplicants LIKE '%government%' OR EligibleApplicants LIKE '%public%' THEN 'Government'
        ELSE 'Multiple'
    END as EligibilityCategory,
    
    -- Geographic scope (derived)
    CASE 
        WHEN Description LIKE '%countries%' OR Description LIKE '%international%' 
        THEN 'Multiple Countries'
        ELSE 'United States'
    END as CountriesEligible,
    
    CASE 
        WHEN UPPER(Title) LIKE '%GLOBAL%' OR UPPER(Description) LIKE '%INTERNATIONAL%' 
             OR UPPER(Description) LIKE '%WORLDWIDE%' OR UPPER(Title) LIKE '%GLOBAL%'
        THEN 1
        ELSE 0
    END as GlobalOpportunity,
    
    'EST' as TimeZone,
    
    -- Basic AI enrichment (keyword-based)
    CASE 
        WHEN UPPER(Description) LIKE '%CLIMATE%' OR UPPER(Description) LIKE '%ENVIRONMENT%' 
             OR UPPER(Description) LIKE '%SUSTAINABILITY%' OR UPPER(Description) LIKE '%GREEN%'
        THEN 'SDG 13: Climate Action'
        WHEN UPPER(Description) LIKE '%HEALTH%' OR UPPER(Description) LIKE '%MEDICAL%' 
             OR UPPER(Description) LIKE '%WELLNESS%' OR UPPER(Title) LIKE '%HEALTH%'
        THEN 'SDG 3: Good Health'
        WHEN UPPER(Description) LIKE '%EDUCATION%' OR UPPER(Description) LIKE '%LEARNING%' 
             OR UPPER(Description) LIKE '%SCHOOL%' OR UPPER(Title) LIKE '%EDUCATION%'
        THEN 'SDG 4: Quality Education'
        WHEN UPPER(Description) LIKE '%POVERTY%' OR UPPER(Description) LIKE '%ECONOMIC%' 
             OR UPPER(Description) LIKE '%DEVELOPMENT%' OR UPPER(Title) LIKE '%ECONOMIC%'
        THEN 'SDG 1: No Poverty'
        ELSE NULL
    END as SDGTags,
    
    CASE 
        WHEN UPPER(Description) LIKE '%DISADVANTAGED%' OR UPPER(Description) LIKE '%UNDERSERVED%' 
             OR UPPER(Description) LIKE '%MINORITY%' OR UPPER(Description) LIKE '%EQUITY%'
        THEN 'Equity Focus'
        WHEN UPPER(Description) LIKE '%RURAL%' OR UPPER(Description) LIKE '%REMOTE%' 
        THEN 'Geographic Gap'
        ELSE NULL
    END as OpportunityGap,
    
    CASE 
        WHEN Title IS NOT NULL AND LEN(LTRIM(RTRIM(Title))) > 0
        THEN LEFT(LTRIM(RTRIM(Title)), 100)
        ELSE NULL
    END as KeywordTags,
    
    -- Data quality score (enhanced calculation)
    CASE 
        WHEN Title IS NOT NULL AND OpportunityNumber IS NOT NULL 
             AND AgencyName IS NOT NULL AND AwardCeiling IS NOT NULL 
             AND CloseDate IS NOT NULL AND Description IS NOT NULL
        THEN 1.0
        WHEN Title IS NOT NULL AND OpportunityNumber IS NOT NULL 
             AND AgencyName IS NOT NULL AND (AwardCeiling IS NOT NULL OR CloseDate IS NOT NULL)
        THEN 0.8
        WHEN Title IS NOT NULL AND OpportunityNumber IS NOT NULL AND AgencyName IS NOT NULL
        THEN 0.6
        WHEN Title IS NOT NULL AND OpportunityNumber IS NOT NULL
        THEN 0.4
        ELSE 0.2
    END as DataQualityScore,
    
    -- Processing flags (identify missing critical data)
    CASE 
        WHEN Title IS NULL OR LEN(LTRIM(RTRIM(Title))) = 0 THEN 'Missing Title; '
        ELSE ''
    END +
    CASE 
        WHEN AgencyName IS NULL OR LEN(LTRIM(RTRIM(AgencyName))) = 0 THEN 'Missing Agency; '
        ELSE ''
    END +
    CASE 
        WHEN AwardCeiling IS NULL AND AwardFloor IS NULL AND EstimatedTotalFunding IS NULL THEN 'Missing Award Info; '
        ELSE ''
    END as ProcessingFlags,
    
    -- References and metadata
    ID as SourceLayerID,
    CFDANumbers,
    Package,
    Status,
    Version,
    
    -- System fields
    GETDATE() as ProcessedDate,
    'Layer2_ETL_Corrected' as ProcessedBy,
    '2.0' as DataVersion,
    GETDATE() as CreatedDate,
    GETDATE() as UpdatedDate

FROM RawGrantsLayer1
-- REMOVED PROBLEMATIC WHERE CLAUSE - process all records
ORDER BY ID;
"""
        
        # Execute the transformation
        temp_file = "layer2_transform_corrected.sql"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(transform_sql)
        
        cmd = [
            "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
            "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
            "-i", temp_file, "-C"
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("✅ Layer 2 transformation completed successfully")
        
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        # Enhanced verification
        print("\n📊 Verifying Layer 2 results...")
        verify_sql = """
-- Summary statistics
SELECT 
    'LAYER_2_SUMMARY' as ReportType,
    COUNT(*) as Total_Records,
    COUNT(CASE WHEN OpportunityURL IS NOT NULL AND OpportunityURL != '' THEN 1 END) as Records_With_URLs,
    COUNT(CASE WHEN AwardValue IS NOT NULL AND AwardValue > 0 THEN 1 END) as Records_With_Awards,
    COUNT(CASE WHEN Deadline IS NOT NULL THEN 1 END) as Records_With_Deadlines,
    COUNT(CASE WHEN GlobalOpportunity = 1 THEN 1 END) as Global_Opportunities,
    ROUND(AVG(DataQualityScore), 3) as Avg_Quality_Score,
    MAX(AwardValue) as Max_Award_Value
FROM CleanGrantsLayer2;

-- Sample enriched records
SELECT TOP 5
    'ENRICHED_SAMPLES' as ReportType,
    OpportunityNumber,
    LEFT(Title, 40) as Title_Sample,
    OpportunityType,
    AwardValue,
    GlobalOpportunity,
    SDGTags,
    DataQualityScore
FROM CleanGrantsLayer2
WHERE DataQualityScore >= 0.6
ORDER BY AwardValue DESC;

-- Opportunity type breakdown
SELECT 
    'OPPORTUNITY_TYPES' as ReportType,
    OpportunityType,
    COUNT(*) as Count,
    AVG(AwardValue) as Avg_Award
FROM CleanGrantsLayer2
WHERE OpportunityType IS NOT NULL
GROUP BY OpportunityType
ORDER BY Count DESC;
"""
        
        cmd = [
            "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
            "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
            "-Q", verify_sql, "-C"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        print("📈 Layer 2 Verification Results:")
        print(result.stdout)
        
        return True
        
    except Exception as e:
        print(f"❌ Error during Layer 2 transformation: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    start_time = datetime.now()
    success = transform_layer1_to_layer2_fixed()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    if success:
        print(f"\n🎯 LAYER 2 TRANSFORMATION COMPLETED!")
        print(f"⏱️ Total time: {duration:.2f} seconds")
        print("✅ Clean, enriched data ready for Layer 3")
        print("🧹 Data quality scoring and enrichment applied")
        print("🌐 Global opportunity detection enabled")
        print("🏷️ SDG tagging and opportunity gap analysis included")
    else:
        print(f"\n❌ LAYER 2 TRANSFORMATION FAILED!")
        print(f"⏱️ Failed after: {duration:.2f} seconds")