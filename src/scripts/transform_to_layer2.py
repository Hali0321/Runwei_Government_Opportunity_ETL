#!/usr/bin/env python3
"""
Transform Layer 1 (Raw) to Layer 2 (Clean & Enriched)
Includes data cleaning, standardization, and AI-powered enrichment
"""

import os
import subprocess
import re
from datetime import datetime
import json

def transform_layer1_to_layer2():
    """Transform raw grants data to clean, enriched Layer 2"""
    
    print("🚀 LAYER 2 TRANSFORMATION PIPELINE")
    print("=" * 50)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🧹 Cleaning, standardizing, and enriching grant data...")
    
    try:
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
        
        # Main ETL transformation SQL
        print("\n🔄 Executing Layer 1 → Layer 2 transformation...")
        
        transform_sql = """
-- ===================================
-- LAYER 1 → LAYER 2 TRANSFORMATION
-- Clean, standardize, and enrich grant data
-- ===================================

INSERT INTO CleanGrantsLayer2 (
    -- Core identifiers
    OpportunityNumber,
    Title,
    Description,
    OpportunityURL,
    
    -- Agency information
    AgencyName,
    AgencyCode,
    
    -- Funding information (cleaned)
    AwardValue,
    AwardCeiling,
    AwardFloor,
    EstimatedTotalFunding,
    ExpectedAwards,
    FundingType,
    
    -- Dates (standardized)
    Deadline,
    PostedDate,
    EstimatedPostDate,
    EstimatedDueDate,
    
    -- Categorization
    Category,
    OpportunityType,
    
    -- Eligibility
    Eligibility,
    EligibilityCategory,
    
    -- Geographic scope
    CountriesEligible,
    GlobalOpportunity,
    TimeZone,
    
    -- Enrichment (basic)
    SDGTags,
    OpportunityGap,
    KeywordTags,
    
    -- Quality score
    DataQualityScore,
    ProcessingFlags,
    
    -- References
    SourceLayerID,
    CFDANumbers,
    Package,
    Status,
    Version,
    
    -- System fields
    ProcessedDate,
    ProcessedBy,
    DataVersion,
    CreatedDate,
    UpdatedDate
)
SELECT 
    -- Core identifiers (cleaned)
    LTRIM(RTRIM(OpportunityNumber)) as OpportunityNumber,
    LTRIM(RTRIM(COALESCE(Title, 'Untitled Opportunity'))) as Title,
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
    
    -- Agency information (cleaned)
    LTRIM(RTRIM(COALESCE(AgencyName, 'Unknown Agency'))) as AgencyName,
    LTRIM(RTRIM(AgencyCode)) as AgencyCode,
    
    -- Funding information (standardized with fallbacks)
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
    LTRIM(RTRIM(FundingType)) as FundingType,
    
    -- Dates (standardized)
    CASE 
        WHEN CloseDate IS NOT NULL AND CloseDate > '1900-01-01' THEN CloseDate
        ELSE NULL 
    END as Deadline,
    CASE 
        WHEN PostedDate IS NOT NULL AND PostedDate > '1900-01-01' THEN PostedDate
        ELSE NULL 
    END as PostedDate,
    CASE 
        WHEN EstimatedPostDate IS NOT NULL AND EstimatedPostDate > '1900-01-01' THEN EstimatedPostDate
        ELSE NULL 
    END as EstimatedPostDate,
    CASE 
        WHEN EstimatedDueDate IS NOT NULL AND EstimatedDueDate > '1900-01-01' THEN EstimatedDueDate
        ELSE NULL 
    END as EstimatedDueDate,
    
    -- Categorization (enhanced)
    LTRIM(RTRIM(Category)) as Category,
    CASE 
        WHEN UPPER(Title) LIKE '%FELLOWSHIP%' OR UPPER(Title) LIKE '%SCHOLAR%' THEN 'Fellowship'
        WHEN UPPER(Title) LIKE '%ACCELERATOR%' OR UPPER(Title) LIKE '%INCUBATOR%' THEN 'Accelerator'
        WHEN UPPER(Title) LIKE '%RESEARCH%' OR UPPER(Title) LIKE '%R&D%' THEN 'Research Grant'
        WHEN UPPER(Title) LIKE '%STARTUP%' OR UPPER(Title) LIKE '%ENTREPRENEUR%' THEN 'Startup Grant'
        WHEN UPPER(FundingType) LIKE '%COOPERATIVE%' THEN 'Cooperative Agreement'
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
        WHEN Description LIKE '%global%' OR Description LIKE '%international%' 
             OR Description LIKE '%worldwide%' OR Title LIKE '%global%'
        THEN 1
        ELSE 0
    END as GlobalOpportunity,
    'EST' as TimeZone,  -- Default to Eastern (most US agencies)
    
    -- Basic AI enrichment (keyword-based)
    CASE 
        WHEN Description LIKE '%climate%' OR Description LIKE '%environment%' 
             OR Description LIKE '%sustainability%' OR Description LIKE '%green%'
        THEN 'SDG 13: Climate Action'
        WHEN Description LIKE '%health%' OR Description LIKE '%medical%' 
             OR Description LIKE '%wellness%' OR Title LIKE '%health%'
        THEN 'SDG 3: Good Health'
        WHEN Description LIKE '%education%' OR Description LIKE '%learning%' 
             OR Description LIKE '%school%' OR Title LIKE '%education%'
        THEN 'SDG 4: Quality Education'
        WHEN Description LIKE '%poverty%' OR Description LIKE '%economic%' 
             OR Description LIKE '%development%' OR Title LIKE '%economic%'
        THEN 'SDG 1: No Poverty'
        ELSE NULL
    END as SDGTags,
    CASE 
        WHEN Description LIKE '%disadvantaged%' OR Description LIKE '%underserved%' 
             OR Description LIKE '%minority%' OR Description LIKE '%equity%'
        THEN 'Equity Focus'
        WHEN Description LIKE '%rural%' OR Description LIKE '%remote%' 
        THEN 'Geographic Gap'
        ELSE NULL
    END as OpportunityGap,
    CASE 
        WHEN Title IS NOT NULL THEN 
            LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(Title, ',', ' '), '  ', ' '), '   ', ' ')))
        ELSE NULL
    END as KeywordTags,
    
    -- Data quality score (basic calculation)
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
    CASE 
        WHEN Title IS NULL THEN 'Missing Title; '
        WHEN AgencyName IS NULL THEN 'Missing Agency; '
        WHEN AwardCeiling IS NULL AND AwardFloor IS NULL THEN 'Missing Award Info; '
        ELSE NULL
    END as ProcessingFlags,
    
    -- References and metadata
    ID as SourceLayerID,
    CFDANumbers,
    Package,
    Status,
    Version,
    
    -- System fields
    GETDATE() as ProcessedDate,
    'Layer2_ETL_Pipeline' as ProcessedBy,
    '2.0' as DataVersion,
    GETDATE() as CreatedDate,
    GETDATE() as UpdatedDate

FROM RawGrantsLayer1
WHERE OpportunityNumber IS NOT NULL  -- Only process records with valid opportunity numbers
ORDER BY ProcessedDate DESC;

-- Update statistics for query optimization
UPDATE STATISTICS CleanGrantsLayer2;
"""
        
        # Execute the transformation
        temp_file = "layer2_transform.sql"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(transform_sql)
        
        cmd = [
            "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
            "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
            "-i", temp_file, "-C"
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        print("✅ Layer 2 transformation completed successfully")
        
        # Verification and statistics
        print("\n📊 Generating Layer 2 verification report...")
        
        verify_sql = """
-- ===================================
-- LAYER 2 VERIFICATION REPORT
-- ===================================

-- Summary statistics
SELECT 
    'LAYER_2_SUMMARY' as ReportType,
    COUNT(*) as Total_Records,
    COUNT(CASE WHEN OpportunityURL IS NOT NULL THEN 1 END) as Records_With_URLs,
    COUNT(CASE WHEN AwardValue IS NOT NULL AND AwardValue > 0 THEN 1 END) as Records_With_Awards,
    COUNT(CASE WHEN Deadline IS NOT NULL THEN 1 END) as Records_With_Deadlines,
    COUNT(CASE WHEN GlobalOpportunity = 1 THEN 1 END) as Global_Opportunities,
    AVG(DataQualityScore) as Avg_Quality_Score,
    MAX(AwardValue) as Max_Award_Value,
    MIN(Deadline) as Earliest_Deadline,
    MAX(Deadline) as Latest_Deadline
FROM CleanGrantsLayer2;

-- Opportunity type breakdown
SELECT 
    'OPPORTUNITY_TYPES' as ReportType,
    OpportunityType,
    COUNT(*) as Count,
    AVG(AwardValue) as Avg_Award,
    COUNT(CASE WHEN GlobalOpportunity = 1 THEN 1 END) as Global_Count
FROM CleanGrantsLayer2
WHERE OpportunityType IS NOT NULL
GROUP BY OpportunityType
ORDER BY Count DESC;

-- Data quality analysis
SELECT 
    'QUALITY_ANALYSIS' as ReportType,
    CASE 
        WHEN DataQualityScore >= 0.8 THEN 'High Quality'
        WHEN DataQualityScore >= 0.6 THEN 'Medium Quality'
        ELSE 'Low Quality'
    END as QualityTier,
    COUNT(*) as Record_Count,
    AVG(AwardValue) as Avg_Award_Value
FROM CleanGrantsLayer2
GROUP BY CASE 
    WHEN DataQualityScore >= 0.8 THEN 'High Quality'
    WHEN DataQualityScore >= 0.6 THEN 'Medium Quality'
    ELSE 'Low Quality'
END
ORDER BY QualityTier;

-- Sample enriched records
SELECT TOP 5
    'ENRICHED_SAMPLES' as ReportType,
    OpportunityNumber,
    LEFT(Title, 50) + '...' as Title_Sample,
    OpportunityType,
    AwardValue,
    GlobalOpportunity,
    SDGTags,
    OpportunityGap,
    DataQualityScore
FROM CleanGrantsLayer2
WHERE DataQualityScore >= 0.8
ORDER BY AwardValue DESC;
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
    success = transform_layer1_to_layer2()
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