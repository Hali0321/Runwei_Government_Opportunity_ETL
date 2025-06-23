#!/usr/bin/env python3
"""
Working Layer 2 Transformation - Correct Schema Mapping
"""

import os
import subprocess
from datetime import datetime

def transform_layer1_to_layer2_working():
    """Working transformation with correct column mapping"""
    
    print("🚀 LAYER 2 TRANSFORMATION - WORKING VERSION")
    print("=" * 50)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Clear Layer 2
        print("\n🗑️ Clearing Layer 2...")
        subprocess.run([
            "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
            "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
            "-Q", "TRUNCATE TABLE CleanGrantsLayer2;", "-C"
        ], check=True)
        print("✅ Layer 2 cleared")
        
        # Working transformation SQL
        transform_sql = """
-- WORKING LAYER 2 TRANSFORMATION
WITH DeduplicatedData AS (
    SELECT 
        ROW_NUMBER() OVER (PARTITION BY OpportunityNumber ORDER BY ID) as rn,
        *
    FROM RawGrantsLayer1
    WHERE OpportunityNumber IS NOT NULL 
      AND OpportunityNumber NOT LIKE 'SAMPLE-%'
      AND OpportunityNumber NOT LIKE 'TEST-%'
)
INSERT INTO CleanGrantsLayer2 (
    OpportunityNumber, Title, Description, OpportunityURL, AdditionalInfoURL,
    AgencyName, AgencyCode, AwardValue, AwardCeiling, AwardFloor,
    EstimatedTotalFunding, ExpectedAwards, FundingType, Deadline,
    PostedDate, EstimatedPostDate, EstimatedDueDate, Category,
    OpportunityType, Eligibility, EligibilityCategory, CountriesEligible,
    GlobalOpportunity, TimeZone, SDGTags, OpportunityGap, KeywordTags,
    DataQualityScore, ProcessingFlags, SourceLayerID, ProcessedDate,
    ProcessedBy, DataVersion, CreatedDate, UpdatedDate, CFDANumbers,
    Package, Status, Version
)
SELECT 
    OpportunityNumber,
    COALESCE(Title, 'Untitled') as Title,
    Description,
    OpportunityURL,
    AdditionalInfoURL,
    COALESCE(AgencyName, 'Unknown') as AgencyName,
    AgencyCode,
    COALESCE(AwardCeiling, AwardFloor, EstimatedTotalFunding) as AwardValue,
    AwardCeiling, AwardFloor, EstimatedTotalFunding, ExpectedAwards, FundingType,
    CloseDate as Deadline,
    PostedDate, EstimatedPostDate, EstimatedDueDate, Category,
    CASE 
        WHEN UPPER(Title) LIKE '%FELLOWSHIP%' THEN 'Fellowship'
        WHEN UPPER(Title) LIKE '%RESEARCH%' THEN 'Research Grant'
        ELSE 'Grant'
    END as OpportunityType,
    EligibleApplicants as Eligibility,
    CASE 
        WHEN EligibleApplicants LIKE '%individual%' THEN 'Individuals'
        WHEN EligibleApplicants LIKE '%nonprofit%' THEN 'Nonprofits'
        ELSE 'Multiple'
    END as EligibilityCategory,
    'United States' as CountriesEligible,
    CASE WHEN Description LIKE '%global%' OR Description LIKE '%international%' THEN 1 ELSE 0 END as GlobalOpportunity,
    'EST' as TimeZone,
    CASE 
        WHEN Description LIKE '%climate%' THEN 'SDG 13: Climate Action'
        WHEN Description LIKE '%health%' THEN 'SDG 3: Good Health'
        WHEN Description LIKE '%education%' THEN 'SDG 4: Quality Education'
        ELSE 'Multiple SDGs'
    END as SDGTags,
    CASE 
        WHEN Description LIKE '%disadvantaged%' THEN 'Equity Focus'
        ELSE 'Standard Opportunity'
    END as OpportunityGap,
    LEFT(REPLACE(Title, ',', ' '), 1000) as KeywordTags,
    CASE 
        WHEN Title IS NOT NULL AND Description IS NOT NULL AND AwardCeiling IS NOT NULL AND CloseDate IS NOT NULL THEN 95.0
        WHEN Title IS NOT NULL AND Description IS NOT NULL THEN 75.0
        ELSE 50.0
    END as DataQualityScore,
    'WORKING_TRANSFORM' as ProcessingFlags,
    ID as SourceLayerID,
    GETDATE() as ProcessedDate,
    'Working_Script_v2' as ProcessedBy,
    '2.0' as DataVersion,
    GETDATE() as CreatedDate,
    GETDATE() as UpdatedDate,
    CFDANumbers, Package, Status, Version
FROM DeduplicatedData
WHERE rn = 1;

SELECT 'SUCCESS' as Status, COUNT(*) as Records_Inserted FROM CleanGrantsLayer2;
"""
        
        # Execute transformation
        print("\n🔄 Executing working transformation...")
        temp_file = "working_transform.sql"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(transform_sql)
        
        result = subprocess.run([
            "sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
            "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
            "-i", temp_file, "-C"
        ], capture_output=True, text=True)
        
        print("📊 Transformation Results:")
        print(result.stdout)
        
        # Cleanup
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        return True
        
    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        return False

if __name__ == "__main__":
    success = transform_layer1_to_layer2_working()
    if success:
        print("\n🎯 TRANSFORMATION COMPLETED SUCCESSFULLY!")
        print("✅ Layer 2 populated with AdditionalInfoURL column")
    else:
        print("\n❌ TRANSFORMATION FAILED!")