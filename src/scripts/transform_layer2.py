#!/usr/bin/env python3
"""
Transform Layer 1 to Layer 2 - SAFE VERSION with constraint handling
"""

import os
import subprocess
from datetime import datetime

def transform_layer1_to_layer2_safe():
    """Safe transformation that handles all constraint violations"""
    
    print("🛡️ LAYER 2 TRANSFORMATION PIPELINE (SAFE)")
    print("=" * 50)
    
    try:
        # Clear Layer 2
        print("\n🗑️ Clearing Layer 2...")
        clear_sql = "TRUNCATE TABLE CleanGrantsLayer2;"
        cmd = ["sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
               "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
               "-Q", clear_sql, "-C"]
        subprocess.run(cmd, check=True)
        
        # Safe transformation with deduplication and constraint handling
        safe_transform = """
WITH DeduplicatedData AS (
    SELECT 
        ROW_NUMBER() OVER (PARTITION BY OpportunityNumber ORDER BY ID) as rn,
        OpportunityNumber,
        Title,
        Description,
        OpportunityURL,
        AgencyName,
        AgencyCode,
        AwardCeiling,
        AwardFloor,
        EstimatedTotalFunding,
        ExpectedAwards,
        FundingType,
        CloseDate,
        PostedDate,
        EstimatedPostDate,
        EstimatedDueDate,
        Category,
        EligibleApplicants,
        CFDANumbers,
        Package,
        Status,
        Version,
        ID
    FROM RawGrantsLayer1
    WHERE OpportunityNumber IS NOT NULL 
      AND LEN(LTRIM(RTRIM(OpportunityNumber))) > 0
      AND LEN(LTRIM(RTRIM(OpportunityNumber))) <= 255
      AND Title IS NOT NULL
      AND LEN(LTRIM(RTRIM(Title))) <= 1000
)
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
    DataQualityScore,
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
    LEFT(LTRIM(RTRIM(OpportunityNumber)), 255) as OpportunityNumber,
    LEFT(LTRIM(RTRIM(Title)), 1000) as Title,
    LEFT(LTRIM(RTRIM(ISNULL(Description, ''))), 8000) as Description,
    LEFT(LTRIM(RTRIM(ISNULL(OpportunityURL, ''))), 2000) as OpportunityURL,
    LEFT(LTRIM(RTRIM(ISNULL(AgencyName, 'Unknown Agency'))), 500) as AgencyName,
    LEFT(LTRIM(RTRIM(ISNULL(AgencyCode, ''))), 100) as AgencyCode,
    COALESCE(AwardCeiling, AwardFloor, EstimatedTotalFunding) as AwardValue,
    AwardCeiling,
    AwardFloor,
    EstimatedTotalFunding,
    ExpectedAwards,
    LEFT(LTRIM(RTRIM(ISNULL(FundingType, ''))), 255) as FundingType,
    CloseDate as Deadline,
    PostedDate,
    EstimatedPostDate,
    EstimatedDueDate,
    LEFT(LTRIM(RTRIM(ISNULL(Category, ''))), 500) as Category,
    'Grant' as OpportunityType,
    LEFT(LTRIM(RTRIM(ISNULL(EligibleApplicants, ''))), 8000) as Eligibility,
    'Multiple' as EligibilityCategory,
    'United States' as CountriesEligible,
    0 as GlobalOpportunity,
    'EST' as TimeZone,
    0.6 as DataQualityScore,
    ID as SourceLayerID,
    LEFT(LTRIM(RTRIM(ISNULL(CFDANumbers, ''))), 500) as CFDANumbers,
    LEFT(LTRIM(RTRIM(ISNULL(Package, ''))), 500) as Package,
    LEFT(LTRIM(RTRIM(ISNULL(Status, ''))), 100) as Status,
    LEFT(LTRIM(RTRIM(ISNULL(Version, ''))), 50) as Version,
    GETDATE() as ProcessedDate,
    'Safe_Transform' as ProcessedBy,
    '2.0' as DataVersion,
    GETDATE() as CreatedDate,
    GETDATE() as UpdatedDate
FROM DeduplicatedData
WHERE rn = 1;  -- Only take first occurrence of each OpportunityNumber

SELECT 'TRANSFORMATION_COMPLETE' as Status, COUNT(*) as Records_Inserted FROM CleanGrantsLayer2;
"""
        
        temp_file = "safe_transform.sql"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(safe_transform)
        
        print("\n🔄 Executing safe transformation...")
        cmd = ["sqlcmd", "-S", "grants-gov-sql-server.database.windows.net",
               "-d", "GrantsGovDB", "-U", "grantsadmin", "-P", "Grant$Admin2024!",
               "-i", temp_file, "-C"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        print("🔍 Safe Transformation Results:")
        print(result.stdout)
        
        # Clean up
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        return True
        
    except Exception as e:
        print(f"❌ Safe transformation failed: {e}")
        return False

if __name__ == "__main__":
    transform_layer1_to_layer2_safe()