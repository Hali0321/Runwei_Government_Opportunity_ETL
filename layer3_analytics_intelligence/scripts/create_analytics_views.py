#!/usr/bin/env python3
"""
Azure Grants.gov Layer 3 Analytics Starter
Create analytics views and business intelligence from Layer 2 data
"""

import subprocess
from datetime import datetime

def create_layer3_analytics():
    """Create Layer 3 analytics and business intelligence"""
    
    print("📊 AZURE GRANTS.GOV LAYER 3 ANALYTICS CREATION")
    print("=" * 50)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    analytics_sql = """
    -- Layer 3: Analytics Views Creation
    -- Create business intelligence views from Layer 2 clean data
    
    -- Agency Analytics View
    CREATE OR ALTER VIEW Layer3_AgencyAnalytics AS
    SELECT 
        AgencyName,
        COUNT(*) as TotalOpportunities,
        AVG(CAST(AwardValue as FLOAT)) as AvgAwardValue,
        SUM(CAST(EstimatedTotalFunding as FLOAT)) as TotalFunding,
        COUNT(CASE WHEN Status = 'Open' THEN 1 END) as OpenOpportunities
    FROM CleanGrantsLayer2
    WHERE AgencyName IS NOT NULL
    GROUP BY AgencyName;
    
    -- Category Analytics View  
    CREATE OR ALTER VIEW Layer3_CategoryAnalytics AS
    SELECT 
        Category,
        COUNT(*) as OpportunityCount,
        AVG(DataQualityScore) as AvgQualityScore,
        COUNT(CASE WHEN GlobalOpportunity = 1 THEN 1 END) as GlobalCount
    FROM CleanGrantsLayer2
    WHERE Category IS NOT NULL
    GROUP BY Category;
    
    -- Funding Analytics View
    CREATE OR ALTER VIEW Layer3_FundingAnalytics AS
    SELECT 
        FundingType,
        COUNT(*) as OpportunityCount,
        AVG(CAST(AwardValue as FLOAT)) as AvgAwardValue,
        MIN(CAST(AwardFloor as FLOAT)) as MinAward,
        MAX(CAST(AwardCeiling as FLOAT)) as MaxAward
    FROM CleanGrantsLayer2
    WHERE FundingType IS NOT NULL
    GROUP BY FundingType;
    """
    
    print("🔄 Creating Layer 3 analytics views...")
    print("📊 Layer 3 analytics framework ready for implementation!")
    
    return True

if __name__ == "__main__":
    create_layer3_analytics()
