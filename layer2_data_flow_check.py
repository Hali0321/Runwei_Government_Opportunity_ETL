#!/usr/bin/env python3
"""
Layer 2 Data Flow Analysis - Check where your data is and how to populate Layer 3
"""

import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_data_flow():
    """Analyze current data distribution and flow"""
    
    server = "grants-gov-sql-server.database.windows.net"
    database = "GrantsGovDB"
    username = "grantsadmin"
    password = "Grant$Admin2024!"
    
    # Comprehensive data analysis
    analysis_sql = """
    -- ===================================
    -- LAYER 2 DATA FLOW ANALYSIS
    -- ===================================
    
    -- 1. Check Layer 1 Raw Data
    SELECT 
        'LAYER1_RAW_DATA' as Layer,
        COUNT(*) as TotalRecords,
        COUNT(CASE WHEN OpportunityID IS NOT NULL THEN 1 END) as ValidOpportunityIDs,
        COUNT(CASE WHEN PostDate >= DATEADD(day, -30, GETDATE()) THEN 1 END) as RecentRecords,
        MIN(PostDate) as EarliestRecord,
        MAX(PostDate) as LatestRecord
    FROM RawGrantsLayer1;
    
    -- 2. Check Layer 2 Cleaned Data  
    SELECT 
        'LAYER2_CLEANED_DATA' as Layer,
        COUNT(*) as TotalRecords,
        COUNT(CASE WHEN OpportunityID IS NOT NULL THEN 1 END) as ValidOpportunityIDs,
        COUNT(CASE WHEN PostDate >= DATEADD(day, -30, GETDATE()) THEN 1 END) as RecentRecords,
        MIN(PostDate) as EarliestRecord,
        MAX(PostDate) as LatestRecord
    FROM CleanGrantsLayer2;
    
    -- 3. Sample Layer 1 data structure
    SELECT TOP 3 
        'LAYER1_SAMPLE' as DataType,
        OpportunityID, 
        OpportunityTitle,
        OpportunityNumber,
        Agency,
        PostDate,
        CloseDate
    FROM RawGrantsLayer1
    WHERE OpportunityID IS NOT NULL;
    
    -- 4. Sample Layer 2 data structure
    SELECT TOP 3 
        'LAYER2_SAMPLE' as DataType,
        OpportunityID,
        OpportunityTitle, 
        OpportunityNumber,
        Agency,
        PostDate,
        CloseDate
    FROM CleanGrantsLayer2
    WHERE OpportunityID IS NOT NULL;
    
    -- 5. Check what columns exist in each layer
    SELECT 
        'LAYER1_COLUMNS' as TableType,
        COLUMN_NAME,
        DATA_TYPE,
        IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'RawGrantsLayer1'
    ORDER BY ORDINAL_POSITION;
    """
    
    try:
        cmd = [
            "sqlcmd", "-S", server, "-d", database, 
            "-U", username, "-P", password,
            "-Q", analysis_sql, "-C", "-I", "-w", "400"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        if result.returncode == 0:
            logger.info("✅ Layer 2 Data Flow Analysis Results:")
            logger.info(result.stdout)
            return True
        else:
            logger.error(f"❌ Analysis failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error analyzing data flow: {e}")
        return False

def create_layer2_to_layer3_pipeline():
    """Create pipeline to populate BusinessIntelligenceLayer3 from CleanGrantsLayer2"""
    
    server = "grants-gov-sql-server.database.windows.net"
    database = "GrantsGovDB"
    username = "grantsadmin"
    password = "Grant$Admin2024!"
    
    # Pipeline to populate Layer 3 analytics from Layer 2 cleaned data
    pipeline_sql = """
    -- ===================================
    -- LAYER 2 TO LAYER 3 DATA PIPELINE
    -- Populate BusinessIntelligenceLayer3 from CleanGrantsLayer2
    -- ===================================
    
    -- Clear existing data if needed
    DELETE FROM BusinessIntelligenceLayer3;
    
    -- Insert analytics data from cleaned grants
    INSERT INTO BusinessIntelligenceLayer3 (
        OpportunityID,
        CompetitiveScore,
        OpportunityValue,
        UrgencyRating,
        RecommendationLevel,
        IndustryTrend,
        FundingTrend,
        CompetitionLevel,
        StrategicFit,
        ROIProjection,
        SuccessProbability,
        ViewCount,
        LastViewedDate,
        BookmarkedCount,
        ApplicationSubmissions,
        AIRecommendationScore,
        SimilarOpportunities,
        KeywordRelevance,
        EngagementScore,
        ConversionRate,
        CreatedDate,
        ModifiedDate,
        IsActive
    )
    SELECT 
        OpportunityID,
        -- Calculate competitive score based on funding amount and deadline
        CASE 
            WHEN TRY_CAST(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', '') AS DECIMAL) >= 1000000 THEN 85.0
            WHEN TRY_CAST(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', '') AS DECIMAL) >= 500000 THEN 75.0
            WHEN TRY_CAST(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', '') AS DECIMAL) >= 100000 THEN 65.0
            ELSE 45.0
        END as CompetitiveScore,
        
        EstimatedTotalFunding as OpportunityValue,
        
        -- Calculate urgency based on days until deadline
        CASE 
            WHEN DATEDIFF(day, GETDATE(), CloseDate) <= 7 THEN 'Critical'
            WHEN DATEDIFF(day, GETDATE(), CloseDate) <= 30 THEN 'High'
            WHEN DATEDIFF(day, GETDATE(), CloseDate) <= 60 THEN 'Medium'
            ELSE 'Low'  
        END as UrgencyRating,
        
        -- Recommendation based on funding and timeline
        CASE 
            WHEN TRY_CAST(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', '') AS DECIMAL) >= 500000 
                 AND DATEDIFF(day, GETDATE(), CloseDate) >= 30 THEN 'Highly Recommended'
            WHEN TRY_CAST(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', '') AS DECIMAL) >= 100000 THEN 'Recommended'
            ELSE 'Consider'
        END as RecommendationLevel,
        
        -- Map agency to industry trend
        CASE 
            WHEN Agency LIKE '%Health%' OR Agency LIKE '%NIH%' OR Agency LIKE '%CDC%' THEN 'Healthcare & Life Sciences'
            WHEN Agency LIKE '%Education%' OR Agency LIKE '%NSF%' THEN 'Education & Research'
            WHEN Agency LIKE '%Defense%' OR Agency LIKE '%DOD%' THEN 'Defense & Security'
            WHEN Agency LIKE '%Energy%' OR Agency LIKE '%DOE%' THEN 'Energy & Environment'
            WHEN Agency LIKE '%Commerce%' OR Agency LIKE '%SBA%' THEN 'Business & Commerce'
            ELSE 'General Government'
        END as IndustryTrend,
        
        -- Funding trend based on opportunity category
        CASE 
            WHEN OpportunityCategory LIKE '%Research%' THEN 'Research & Development'
            WHEN OpportunityCategory LIKE '%Education%' THEN 'Educational Funding'
            WHEN OpportunityCategory LIKE '%Infrastructure%' THEN 'Infrastructure Investment'
            ELSE 'General Funding'
        END as FundingTrend,
        
        -- Competition level based on expected applications
        CASE 
            WHEN ExpectedNumberOfAwards <= 5 THEN 'Very High Competition'
            WHEN ExpectedNumberOfAwards <= 20 THEN 'High Competition'  
            WHEN ExpectedNumberOfAwards <= 50 THEN 'Moderate Competition'
            ELSE 'Low Competition'
        END as CompetitionLevel,
        
        -- Strategic fit score
        CASE 
            WHEN TRY_CAST(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', '') AS DECIMAL) >= 1000000 
                 AND DATEDIFF(day, GETDATE(), CloseDate) >= 45 THEN 95.0
            WHEN TRY_CAST(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', '') AS DECIMAL) >= 500000 THEN 85.0
            ELSE 65.0
        END as StrategicFit,
        
        -- ROI Projection
        CASE 
            WHEN TRY_CAST(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', '') AS DECIMAL) >= 1000000 THEN 8.5
            WHEN TRY_CAST(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', '') AS DECIMAL) >= 500000 THEN 7.2
            ELSE 5.8
        END as ROIProjection,
        
        -- Success probability
        CASE 
            WHEN ExpectedNumberOfAwards >= 50 THEN 75.0
            WHEN ExpectedNumberOfAwards >= 20 THEN 45.0
            WHEN ExpectedNumberOfAwards >= 10 THEN 25.0
            ELSE 15.0
        END as SuccessProbability,
        
        0 as ViewCount,  -- Default values for new records
        GETDATE() as LastViewedDate,
        0 as BookmarkedCount,
        0 as ApplicationSubmissions,
        
        -- AI Recommendation Score (composite)
        (CASE 
            WHEN TRY_CAST(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', '') AS DECIMAL) >= 1000000 THEN 85.0
            WHEN TRY_CAST(REPLACE(REPLACE(EstimatedTotalFunding, '$', ''), ',', '') AS DECIMAL) >= 500000 THEN 75.0
            ELSE 55.0
        END + 
        CASE 
            WHEN DATEDIFF(day, GETDATE(), CloseDate) >= 60 THEN 15.0
            WHEN DATEDIFF(day, GETDATE(), CloseDate) >= 30 THEN 10.0
            ELSE 5.0
        END) / 2 as AIRecommendationScore,
        
        'Similar opportunities in ' + Agency as SimilarOpportunities,
        
        -- Keyword relevance based on title and description
        CASE 
            WHEN OpportunityTitle LIKE '%Innovation%' OR Description LIKE '%Innovation%' THEN 'High Innovation Relevance'
            WHEN OpportunityTitle LIKE '%Research%' OR Description LIKE '%Research%' THEN 'High Research Relevance'
            ELSE 'Standard Relevance'
        END as KeywordRelevance,
        
        50.0 as EngagementScore,  -- Default engagement
        15.0 as ConversionRate,   -- Default conversion
        
        ISNULL(PostDate, GETDATE()) as CreatedDate,
        GETDATE() as ModifiedDate,
        1 as IsActive  -- Mark all as active
        
    FROM CleanGrantsLayer2
    WHERE OpportunityID IS NOT NULL
      AND OpportunityTitle IS NOT NULL
      AND CloseDate IS NOT NULL
      AND CloseDate > GETDATE();  -- Only future opportunities
    
    -- Report results
    SELECT 
        'LAYER3_POPULATION_COMPLETE' as Status,
        @@ROWCOUNT as RecordsInserted,
        (SELECT COUNT(*) FROM BusinessIntelligenceLayer3) as TotalLayer3Records,
        (SELECT COUNT(*) FROM CleanGrantsLayer2) as TotalLayer2Records,
        GETDATE() as ProcessedAt;
    """
    
    try:
        cmd = [
            "sqlcmd", "-S", server, "-d", database, 
            "-U", username, "-P", password,
            "-Q", pipeline_sql, "-C", "-I", "-t", "300"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=330)
        
        if result.returncode == 0:
            logger.info("✅ Layer 2 to Layer 3 Pipeline Results:")
            logger.info(result.stdout)
            return True
        else:
            logger.error(f"❌ Pipeline failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error running pipeline: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Analyzing Layer 2 Data Flow...")
    analyze_data_flow()
    
    print("\n🚀 Creating Layer 2 to Layer 3 Pipeline...")
    create_layer2_to_layer3_pipeline()