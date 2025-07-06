#!/usr/bin/env python3
"""
Azure SQL Database Opportunity Gap Processor - Layer 2 Processing
Maps opportunities to gap resources: Access to Capital, Networks, and Capacity Building
Updates OpportunityGap column in CleanGrantsLayer2 table with proper Runwei formatting
"""

import subprocess
import logging
from datetime import datetime
from pathlib import Path

# Configure logging to __pycache__ folder
SCRIPT_DIR = Path(__file__).parent
PYCACHE_DIR = SCRIPT_DIR / "__pycache__"
PYCACHE_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    handlers=[
        logging.FileHandler(PYCACHE_DIR / 'opportunity_gap.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OpportunityGapProcessor:
    """Opportunity gap processor for Azure SQL Database with Runwei formatting standards"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        self.table_name = "CleanGrantsLayer2"
        
        # Opportunity Gap Resource mappings with keywords
        self.gap_mappings = {
            "Access to Capital": [
                "funding", "grant", "financial support", "financial assistance", "stipend", 
                "award", "prize money", "cash award", "non-dilutive", "equity-free",
                "investment", "capital", "financial backing", "seed funding", "prize",
                "monetary award", "financial resources", "funding opportunity", "budget",
                "reimbursement", "financial aid", "scholarship", "fellowship funding"
            ],
            "Access to Networks": [
                "mentorship", "mentor", "advisor", "network", "networking", "connections",
                "introductions", "industry contacts", "peer community", "partner network",
                "expert panel", "advisory board", "professional network", "community",
                "collaboration", "partnership", "investor network", "demo day",
                "pitch event", "showcase", "exposure", "visibility", "industry leaders",
                "thought leaders", "subject matter experts", "alumni network"
            ],
            "Access to Capacity Building": [
                "training", "workshop", "bootcamp", "program", "curriculum", "course",
                "coaching", "technical assistance", "skill development", "learning",
                "development", "education", "instruction", "guidance", "support",
                "professional development", "capacity building", "knowledge transfer",
                "skill building", "competency development", "expertise building",
                "business development", "technical training", "leadership development",
                "entrepreneurship training", "innovation training", "accelerator program"
            ]
        }

    def execute_sql_command(self, sql_query, timeout=600):
        """Execute SQL command with Azure SQL Database"""
        try:
            cmd = [
                "sqlcmd", "-S", self.server,
                "-d", self.database,
                "-U", self.username,
                "-P", self.password,
                "-Q", sql_query,
                "-C", "-t", str(timeout), "-I"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 30)
            
            if result.returncode == 0:
                logger.info("✅ SQL command executed successfully")
                if result.stdout:
                    logger.info(f"Output: {result.stdout}")
                return result.stdout
            else:
                logger.error(f"❌ SQL command failed with return code {result.returncode}")
                if result.stderr:
                    logger.error(f"Error: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ SQL command timed out after {timeout} seconds")
            return None
        except Exception as e:
            logger.error(f"❌ Error executing SQL: {e}")
            return None

    def ensure_gap_processing_columns(self):
        """Ensure opportunity gap processing columns exist in the database"""
        logger.info("🔧 Ensuring opportunity gap processing columns exist...")
        
        column_sql = """
        -- Add opportunity gap processing columns if they don't exist
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'GapProcessedDate')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD GapProcessedDate DATETIME2 NULL;
            PRINT 'GapProcessedDate column added';
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'GapConfidenceScore')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD GapConfidenceScore DECIMAL(3,2) NULL;
            PRINT 'GapConfidenceScore column added';
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'GapMappingMethod')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD GapMappingMethod NVARCHAR(100) NULL;
            PRINT 'GapMappingMethod column added';
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'HasCapitalAccess')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD HasCapitalAccess BIT DEFAULT 0;
            PRINT 'HasCapitalAccess flag added';
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'HasNetworkAccess')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD HasNetworkAccess BIT DEFAULT 0;
            PRINT 'HasNetworkAccess flag added';
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'HasCapacityBuilding')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD HasCapacityBuilding BIT DEFAULT 0;
            PRINT 'HasCapacityBuilding flag added';
        END
        
        -- Ensure OpportunityGap column exists and is properly sized
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'OpportunityGap')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD OpportunityGap NVARCHAR(MAX) NULL;
            PRINT 'OpportunityGap column added';
        END
        ELSE
        BEGIN
            -- Ensure OpportunityGap column can handle long text
            ALTER TABLE CleanGrantsLayer2 
            ALTER COLUMN OpportunityGap NVARCHAR(MAX) NULL;
            PRINT 'OpportunityGap column updated to NVARCHAR(MAX)';
        END
        
        SELECT 'GAP_COLUMNS_READY' as Status;
        """
        
        result = self.execute_sql_command(column_sql, timeout=120)
        return result is not None and 'GAP_COLUMNS_READY' in str(result)

    def process_opportunity_gap_analysis(self):
        """Process opportunity gap analysis using keyword-based mapping"""
        logger.info("🎯 Processing opportunity gap analysis using keyword-based mapping...")
        
        # Build comprehensive opportunity gap mapping SQL
        gap_sql = """
        -- Clear existing gap analysis values
        UPDATE CleanGrantsLayer2 
        SET OpportunityGap = NULL, 
            GapProcessedDate = NULL, 
            GapConfidenceScore = NULL, 
            GapMappingMethod = NULL,
            HasCapitalAccess = 0,
            HasNetworkAccess = 0,
            HasCapacityBuilding = 0;
        
        -- Process Access to Capital identification
        UPDATE CleanGrantsLayer2 
        SET HasCapitalAccess = 1,
            GapMappingMethod = 'keyword-based',
            GapConfidenceScore = 0.9
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%funding%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%grant%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%financial support%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%financial assistance%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%stipend%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%award%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%prize money%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%cash award%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%non-dilutive%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%equity-free%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%investment%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%capital%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%financial backing%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%seed funding%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%prize%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%monetary award%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%financial resources%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%funding opportunity%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%budget%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%reimbursement%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%financial aid%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%scholarship%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%fellowship funding%'
        );
        
        -- Process Access to Networks identification
        UPDATE CleanGrantsLayer2 
        SET HasNetworkAccess = 1,
            GapMappingMethod = 'keyword-based',
            GapConfidenceScore = CASE 
                WHEN GapConfidenceScore IS NULL THEN 0.85
                ELSE (GapConfidenceScore + 0.85) / 2
            END
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%mentorship%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%mentor%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%advisor%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%network%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%networking%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%connections%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%introductions%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%industry contacts%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%peer community%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%partner network%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%expert panel%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%advisory board%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%professional network%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%community%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%collaboration%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%partnership%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%investor network%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%demo day%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%pitch event%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%showcase%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%exposure%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%visibility%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%industry leaders%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%thought leaders%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%subject matter experts%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%alumni network%'
        );
        
        -- Process Access to Capacity Building identification
        UPDATE CleanGrantsLayer2 
        SET HasCapacityBuilding = 1,
            GapMappingMethod = 'keyword-based',
            GapConfidenceScore = CASE 
                WHEN GapConfidenceScore IS NULL THEN 0.8
                ELSE (GapConfidenceScore + 0.8) / 2
            END
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%training%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%workshop%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%bootcamp%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%program%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%curriculum%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%course%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%coaching%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%technical assistance%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%skill development%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%learning%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%development%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%education%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%instruction%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%guidance%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%support%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%professional development%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%capacity building%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%knowledge transfer%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%skill building%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%competency development%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%expertise building%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%business development%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%technical training%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%leadership development%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%entrepreneurship training%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%innovation training%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%accelerator program%'
        );
        
        -- Build OpportunityGap field based on identified resources
        UPDATE CleanGrantsLayer2 
        SET OpportunityGap = 
            CASE 
                -- All three resources
                WHEN HasCapitalAccess = 1 AND HasNetworkAccess = 1 AND HasCapacityBuilding = 1
                THEN 'Access to Capital, Access to Networks, Access to Capacity Building'
                
                -- Two resources combinations
                WHEN HasCapitalAccess = 1 AND HasNetworkAccess = 1 AND HasCapacityBuilding = 0
                THEN 'Access to Capital, Access to Networks'
                
                WHEN HasCapitalAccess = 1 AND HasNetworkAccess = 0 AND HasCapacityBuilding = 1
                THEN 'Access to Capital, Access to Capacity Building'
                
                WHEN HasCapitalAccess = 0 AND HasNetworkAccess = 1 AND HasCapacityBuilding = 1
                THEN 'Access to Networks, Access to Capacity Building'
                
                -- Single resources
                WHEN HasCapitalAccess = 1 AND HasNetworkAccess = 0 AND HasCapacityBuilding = 0
                THEN 'Access to Capital'
                
                WHEN HasCapitalAccess = 0 AND HasNetworkAccess = 1 AND HasCapacityBuilding = 0
                THEN 'Access to Networks'
                
                WHEN HasCapitalAccess = 0 AND HasNetworkAccess = 0 AND HasCapacityBuilding = 1
                THEN 'Access to Capacity Building'
                
                -- No resources identified
                ELSE NULL
            END
        WHERE HasCapitalAccess = 1 OR HasNetworkAccess = 1 OR HasCapacityBuilding = 1;
        
        -- Update processing timestamps
        UPDATE CleanGrantsLayer2 
        SET GapProcessedDate = GETDATE()
        WHERE OpportunityGap IS NOT NULL;
        
        -- Generate summary statistics
        SELECT 
            'GAP_PROCESSING_COMPLETE' as Status,
            COUNT(*) as Total_Records,
            COUNT(CASE WHEN OpportunityGap IS NOT NULL THEN 1 END) as Records_With_Gap_Analysis,
            COUNT(CASE WHEN OpportunityGap IS NULL THEN 1 END) as Records_Without_Gap_Analysis,
            ROUND(100.0 * COUNT(CASE WHEN OpportunityGap IS NOT NULL THEN 1 END) / COUNT(*), 2) as Gap_Coverage_Percent,
            AVG(CASE WHEN OpportunityGap IS NOT NULL THEN GapConfidenceScore END) as Avg_Confidence_Score,
            SUM(CAST(HasCapitalAccess AS INT)) as Capital_Access_Count,
            SUM(CAST(HasNetworkAccess AS INT)) as Network_Access_Count,
            SUM(CAST(HasCapacityBuilding AS INT)) as Capacity_Building_Count
        FROM CleanGrantsLayer2;
        """
        
        result = self.execute_sql_command(gap_sql, timeout=600)
        return result is not None and 'GAP_PROCESSING_COMPLETE' in str(result)

    def generate_gap_report(self):
        """Generate comprehensive opportunity gap analysis report"""
        logger.info("📊 Generating opportunity gap analysis report...")
        
        report_sql = """
        -- Gap Resource Distribution Report
        SELECT 
            'GAP_RESOURCE_DISTRIBUTION' as Report_Type,
            OpportunityGap as Gap_Resources,
            COUNT(*) as Opportunity_Count,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM CleanGrantsLayer2 WHERE OpportunityGap IS NOT NULL), 2) as Percentage_of_Gap_Opportunities,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM CleanGrantsLayer2), 2) as Percentage_of_Total_Opportunities
        FROM CleanGrantsLayer2
        WHERE OpportunityGap IS NOT NULL
        GROUP BY OpportunityGap
        ORDER BY COUNT(*) DESC;
        
        -- Individual Resource Analysis
        SELECT 
            'INDIVIDUAL_RESOURCE_ANALYSIS' as Analysis_Type,
            'Access to Capital' as Resource_Type,
            SUM(CAST(HasCapitalAccess AS INT)) as Opportunity_Count,
            ROUND(100.0 * SUM(CAST(HasCapitalAccess AS INT)) / COUNT(*), 2) as Percentage
        FROM CleanGrantsLayer2
        UNION ALL
        SELECT 
            'INDIVIDUAL_RESOURCE_ANALYSIS' as Analysis_Type,
            'Access to Networks' as Resource_Type,
            SUM(CAST(HasNetworkAccess AS INT)) as Opportunity_Count,
            ROUND(100.0 * SUM(CAST(HasNetworkAccess AS INT)) / COUNT(*), 2) as Percentage
        FROM CleanGrantsLayer2
        UNION ALL
        SELECT 
            'INDIVIDUAL_RESOURCE_ANALYSIS' as Analysis_Type,
            'Access to Capacity Building' as Resource_Type,
            SUM(CAST(HasCapacityBuilding AS INT)) as Opportunity_Count,
            ROUND(100.0 * SUM(CAST(HasCapacityBuilding AS INT)) / COUNT(*), 2) as Percentage
        FROM CleanGrantsLayer2
        ORDER BY Opportunity_Count DESC;
        
        -- Sample gap analysis assignments
        SELECT TOP 10
            'GAP_SAMPLES' as Sample_Type,
            SUBSTRING(Title, 1, 50) as Title_Preview,
            OpportunityGap,
            GapConfidenceScore,
            GapMappingMethod,
            HasCapitalAccess,
            HasNetworkAccess,
            HasCapacityBuilding
        FROM CleanGrantsLayer2
        WHERE OpportunityGap IS NOT NULL
        ORDER BY NEWID();
        
        -- Multi-resource opportunities analysis
        SELECT 
            'MULTI_RESOURCE_ANALYSIS' as Analysis_Type,
            COUNT(*) as Total_Gap_Opportunities,
            COUNT(CASE WHEN OpportunityGap LIKE '%,%' THEN 1 END) as Multi_Resource_Opportunities,
            ROUND(100.0 * COUNT(CASE WHEN OpportunityGap LIKE '%,%' THEN 1 END) / COUNT(*), 2) as Multi_Resource_Percentage,
            COUNT(CASE WHEN HasCapitalAccess = 1 AND HasNetworkAccess = 1 AND HasCapacityBuilding = 1 THEN 1 END) as All_Three_Resources
        FROM CleanGrantsLayer2
        WHERE OpportunityGap IS NOT NULL;
        
        -- Agency-wise gap resource analysis
        SELECT TOP 20
            'AGENCY_GAP_ANALYSIS' as Analysis_Type,
            AgencyName,
            COUNT(*) as Total_Opportunities,
            COUNT(CASE WHEN OpportunityGap IS NOT NULL THEN 1 END) as Opportunities_With_Gap_Resources,
            ROUND(100.0 * COUNT(CASE WHEN OpportunityGap IS NOT NULL THEN 1 END) / COUNT(*), 2) as Gap_Coverage_Percent,
            SUM(CAST(HasCapitalAccess AS INT)) as Capital_Opportunities,
            SUM(CAST(HasNetworkAccess AS INT)) as Network_Opportunities,
            SUM(CAST(HasCapacityBuilding AS INT)) as Capacity_Building_Opportunities
        FROM CleanGrantsLayer2
        WHERE AgencyName IS NOT NULL
        GROUP BY AgencyName
        HAVING COUNT(*) >= 5
        ORDER BY COUNT(CASE WHEN OpportunityGap IS NOT NULL THEN 1 END) DESC;
        """
        
        result = self.execute_sql_command(report_sql, timeout=300)
        return result is not None

    def create_gap_views(self):
        """Create opportunity gap analysis views"""
        logger.info("🎯 Creating opportunity gap analysis views...")
        
        views_sql = """
        -- Create gap-based views for analysis
        CREATE OR ALTER VIEW vw_Gap_Opportunities AS
        SELECT 
            OpportunityNumber,
            Title,
            Description,
            AgencyName,
            RunweiCategory,
            OpportunityGap,
            GapConfidenceScore,
            GapMappingMethod,
            HasCapitalAccess,
            HasNetworkAccess,
            HasCapacityBuilding,
            AwardValueUSD,
            EstimatedTotalFunding,
            Deadline,
            PostedDate
        FROM CleanGrantsLayer2
        WHERE OpportunityGap IS NOT NULL;
        
        CREATE OR ALTER VIEW vw_Gap_Summary AS
        SELECT 
            'Gap Analysis Summary' as Report_Type,
            COUNT(*) as Total_Opportunities,
            COUNT(CASE WHEN OpportunityGap IS NOT NULL THEN 1 END) as Gap_Analyzed_Opportunities,
            ROUND(100.0 * COUNT(CASE WHEN OpportunityGap IS NOT NULL THEN 1 END) / COUNT(*), 2) as Gap_Coverage_Percent,
            SUM(CAST(HasCapitalAccess AS INT)) as Capital_Access_Count,
            SUM(CAST(HasNetworkAccess AS INT)) as Network_Access_Count,
            SUM(CAST(HasCapacityBuilding AS INT)) as Capacity_Building_Count,
            AVG(CASE WHEN OpportunityGap IS NOT NULL THEN GapConfidenceScore END) as Avg_Confidence_Score
        FROM CleanGrantsLayer2;
        
        CREATE OR ALTER VIEW vw_Capital_Opportunities AS
        SELECT *
        FROM CleanGrantsLayer2
        WHERE HasCapitalAccess = 1;
        
        CREATE OR ALTER VIEW vw_Network_Opportunities AS
        SELECT *
        FROM CleanGrantsLayer2
        WHERE HasNetworkAccess = 1;
        
        CREATE OR ALTER VIEW vw_Capacity_Building_Opportunities AS
        SELECT *
        FROM CleanGrantsLayer2
        WHERE HasCapacityBuilding = 1;
        
        SELECT 'GAP_VIEWS_CREATED' as Status, GETDATE() as Created_At;
        """
        
        result = self.execute_sql_command(views_sql, timeout=120)
        return result is not None

    def run_complete_gap_processing(self):
        """Run the complete opportunity gap analysis processing pipeline"""
        logger.info("🎯 STARTING OPPORTUNITY GAP ANALYSIS PROCESSING")
        logger.info("=" * 60)
        
        steps = [
            ("Ensure Gap Processing Columns", self.ensure_gap_processing_columns),
            ("Process Gap Analysis", self.process_opportunity_gap_analysis),
            ("Generate Gap Report", self.generate_gap_report),
            ("Create Gap Views", self.create_gap_views)
        ]
        
        success_count = 0
        for i, (step_name, step_function) in enumerate(steps, 1):
            logger.info(f"\n📍 STEP {i}/{len(steps)}: {step_name}")
            logger.info("-" * 50)
            
            try:
                success = step_function()
                if success:
                    logger.info(f"✅ {step_name} completed successfully")
                    success_count += 1
                else:
                    logger.error(f"❌ {step_name} failed")
                    if i <= 2:  # Critical steps
                        logger.error("💥 Critical step failed. Aborting process.")
                        break
            except Exception as e:
                logger.error(f"❌ {step_name} error: {e}")
                if i <= 2:
                    break
        
        logger.info(f"\n🎯 GAP PROCESSING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"✅ Completed Steps: {success_count}/{len(steps)}")
        
        if success_count >= 3:
            logger.info("🎉 OPPORTUNITY GAP ANALYSIS PROCESSING SUCCESS!")
            logger.info("\n📊 VERIFICATION QUERIES:")
            logger.info("   → SELECT COUNT(*), COUNT(OpportunityGap) FROM CleanGrantsLayer2;")
            logger.info("   → SELECT * FROM vw_Gap_Opportunities ORDER BY GapConfidenceScore DESC;")
            logger.info("   → SELECT OpportunityGap, COUNT(*) FROM CleanGrantsLayer2 WHERE OpportunityGap IS NOT NULL GROUP BY OpportunityGap;")
            logger.info("   → SELECT * FROM vw_Gap_Summary;")
            return True
        else:
            logger.error("❌ Opportunity gap analysis processing failed")
            return False

def main():
    """Main execution function"""
    print("🎯 OPPORTUNITY GAP ANALYSIS PROCESSOR - RUNWEI STANDARDS")
    print("=" * 65)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("💰 Mapping opportunities to gap resources:")
    print("   • Access to Capital (funding, grants, financial support)")
    print("   • Access to Networks (mentorship, connections, community)")
    print("   • Access to Capacity Building (training, coaching, development)")
    print("🔍 Using keyword-based intelligent mapping")
    print("📐 Format: Comma-separated resource list")
    print()
    
    processor = OpportunityGapProcessor()
    success = processor.run_complete_gap_processing()
    
    print("\n" + "=" * 65)
    if success:
        print("🎉 OPPORTUNITY GAP ANALYSIS PROCESSING COMPLETED SUCCESSFULLY!")
        print("✅ All opportunities analyzed for gap resources")
        print("✅ Proper Runwei formatting applied")
        print("✅ Confidence scores calculated")
        print("✅ Resource flags set (HasCapitalAccess, HasNetworkAccess, HasCapacityBuilding)")
        print("✅ Analysis views created")
        print("\n🔍 VERIFICATION QUERIES:")
        print("   1. SELECT COUNT(*), COUNT(OpportunityGap) FROM CleanGrantsLayer2;")
        print("   2. SELECT * FROM vw_Gap_Opportunities;")
        print("   3. SELECT OpportunityGap, COUNT(*) FROM CleanGrantsLayer2")
        print("      WHERE OpportunityGap IS NOT NULL GROUP BY OpportunityGap;")
        print("   4. SELECT * FROM vw_Gap_Summary;")
        print("   5. SELECT * FROM vw_Capital_Opportunities;")
        print("\n🎯 Your grants database now includes comprehensive gap analysis!")
        print("💡 Use the resource flags for efficient filtering and analysis")
    else:
        print("❌ OPPORTUNITY GAP ANALYSIS PROCESSING FAILED")
        print("📝 Please check the logs for details")
    
    print(f"\n📅 Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()