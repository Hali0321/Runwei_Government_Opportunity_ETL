#!/usr/bin/env python3
"""
Azure SQL Database Rolling Application Detector - Layer 2 Processing  
Identifies opportunities with rolling/ongoing application deadlines
Updates IsRolling column in CleanGrantsLayer2 table with proper Runwei formatting
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
        logging.FileHandler(PYCACHE_DIR / 'is_rolling.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RollingApplicationDetector:
    """Rolling application detector for Azure SQL Database with Runwei formatting standards"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        self.table_name = "CleanGrantsLayer2"
        
        # Rolling application keywords and patterns
        self.rolling_keywords = [
            "rolling", "rolling basis", "rolling deadline", "rolling applications",
            "rolling admission", "rolling review", "rolling acceptance",
            "year-round", "year round", "accepted year-round", "open year-round",
            "ongoing", "continuous", "continual", "perpetual", "open-ended",
            "no deadline", "no specific deadline", "no application deadline",
            "apply anytime", "apply any time", "anytime applications",
            "applications accepted continuously", "continuous applications",
            "open applications", "applications always open", "always accepting",
            "until filled", "until positions are filled", "until slots are filled",
            "until funds are exhausted", "until funding is exhausted",
            "multiple deadlines", "multiple rounds", "multiple cycles",
            "quarterly", "monthly", "weekly", "regular intervals",
            "submit when ready", "when ready", "as needed",
            "tbd", "to be determined", "to be announced", "tba"
        ]
        
        # Non-rolling indicators (specific dates/times)
        self.non_rolling_patterns = [
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december",
            "jan", "feb", "mar", "apr", "may", "jun",
            "jul", "aug", "sep", "oct", "nov", "dec",
            "2024", "2025", "2026", "2027", "2028", "2029", "2030",
            "due", "deadline", "must be submitted by", "submit by",
            "application due", "proposals due", "close", "closes"
        ]

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

    def ensure_rolling_processing_columns(self):
        """Ensure rolling application processing columns exist in the database"""
        logger.info("🔧 Ensuring rolling application processing columns exist...")
        
        column_sql = """
        -- Add rolling application processing columns if they don't exist
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'IsRolling')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD IsRolling BIT DEFAULT 0;
            PRINT 'IsRolling column added';
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'RollingProcessedDate')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD RollingProcessedDate DATETIME2 NULL;
            PRINT 'RollingProcessedDate column added';
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'RollingConfidenceScore')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD RollingConfidenceScore DECIMAL(3,2) NULL;
            PRINT 'RollingConfidenceScore column added';
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'RollingDetectionMethod')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD RollingDetectionMethod NVARCHAR(100) NULL;
            PRINT 'RollingDetectionMethod column added';
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'RollingKeywords')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD RollingKeywords NVARCHAR(500) NULL;
            PRINT 'RollingKeywords column added';
        END
        
        SELECT 'ROLLING_COLUMNS_READY' as Status;
        """
        
        result = self.execute_sql_command(column_sql, timeout=120)
        return result is not None and 'ROLLING_COLUMNS_READY' in str(result)

    def process_rolling_detection(self):
        """Process rolling application detection using keyword-based analysis"""
        logger.info("📅 Processing rolling application detection using keyword-based analysis...")
        
        # Build comprehensive rolling detection SQL
        rolling_sql = """
        -- Clear existing rolling detection values
        UPDATE CleanGrantsLayer2 
        SET IsRolling = 0, 
            RollingProcessedDate = NULL, 
            RollingConfidenceScore = NULL, 
            RollingDetectionMethod = NULL,
            RollingKeywords = NULL;
        
        -- HIGH CONFIDENCE: Explicit rolling keywords in title or description
        UPDATE CleanGrantsLayer2 
        SET IsRolling = 1,
            RollingDetectionMethod = 'explicit-keywords',
            RollingConfidenceScore = 0.95,
            RollingKeywords = 'rolling'
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%rolling%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%rolling basis%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%rolling deadline%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%rolling applications%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%rolling admission%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%rolling review%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%rolling acceptance%'
        );
        
        -- HIGH CONFIDENCE: Year-round applications
        UPDATE CleanGrantsLayer2 
        SET IsRolling = 1,
            RollingDetectionMethod = 'year-round-keywords',
            RollingConfidenceScore = 0.9,
            RollingKeywords = 'year-round'
        WHERE IsRolling = 0 AND (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%year-round%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%year round%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%accepted year-round%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%open year-round%'
        );
        
        -- HIGH CONFIDENCE: No deadline indicators
        UPDATE CleanGrantsLayer2 
        SET IsRolling = 1,
            RollingDetectionMethod = 'no-deadline-keywords',
            RollingConfidenceScore = 0.9,
            RollingKeywords = 'no deadline'
        WHERE IsRolling = 0 AND (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%no deadline%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%no specific deadline%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%no application deadline%'
        );
        
        -- HIGH CONFIDENCE: Apply anytime indicators
        UPDATE CleanGrantsLayer2 
        SET IsRolling = 1,
            RollingDetectionMethod = 'apply-anytime-keywords',
            RollingConfidenceScore = 0.9,
            RollingKeywords = 'apply anytime'
        WHERE IsRolling = 0 AND (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%apply anytime%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%apply any time%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%anytime applications%'
        );
        
        -- MEDIUM-HIGH CONFIDENCE: Ongoing/continuous indicators
        UPDATE CleanGrantsLayer2 
        SET IsRolling = 1,
            RollingDetectionMethod = 'ongoing-keywords',
            RollingConfidenceScore = 0.85,
            RollingKeywords = 'ongoing'
        WHERE IsRolling = 0 AND (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%ongoing%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%continuous%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%continual%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%perpetual%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%open-ended%'
        );
        
        -- MEDIUM-HIGH CONFIDENCE: Continuous applications
        UPDATE CleanGrantsLayer2 
        SET IsRolling = 1,
            RollingDetectionMethod = 'continuous-applications',
            RollingConfidenceScore = 0.85,
            RollingKeywords = 'continuous applications'
        WHERE IsRolling = 0 AND (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%applications accepted continuously%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%continuous applications%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%open applications%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%applications always open%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%always accepting%'
        );
        
        -- MEDIUM CONFIDENCE: Until filled indicators
        UPDATE CleanGrantsLayer2 
        SET IsRolling = 1,
            RollingDetectionMethod = 'until-filled-keywords',
            RollingConfidenceScore = 0.8,
            RollingKeywords = 'until filled'
        WHERE IsRolling = 0 AND (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%until filled%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%until positions are filled%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%until slots are filled%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%until funds are exhausted%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%until funding is exhausted%'
        );
        
        -- MEDIUM CONFIDENCE: Multiple cycles/rounds
        UPDATE CleanGrantsLayer2 
        SET IsRolling = 1,
            RollingDetectionMethod = 'multiple-cycles',
            RollingConfidenceScore = 0.75,
            RollingKeywords = 'multiple cycles'
        WHERE IsRolling = 0 AND (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%multiple deadlines%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%multiple rounds%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%multiple cycles%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%quarterly%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%monthly%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%regular intervals%'
        );
        
        -- MEDIUM CONFIDENCE: Submit when ready
        UPDATE CleanGrantsLayer2 
        SET IsRolling = 1,
            RollingDetectionMethod = 'submit-when-ready',
            RollingConfidenceScore = 0.75,
            RollingKeywords = 'submit when ready'
        WHERE IsRolling = 0 AND (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%submit when ready%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%when ready%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%as needed%'
        );
        
        -- DEADLINE FIELD ANALYSIS: Check for TBD/rolling in deadline field
        UPDATE CleanGrantsLayer2 
        SET IsRolling = 1,
            RollingDetectionMethod = 'deadline-field-analysis',
            RollingConfidenceScore = 0.9,
            RollingKeywords = 'deadline field'
        WHERE IsRolling = 0 AND (
            LOWER(ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%rolling%' OR
            LOWER(ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%tbd%' OR
            LOWER(ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%to be determined%' OR
            LOWER(ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%to be announced%' OR
            LOWER(ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%tba%' OR
            LOWER(ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%ongoing%' OR
            LOWER(ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%continuous%' OR
            LOWER(ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%no deadline%' OR
            LOWER(ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%none%'
        );
        
        -- NEGATIVE INDICATORS: Reduce confidence if specific dates are mentioned
        UPDATE CleanGrantsLayer2 
        SET RollingConfidenceScore = RollingConfidenceScore * 0.7,
            RollingDetectionMethod = RollingDetectionMethod + '-with-date-conflict'
        WHERE IsRolling = 1 AND (
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%january%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%february%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%march%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%april%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%may%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%june%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%july%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%august%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%september%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%october%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%november%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%december%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%2024%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%2025%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%2026%' OR
            LOWER(Title + ' ' + ISNULL(Description, '') + ' ' + ISNULL(CAST(Deadline AS NVARCHAR(MAX)), '')) LIKE '%due%'
        );
        
        -- FINAL CONFIDENCE CHECK: Remove rolling designation if confidence too low
        UPDATE CleanGrantsLayer2 
        SET IsRolling = 0,
            RollingDetectionMethod = 'removed-low-confidence',
            RollingConfidenceScore = NULL
        WHERE IsRolling = 1 AND RollingConfidenceScore < 0.5;
        
        -- Update processing timestamps
        UPDATE CleanGrantsLayer2 
        SET RollingProcessedDate = GETDATE()
        WHERE IsRolling = 1;
        
        -- Generate summary statistics
        SELECT 
            'ROLLING_DETECTION_COMPLETE' as Status,
            COUNT(*) as Total_Records,
            COUNT(CASE WHEN IsRolling = 1 THEN 1 END) as Rolling_Opportunities,
            COUNT(CASE WHEN IsRolling = 0 THEN 1 END) as Fixed_Deadline_Opportunities,
            ROUND(100.0 * COUNT(CASE WHEN IsRolling = 1 THEN 1 END) / COUNT(*), 2) as Rolling_Percentage,
            AVG(CASE WHEN IsRolling = 1 THEN RollingConfidenceScore END) as Avg_Confidence_Score,
            MIN(CASE WHEN IsRolling = 1 THEN RollingConfidenceScore END) as Min_Confidence_Score,
            MAX(CASE WHEN IsRolling = 1 THEN RollingConfidenceScore END) as Max_Confidence_Score
        FROM CleanGrantsLayer2;
        """
        
        result = self.execute_sql_command(rolling_sql, timeout=600)
        return result is not None and 'ROLLING_DETECTION_COMPLETE' in str(result)

    def generate_rolling_report(self):
        """Generate comprehensive rolling application detection report"""
        logger.info("📊 Generating rolling application detection report...")
        
        report_sql = """
        -- Rolling Detection Method Distribution
        SELECT 
            'ROLLING_DETECTION_METHODS' as Report_Type,
            RollingDetectionMethod as Detection_Method,
            COUNT(*) as Opportunity_Count,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM CleanGrantsLayer2 WHERE IsRolling = 1), 2) as Percentage_of_Rolling,
            AVG(RollingConfidenceScore) as Avg_Confidence_Score,
            MIN(RollingConfidenceScore) as Min_Confidence,
            MAX(RollingConfidenceScore) as Max_Confidence
        FROM CleanGrantsLayer2
        WHERE IsRolling = 1
        GROUP BY RollingDetectionMethod
        ORDER BY COUNT(*) DESC;
        
        -- Rolling Keywords Analysis
        SELECT 
            'ROLLING_KEYWORDS_ANALYSIS' as Analysis_Type,
            RollingKeywords as Keywords_Found,
            COUNT(*) as Opportunity_Count,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM CleanGrantsLayer2 WHERE IsRolling = 1), 2) as Percentage_of_Rolling,
            AVG(RollingConfidenceScore) as Avg_Confidence_Score
        FROM CleanGrantsLayer2
        WHERE IsRolling = 1 AND RollingKeywords IS NOT NULL
        GROUP BY RollingKeywords
        ORDER BY COUNT(*) DESC;
        
        -- Sample rolling opportunities
        SELECT TOP 10
            'ROLLING_SAMPLES' as Sample_Type,
            SUBSTRING(Title, 1, 60) as Title_Preview,
            SUBSTRING(ISNULL(CAST(Deadline AS NVARCHAR(MAX)), 'No deadline specified'), 1, 30) as Deadline_Preview,
            RollingDetectionMethod,
            RollingConfidenceScore,
            RollingKeywords
        FROM CleanGrantsLayer2
        WHERE IsRolling = 1
        ORDER BY RollingConfidenceScore DESC;
        
        -- Agency-wise rolling analysis
        SELECT TOP 20
            'AGENCY_ROLLING_ANALYSIS' as Analysis_Type,
            AgencyName,
            COUNT(*) as Total_Opportunities,
            COUNT(CASE WHEN IsRolling = 1 THEN 1 END) as Rolling_Opportunities,
            ROUND(100.0 * COUNT(CASE WHEN IsRolling = 1 THEN 1 END) / COUNT(*), 2) as Rolling_Percentage,
            AVG(CASE WHEN IsRolling = 1 THEN RollingConfidenceScore END) as Avg_Rolling_Confidence
        FROM CleanGrantsLayer2
        WHERE AgencyName IS NOT NULL
        GROUP BY AgencyName
        HAVING COUNT(*) >= 5
        ORDER BY COUNT(CASE WHEN IsRolling = 1 THEN 1 END) DESC;
        
        -- Rolling vs Fixed Deadline Comparison
        SELECT 
            'ROLLING_VS_FIXED_COMPARISON' as Analysis_Type,
            'Rolling Applications' as Application_Type,
            COUNT(*) as Opportunity_Count,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM CleanGrantsLayer2), 2) as Percentage_of_Total,
            AVG(CASE WHEN AwardValueUSD IS NOT NULL THEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardValueUSD, '$', ''), ',', ''), ' USD', '') AS BIGINT) END) as Avg_Award_Value
        FROM CleanGrantsLayer2
        WHERE IsRolling = 1
        UNION ALL
        SELECT 
            'ROLLING_VS_FIXED_COMPARISON' as Analysis_Type,
            'Fixed Deadline Applications' as Application_Type,
            COUNT(*) as Opportunity_Count,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM CleanGrantsLayer2), 2) as Percentage_of_Total,
            AVG(CASE WHEN AwardValueUSD IS NOT NULL THEN TRY_CAST(REPLACE(REPLACE(REPLACE(AwardValueUSD, '$', ''), ',', ''), ' USD', '') AS BIGINT) END) as Avg_Award_Value
        FROM CleanGrantsLayer2
        WHERE IsRolling = 0
        ORDER BY Opportunity_Count DESC;
        
        -- High confidence rolling opportunities
        SELECT TOP 20
            'HIGH_CONFIDENCE_ROLLING' as Sample_Type,
            SUBSTRING(Title, 1, 50) as Title_Preview,
            SUBSTRING(ISNULL(CAST(Deadline AS NVARCHAR(MAX)), 'No deadline'), 1, 25) as Deadline_Preview,
            RollingDetectionMethod,
            RollingConfidenceScore,
            RollingKeywords,
            AgencyName
        FROM CleanGrantsLayer2
        WHERE IsRolling = 1 AND RollingConfidenceScore >= 0.9
        ORDER BY RollingConfidenceScore DESC;
        """
        
        result = self.execute_sql_command(report_sql, timeout=300)
        return result is not None

    def create_rolling_views(self):
        """Create rolling application analysis views"""
        logger.info("🎯 Creating rolling application analysis views...")
        
        views_sql = """
        -- Create rolling-based views for analysis
        CREATE OR ALTER VIEW vw_Rolling_Opportunities AS
        SELECT 
            OpportunityNumber,
            Title,
            Description,
            Deadline,
            AgencyName,
            RunweiCategory,
            IsRolling,
            RollingDetectionMethod,
            RollingConfidenceScore,
            RollingKeywords,
            AwardValueUSD,
            EstimatedTotalFunding,
            PostedDate,
            RollingProcessedDate
        FROM CleanGrantsLayer2
        WHERE IsRolling = 1;
        
        CREATE OR ALTER VIEW vw_Fixed_Deadline_Opportunities AS
        SELECT 
            OpportunityNumber,
            Title,
            Description,
            Deadline,
            AgencyName,
            RunweiCategory,
            AwardValueUSD,
            EstimatedTotalFunding,
            PostedDate
        FROM CleanGrantsLayer2
        WHERE IsRolling = 0 AND Deadline IS NOT NULL;
        
        CREATE OR ALTER VIEW vw_Rolling_Summary AS
        SELECT 
            'Rolling Applications Summary' as Report_Type,
            COUNT(*) as Total_Opportunities,
            COUNT(CASE WHEN IsRolling = 1 THEN 1 END) as Rolling_Opportunities,
            COUNT(CASE WHEN IsRolling = 0 THEN 1 END) as Fixed_Deadline_Opportunities,
            ROUND(100.0 * COUNT(CASE WHEN IsRolling = 1 THEN 1 END) / COUNT(*), 2) as Rolling_Percentage,
            AVG(CASE WHEN IsRolling = 1 THEN RollingConfidenceScore END) as Avg_Rolling_Confidence
        FROM CleanGrantsLayer2;
        
        CREATE OR ALTER VIEW vw_High_Confidence_Rolling AS
        SELECT *
        FROM CleanGrantsLayer2
        WHERE IsRolling = 1 AND RollingConfidenceScore >= 0.9;
        
        SELECT 'ROLLING_VIEWS_CREATED' as Status, GETDATE() as Created_At;
        """
        
        result = self.execute_sql_command(views_sql, timeout=120)
        return result is not None

    def run_complete_rolling_processing(self):
        """Run the complete rolling application detection processing pipeline"""
        logger.info("📅 STARTING ROLLING APPLICATION DETECTION PROCESSING")
        logger.info("=" * 65)
        
        steps = [
            ("Ensure Rolling Processing Columns", self.ensure_rolling_processing_columns),
            ("Process Rolling Detection", self.process_rolling_detection),
            ("Generate Rolling Report", self.generate_rolling_report),
            ("Create Rolling Views", self.create_rolling_views)
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
        
        logger.info(f"\n📅 ROLLING PROCESSING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"✅ Completed Steps: {success_count}/{len(steps)}")
        
        if success_count >= 3:
            logger.info("🎉 ROLLING APPLICATION DETECTION PROCESSING SUCCESS!")
            logger.info("\n📊 VERIFICATION QUERIES:")
            logger.info("   → SELECT COUNT(*), SUM(CAST(IsRolling AS INT)) FROM CleanGrantsLayer2;")
            logger.info("   → SELECT * FROM vw_Rolling_Opportunities ORDER BY RollingConfidenceScore DESC;")
            logger.info("   → SELECT RollingDetectionMethod, COUNT(*) FROM CleanGrantsLayer2 WHERE IsRolling = 1 GROUP BY RollingDetectionMethod;")
            logger.info("   → SELECT * FROM vw_Rolling_Summary;")
            return True
        else:
            logger.error("❌ Rolling application detection processing failed")
            return False

def main():
    """Main execution function"""
    print("📅 ROLLING APPLICATION DETECTOR - RUNWEI STANDARDS")
    print("=" * 60)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Detecting opportunities with rolling/ongoing deadlines:")
    print("   • Rolling basis applications")
    print("   • Year-round applications")
    print("   • No deadline specified")
    print("   • Apply anytime opportunities")
    print("   • Until filled/ongoing opportunities")
    print("🔍 Using intelligent keyword and deadline field analysis")
    print("📐 Output: IsRolling flag (True/False)")
    print()
    
    detector = RollingApplicationDetector()
    success = detector.run_complete_rolling_processing()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 ROLLING APPLICATION DETECTION COMPLETED SUCCESSFULLY!")
        print("✅ All opportunities analyzed for rolling deadlines")
        print("✅ Confidence scores calculated for rolling detection")
        print("✅ Detection methods documented")
        print("✅ Keywords captured for analysis")
        print("✅ Analysis views created")
        print("\n🔍 VERIFICATION QUERIES:")
        print("   1. SELECT COUNT(*), SUM(CAST(IsRolling AS INT)) FROM CleanGrantsLayer2;")
        print("   2. SELECT * FROM vw_Rolling_Opportunities;")
        print("   3. SELECT RollingDetectionMethod, COUNT(*) FROM CleanGrantsLayer2")
        print("      WHERE IsRolling = 1 GROUP BY RollingDetectionMethod;")
        print("   4. SELECT * FROM vw_Rolling_Summary;")
        print("   5. SELECT * FROM vw_High_Confidence_Rolling;")
        print("\n📅 Your grants database now includes rolling deadline detection!")
        print("💡 Use IsRolling flag for filtering and time-sensitive analysis")
    else:
        print("❌ ROLLING APPLICATION DETECTION PROCESSING FAILED")
        print("📝 Please check the logs for details")
    
    print(f"\n📅 Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()