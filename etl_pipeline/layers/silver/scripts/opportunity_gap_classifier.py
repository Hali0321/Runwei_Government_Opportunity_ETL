#!/usr/bin/env python3
"""
Azure SQL Database - Enhanced Opportunity Gap Classifier (v3.1)
Handles blanks investigation and standardized formatting:
💰 Access to: Capital (primary)
🤝 Access to: Networks  
📚 Access to: Capacity Building

Standardized Format with Capital prioritization and blank investigation
"""

import subprocess
import logging
import re
from datetime import datetime
from pathlib import Path

# Configure logging
SCRIPT_DIR = Path(__file__).parent
PYCACHE_DIR = SCRIPT_DIR / "__pycache__"
PYCACHE_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    handlers=[
        logging.FileHandler(PYCACHE_DIR / 'opportunity_gap_classifier_enhanced.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedOpportunityGapClassifier:
    """Enhanced Opportunity Gap Classifier with blank investigation and standardized formatting"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        
        # Enhanced keyword dictionaries for better detection
        self.capital_keywords = {
            'strong': ['grant', 'funding', 'fund', 'prize', 'award', 'stipend', 'cash', 'money', 'financial assistance', 'investment'],
            'moderate': ['budget', 'cost', 'expense', 'reimbursement', 'allowance', 'subsidy', 'scholarship', 'financial support'],
            'weak': ['dollar', 'million', 'thousand', 'funding amount', 'maximum award']
        }
        
        self.networks_keywords = {
            'strong': ['mentor', 'mentorship', 'advisor', 'fellowship', 'accelerator', 'partnership', 'collaboration'],
            'moderate': ['network', 'networking', 'connect', 'peer', 'showcase', 'investor', 'demo day'],
            'weak': ['exhibition', 'conference', 'cohort', 'community', 'alumni']
        }
        
        self.capacity_keywords = {
            'strong': ['training', 'workshop', 'bootcamp', 'coaching', 'technical assistance', 'professional development'],
            'moderate': ['incubator', 'business support', 'capacity building', 'skill building', 'education'],
            'weak': ['program', 'certification', 'learning', 'development', 'consulting']
        }

    def execute_sql_command(self, sql_query, timeout=300):
        """Execute SQL command with Azure SQL Database"""
        try:
            cmd = [
                "sqlcmd", "-S", self.server,
                "-d", self.database, 
                "-U", self.username, 
                "-P", self.password,
                "-Q", sql_query, 
                "-C", "-t", str(timeout), "-I", "-b"
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
                
        except Exception as e:
            logger.error(f"❌ Error executing SQL: {e}")
            return None

    def investigate_blank_records(self):
        """Investigate records that would result in blanks and attempt to classify them"""
        logger.info("🔍 Investigating potentially blank records...")
        
        investigation_sql = """
        -- BLANK RECORDS INVESTIGATION
        -- Find records that don't match our current criteria and analyze them
        
        -- Step 1: Identify potentially blank records
        WITH PotentialBlanks AS (
            SELECT 
                OpportunityNumber,
                Title,
                Description,
                FundingType,
                AwardCeiling,
                EstimatedTotalFunding,
                -- Current classification attempt
                CASE 
                    WHEN (
                        UPPER(COALESCE(Title, '')) LIKE '%GRANT%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%FUNDING%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%AWARD%' OR 
                        UPPER(COALESCE(Description, '')) LIKE '%GRANT%' OR 
                        UPPER(COALESCE(Description, '')) LIKE '%FUNDING%' OR 
                        AwardCeiling IS NOT NULL OR
                        EstimatedTotalFunding IS NOT NULL OR
                        UPPER(FundingType) LIKE '%GRANT%'
                    ) THEN 1 ELSE 0 END as Has_Capital_Keywords,
                CASE 
                    WHEN (
                        UPPER(COALESCE(Title, '')) LIKE '%FELLOWSHIP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%MENTOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%NETWORK%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%ADVISOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%ACCELERATOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%PARTNERSHIP%'
                    ) THEN 1 ELSE 0 END as Has_Networks_Keywords,
                CASE 
                    WHEN (
                        UPPER(COALESCE(Description, '')) LIKE '%TRAINING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%WORKSHOP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%BOOTCAMP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%COACHING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%TECHNICAL ASSISTANCE%'
                    ) THEN 1 ELSE 0 END as Has_Capacity_Keywords
            FROM CleanGrantsLayer2
        )
        SELECT 
            'BLANK_INVESTIGATION' as Analysis_Type,
            COUNT(*) as Total_Records,
            COUNT(CASE WHEN Has_Capital_Keywords = 0 AND Has_Networks_Keywords = 0 AND Has_Capacity_Keywords = 0 THEN 1 END) as Potential_Blanks,
            COUNT(CASE WHEN Has_Capital_Keywords = 1 THEN 1 END) as Has_Capital,
            COUNT(CASE WHEN Has_Networks_Keywords = 1 THEN 1 END) as Has_Networks,
            COUNT(CASE WHEN Has_Capacity_Keywords = 1 THEN 1 END) as Has_Capacity
        FROM PotentialBlanks;
        
        -- Step 2: Sample potential blank records for investigation
        SELECT TOP 10
            'POTENTIAL_BLANK_SAMPLES' as Sample_Type,
            OpportunityNumber,
            LEFT(Title, 80) as Title_Sample,
            LEFT(Description, 120) as Description_Sample,
            FundingType,
            CASE WHEN AwardCeiling IS NOT NULL THEN 'HAS_AWARD' ELSE 'NO_AWARD' END as Award_Status,
            -- Weak signal detection
            CASE 
                WHEN UPPER(COALESCE(Description, '')) LIKE '%SUPPORT%' THEN 'SUPPORT_MENTIONED'
                WHEN UPPER(COALESCE(Description, '')) LIKE '%ASSISTANCE%' THEN 'ASSISTANCE_MENTIONED'  
                WHEN UPPER(COALESCE(Description, '')) LIKE '%PROGRAM%' THEN 'PROGRAM_MENTIONED'
                WHEN UPPER(COALESCE(Description, '')) LIKE '%OPPORTUNITY%' THEN 'OPPORTUNITY_MENTIONED'
                WHEN UPPER(COALESCE(Description, '')) LIKE '%RESEARCH%' THEN 'RESEARCH_MENTIONED'
                WHEN UPPER(COALESCE(Description, '')) LIKE '%PROJECT%' THEN 'PROJECT_MENTIONED'
                ELSE 'NO_WEAK_SIGNALS'
            END as Weak_Signals
        FROM CleanGrantsLayer2
        WHERE NOT (
            -- Capital keywords
            (UPPER(COALESCE(Title, '')) LIKE '%GRANT%' OR 
             UPPER(COALESCE(Title, '')) LIKE '%FUNDING%' OR 
             UPPER(COALESCE(Title, '')) LIKE '%AWARD%' OR 
             UPPER(COALESCE(Description, '')) LIKE '%GRANT%' OR 
             UPPER(COALESCE(Description, '')) LIKE '%FUNDING%' OR 
             AwardCeiling IS NOT NULL OR
             EstimatedTotalFunding IS NOT NULL OR
             UPPER(FundingType) LIKE '%GRANT%')
            OR
            -- Networks keywords
            (UPPER(COALESCE(Title, '')) LIKE '%FELLOWSHIP%' OR
             UPPER(COALESCE(Description, '')) LIKE '%MENTOR%' OR
             UPPER(COALESCE(Description, '')) LIKE '%NETWORK%' OR
             UPPER(COALESCE(Description, '')) LIKE '%ADVISOR%' OR
             UPPER(COALESCE(Description, '')) LIKE '%ACCELERATOR%' OR
             UPPER(COALESCE(Description, '')) LIKE '%PARTNERSHIP%')
            OR
            -- Capacity keywords
            (UPPER(COALESCE(Description, '')) LIKE '%TRAINING%' OR
             UPPER(COALESCE(Description, '')) LIKE '%WORKSHOP%' OR
             UPPER(COALESCE(Description, '')) LIKE '%BOOTCAMP%' OR
             UPPER(COALESCE(Description, '')) LIKE '%COACHING%' OR
             UPPER(COALESCE(Description, '')) LIKE '%TECHNICAL ASSISTANCE%')
        )
        ORDER BY NEWID();
        """
        
        result = self.execute_sql_command(investigation_sql, timeout=120)
        return result is not None

    def run_enhanced_opportunity_gap_classification(self):
        """Run enhanced classification with blank investigation and standardized formatting"""
        logger.info("🎯 Running Enhanced Opportunity Gap Classification...")
        logger.info("💰 Capital-First Hierarchy with Blank Investigation")
        logger.info("🔍 Investigating weak signals for potential blanks")
        logger.info("📝 Standardized Format: 'Access to: Capital, Networks, Capacity Building'")
        
        classification_sql = """
        -- ENHANCED OPPORTUNITY GAP CLASSIFICATION v3.1
        -- Capital-first hierarchy with blank investigation and standardized formatting
        
        UPDATE CleanGrantsLayer2
        SET OpportunityGap = 
            CASE 
                -- CAPITAL + NETWORKS + CAPACITY BUILDING (All three - standardized order)
                WHEN (
                    -- Strong Capital indicators (prioritized)
                    (
                        UPPER(COALESCE(Title, '')) LIKE '%GRANT%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%FUNDING%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%AWARD%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%PRIZE%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%GRANT%' OR 
                        UPPER(COALESCE(Description, '')) LIKE '%FUNDING%' OR 
                        UPPER(COALESCE(Description, '')) LIKE '%FINANCIAL%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%CASH%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%STIPEND%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%INVESTMENT%' OR
                        AwardCeiling IS NOT NULL OR
                        EstimatedTotalFunding IS NOT NULL OR
                        UPPER(FundingType) LIKE '%GRANT%'
                    )
                    AND
                    -- Strong Networks indicators  
                    (
                        UPPER(COALESCE(Title, '')) LIKE '%FELLOWSHIP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%MENTOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%NETWORK%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%ADVISOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%ACCELERATOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%PARTNERSHIP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%COLLABORATION%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%SHOWCASE%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%INVESTOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%PEER%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%COHORT%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%COMMUNITY%'
                    )
                    AND
                    -- Strong Capacity Building indicators
                    (
                        UPPER(COALESCE(Description, '')) LIKE '%TRAINING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%WORKSHOP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%BOOTCAMP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%COACHING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%TECHNICAL ASSISTANCE%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%PROFESSIONAL DEVELOPMENT%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%CAPACITY BUILDING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%SKILL BUILDING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%INCUBATOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%BUSINESS SUPPORT%'
                    )
                )
                THEN 'Access to: Capital, Networks, Capacity Building'
                
                -- CAPITAL + NETWORKS (standardized order)
                WHEN (
                    -- Capital indicators (always first)
                    (
                        UPPER(COALESCE(Title, '')) LIKE '%GRANT%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%FUNDING%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%AWARD%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%PRIZE%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%GRANT%' OR 
                        UPPER(COALESCE(Description, '')) LIKE '%FUNDING%' OR 
                        UPPER(COALESCE(Description, '')) LIKE '%FINANCIAL%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%CASH%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%STIPEND%' OR
                        AwardCeiling IS NOT NULL OR
                        EstimatedTotalFunding IS NOT NULL OR
                        UPPER(FundingType) LIKE '%GRANT%'
                    )
                    AND
                    -- Networks indicators  
                    (
                        UPPER(COALESCE(Title, '')) LIKE '%FELLOWSHIP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%MENTOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%NETWORK%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%ADVISOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%ACCELERATOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%PARTNERSHIP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%COLLABORATION%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%SHOWCASE%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%INVESTOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%COHORT%'
                    )
                )
                THEN 'Access to: Capital, Networks'
                
                -- CAPITAL + CAPACITY BUILDING (standardized order)
                WHEN (
                    -- Capital indicators (always first)
                    (
                        UPPER(COALESCE(Title, '')) LIKE '%GRANT%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%FUNDING%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%AWARD%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%GRANT%' OR 
                        UPPER(COALESCE(Description, '')) LIKE '%FUNDING%' OR 
                        UPPER(COALESCE(Description, '')) LIKE '%FINANCIAL%' OR
                        AwardCeiling IS NOT NULL OR
                        EstimatedTotalFunding IS NOT NULL OR
                        UPPER(FundingType) LIKE '%GRANT%'
                    )
                    AND
                    -- Capacity Building indicators
                    (
                        UPPER(COALESCE(Description, '')) LIKE '%TRAINING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%WORKSHOP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%BOOTCAMP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%COACHING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%TECHNICAL ASSISTANCE%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%PROFESSIONAL DEVELOPMENT%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%CAPACITY BUILDING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%BUSINESS SUPPORT%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%INCUBATOR%'
                    )
                )
                THEN 'Access to: Capital, Capacity Building'
                
                -- NETWORKS + CAPACITY BUILDING (only if Capital is truly not included)
                WHEN (
                    -- Explicitly NO Capital indicators
                    NOT (
                        UPPER(COALESCE(Title, '')) LIKE '%GRANT%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%FUNDING%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%AWARD%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%GRANT%' OR 
                        UPPER(COALESCE(Description, '')) LIKE '%FUNDING%' OR 
                        UPPER(COALESCE(Description, '')) LIKE '%FINANCIAL%' OR
                        AwardCeiling IS NOT NULL OR
                        EstimatedTotalFunding IS NOT NULL OR
                        UPPER(FundingType) LIKE '%GRANT%'
                    )
                    AND
                    -- Has Networks indicators  
                    (
                        UPPER(COALESCE(Title, '')) LIKE '%FELLOWSHIP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%MENTOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%NETWORK%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%ADVISOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%ACCELERATOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%PARTNERSHIP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%COLLABORATION%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%SHOWCASE%'
                    )
                    AND
                    -- Has Capacity Building indicators
                    (
                        UPPER(COALESCE(Description, '')) LIKE '%TRAINING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%WORKSHOP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%BOOTCAMP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%COACHING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%TECHNICAL ASSISTANCE%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%PROFESSIONAL DEVELOPMENT%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%INCUBATOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%BUSINESS SUPPORT%'
                    )
                )
                THEN 'Access to: Networks, Capacity Building'
                
                -- PURE ACCESS TO CAPITAL (most common - strong indicators)
                WHEN (
                    UPPER(COALESCE(Title, '')) LIKE '%GRANT%' OR 
                    UPPER(COALESCE(Title, '')) LIKE '%FUNDING%' OR 
                    UPPER(COALESCE(Title, '')) LIKE '%AWARD%' OR 
                    UPPER(COALESCE(Title, '')) LIKE '%PRIZE%' OR
                    UPPER(COALESCE(Description, '')) LIKE '%GRANT%' OR 
                    UPPER(COALESCE(Description, '')) LIKE '%FUNDING%' OR 
                    UPPER(COALESCE(Description, '')) LIKE '%FINANCIAL SUPPORT%' OR
                    UPPER(COALESCE(Description, '')) LIKE '%CASH PRIZE%' OR
                    UPPER(COALESCE(Description, '')) LIKE '%STIPEND%' OR
                    UPPER(COALESCE(Description, '')) LIKE '%INVESTMENT%' OR
                    UPPER(COALESCE(Description, '')) LIKE '%SEED FUNDING%' OR
                    AwardCeiling IS NOT NULL OR
                    EstimatedTotalFunding IS NOT NULL OR
                    UPPER(FundingType) LIKE '%GRANT%'
                )
                THEN 'Access to: Capital'
                
                -- PURE ACCESS TO NETWORKS (only if Capital truly not present)
                WHEN (
                    -- Explicitly NO Capital indicators
                    NOT (
                        UPPER(COALESCE(Title, '')) LIKE '%GRANT%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%FUNDING%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%AWARD%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%GRANT%' OR 
                        UPPER(COALESCE(Description, '')) LIKE '%FUNDING%' OR 
                        AwardCeiling IS NOT NULL OR
                        EstimatedTotalFunding IS NOT NULL OR
                        UPPER(FundingType) LIKE '%GRANT%'
                    )
                    AND
                    -- Strong Networks indicators
                    (
                        UPPER(COALESCE(Title, '')) LIKE '%FELLOWSHIP%' OR
                        UPPER(COALESCE(Title, '')) LIKE '%NETWORK%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%MENTOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%MENTORSHIP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%ADVISOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%ACCELERATOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%PARTNERSHIP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%COLLABORATION%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%SHOWCASE%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%INVESTOR ACCESS%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%PEER NETWORK%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%COHORT%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%COMMUNITY%'
                    )
                )
                THEN 'Access to: Networks'
                
                -- PURE ACCESS TO CAPACITY BUILDING (only if Capital truly not present)
                WHEN (
                    -- Explicitly NO Capital indicators
                    NOT (
                        UPPER(COALESCE(Title, '')) LIKE '%GRANT%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%FUNDING%' OR 
                        UPPER(COALESCE(Title, '')) LIKE '%AWARD%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%GRANT%' OR 
                        UPPER(COALESCE(Description, '')) LIKE '%FUNDING%' OR 
                        AwardCeiling IS NOT NULL OR
                        EstimatedTotalFunding IS NOT NULL OR
                        UPPER(FundingType) LIKE '%GRANT%'
                    )
                    AND
                    -- Strong Capacity Building indicators
                    (
                        UPPER(COALESCE(Title, '')) LIKE '%TRAINING%' OR
                        UPPER(COALESCE(Title, '')) LIKE '%WORKSHOP%' OR
                        UPPER(COALESCE(Title, '')) LIKE '%BOOTCAMP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%TRAINING PROGRAM%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%WORKSHOP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%BOOTCAMP%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%COACHING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%TECHNICAL ASSISTANCE%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%PROFESSIONAL DEVELOPMENT%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%CAPACITY BUILDING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%SKILL BUILDING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%BUSINESS COACHING%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%INCUBATOR%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%BUSINESS SUPPORT%' OR
                        UPPER(COALESCE(Description, '')) LIKE '%CONSULTING%'
                    )
                )
                THEN 'Access to: Capacity Building'
                
                -- WEAK SIGNAL DETECTION for potential blanks
                WHEN (
                    -- Research/project opportunities (often have capital component)
                    UPPER(COALESCE(Title, '')) LIKE '%RESEARCH%' OR
                    UPPER(COALESCE(Description, '')) LIKE '%RESEARCH%' OR
                    UPPER(COALESCE(Description, '')) LIKE '%PROJECT%' OR
                    UPPER(COALESCE(Description, '')) LIKE '%STUDY%' OR
                    -- Support programs (often capital or capacity)
                    UPPER(COALESCE(Description, '')) LIKE '%SUPPORT%' OR
                    UPPER(COALESCE(Description, '')) LIKE '%ASSISTANCE%' OR
                    UPPER(COALESCE(Description, '')) LIKE '%HELP%' OR
                    -- General opportunity language (likely capital)
                    UPPER(COALESCE(Description, '')) LIKE '%OPPORTUNITY%' OR
                    UPPER(COALESCE(Description, '')) LIKE '%PROGRAM%'
                )
                THEN 'Access to: Capital'  -- Default weak signals to Capital
                
                -- FINAL FALLBACK: Mark as "Not specified" instead of blank
                ELSE 'Not specified'
            END,
            ProcessedBy = 'Enhanced_OpportunityGap_Classifier_v3.1_Standardized',
            UpdatedDate = GETDATE()
        WHERE OpportunityGap IS NULL 
           OR OpportunityGap = 'TBD_OPPORTUNITY_GAP'
           OR OpportunityGap = 'Standard Opportunity'
           OR OpportunityGap = 'Equity Focus'
           OR OpportunityGap = '';
        
        -- Results Summary with standardized format verification
        SELECT 
            'ENHANCED_CLASSIFICATION_RESULTS' as Status,
            COUNT(*) as Total_Records_Processed,
            COUNT(CASE WHEN OpportunityGap = 'Access to: Capital' THEN 1 END) as Pure_Capital,
            COUNT(CASE WHEN OpportunityGap = 'Access to: Networks' THEN 1 END) as Pure_Networks, 
            COUNT(CASE WHEN OpportunityGap = 'Access to: Capacity Building' THEN 1 END) as Pure_Capacity_Building,
            COUNT(CASE WHEN OpportunityGap = 'Access to: Capital, Networks' THEN 1 END) as Capital_Networks_Combo,
            COUNT(CASE WHEN OpportunityGap = 'Access to: Capital, Capacity Building' THEN 1 END) as Capital_Capacity_Combo,
            COUNT(CASE WHEN OpportunityGap = 'Access to: Networks, Capacity Building' THEN 1 END) as Networks_Capacity_Combo,
            COUNT(CASE WHEN OpportunityGap = 'Access to: Capital, Networks, Capacity Building' THEN 1 END) as All_Three_Combo,
            COUNT(CASE WHEN OpportunityGap = 'Not specified' THEN 1 END) as Not_Specified,
            COUNT(CASE WHEN OpportunityGap = '' OR OpportunityGap IS NULL THEN 1 END) as Still_Blank,
            ROUND(AVG(CASE WHEN OpportunityGap != 'Not specified' AND OpportunityGap != '' AND OpportunityGap IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) as Clear_Classification_Rate,
            -- Format verification
            COUNT(CASE WHEN OpportunityGap LIKE 'Access to:%' THEN 1 END) as Properly_Formatted,
            COUNT(CASE WHEN OpportunityGap NOT LIKE 'Access to:%' AND OpportunityGap != 'Not specified' THEN 1 END) as Format_Issues
        FROM CleanGrantsLayer2;
        """
        
        result = self.execute_sql_command(classification_sql, timeout=300)
        
        if result:
            logger.info("🎯 Enhanced Opportunity Gap Classification Results:")
            logger.info(result)
            return 'ENHANCED_CLASSIFICATION_RESULTS' in str(result)
        else:
            logger.error("❌ Enhanced opportunity gap classification failed")
            return False

    def show_standardized_distribution_and_quality_check(self):
        """Show final standardized distribution and quality verification"""
        logger.info("📊 Generating standardized distribution and quality check...")
        
        distribution_sql = """
        -- STANDARDIZED DISTRIBUTION WITH QUALITY CHECK
        SELECT 
            'STANDARDIZED_FINAL_DISTRIBUTION' as Report_Type,
            OpportunityGap as Gap_Category,
            COUNT(*) as Record_Count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM CleanGrantsLayer2), 2) as Percentage,
            -- Quality indicators
            CASE 
                WHEN OpportunityGap LIKE 'Access to:%' THEN '✅ Properly Formatted'
                WHEN OpportunityGap = 'Not specified' THEN '⚠️ Needs Investigation'
                WHEN OpportunityGap = '' OR OpportunityGap IS NULL THEN '❌ Still Blank'
                ELSE '❓ Format Issue'
            END as Quality_Status
        FROM CleanGrantsLayer2
        GROUP BY OpportunityGap
        ORDER BY Record_Count DESC;
        
        -- HIERARCHY VERIFICATION (Capital should be most common)
        SELECT 
            'HIERARCHY_VERIFICATION' as Report_Type,
            'Capital dominance check' as Check_Type,
            COUNT(CASE WHEN OpportunityGap LIKE '%Capital%' THEN 1 END) as Records_With_Capital,
            COUNT(CASE WHEN OpportunityGap NOT LIKE '%Capital%' AND OpportunityGap != 'Not specified' THEN 1 END) as Records_Without_Capital,
            ROUND(COUNT(CASE WHEN OpportunityGap LIKE '%Capital%' THEN 1 END) * 100.0 / 
                  NULLIF(COUNT(CASE WHEN OpportunityGap != 'Not specified' THEN 1 END), 0), 2) as Capital_Percentage,
            CASE 
                WHEN COUNT(CASE WHEN OpportunityGap LIKE '%Capital%' THEN 1 END) > 
                     COUNT(CASE WHEN OpportunityGap NOT LIKE '%Capital%' AND OpportunityGap != 'Not specified' THEN 1 END)
                THEN '✅ Capital is dominant (expected)'
                ELSE '⚠️ Capital not dominant (unexpected)'
            END as Hierarchy_Status
        FROM CleanGrantsLayer2;
        
        -- SAMPLES FOR EACH STANDARDIZED CATEGORY
        SELECT TOP 3
            'CAPITAL_SAMPLES' as Category,
            OpportunityNumber,
            LEFT(Title, 60) as Title_Sample,
            LEFT(Description, 100) as Description_Sample,
            OpportunityGap
        FROM CleanGrantsLayer2
        WHERE OpportunityGap = 'Access to: Capital'
        ORDER BY NEWID();
        
        SELECT TOP 3
            'CAPITAL_NETWORKS_SAMPLES' as Category,
            OpportunityNumber,
            LEFT(Title, 60) as Title_Sample,
            LEFT(Description, 100) as Description_Sample,
            OpportunityGap
        FROM CleanGrantsLayer2
        WHERE OpportunityGap = 'Access to: Capital, Networks'
        ORDER BY NEWID();
        
        SELECT TOP 3
            'ALL_THREE_SAMPLES' as Category,
            OpportunityNumber,
            LEFT(Title, 60) as Title_Sample,
            LEFT(Description, 100) as Description_Sample,
            OpportunityGap
        FROM CleanGrantsLayer2
        WHERE OpportunityGap = 'Access to: Capital, Networks, Capacity Building'
        ORDER BY NEWID();
        
        -- NOT SPECIFIED SAMPLES (for manual investigation)
        SELECT TOP 5
            'NOT_SPECIFIED_SAMPLES' as Category,
            OpportunityNumber,
            LEFT(Title, 60) as Title_Sample,
            LEFT(Description, 100) as Description_Sample,
            FundingType,
            CASE WHEN AwardCeiling IS NOT NULL THEN 'HAS_AWARD' ELSE 'NO_AWARD' END as Award_Status,
            OpportunityGap
        FROM CleanGrantsLayer2
        WHERE OpportunityGap = 'Not specified'
        ORDER BY NEWID();
        """
        
        result = self.execute_sql_command(distribution_sql, timeout=120)
        return result is not None

    def run_complete_enhanced_classification(self):
        """Run complete enhanced opportunity gap classification with blank investigation"""
        logger.info("🎯 ENHANCED OPPORTUNITY GAP CLASSIFIER v3.1 - Starting...")
        logger.info("=" * 60)
        logger.info("💰 Capital-First Hierarchy (prioritized)")
        logger.info("🤝 Networks Support")
        logger.info("📚 Capacity Building Support")
        logger.info("🔍 Blank Investigation & Weak Signal Detection")
        logger.info("📝 Standardized Format: 'Access to: Capital, Networks, Capacity Building'")
        logger.info("⚠️ Unclear cases marked as 'Not specified'")
        
        steps = [
            ("Investigate Blank Records", self.investigate_blank_records),
            ("Run Enhanced Classification", self.run_enhanced_opportunity_gap_classification),
            ("Show Standardized Distribution & Quality Check", self.show_standardized_distribution_and_quality_check)
        ]
        
        success_count = 0
        for i, (step_name, step_function) in enumerate(steps, 1):
            logger.info(f"\n📍 STEP {i}/{len(steps)}: {step_name}")
            
            try:
                success = step_function()
                if success:
                    logger.info(f"✅ {step_name} completed successfully")
                    success_count += 1
                else:
                    logger.error(f"❌ {step_name} failed")
            except Exception as e:
                logger.error(f"❌ {step_name} error: {e}")
        
        logger.info(f"\n🎯 ENHANCED CLASSIFICATION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"✅ Completed Steps: {success_count}/{len(steps)}")
        
        if success_count == len(steps):
            logger.info("🎉 Enhanced Opportunity Gap Classification SUCCESS!")
            logger.info("💰 Capital-first hierarchy implemented")
            logger.info("🔍 Blank investigation completed") 
            logger.info("📚 Weak signals captured")
            logger.info("📝 Standardized format applied")
            logger.info("⚠️ Unclear cases marked as 'Not specified'")
            logger.info("✅ Zero blank records remaining")
            return True
        else:
            logger.error("❌ Some classification steps failed")
            return False

def main():
    """Main execution function"""
    print("🎯 ENHANCED OPPORTUNITY GAP CLASSIFIER v3.1")
    print("=" * 60)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Capital-First Hierarchy Classification:")
    print("   💰 Access to: Capital (prioritized)")
    print("   🤝 Access to: Networks")
    print("   📚 Access to: Capacity Building")
    print("📝 Standardized Combinations:")
    print("   → Access to: Capital, Networks")
    print("   → Access to: Capital, Capacity Building")
    print("   → Access to: Capital, Networks, Capacity Building")
    print("   → Access to: Networks, Capacity Building (only if no Capital)")
    print("🔍 Blank Investigation: Weak signals → 'Access to: Capital'")
    print("⚠️ Unclear cases → 'Not specified'")
    
    classifier = EnhancedOpportunityGapClassifier()
    success = classifier.run_complete_enhanced_classification()
    
    if success:
        print("\n🎉 ENHANCED CLASSIFICATION COMPLETED!")
        print("\n📊 RESULTS:")
        print("   🎯 Capital-first hierarchy successfully implemented")
        print("   💰 Most records should show 'Access to: Capital' (expected)")
        print("   🔍 Blank investigation completed with weak signal detection")
        print("   📝 Standardized format: 'Access to: Capital, Networks, Capacity Building'")
        print("   ⚠️ Unclear opportunities marked as 'Not specified' for manual review")
        print("   ✅ Zero blank records remaining")
        print("\n🔍 QUERY YOUR STANDARDIZED RESULTS:")
        print("   📊 Distribution:")
        print("      → SELECT OpportunityGap, COUNT(*), ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM CleanGrantsLayer2),2) as Pct")
        print("        FROM CleanGrantsLayer2 GROUP BY OpportunityGap ORDER BY COUNT(*) DESC")
        print("\n   💰 Capital Opportunities:")
        print("      → SELECT COUNT(*) FROM CleanGrantsLayer2 WHERE OpportunityGap LIKE '%Capital%'")
        print("\n   🎯 All Combinations:")
        print("      → SELECT OpportunityGap, COUNT(*) FROM CleanGrantsLayer2")
        print("        WHERE OpportunityGap LIKE 'Access to:%' GROUP BY OpportunityGap")
        print("\n   ⚠️ Not Specified (Manual Review Needed):")
        print("      → SELECT * FROM CleanGrantsLayer2 WHERE OpportunityGap = 'Not specified'")
        print("\n✅ Perfect for Layer 3 with standardized Opportunity Gap values!")
    else:
        print("\n❌ Enhanced classification failed. Check logs for details.")

if __name__ == "__main__":
    main()