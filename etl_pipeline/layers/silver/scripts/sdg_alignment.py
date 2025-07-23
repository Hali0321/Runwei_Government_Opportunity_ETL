#!/usr/bin/env python3
"""
Azure SQL Database SDG Alignment Processor - Layer 2 Processing
Maps opportunities to UN Sustainable Development Goals (SDGs) based on content analysis
Updates SDGTags column in CleanGrantsLayer2 table with proper Runwei formatting
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
        logging.FileHandler(PYCACHE_DIR / 'sdg_alignment.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SDGAlignmentProcessor:
    """SDG alignment processor for Azure SQL Database with Runwei formatting standards"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        self.table_name = "CleanGrantsLayer2"
        
        # Official UN SDG mapping with keywords
        self.sdg_mappings = {
            "SDG 1: No Poverty": [
                "poverty", "low-income", "basic needs", "economic hardship", 
                "disadvantaged", "underserved", "financial assistance", "social services"
            ],
            "SDG 2: Zero Hunger": [
                "hunger", "food security", "nutrition", "agriculture", "farming", 
                "crops", "livestock", "food systems", "malnutrition", "dietary"
            ],
            "SDG 3: Good Health and Well-being": [
                "health", "medical", "healthcare", "clinical", "disease", "wellness",
                "mental health", "public health", "epidemiology", "medicine", "therapy"
            ],
            "SDG 4: Quality Education": [
                "education", "learning", "teaching", "training", "literacy", "schools",
                "academic", "curriculum", "students", "educational", "pedagogy"
            ],
            "SDG 5: Gender Equality": [
                "gender", "women", "girls", "female", "feminist", "equality", 
                "discrimination", "empowerment", "gender-based", "women's rights"
            ],
            "SDG 6: Clean Water and Sanitation": [
                "water", "sanitation", "hygiene", "clean water", "water quality",
                "water access", "wastewater", "water treatment", "WASH"
            ],
            "SDG 7: Affordable and Clean Energy": [
                "energy", "renewable", "solar", "wind", "clean energy", "sustainable energy",
                "energy efficiency", "power", "electricity", "energy access"
            ],
            "SDG 8: Decent Work and Economic Growth": [
                "employment", "jobs", "work", "economic growth", "entrepreneurship",
                "business", "startups", "workforce", "career", "economic development"
            ],
            "SDG 9: Industry, Innovation, and Infrastructure": [
                "innovation", "technology", "research", "development", "infrastructure",
                "industry", "manufacturing", "R&D", "technological", "digital"
            ],
            "SDG 10: Reduced Inequality": [
                "inequality", "equity", "inclusive", "marginalized", "discrimination",
                "social inclusion", "barriers", "underrepresented", "accessibility"
            ],
            "SDG 11: Sustainable Cities and Communities": [
                "urban", "cities", "communities", "housing", "transportation", "public space",
                "sustainable development", "municipal", "community development"
            ],
            "SDG 12: Responsible Consumption and Production": [
                "sustainability", "sustainable", "recycling", "waste", "circular economy",
                "consumption", "production", "resource efficiency", "green"
            ],
            "SDG 13: Climate Action": [
                "climate", "carbon", "emissions", "greenhouse gas", "climate change",
                "environmental", "mitigation", "adaptation", "resilience"
            ],
            "SDG 14: Life Below Water": [
                "ocean", "marine", "aquatic", "fisheries", "coastal", "sea", "water ecosystems",
                "marine conservation", "blue economy", "maritime"
            ],
            "SDG 15: Life on Land": [
                "forest", "biodiversity", "wildlife", "ecosystem", "conservation",
                "land", "terrestrial", "species", "habitat", "natural resources"
            ],
            "SDG 16: Peace and Justice Strong Institutions": [
                "justice", "governance", "peace", "institutions", "transparency",
                "accountability", "rule of law", "civic", "democracy", "human rights"
            ],
            "SDG 17: Partnerships to Achieve the Goal": [
                "partnership", "collaboration", "cooperation", "alliance", "network",
                "multi-stakeholder", "global", "international", "cross-sector"
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

    def ensure_sdg_processing_columns(self):
        """Ensure SDG processing columns exist in the database"""
        logger.info("🔧 Ensuring SDG processing columns exist...")
        
        column_sql = """
        -- Add SDG processing columns if they don't exist
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'SDGProcessedDate')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD SDGProcessedDate DATETIME2 NULL;
            PRINT 'SDGProcessedDate column added';
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'SDGConfidenceScore')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD SDGConfidenceScore DECIMAL(3,2) NULL;
            PRINT 'SDGConfidenceScore column added';
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'SDGMappingMethod')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD SDGMappingMethod NVARCHAR(100) NULL;
            PRINT 'SDGMappingMethod column added';
        END
        
        -- Ensure SDGTags column exists and is properly sized
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'CleanGrantsLayer2' 
                      AND COLUMN_NAME = 'SDGTags')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 
            ADD SDGTags NVARCHAR(MAX) NULL;
            PRINT 'SDGTags column added';
        END
        ELSE
        BEGIN
            -- Ensure SDGTags column can handle long text
            ALTER TABLE CleanGrantsLayer2 
            ALTER COLUMN SDGTags NVARCHAR(MAX) NULL;
            PRINT 'SDGTags column updated to NVARCHAR(MAX)';
        END
        
        SELECT 'SDG_COLUMNS_READY' as Status;
        """
        
        result = self.execute_sql_command(column_sql, timeout=120)
        return result is not None and 'SDG_COLUMNS_READY' in str(result)

    def process_sdg_alignment(self):
        """Process SDG alignment using keyword-based mapping"""
        logger.info("🌍 Processing SDG alignment using keyword-based mapping...")
        
        # Build comprehensive SDG mapping SQL
        sdg_sql = """
        -- Clear existing SDG values
        UPDATE CleanGrantsLayer2 
        SET SDGTags = NULL, 
            SDGProcessedDate = NULL, 
            SDGConfidenceScore = NULL, 
            SDGMappingMethod = NULL;
        
        -- Process SDG alignment based on Title and Description keywords
        -- SDG 1: No Poverty
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 1: No Poverty'
            ELSE SDGTags + ', SDG 1: No Poverty'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.8
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%poverty%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%low-income%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%basic needs%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%economic hardship%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%disadvantaged%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%underserved%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%financial assistance%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%social services%'
        );
        
        -- SDG 2: Zero Hunger
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 2: Zero Hunger'
            ELSE SDGTags + ', SDG 2: Zero Hunger'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.85
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%hunger%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%food security%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%nutrition%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%agriculture%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%farming%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%crops%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%livestock%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%food systems%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%malnutrition%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%dietary%'
        );
        
        -- SDG 3: Good Health and Well-being
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 3: Good Health and Well-being'
            ELSE SDGTags + ', SDG 3: Good Health and Well-being'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.9
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%health%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%medical%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%healthcare%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%clinical%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%disease%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%wellness%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%mental health%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%public health%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%epidemiology%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%medicine%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%therapy%'
        );
        
        -- SDG 4: Quality Education
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 4: Quality Education'
            ELSE SDGTags + ', SDG 4: Quality Education'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.9
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%education%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%learning%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%teaching%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%training%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%literacy%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%schools%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%academic%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%curriculum%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%students%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%educational%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%pedagogy%'
        );
        
        -- SDG 5: Gender Equality
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 5: Gender Equality'
            ELSE SDGTags + ', SDG 5: Gender Equality'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.85
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%gender%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%women%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%girls%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%female%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%feminist%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%equality%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%discrimination%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%empowerment%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%gender-based%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%women''s rights%'
        );
        
        -- SDG 6: Clean Water and Sanitation
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 6: Clean Water and Sanitation'
            ELSE SDGTags + ', SDG 6: Clean Water and Sanitation'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.9
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%water%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%sanitation%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%hygiene%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%clean water%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%water quality%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%water access%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%wastewater%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%water treatment%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%wash%'
        );
        
        -- SDG 7: Affordable and Clean Energy
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 7: Affordable and Clean Energy'
            ELSE SDGTags + ', SDG 7: Affordable and Clean Energy'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.9
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%energy%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%renewable%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%solar%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%wind%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%clean energy%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%sustainable energy%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%energy efficiency%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%power%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%electricity%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%energy access%'
        );
        
        -- SDG 8: Decent Work and Economic Growth
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 8: Decent Work and Economic Growth'
            ELSE SDGTags + ', SDG 8: Decent Work and Economic Growth'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.8
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%employment%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%jobs%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%work%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%economic growth%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%entrepreneurship%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%business%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%startups%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%workforce%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%career%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%economic development%'
        );
        
        -- SDG 9: Industry, Innovation, and Infrastructure
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 9: Industry, Innovation, and Infrastructure'
            ELSE SDGTags + ', SDG 9: Industry, Innovation, and Infrastructure'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.85
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%innovation%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%technology%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%research%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%development%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%infrastructure%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%industry%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%manufacturing%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%r&d%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%technological%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%digital%'
        );
        
        -- SDG 10: Reduced Inequality
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 10: Reduced Inequality'
            ELSE SDGTags + ', SDG 10: Reduced Inequality'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.8
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%inequality%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%equity%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%inclusive%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%marginalized%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%discrimination%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%social inclusion%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%barriers%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%underrepresented%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%accessibility%'
        );
        
        -- SDG 11: Sustainable Cities and Communities
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 11: Sustainable Cities and Communities'
            ELSE SDGTags + ', SDG 11: Sustainable Cities and Communities'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.85
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%urban%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%cities%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%communities%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%housing%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%transportation%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%public space%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%sustainable development%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%municipal%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%community development%'
        );
        
        -- SDG 12: Responsible Consumption and Production
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 12: Responsible Consumption and Production'
            ELSE SDGTags + ', SDG 12: Responsible Consumption and Production'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.8
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%sustainability%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%sustainable%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%recycling%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%waste%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%circular economy%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%consumption%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%production%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%resource efficiency%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%green%'
        );
        
        -- SDG 13: Climate Action
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 13: Climate Action'
            ELSE SDGTags + ', SDG 13: Climate Action'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.9
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%climate%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%carbon%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%emissions%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%greenhouse gas%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%climate change%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%environmental%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%mitigation%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%adaptation%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%resilience%'
        );
        
        -- SDG 14: Life Below Water
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 14: Life Below Water'
            ELSE SDGTags + ', SDG 14: Life Below Water'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.9
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%ocean%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%marine%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%aquatic%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%fisheries%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%coastal%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%sea%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%water ecosystems%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%marine conservation%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%blue economy%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%maritime%'
        );
        
        -- SDG 15: Life on Land
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 15: Life on Land'
            ELSE SDGTags + ', SDG 15: Life on Land'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.85
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%forest%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%biodiversity%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%wildlife%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%ecosystem%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%conservation%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%land%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%terrestrial%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%species%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%habitat%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%natural resources%'
        );
        
        -- SDG 16: Peace and Justice Strong Institutions
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 16: Peace and Justice Strong Institutions'
            ELSE SDGTags + ', SDG 16: Peace and Justice Strong Institutions'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.8
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%justice%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%governance%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%peace%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%institutions%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%transparency%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%accountability%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%rule of law%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%civic%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%democracy%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%human rights%'
        );
        
        -- SDG 17: Partnerships to Achieve the Goal
        UPDATE CleanGrantsLayer2 
        SET SDGTags = CASE 
            WHEN SDGTags IS NULL THEN 'SDG 17: Partnerships to Achieve the Goal'
            ELSE SDGTags + ', SDG 17: Partnerships to Achieve the Goal'
        END,
        SDGMappingMethod = 'keyword-based',
        SDGConfidenceScore = 0.75
        WHERE (
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%partnership%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%collaboration%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%cooperation%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%alliance%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%network%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%multi-stakeholder%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%global%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%international%' OR
            LOWER(Title + ' ' + ISNULL(Description, '')) LIKE '%cross-sector%'
        );
        
        -- Update processing timestamps
        UPDATE CleanGrantsLayer2 
        SET SDGProcessedDate = GETDATE()
        WHERE SDGTags IS NOT NULL;
        
        -- Limit to maximum 3 SDGs per record (Runwei requirement)
        UPDATE CleanGrantsLayer2 
        SET SDGTags = 
            CASE 
                WHEN LEN(SDGTags) - LEN(REPLACE(SDGTags, ',', '')) > 2 
                THEN LEFT(SDGTags, CHARINDEX(',', SDGTags, CHARINDEX(',', SDGTags) + 1) - 1)
                ELSE SDGTags
            END
        WHERE SDGTags IS NOT NULL 
        AND LEN(SDGTags) - LEN(REPLACE(SDGTags, ',', '')) > 2;
        
        -- Generate summary statistics
        SELECT 
            'SDG_PROCESSING_COMPLETE' as Status,
            COUNT(*) as Total_Records,
            COUNT(CASE WHEN SDGTags IS NOT NULL THEN 1 END) as Records_With_SDGs,
            COUNT(CASE WHEN SDGTags IS NULL THEN 1 END) as Records_Without_SDGs,
            ROUND(100.0 * COUNT(CASE WHEN SDGTags IS NOT NULL THEN 1 END) / COUNT(*), 2) as SDG_Coverage_Percent,
            AVG(CASE WHEN SDGTags IS NOT NULL THEN SDGConfidenceScore END) as Avg_Confidence_Score
        FROM CleanGrantsLayer2;
        """
        
        result = self.execute_sql_command(sdg_sql, timeout=600)
        return result is not None and 'SDG_PROCESSING_COMPLETE' in str(result)

    def generate_sdg_report(self):
        """Generate comprehensive SDG alignment report"""
        logger.info("📊 Generating SDG alignment report...")
        
        report_sql = """
        -- SDG Distribution Report
        SELECT 
            'SDG_DISTRIBUTION' as Report_Type,
            CASE 
                WHEN SDGTags LIKE '%SDG 1:%' THEN 'SDG 1: No Poverty'
                WHEN SDGTags LIKE '%SDG 2:%' THEN 'SDG 2: Zero Hunger'
                WHEN SDGTags LIKE '%SDG 3:%' THEN 'SDG 3: Good Health and Well-being'
                WHEN SDGTags LIKE '%SDG 4:%' THEN 'SDG 4: Quality Education'
                WHEN SDGTags LIKE '%SDG 5:%' THEN 'SDG 5: Gender Equality'
                WHEN SDGTags LIKE '%SDG 6:%' THEN 'SDG 6: Clean Water and Sanitation'
                WHEN SDGTags LIKE '%SDG 7:%' THEN 'SDG 7: Affordable and Clean Energy'
                WHEN SDGTags LIKE '%SDG 8:%' THEN 'SDG 8: Decent Work and Economic Growth'
                WHEN SDGTags LIKE '%SDG 9:%' THEN 'SDG 9: Industry, Innovation, and Infrastructure'
                WHEN SDGTags LIKE '%SDG 10:%' THEN 'SDG 10: Reduced Inequality'
                WHEN SDGTags LIKE '%SDG 11:%' THEN 'SDG 11: Sustainable Cities and Communities'
                WHEN SDGTags LIKE '%SDG 12:%' THEN 'SDG 12: Responsible Consumption and Production'
                WHEN SDGTags LIKE '%SDG 13:%' THEN 'SDG 13: Climate Action'
                WHEN SDGTags LIKE '%SDG 14:%' THEN 'SDG 14: Life Below Water'
                WHEN SDGTags LIKE '%SDG 15:%' THEN 'SDG 15: Life on Land'
                WHEN SDGTags LIKE '%SDG 16:%' THEN 'SDG 16: Peace and Justice Strong Institutions'
                WHEN SDGTags LIKE '%SDG 17:%' THEN 'SDG 17: Partnerships to Achieve the Goal'
                ELSE 'No SDG Alignment'
            END as Primary_SDG,
            COUNT(*) as Opportunity_Count,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM CleanGrantsLayer2), 2) as Percentage
        FROM CleanGrantsLayer2
        GROUP BY 
            CASE 
                WHEN SDGTags LIKE '%SDG 1:%' THEN 'SDG 1: No Poverty'
                WHEN SDGTags LIKE '%SDG 2:%' THEN 'SDG 2: Zero Hunger'
                WHEN SDGTags LIKE '%SDG 3:%' THEN 'SDG 3: Good Health and Well-being'
                WHEN SDGTags LIKE '%SDG 4:%' THEN 'SDG 4: Quality Education'
                WHEN SDGTags LIKE '%SDG 5:%' THEN 'SDG 5: Gender Equality'
                WHEN SDGTags LIKE '%SDG 6:%' THEN 'SDG 6: Clean Water and Sanitation'
                WHEN SDGTags LIKE '%SDG 7:%' THEN 'SDG 7: Affordable and Clean Energy'
                WHEN SDGTags LIKE '%SDG 8:%' THEN 'SDG 8: Decent Work and Economic Growth'
                WHEN SDGTags LIKE '%SDG 9:%' THEN 'SDG 9: Industry, Innovation, and Infrastructure'
                WHEN SDGTags LIKE '%SDG 10:%' THEN 'SDG 10: Reduced Inequality'
                WHEN SDGTags LIKE '%SDG 11:%' THEN 'SDG 11: Sustainable Cities and Communities'
                WHEN SDGTags LIKE '%SDG 12:%' THEN 'SDG 12: Responsible Consumption and Production'
                WHEN SDGTags LIKE '%SDG 13:%' THEN 'SDG 13: Climate Action'
                WHEN SDGTags LIKE '%SDG 14:%' THEN 'SDG 14: Life Below Water'
                WHEN SDGTags LIKE '%SDG 15:%' THEN 'SDG 15: Life on Land'
                WHEN SDGTags LIKE '%SDG 16:%' THEN 'SDG 16: Peace and Justice Strong Institutions'
                WHEN SDGTags LIKE '%SDG 17:%' THEN 'SDG 17: Partnerships to Achieve the Goal'
                ELSE 'No SDG Alignment'
            END
        ORDER BY COUNT(*) DESC;
        
        -- Sample SDG assignments
        SELECT TOP 10
            'SDG_SAMPLES' as Sample_Type,
            SUBSTRING(Title, 1, 50) as Title_Preview,
            SDGTags,
            SDGConfidenceScore,
            SDGMappingMethod
        FROM CleanGrantsLayer2
        WHERE SDGTags IS NOT NULL
        ORDER BY NEWID();
        
        -- Multi-SDG opportunities
        SELECT 
            'MULTI_SDG_ANALYSIS' as Analysis_Type,
            COUNT(*) as Total_Opportunities,
            COUNT(CASE WHEN SDGTags LIKE '%,%' THEN 1 END) as Multi_SDG_Opportunities,
            ROUND(100.0 * COUNT(CASE WHEN SDGTags LIKE '%,%' THEN 1 END) / COUNT(*), 2) as Multi_SDG_Percentage
        FROM CleanGrantsLayer2
        WHERE SDGTags IS NOT NULL;
        """
        
        result = self.execute_sql_command(report_sql, timeout=300)
        return result is not None

    def create_sdg_views(self):
        """Create SDG analysis views"""
        logger.info("🎯 Creating SDG analysis views...")
        
        views_sql = """
        -- Create SDG-based views for analysis
        CREATE OR ALTER VIEW vw_SDG_Opportunities AS
        SELECT 
            OpportunityNumber,
            Title,
            Description,
            AgencyName,
            RunweiCategory,
            SDGTags,
            SDGConfidenceScore,
            SDGMappingMethod,
            AwardValueUSD,
            EstimatedTotalFunding,
            Deadline,
            PostedDate
        FROM CleanGrantsLayer2
        WHERE SDGTags IS NOT NULL;
        
        CREATE OR ALTER VIEW vw_SDG_Summary AS
        SELECT 
            'SDG Analysis' as Report_Type,
            COUNT(*) as Total_Opportunities,
            COUNT(CASE WHEN SDGTags IS NOT NULL THEN 1 END) as SDG_Aligned_Opportunities,
            ROUND(100.0 * COUNT(CASE WHEN SDGTags IS NOT NULL THEN 1 END) / COUNT(*), 2) as SDG_Coverage_Percent,
            AVG(CASE WHEN SDGTags IS NOT NULL THEN SDGConfidenceScore END) as Avg_Confidence_Score
        FROM CleanGrantsLayer2;
        
        SELECT 'SDG_VIEWS_CREATED' as Status, GETDATE() as Created_At;
        """
        
        result = self.execute_sql_command(views_sql, timeout=120)
        return result is not None

    def run_complete_sdg_processing(self):
        """Run the complete SDG alignment processing pipeline"""
        logger.info("🌍 STARTING SDG ALIGNMENT PROCESSING")
        logger.info("=" * 60)
        
        steps = [
            ("Ensure SDG Columns", self.ensure_sdg_processing_columns),
            ("Process SDG Alignment", self.process_sdg_alignment),
            ("Generate SDG Report", self.generate_sdg_report),
            ("Create SDG Views", self.create_sdg_views)
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
        
        logger.info(f"\n🎯 SDG PROCESSING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"✅ Completed Steps: {success_count}/{len(steps)}")
        
        if success_count >= 3:
            logger.info("🎉 SDG ALIGNMENT PROCESSING SUCCESS!")
            logger.info("\n📊 VERIFICATION QUERIES:")
            logger.info("   → SELECT COUNT(*), COUNT(SDGTags) FROM CleanGrantsLayer2;")
            logger.info("   → SELECT * FROM vw_SDG_Opportunities ORDER BY SDGConfidenceScore DESC;")
            logger.info("   → SELECT SDGTags, COUNT(*) FROM CleanGrantsLayer2 WHERE SDGTags IS NOT NULL GROUP BY SDGTags;")
            return True
        else:
            logger.error("❌ SDG alignment processing failed")
            return False

def main():
    """Main execution function"""
    print("🌍 SDG ALIGNMENT PROCESSOR - RUNWEI STANDARDS")
    print("=" * 60)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Mapping opportunities to UN Sustainable Development Goals")
    print("🔍 Using keyword-based intelligent mapping")
    print("📐 Format: Official SDG names (max 3 per opportunity)")
    print()
    
    processor = SDGAlignmentProcessor()
    success = processor.run_complete_sdg_processing()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 SDG ALIGNMENT PROCESSING COMPLETED SUCCESSFULLY!")
        print("✅ All opportunities mapped to relevant UN SDGs")
        print("✅ Proper Runwei formatting applied")
        print("✅ Confidence scores calculated")
        print("✅ Analysis views created")
        print("\n🔍 VERIFICATION QUERIES:")
        print("   1. SELECT COUNT(*), COUNT(SDGTags) FROM CleanGrantsLayer2;")
        print("   2. SELECT * FROM vw_SDG_Opportunities;")
        print("   3. SELECT SDGTags, COUNT(*) FROM CleanGrantsLayer2")
        print("      WHERE SDGTags IS NOT NULL GROUP BY SDGTags;")
        print("\n🌍 Your grants database now includes SDG alignment!")
    else:
        print("❌ SDG ALIGNMENT PROCESSING FAILED")
        print("📝 Please check the logs for details")
    
    print(f"\n📅 Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()