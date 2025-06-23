#!/usr/bin/env python3
"""
Complete Azure Grants Pipeline - Single File Solution
Runs entire pipeline: Grants.gov → Azure Storage → Layer 1 → Layer 2 → Layer 3
"""

import os
import csv
import re
import time
import logging
import tempfile
import shutil
import subprocess
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceNotFoundError, AzureError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'complete_pipeline_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)
logger = logging.getLogger(__name__)

class CompleteGrantsPipeline:
    """Complete Azure grants pipeline in a single class"""
    
    def __init__(self):
        """Initialize with Azure and SQL Database connections"""
        
        # Azure Configuration
        self.azure_connection_string = "DefaultEndpointsProtocol=https;AccountName=grantsgov225756;AccountKey=UXwW5dfy9MY9nh2BGmWhYUbzBve+6LUyT3F7+N3Cp0kWUoEk4AO3z5U6LrBYvo/VwO+Nduq2ay9E+AStKQb86Q=="
        self.storage_account_name = "grantsgov225756"
        
        # SQL Database Configuration  
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"  # 🔧 FIXED: Removed extra 't'
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        
        # Setup Azure environment
        os.environ["AzureWebJobsStorage"] = f"{self.azure_connection_string};EndpointSuffix=core.windows.net"
        os.environ["STORAGE_ACCOUNT_NAME"] = self.storage_account_name
        
        # Initialize Azure clients
        self.table_client = None
        self._init_azure_clients()
        
        # Create temp directory
        self.download_dir = Path(tempfile.mkdtemp(prefix="grants_pipeline_"))
        self.download_dir.mkdir(mode=0o755, exist_ok=True)
        
        # Pipeline statistics
        self.stats = {
            'start_time': datetime.now(),
            'grants_collected': 0,
            'layer1_records': 0,
            'layer2_enhanced': 0,
            'layer3_selected': 0,
            'total_time': 0
        }
        
        logger.info("🚀 Complete Grants Pipeline initialized")

    def _init_azure_clients(self):
        """Initialize Azure Table Storage client"""
        try:
            table_service = TableServiceClient.from_connection_string(
                f"{self.azure_connection_string};EndpointSuffix=core.windows.net"
            )
            self.table_client = table_service.get_table_client("GrantDetails")
            logger.info("✅ Azure Table Storage client initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Azure clients: {e}")
            raise

    def run_complete_pipeline(self) -> bool:
        """Run the complete pipeline from start to finish"""
        logger.info("🚀 STARTING COMPLETE GRANTS PIPELINE")
        logger.info("=" * 60)
        
        try:
            # Step 1: Collect from Grants.gov
            if not self._step1_collect_grants():
                logger.error("❌ Pipeline failed at Step 1: Grants collection")
                return False
            
            # Step 2: Transfer to Layer 1
            if not self._step2_transfer_to_layer1():
                logger.error("❌ Pipeline failed at Step 2: Layer 1 transfer")
                return False
            
            # Step 3: Layer 2 Enhancement
            if not self._step3_layer2_enhancement():
                logger.error("❌ Pipeline failed at Step 3: Layer 2 enhancement")
                return False
            
            # Step 4: Layer 3 Selection
            if not self._step4_layer3_selection():
                logger.error("❌ Pipeline failed at Step 4: Layer 3 selection")
                return False
            
            # Success!
            self._generate_success_report()
            return True
            
        except Exception as e:
            logger.error(f"💥 Fatal pipeline error: {e}")
            return False
        finally:
            # Cleanup
            if self.download_dir.exists():
                shutil.rmtree(self.download_dir, ignore_errors=True)

    def _step1_collect_grants(self) -> bool:
        """Step 1: Collect grants from Grants.gov website"""
        logger.info("\n🌐 STEP 1: COLLECTING GRANTS FROM GRANTS.GOV")
        logger.info("=" * 50)
        
        driver = None
        try:
            # Setup Chrome driver
            driver = self._setup_chrome_driver()
            
            # Navigate and collect data
            logger.info("🔍 Navigating to grants.gov...")
            driver.get("https://www.grants.gov/search-grants")
            
            # Wait for page to load
            WebDriverWait(driver, 30).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            
            # Wait for SPA to initialize
            time.sleep(5)
            
            # Execute search (empty search gets all grants)
            search_button = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[id='btn-search']"))
            )
            driver.execute_script("arguments[0].click();", search_button)
            logger.info("🔍 Search executed")
            
            # Wait for results
            time.sleep(10)
            
            # Find and click export
            csv_file = self._execute_export(driver)
            if not csv_file:
                logger.error("❌ Failed to download CSV file")
                return False
            
            # Process CSV to Azure Storage
            if not self._process_csv_to_azure(csv_file):
                logger.error("❌ Failed to process CSV to Azure Storage")
                return False
            
            logger.info("✅ Step 1 completed: Grants collected and stored in Azure")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error in Step 1: {e}")
            return False
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    def _setup_chrome_driver(self) -> webdriver.Chrome:
        """Setup Chrome driver for automation"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Download settings
        prefs = {
            "download.default_directory": str(self.download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)
        
        return driver

    def _execute_export(self, driver: webdriver.Chrome) -> Optional[Path]:
        """Execute CSV export from grants.gov"""
        initial_files = len(list(self.download_dir.glob("*.csv")))
        
        export_strategies = [
            ("Export by ARIA Label", "css", 'a[aria-label*="Export Detailed Data"]'),
            ("Export by Text", "xpath", "//a[contains(text(), 'Export Detailed Data')]"),
            ("Any Export Link", "xpath", "//a[contains(text(), 'Export')]")
        ]
        
        for name, method, selector in export_strategies:
            try:
                logger.info(f"🎯 Trying: {name}")
                
                if method == "xpath":
                    element = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                else:
                    element = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                
                # Try multiple click methods
                click_methods = [
                    lambda: driver.execute_script("arguments[0].click();", element),
                    lambda: ActionChains(driver).move_to_element(element).click().perform(),
                    lambda: element.click()
                ]
                
                for click_method in click_methods:
                    try:
                        click_method()
                        time.sleep(3)
                        break
                    except:
                        continue
                
                # Monitor for download
                csv_file = self._monitor_download(initial_files, timeout=60)
                if csv_file:
                    logger.info(f"✅ Successfully downloaded using: {name}")
                    return csv_file
                    
            except Exception as e:
                logger.debug(f"Strategy '{name}' failed: {e}")
                continue
        
        return None

    def _monitor_download(self, initial_count: int, timeout: int = 60) -> Optional[Path]:
        """Monitor for file download completion"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            current_files = list(self.download_dir.glob("*.csv"))
            
            if len(current_files) > initial_count:
                newest_file = max(current_files, key=lambda f: f.stat().st_mtime)
                
                # Wait for file to be complete
                initial_size = newest_file.stat().st_size
                time.sleep(3)
                final_size = newest_file.stat().st_size
                
                if initial_size == final_size and final_size > 200:
                    return newest_file
            
            time.sleep(3)
        
        return None

    def _process_csv_to_azure(self, csv_file: Path) -> bool:
        """Process CSV file and store in Azure Table Storage"""
        logger.info(f"📊 Processing CSV file: {csv_file.name}")
        
        try:
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                
                batch_size = 25
                batch = []
                processed = 0
                
                for row_index, row in enumerate(reader, 1):
                    entity = self._create_entity_from_row(row, row_index)
                    if entity:
                        batch.append(entity)
                        processed += 1
                    
                    if len(batch) >= batch_size:
                        self._process_batch(batch)
                        batch = []
                    
                    if row_index % 100 == 0:
                        logger.info(f"📈 Processed {row_index} rows...")
                
                # Process final batch
                if batch:
                    self._process_batch(batch)
                
                self.stats['grants_collected'] = processed
                logger.info(f"✅ Processed {processed} grants to Azure Storage")
                return True
                
        except Exception as e:
            logger.error(f"❌ Error processing CSV: {e}")
            return False

    def _create_entity_from_row(self, row: Dict, row_index: int) -> Optional[Dict]:
        """Create Azure Table entity from CSV row"""
        try:
            # Extract opportunity ID
            row_key = self._extract_opportunity_id(row)
            if not row_key:
                return None
            
            # Create entity
            entity = {
                "PartitionKey": "Grant",
                "RowKey": str(row_key),
                "ProcessedDate": datetime.now().isoformat(),
                "ProcessedBy": "CompleteGrantsPipeline"
            }
            
            # Map all CSV columns
            column_mappings = {
                "OPPORTUNITY NUMBER": "OpportunityNumber",
                "OPPORTUNITY TITLE": "Title",
                "AGENCY CODE": "AgencyCode", 
                "AGENCY NAME": "AgencyName",
                "CATEGORY OF FUNDING ACTIVITY": "Category",
                "FUNDING INSTRUMENT TYPE": "FundingType",
                "ASSISTANCE LISTINGS": "CFDANumbers",
                "ESTIMATED TOTAL FUNDING": "EstimatedTotalFunding",
                "EXPECTED NUMBER OF AWARDS": "ExpectedAwards",
                "AWARD CEILING": "AwardCeiling",
                "AWARD FLOOR": "AwardFloor",
                "LINK TO ADDITIONAL INFORMATION": "AdditionalInfoURL",
                "POSTED DATE": "PostedDate",
                "CLOSE DATE": "CloseDate",
                "FUNDING DESCRIPTION": "Description",
                "ELIGIBLE APPLICANTS": "EligibleApplicants"
            }
            
            for csv_col, azure_prop in column_mappings.items():
                value = row.get(csv_col, "")
                if value:
                    # Clean hyperlinks
                    if str(value).startswith('=HYPERLINK('):
                        matches = re.findall(r'"([^"]*)"', str(value))
                        if matches:
                            value = matches[-1]
                    
                    # Handle numeric fields
                    if azure_prop in ["EstimatedTotalFunding", "AwardCeiling", "AwardFloor", "ExpectedAwards"]:
                        try:
                            cleaned = re.sub(r'[^\d.-]', '', str(value))
                            entity[azure_prop] = float(cleaned) if cleaned else 0.0
                        except:
                            entity[azure_prop] = 0.0
                    else:
                        entity[azure_prop] = str(value)[:1000]  # Truncate for Azure limits
            
            # Build opportunity URL if missing
            if not entity.get("OpportunityURL") and row_key:
                entity["OpportunityURL"] = f"https://www.grants.gov/search-results-detail/{row_key}"
            
            return entity
            
        except Exception as e:
            logger.error(f"Error creating entity from row {row_index}: {e}")
            return None

    def _extract_opportunity_id(self, row: Dict) -> Optional[str]:
        """Extract opportunity ID from row"""
        id_fields = ["OPPORTUNITY NUMBER", "\ufeffOPPORTUNITY NUMBER"]
        
        for field in id_fields:
            value = row.get(field)
            if value:
                if str(value).startswith('=HYPERLINK('):
                    match = re.search(r'/search-results-detail/(\d+)', str(value))
                    if match:
                        return match.group(1)
                    quotes = re.findall(r'"([^"]*)"', str(value))
                    if quotes:
                        return quotes[-1]
                else:
                    return str(value).strip()
        
        return None

    def _process_batch(self, batch: List[Dict]):
        """Process batch to Azure Table Storage"""
        for entity in batch:
            try:
                self.table_client.upsert_entity(entity)
            except Exception as e:
                logger.error(f"Error upserting entity {entity.get('RowKey')}: {e}")

    def _step2_transfer_to_layer1(self) -> bool:
        """Step 2: Transfer data from Azure Storage to SQL Layer 1"""
        logger.info("\n📤 STEP 2: TRANSFERRING TO LAYER 1")
        logger.info("=" * 50)
        
        try:
            # Clear existing Layer 1 data
            logger.info("🗑️ Clearing existing Layer 1 data...")
            self._execute_sql("DELETE FROM RawGrantsLayer1;")
            
            # Get data from Azure Storage
            logger.info("📥 Fetching data from Azure Storage...")
            entities = list(self.table_client.query_entities("PartitionKey eq 'Grant'"))
            logger.info(f"📊 Retrieved {len(entities)} entities from Azure")
            
            if not entities:
                logger.error("❌ No data found in Azure Storage")
                return False
            
            # Process in batches
            batch_size = 25
            total_inserted = 0
            
            for i in range(0, len(entities), batch_size):
                batch = entities[i:i + batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(entities) + batch_size - 1) // batch_size
                
                logger.info(f"🔄 Processing batch {batch_num}/{total_batches}...")
                
                # Create INSERT statements
                insert_statements = []
                for entity in batch:
                    sql = self._create_insert_sql(entity)
                    if sql:
                        insert_statements.append(sql)
                
                # Execute batch
                if insert_statements:
                    batch_sql = "\n".join(insert_statements)
                    temp_file = f"batch_{batch_num}.sql"
                    
                    try:
                        with open(temp_file, 'w', encoding='utf-8') as f:
                            f.write(batch_sql)
                        
                        self._execute_sql_file(temp_file)
                        total_inserted += len(insert_statements)
                        
                        logger.info(f"   ✅ Batch {batch_num} completed - {len(insert_statements)} records")
                        
                    finally:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
            
            self.stats['layer1_records'] = total_inserted
            logger.info(f"✅ Step 2 completed: {total_inserted} records in Layer 1")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error in Step 2: {e}")
            return False

    def _create_insert_sql(self, entity: Dict) -> Optional[str]:
        """Create INSERT SQL statement for entity"""
        try:
            def safe_get(key, default=''):
                value = entity.get(key, default)
                if value is None:
                    return 'NULL'
                if str(value).strip() == '':
                    return 'NULL'
                escaped = str(value).replace("'", "''")
                return f"'{escaped}'"
            
            def safe_get_decimal(key):
                value = entity.get(key)
                if value is None:
                    return 'NULL'
                try:
                    return str(float(value))
                except:
                    return 'NULL'
            
            sql = f"""
INSERT INTO RawGrantsLayer1 (
    PartitionKey, RowKey, OpportunityNumber, OpportunityURL, Title,
    AgencyCode, AgencyName, Category, FundingType, CFDANumbers,
    EstimatedTotalFunding, AwardCeiling, AwardFloor, AdditionalInfoURL,
    PostedDate, CloseDate, Description, EligibleApplicants,
    ProcessedDate, ProcessedBy, SourceType, CreatedDate, UpdatedDate
) VALUES (
    {safe_get('PartitionKey', 'Grant')},
    {safe_get('RowKey')},
    {safe_get('OpportunityNumber')},
    {safe_get('OpportunityURL')},
    {safe_get('Title')},
    {safe_get('AgencyCode')},
    {safe_get('AgencyName')},
    {safe_get('Category')},
    {safe_get('FundingType')},
    {safe_get('CFDANumbers')},
    {safe_get_decimal('EstimatedTotalFunding')},
    {safe_get_decimal('AwardCeiling')},
    {safe_get_decimal('AwardFloor')},
    {safe_get('AdditionalInfoURL')},
    {safe_get('PostedDate')},
    {safe_get('CloseDate')},
    {safe_get('Description')},
    {safe_get('EligibleApplicants')},
    GETDATE(),
    'CompleteGrantsPipeline',
    'Azure_Sync',
    GETDATE(),
    GETDATE()
);
"""
            return sql
            
        except Exception as e:
            logger.error(f"Error creating SQL for entity {entity.get('RowKey')}: {e}")
            return None

    def _step3_layer2_enhancement(self) -> bool:
        """Step 3: Layer 2 data enhancement"""
        logger.info("\n🧹 STEP 3: LAYER 2 ENHANCEMENT")
        logger.info("=" * 50)
        
        try:
            # Transfer Layer 1 to Layer 2
            logger.info("🔄 Transferring Layer 1 to Layer 2...")
            transfer_sql = """
            DELETE FROM CleanGrantsLayer2;
            
            INSERT INTO CleanGrantsLayer2 (
                OpportunityNumber, Title, Description, OpportunityURL, AdditionalInfoURL,
                AgencyName, AgencyCode, AwardValue, AwardCeiling, AwardFloor,
                EstimatedTotalFunding, FundingType, Deadline, PostedDate,
                Category, OpportunityType, Eligibility, EligibilityCategory,
                CountriesEligible, GlobalOpportunity, TimeZone, SDGTags,
                OpportunityGap, KeywordTags, DataQualityScore, ProcessingFlags,
                SourceLayerID, ProcessedDate, ProcessedBy, DataVersion,
                CreatedDate, UpdatedDate, CFDANumbers, Status, Version,
                CostSharingRequired, BusinessRules
            )
            SELECT 
                OpportunityNumber, Title, Description, OpportunityURL, AdditionalInfoURL,
                AgencyName, AgencyCode, 0.0, AwardCeiling, AwardFloor,
                EstimatedTotalFunding, FundingType, CloseDate, PostedDate,
                Category, 'Grant', EligibleApplicants, 'Multiple',
                'United States', 0, 'EST', 'SDG 4: Quality Education',
                'Standard Opportunity', Category, 95.0, 'WORKING_TRANSFORM',
                ID, GETDATE(), 'CompleteGrantsPipeline', '3.0',
                GETDATE(), GETDATE(), CFDANumbers, 'Active', '1.0',
                'False', 'Automated pipeline processing'
            FROM RawGrantsLayer1
            WHERE Title IS NOT NULL AND Title != '';
            """
            
            self._execute_sql(transfer_sql)
            logger.info("✅ Layer 1 to Layer 2 transfer completed")
            
            # Enhanced processing
            logger.info("🧹 Enhancing Layer 2 data...")
            enhancement_sql = """
            -- Generate visual assets
            UPDATE CleanGrantsLayer2
            SET LogoUrl = CASE
                WHEN AgencyName LIKE '%Department%' THEN 'https://www.grants.gov/assets/img/logo.png'
                WHEN Category LIKE '%Health%' THEN 'https://via.placeholder.com/150x150/dc2626/ffffff?text=HEALTH'
                WHEN Category LIKE '%Education%' THEN 'https://via.placeholder.com/150x150/059669/ffffff?text=EDU'
                ELSE 'https://via.placeholder.com/150x150/4a90e2/ffffff?text=GRANT'
            END,
            CoverImage = CASE
                WHEN Category LIKE '%Research%' THEN 'https://via.placeholder.com/800x400/1e3a8a/ffffff?text=Research+Grant'
                WHEN Category LIKE '%Health%' THEN 'https://via.placeholder.com/800x400/dc2626/ffffff?text=Health+Grant'
                WHEN Category LIKE '%Education%' THEN 'https://via.placeholder.com/800x400/059669/ffffff?text=Education+Grant'
                ELSE 'https://via.placeholder.com/800x400/6b7280/ffffff?text=Grant+Opportunity'
            END;
            
            -- Generate summaries
            UPDATE CleanGrantsLayer2
            SET Summary = CASE
                WHEN Description IS NOT NULL AND LEN(Description) > 50 THEN
                    LEFT(Description, 250) + CASE WHEN LEN(Description) > 250 THEN '...' ELSE '' END
                WHEN Title IS NOT NULL THEN
                    Title + ' - Federal grant opportunity providing funding support.'
                ELSE 'Federal grant opportunity providing funding and support for eligible applicants.'
            END;
            
            -- Format award values
            UPDATE CleanGrantsLayer2
            SET AwardValueFormatted = CASE
                WHEN AwardCeiling IS NOT NULL AND AwardCeiling > 0 THEN
                    '$' + FORMAT(AwardCeiling, 'N0') + ' USD'
                WHEN AwardFloor IS NOT NULL AND AwardFloor > 0 THEN
                    'From $' + FORMAT(AwardFloor, 'N0') + ' USD'
                WHEN EstimatedTotalFunding IS NOT NULL AND EstimatedTotalFunding > 0 THEN
                    'Total: $' + FORMAT(EstimatedTotalFunding, 'N0') + ' USD'
                ELSE 'Amount varies'
            END;
            
            -- Calculate quality scores and readiness
            UPDATE CleanGrantsLayer2
            SET DataQualityScore = (
                CASE WHEN Title IS NOT NULL AND LEN(Title) > 10 THEN 2.0 ELSE 0 END +
                CASE WHEN Description IS NOT NULL AND LEN(Description) > 50 THEN 2.0 ELSE 0 END +
                CASE WHEN Summary IS NOT NULL AND LEN(Summary) > 20 THEN 1.0 ELSE 0 END +
                CASE WHEN AgencyName IS NOT NULL AND AgencyName != '' THEN 1.0 ELSE 0 END +
                CASE WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN 2.0 ELSE 0 END +
                CASE WHEN Eligibility IS NOT NULL AND LEN(Eligibility) > 20 THEN 1.0 ELSE 0 END +
                CASE WHEN Category IS NOT NULL AND Category != '' THEN 1.0 ELSE 0 END
            ),
            EnhancementStatus = CASE
                WHEN (CASE WHEN Title IS NOT NULL AND LEN(Title) > 10 THEN 2.0 ELSE 0 END +
                      CASE WHEN Description IS NOT NULL AND LEN(Description) > 50 THEN 2.0 ELSE 0 END +
                      CASE WHEN Summary IS NOT NULL AND LEN(Summary) > 20 THEN 1.0 ELSE 0 END +
                      CASE WHEN AgencyName IS NOT NULL AND AgencyName != '' THEN 1.0 ELSE 0 END +
                      CASE WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN 2.0 ELSE 0 END +
                      CASE WHEN Eligibility IS NOT NULL AND LEN(Eligibility) > 20 THEN 1.0 ELSE 0 END +
                      CASE WHEN Category IS NOT NULL AND Category != '' THEN 1.0 ELSE 0 END) >= 8.0 
                    THEN 'Excellent - Production Ready'
                WHEN (CASE WHEN Title IS NOT NULL AND LEN(Title) > 10 THEN 2.0 ELSE 0 END +
                      CASE WHEN Description IS NOT NULL AND LEN(Description) > 50 THEN 2.0 ELSE 0 END +
                      CASE WHEN Summary IS NOT NULL AND LEN(Summary) > 20 THEN 1.0 ELSE 0 END +
                      CASE WHEN AgencyName IS NOT NULL AND AgencyName != '' THEN 1.0 ELSE 0 END +
                      CASE WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN 2.0 ELSE 0 END +
                      CASE WHEN Eligibility IS NOT NULL AND LEN(Eligibility) > 20 THEN 1.0 ELSE 0 END +
                      CASE WHEN Category IS NOT NULL AND Category != '' THEN 1.0 ELSE 0 END) >= 6.0 
                    THEN 'Good - Enhanced and ready'
                ELSE 'Needs improvement'
            END,
            ReadyForLayer3 = CASE
                WHEN Title IS NOT NULL AND Title != '' AND Summary IS NOT NULL
                    AND AwardValueFormatted IS NOT NULL
                    AND (CASE WHEN Title IS NOT NULL AND LEN(Title) > 10 THEN 2.0 ELSE 0 END +
                         CASE WHEN Description IS NOT NULL AND LEN(Description) > 50 THEN 2.0 ELSE 0 END +
                         CASE WHEN Summary IS NOT NULL AND LEN(Summary) > 20 THEN 1.0 ELSE 0 END +
                         CASE WHEN AgencyName IS NOT NULL AND AgencyName != '' THEN 1.0 ELSE 0 END +
                         CASE WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN 2.0 ELSE 0 END +
                         CASE WHEN Eligibility IS NOT NULL AND LEN(Eligibility) > 20 THEN 1.0 ELSE 0 END +
                         CASE WHEN Category IS NOT NULL AND Category != '' THEN 1.0 ELSE 0 END) >= 6.0
                    THEN 1
                ELSE 0
            END,
            EnhancementDate = GETDATE();
            """
            
            self._execute_sql(enhancement_sql)
            
            # Get count
            count_result = self._execute_sql("SELECT COUNT(*) FROM CleanGrantsLayer2;")
            self.stats['layer2_enhanced'] = self._extract_count_from_result(count_result)
            
            logger.info(f"✅ Step 3 completed: {self.stats['layer2_enhanced']} records enhanced in Layer 2")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error in Step 3: {e}")
            return False

    def _step4_layer3_selection(self) -> bool:
        """Step 4: Layer 3 final selection"""
        logger.info("\n🎯 STEP 4: LAYER 3 FINAL SELECTION")
        logger.info("=" * 50)
        
        try:
            # Create FinalOpportunities table
            logger.info("🏗️ Creating FinalOpportunities table...")
            create_table_sql = """
            IF OBJECT_ID('dbo.FinalOpportunities', 'U') IS NOT NULL
                DROP TABLE dbo.FinalOpportunities;
            
            CREATE TABLE dbo.FinalOpportunities (
                ID NVARCHAR(50) PRIMARY KEY,
                Title NVARCHAR(MAX),
                Url NVARCHAR(MAX),
                Deadline DATETIME2,
                AwardValue NVARCHAR(100),
                CashAward DECIMAL(18,2),
                ContactEmail NVARCHAR(MAX),
                LogoUrl NVARCHAR(MAX),
                CoverImage NVARCHAR(MAX),
                ShortDescription NVARCHAR(1000),
                Description NVARCHAR(MAX),
                Eligibility NVARCHAR(MAX),
                ContactNames NVARCHAR(500),
                OpportunityTypeId INT,
                IndustryId INT,
                TargetCommunityId INT,
                TimeZone NVARCHAR(50),
                DirectApplyLink NVARCHAR(MAX),
                OpportunityGap NVARCHAR(255),
                GlobalOpportunity BIT,
                GlobalLocations NVARCHAR(1000),
                CountriesEligible NVARCHAR(1000),
                LocationDetails NVARCHAR(1000),
                SdgAlignment NVARCHAR(500),
                EsoWebsite NVARCHAR(MAX),
                ServiceProviderEso NVARCHAR(500),
                ApprovalStatus NVARCHAR(100),
                Cost NVARCHAR(255),
                FinancialTerms NVARCHAR(MAX),
                AreaOfFocus NVARCHAR(500),
                Tags NVARCHAR(1000),
                Industry NVARCHAR(500),
                Slug NVARCHAR(255),
                AwardValueStr NVARCHAR(100),
                DeadlineStr NVARCHAR(100),
                DatePosted DATETIME2,
                OpportunityType NVARCHAR(100),
                IsFeatured BIT DEFAULT 0,
                PublishOnLinkedin BIT DEFAULT 0,
                TargetCommunity NVARCHAR(500),
                CreatedAt DATETIME2 DEFAULT GETDATE()
            );
            """
            
            self._execute_sql(create_table_sql)
            logger.info("✅ FinalOpportunities table created")
            
            # Populate with data
            logger.info("🎯 Selecting and populating final data...")
            populate_sql = """
            INSERT INTO dbo.FinalOpportunities (
                ID, Title, Url, Deadline, AwardValue, CashAward, ContactEmail,
                LogoUrl, CoverImage, ShortDescription, Description, Eligibility,
                ContactNames, OpportunityTypeId, IndustryId, TargetCommunityId,
                TimeZone, DirectApplyLink, OpportunityGap, GlobalOpportunity,
                GlobalLocations, CountriesEligible, LocationDetails, SdgAlignment,
                EsoWebsite, ServiceProviderEso, ApprovalStatus, Cost, FinancialTerms,
                AreaOfFocus, Tags, Industry, Slug, AwardValueStr, DeadlineStr,
                DatePosted, OpportunityType, IsFeatured, PublishOnLinkedin, TargetCommunity
            )
            SELECT 
                CAST(ID AS NVARCHAR(50)),
                Title,
                ISNULL(OpportunityURL, 'https://www.grants.gov/search-results-detail/' + OpportunityNumber),
                Deadline,
                AwardValueFormatted,
                ISNULL(AwardCeiling, ISNULL(AwardFloor, ISNULL(EstimatedTotalFunding, 0))),
                ISNULL(AgencyName + '@grants.gov', 'contact@grants.gov'),
                LogoUrl,
                CoverImage,
                Summary,
                Description,
                Eligibility,
                AgencyName,
                CASE WHEN OpportunityType LIKE '%Grant%' THEN 1 ELSE 2 END,
                CASE 
                    WHEN Category LIKE '%Education%' THEN 1
                    WHEN Category LIKE '%Health%' THEN 2
                    WHEN Category LIKE '%Science%' THEN 3
                    WHEN Category LIKE '%Technology%' THEN 4
                    WHEN Category LIKE '%Environment%' THEN 5
                    ELSE 6
                END,
                CASE 
                    WHEN EligibilityCategory LIKE '%Individual%' THEN 1
                    WHEN EligibilityCategory LIKE '%Organization%' THEN 2
                    ELSE 3
                END,
                ISNULL(TimeZone, 'EST'),
                OpportunityURL,
                OpportunityGap,
                ISNULL(GlobalOpportunity, 0),
                'United States',
                ISNULL(CountriesEligible, 'United States'),
                'Federal Grant Program',
                SDGTags,
                'https://www.grants.gov',
                AgencyName,
                'Approved',
                'No Cost Sharing',
                'Federal funding terms apply',
                Category,
                KeywordTags,
                Category,
                LOWER(REPLACE(REPLACE(Title, ' ', '-'), '''', '')),
                AwardValueFormatted,
                CASE WHEN Deadline IS NOT NULL THEN FORMAT(Deadline, 'MMM dd, yyyy') ELSE 'No deadline' END,
                ISNULL(PostedDate, CreatedDate),
                OpportunityType,
                CASE WHEN DataQualityScore >= 9.0 THEN 1 ELSE 0 END,
                0,
                EligibilityCategory
            FROM CleanGrantsLayer2
            WHERE ReadyForLayer3 = 1
              AND DataQualityScore >= 6.0
              AND Title IS NOT NULL AND Title != ''
            ORDER BY DataQualityScore DESC;
            """
            
            self._execute_sql(populate_sql)
            
            # Get final count
            count_result = self._execute_sql("SELECT COUNT(*) FROM FinalOpportunities;")
            self.stats['layer3_selected'] = self._extract_count_from_result(count_result)
            
            logger.info(f"✅ Step 4 completed: {self.stats['layer3_selected']} records in FinalOpportunities")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error in Step 4: {e}")
            return False

    def _execute_sql(self, sql: str) -> str:
        """Execute SQL command"""
        try:
            cmd = [
                "sqlcmd", "-S", self.server, "-d", self.database,
                "-U", self.username, "-P", self.password,
                "-Q", sql, "-C"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                return result.stdout
            else:
                logger.error(f"SQL execution failed: {result.stderr}")
                return ""
                
        except Exception as e:
            logger.error(f"Error executing SQL: {e}")
            return ""

    def _execute_sql_file(self, file_path: str):
        """Execute SQL file"""
        cmd = [
            "sqlcmd", "-S", self.server, "-d", self.database,
            "-U", self.username, "-P", self.password,
            "-i", file_path, "-C"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            raise Exception(f"SQL file execution failed: {result.stderr}")

    def _extract_count_from_result(self, result: str) -> int:
        """Extract count from SQL result"""
        try:
            lines = result.strip().split('\n')
            for line in lines:
                if line.strip().isdigit():
                    return int(line.strip())
            return 0
        except:
            return 0

    def _generate_success_report(self):
        """Generate success report"""
        end_time = datetime.now()
        self.stats['total_time'] = (end_time - self.stats['start_time']).total_seconds()
        
        report = f"""
🎊 COMPLETE GRANTS PIPELINE SUCCESS!
={'='*50}
⏱️  Total Time: {self.stats['total_time']:.2f} seconds
📊 Grants Collected: {self.stats['grants_collected']}
📤 Layer 1 Records: {self.stats['layer1_records']}
🧹 Layer 2 Enhanced: {self.stats['layer2_enhanced']}
🎯 Layer 3 Selected: {self.stats['layer3_selected']}

✅ Your fresh grant opportunities are ready in FinalOpportunities table!
🚀 Pipeline completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        logger.info(report)
        print(report)

def main():
    """Main execution function"""
    print("🚀 COMPLETE GRANTS PIPELINE")
    print("=" * 30)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        pipeline = CompleteGrantsPipeline()
        success = pipeline.run_complete_pipeline()
        
        if success:
            print("\n✅ PIPELINE COMPLETED SUCCESSFULLY!")
            return 0
        else:
            print("\n❌ PIPELINE FAILED!")
            return 1
            
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        logger.exception("Full error details:")
        return 1

if __name__ == "__main__":
    exit(main())