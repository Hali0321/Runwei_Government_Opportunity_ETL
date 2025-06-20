import os
import csv
import re
import time
import logging
import tempfile
import shutil
import subprocess
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceNotFoundError, AzureError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

class GrantsAutomationError(Exception):
    """Custom exception for grants automation errors"""
    pass

class AutomatedGrantsFetcher:
    """Azure-optimized automated fetcher for Grants.gov SPA with enhanced JavaScript handling"""
    
    def __init__(self, connection_string: Optional[str] = None, 
                 storage_account_name: Optional[str] = None,
                 download_dir: Optional[str] = None):
        """Initialize with Azure security and performance best practices"""
        
        # Azure Application Insights compatible logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Azure configuration following Well-Architected Framework
        self.storage_account_name = storage_account_name or os.environ.get("STORAGE_ACCOUNT_NAME", "grantsgov225756")
        self.connection_string = self._get_azure_connection_string(connection_string)
        
        # Secure temporary directory with proper cleanup
        self.download_dir = Path(download_dir) if download_dir else Path(tempfile.mkdtemp(prefix="grants_azure_"))
        self.download_dir.mkdir(mode=0o755, exist_ok=True)
        
        # Azure clients with connection pooling
        self.table_client = None
        self.blob_client = None
        self._init_azure_clients()
        
        # Enhanced telemetry for Azure monitoring
        self.stats = {
            'downloaded_files': 0,
            'processed_rows': 0,
            'updated_entities': 0,
            'new_entities': 0,
            'errors': 0,
            'skipped': 0,
            'start_time': None,
            'end_time': None,
            'azure_operations': 0,
            'selenium_operations': 0,
            'javascript_waits': 0,
            'spa_interactions': 0
        }

    def _get_azure_connection_string(self, provided_connection_string: Optional[str]) -> str:
        """Azure-native connection string management with security best practices"""
        
        if provided_connection_string and self._validate_connection_string(provided_connection_string):
            self.logger.info("✓ Using provided Azure Storage connection string")
            return provided_connection_string
        
        # Azure Key Vault integration (recommended for production)
        try:
            from azure.keyvault.secrets import SecretClient
            from azure.identity import DefaultAzureCredential
            
            key_vault_url = os.environ.get("AZURE_KEY_VAULT_URL")
            if key_vault_url:
                credential = DefaultAzureCredential()
                secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
                
                secret = secret_client.get_secret("storage-connection-string")
                if secret.value and self._validate_connection_string(secret.value):
                    self.logger.info("✓ Retrieved connection string from Azure Key Vault")
                    return secret.value
        except ImportError:
            self.logger.debug("Azure Key Vault SDK not available")
        except Exception as e:
            self.logger.warning(f"Key Vault access failed: {e}")
        
        # Azure Functions environment variables
        connection_sources = [
            ("AzureWebJobsStorage", os.environ.get("AzureWebJobsStorage")),
            ("AZURE_STORAGE_CONNECTION_STRING", os.environ.get("AZURE_STORAGE_CONNECTION_STRING")),
        ]
        
        for source_name, connection_string in connection_sources:
            if connection_string and self._validate_connection_string(connection_string):
                self.logger.info(f"✓ Using {source_name} from Azure environment")
                return connection_string
        
        # Component-based construction for flexibility
        account_name = os.environ.get("AZURE_STORAGE_ACCOUNT", self.storage_account_name)
        account_key = os.environ.get("AZURE_STORAGE_KEY")
        
        if account_name and account_key:
            built_conn_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
            if self._validate_connection_string(built_conn_string):
                self.logger.info("✓ Built connection string from Azure components")
                return built_conn_string
        
        # Development fallback (replace with Azure Key Vault in production)
        fallback_conn_string = "DefaultEndpointsProtocol=https;AccountName=grantsgov225756;AccountKey=UXwW5dfy9MY9nh2BGmWhYUbzBve+6LUyT3F7+N3Cp0kWUoEk4AO3z5U6LrBYvo/VwO+Nduq2ay9E+AStKQb86Q==;EndpointSuffix=core.windows.net"
        
        if self._validate_connection_string(fallback_conn_string):
            self.logger.warning("⚠️ Using development connection string - configure Azure Key Vault for production")
            return fallback_conn_string
        
        raise GrantsAutomationError("Unable to obtain valid Azure Storage connection string")

    def _validate_connection_string(self, connection_string: str) -> bool:
        """Validate Azure Storage connection string format"""
        if not connection_string or not isinstance(connection_string, str):
            return False
        
        required_components = ['DefaultEndpointsProtocol', 'AccountName', 'AccountKey', 'EndpointSuffix']
        return all(component in connection_string for component in required_components)

    def _init_azure_clients(self) -> None:
        """Initialize Azure clients with retry logic and connection pooling"""
        max_retries = 3
        base_delay = 2
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"🔄 Initializing Azure clients (attempt {attempt + 1}/{max_retries})")
                
                # Table Storage with enhanced configuration
                table_service = TableServiceClient.from_connection_string(
                    self.connection_string,
                    logging_enable=True,
                    api_version="2019-02-02"
                )
                self.table_client = table_service.get_table_client("GrantDetails")
                
                # Ensure table exists
                self._ensure_azure_table_exists(table_service)
                
                # Blob Storage with optimized settings
                self.blob_client = BlobServiceClient.from_connection_string(
                    self.connection_string,
                    logging_enable=True,
                    api_version="2023-01-03"
                )
                
                # Create containers if needed
                self._ensure_azure_blob_containers()
                
                self.logger.info("✅ Azure clients initialized successfully")
                return
                
            except Exception as e:
                error_msg = str(e)
                self.logger.warning(f"❌ Azure client initialization attempt {attempt + 1} failed: {error_msg}")
                
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    self.logger.info(f"⏳ Retrying Azure connection in {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise GrantsAutomationError(f"Azure client initialization failed after {max_retries} attempts: {error_msg}")

    def _ensure_azure_table_exists(self, table_service: TableServiceClient) -> None:
        """Ensure Azure Table Storage table exists with proper error handling"""
        try:
            list(self.table_client.query_entities("PartitionKey eq 'Grant'", results_per_page=1))
            self.logger.info("✓ Azure Table Storage connection verified")
        except Exception:
            try:
                table_service.create_table("GrantDetails")
                self.logger.info("✓ Created Azure Table: GrantDetails")
            except Exception as create_error:
                if "already exists" not in str(create_error).lower():
                    raise create_error
                self.logger.info("✓ Azure Table GrantDetails already exists")

    def _ensure_azure_blob_containers(self) -> None:
        """Ensure Azure Blob Storage containers exist"""
        try:
            containers = list(self.blob_client.list_containers())
            self.logger.info(f"✓ Azure Blob Storage verified ({len(containers)} containers)")
            
            container_name = "grants-backups"
            try:
                container_client = self.blob_client.get_container_client(container_name)
                container_client.create_container()
                self.logger.info(f"✓ Created Azure Blob container: {container_name}")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    self.logger.warning(f"Could not create container {container_name}: {e}")
                else:
                    self.logger.info(f"✓ Azure Blob container {container_name} already exists")
                    
        except Exception as e:
            self.logger.warning(f"⚠️ Azure Blob Storage connection issue: {e}")

    def download_grants_data_with_spa_automation(self, search_params: Optional[Dict] = None) -> List[Path]:
        """Enhanced automation specifically designed for Grants.gov SPA with Azure monitoring"""
        self.logger.info("🚀 Starting Azure-optimized SPA automation for Grants.gov...")
        
        driver = None
        downloaded_files = []
        
        try:
            # Setup Azure Functions optimized Chrome driver
            driver = self._setup_azure_chrome_driver()
            
            # Navigate to SPA with enhanced waiting
            self._navigate_to_grants_spa(driver)
            
            # Wait for SPA to fully initialize
            self._wait_for_spa_initialization(driver)
            
            # Execute search in SPA context
            self._execute_spa_search(driver, search_params)
            
            # Wait for search results with SPA-specific logic
            self._wait_for_spa_search_results(driver)
            
            # Find and execute export in SPA
            csv_file = self._execute_spa_export_detailed_data(driver)
            if csv_file:
                downloaded_files.append(csv_file)
                self.stats['downloaded_files'] += 1
                self.stats['selenium_operations'] += 1
                
        except Exception as e:
            self.logger.error(f"Azure SPA automation failed: {e}")
            
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
        
        return downloaded_files

    def _setup_azure_chrome_driver(self) -> webdriver.Chrome:
        """Setup Chrome driver optimized for Azure Functions and SPA interactions"""
        chrome_options = Options()
        
        # Azure Functions essential optimizations
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--memory-pressure-off")
        chrome_options.add_argument("--max_old_space_size=4096")
        
        # SPA-specific optimizations
        chrome_options.add_argument("--enable-javascript")
        chrome_options.add_argument("--enable-experimental-web-platform-features")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--start-maximized")
        
        # Government site compatibility
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Enhanced download configuration for Azure
        prefs = {
            "download.default_directory": str(self.download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_settings.popups": 0,
            "profile.default_content_setting_values.notifications": 2,
            "profile.managed_default_content_settings": {
                "images": 2
            }
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # User agent for Grants.gov compatibility
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(90)  # Increased for SPA
        driver.implicitly_wait(15)  # Increased for dynamic content
        
        # Remove automation detection for government sites
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
        driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
        
        return driver

    def _navigate_to_grants_spa(self, driver: webdriver.Chrome) -> None:
        """Navigate to Grants.gov SPA with enhanced error handling"""
        target_url = "https://www.grants.gov/search-grants"
        
        self.logger.info(f"🌐 Navigating to Grants.gov SPA: {target_url}")
        
        try:
            driver.get(target_url)
            
            # Wait for initial page load
            WebDriverWait(driver, 45).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            
            # Verify we're on the correct page
            if "grants.gov" not in driver.current_url.lower():
                raise Exception(f"Failed to navigate to correct page: {driver.current_url}")
            
            self.logger.info("✓ Successfully navigated to Grants.gov SPA")
            
        except Exception as e:
            raise GrantsAutomationError(f"Navigation to Grants.gov SPA failed: {e}")

    def _wait_for_spa_initialization(self, driver: webdriver.Chrome) -> None:
        """Wait for SPA (Nuxt.js/Vue.js) to fully initialize with Azure monitoring"""
        self.logger.info("⏳ Waiting for SPA initialization...")
        
        try:
            # Wait for Vue.js/Nuxt.js to be available
            WebDriverWait(driver, 30).until(
                lambda d: d.execute_script("return typeof window.$nuxt !== 'undefined' || typeof window.Vue !== 'undefined'")
            )
            
            # Wait for main search components to be ready
            search_indicators = [
                "input[name='inp-keywords']",
                "input[name='inp-opportunity-number']", 
                "button[id='btn-search']",
                ".gg-search-filter-section"
            ]
            
            for indicator in search_indicators:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, indicator))
                )
            
            # Additional wait for any lazy-loaded components
            time.sleep(3)
            self.stats['spa_interactions'] += 1
            
            self.logger.info("✅ SPA initialization complete")
            
        except Exception as e:
            self.logger.warning(f"SPA initialization warning: {e}")
            # Continue anyway as some components might still work

    def _execute_spa_search(self, driver: webdriver.Chrome, search_params: Optional[Dict]) -> None:
        """Execute search in SPA context with enhanced parameter handling"""
        try:
            # Apply search parameters if provided
            if search_params:
                self._apply_spa_search_parameters(driver, search_params)
            
            # Find and click search button in SPA
            search_button = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[id='btn-search']"))
            )
            
            self.logger.info("🔍 Executing search in SPA...")
            
            # Click search button with JavaScript for SPA compatibility
            driver.execute_script("arguments[0].click();", search_button)
            
            # Brief wait for search to initiate
            time.sleep(2)
            self.stats['spa_interactions'] += 1
            
            self.logger.info("✓ Search executed in SPA")
            
        except Exception as e:
            self.logger.warning(f"SPA search execution issue: {e}")
            # Try fallback: just press Enter in keyword field
            try:
                keyword_input = driver.find_element(By.CSS_SELECTOR, "input[name='inp-keywords']")
                from selenium.webdriver.common.keys import Keys
                keyword_input.send_keys(Keys.RETURN)
                self.logger.info("✓ Fallback search executed")
            except:
                pass

    def _apply_spa_search_parameters(self, driver: webdriver.Chrome, search_params: Dict) -> None:
        """Apply search parameters in SPA context"""
        try:
            # Keyword search
            if search_params.get('keyword'):
                keyword_input = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='inp-keywords']"))
                )
                keyword_input.clear()
                keyword_input.send_keys(search_params['keyword'])
                self.logger.info(f"✓ Set SPA keyword: {search_params['keyword']}")
            
            # Opportunity number
            if search_params.get('opportunity_number'):
                opp_input = driver.find_element(By.CSS_SELECTOR, "input[name='inp-opportunity-number']")
                opp_input.clear()
                opp_input.send_keys(search_params['opportunity_number'])
                self.logger.info(f"✓ Set SPA opportunity number: {search_params['opportunity_number']}")
            
            # Assistance listings (CFDA)
            if search_params.get('assistance_listings'):
                cfda_input = driver.find_element(By.CSS_SELECTOR, "input[name='inp-CFDA']")
                cfda_input.clear()
                cfda_input.send_keys(search_params['assistance_listings'])
                self.logger.info(f"✓ Set SPA assistance listings: {search_params['assistance_listings']}")
                
        except Exception as e:
            self.logger.warning(f"Could not apply all SPA search parameters: {e}")

    def _wait_for_spa_search_results(self, driver: webdriver.Chrome) -> None:
        """Wait for search results in SPA with enhanced detection"""
        self.logger.info("⏳ Waiting for SPA search results...")
        
        try:
            # Wait for loading to disappear
            WebDriverWait(driver, 30).until_not(
                EC.text_to_be_present_in_element((By.TAG_NAME, "h1"), "Loading...")
            )
            
            # Wait for results table or export link to appear
            result_indicators = [
                ".usa-table-container--scrollable",
                "table",
                ".search-results",
                "a[aria-label*='Export Detailed Data']"
            ]
            
            for indicator in result_indicators:
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, indicator))
                    )
                    self.logger.info(f"✓ Found SPA result indicator: {indicator}")
                    break
                except:
                    continue
            
            # Additional wait for results to stabilize
            time.sleep(5)
            self.stats['spa_interactions'] += 1
            
            self.logger.info("✅ SPA search results loaded")
            
        except Exception as e:
            self.logger.warning(f"SPA search results loading issue: {e}")

    def _execute_spa_export_detailed_data(self, driver: webdriver.Chrome) -> Optional[Path]:
        """Execute Export Detailed Data in SPA context with comprehensive strategies"""
        
        files_before = len(list(self.download_dir.glob("*.csv")))
        self.logger.info(f"📊 Starting SPA export execution (current CSV files: {files_before})")
        
        # SPA-specific export strategies based on the HTML analysis
        spa_export_strategies = [
            {
                'name': 'SPA Export Detailed Data by ARIA Label',
                'method': 'css',
                'selector': 'a[aria-label*="Export Detailed Data"]',
                'wait_time': 10
            },
            {
                'name': 'SPA Export Detailed Data by Text Content',
                'method': 'xpath',
                'selector': "//a[contains(text(), 'Export Detailed Data')]",
                'wait_time': 10
            },
            {
                'name': 'SPA Export Link by Role Button',
                'method': 'css',
                'selector': 'a[role="button"]:has-text("Export")',
                'wait_time': 8
            },
            {
                'name': 'SPA Export by USA Link Class',
                'method': 'css',
                'selector': 'a.usa-link[aria-label*="Export"]',
                'wait_time': 8
            },
            {
                'name': 'SPA Any Export Link',
                'method': 'xpath',
                'selector': "//a[contains(text(), 'Export')]",
                'wait_time': 8
            }
        ]
        
        for strategy in spa_export_strategies:
            try:
                self.logger.info(f"🎯 Trying SPA strategy: {strategy['name']}")
                
                # Find the export element with SPA-aware waiting
                if strategy['method'] == 'xpath':
                    export_element = WebDriverWait(driver, strategy['wait_time']).until(
                        EC.element_to_be_clickable((By.XPATH, strategy['selector']))
                    )
                else:
                    export_element = WebDriverWait(driver, strategy['wait_time']).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, strategy['selector']))
                    )
                
                # Log element details for debugging
                element_text = export_element.text
                element_aria_label = export_element.get_attribute('aria-label')
                element_href = export_element.get_attribute('href')
                self.logger.info(f"✓ Found SPA element - Text: '{element_text}', ARIA: '{element_aria_label}', Href: '{element_href}'")
                
                # Execute click with SPA-specific methods
                click_success = self._execute_spa_click(driver, export_element, strategy['name'])
                
                if click_success:
                    # Monitor download with SPA-aware validation
                    downloaded_file = self._monitor_spa_download(files_before, timeout=120)
                    if downloaded_file:
                        self.logger.info(f"🎉 Successfully downloaded using SPA strategy: {strategy['name']}")
                        self.stats['spa_interactions'] += 1
                        return downloaded_file
                
            except Exception as e:
                self.logger.debug(f"SPA strategy '{strategy['name']}' failed: {e}")
                continue
        
        # SPA-specific fallback strategies
        return self._spa_fallback_export_attempt(driver, files_before)

    def _execute_spa_click(self, driver: webdriver.Chrome, element, strategy_name: str) -> bool:
        """Execute click in SPA context with enhanced methods"""
        spa_click_methods = [
            {
                'name': 'SPA JavaScript Click',
                'action': lambda: driver.execute_script("arguments[0].click();", element)
            },
            {
                'name': 'SPA Mouse Event Dispatch',
                'action': lambda: driver.execute_script("arguments[0].dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true, view: window}));", element)
            },
            {
                'name': 'SPA ActionChains Click',
                'action': lambda: ActionChains(driver).move_to_element(element).click().perform()
            },
            {
                'name': 'SPA Standard Click',
                'action': lambda: element.click()
            },
            {
                'name': 'SPA Vue Event Trigger',
                'action': lambda: driver.execute_script("if(arguments[0]._vnode && arguments[0]._vnode.componentInstance) { arguments[0]._vnode.componentInstance.$emit('click'); } else { arguments[0].click(); }", element)
            }
        ]
        
        for method in spa_click_methods:
            try:
                self.logger.info(f"🖱️ Attempting {method['name']} for {strategy_name}")
                
                # Scroll element into view for SPA
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
                time.sleep(1)
                
                # Execute SPA click method
                method['action']()
                
                # Wait for SPA to process the click
                time.sleep(3)
                self.stats['javascript_waits'] += 1
                
                self.logger.info(f"✓ {method['name']} executed successfully")
                return True
                
            except Exception as e:
                self.logger.debug(f"{method['name']} failed: {e}")
                continue
        
        return False

    def _monitor_spa_download(self, initial_file_count: int, timeout: int = 120) -> Optional[Path]:
        """Monitor download in SPA context with enhanced validation"""
        start_time = time.time()
        check_interval = 3  # Slightly longer for SPA processing
        
        self.logger.info(f"⏳ Monitoring SPA download (timeout: {timeout}s)")
        
        while time.time() - start_time < timeout:
            current_files = list(self.download_dir.glob("*.csv"))
            current_count = len(current_files)
            
            if current_count > initial_file_count:
                # New file detected
                newest_files = sorted(current_files, key=lambda f: f.stat().st_mtime, reverse=True)
                newest_file = newest_files[0]
                
                self.logger.info(f"📁 New file detected from SPA: {newest_file.name}")
                
                # Enhanced download completion check for SPA
                if self._verify_spa_download_completion(newest_file):
                    # Comprehensive CSV validation
                    if self._validate_grants_csv_from_spa(newest_file):
                        self.logger.info(f"✅ SPA download completed and validated: {newest_file.name}")
                        return newest_file
                    else:
                        self.logger.warning(f"SPA downloaded file failed validation: {newest_file.name}")
                        try:
                            newest_file.unlink()
                        except:
                            pass
                        
            # Enhanced progress logging for SPA
            elapsed = time.time() - start_time
            if int(elapsed) % 15 == 0 and elapsed > 15:  # Log every 15 seconds
                self.logger.info(f"📊 SPA download monitoring: {elapsed:.0f}s elapsed, {current_count} CSV files")
                
            time.sleep(check_interval)
        
        self.logger.warning("⏰ SPA download monitoring timeout reached")
        return None

    def _verify_spa_download_completion(self, file_path: Path, stability_checks: int = 4) -> bool:
        """Verify download completion in SPA context with multiple stability checks"""
        try:
            for check in range(stability_checks):
                initial_size = file_path.stat().st_size
                time.sleep(3)  # Longer wait for SPA processing
                final_size = file_path.stat().st_size
                
                # Check if file size is stable and reasonable
                if initial_size != final_size:
                    self.logger.debug(f"SPA file size changing: {initial_size} -> {final_size}")
                    continue
                    
                if final_size < 200:  # Minimum reasonable file size for grants data
                    self.logger.debug(f"SPA file too small: {final_size} bytes")
                    continue
                    
                # File appears stable
                return True
                
            return False
            
        except Exception as e:
            self.logger.error(f"Error verifying SPA download completion: {e}")
            return False

    def _validate_grants_csv_from_spa(self, file_path: Path) -> bool:
        """Enhanced validation for CSV files downloaded from SPA"""
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                # Read first few lines for validation
                first_line = f.readline().strip()
                
                if not first_line:
                    return False
                
                # Check for CSV structure
                if ',' not in first_line:
                    return False
                
                # Enhanced Grants.gov specific headers for SPA output
                grants_headers = [
                    'OPPORTUNITY', 'TITLE', 'AGENCY', 'POSTED', 'CLOSE', 'CFDA',
                    'FUNDING', 'AWARD', 'DESCRIPTION', 'LISTING', 'NUMBER', 'STATUS'
                ]
                
                header_upper = first_line.upper()
                matching_headers = sum(1 for header in grants_headers if header in header_upper)
                
                if matching_headers >= 3:  # At least 3 expected headers
                    self.logger.info(f"✓ Valid Grants.gov CSV from SPA detected ({matching_headers} matching headers)")
                    return True
                
                # Generic CSV validation fallback
                if first_line.count(',') >= 5:  # At least 6 columns
                    self.logger.info("✓ Valid CSV format from SPA detected (generic validation)")
                    return True
                    
            return False
            
        except Exception as e:
            self.logger.error(f"Error validating CSV from SPA: {e}")
            return False

    def _spa_fallback_export_attempt(self, driver: webdriver.Chrome, initial_file_count: int) -> Optional[Path]:
        """SPA-specific fallback export strategies"""
        self.logger.info("🔄 Executing SPA fallback export strategies...")
        
        spa_fallback_strategies = [
            # Try interacting with Vue.js components directly
            lambda: self._try_vue_component_interaction(driver),
            # Try triggering export through JavaScript events
            lambda: self._try_spa_javascript_export(driver),
            # Try finding hidden or dynamically created elements
            lambda: self._try_spa_dynamic_elements(driver)
        ]
        
        for strategy in spa_fallback_strategies:
            try:
                strategy()
                downloaded_file = self._monitor_spa_download(initial_file_count, timeout=45)
                if downloaded_file:
                    return downloaded_file
            except Exception as e:
                self.logger.debug(f"SPA fallback strategy failed: {e}")
                continue
        
        return None

    def _try_vue_component_interaction(self, driver: webdriver.Chrome) -> None:
        """Try interacting with Vue.js components directly"""
        try:
            # Try to trigger export through Vue component
            js_code = """
            // Try to find Vue component with export functionality
            if (window.$nuxt) {
                let app = window.$nuxt;
                if (app.$store && app.$store.dispatch) {
                    app.$store.dispatch('exportData');
                }
            }
            
            // Try to trigger export event
            document.dispatchEvent(new CustomEvent('exportDetailedData'));
            
            // Try common export function names
            ['exportCSV', 'exportData', 'downloadCSV', 'exportDetailedData'].forEach(func => {
                if (window[func] && typeof window[func] === 'function') {
                    window[func]();
                }
            });
            """
            driver.execute_script(js_code)
            time.sleep(3)
            self.stats['javascript_waits'] += 1
        except Exception as e:
            self.logger.debug(f"Vue component interaction failed: {e}")

    def _try_spa_javascript_export(self, driver: webdriver.Chrome) -> None:
        """Try triggering export through SPA JavaScript patterns"""
        js_patterns = [
            "if(window.exportDetailedData) exportDetailedData();",
            "if(window.downloadCSV) downloadCSV();",
            "document.querySelector('a[aria-label*=\"Export\"]')?.click();",
            "Array.from(document.querySelectorAll('a')).find(a => a.textContent.includes('Export'))?.click();"
        ]
        
        for pattern in js_patterns:
            try:
                driver.execute_script(pattern)
                time.sleep(2)
            except:
                continue

    def _try_spa_dynamic_elements(self, driver: webdriver.Chrome) -> None:
        """Try finding dynamically created elements in SPA"""
        try:
            # Wait for any dynamically created export elements
            dynamic_selectors = [
                "a[data-export]",
                "button[data-export]",
                ".export-button",
                ".download-csv"
            ]
            
            for selector in dynamic_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    try:
                        driver.execute_script("arguments[0].click();", element)
                        time.sleep(2)
                        break
                    except:
                        continue
        except:
            pass

    def process_csv_file(self, csv_file_path: Path) -> bool:
        """Process CSV with Azure optimizations and enhanced error handling"""
        self.logger.info(f"🔄 Processing CSV file with Azure integration: {csv_file_path}")
        
        try:
            # Backup to Azure Blob Storage first
            self._backup_csv_to_azure_blob(csv_file_path)
            
            # Process the CSV data with Azure batch operations
            success = self._process_csv_data_with_azure_batching(csv_file_path)
            
            if success:
                self.logger.info(f"✅ Successfully processed {csv_file_path.name} with Azure")
                self.stats['azure_operations'] += 1
            else:
                self.logger.error(f"❌ Failed to process {csv_file_path.name}")
                
            return success
            
        except Exception as e:
            self.logger.error(f"Error processing CSV: {e}")
            return False
        finally:
            # Always clean up the CSV file securely
            self._secure_azure_file_cleanup(csv_file_path)

    def _backup_csv_to_azure_blob(self, csv_file_path: Path) -> None:
        """Backup CSV to Azure Blob Storage with enhanced metadata"""
        try:
            if not self.blob_client:
                self.logger.warning("Azure Blob client not available, skipping backup")
                return
                
            container_name = "grants-backups"
            blob_name = f"exports/{datetime.now().strftime('%Y/%m/%d')}/{csv_file_path.name}"
            
            blob_client = self.blob_client.get_blob_client(container=container_name, blob=blob_name)
            
            # Upload with comprehensive metadata
            with open(csv_file_path, 'rb') as data:
                blob_client.upload_blob(
                    data, 
                    overwrite=True,
                    metadata={
                        'source': 'grants_gov_spa_automation',
                        'processed_date': datetime.now().isoformat(),
                        'file_size': str(csv_file_path.stat().st_size),
                        'automation_type': 'spa_selenium',
                        'azure_function': os.environ.get('AZURE_FUNCTIONS_ENVIRONMENT', 'local')
                    }
                )
            
            self.logger.info(f"✅ Backed up to Azure Blob: {blob_name}")
            
        except Exception as e:
            self.logger.warning(f"Azure Blob backup failed: {e}")

    def _process_csv_data_with_azure_batching(self, csv_file_path: Path) -> bool:
        """Process CSV data with Azure Table Storage batch optimizations"""
        try:
            with open(csv_file_path, newline='', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Validate CSV structure
                if not self._validate_csv_structure(reader):
                    return False
                
                # Azure Table Storage optimal batch size
                batch_size = 25
                batch = []
                
                for row_index, row in enumerate(reader, 1):
                    try:
                        entity = self._create_azure_optimized_entity_from_row(row, row_index)
                        if entity:
                            batch.append(entity)
                            
                        # Process batch when full
                        if len(batch) >= batch_size:
                            self._process_azure_table_batch(batch)
                            batch = []
                            
                        # Progress logging with Azure metrics
                        if row_index % 100 == 0:
                            self.logger.info(f"📈 Processed {row_index} rows - Azure operations: {self.stats['azure_operations']}")
                            
                    except Exception as e:
                        self.logger.error(f"Error processing row {row_index}: {e}")
                        self.stats['errors'] += 1
                
                # Process final batch
                if batch:
                    self._process_azure_table_batch(batch)
                
                return True
                
        except Exception as e:
            self.logger.error(f"Error processing CSV data: {e}")
            return False

    def _validate_csv_structure(self, reader: csv.DictReader) -> bool:
        """Enhanced CSV structure validation for SPA output with dynamic column handling"""
        # More flexible validation - just need some basic columns
        basic_required_fields = ["OPPORTUNITY TITLE", "AGENCY NAME"]
        missing_critical = [field for field in basic_required_fields 
                        if not any(field.upper() in col.upper() for col in reader.fieldnames)]
        
        if missing_critical:
            self.logger.error(f"CSV missing critical fields: {missing_critical}")
            return False
        
        self.logger.info(f"✓ CSV validation passed ({len(reader.fieldnames)} columns detected)")
        self.logger.info(f"📋 Available columns: {', '.join(reader.fieldnames[:10])}{'...' if len(reader.fieldnames) > 10 else ''}")
        return True


    def _create_azure_optimized_entity_from_row(self, row: Dict, row_index: int) -> Optional[Dict]:
        """Create Azure Table entity with ALL CSV columns mapped dynamically"""
        try:
            # Extract opportunity ID with enhanced parsing for SPA output
            row_key = self._extract_opportunity_id_from_row(row)
            
            if not row_key:
                self.stats['skipped'] += 1
                self.logger.debug(f"Skipping row {row_index}: No valid opportunity ID found")
                return None
            
            # Start with base entity structure
            entity = {
                "PartitionKey": "Grant",
                "RowKey": str(row_key),
                "LastUpdated": datetime.now().isoformat(),
                "ProcessedDate": datetime.now().isoformat(),
                "ProcessedBy": "AutomatedGrantsFetcher_Azure_SPA_Enhanced",
                "SourceType": "SPA_Automation_Complete",
                "DataVersion": "2.0"
            }
            
            # Dynamic mapping of ALL CSV columns to Azure Table properties
            column_mappings = self._get_complete_column_mappings()
            
            # Process every column in the CSV
            for csv_column, value in row.items():
                if not csv_column or csv_column.strip() == "":
                    continue
                    
                # Clean and prepare the value
                cleaned_value = self._clean_csv_value(value)
                
                # Map to Azure property name
                azure_property = self._map_csv_column_to_azure_property(csv_column, column_mappings)
                
                # Handle different data types appropriately
                typed_value = self._convert_to_appropriate_type(azure_property, cleaned_value)
                
                # Store in entity
                entity[azure_property] = typed_value
            
            # Ensure critical URLs are built if missing
            if not entity.get("OpportunityURL") and row_key:
                entity["OpportunityURL"] = f"https://www.grants.gov/search-results-detail/{row_key}"
            
            # Add comprehensive metadata
            entity["TotalColumns"] = len(row)
            entity["ProcessingTimestamp"] = int(time.time())
            
            self.stats['processed_rows'] += 1
            return entity
            
        except Exception as e:
            self.logger.error(f"Error creating entity from row {row_index}: {e}")
            self.stats['errors'] += 1
            return None

    def _extract_opportunity_id_from_row(self, row: Dict) -> Optional[str]:
        """Enhanced opportunity ID extraction with multiple fallback strategies"""
        
        # Strategy 1: Direct OPPORTUNITY NUMBER field
        id_fields = [
            "OPPORTUNITY NUMBER",
            "\ufeffOPPORTUNITY NUMBER",  # BOM variant
            "OPPORTUNITY_NUMBER",
            "OPP_NUMBER",
            "NUMBER"
        ]
        
        for field in id_fields:
            raw_value = row.get(field)
            if raw_value:
                # Handle hyperlink format: =HYPERLINK("url","ID")
                if raw_value.startswith('=HYPERLINK('):
                    hyperlink_match = re.search(r'/search-results-detail/(\d+)', raw_value)
                    if hyperlink_match:
                        return hyperlink_match.group(1)
                        
                    # Extract ID from quotes
                    quote_match = re.search(r'"([^"]+)"[^"]*$', raw_value)
                    if quote_match:
                        potential_id = quote_match.group(1)
                        if potential_id and not potential_id.startswith('http'):
                            return potential_id
                else:
                    # Direct ID value
                    cleaned = str(raw_value).strip()
                    if cleaned and cleaned != "":
                        return cleaned
        
        # Strategy 2: Extract from any URL field
        url_fields = ["LINK TO ADDITIONAL INFORMATION", "URL", "OPPORTUNITY_URL", "LINK"]
        for field in url_fields:
            url_value = row.get(field)
            if url_value and "search-results-detail" in str(url_value):
                url_match = re.search(r'/search-results-detail/(\d+)', str(url_value))
                if url_match:
                    return url_match.group(1)
        
        # Strategy 3: Use row index as fallback if we have a title
        if row.get("OPPORTUNITY TITLE"):
            return f"ROW_{int(time.time())}_{hash(row.get('OPPORTUNITY TITLE', ''))}"
        
        return None

    def _get_complete_column_mappings(self) -> Dict[str, str]:
        """Complete mapping of CSV columns to Azure Table Storage properties"""
        return {
            # EXACT grants.gov CSV columns (verified structure)
            "OPPORTUNITY NUMBER": "OpportunityNumber",
            "OPPORTUNITY TITLE": "Title", 
            "AGENCY CODE": "AgencyCode",
            "AGENCY NAME": "AgencyName",
            "CATEGORY OF FUNDING ACTIVITY": "Category",
            "FUNDING CATEGORY EXPLANATION": "CategoryExplanation",
            "FUNDING INSTRUMENT TYPE": "FundingType",
            "ASSISTANCE LISTINGS": "CFDANumbers",
            "ESTIMATED TOTAL FUNDING": "EstimatedTotalFunding",
            "EXPECTED NUMBER OF AWARDS": "ExpectedAwards",
            "AWARD CEILING": "AwardCeiling",
            "AWARD FLOOR": "AwardFloor",
            "COST SHARING / MATCH REQUIREMENT": "CostSharing",
            "LINK TO ADDITIONAL INFORMATION": "AdditionalInfoURL",
            "GRANTOR CONTACT": "GrantorContact",
            "GRANTOR CONTACT PHONE": "GrantorPhone", 
            "GRANTOR CONTACT EMAIL": "GrantorEmail",
            "ESTIMATED POST DATE": "EstimatedPostDate",
            "ESTIMATED APPLICATION DUE DATE": "EstimatedDueDate",
            "POSTED DATE": "PostedDate",
            "CLOSE DATE": "CloseDate",
            "LAST UPDATED DATE/TIME": "LastUpdatedOriginal",
            "VERSION": "Version",
            "OPPORTUNITY STATUS": "Status",
            "OPPORTUNITY PACKAGE": "Package", 
            "SYNOPSIS ARCHIVED": "SynopsisArchived",
            "FUNDING DESCRIPTION": "Description",
            "ELIGIBLE APPLICANTS": "EligibleApplicants",
            
            # Handle BOM variants (byte order mark)
            "\ufeffOPPORTUNITY NUMBER": "OpportunityNumber",
            "\ufeffOPPORTUNITY TITLE": "Title",
            
            # Legacy/alternative column names (for compatibility)
            "CFDA NUMBER": "CFDANumbers",
            "CFDA_NUMBER": "CFDANumbers",
            "OPPORTUNITY_NUMBER": "OpportunityNumber",
            "OPPORTUNITY_TITLE": "Title",
            "AGENCY_CODE": "AgencyCode", 
            "AGENCY_NAME": "AgencyName"
        }

    def _safe_string_truncate(self, value: str, max_length: int) -> str:
        """Safely truncate string for Azure Table Storage limits"""
        if not value:
            return ""
        str_value = str(value)
        return str_value[:max_length] if len(str_value) > max_length else str_value

    def _safe_float_conversion(self, value: str) -> float:
        """Safe float conversion with enhanced cleaning"""
        try:
            if not value:
                return 0.0
            cleaned = str(value).replace(',', '').replace('$', '').replace('%', '').strip()
            return float(cleaned) if cleaned else 0.0
        except (ValueError, TypeError):
            return 0.0

    def _safe_int_conversion(self, value: str) -> int:
        """Safe integer conversion with enhanced cleaning"""
        try:
            if not value:
                return 0
            cleaned = str(value).replace(',', '').strip()
            return int(float(cleaned)) if cleaned else 0
        except (ValueError, TypeError):
            return 0

    def _process_azure_table_batch(self, batch: List[Dict]) -> None:
        """Process batch with Azure Table Storage optimizations and retry logic"""
        for entity in batch:
            try:
                # Check existence for metrics
                try:
                    self.table_client.get_entity(
                        partition_key=entity["PartitionKey"], 
                        row_key=entity["RowKey"]
                    )
                    self.stats['updated_entities'] += 1
                    operation = "updated"
                except ResourceNotFoundError:
                    self.stats['new_entities'] += 1
                    operation = "created"
                
                # Upsert with Azure retry logic
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        self.table_client.upsert_entity(entity)
                        self.stats['azure_operations'] += 1
                        break
                    except Exception as e:
                        if attempt == max_retries - 1:
                            raise e
                        time.sleep(1)  # Brief retry delay
                
                # Periodic logging
                if self.stats['azure_operations'] % 100 == 0:
                    self.logger.info(f"📊 Azure operations: {self.stats['azure_operations']} ({operation} {entity['RowKey']})")
                
            except Exception as e:
                self.logger.error(f"Error upserting entity {entity.get('RowKey')}: {e}")
                self.stats['errors'] += 1

    def _map_csv_column_to_azure_property(self, csv_column: str, mappings: Dict[str, str]) -> str:
        """Map CSV column name to Azure Table property name"""
        
        # Direct mapping first
        if csv_column in mappings:
            return mappings[csv_column]
        
        # Case-insensitive mapping
        csv_upper = csv_column.upper()
        for map_key, azure_prop in mappings.items():
            if map_key.upper() == csv_upper:
                return azure_prop
        
        # Fuzzy matching for similar columns
        csv_clean = re.sub(r'[^A-Z0-9]', '', csv_upper)
        for map_key, azure_prop in mappings.items():
            map_clean = re.sub(r'[^A-Z0-9]', '', map_key.upper())
            if csv_clean == map_clean:
                return azure_prop
        
        # Generate property name from column name
        # Convert to PascalCase and remove special characters
        clean_name = re.sub(r'[^A-Za-z0-9]', ' ', csv_column)
        words = clean_name.split()
        pascal_case = ''.join(word.capitalize() for word in words if word)
        
        # Ensure it's a valid Azure property name
        if not pascal_case or not pascal_case[0].isalpha():
            pascal_case = f"Field{pascal_case}"
        
        # Truncate if too long (Azure limit is 255 chars)
        return pascal_case[:255]

    def _clean_csv_value(self, value: str) -> str:
        """Clean CSV value for storage"""
        if value is None:
            return ""
        
        str_value = str(value).strip()
        
        # Handle hyperlink format
        if str_value.startswith('=HYPERLINK('):
            # Extract the display text (usually the last quoted part)
            quote_parts = re.findall(r'"([^"]*)"', str_value)
            if quote_parts:
                # Use the last quoted part as the display value
                return quote_parts[-1] if quote_parts[-1] else str_value
        
        return str_value

    def _convert_to_appropriate_type(self, property_name: str, value: str):
        """Convert value to appropriate type for Azure Table Storage"""
        if not value or value == "":
            return ""
        
        # Numeric fields
        numeric_fields = [
            "AwardCeiling", "AwardFloor", "ExpectedAwards", "EstimatedTotalFunding",
            "ProcessingTimestamp", "TotalColumns"
        ]
        
        if property_name in numeric_fields:
            return self._safe_numeric_conversion(value)
        
        # Boolean fields
        boolean_fields = [
            "CostSharing", "SynopsisArchived", "Package"
        ]
        
        if property_name in boolean_fields:
            return self._safe_boolean_conversion(value)
        
        # Date fields
        date_fields = [
            "PostedDate", "CloseDate", "EstimatedPostDate", "EstimatedDueDate", 
            "LastUpdatedOriginal"
        ]
        
        if property_name in date_fields:
            return self._safe_date_conversion(value)
        
        # String fields (default) - ensure they meet Azure limits
        return self._safe_string_truncate(str(value), 32000)  # Azure string limit

    def _safe_numeric_conversion(self, value: str) -> float:
        """Enhanced numeric conversion"""
        try:
            if not value or value.strip() == "":
                return 0.0
            
            # Clean the value
            cleaned = re.sub(r'[^\d.-]', '', str(value))
            if not cleaned:
                return 0.0
                
            return float(cleaned)
        except (ValueError, TypeError):
            return 0.0

    def _safe_boolean_conversion(self, value: str) -> bool:
        """Convert string to boolean"""
        if not value:
            return False
        
        str_val = str(value).lower().strip()
        return str_val in ['true', 'yes', '1', 'y', 'on', 'enabled']

    def _secure_azure_file_cleanup(self, file_path: Path) -> None:
        """Secure file cleanup with Azure best practices"""
        try:
            if file_path.exists():
                # Secure deletion for sensitive data
                file_path.unlink()
                self.logger.info(f"🗑️ Securely cleaned up: {file_path.name}")
        except Exception as e:
            self.logger.warning(f"Could not delete {file_path.name}: {e}")
    
    def _safe_date_conversion(self, value: str) -> str:
        """Convert date string to ISO format or keep as string"""
        if not value:
            return ""
        
        # For now, keep as string since grants.gov uses various date formats
        # Could enhance this later with proper date parsing
        return str(value).strip()

    def _process_csv_data_with_azure_batching(self, csv_file_path: Path) -> bool:
        """Process CSV data with Azure Table Storage batch optimizations - NO LIMITS"""
        try:
            with open(csv_file_path, newline='', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Validate CSV structure
                if not self._validate_csv_structure(reader):
                    return False
                
                # Log all available columns for debugging
                self.logger.info(f"📊 Processing ALL {len(reader.fieldnames)} CSV columns:")
                for i, col in enumerate(reader.fieldnames, 1):
                    self.logger.info(f"  {i:2d}. {col}")
                
                # Azure Table Storage optimal batch size
                batch_size = 25
                batch = []
                total_processed = 0
                
                # Process ALL rows without limits
                for row_index, row in enumerate(reader, 1):
                    try:
                        entity = self._create_azure_optimized_entity_from_row(row, row_index)
                        if entity:
                            batch.append(entity)
                            total_processed += 1
                            
                        # Process batch when full
                        if len(batch) >= batch_size:
                            self._process_azure_table_batch(batch)
                            batch = []
                            
                        # Enhanced progress logging
                        if row_index % 50 == 0:  # More frequent logging
                            self.logger.info(f"📈 Processed {row_index} rows | Stored: {total_processed} | Azure ops: {self.stats['azure_operations']}")
                            
                    except Exception as e:
                        self.logger.error(f"Error processing row {row_index}: {e}")
                        self.stats['errors'] += 1
                        continue  # Continue processing other rows
                
                # Process final batch
                if batch:
                    self._process_azure_table_batch(batch)
                
                self.logger.info(f"🎉 Completed processing ALL {row_index} rows from CSV")
                self.logger.info(f"📊 Successfully stored: {total_processed} entities")
                
                return True
                    
        except Exception as e:
            self.logger.error(f"Error processing CSV data: {e}")
            return False

    def run_automated_fetch(self, search_params: Optional[Dict] = None, cleanup: bool = True) -> bool:
        """Run complete automated workflow with NO LIMITS - process ALL available data"""
        self.logger.info("🚀 Starting Azure-optimized automated grants fetch (UNLIMITED MODE)...")
        self.stats['start_time'] = datetime.now()
        
        try:
            # Verify Azure connectivity
            if not self.table_client:
                raise GrantsAutomationError("Azure Table Storage client not initialized")
            
            # Execute SPA automation with enhanced search parameters for maximum data
            enhanced_search_params = search_params or {}
            
            # Execute automation
            downloaded_files = self.download_grants_data_with_spa_automation(enhanced_search_params)
            
            if not downloaded_files:
                self.logger.error("❌ No files downloaded - SPA automation failed")
                return False
            
            # Process ALL files with complete data extraction
            success = True
            for file_path in downloaded_files:
                self.logger.info(f"🔄 Processing complete dataset from: {file_path}")
                if not self.process_csv_file(file_path):
                    success = False
            
            # Cleanup temporary resources
            if cleanup and str(self.download_dir).startswith(tempfile.gettempdir()):
                shutil.rmtree(self.download_dir, ignore_errors=True)
                self.logger.info("🗑️ Cleaned up Azure temporary directory")
            
            self.stats['end_time'] = datetime.now()
            self._log_azure_comprehensive_summary()
            
            return success
            
        except Exception as e:
            self.logger.error(f"Fatal error in Azure automated fetch: {e}")
            self.stats['end_time'] = datetime.now() if not self.stats['end_time'] else self.stats['end_time']
            return False

    def _log_azure_comprehensive_summary(self) -> None:
        """Log comprehensive execution summary with Azure and SPA metrics"""
        if not self.stats['end_time']:
            self.stats['end_time'] = datetime.now()
            
        elapsed = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        summary = f"""
{'='*80}
📊 AZURE-OPTIMIZED GRANTS FETCH - SPA AUTOMATION SUMMARY
{'='*80}
⏱️  Duration: {elapsed:.2f} seconds
📁 Files downloaded: {self.stats['downloaded_files']}
📝 Rows processed: {self.stats['processed_rows']}
🆕 New entities: {self.stats['new_entities']}
🔄 Updated entities: {self.stats['updated_entities']}
⏭️  Skipped rows: {self.stats['skipped']}
❌ Errors: {self.stats['errors']}
🔧 Selenium operations: {self.stats['selenium_operations']}
🌐 SPA interactions: {self.stats['spa_interactions']}
⏳ JavaScript waits: {self.stats['javascript_waits']}
☁️  Azure operations: {self.stats['azure_operations']}
💾 Azure Table Storage: {'✅ Connected' if self.table_client else '❌ Not connected'}
📦 Azure Blob Storage: {'✅ Connected' if self.blob_client else '❌ Not connected'}
⚡ Avg processing speed: {self.stats['processed_rows'] / elapsed:.1f} rows/sec
🎯 SPA efficiency: {self.stats['spa_interactions'] / elapsed:.2f} interactions/sec
{'='*80}
"""
        self.logger.info(summary)


def setup_azure_environment():
    """Setup environment variables following Azure best practices"""
    
    logger = logging.getLogger(__name__)
    
    # Detect Azure Functions environment
    if os.environ.get("AZURE_FUNCTIONS_ENVIRONMENT"):
        logger.info("✓ Running in Azure Functions environment")
        return
    
    # Local development configuration with Azure compatibility
    if not os.environ.get("AzureWebJobsStorage"):
        default_connection_string = "DefaultEndpointsProtocol=https;AccountName=grantsgov225756;AccountKey=UXwW5dfy9MY9nh2BGmWhYUbzBve+6LUyT3F7+N3Cp0kWUoEk4AO3z5U6LrBYvo/VwO+Nduq2ay9E+AStKQb86Q==;EndpointSuffix=core.windows.net"
        os.environ["AzureWebJobsStorage"] = default_connection_string
        logger.info("✓ Set AzureWebJobsStorage for local Azure development")
    
    if not os.environ.get("STORAGE_ACCOUNT_NAME"):
        os.environ["STORAGE_ACCOUNT_NAME"] = "grantsgov225756"
        logger.info("✓ Set STORAGE_ACCOUNT_NAME for Azure")


def main():
    """Main execution function with Azure integration and SPA optimization"""
    # Setup Azure Application Insights compatible logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('grants_fetch_azure.log', encoding='utf-8')
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Azure environment setup
        setup_azure_environment()
        
        # Initialize with Azure optimizations
        fetcher = AutomatedGrantsFetcher()
        
        # Execute automated fetch with SPA support
        success = fetcher.run_automated_fetch(cleanup=True)
        
        if success:
            logger.info("✅ Azure-optimized automated grants fetch completed successfully!")
            print("✅ Azure-optimized automated grants fetch completed successfully!")
            return 0
        else:
            logger.error("❌ Azure automated grants fetch completed with errors")
            print("❌ Azure automated grants fetch completed with errors")
            return 1
            
    except Exception as e:
        logger.error(f"💥 Fatal error in Azure automation: {e}")
        print(f"💥 Fatal error in Azure automation: {e}")
        logging.exception("Full error details:")
        return 1

if __name__ == "__main__":
    exit(main())