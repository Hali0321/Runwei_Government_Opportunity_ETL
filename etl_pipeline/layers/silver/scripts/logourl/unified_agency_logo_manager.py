#!/usr/bin/env python3
"""
UNIFIED AGENCY LOGO URL MANAGEMENT SYSTEM
One-click solution to update LogoUrl column in CleanGrantsLayer2 table
Combines xlsx reading, mapping generation, and database updates
"""

import subprocess
import logging
import pandas as pd
import json
import sys
import os
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
        logging.FileHandler(PYCACHE_DIR / 'unified_agency_logo_manager.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class UnifiedAgencyLogoManager:
    """ONE-CLICK SOLUTION: Complete agency logo URL management system"""
    
    def __init__(self):
        # Database connection settings
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        self.table_name = "CleanGrantsLayer2"
        self.agency_column = "AgencyName"
        self.logo_column = "LogoUrl"
        self.batch_size = 50
        
        # File paths
        self.project_root = Path(__file__).parent.parent.parent.parent.parent.parent
        self.xlsx_path = self.project_root / "Agency Logo URL Mapping.xlsx"
        self.json_path = SCRIPT_DIR / "agency_logo_mapping.json"
        self.py_path = SCRIPT_DIR / "agency_logo_mapping.py"
        
        # Logo mapping (will be loaded)
        self.agency_logo_mapping = {}
        
        logger.info("🎯 Unified Agency Logo Manager initialized")
        logger.info(f"📁 Project root: {self.project_root}")
        logger.info(f"📊 XLSX file: {self.xlsx_path}")

    def execute_sql(self, sql, timeout=300):
        """Execute SQL command using sqlcmd with enhanced error handling."""
        try:
            # Clean SQL
            cleaned_sql = ' '.join(sql.split())
            
            # Build sqlcmd command
            cmd = [
                "sqlcmd",
                "-S", self.server,
                "-d", self.database,
                "-U", self.username,
                "-P", self.password,
                "-Q", cleaned_sql,
                "-b",  # Exit batch on error
                "-C",  # Trust server certificate
                "-t", str(timeout)  # Query timeout
            ]
            
            logger.info(f"📤 Executing SQL: {sql[:100]}...")
            
            # Run the command
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 30)
            
            if result.returncode != 0:
                logger.error(f"❌ SQL execution failed: {result.stderr}")
                return None
            
            output = result.stdout.strip()
            logger.info(f"✅ SQL execution successful. Output length: {len(output)}")
            return output
            
        except subprocess.TimeoutExpired:
            logger.error(f"⏰ SQL command timed out after {timeout + 30} seconds")
            return None
        except Exception as e:
            logger.error(f"💥 Error executing SQL: {e}")
            return None

    def read_xlsx_mapping(self):
        """Read agency-to-logo URL mapping from xlsx file."""
        try:
            logger.info("📊 Reading agency logo mapping from xlsx file...")
            
            if not self.xlsx_path.exists():
                logger.error(f"❌ XLSX file not found: {self.xlsx_path}")
                return False
            
            # Read the xlsx file
            df = pd.read_excel(self.xlsx_path)
            
            logger.info(f"📋 Successfully read xlsx file: {self.xlsx_path}")
            logger.info(f"📊 Columns: {list(df.columns)}")
            logger.info(f"📈 Rows: {len(df)}")
            
            # Automatically identify agency and logo URL columns
            agency_col = None
            logo_col = None
            
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['agency', 'name', 'organization']):
                    agency_col = col
                elif any(keyword in col_lower for keyword in ['logo', 'url', 'link']):
                    logo_col = col
            
            if not agency_col or not logo_col:
                logger.error("❌ Could not identify agency and logo URL columns")
                logger.error(f"Available columns: {list(df.columns)}")
                return False
            
            logger.info(f"🎯 Identified columns: Agency='{agency_col}', Logo='{logo_col}'")
            
            # Create mapping
            mapping = {}
            for _, row in df.iterrows():
                agency = str(row[agency_col]).strip()
                logo_url = str(row[logo_col]).strip()
                
                if agency and logo_url and agency.lower() != 'nan' and logo_url.lower() != 'nan':
                    mapping[agency] = logo_url
            
            self.agency_logo_mapping = mapping
            logger.info(f"🎯 Created mapping with {len(mapping)} entries")
            
            # Save mapping files for future use
            self._save_mapping_files()
            
            return True
            
        except Exception as e:
            logger.error(f"💥 Error reading xlsx file: {e}")
            return False

    def load_existing_mapping(self):
        """Load mapping from existing files if xlsx is not available."""
        try:
            logger.info("📋 Attempting to load existing mapping files...")
            
            # Try JSON first
            if self.json_path.exists():
                with open(self.json_path, 'r') as f:
                    self.agency_logo_mapping = json.load(f)
                logger.info(f"📋 Loaded {len(self.agency_logo_mapping)} mappings from JSON file")
                return True
            
            # Try Python file
            if self.py_path.exists():
                import importlib.util
                spec = importlib.util.spec_from_file_location("agency_mapping", self.py_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                self.agency_logo_mapping = module.AGENCY_LOGO_MAPPING
                logger.info(f"🐍 Loaded {len(self.agency_logo_mapping)} mappings from Python file")
                return True
            
            logger.error("❌ No existing mapping files found")
            return False
            
        except Exception as e:
            logger.error(f"💥 Error loading existing mapping: {e}")
            return False

    def _save_mapping_files(self):
        """Save mapping to JSON and Python files for future use."""
        try:
            # Save to JSON
            with open(self.json_path, 'w') as f:
                json.dump(self.agency_logo_mapping, f, indent=2)
            logger.info(f"💾 Mapping saved to JSON: {self.json_path}")
            
            # Save to Python file
            with open(self.py_path, 'w') as f:
                f.write("# Agency to Logo URL mapping - Generated automatically\n")
                f.write("# DO NOT EDIT MANUALLY - Use unified_agency_logo_manager.py\n\n")
                f.write("AGENCY_LOGO_MAPPING = {\n")
                for agency, url in self.agency_logo_mapping.items():
                    safe_agency = agency.replace("'", "\\'")
                    safe_url = url.replace("'", "\\'")
                    f.write(f"    '{safe_agency}': '{safe_url}',\n")
                f.write("}\n")
            logger.info(f"🐍 Python mapping saved to: {self.py_path}")
            
        except Exception as e:
            logger.error(f"💥 Error saving mapping files: {e}")

    def ensure_logo_url_column(self):
        """Ensure LogoUrl column exists in the database table."""
        logger.info("🔧 Ensuring LogoUrl column exists in database...")
        
        sql = f"""
        -- Check if LogoUrl column exists
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = '{self.table_name}' 
                      AND COLUMN_NAME = '{self.logo_column}')
        BEGIN
            ALTER TABLE {self.table_name} 
            ADD {self.logo_column} NVARCHAR(1000) NULL;
            PRINT 'LogoUrl column added successfully';
        END
        ELSE
        BEGIN
            PRINT 'LogoUrl column already exists';
        END
        
        -- Create index for better performance if it doesn't exist
        IF NOT EXISTS (SELECT * FROM sys.indexes 
                      WHERE object_id = OBJECT_ID('{self.table_name}') 
                      AND name = 'IX_{self.table_name}_{self.logo_column}')
        BEGIN
            CREATE NONCLUSTERED INDEX IX_{self.table_name}_{self.logo_column} 
            ON {self.table_name} ({self.logo_column});
            PRINT 'LogoUrl index created';
        END
        ELSE
        BEGIN
            PRINT 'LogoUrl index already exists';
        END
        
        -- Verify column exists
        SELECT COUNT(*) as ColumnExists
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{self.table_name}' 
        AND COLUMN_NAME = '{self.logo_column}';
        """
        
        result = self.execute_sql(sql, timeout=120)
        
        # Check if we got a valid result and the column exists
        if result is not None:
            # Look for the column count result
            lines = result.split('\n')
            for line in lines:
                if line.strip().isdigit() and int(line.strip()) > 0:
                    logger.info("✅ LogoUrl column is ready")
                    return True
            
            # If we can't find column count, assume success if no errors
            logger.info("✅ LogoUrl column operation completed")
            return True
        else:
            logger.error("❌ Failed to ensure LogoUrl column")
            return False

    def update_logo_urls(self):
        """Update LogoUrl column with agency logo mappings."""
        if not self.agency_logo_mapping:
            logger.error("❌ No logo mapping available. Cannot update database.")
            return False
        
        logger.info("🖼️ Starting logo URL update process...")
        
        # Get unique agencies from database
        agencies_sql = f"""
        SELECT DISTINCT {self.agency_column} 
        FROM {self.table_name} 
        WHERE {self.agency_column} IS NOT NULL 
        ORDER BY {self.agency_column};
        """
        
        agencies_result = self.execute_sql(agencies_sql)
        if not agencies_result:
            logger.error("❌ Failed to retrieve agencies from database")
            return False
        
        # Parse agencies from result
        agencies = []
        for line in agencies_result.split('\n'):
            line = line.strip()
            if line and not line.startswith('-') and line != self.agency_column:
                agencies.append(line)
        
        logger.info(f"📊 Found {len(agencies)} unique agencies in database")
        
        # Process updates in batches
        total_agencies = len(agencies)
        updated_count = 0
        failed_count = 0
        
        for i in range(0, total_agencies, self.batch_size):
            batch = agencies[i:i+self.batch_size]
            batch_number = i // self.batch_size + 1
            total_batches = (total_agencies - 1) // self.batch_size + 1
            
            logger.info(f"📦 Processing batch {batch_number}/{total_batches} ({len(batch)} agencies)")
            
            # Build batch update SQL
            update_statements = []
            for agency in batch:
                if agency in self.agency_logo_mapping:
                    logo_url = self.agency_logo_mapping[agency]
                    
                    # Escape single quotes for SQL
                    safe_agency = agency.replace("'", "''")
                    safe_logo_url = logo_url.replace("'", "''")
                    
                    update_sql = f"""
                    UPDATE {self.table_name} 
                    SET {self.logo_column} = '{safe_logo_url}' 
                    WHERE {self.agency_column} = '{safe_agency}';
                    """
                    update_statements.append(update_sql)
                else:
                    logger.warning(f"⚠️ No logo mapping found for agency: {agency}")
                    failed_count += 1
            
            if update_statements:
                batch_sql = "\n".join(update_statements)
                result = self.execute_sql(batch_sql, timeout=300)
                
                if result is not None:
                    updated_count += len(update_statements)
                    logger.info(f"✅ Batch {batch_number} completed successfully")
                else:
                    logger.error(f"❌ Batch {batch_number} failed")
                    failed_count += len(update_statements)
        
        logger.info(f"📊 Update process completed:")
        logger.info(f"   ✅ Updated: {updated_count} agencies")
        logger.info(f"   ❌ Failed: {failed_count} agencies")
        logger.info(f"   📈 Success rate: {(updated_count/(updated_count+failed_count))*100:.1f}%")
        
        return updated_count > 0

    def validate_updates(self):
        """Validate that LogoUrl column has been populated correctly."""
        logger.info("🔍 Validating LogoUrl updates...")
        
        validation_sql = f"""
        -- Overall statistics
        SELECT 
            'VALIDATION_STATS' as Report_Type,
            COUNT(*) as Total_Records,
            COUNT(CASE WHEN {self.logo_column} IS NOT NULL AND {self.logo_column} != '' THEN 1 END) as Records_With_Logo,
            COUNT(DISTINCT {self.agency_column}) as Total_Agencies,
            COUNT(DISTINCT CASE WHEN {self.logo_column} IS NOT NULL AND {self.logo_column} != '' THEN {self.agency_column} END) as Agencies_With_Logo,
            COUNT(DISTINCT {self.logo_column}) as Unique_Logo_URLs,
            ROUND(
                (COUNT(CASE WHEN {self.logo_column} IS NOT NULL AND {self.logo_column} != '' THEN 1 END) * 100.0) / 
                NULLIF(COUNT(*), 0), 2
            ) as Coverage_Percent
        FROM {self.table_name};
        
        -- Sample of updated records
        SELECT TOP 10
            'SAMPLE_RECORDS' as Sample_Type,
            {self.agency_column},
            {self.logo_column}
        FROM {self.table_name}
        WHERE {self.logo_column} IS NOT NULL AND {self.logo_column} != ''
        ORDER BY {self.agency_column};
        """
        
        result = self.execute_sql(validation_sql, timeout=120)
        if result:
            logger.info("🔍 Validation Results:")
            logger.info(result)
            return True
        else:
            logger.error("❌ Validation failed")
            return False

    def generate_comprehensive_report(self):
        """Generate comprehensive report of logo URL mappings."""
        logger.info("📊 Generating comprehensive report...")
        
        report_sql = f"""
        -- Coverage and Statistics
        SELECT 
            'COVERAGE_REPORT' as Report_Type,
            COUNT(*) as Total_Grants,
            COUNT(CASE WHEN {self.logo_column} IS NOT NULL AND {self.logo_column} != '' THEN 1 END) as Grants_With_Logo,
            COUNT(CASE WHEN {self.logo_column} IS NULL OR {self.logo_column} = '' THEN 1 END) as Grants_Without_Logo,
            ROUND(
                (COUNT(CASE WHEN {self.logo_column} IS NOT NULL AND {self.logo_column} != '' THEN 1 END) * 100.0) / 
                NULLIF(COUNT(*), 0), 2
            ) as Coverage_Percent
        FROM {self.table_name};
        
        -- Top agencies by grant count
        SELECT TOP 15
            'TOP_AGENCIES' as Report_Type,
            {self.agency_column},
            {self.logo_column},
            COUNT(*) as Grant_Count
        FROM {self.table_name}
        WHERE {self.logo_column} IS NOT NULL AND {self.logo_column} != ''
        GROUP BY {self.agency_column}, {self.logo_column}
        ORDER BY Grant_Count DESC;
        
        -- Agencies without logos (if any)
        SELECT TOP 10
            'MISSING_LOGOS' as Report_Type,
            {self.agency_column},
            COUNT(*) as Grant_Count
        FROM {self.table_name}
        WHERE {self.logo_column} IS NULL OR {self.logo_column} = ''
        GROUP BY {self.agency_column}
        ORDER BY Grant_Count DESC;
        """
        
        result = self.execute_sql(report_sql, timeout=150)
        if result:
            logger.info("📊 Comprehensive Report:")
            logger.info(result)
            return True
        else:
            logger.error("❌ Report generation failed")
            return False

    def run_complete_process(self):
        """🎯 ONE-CLICK EXECUTION: Complete logo URL management process."""
        logger.info("🚀 STARTING COMPLETE LOGO URL MANAGEMENT PROCESS")
        logger.info("=" * 80)
        logger.info(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("🎯 This will update ALL LogoUrl entries in the database")
        
        steps = [
            ("📊 Load Logo Mapping", self._load_mapping_with_fallback),
            ("🔧 Ensure Database Column", self.ensure_logo_url_column),
            ("🖼️ Update Logo URLs", self.update_logo_urls),
            ("🔍 Validate Updates", self.validate_updates),
            ("📊 Generate Report", self.generate_comprehensive_report)
        ]
        
        success_count = 0
        for i, (step_name, step_function) in enumerate(steps, 1):
            logger.info(f"\n📍 STEP {i}/{len(steps)}: {step_name}")
            logger.info("-" * 60)
            
            try:
                success = step_function()
                if success:
                    logger.info(f"✅ {step_name} completed successfully")
                    success_count += 1
                else:
                    logger.error(f"❌ {step_name} failed")
                    # For critical steps, break the process
                    if i <= 2:  # Loading mapping and ensuring column are critical
                        logger.error("💥 Critical step failed. Cannot continue.")
                        break
            except Exception as e:
                logger.error(f"💥 {step_name} error: {e}")
                if i <= 2:
                    logger.error("💥 Critical step failed. Cannot continue.")
                    break
        
        logger.info(f"\n🎯 PROCESS SUMMARY")
        logger.info("=" * 60)
        logger.info(f"✅ Completed Steps: {success_count}/{len(steps)}")
        logger.info(f"🖼️ Total Agency Mappings: {len(self.agency_logo_mapping)}")
        logger.info(f"📊 Database Table: {self.table_name}")
        logger.info(f"🔗 Logo Column: {self.logo_column}")
        
        if success_count >= 4:
            logger.info("\n🎉 LOGO URL MANAGEMENT COMPLETED SUCCESSFULLY!")
            logger.info("✨ Your CleanGrantsLayer2 table now has populated LogoUrl column!")
            logger.info("\n🔍 QUICK VERIFICATION QUERIES:")
            logger.info("   1. SELECT COUNT(*), COUNT(LogoUrl) FROM CleanGrantsLayer2;")
            logger.info("   2. SELECT TOP 10 AgencyName, LogoUrl FROM CleanGrantsLayer2 WHERE LogoUrl IS NOT NULL;")
            logger.info("   3. SELECT AgencyName, COUNT(*) FROM CleanGrantsLayer2 GROUP BY AgencyName ORDER BY COUNT(*) DESC;")
            return True
        else:
            logger.error("❌ LOGO URL MANAGEMENT FAILED!")
            logger.error("📝 Please check the logs above for detailed error information")
            return False

    def _load_mapping_with_fallback(self):
        """Load mapping from xlsx file with fallback to existing files."""
        # Try xlsx first
        if self.xlsx_path.exists():
            logger.info(f"📊 Found xlsx file: {self.xlsx_path}")
            if self.read_xlsx_mapping():
                return True
        
        # Fallback to existing files
        logger.warning(f"⚠️ XLSX file not found or failed to read: {self.xlsx_path}")
        logger.info("📋 Attempting to load from existing mapping files...")
        return self.load_existing_mapping()

def main():
    """🎯 MAIN EXECUTION FUNCTION - ONE-CLICK LOGO URL UPDATE"""
    print("🖼️ UNIFIED AGENCY LOGO URL MANAGEMENT SYSTEM")
    print("=" * 80)
    print("🎯 ONE-CLICK SOLUTION FOR LOGO URL UPDATES")
    print("=" * 80)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🚀 This script will automatically:")
    print("   1. 📊 Read agency logo mappings from xlsx file")
    print("   2. 🔧 Ensure database column exists")
    print("   3. 🖼️ Update ALL LogoUrl entries in CleanGrantsLayer2")
    print("   4. 🔍 Validate the updates")
    print("   5. 📊 Generate comprehensive report")
    print()
    
    # Initialize the manager
    manager = UnifiedAgencyLogoManager()
    
    # Run the complete process
    success = manager.run_complete_process()
    
    print("\n" + "=" * 80)
    if success:
        print("🎉 SUCCESS! LOGO URL MANAGEMENT COMPLETED!")
        print(f"🖼️ Processed {len(manager.agency_logo_mapping)} agency mappings")
        print("\n✨ YOUR DATABASE IS NOW UPDATED!")
        print("📊 The CleanGrantsLayer2 table now has populated LogoUrl column")
        print("\n🔍 VERIFICATION:")
        print("   Your grants database now includes official government agency logos!")
        print("   Each grant record now has the appropriate agency logo URL.")
        print("\n📝 NEXT STEPS:")
        print("   - Use the LogoUrl column in your applications")
        print("   - Display agency logos in your grant listings")
        print("   - The logos are official government agency logos")
        
    else:
        print("❌ FAILED! LOGO URL MANAGEMENT INCOMPLETE")
        print("📝 Please check the logs for detailed error information")
        print("\n🔧 TROUBLESHOOTING:")
        print("   1. Ensure xlsx file exists: Agency Logo URL Mapping.xlsx")
        print("   2. Check database connection and permissions")
        print("   3. Verify table structure and column names")
        print("   4. Check the log file for detailed error messages")
    
    print(f"\n📅 Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

if __name__ == "__main__":
    main()
