#!/bin/bash

# Complete Azure Data Pipeline Setup and Execution Script
# This single script handles everything: setup, restructuring, and execution

echo "🚀 Complete Azure Data Pipeline Setup & Execution"

echo "🚀 Complete Azure Data Pipeline Setup & Execution"
echo "================================================="
echo "📅 Started: $(date)"
echo "🎯 Goal: Complete pipeline setup and execution in one script"
echo "🖥️ Running on: $(uname -s) $(uname -m)"
echo "📂 Current directory: $(pwd)"

# Set base directory with validation
BASE_DIR="/Users/dinghali/Desktop/Runwei/grants_gov_api_azure"
echo "🔍 Checking base directory: $BASE_DIR"

if [ ! -d "$BASE_DIR" ]; then
    echo "❌ ERROR: Base directory does not exist: $BASE_DIR"
    exit 1
fi

echo "✅ Base directory confirmed: $BASE_DIR"
cd "$BASE_DIR"

# Function to create Layer 2 comprehensive enhancement script
create_layer2_script() {
    echo "🔧 Creating Layer 2 comprehensive enhancement script..."
    
    mkdir -p "layers/layer2_clean_business_data/scripts"
    
    cat > "layers/layer2_clean_business_data/scripts/comprehensive_layer2_enhancement.py" << 'LAYER2_EOF'
#!/usr/bin/env python3
"""
Layer 2 - Comprehensive Data Enhancement - Azure SQL Database
Complete all missing fields, format properly, and add validation flags
"""

import subprocess
import logging
from datetime import datetime
import time
import sys

# Configure logging to show output immediately
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class Layer2ComprehensiveEnhancer:
    """Comprehensive Layer 2 data enhancement - Azure SQL Database optimized"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        print("🔧 Initialized Layer 2 Enhancer")
        
    def execute_sql_command(self, sql_query, timeout=600):
        """Execute SQL command with Azure SQL Database optimizations"""
        print(f"📊 Executing SQL command (timeout: {timeout}s)...")
        try:
            cmd = [
                "sqlcmd", "-S", self.server, "-d", self.database, 
                "-U", self.username, "-P", self.password,
                "-Q", sql_query, "-C", "-t", str(timeout), "-I", "-b"
            ]
            
            print("🔄 Running sqlcmd...")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 30)
            
            if result.returncode == 0:
                print("✅ SQL command executed successfully")
                if result.stdout and result.stdout.strip():
                    print(f"📋 Output: {result.stdout.strip()}")
                return result.stdout
            else:
                print(f"❌ SQL command failed with return code {result.returncode}")
                if result.stderr:
                    print(f"🔴 Error: {result.stderr}")
                if result.stdout:
                    print(f"📋 Output: {result.stdout}")
                return None
                
        except subprocess.TimeoutExpired:
            print(f"⏰ SQL command timed out after {timeout} seconds")
            return None
        except Exception as e:
            print(f"💥 Error executing SQL: {e}")
            return None

    def add_enhancement_columns(self):
        """Add all enhancement columns to Layer 2"""
        print("🔧 Adding enhancement columns to Layer 2...")
        
        sql = """
        -- Add comprehensive enhancement columns to CleanGrantsLayer2
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'LogoUrl')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD LogoUrl NVARCHAR(MAX);
            PRINT 'Added LogoUrl column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'CoverImage')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD CoverImage NVARCHAR(MAX);
            PRINT 'Added CoverImage column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'Summary')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD Summary NVARCHAR(1000);
            PRINT 'Added Summary column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'AwardValueFormatted')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD AwardValueFormatted NVARCHAR(100);
            PRINT 'Added AwardValueFormatted column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'DataQualityScore')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD DataQualityScore DECIMAL(3,1) DEFAULT 0.0;
            PRINT 'Added DataQualityScore column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'EnhancementStatus')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD EnhancementStatus NVARCHAR(100) DEFAULT 'Pending';
            PRINT 'Added EnhancementStatus column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'ReadyForLayer3')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD ReadyForLayer3 BIT DEFAULT 0;
            PRINT 'Added ReadyForLayer3 column';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('CleanGrantsLayer2') AND name = 'EnhancementDate')
        BEGIN
            ALTER TABLE CleanGrantsLayer2 ADD EnhancementDate DATETIME2 DEFAULT GETDATE();
            PRINT 'Added EnhancementDate column';
        END
        
        PRINT 'Enhancement columns added successfully';
        """
        
        result = self.execute_sql_command(sql)
        if result is not None:
            print("✅ Enhancement columns added successfully")
            time.sleep(2)
            return True
        else:
            print("❌ Failed to add enhancement columns")
            return False

    def comprehensive_enhancement(self):
        """Run comprehensive data enhancement on Layer 2"""
        print("🚀 Running comprehensive data enhancement...")
        
        sql = """
        BEGIN TRANSACTION ComprehensiveEnhancement;
        
        -- Step 1: Generate LogoUrl and visual assets
        UPDATE CleanGrantsLayer2
        SET LogoUrl = CASE
            WHEN LogoUrl IS NOT NULL AND LogoUrl != '' THEN LogoUrl
            WHEN ServiceProviderName LIKE '%Department%' OR ServiceProviderName LIKE '%Agency%' THEN
                'https://www.grants.gov/assets/img/logo.png'
            WHEN ServiceProviderName LIKE '%University%' OR ServiceProviderName LIKE '%Educational%' THEN
                'https://via.placeholder.com/150x150/1f4e79/ffffff?text=EDU'
            WHEN ServiceProviderName LIKE '%Foundation%' OR ServiceProviderName LIKE '%Non-Profit%' THEN
                'https://via.placeholder.com/150x150/2d5a87/ffffff?text=NPO'
            WHEN CategoryOfFundingActivity LIKE '%Health%' OR CategoryOfFundingActivity LIKE '%Medical%' THEN
                'https://via.placeholder.com/150x150/dc2626/ffffff?text=HEALTH'
            ELSE 'https://via.placeholder.com/150x150/4a90e2/ffffff?text=GRANT'
        END,
        CoverImage = CASE
            WHEN CoverImage IS NOT NULL AND CoverImage != '' THEN CoverImage
            WHEN CategoryOfFundingActivity LIKE '%Research%' THEN
                'https://via.placeholder.com/800x400/1e3a8a/ffffff?text=Research+Grant'
            WHEN CategoryOfFundingActivity LIKE '%Innovation%' THEN
                'https://via.placeholder.com/800x400/7c3aed/ffffff?text=Innovation+Grant'
            WHEN CategoryOfFundingActivity LIKE '%Health%' THEN
                'https://via.placeholder.com/800x400/dc2626/ffffff?text=Health+Grant'
            WHEN CategoryOfFundingActivity LIKE '%Education%' THEN
                'https://via.placeholder.com/800x400/059669/ffffff?text=Education+Grant'
            WHEN CategoryOfFundingActivity LIKE '%Environment%' THEN
                'https://via.placeholder.com/800x400/16a34a/ffffff?text=Environment+Grant'
            WHEN CategoryOfFundingActivity LIKE '%Technology%' THEN
                'https://via.placeholder.com/800x400/6366f1/ffffff?text=Tech+Grant'
            ELSE 'https://via.placeholder.com/800x400/6b7280/ffffff?text=Grant+Opportunity'
        END;
        
        PRINT CONCAT('Enhanced visual assets for ', @@ROWCOUNT, ' records');
        
        -- Step 2: Generate Summary from OpportunityDescription
        UPDATE CleanGrantsLayer2
        SET Summary = CASE
            WHEN OpportunityDescription IS NOT NULL AND LEN(OpportunityDescription) BETWEEN 50 AND 300 THEN
                LEFT(OpportunityDescription, 250) + CASE WHEN LEN(OpportunityDescription) > 250 THEN '...' ELSE '' END
            WHEN OpportunityDescription IS NOT NULL AND LEN(OpportunityDescription) > 20 THEN
                LEFT(OpportunityDescription, 250) + CASE WHEN LEN(OpportunityDescription) > 250 THEN '...' ELSE '' END
            WHEN OpportunityTitle IS NOT NULL THEN
                OpportunityTitle + ' - ' + ISNULL(CategoryOfFundingActivity, 'Grant opportunity') + ' providing federal funding.'
            ELSE 'Federal grant opportunity providing funding and support for eligible applicants.'
        END;
        
        PRINT CONCAT('Generated Summary for ', @@ROWCOUNT, ' records');
        
        -- Step 3: Format AwardValue
        UPDATE CleanGrantsLayer2
        SET AwardValueFormatted = CASE
            WHEN AwardCeiling IS NOT NULL AND ISNUMERIC(AwardCeiling) = 1 AND CAST(AwardCeiling AS BIGINT) > 0 THEN
                '$' + FORMAT(CAST(AwardCeiling AS MONEY), 'N0') + ' USD'
            WHEN AwardFloor IS NOT NULL AND ISNUMERIC(AwardFloor) = 1 AND CAST(AwardFloor AS BIGINT) > 0 THEN
                'From $' + FORMAT(CAST(AwardFloor AS MONEY), 'N0') + ' USD'
            WHEN EstimatedTotalProgramFunding IS NOT NULL AND ISNUMERIC(EstimatedTotalProgramFunding) = 1 AND CAST(EstimatedTotalProgramFunding AS BIGINT) > 0 THEN
                'Total: $' + FORMAT(CAST(EstimatedTotalProgramFunding AS MONEY), 'N0') + ' USD'
            ELSE 'Amount varies'
        END;
        
        PRINT CONCAT('Formatted AwardValue for ', @@ROWCOUNT, ' records');
        
        -- Step 4: Calculate quality scores and readiness
        UPDATE CleanGrantsLayer2
        SET DataQualityScore = (
            CASE WHEN OpportunityTitle IS NOT NULL AND LEN(OpportunityTitle) > 10 THEN 2.0 ELSE 0 END +
            CASE WHEN OpportunityDescription IS NOT NULL AND LEN(OpportunityDescription) > 50 THEN 2.0 ELSE 0 END +
            CASE WHEN Summary IS NOT NULL AND LEN(Summary) > 20 THEN 1.0 ELSE 0 END +
            CASE WHEN ServiceProviderName IS NOT NULL AND ServiceProviderName != '' THEN 1.0 ELSE 0 END +
            CASE WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN 2.0 ELSE 0 END +
            CASE WHEN EligibilityDescription IS NOT NULL AND LEN(EligibilityDescription) > 20 THEN 1.0 ELSE 0 END +
            CASE WHEN CategoryOfFundingActivity IS NOT NULL AND CategoryOfFundingActivity != '' THEN 1.0 ELSE 0 END
        ),
        EnhancementStatus = CASE
            WHEN (CASE WHEN OpportunityTitle IS NOT NULL AND LEN(OpportunityTitle) > 10 THEN 2.0 ELSE 0 END +
                  CASE WHEN OpportunityDescription IS NOT NULL AND LEN(OpportunityDescription) > 50 THEN 2.0 ELSE 0 END +
                  CASE WHEN Summary IS NOT NULL AND LEN(Summary) > 20 THEN 1.0 ELSE 0 END +
                  CASE WHEN ServiceProviderName IS NOT NULL AND ServiceProviderName != '' THEN 1.0 ELSE 0 END +
                  CASE WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN 2.0 ELSE 0 END +
                  CASE WHEN EligibilityDescription IS NOT NULL AND LEN(EligibilityDescription) > 20 THEN 1.0 ELSE 0 END +
                  CASE WHEN CategoryOfFundingActivity IS NOT NULL AND CategoryOfFundingActivity != '' THEN 1.0 ELSE 0 END) >= 8.0 
                THEN 'Excellent - Production Ready'
            WHEN (CASE WHEN OpportunityTitle IS NOT NULL AND LEN(OpportunityTitle) > 10 THEN 2.0 ELSE 0 END +
                  CASE WHEN OpportunityDescription IS NOT NULL AND LEN(OpportunityDescription) > 50 THEN 2.0 ELSE 0 END +
                  CASE WHEN Summary IS NOT NULL AND LEN(Summary) > 20 THEN 1.0 ELSE 0 END +
                  CASE WHEN ServiceProviderName IS NOT NULL AND ServiceProviderName != '' THEN 1.0 ELSE 0 END +
                  CASE WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN 2.0 ELSE 0 END +
                  CASE WHEN EligibilityDescription IS NOT NULL AND LEN(EligibilityDescription) > 20 THEN 1.0 ELSE 0 END +
                  CASE WHEN CategoryOfFundingActivity IS NOT NULL AND CategoryOfFundingActivity != '' THEN 1.0 ELSE 0 END) >= 6.0 
                THEN 'Good - Enhanced and ready'
            ELSE 'Needs improvement'
        END,
        ReadyForLayer3 = CASE
            WHEN OpportunityTitle IS NOT NULL 
                AND OpportunityTitle != ''
                AND Summary IS NOT NULL
                AND AwardValueFormatted IS NOT NULL
                AND (CASE WHEN OpportunityTitle IS NOT NULL AND LEN(OpportunityTitle) > 10 THEN 2.0 ELSE 0 END +
                     CASE WHEN OpportunityDescription IS NOT NULL AND LEN(OpportunityDescription) > 50 THEN 2.0 ELSE 0 END +
                     CASE WHEN Summary IS NOT NULL AND LEN(Summary) > 20 THEN 1.0 ELSE 0 END +
                     CASE WHEN ServiceProviderName IS NOT NULL AND ServiceProviderName != '' THEN 1.0 ELSE 0 END +
                     CASE WHEN AwardValueFormatted IS NOT NULL AND AwardValueFormatted != 'Amount varies' THEN 2.0 ELSE 0 END +
                     CASE WHEN EligibilityDescription IS NOT NULL AND LEN(EligibilityDescription) > 20 THEN 1.0 ELSE 0 END +
                     CASE WHEN CategoryOfFundingActivity IS NOT NULL AND CategoryOfFundingActivity != '' THEN 1.0 ELSE 0 END) >= 6.0
                THEN 1
            ELSE 0
        END,
        EnhancementDate = GETDATE();
        
        PRINT CONCAT('Calculated quality scores for ', @@ROWCOUNT, ' records');
        
        COMMIT TRANSACTION ComprehensiveEnhancement;
        
        -- Get final statistics
        SELECT 
            COUNT(*) as TotalRecords,
            AVG(DataQualityScore) as AvgQuality,
            SUM(CASE WHEN ReadyForLayer3 = 1 THEN 1 ELSE 0 END) as ReadyForLayer3Count,
            ROUND(100.0 * SUM(CASE WHEN ReadyForLayer3 = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) as Layer3ReadyPercentage
        FROM CleanGrantsLayer2;
        """
        
        result = self.execute_sql_command(sql, timeout=600)
        if result is not None:
            print("✅ Comprehensive enhancement completed successfully")
            return True
        else:
            print("❌ Failed to run comprehensive enhancement")
            return False

def main():
    """Main execution function"""
    print("=" * 70)
    print("🚀 Layer 2 - Comprehensive Data Enhancement - Azure SQL Database")
    print("=" * 70)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Goal: Comprehensive data enhancement and quality scoring")
    
    try:
        enhancer = Layer2ComprehensiveEnhancer()
        
        # Step 1: Add enhancement columns
        print("\n🔧 Step 1: Adding comprehensive enhancement columns...")
        if not enhancer.add_enhancement_columns():
            print("❌ Failed to add enhancement columns")
            return False
        
        # Step 2: Run comprehensive enhancement
        print("\n🚀 Step 2: Running comprehensive data enhancement...")
        if not enhancer.comprehensive_enhancement():
            print("❌ Failed to enhance data")
            return False
        
        print("\n🎊 SUCCESS! Layer 2 Comprehensive Enhancement Complete!")
        print("=" * 70)
        print("✅ Visual Assets: LogoUrl and CoverImage generated")
        print("✅ Content: Summary field generated from descriptions")
        print("✅ Financial: AwardValue formatted to currency standard")
        print("✅ Quality: Comprehensive scoring (0-10 scale)")
        print("✅ Readiness: Records marked ready for Layer 3 selection")
        
        return True
        
    except Exception as e:
        print(f"\n💥 Layer 2 enhancement failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🏁 Starting Layer 2 Enhancement...")
    success = main()
    if success:
        print("\n🚀 Layer 2 Enhancement Successfully Completed!")
        print("📊 Your CleanGrantsLayer2 contains fully enhanced data")
        print("🎯 Ready for Layer 3 simple selection")
    else:
        print("\n❌ Layer 2 enhancement failed - check logs for details")
        exit(1)
LAYER2_EOF

    chmod +x "layers/layer2_clean_business_data/scripts/comprehensive_layer2_enhancement.py"
    echo "✅ Layer 2 script created"
}

# Function to create Layer 3 simple selection script
create_layer3_script() {
    echo "🎯 Creating Layer 3 simple selection script..."
    
    mkdir -p "layers/layer3_final_opportunities/scripts"
    
    cat > "layers/layer3_final_opportunities/scripts/simple_layer3_selection.py" << 'LAYER3_EOF'
#!/usr/bin/env python3
"""
Layer 3 - Simple Selection - Azure SQL Database
Simple selection of high-quality records from Layer 2 for final output
This is the SELECTION LAYER - no data enhancement, just selection
"""

import subprocess
import logging
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class Layer3SimpleSelector:
    """Simple selection of high-quality records from Layer 2 - Azure SQL Database optimized"""
    
    def __init__(self):
        self.server = "grants-gov-sql-server.database.windows.net"
        self.database = "GrantsGovDB"
        self.username = "grantsadmin"
        self.password = "Grant$Admin2024!"
        print("🎯 Initialized Layer 3 Selector")
        
    def execute_sql_command(self, sql_query, timeout=300):
        """Execute SQL command with Azure SQL Database optimizations"""
        print(f"📊 Executing SQL command (timeout: {timeout}s)...")
        try:
            cmd = [
                "sqlcmd", "-S", self.server, "-d", self.database,
                "-U", self.username, "-P", self.password,
                "-Q", sql_query, "-C", "-t", str(timeout), "-I", "-b"
            ]
            
            print("🔄 Running sqlcmd...")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 30)
            
            if result.returncode == 0:
                print("✅ SQL command executed successfully")
                if result.stdout and result.stdout.strip():
                    print(f"📋 Output: {result.stdout.strip()}")
                return result.stdout
            else:
                print(f"❌ SQL command failed with return code {result.returncode}")
                if result.stderr:
                    print(f"🔴 Error: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            print(f"⏰ SQL command timed out after {timeout} seconds")
            return None
        except Exception as e:
            print(f"💥 Error executing SQL: {e}")
            return None

    def create_simple_layer3_table(self):
        """Create simple Layer 3 table structure"""
        print("🏗️ Creating simple Layer 3 table structure...")
        
        sql = """
        -- Drop existing FinalOpportunities table if it exists
        IF OBJECT_ID('dbo.FinalOpportunities', 'U') IS NOT NULL
            DROP TABLE dbo.FinalOpportunities;
        
        -- Create simple FinalOpportunities table
        CREATE TABLE dbo.FinalOpportunities (
            ID NVARCHAR(50) PRIMARY KEY,
            Title NVARCHAR(MAX),
            Summary NVARCHAR(1000),
            AwardValue NVARCHAR(100),
            Deadline NVARCHAR(100),
            Url NVARCHAR(MAX),
            ContactEmail NVARCHAR(MAX),
            ContactNames NVARCHAR(MAX),
            LogoUrl NVARCHAR(MAX),
            CoverImage NVARCHAR(MAX),
            Description NVARCHAR(MAX),
            Eligibility NVARCHAR(MAX),
            ServiceProvider NVARCHAR(MAX),
            Category NVARCHAR(MAX),
            DataQualityScore DECIMAL(3,1),
            IsFeatured NVARCHAR(10) DEFAULT 'No',
            SelectedAt DATETIME2 DEFAULT GETDATE(),
            CreatedAt DATETIME2 DEFAULT GETDATE()
        );
        
        PRINT 'Created FinalOpportunities table structure successfully';
        """
        
        result = self.execute_sql_command(sql)
        return result is not None

    def select_high_quality_records(self):
        """Select high-quality records from Layer 2"""
        print("🎯 Selecting high-quality records from Layer 2...")
        
        sql = """
        -- Insert high-quality records from Layer 2 into Layer 3
        INSERT INTO dbo.FinalOpportunities (
            ID,
            Title,
            Summary,
            AwardValue,
            Deadline,
            Url,
            ContactEmail,
            ContactNames,
            LogoUrl,
            CoverImage,
            Description,
            Eligibility,
            ServiceProvider,
            Category,
            DataQualityScore,
            IsFeatured
        )
        SELECT 
            OpportunityNumber as ID,
            OpportunityTitle as Title,
            Summary,
            AwardValueFormatted as AwardValue,
            CloseDate as Deadline,
            'https://www.grants.gov/web/grants/view-opportunity.html?oppId=' + OpportunityNumber as Url,
            AgencyContactInfo as ContactEmail,
            ISNULL(ServiceProviderName, 'Grant Administrator') as ContactNames,
            LogoUrl,
            CoverImage,
            OpportunityDescription as Description,
            EligibilityDescription as Eligibility,
            ServiceProviderName as ServiceProvider,
            CategoryOfFundingActivity as Category,
            DataQualityScore,
            CASE 
                WHEN DataQualityScore >= 8.0 THEN 'Yes'
                ELSE 'No'
            END as IsFeatured
        FROM CleanGrantsLayer2
        WHERE ReadyForLayer3 = 1
          AND DataQualityScore >= 6.0
          AND OpportunityTitle IS NOT NULL
          AND OpportunityTitle != '';
        
        PRINT CONCAT('Selected ', @@ROWCOUNT, ' high-quality records for Layer 3');
        
        -- Get final statistics
        SELECT 
            COUNT(*) as TotalSelectedRecords,
            AVG(DataQualityScore) as AverageQualityScore,
            SUM(CASE WHEN IsFeatured = 'Yes' THEN 1 ELSE 0 END) as FeaturedRecords,
            (SELECT COUNT(*) FROM CleanGrantsLayer2 WHERE ReadyForLayer3 = 1) as TotalLayer2ReadyRecords,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM CleanGrantsLayer2 WHERE ReadyForLayer3 = 1), 1) as SelectionRate
        FROM dbo.FinalOpportunities;
        """
        
        result = self.execute_sql_command(sql)
        return result is not None

def main():
    """Main execution function"""
    print("=" * 55)
    print("🎯 Layer 3 - Simple Selection - Azure SQL Database")
    print("=" * 55)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Goal: Simple selection of high-quality records from Layer 2")
    
    try:
        selector = Layer3SimpleSelector()
        
        # Step 1: Create simple Layer 3 table
        print("\n🏗️ Step 1: Creating simple Layer 3 table structure...")
        if not selector.create_simple_layer3_table():
            print("❌ Failed to create Layer 3 table")
            return False
        
        # Step 2: Select high-quality records
        print("\n🎯 Step 2: Selecting high-quality records from Layer 2...")
        if not selector.select_high_quality_records():
            print("❌ Failed to select records from Layer 2")
            return False
        
        print("\n🎊 SUCCESS! Layer 3 Simple Selection Complete!")
        print("=" * 55)
        print("✅ Source: CleanGrantsLayer2 (comprehensive enhanced data)")
        print("✅ Target: dbo.FinalOpportunities (selected high-quality records)")
        print("✅ Selection Criteria: DataQualityScore >= 6.0, ReadyForLayer3 = 1")
        print("✅ Process: Simple field mapping and selection only")
        
        return True
        
    except Exception as e:
        print(f"\n💥 Layer 3 selection failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🏁 Starting Layer 3 Selection...")
    success = main()
    if success:
        print("\n🚀 Layer 3 Selection Successfully Completed!")
        print("📊 Your dbo.FinalOpportunities contains selected high-quality records")
        print("🎯 Ready for production use and API integration")
    else:
        print("\n❌ Layer 3 selection failed - check logs for details")
        exit(1)
LAYER3_EOF

    chmod +x "layers/layer3_final_opportunities/scripts/simple_layer3_selection.py"
    echo "✅ Layer 3 script created"
}

# Function to run Layer 2 enhancement
run_layer2() {
    echo ""
    echo "🚀 Running Layer 2 - Comprehensive Enhancement..."
    echo "=================================================="
    
    if [ ! -f "layers/layer2_clean_business_data/scripts/comprehensive_layer2_enhancement.py" ]; then
        echo "❌ Layer 2 script not found"
        return 1
    fi
    
    echo "📂 Changing to Layer 2 directory..."
    cd layers/layer2_clean_business_data/scripts || return 1
    
    echo "🐍 Running Python script..."
    if python3 comprehensive_layer2_enhancement.py; then
        cd ../../..
        echo "✅ Layer 2 enhancement completed successfully"
        return 0
    else
        cd ../../..
        echo "❌ Layer 2 enhancement failed"
        return 1
    fi
}

# Function to run Layer 3 selection
run_layer3() {
    echo ""
    echo "🎯 Running Layer 3 - Simple Selection..."
    echo "========================================"
    
    if [ ! -f "layers/layer3_final_opportunities/scripts/simple_layer3_selection.py" ]; then
        echo "❌ Layer 3 script not found"
        return 1
    fi
    
    echo "📂 Changing to Layer 3 directory..."
    cd layers/layer3_final_opportunities/scripts || return 1
    
    echo "🐍 Running Python script..."
    if python3 simple_layer3_selection.py; then
        cd ../../..
        echo "✅ Layer 3 selection completed successfully"
        return 0
    else
        cd ../../..
        echo "❌ Layer 3 selection failed"
        return 1
    fi
}

# Show usage if requested
if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    echo "🚀 Complete Azure Data Pipeline Setup & Execution"
    echo ""
    echo "Usage: $0 [option]"
    echo ""
    echo "Options:"
    echo "  setup    - Setup directory structure and create scripts only"
    echo "  layer2   - Run Layer 2 comprehensive enhancement only"
    echo "  layer3   - Run Layer 3 simple selection only"
    echo "  full     - Run complete pipeline (default)"
    echo "  --help   - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run complete pipeline"
    echo "  $0 setup             # Setup only"
    echo "  $0 layer2            # Layer 2 only"
    echo "  $0 full              # Complete pipeline"
    exit 0
fi

# Main execution
echo ""
echo "🏗️ Step 1: Setting up directory structure..."
mkdir -p "layers/layer1_raw_data/scripts"
mkdir -p "layers/layer2_clean_business_data/scripts"
mkdir -p "layers/layer3_final_opportunities/scripts"
mkdir -p "backup/$(date +%Y%m%d_%H%M%S)"
echo "✅ Directory structure ready"

echo ""
echo "📝 Step 2: Creating pipeline scripts..."
create_layer2_script
create_layer3_script

echo ""
echo "📋 Step 3: Creating documentation..."
cat > "PIPELINE_ARCHITECTURE.md" << 'DOC_END'
# Azure Data Pipeline Architecture

## 🏗️ Three-Layer Architecture

### Layer 1: Raw Data Ingestion
- **Table**: `RawGrantsLayer1`
- **Purpose**: Raw API data from Grants.gov

### Layer 2: Comprehensive Enhancement  
- **Table**: `CleanGrantsLayer2`
- **Purpose**: Complete data enhancement and quality scoring
- **Features**: Visual assets, summaries, formatted values, quality scores

### Layer 3: Simple Selection
- **Table**: `FinalOpportunities`
- **Purpose**: Selection of high-quality records for production
- **Criteria**: DataQualityScore >= 6.0, ReadyForLayer3 = 1

## 🔄 Execution Flow
```
Raw Data → Enhanced Data → Selected Output
Layer 1  →    Layer 2    →    Layer 3
```

## 🎯 Quality Scoring (0-10 scale)
- **8.0+**: Excellent - Production Ready
- **6.0-7.9**: Good - Enhanced and ready
- **< 6.0**: Needs improvement

## 🚀 Azure SQL Database Integration
- Optimized for Azure SQL Database
- Uses sqlcmd for reliable connections
- Proper error handling and timeouts
- Transaction-based operations

## 📊 Data Flow
1. **Layer 1**: Raw data ingestion from Grants.gov API
2. **Layer 2**: Comprehensive enhancement with quality scoring
3. **Layer 3**: Simple selection of production-ready records

## 🎯 Benefits
- Clean separation of concerns
- Quality-driven data processing
- Azure-optimized performance
- Flexible selection criteria
DOC_END

echo "✅ Documentation created"

# Execute based on argument
case "${1:-full}" in
    "setup")
        echo ""
        echo "🎊 Setup completed successfully!"
        echo "📋 Run '$0 full' to execute pipeline"
        ;;
    "layer2")
        if run_layer2; then
            echo ""
            echo "🎊 Layer 2 completed successfully!"
        else
            echo ""
            echo "❌ Layer 2 failed"
            exit 1
        fi
        ;;
    "layer3")
        if run_layer3; then
            echo ""
            echo "🎊 Layer 3 completed successfully!"
        else
            echo ""
            echo "❌ Layer 3 failed" 
            exit 1
        fi
        ;;
    "full"|*)
        echo ""
        echo "🚀 Running complete pipeline..."
        
        if run_layer2; then
            if run_layer3; then
                echo ""
                echo "🎊 COMPLETE SUCCESS! Full Pipeline Executed!"
                echo "=============================================="
                echo "✅ Layer 2: CleanGrantsLayer2 (comprehensive enhancement)"
                echo "✅ Layer 3: FinalOpportunities (selected high-quality records)"
                echo "📊 Your data is ready for production use!"
                echo ""
                echo "💡 Quick checks:"
                echo "-- View Layer 2 enhanced records"
                echo "SELECT TOP 10 * FROM CleanGrantsLayer2 WHERE ReadyForLayer3 = 1 ORDER BY DataQualityScore DESC;"
                echo ""
                echo "-- View final selected opportunities"
                echo "SELECT TOP 10 * FROM dbo.FinalOpportunities ORDER BY DataQualityScore DESC;"
                echo ""
                echo "-- Get summary statistics"
                echo "SELECT COUNT(*) as TotalFinal, AVG(DataQualityScore) as AvgQuality FROM dbo.FinalOpportunities;"
            else
                echo ""
                echo "❌ Layer 3 failed"
                exit 1
            fi
        else
            echo ""
            echo "❌ Layer 2 failed"
            exit 1
        fi
        ;;
esac

echo ""
echo "🎯 Script execution completed at: $(date)"
echo "✅ Azure Data Pipeline Ready!"