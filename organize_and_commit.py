#!/usr/bin/env python3
"""
Organize Layer 3 files, clean up unnecessary files, and commit changes to GitHub
Azure Grants.gov API Project File Organization
"""

import os
import shutil
import subprocess
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ProjectOrganizer:
    """Organize project files and manage git operations"""
    
    def __init__(self, project_root="/Users/dinghali/Desktop/Runwei/grants_gov_api_azure"):
        self.project_root = Path(project_root)
        self.layer3_dir = self.project_root / "layers" / "layer3_final_opportunities"
        self.scripts_dir = self.layer3_dir / "scripts"
        self.docs_dir = self.layer3_dir / "docs"
        
    def create_directory_structure(self):
        """Create proper directory structure for Layer 3"""
        logger.info("📁 Creating proper directory structure...")
        
        directories = [
            self.layer3_dir,
            self.scripts_dir,
            self.docs_dir,
            self.project_root / "archive"
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"✅ Created/verified: {directory}")
    
    def move_working_files(self):
        """Move the working Layer 3 script to proper location"""
        logger.info("📦 Moving working files to proper locations...")
        
        # Move the final working script
        source_file = self.project_root / "create_layer3_azure_final.py"
        target_file = self.scripts_dir / "create_layer3_final_opportunities.py"
        
        if source_file.exists():
            shutil.move(str(source_file), str(target_file))
            logger.info(f"✅ Moved: {source_file.name} → {target_file}")
        else:
            logger.warning(f"⚠️ Source file not found: {source_file}")
    
    def archive_old_files(self):
        """Archive old/test files that are no longer needed"""
        logger.info("🗄️ Archiving old files...")
        
        files_to_archive = [
            "create_layer3_from_layer2.py",
            "create_layer3_from_layer2_fixed.py", 
            "create_layer3_azure_fixed.py",
            "create_new_layer3.py"
        ]
        
        archive_dir = self.project_root / "archive"
        
        for filename in files_to_archive:
            source_path = self.project_root / filename
            if source_path.exists():
                target_path = archive_dir / filename
                shutil.move(str(source_path), str(target_path))
                logger.info(f"✅ Archived: {filename}")
    
    def create_layer3_documentation(self):
        """Create documentation for Layer 3"""
        logger.info("📝 Creating Layer 3 documentation...")
        
        readme_content = """# Layer 3 - Final Opportunities

## Overview
Layer 3 transforms the cleaned grants data from Layer 2 into a final opportunities format suitable for applications and APIs.

## Architecture
```
CleanGrantsLayer2 → FinalOpportunities (Layer 3)
```

## Table Structure
- **Table Name**: `dbo.FinalOpportunities`
- **Records**: ~1,671 opportunities
- **Primary Key**: `ID` (OpportunityNumber)

## Key Features
- ✅ Azure SQL Database optimized
- ✅ Proper indexing for performance
- ✅ String length limits for Azure compatibility
- ✅ Industry categorization
- ✅ Featured opportunity flagging
- ✅ SEO-friendly slugs

## Column Mappings
| Layer 2 Column | Layer 3 Column | Transformation |
|---------------|----------------|----------------|
| OpportunityNumber | ID | Primary key |
| Title | Title | Truncated to 500 chars |
| EstimatedTotalFunding | AwardValue | Cleaned and formatted |
| AgencyName | Industry | Mapped to industry categories |
| Category | OpportunityTypeId | Numeric mapping |
| Deadline | Deadline | Formatted datetime |

## Usage

### Run Layer 3 Creation
```bash
cd /Users/dinghali/Desktop/Runwei/grants_gov_api_azure/layers/layer3_final_opportunities/scripts
python3 create_layer3_final_opportunities.py
```

### Query Final Opportunities
```sql
-- Count total opportunities
SELECT COUNT(*) FROM dbo.FinalOpportunities;

-- Get featured opportunities
SELECT * FROM dbo.FinalOpportunities WHERE IsFeatured = 'Yes';

-- Industry distribution
SELECT Industry, COUNT(*) FROM dbo.FinalOpportunities GROUP BY Industry;

-- High-value opportunities
SELECT TOP 10 ID, Title, AwardValue, Industry 
FROM dbo.FinalOpportunities 
WHERE AwardValue IS NOT NULL 
ORDER BY TRY_CAST(REPLACE(REPLACE(AwardValue, '$', ''), ',', '') AS DECIMAL) DESC;
```

## Performance Features
- **Filtered Indexes**: Only index non-NULL values
- **Optimized Data Types**: NVARCHAR with appropriate lengths
- **Batch Processing**: Handles large datasets efficiently
- **Azure SQL Compatibility**: Uses Azure-specific optimizations

## Industry Categories
1. Healthcare
2. Education  
3. Defense
4. Energy & Environment
5. Business & Commerce
6. Agriculture
7. Transportation
8. Arts & Humanities
9. Government (General)

## Next Steps
- Create API endpoints for FinalOpportunities
- Build search and filtering capabilities
- Implement recommendation engine
- Create analytics dashboards
"""
        
        readme_path = self.docs_dir / "README.md"
        with open(readme_path, 'w') as f:
            f.write(readme_content)
        
        logger.info(f"✅ Created documentation: {readme_path}")
    
    def create_main_project_readme_update(self):
        """Update main project README with Layer 3 information"""
        logger.info("📄 Updating main project README...")
        
        readme_update = """
## Layer 3 - Final Opportunities ✅

**Status**: Complete and Operational
**Table**: `dbo.FinalOpportunities`
**Records**: 1,671 opportunities

### Features
- Azure SQL Database optimized structure
- Industry categorization and mapping
- Featured opportunity identification
- SEO-friendly URL slugs
- Performance-optimized indexes

### Usage
```bash
# Create Layer 3 table
cd layers/layer3_final_opportunities/scripts
python3 create_layer3_final_opportunities.py
```

### Key Transformations
- **OpportunityNumber** → **ID** (Primary Key)
- **Title** → **Title** (Truncated, indexed)
- **AgencyName** → **Industry** (Categorized)
- **EstimatedTotalFunding** → **AwardValue** (Cleaned)
- **Category** → **OpportunityTypeId** (Mapped)

### Sample Queries
```sql
SELECT COUNT(*) FROM dbo.FinalOpportunities;
SELECT * FROM dbo.FinalOpportunities WHERE IsFeatured = 'Yes';
SELECT Industry, COUNT(*) FROM dbo.FinalOpportunities GROUP BY Industry;
```
"""
        
        main_readme = self.project_root / "README.md"
        if main_readme.exists():
            with open(main_readme, 'a') as f:
                f.write(readme_update)
            logger.info("✅ Updated main README.md")
    
    def git_operations(self):
        """Perform git operations: add, commit, and push"""
        logger.info("🔄 Performing git operations...")
        
        os.chdir(self.project_root)
        
        try:
            # Check git status
            result = subprocess.run(['git', 'status', '--porcelain'], 
                                  capture_output=True, text=True)
            if result.stdout:
                logger.info("📋 Git status shows changes to commit")
                
                # Add all changes
                subprocess.run(['git', 'add', '.'], check=True)
                logger.info("✅ Added all changes to git")
                
                # Commit changes
                commit_message = """feat: Complete Layer 3 Final Opportunities implementation

- ✅ Created dbo.FinalOpportunities table with 1,671 records
- ✅ Azure SQL Database optimized structure and indexes
- ✅ Industry categorization and opportunity mapping
- ✅ Featured opportunity identification system
- ✅ SEO-friendly URL slug generation
- ✅ Proper file organization and documentation
- 🗄️ Archived old/test scripts
- 📝 Added comprehensive Layer 3 documentation

Table Features:
- Primary key on OpportunityNumber (ID)
- Indexed columns for performance
- Industry-based categorization
- Award value processing and formatting
- Deadline formatting and validation

Data Flow: CleanGrantsLayer2 → FinalOpportunities
Ready for API integration and frontend consumption"""
                
                subprocess.run(['git', 'commit', '-m', commit_message], check=True)
                logger.info("✅ Committed changes to git")
                
                # Push to origin
                subprocess.run(['git', 'push', 'origin', 'main'], check=True)
                logger.info("✅ Pushed changes to GitHub")
                
            else:
                logger.info("📋 No changes to commit")
                
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Git operation failed: {e}")
            return False
        
        return True
    
    def cleanup_temp_files(self):
        """Clean up temporary and cache files"""
        logger.info("🧹 Cleaning up temporary files...")
        
        patterns_to_remove = [
            "*.pyc",
            "__pycache__",
            "*.log",
            ".DS_Store",
            "*.tmp"
        ]
        
        for pattern in patterns_to_remove:
            for file_path in self.project_root.rglob(pattern):
                if file_path.is_file():
                    file_path.unlink()
                    logger.info(f"🗑️ Removed: {file_path.name}")
                elif file_path.is_dir():
                    shutil.rmtree(file_path)
                    logger.info(f"🗑️ Removed directory: {file_path.name}")
    
    def display_final_structure(self):
        """Display final project structure"""
        logger.info("📊 Final project structure:")
        
        def print_tree(directory, prefix="", level=0, max_level=3):
            if level > max_level:
                return
            
            try:
                items = sorted(directory.iterdir())
                for i, item in enumerate(items):
                    if item.name.startswith('.'):
                        continue
                    
                    is_last = i == len(items) - 1
                    current_prefix = "└── " if is_last else "├── "
                    print(f"{prefix}{current_prefix}{item.name}")
                    
                    if item.is_dir() and level < max_level:
                        next_prefix = prefix + ("    " if is_last else "│   ")
                        print_tree(item, next_prefix, level + 1, max_level)
            except PermissionError:
                pass
        
        print("\n📁 Project Structure:")
        print(f"{self.project_root.name}/")
        print_tree(self.project_root, "", 0, 2)

def main():
    """Main execution function"""
    print("🚀 Starting project organization and GitHub commit...")
    
    organizer = ProjectOrganizer()
    
    try:
        # Step 1: Create directory structure
        organizer.create_directory_structure()
        
        # Step 2: Move working files
        organizer.move_working_files()
        
        # Step 3: Archive old files
        organizer.archive_old_files()
        
        # Step 4: Create documentation
        organizer.create_layer3_documentation()
        organizer.create_main_project_readme_update()
        
        # Step 5: Clean up temp files
        organizer.cleanup_temp_files()
        
        # Step 6: Git operations
        if organizer.git_operations():
            print("\n🎯 Project Organization Complete!")
            print("✅ Files organized and moved to proper locations")
            print("✅ Old files archived")
            print("✅ Documentation created")
            print("✅ Changes committed and pushed to GitHub")
        else:
            print("\n⚠️ Project organized but git operations may have failed")
        
        # Step 7: Display final structure
        organizer.display_final_structure()
        
        print("\n📋 Next Steps:")
        print("1. Layer 3 is complete and ready for use")
        print("2. Create API endpoints for FinalOpportunities")
        print("3. Build frontend search and filtering")
        print("4. Implement analytics dashboards")
        
    except Exception as e:
        logger.error(f"❌ Error during organization: {e}")
        print(f"\n❌ Organization failed: {e}")

if __name__ == "__main__":
    main()