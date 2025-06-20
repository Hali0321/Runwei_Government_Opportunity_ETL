#!/usr/bin/env python3
"""
Project Organization and Cleanup Script
Removes duplicates, organizes files, and prepares for GitHub
"""

import os
import shutil
import glob
from pathlib import Path

def organize_project():
    """Organize project files and remove duplicates"""
    
    print("🧹 GRANTS.GOV PROJECT ORGANIZATION")
    print("=" * 45)
    
    # Define the optimal structure
    structure = {
        'docs': ['README.md', 'ARCHITECTURE.md', 'API_DOCS.md'],
        'src/scripts': ['sync_azure_data.py', 'transform_layer2.py', 'create_layer3_final.py'],
        'src/functions': [],  # For Azure Functions
        'src/utils': [],      # Utility modules
        'sql/schemas': ['create_raw_grants_layer1.sql', 'create_clean_grants_layer2.sql'],
        'sql/transforms': [],  # ETL transformation queries
        'sql/queries': [],     # Business logic queries
        'config': ['azure_config.json', 'database_config.json'],
        'tests': [],          # Unit tests
        '.github/workflows': ['azure_deploy.yml', 'data_pipeline.yml']
    }
    
    # Create directories
    for directory in structure.keys():
        os.makedirs(directory, exist_ok=True)
        print(f"📁 Created: {directory}")
    
    # Remove duplicate and unnecessary files
    duplicates_to_remove = [
        'transform_to_layer1.py',
        'transform_to_layer2_fixed.py', 
        'debug_*.py',
        'create_clean_grants_layer2_fixed.sql',
        '*_temp.sql',
        '*.tmp',
        'safe_transform.sql',
        'layer2_transform*.sql'
    ]
    
    print("\n🗑️ Removing duplicate and temporary files...")
    for pattern in duplicates_to_remove:
        for file in glob.glob(pattern, recursive=True):
            try:
                os.remove(file)
                print(f"   Removed: {file}")
            except FileNotFoundError:
                pass
    
    # Keep only the best version of each script
    final_scripts = {
        'transform_to_layer2_safe.py': 'src/scripts/transform_layer2.py',
        'sync_azure_data.py': 'src/scripts/sync_azure_data.py',
        'create_layer3_final.py': 'src/scripts/create_layer3_final.py'
    }
    
    for old_name, new_path in final_scripts.items():
        if os.path.exists(old_name):
            shutil.move(old_name, new_path)
            print(f"📦 Moved: {old_name} → {new_path}")
    
    print("✅ Project organization completed")

if __name__ == "__main__":
    organize_project()