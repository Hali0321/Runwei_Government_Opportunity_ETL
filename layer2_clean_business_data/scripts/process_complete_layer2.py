#!/usr/bin/env python3
"""
Azure Grants.gov Layer 2 Combined Processor
Transform Layer 1 → Clean Layer 2 → Validate Production Data
"""

import subprocess
import sys
import os
from datetime import datetime

def run_layer2_pipeline():
    """Execute complete Layer 2 processing pipeline"""
    
    print("🏗️ AZURE GRANTS.GOV LAYER 2 PROCESSING PIPELINE")
    print("=" * 55)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Layer 2 processing steps
    processing_steps = [
        {
            'name': 'Transform Raw to Business Data',
            'script': 'transform_raw_to_business.py',
            'description': 'Convert Layer 1 raw data to Layer 2 business format'
        },
        {
            'name': 'Clean Text Formatting',
            'script': 'clean_text_formatting.py', 
            'description': 'Remove HTML tags and clean text formatting'
        },
        {
            'name': 'Remove Test Data',
            'script': 'remove_test_data.py',
            'description': 'Remove sample/test records for production'
        }
    ]
    
    print("\n🔄 Executing Layer 2 processing steps...")
    
    success_count = 0
    total_steps = len(processing_steps)
    
    for i, step in enumerate(processing_steps, 1):
        print(f"\n📋 Step {i}/{total_steps}: {step['name']}")
        print(f"   📝 {step['description']}")
        
        try:
            script_path = f"layer2_clean_business_data/scripts/{step['script']}"
            if os.path.exists(script_path):
                result = subprocess.run([sys.executable, script_path], 
                                      capture_output=True, text=True, timeout=300)
                
                if result.returncode == 0:
                    print(f"   ✅ {step['name']} completed successfully")
                    success_count += 1
                else:
                    print(f"   ❌ {step['name']} failed: {result.stderr}")
            else:
                print(f"   ⚠️ Script not found: {script_path}")
                
        except Exception as e:
            print(f"   ❌ Error executing {step['name']}: {e}")
    
    # Final summary
    print(f"\n📊 LAYER 2 PROCESSING SUMMARY")
    print("=" * 35)
    print(f"✅ Successful steps: {success_count}/{total_steps}")
    print(f"📅 Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if success_count == total_steps:
        print("🏆 LAYER 2 PROCESSING COMPLETED SUCCESSFULLY!")
        print("🚀 Layer 2 data is now production-ready!")
    else:
        print(f"⚠️ {total_steps - success_count} steps had issues")
        print("🔍 Check individual step outputs for details")
    
    return success_count == total_steps

if __name__ == "__main__":
    success = run_layer2_pipeline()
    sys.exit(0 if success else 1)
