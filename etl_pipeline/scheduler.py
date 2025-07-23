#!/usr/bin/env python3
"""
Local ETL Pipeline Scheduler - VS Code Testing
Runs Bronze → Silver → Gold ETL pipeline daily at 8:00 AM EST
"""

import sys
import time
import schedule
import logging
import subprocess
import threading
from datetime import datetime, timedelta
from pathlib import Path
import json
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('../logs/etl_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)

class LocalETLScheduler:
    """Local ETL Pipeline Scheduler for VS Code Testing"""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.running = False
        self.last_run_status = {}
        self.run_count = 0
        
        # Script paths (updated for new structure)
        self.scripts = {
            'bronze': self.base_dir / "layers/bronze/scripts/run_layer1.py",
            'silver': self.base_dir / "layers/silver/scripts/run_layer2.py", 
            'gold': self.base_dir / "layers/gold/scripts/run_layer3.py"
        }
        
        # Validate script paths
        self.validate_scripts()
        
        print("🎯 Local ETL Pipeline Scheduler Initialized")
        print(f"📁 Base Directory: {self.base_dir}")
        print("⏰ Schedule: Daily at 8:00 AM EST")
        print("🔄 Pipeline: Bronze → Silver → Gold")
        
    def validate_scripts(self):
        """Validate that all required scripts exist"""
        missing_scripts = []
        for layer, script_path in self.scripts.items():
            if not script_path.exists():
                missing_scripts.append(f"{layer}: {script_path}")
                
        if missing_scripts:
            print("❌ Missing ETL scripts:")
            for script in missing_scripts:
                print(f"   - {script}")
            print("\n💡 Make sure your ETL scripts are in the correct locations")
            sys.exit(1)
        else:
            print("✅ All ETL scripts found")
            
    def run_script(self, script_path, layer_name, timeout=900):  # 15 minute timeout
        """Run a single ETL script with error handling"""
        print(f"\n🔄 Running {layer_name} Layer: {script_path.name}")
        start_time = datetime.now()
        
        try:
            # Run the script
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=script_path.parent
            )
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            if result.returncode == 0:
                print(f"✅ {layer_name} Layer completed successfully in {duration}")
                if result.stdout:
                    # Show last few lines of output
                    output_lines = result.stdout.strip().split('\n')
                    for line in output_lines[-5:]:  # Show last 5 lines
                        print(f"   📋 {line}")
                        
                self.last_run_status[layer_name] = {
                    'status': 'success',
                    'duration': str(duration),
                    'timestamp': end_time.isoformat()
                }
                return True
            else:
                print(f"❌ {layer_name} Layer failed (exit code: {result.returncode})")
                if result.stderr:
                    print(f"🔴 Error: {result.stderr}")
                if result.stdout:
                    print(f"📋 Output: {result.stdout}")
                    
                self.last_run_status[layer_name] = {
                    'status': 'failed',
                    'error': result.stderr or 'Unknown error',
                    'duration': str(duration),
                    'timestamp': end_time.isoformat()
                }
                return False
                
        except subprocess.TimeoutExpired:
            print(f"⏰ {layer_name} Layer timed out after {timeout} seconds")
            self.last_run_status[layer_name] = {
                'status': 'timeout',
                'duration': f"{timeout}s (timeout)",
                'timestamp': datetime.now().isoformat()
            }
            return False
        except Exception as e:
            print(f"💥 Error running {layer_name} Layer: {e}")
            self.last_run_status[layer_name] = {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            return False
            
    def run_etl_pipeline(self):
        """Run the complete ETL pipeline: Bronze → Silver → Gold"""
        self.run_count += 1
        pipeline_start = datetime.now()
        
        print("=" * 80)
        print(f"🚀 ETL Pipeline Run #{self.run_count}")
        print(f"📅 Started: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        pipeline_success = True
        
        # Step 1: Bronze Layer (Data Collection)
        if not self.run_script(self.scripts['bronze'], 'Bronze'):
            print("❌ Bronze Layer failed - stopping pipeline")
            pipeline_success = False
            self.log_pipeline_result(pipeline_start, False, "Bronze layer failure")
            return
            
        # Step 2: Silver Layer (Data Cleaning & Transformation)
        if not self.run_script(self.scripts['silver'], 'Silver'):
            print("❌ Silver Layer failed - stopping pipeline")
            pipeline_success = False
            self.log_pipeline_result(pipeline_start, False, "Silver layer failure")
            return
            
        # Step 3: Gold Layer (Final Processing)
        if not self.run_script(self.scripts['gold'], 'Gold'):
            print("❌ Gold Layer failed")
            pipeline_success = False
            self.log_pipeline_result(pipeline_start, False, "Gold layer failure")
            return
            
        # Pipeline completed successfully
        pipeline_end = datetime.now()
        total_duration = pipeline_end - pipeline_start
        
        print("\n🎊 ETL Pipeline Completed Successfully!")
        print(f"⏱️  Total Duration: {total_duration}")
        print(f"📊 Next run tomorrow at 8:00 AM EST")
        
        self.log_pipeline_result(pipeline_start, True, "All layers completed successfully")
        
    def log_pipeline_result(self, start_time, success, message):
        """Log pipeline execution results"""
        end_time = datetime.now()
        duration = end_time - start_time
        
        result = {
            'run_number': self.run_count,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration': str(duration),
            'success': success,
            'message': message,
            'layer_status': self.last_run_status.copy()
        }
        
        # Log to file
        log_file = self.base_dir.parent / "logs/etl_pipeline_results.json"
        results = []
        
        # Load existing results
        if log_file.exists():
            try:
                with open(log_file, 'r') as f:
                    results = json.load(f)
            except:
                results = []
                
        # Add new result
        results.append(result)
        
        # Keep only last 50 runs
        results = results[-50:]
        
        # Save results
        with open(log_file, 'w') as f:
            json.dump(results, f, indent=2)
            
        # Log summary
        status_emoji = "✅" if success else "❌"
        logger.info(f"{status_emoji} Pipeline Run #{self.run_count}: {message} (Duration: {duration})")
        
    def print_status(self):
        """Print current scheduler status"""
        print("\n📊 Local ETL Scheduler Status")
        print("=" * 50)
        print(f"🔄 Running: {'Yes' if self.running else 'No'}")
        print(f"📈 Total Runs: {self.run_count}")
        
        if self.last_run_status:
            print("\n📋 Last Run Status:")
            for layer, status in self.last_run_status.items():
                status_emoji = {"success": "✅", "failed": "❌", "timeout": "⏰", "error": "💥"}.get(status['status'], "❓")
                print(f"   {status_emoji} {layer}: {status['status']} ({status.get('duration', 'N/A')})")
                
        next_run = schedule.next_run()
        if next_run:
            print(f"\n⏰ Next run: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            
    def start_scheduler(self):
        """Start the daily 8:00 AM EST scheduler"""
        self.running = True
        
        # Schedule the job daily at 8:00 AM EST
        schedule.every().day.at("08:00").do(self.run_etl_pipeline)
        
        print("\n🎯 Local ETL Scheduler Started!")
        print("⏰ Running daily at 8:00 AM EST")
        print("🛑 Press Ctrl+C to stop")
        print("💡 Check 'etl_pipeline_results.json' for detailed logs")
        
        # Run immediately on startup (optional)
        print("\n🚀 Running initial ETL pipeline...")
        self.run_etl_pipeline()
        
        # Keep the scheduler running
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\n🛑 Scheduler stopped by user")
            self.running = False
        except Exception as e:
            print(f"\n💥 Scheduler error: {e}")
            self.running = False
            
    def stop_scheduler(self):
        """Stop the scheduler"""
        self.running = False
        schedule.clear()
        print("🛑 Scheduler stopped")

def main():
    """Main execution function"""
    print("🎯 Local ETL Pipeline Scheduler for VS Code")
    print("=" * 60)
    
    # Create scheduler
    scheduler = LocalETLScheduler()
    
    # Check if we want to run once or start scheduler
    if len(sys.argv) > 1:
        if sys.argv[1] == '--once':
            print("🔄 Running ETL pipeline once...")
            scheduler.run_etl_pipeline()
            return
        elif sys.argv[1] == '--status':
            scheduler.print_status()
            return
        elif sys.argv[1] == '--help':
            print("\nUsage:")
            print("  python local_etl_scheduler.py           # Start daily 8:00 AM EST scheduler")
            print("  python local_etl_scheduler.py --once    # Run pipeline once")
            print("  python local_etl_scheduler.py --status  # Show status")
            print("  python local_etl_scheduler.py --help    # Show this help")
            return
    
    # Start the scheduler
    scheduler.start_scheduler()

if __name__ == "__main__":
    main()