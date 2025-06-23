#!/usr/bin/env python3
"""
Azure Functions Daily Trigger
Scheduled function to run the complete grants pipeline daily
"""

import azure.functions as func
import logging
import subprocess
import sys
import os
from datetime import datetime

def main(mytimer: func.TimerRequest) -> None:
    """Azure Functions timer trigger for daily pipeline execution"""
    
    # Configure Azure Functions logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    utc_timestamp = datetime.utcnow().replace(tzinfo=None).isoformat()
    
    if mytimer.past_due:
        logger.info('The timer is past due!')

    logger.info(f'Azure Functions trigger executed at {utc_timestamp}')
    
    try:
        # Set working directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)
        
        # Execute master automation controller
        result = subprocess.run([
            sys.executable, 
            'master_automation_controller.py'
        ], timeout=7200, capture_output=True, text=True)  # 2 hour timeout
        
        if result.returncode == 0:
            logger.info("✅ Daily pipeline execution completed successfully")
            logger.info(f"Output: {result.stdout}")
        else:
            logger.error(f"❌ Daily pipeline execution failed: {result.stderr}")
            
            # Send notification (implement your notification system)
            # send_error_notification(result.stderr)
            
    except subprocess.TimeoutExpired:
        logger.error("⏰ Daily pipeline execution timed out after 2 hours")
    except Exception as e:
        logger.error(f"💥 Fatal error in Azure Functions trigger: {e}")

# Azure Functions configuration
app = func.FunctionApp()

@app.function_name(name="DailyGrantsPipelineTrigger")
@app.schedule(schedule="0 0 6 * * *", arg_name="mytimer", run_on_startup=False)
def daily_grants_pipeline_trigger(mytimer: func.TimerRequest) -> None:
    """Timer trigger: Daily at 6:00 AM UTC"""
    main(mytimer)