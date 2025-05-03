#!/usr/bin/env python3

import argparse
import subprocess
import logging
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"etl_runner_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_etl_process(mode="incremental"):
    """
    Run the ETL process with appropriate parameters.
    
    Args:
        mode: Either "incremental" or "full" for incremental or full load
    """
    start_time = time.time()
    logger.info(f"Starting ETL process in {mode} mode")
    
    cmd = ["python", "main.py", "--enrich"]
    
    if mode == "full":
        cmd.append("--full-load")
        logger.info("Running full load ETL process")
    else:
        cmd.extend(["--max-results", "100"])
        logger.info("Running incremental ETL process")
    
    try:
        logger.info(f"Executing command: {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            logger.info(f"ETL process completed successfully in {time.time() - start_time:.2f} seconds")
            logger.info(f"Output: {stdout}")
            return True
        else:
            logger.error(f"ETL process failed with return code {process.returncode}")
            logger.error(f"Error: {stderr}")
            return False
    except Exception as e:
        logger.error(f"Failed to run ETL process: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the ETL process with scheduling options")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental",
                       help="ETL mode: incremental (default) or full load")
    
    args = parser.parse_args()
    
    success = run_etl_process(args.mode)
    
    if success:
        print("ETL process completed successfully")
    else:
        print("ETL process failed, check logs for details")
        exit(1)