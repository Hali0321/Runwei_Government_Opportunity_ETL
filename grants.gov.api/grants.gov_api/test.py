#!/usr/bin/env python3
"""
Main entry point for the Grants.gov API project.
This script allows running various commands without having to specify the full path.
"""

import sys
import os
from grants_gov.cli.main import main as etl_main
from grants_gov.cli.query_db import main as query_main

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        # Run database query
        sys.argv.pop(1)  # Remove 'query' argument
        query_main()
    else:
        # Run ETL by default
        etl_main()