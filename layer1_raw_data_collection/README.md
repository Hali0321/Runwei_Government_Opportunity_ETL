# Layer 1: Raw Data Collection

## Purpose
Collect data from Grants.gov and import into Azure SQL Database as raw, unprocessed data.

## Scripts
- `collect_grants_from_website.py` - Scrape Grants.gov using Selenium
- `import_storage_to_layer1.py` - Import from Azure Storage to SQL Layer 1

## Data Flow
Grants.gov Website → Azure Table Storage → RawGrantsLayer1 (SQL)

## Usage
```bash
# Collect fresh data
python layer1_raw_data_collection/scripts/collect_grants_from_website.py

# Import to Layer 1
python layer1_raw_data_collection/scripts/import_storage_to_layer1.py
```
