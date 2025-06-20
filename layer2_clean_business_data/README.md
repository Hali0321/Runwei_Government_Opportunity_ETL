# Layer 2: Clean Business Data

## Purpose
Transform raw Layer 1 data into clean, business-ready format with data quality enhancements.

## Scripts
- `transform_raw_to_business.py` - Main ETL transformation
- `clean_text_formatting.py` - HTML and text cleanup
- `remove_test_data.py` - Remove sample/test records
- `process_complete_layer2.py` - **Run all Layer 2 processing**

## Data Flow
RawGrantsLayer1 → CleanGrantsLayer2 (Business-ready)

## Usage
```bash
# Process complete Layer 2 pipeline
python layer2_clean_business_data/scripts/process_complete_layer2.py

# Or run individual steps
python layer2_clean_business_data/scripts/transform_raw_to_business.py
python layer2_clean_business_data/scripts/clean_text_formatting.py
python layer2_clean_business_data/scripts/remove_test_data.py
```
