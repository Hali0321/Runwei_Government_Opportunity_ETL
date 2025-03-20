import os
import requests
import zipfile
import json
import datetime
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import xmltodict  # For XML to JSON conversion
import traceback  # Adding explicit import for traceback

# Set the base URL for Grants.gov database extracts
BASE_URL = "https://prod-grants-gov-chatbot.s3.amazonaws.com/extracts/"

# Function to download a specific file by date
def download_specific_file(date_str):
    """
    Download a specific file from Grants.gov by date string (YYYYMMDD)
    """
    zip_url = f"{BASE_URL}GrantsDBExtract{date_str}v2.zip"
    zip_filename = f"GrantsDBExtract{date_str}v2.zip"
    
    print(f"Attempting to download: {zip_url}")
    
    try:
        # Download the file
        response = requests.get(zip_url, stream=True)
        response.raise_for_status()  # Raise exception for bad status codes
        
        # Save the zip file
        with open(zip_filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        
        print(f"Successfully downloaded: {zip_filename} ({os.path.getsize(zip_filename) / (1024*1024):.1f} MB)")
        return zip_filename
    
    except requests.exceptions.RequestException as e:
        print(f"Download failed: {e}")
        return None

def extract_xml_from_zip_to_json(zip_filename):
    """
    Extract XML data from the downloaded zip file and convert to JSON
    """
    try:
        # Check if zip file exists
        if not os.path.exists(zip_filename):
            print(f"Error: {zip_filename} does not exist")
            return None
            
        # Extract XML from zip
        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            # Print all files in the zip for debugging
            file_list = zip_ref.namelist()
            print(f"Files in the zip archive: {file_list}")
            
            # Find XML files
            xml_files = [f for f in file_list if f.endswith('.xml')]
            
            if not xml_files:
                print("No XML files found in the zip archive")
                return None
                
            # Extract the first XML file
            xml_filename = xml_files[0]
            print(f"Extracting: {xml_filename}")
            zip_ref.extract(xml_filename)
            
            # Load the XML data and convert to JSON with ENHANCED attribute handling
            with open(xml_filename, 'r', encoding='utf-8') as f:
                xml_data = f.read()
                
                # Use more sophisticated xmltodict options for better XML parsing
                json_data = xmltodict.parse(
                    xml_data, 
                    attr_prefix='',               # Don't prefix attributes
                    cdata_key='text',             # Handle CDATA sections
                    dict_constructor=dict,
                    postprocessor=None,           # No post-processing to preserve structure
                    force_list=('OpportunitySynopsisDetail_1_0',)  # Force these tags to be lists
                )
                
                # Save JSON data to file for reference
                json_filename = xml_filename.replace('.xml', '.json')
                with open(json_filename, 'w', encoding='utf-8') as json_file:
                    json.dump(json_data, json_file, indent=2)
                    
                print(f"Successfully converted XML to JSON and saved as {json_filename}")
                
                # Find the grant records with enhanced search
                grants_data = None
                
                # Try OpportunitySynopsisDetail_1_0 path specifically first (based on your logs)
                if ('Grants' in json_data and 
                    'OpportunitySynopsisDetail_1_0' in json_data['Grants']):
                    grants_data = json_data['Grants']['OpportunitySynopsisDetail_1_0']
                    print(f"Found grant data at Grants.OpportunitySynopsisDetail_1_0")
                
                # If that specific path didn't work, continue with broader search
                if not grants_data:
                    # Try standard paths first
                    if 'Grants' in json_data:
                        grants_container = json_data['Grants']
                        print("Found 'Grants' container")
                        
                        # Check common field names
                        possible_keys = ['Grant', 'Opportunity', 'FundingOpportunity', 
                                        'OpportunityList', 'OpportunitySynopsisDetail_1_0']
                        for key in possible_keys:
                            if key in grants_container:
                                grants_data = grants_container[key]
                                print(f"Found grants in Grants.{key}")
                                break
                
                # Ensure we have a list (even if there's only one grant)
                if grants_data and not isinstance(grants_data, list):
                    print("Converting grants_data to list")
                    grants_data = [grants_data]
                
                if grants_data:
                    print(f"Found {len(grants_data)} grant records")
                    
                    # Inspect first few records for the specifically problematic fields
                    print("\nInspecting first record for problematic fields:")
                    problem_fields = [
                        'CategoryOfFundingActivity', 'AdditionalInformationOnEligibility',
                        'PostDate', 'CloseDate', 'LastUpdatedDate',
                        'EstimatedTotalProgramFunding', 'ExpectedNumberOfAwards',
                        'CostSharingOrMatchingRequirement', 'ArchiveDate',
                        'GrantorContactEmail', 'GrantorContactEmailDescription', 'GrantorContactText'
                    ]
                    
                    # Examine first record in detail - print all fields and their values
                    first_record = grants_data[0]
                    print("\nFirst record field details:")
                    for field in problem_fields:
                        if field in first_record:
                            value = first_record[field]
                            print(f"  - {field}: {value} (type: {type(value)})")
                        else:
                            print(f"  - {field}: NOT FOUND in record")
                    
                    return grants_data
                else:
                    print("Could not identify grant data in the JSON structure")
                    return None
    
    except zipfile.BadZipFile:
        print(f"Error: {zip_filename} is not a valid zip file")
    except Exception as e:
        print(f"Error extracting data: {str(e)}")
        traceback.print_exc()
    
    return None

def process_data(data):
    """
    Process and transform the JSON data to match our database schema
    With specific focus on preserving original values for problematic fields
    """
    if not data or len(data) == 0:
        print("No data to process")
        return pd.DataFrame()
    
    # Before normalizing, extract a sample record for debugging
    sample_record = data[0]
    print("\nRaw sample record format before normalization:")
    for k, v in sample_record.items():
        if k in ['CategoryOfFundingActivity', 'AdditionalInformationOnEligibility', 
                'PostDate', 'CloseDate', 'LastUpdatedDate',
                'EstimatedTotalProgramFunding', 'ExpectedNumberOfAwards',
                'CostSharingOrMatchingRequirement', 'ArchiveDate',
                'GrantorContactEmail', 'GrantorContactEmailDescription', 'GrantorContactText']:
            print(f"  {k}: {v} (type: {type(v)}) {'NESTED' if isinstance(v, dict) else ''}")
            if isinstance(v, dict):
                print(f"    Nested structure: {v}")
    
    # Better handle nested structures - create custom normalized data
    processed_data = []
    for record in data:
        flat_record = {}
        
        # Copy all basic fields directly
        for k, v in record.items():
            if not isinstance(v, dict) and not isinstance(v, list):
                flat_record[k] = v
            elif isinstance(v, dict):
                # Handle nested dictionary - extract text or value
                if 'text' in v:  # Most common pattern in XML->JSON conversion
                    flat_record[k] = v['text']
                elif '#text' in v:  # Alternative pattern
                    flat_record[k] = v['#text']
                else:
                    # Just take the first value if no text field
                    for nested_k, nested_v in v.items():
                        flat_record[f"{k}_{nested_k}"] = nested_v
                        break
        
        processed_data.append(flat_record)
    
    # Convert to DataFrame
    df = pd.DataFrame(processed_data)
    
    print(f"Custom normalized DataFrame has {len(df)} rows and {len(df.columns)} columns")
    
    # Create a new DataFrame with our schema columns
    processed_df = pd.DataFrame()
    
    # Print the columns in the original dataframe for debugging
    print("\nOriginal DataFrame columns:")
    for col in df.columns:
        print(f"  - {col}")
        # Print a sample for problem columns
        if any(prob in col.lower() for prob in ['categ', 'elig', 'date', 'award', 'fund', 'cost', 'email', 'contact']):
            sample = df[col].head(2).tolist()
            print(f"      Sample: {sample}")
    
    # Updated detailed column mapping
    column_mapping = {
        # Core fields
        'OpportunityID': 'opportunity_id',
        'OpportunityTitle': 'opportunity_title',
        'OpportunityNumber': 'opportunity_number',
        'OpportunityCategory': 'opportunity_category',
        'FundingInstrumentType': 'funding_instrument_type',
        
        # Problematic fields with all possible variants
        'CategoryOfFundingActivity': 'category_of_funding',
        'AdditionalInformationOnEligibility': 'eligibility_info',
        'PostDate': 'post_date',
        'CloseDate': 'close_date',
        'LastUpdatedDate': 'last_updated_date',
        'EstimatedTotalProgramFunding': 'estimated_funding',
        'ExpectedNumberOfAwards': 'expected_awards',
        'CostSharingOrMatchingRequirement': 'cost_sharing_required',
        'ArchiveDate': 'archive_date',
        'GrantorContactEmail': 'grantor_email',
        'GrantorContactEmailDescription': 'grantor_email_desc',
        'GrantorContactText': 'grantor_contact',
        
        # Continue with other standard fields
        'CategoryExplanation': 'category_explanation',
        'CFDANumbers': 'cfda_numbers',
        'EligibleApplicants': 'eligible_applicants',
        'AgencyCode': 'agency_code',
        'AgencyName': 'agency_name',
        'AwardCeiling': 'award_ceiling',
        'AwardFloor': 'award_floor',
        'Description': 'description'
    }
    
    # Map columns
    for source_col, target_col in column_mapping.items():
        if source_col in df.columns:
            processed_df[target_col] = df[source_col]
            print(f"Direct mapping: {source_col} -> {target_col}")
            
            # Show sample values for problematic fields
            if target_col in ['category_of_funding', 'eligibility_info', 'post_date', 
                            'close_date', 'last_updated_date', 'estimated_funding', 
                            'expected_awards', 'cost_sharing_required', 'archive_date', 
                            'grantor_email', 'grantor_email_desc', 'grantor_contact']:
                print(f"  Sample values for {target_col}: {df[source_col].head(3).tolist()}")
                print(f"  NULL count: {df[source_col].isna().sum()} of {len(df)}")
    
    # Convert date columns properly - using custom date parser
    date_columns = ['post_date', 'close_date', 'last_updated_date', 'archive_date']
    for col in date_columns:
        if col in processed_df.columns:
            try:
                # Save original values before conversion for debugging
                original_values = processed_df[col].head(5).tolist()
                print(f"Original {col} values: {original_values}")
                
                # Parse dates with custom format MMDDYYYY
                def parse_grants_date(date_str):
                    if not date_str or not isinstance(date_str, str):
                        return None
                        
                    if len(date_str) == 8:
                        # Format: MMDDYYYY
                        try:
                            month = int(date_str[0:2])
                            day = int(date_str[2:4])
                            year = int(date_str[4:8])
                            
                            # Simple validation
                            if 1 <= month <= 12 and 1 <= day <= 31:
                                import datetime
                                return datetime.date(year, month, day)
                        except (ValueError, TypeError):
                            pass
                    return None
                
                # Apply custom parsing
                processed_df[col] = processed_df[col].apply(parse_grants_date)
                print(f"Converted {col} to date format using custom parser")
                
                # Check for nulls after conversion
                null_count = processed_df[col].isna().sum() 
                if null_count > 0:
                    print(f"WARNING: {null_count} null values in {col} after conversion")
                    
            except Exception as e:
                print(f"Error converting {col} to date: {e}")
                traceback.print_exc()
    
    # Convert numeric columns correctly
    numeric_columns = ['award_ceiling', 'award_floor', 'estimated_funding', 'expected_awards']
    for col in numeric_columns:
        if col in processed_df.columns:
            try:
                # Save original values for debugging
                original_values = processed_df[col].head(5).tolist()
                print(f"Original {col} values: {original_values}")
                
                # Handle text/string values with $ and commas
                if processed_df[col].dtype == 'object':
                    processed_df[col] = processed_df[col].astype(str).str.replace('$', '', regex=False)
                    processed_df[col] = processed_df[col].str.replace(',', '', regex=False)
                
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')
                print(f"Converted {col} to numeric format")
                
                # Check for nulls after conversion
                null_count = processed_df[col].isna().sum() 
                if null_count > 0:
                    print(f"WARNING: {null_count} null values in {col} after conversion")
            except Exception as e:
                print(f"Error converting {col} to numeric: {e}")
    
    # Handle boolean cost_sharing_required field correctly
    if 'cost_sharing_required' in processed_df.columns:
        try:
            # Save original values for debugging
            original_values = processed_df['cost_sharing_required'].head(5).tolist()
            print(f"Original cost_sharing_required values: {original_values}")
            
            # Convert Yes/No text to boolean properly
            processed_df['cost_sharing_required'] = processed_df['cost_sharing_required'].apply(
                lambda x: True if isinstance(x, str) and x.lower() in ('yes', 'true', 'y', '1') 
                        else False if isinstance(x, str) and x.lower() in ('no', 'false', 'n', '0')
                        else x
            )
            print("Converted cost_sharing_required to boolean format")
        except Exception as e:
            print(f"Error converting cost_sharing_required: {e}")
    
    # Final check of all column data with stats
    print("\nFinal processed data statistics:")
    for col in processed_df.columns:
        non_null = processed_df[col].count()
        null_pct = (len(processed_df) - non_null) / len(processed_df) * 100
        if non_null > 0:
            if pd.api.types.is_numeric_dtype(processed_df[col]):
                print(f"  {col}: {non_null} non-null ({null_pct:.1f}% null), range: {processed_df[col].min()} to {processed_df[col].max()}")
            else:
                print(f"  {col}: {non_null} non-null ({null_pct:.1f}% null), sample: {processed_df[col].dropna().head(1).tolist()}")
        else:
            print(f"  {col}: ALL NULL")
    
    return processed_df

def store_in_postgres(data_frame):
    """
    Store the processed dataframe into PostgreSQL
    """
    try:
        # Create database connection with correct connection string format
        username = "postgres"
        password = "Ding86000688"
        host = "localhost"
        port = "5432"
        database = "grants_db"
        
        # Construct proper connection string
        connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        
        # Create engine with a specific SQLAlchemy URL format
        from sqlalchemy.engine import URL
        from sqlalchemy import text, inspect
        
        engine = create_engine(connection_string)
        inspector = inspect(engine)
        
        # Check if table exists first using the inspector
        table_exists = inspector.has_table('grants_data')
        
        # Get existing IDs if table exists
        if table_exists:
            with engine.connect() as connection:
                result = connection.execute(text("SELECT opportunity_id FROM grants_data"))
                existing_ids = [str(row[0]) for row in result]
                existing_id_set = set(existing_ids)
        else:
            existing_id_set = set()
            
        # Identify new and existing records
        data_frame['exists_in_db'] = data_frame['opportunity_id'].astype(str).isin(existing_id_set)
        new_records = data_frame[~data_frame['exists_in_db']].drop(columns=['exists_in_db'])
        existing_records = data_frame[data_frame['exists_in_db']].drop(columns=['exists_in_db'])
        
        print(f"Found {len(new_records)} new records and {len(existing_records)} existing records")
        
        # Insert new records - use a direct connection approach rather than to_sql
        if not new_records.empty:
            # Create table if it doesn't exist (this will only run the first time)
            if not table_exists:
                print("Creating table for the first time...")
                # The table schema is defined in ensure_postgres_table, so we don't need to recreate it
                # It should be created before this function is called
            
            # Use a more direct method to insert data in batches
            batch_size = 1000
            total_batches = (len(new_records) + batch_size - 1) // batch_size
            records_inserted = 0
            
            print(f"Inserting {len(new_records)} records in {total_batches} batches...")
            
            # Insert records in batches to avoid memory issues
            for i in range(0, len(new_records), batch_size):
                batch = new_records.iloc[i:i+batch_size]
                
                # Convert DataFrame to a list of dictionaries for SQLAlchemy
                records = batch.to_dict(orient='records')
                
                with engine.begin() as conn:
                    # Use SQLAlchemy Core for bulk insert
                    from sqlalchemy import Table, MetaData
                    from sqlalchemy.dialects.postgresql import insert
                    
                    metadata = MetaData()
                    # Reflect the existing table
                    grants_table = Table('grants_data', metadata, autoload_with=engine)
                    
                    # Perform the insert
                    conn.execute(grants_table.insert(), records)
                
                records_inserted += len(batch)
                print(f"Batch {i//batch_size + 1}/{total_batches} complete, total {records_inserted} inserted")
            
            print(f"Successfully inserted {records_inserted} new records")
        
        # Update existing records
        if not existing_records.empty:
            print(f"Updating {len(existing_records)} existing records with fresh data")
            
            from sqlalchemy import text
            # Use a transaction for updating all records
            with engine.begin() as conn:
                for _, row in existing_records.iterrows():
                    # Build update SQL dynamically based on non-null values
                    update_cols = []
                    update_values = {}
                    
                    for col, value in row.items():
                        if pd.notna(value) and col != 'opportunity_id':  # Skip NaN and opportunity_id
                            update_cols.append(f"{col} = :{col}")
                            update_values[col] = value
                    
                    update_values['opportunity_id'] = row['opportunity_id']
                    
                    if update_cols:  # Only perform update if we have columns to update
                        update_sql = f"UPDATE grants_data SET {', '.join(update_cols)}, updated_at = CURRENT_TIMESTAMP WHERE opportunity_id = :opportunity_id"
                        conn.execute(text(update_sql), update_values)
            
            print("All existing records updated")
        
        # In the store_in_postgres function, where you verify updated records:
        if not existing_records.empty:
            # Use a direct connection approach for verification
            with engine.connect() as conn:
                sample_ids = existing_records.head(3)['opportunity_id'].tolist()
                sample_ids_str = ','.join([f"'{id}'" for id in sample_ids])
                
                sample_sql = f"""
                    SELECT opportunity_id, category_of_funding, eligibility_info, post_date, 
                            grantor_email, grantor_contact, updated_at 
                    FROM grants_data 
                    WHERE opportunity_id IN ({sample_ids_str})
                """
                
                result = conn.execute(text(sample_sql))
                columns = result.keys()
                rows = result.fetchall()
                
                print("\nSample of updated records:")
                for row in rows:
                    print(dict(zip(columns, row)))
        
        return True
        
    except Exception as e:
        print(f"Error storing data in PostgreSQL: {e}")
        traceback.print_exc()
        return False

def reset_database():
    """Drop and recreate the grants_data table to start fresh"""
    try:
        username = "postgres"
        password = "Ding86000688"
        host = "localhost"
        port = "5432"
        database = "grants_db"
        
        connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        
        # Use engine.begin() instead of engine.connect() to handle transactions automatically
        with engine.begin() as connection:
            from sqlalchemy.sql import text
            print("Dropping existing grants_data table...")
            connection.execute(text("DROP TABLE IF EXISTS grants_data"))
            # No need for explicit commit with begin()
            print("Table dropped successfully")
        
        # Now ensure the schema is created fresh
        ensure_postgres_table()
        return True
    except Exception as e:
        print(f"Error resetting database: {e}")
        traceback.print_exc()
        return False
    
def ensure_postgres_table():
    """Ensure the grants_data table exists in PostgreSQL with the correct schema"""
    try:
        # Create database connection
        username = "postgres"
        password = "Ding86000688"
        host = "localhost" 
        port = "5432"
        database = "grants_db"
        
        connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        
        # SQL to create the table with both auto-incrementing ID and original opportunity_id
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS grants_data (
            id SERIAL PRIMARY KEY,
            opportunity_id VARCHAR(255) UNIQUE,
            opportunity_title TEXT,
            opportunity_number VARCHAR(255),
            opportunity_category VARCHAR(50),
            funding_instrument_type VARCHAR(50),
            category_of_funding VARCHAR(50),
            category_explanation TEXT,
            cfda_numbers VARCHAR(255),
            eligible_applicants VARCHAR(255),
            eligibility_info TEXT,
            agency_code VARCHAR(50),
            agency_name VARCHAR(255),
            post_date DATE,
            close_date DATE,
            last_updated_date DATE,
            award_ceiling NUMERIC,
            award_floor NUMERIC,
            estimated_funding NUMERIC,
            expected_awards NUMERIC,
            description TEXT,
            cost_sharing_required BOOLEAN,
            archive_date DATE,
            grantor_email TEXT,
            grantor_email_desc TEXT,
            grantor_contact TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        create_indexes_sql = """
        CREATE INDEX IF NOT EXISTS idx_opportunity_number ON grants_data(opportunity_number);
        CREATE INDEX IF NOT EXISTS idx_agency_code ON grants_data(agency_code);
        CREATE INDEX IF NOT EXISTS idx_post_date ON grants_data(post_date);
        CREATE INDEX IF NOT EXISTS idx_close_date ON grants_data(close_date)
        """
        
        # Execute the SQL - using begin() for proper transaction handling
        with engine.begin() as connection:
            from sqlalchemy.sql import text
            connection.execute(text(create_table_sql))
            connection.execute(text(create_indexes_sql))
            # No need for explicit commit with begin()
            print("Verified database schema exists with auto-incrementing ID and original opportunity_id")
        
        return True
    except Exception as e:
        print(f"Error ensuring database schema: {e}")
        traceback.print_exc()
        return False

# Main function to run the entire ETL process
def run_etl(date_str):
    """
    Run the ETL process for a specific date
    """
    print(f"Starting ETL process for date {date_str}...")
    
    # Ensure the database table is ready
    ensure_postgres_table()

    # Download the zip file
    zip_filename = download_specific_file(date_str)
    if not zip_filename:
        print("Download failed, stopping ETL process.")
        return False
    
    # Extract and load XML data, converting to JSON
    json_data = extract_xml_from_zip_to_json(zip_filename)
    if not json_data:
        print("Data extraction failed, stopping ETL process.")
        return False
    
    # Process the data
    processed_data = process_data(json_data)
    if processed_data.empty:
        print("Data processing failed, stopping ETL process.")
        return False
    
    # Store in PostgreSQL
    result = store_in_postgres(processed_data)
    
    # Clean up temporary files
    if os.path.exists(zip_filename):
        os.remove(zip_filename)
        print(f"Removed temporary file: {zip_filename}")
    
    # Find and remove any extracted XML and JSON files
    for file in os.listdir('.'):
        if file.endswith('.xml') or file.endswith('.json'):
            os.remove(file)
            print(f"Removed temporary file: {file}")
    
    return result

# Run the script
if __name__ == "__main__":
    # Option to reset database - set to True to drop and recreate the table
    reset_db = True
    
    if reset_db:
        print("Resetting database to start fresh...")
        if not reset_database():
            print("Database reset failed. Exiting.")
            exit(1)
    
    # Try today's date first
    today_date = datetime.datetime.now().strftime("%Y%m%d")
    print(f"Trying today's date: {today_date}")
    success = run_etl(today_date)
    
    # If today's extract isn't available, try yesterday's
    if not success:
        yesterday_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
        print(f"Today's extract not available, trying yesterday's date: {yesterday_date}")
        success = run_etl(yesterday_date)
    
    if success:
        print("ETL process completed successfully!")
    else:
        print("ETL process failed.")