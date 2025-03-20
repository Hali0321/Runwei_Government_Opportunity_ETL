from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import datetime, timedelta
import os
import sys

# Add the dags directory to Python path
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
DAGS_FOLDER = os.path.join(AIRFLOW_HOME, 'dags')
sys.path.append(DAGS_FOLDER)

# Import from etl_script module directly
from etl_script import (
    download_specific_file, 
    extract_xml_from_zip_to_json, 
    process_data, 
    store_in_postgres, 
    ensure_postgres_table
)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': pendulum.today('America/New_York').add(days=-1),
}

def get_extract_date(**context):
    """Get the date for which to extract data"""
    current_date = datetime.now().strftime("%Y%m%d")
    return current_date

def download_file(**context):
    """Download the zip file for the given date"""
    date_str = context['task_instance'].xcom_pull(task_ids='get_date')
    zip_filename = download_specific_file(date_str)
    if not zip_filename:
        raise Exception("Failed to download file")
    return zip_filename

def extract_and_transform_data(**context):
    """Extract XML from zip and transform to JSON"""
    zip_filename = context['task_instance'].xcom_pull(task_ids='download_file')
    json_data = extract_xml_from_zip_to_json(zip_filename)
    if not json_data:
        raise Exception("Failed to extract and transform data")
    
    # Process the JSON data
    processed_data = process_data(json_data)
    if processed_data.empty:
        raise Exception("Failed to process data")
    
    # Save processed data to a temporary file
    temp_file = 'processed_data.pkl'
    processed_data.to_pickle(temp_file)
    
    # Clean up zip file
    if os.path.exists(zip_filename):
        os.remove(zip_filename)
        
    # Clean up XML and JSON files
    for file in os.listdir('.'):
        if file.endswith('.xml') or file.endswith('.json'):
            os.remove(file)
    
    return temp_file

def load_to_postgres(**context):
    """Load the processed data into PostgreSQL"""
    temp_file = context['task_instance'].xcom_pull(task_ids='extract_and_transform')
    
    # Ensure the database table exists
    try:
        ensure_postgres_table()
    except Exception as e:
        raise Exception(f"Failed to ensure PostgreSQL table exists: {str(e)}")
    
    # Load the processed data
    try:
        import pandas as pd
        processed_data = pd.read_pickle(temp_file)
    except Exception as e:
        raise Exception(f"Failed to load processed data from pickle: {str(e)}")
    
    # Store in PostgreSQL
    try:
        success = store_in_postgres(processed_data)
        if not success:
            raise Exception("store_in_postgres returned False")
    except Exception as e:
        raise Exception(f"Failed to load data to PostgreSQL: {str(e)}")
    
    # Clean up temporary file
    if os.path.exists(temp_file):
        os.remove(temp_file)

def cleanup(**context):
    """Clean up any remaining temporary files"""
    # Remove any remaining temporary files
    temp_files = [f for f in os.listdir('.') if f.endswith(('.zip', '.xml', '.json', '.pkl'))]
    for file in temp_files:
        try:
            os.remove(file)
            print(f"Cleaned up: {file}")
        except Exception as e:
            print(f"Error cleaning up {file}: {e}")

# Create the DAG
with DAG(
    'grants_gov_etl',
    default_args=default_args,
    description='Daily ETL process for Grants.gov data',
    schedule='0 5 * * *',  # Run at 5 AM EDT/EST
    catchup=False,
    tags=['grants', 'etl'],
) as dag:


    # Task 1: Get the date for extraction
    get_date = PythonOperator(
        task_id='get_date',
        python_callable=get_extract_date,
    )

    # Task 2: Download the file
    download = PythonOperator(
        task_id='download_file',
        python_callable=download_file,
    )

    # Task 3: Extract and transform data
    transform = PythonOperator(
        task_id='extract_and_transform',
        python_callable=extract_and_transform_data,
        execution_timeout=timedelta(minutes=30),  # Set 30-minute timeout
        retry_delay=timedelta(minutes=5),
        retries=2,
    )

    # Remove the duplicate transform task (Task 4) entirely

    # Task 4: Load to PostgreSQL (renumbered from Task 5)
    load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    # Task 5: Cleanup (renumbered from Task 6)
    cleanup_task = PythonOperator(
        task_id='cleanup',
        python_callable=cleanup,
        trigger_rule='all_done',  # Run even if upstream tasks fail
    )

    # Set task dependencies
    get_date >> download >> transform >> load >> cleanup_task
