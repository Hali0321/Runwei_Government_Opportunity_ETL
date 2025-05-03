import logging
import psycopg2
import sys
import os

# Add the parent directory to sys.path to be able to import from the database directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, inspect, MetaData, Table, Column, String, Date, Text, Boolean, Float, JSON
from sqlalchemy_utils import database_exists, create_database
from database.db_config import DB_CONFIG, get_connection_string

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_tables_manually(engine):
    """Create tables manually using SQLAlchemy Table objects."""
    metadata = MetaData()
    
    # Define the opportunities table
    opportunities = Table(
        'opportunities', metadata,
        Column('id', String, primary_key=True),
        Column('opportunity_number', String),
        Column('title', String),
        Column('opportunity_url', String),
        Column('direct_link_to_apply_url', String),
        Column('opportunity_status', String),
        Column('open_date', Date),
        Column('close_date', Date),
        Column('deadline', Date),
        Column('time_zone', String),
        Column('date_posted', Date),
        Column('award_value', Float),
        Column('cash_award', Float),
        Column('award_floor', Float),
        Column('total_funding', Float),
        Column('cost', Float),
        Column('cost_sharing_required', Boolean, default=False),
        Column('agency', String),
        Column('agency_code', String),
        Column('service_provider', String),
        Column('eso_website', String),
        Column('short_description', Text),
        Column('long_description', Text),
        Column('eligibility', Text),
        Column('type', String),
        Column('category', String),
        Column('industry', String),
        Column('target_community', String),
        Column('opportunity_gap', String),
        Column('global_opportunity', Boolean, default=False),
        Column('global_locations', String),
        Column('countries_eligible', String),
        Column('location_details', String),
        Column('opportunity_logo_url', String),
        Column('contact_email', String),
        Column('raw_data', JSON)
    )
    
    # Create all tables
    metadata.create_all(engine)
    logger.info("Tables created manually")

def ensure_database_exists():
    """Create the PostgreSQL database if it doesn't exist."""
    connection_string = get_connection_string()
    
    # Check if database exists
    if not database_exists(connection_string):
        try:
            # Connect to PostgreSQL server using default database
            conn_string = f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/postgres"
            conn = psycopg2.connect(conn_string)
            conn.autocommit = True
            cursor = conn.cursor()
            
            # Create the database
            cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']}")
            logger.info(f"Created database {DB_CONFIG['database']}")
            
            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"Error creating database: {e}")
            raise
    else:
        logger.info(f"Database {DB_CONFIG['database']} already exists")
    
    # Create tables manually instead of using the models
    engine = create_engine(connection_string)
    create_tables_manually(engine)
    
    # Print table information
    inspector = inspect(engine)
    logger.info("Available tables:")
    for table_name in inspector.get_table_names():
        logger.info(f"- {table_name}")
        for column in inspector.get_columns(table_name):
            logger.info(f"  - {column['name']}: {column['type']}")

if __name__ == "__main__":
    try:
        ensure_database_exists()
        print("Database setup complete!")
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        print(f"Database setup failed: {e}")