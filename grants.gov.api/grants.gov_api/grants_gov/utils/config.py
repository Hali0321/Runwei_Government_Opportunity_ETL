import os
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Database Configuration
DB_CONFIG = {
    "username": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "Ding86000688"),
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "database": os.getenv("PG_DATABASE", "grants_api")
}

# Generate connection string from config
def get_connection_string():
    return f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# API Configuration
API_SETTINGS = {
    "base_url": "https://www.grants.gov",
    "max_results_per_query": 100
}