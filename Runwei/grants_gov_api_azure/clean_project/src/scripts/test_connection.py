import os
import sys
from azure.data.tables import TableServiceClient

def main():
    # Get connection string from environment
    connection_string = os.environ.get("STORAGE_CONNECTION")
    
    if not connection_string:
        print("ERROR: No connection string found in environment variable STORAGE_CONNECTION")
        print("Please set it with:")
        print("export STORAGE_CONNECTION=\"your_connection_string_here\"")
        return
        
    print(f"Connection string found (length: {len(connection_string)})")
    
    # Test the connection string
    try:
        print("Testing connection to Azure Storage...")
        table_service = TableServiceClient.from_connection_string(connection_string)
        tables = list(table_service.list_tables())
        print(f"Connection successful! Found {len(tables)} tables:")
        for table in tables:
            print(f" - {table.name}")
    except Exception as e:
        print(f"ERROR: Failed to connect to Azure Storage: {str(e)}")
        
if __name__ == "__main__":
    main()
