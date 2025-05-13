import os
from azure.data.tables import TableServiceClient
from datetime import datetime, timedelta

def main():
    connection_string = os.environ.get("STORAGE_CONNECTION")
    if not connection_string:
        print("Please set the STORAGE_CONNECTION environment variable.")
        return
        
    # Initialize table client
    table_service = TableServiceClient.from_connection_string(connection_string)
    table_client = table_service.get_table_client("GrantDetails")
    
    # Calculate date threshold (e.g., 90 days ago)
    threshold_date = (datetime.now() - timedelta(days=90)).isoformat()
    
    print(f"Looking for grants older than {threshold_date}...")
    
    # Query for old records - get all and filter in memory since Azure Table doesn't support
    # complex date comparisons well
    all_grants = list(table_client.query_entities("PartitionKey eq 'Grant'"))
    
    # Filter grants older than the threshold
    old_grants = [
        grant for grant in all_grants 
        if grant.get("LastUpdated", datetime.max.isoformat()) < threshold_date
    ]
    
    print(f"Found {len(old_grants)} outdated grant records out of {len(all_grants)} total records")
    
    # Delete old records
    for grant in old_grants:
        print(f"Deleting grant {grant['RowKey']}: {grant.get('Title', '')}")
        table_client.delete_entity(
            partition_key=grant["PartitionKey"],
            row_key=grant["RowKey"]
        )
        
    print(f"Cleanup complete. Deleted {len(old_grants)} records.")

if __name__ == "__main__":
    main()
