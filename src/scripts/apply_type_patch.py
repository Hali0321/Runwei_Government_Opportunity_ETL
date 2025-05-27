#!/usr/bin/env python3
import re
import sys

def patch_file(filename):
    with open(filename, 'r') as file:
        content = file.read()
    
    # Add imports
    imports = """
import re
from azure.cosmosdb.table.models import EntityProperty, EdmType
"""
    if "from azure.cosmosdb.table.models import" not in content:
        content = re.sub(r'import logging\n', f'import logging{imports}', content)
    
    # Update entity creation with explicit EDM type annotations
    entity_pattern = r"""entity = \{
                                    "PartitionKey": "Grant",
                                    "RowKey": opportunity_id,
                                    (.*?)
                                    # Numeric fields - use safe conversion
                                    "AwardFloor": safe_float\(opportunity\.get\("awardFloor", 0\)\),
                                    "AwardCeiling": safe_float\(opportunity\.get\("awardCeiling", 0\)\),
                                    "ExpectedAwards": safe_int\(opportunity\.get\("expectedNumOfAwards", 0\)\),
                                    (.*?)
                                \}"""
    
    replacement = """entity = {
                                    "PartitionKey": "Grant",
                                    "RowKey": opportunity_id,
                                    \\1
                                    # Numeric fields - use explicit EDM type annotations
                                    "AwardFloor": EntityProperty(safe_float(opportunity.get("awardFloor", 0)), EdmType.DOUBLE),
                                    "AwardCeiling": EntityProperty(safe_float(opportunity.get("awardCeiling", 0)), EdmType.DOUBLE),
                                    "ExpectedAwards": EntityProperty(safe_int(opportunity.get("expectedNumOfAwards", 0)), EdmType.INT32),
                                    \\2
                                }"""
    
    # Use a non-greedy pattern match and re.DOTALL to match across multiple lines
    modified = re.sub(entity_pattern, replacement, content, flags=re.DOTALL)
    
    # Fix TableServiceClient import for compatibility
    modified = modified.replace("from azure.data.tables", "from azure.cosmosdb.table")
    modified = modified.replace("TableServiceClient", "TableService")
    modified = modified.replace("table_service.get_table_client", "table_service.get_table_service_client")
    
    # Write back to the file
    with open(filename, 'w') as file:
        file.write(modified)
    
    print(f"Updated {filename} to use explicit EDM types")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python apply_type_patch.py <filename>")
        sys.exit(1)
    
    patch_file(sys.argv[1])
