import azure.functions as func
import json
import logging
import os
from typing import Dict, List
from datetime import datetime
from azure.data.tables import TableServiceClient
from azure.identity import DefaultAzureCredential
import pyodbc
import struct

# Storage connection
STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=grantsgov225756;AccountKey=UXwW5dfy9MY9nh2BGmWhYUbzBve+6LUyT3F7+N3Cp0kWUoEk4AO3z5U6LrBYvo/VwO+Nduq2ay9E+AStKQb86Q==;EndpointSuffix=core.windows.net"

# SQL Database settings
SQL_SERVER = "grants-gov-sql-server.database.windows.net"
SQL_DATABASE = "GrantsGovDB"

def get_sql_connection():
    """Get SQL connection using Azure Functions optimized authentication"""
    try:
        logging.info("Attempting SQL connection with managed identity...")
        
        # Import required modules
        from azure.identity import ManagedIdentityCredential, DefaultAzureCredential
        import struct
        
        # Try ManagedIdentityCredential first (best for Azure Functions)
        try:
            logging.info("Using ManagedIdentityCredential...")
            credential = ManagedIdentityCredential()
            
            # Get access token for Azure SQL Database
            token = credential.get_token("https://database.windows.net/")
            logging.info("Successfully obtained access token")
            
            # Create connection string
            connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER=tcp:{SQL_SERVER},1433;"
                f"DATABASE={SQL_DATABASE};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
                f"Command Timeout=30;"
            )
            
            # Prepare token for pyodbc
            token_bytes = token.token.encode("UTF-16-LE")
            token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
            
            # Connect with access token
            attrs_before = {1256: token_struct}  # SQL_COPT_SS_ACCESS_TOKEN
            connection = pyodbc.connect(connection_string, attrs_before=attrs_before)
            
            logging.info("SQL connection successful with ManagedIdentityCredential")
            return connection
            
        except Exception as mi_error:
            logging.warning(f"ManagedIdentityCredential failed: {mi_error}")
            
            # Fallback to DefaultAzureCredential
            try:
                logging.info("Falling back to DefaultAzureCredential...")
                credential = DefaultAzureCredential()
                token = credential.get_token("https://database.windows.net/")
                
                # Same connection logic as above
                connection_string = (
                    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                    f"SERVER=tcp:{SQL_SERVER},1433;"
                    f"DATABASE={SQL_DATABASE};"
                    f"Encrypt=yes;"
                    f"TrustServerCertificate=no;"
                    f"Connection Timeout=30;"
                    f"Command Timeout=30;"
                )
                
                token_bytes = token.token.encode("UTF-16-LE")
                token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
                attrs_before = {1256: token_struct}
                
                connection = pyodbc.connect(connection_string, attrs_before=attrs_before)
                logging.info("SQL connection successful with DefaultAzureCredential")
                return connection
                
            except Exception as default_error:
                logging.error(f"DefaultAzureCredential also failed: {default_error}")
                raise Exception(f"All authentication methods failed. MI Error: {mi_error}, Default Error: {default_error}")
        
    except Exception as e:
        logging.error(f"SQL connection failed: {str(e)}")
        raise Exception(f"Could not connect to SQL Database: {str(e)}")

def test_connection() -> Dict:
    """Test both storage and SQL connections with detailed diagnostics"""
    
    result = {"status": "success", "tests": {}}
    
    # Test Table Storage (this is working)
    try:
        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        tables = list(table_service.list_tables())
        table_names = [table.name for table in tables]
        
        result["tests"]["table_storage"] = {
            "status": "success",
            "storage_account": "grantsgov225756",
            "tables_found": table_names,
            "grant_details_exists": "GrantDetails" in table_names
        }
        
    except Exception as e:
        result["tests"]["table_storage"] = {
            "status": "error",
            "error": str(e)
        }
    
    # Test SQL Database with enhanced diagnostics
    try:
        # Check environment variables
        env_info = {
            "AZURE_TENANT_ID": "***" if os.environ.get('AZURE_TENANT_ID') else "Not set",
            "AZURE_CLIENT_ID": "***" if os.environ.get('AZURE_CLIENT_ID') else "Not set",
            "AZURE_CLIENT_SECRET": "***" if os.environ.get('AZURE_CLIENT_SECRET') else "Not set",
            "MSI_ENDPOINT": "***" if os.environ.get('MSI_ENDPOINT') else "Not set",
            "MSI_SECRET": "***" if os.environ.get('MSI_SECRET') else "Not set"
        }
        
        sql_conn = get_sql_connection()
        cursor = sql_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        table_count = cursor.fetchone()[0]
        
        # List existing tables
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        # Test if we can create/modify tables (check permissions)
        try:
            cursor.execute("SELECT 1")  # Simple permission test
            can_query = True
        except:
            can_query = False
        
        sql_conn.close()
        
        result["tests"]["sql_database"] = {
            "status": "success",
            "server": SQL_SERVER,
            "database": SQL_DATABASE,
            "table_count": table_count,
            "existing_tables": existing_tables,
            "grants_table_exists": "Grants" in existing_tables,
            "can_query": can_query,
            "environment_info": env_info
        }
        
    except Exception as e:
        result["tests"]["sql_database"] = {
            "status": "error",
            "error": str(e),
            "server": SQL_SERVER,
            "database": SQL_DATABASE,
            "environment_info": {
                "AZURE_TENANT_ID": "***" if os.environ.get('AZURE_TENANT_ID') else "Not set",
                "AZURE_CLIENT_ID": "***" if os.environ.get('AZURE_CLIENT_ID') else "Not set",
                "MSI_ENDPOINT": "***" if os.environ.get('MSI_ENDPOINT') else "Not set"
            },
            "suggestion": "Check Azure Function App managed identity configuration and SQL Server permissions"
        }
    
    # Overall status
    table_storage_ok = result["tests"]["table_storage"]["status"] == "success"
    sql_ok = result["tests"]["sql_database"]["status"] == "success"
    
    if not table_storage_ok or not sql_ok:
        result["status"] = "partial_failure"
    
    result["ready_for_migration"] = table_storage_ok and sql_ok
    result["timestamp"] = datetime.utcnow().isoformat()
    
    # Add recommendations based on status
    if table_storage_ok and not sql_ok:
        result["recommendations"] = [
            "Table Storage connection is working perfectly",
            "SQL Database connection failed - check Azure Function App managed identity",
            "Ensure Function App has 'Azure SQL Database Contributor' role",
            "Verify SQL Server firewall allows Azure services",
            "Check if user 'hali.intern@Runwei.onmicrosoft.com' has access to GrantsGovDB"
        ]
    
    return result

# Add a new operation to test SQL connection specifically
def test_sql_connection_only() -> Dict:
    """Test only SQL connection with detailed error reporting"""
    
    try:
        # Try each connection method individually
        connection_attempts = []
        
        # Method 1: Managed Identity
        try:
            credential = DefaultAzureCredential()
            token = credential.get_token("https://database.windows.net/")
            
            connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER=tcp:{SQL_SERVER},1433;"
                f"DATABASE={SQL_DATABASE};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )
            
            token_bytes = token.token.encode("UTF-16-LE")
            token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
            attrs_before = {1256: token_struct}
            
            connection = pyodbc.connect(connection_string, attrs_before=attrs_before)
            connection.close()
            
            connection_attempts.append({
                "method": "Managed Identity with Access Token",
                "status": "success",
                "error": None
            })
            
            return {
                "status": "success",
                "message": "SQL connection successful with Managed Identity",
                "connection_attempts": connection_attempts,
                "server": SQL_SERVER,
                "database": SQL_DATABASE
            }
            
        except Exception as e1:
            connection_attempts.append({
                "method": "Managed Identity with Access Token",
                "status": "failed",
                "error": str(e1)
            })
        
        # If we get here, all methods failed
        return {
            "status": "error",
            "error": "All SQL connection methods failed",
            "connection_attempts": connection_attempts,
            "server": SQL_SERVER,
            "database": SQL_DATABASE,
            "recommendations": [
                "Enable System Managed Identity on your Azure Function App",
                "Grant Function App access to SQL Database",
                "Check SQL Server firewall settings",
                "Verify ODBC driver version compatibility"
            ]
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": f"SQL connection test failed: {str(e)}",
            "server": SQL_SERVER,
            "database": SQL_DATABASE
        }

def main(req: func.HttpRequest) -> func.HttpResponse:
    """Data retrieval and migration to SQL Database"""
    
    logging.info('DataMigrator: Starting data retrieval and migration')
    
    try:
        # Get parameters
        operation = req.params.get('operation', 'get_data')
        limit = int(req.params.get('limit', 100))
        
        logging.info(f'Operation: {operation}, Limit: {limit}')
        
        # Operation routing
        if operation == 'test_connection':
            result = test_connection()
        elif operation == 'test_sql_only':
            result = test_sql_connection_only()
        elif operation == 'get_data':
            result = get_grants_data(limit)
        elif operation == 'get_all':
            result = get_grants_data(1000)
        elif operation == 'migrate_to_sql':
            result = migrate_data_to_sql(limit)
        elif operation == 'create_tables':
            result = create_sql_tables()
        elif operation == 'check_sql_tables':
            result = check_sql_tables()
        else:
            result = get_grants_data(limit)
        
        return func.HttpResponse(
            json.dumps(result, default=str, indent=2),
            mimetype="application/json",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"DataMigrator error: {str(e)}", exc_info=True)
        
        error_result = {
            "status": "error",
            "error": str(e),
            "operation": operation,
            "timestamp": datetime.utcnow().isoformat(),
            "storage_account": "grantsgov225756"
        }
        
        return func.HttpResponse(
            json.dumps(error_result, default=str, indent=2),
            mimetype="application/json",
            status_code=200
        )

def get_grants_data(limit: int) -> Dict:
    """Get grants data from Table Storage"""
    
    try:
        logging.info(f"Retrieving {limit} grants from Table Storage")
        
        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        table_client = table_service.get_table_client("GrantDetails")
        
        # Try filtered query first
        try:
            entities = list(table_client.query_entities(
                query_filter="PartitionKey eq 'Grant'",
                results_per_page=limit
            ))
            query_method = "Filtered by PartitionKey='Grant'"
        except Exception:
            entities = list(table_client.list_entities(results_per_page=limit))
            query_method = "List all entities"
        
        # Clean the data
        grants_data = []
        for entity in entities:
            clean_record = {}
            for key, value in entity.items():
                if key not in ['etag', 'odata_etag', 'odata_metadata', 'Timestamp']:
                    if isinstance(value, datetime):
                        clean_record[key] = value.isoformat()
                    else:
                        clean_record[key] = value
            grants_data.append(clean_record)
        
        grants_data.sort(key=lambda x: x.get('RowKey', ''))
        
        return {
            "status": "success",
            "storage_account": "grantsgov225756",
            "table_name": "GrantDetails",
            "query_method": query_method,
            "total_records": len(grants_data),
            "data": grants_data,
            "timestamp": datetime.utcnow().isoformat(),
            "ready_for_sql_migration": True
        }
        
    except Exception as e:
        logging.error(f"Data retrieval failed: {str(e)}")
        return {
            "status": "error",
            "error": f"Data retrieval failed: {str(e)}",
            "storage_account": "grantsgov225756"
        }

def create_sql_tables() -> Dict:
    """Create tables in SQL Database for grants data"""
    
    try:
        logging.info("Creating SQL tables for grants data")
        
        sql_conn = get_sql_connection()
        cursor = sql_conn.cursor()
        
        # Create main Grants table
        create_grants_table = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Grants' AND xtype='U')
        CREATE TABLE Grants (
            ID int IDENTITY(1,1) PRIMARY KEY,
            RowKey nvarchar(100),
            PartitionKey nvarchar(50),
            Title nvarchar(500),
            AgencyName nvarchar(200),
            AgencyCode nvarchar(50),
            Description ntext,
            PostedDate nvarchar(50),
            CloseDate nvarchar(50),
            AwardCeiling nvarchar(50),
            AwardFloor nvarchar(50),
            Category nvarchar(200),
            FundingType nvarchar(100),
            EligibleApplicants ntext,
            AdditionalInfoURL nvarchar(500),
            CFDANumbers nvarchar(200),
            ExpectedAwards nvarchar(50),
            EstimatedTotalFunding nvarchar(50),
            GrantorEmail nvarchar(200),
            LastUpdated nvarchar(50),
            ImportDate datetime2 DEFAULT GETUTCDATE(),
            DataSource nvarchar(100) DEFAULT 'Azure_Table_Storage'
        )
        """
        
        cursor.execute(create_grants_table)
        sql_conn.commit()
        
        # Check if table was created
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Grants'")
        grants_table_exists = cursor.fetchone()[0] > 0
        
        sql_conn.close()
        
        return {
            "status": "success",
            "message": "SQL tables created successfully",
            "grants_table_created": grants_table_exists,
            "database": SQL_DATABASE,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logging.error(f"Table creation failed: {str(e)}")
        return {
            "status": "error",
            "error": f"Table creation failed: {str(e)}",
            "database": SQL_DATABASE
        }

def migrate_data_to_sql(limit: int) -> Dict:
    """Migrate data from Table Storage to SQL Database"""
    
    try:
        logging.info(f"Starting migration of {limit} records to SQL Database")
        
        # First, get the data from Table Storage
        table_data = get_grants_data(limit)
        if table_data["status"] != "success":
            return {
                "status": "error",
                "error": "Failed to retrieve data from Table Storage",
                "table_storage_error": table_data.get("error")
            }
        
        grants_data = table_data["data"]
        if not grants_data:
            return {
                "status": "error",
                "error": "No data found in Table Storage to migrate"
            }
        
        # Connect to SQL Database
        sql_conn = get_sql_connection()
        cursor = sql_conn.cursor()
        
        # Ensure table exists
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Grants'")
        if cursor.fetchone()[0] == 0:
            # Create table first
            create_result = create_sql_tables()
            if create_result["status"] != "success":
                return create_result
        
        # Clear existing data (optional)
        cursor.execute("DELETE FROM Grants WHERE DataSource = 'Azure_Table_Storage'")
        
        # Insert data
        inserted_count = 0
        failed_count = 0
        
        insert_sql = """
        INSERT INTO Grants (
            RowKey, PartitionKey, Title, AgencyName, AgencyCode,
            Description, PostedDate, CloseDate, AwardCeiling, AwardFloor,
            Category, FundingType, EligibleApplicants, AdditionalInfoURL,
            CFDANumbers, ExpectedAwards, EstimatedTotalFunding, GrantorEmail, LastUpdated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        for grant in grants_data:
            try:
                # Better data cleaning and null handling
                cursor.execute(insert_sql, (
                    str(grant.get('RowKey', '') or '')[:100],
                    str(grant.get('PartitionKey', '') or '')[:50],
                    str(grant.get('Title', '') or '')[:500],
                    str(grant.get('AgencyName', '') or '')[:200],
                    str(grant.get('AgencyCode', '') or '')[:50],
                    str(grant.get('Description', '') or ''),  # Full text for ntext
                    str(grant.get('PostedDate', '') or '')[:50],
                    str(grant.get('CloseDate', '') or '')[:50],
                    str(grant.get('AwardCeiling', '') or '')[:50],
                    str(grant.get('AwardFloor', '') or '')[:50],
                    str(grant.get('Category', '') or '')[:200],
                    str(grant.get('FundingType', 'Grant') or 'Grant')[:100],
                    str(grant.get('EligibleApplicants', '') or ''),  # Full text for ntext
                    str(grant.get('AdditionalInfoURL', '') or '')[:500],
                    str(grant.get('CFDANumbers', '') or '')[:200],
                    str(grant.get('ExpectedAwards', '') or '')[:50],
                    str(grant.get('EstimatedTotalFunding', '') or '')[:50],
                    str(grant.get('GrantorEmail', '') or '')[:200],
                    str(grant.get('LastUpdated', '') or '')[:50]
                ))
                inserted_count += 1
                
                # Log progress every 10 records
                if inserted_count % 10 == 0:
                    logging.info(f"Inserted {inserted_count} records...")
                
            except Exception as insert_error:
                logging.warning(f"Failed to insert grant {grant.get('RowKey', 'unknown')}: {insert_error}")
                failed_count += 1
                
                # Log the problematic record for debugging
                if failed_count <= 3:  # Only log first 3 failures to avoid spam
                    logging.warning(f"Problematic record: {grant}")
        
        sql_conn.commit()
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM Grants WHERE DataSource = 'Azure_Table_Storage'")
        final_count = cursor.fetchone()[0]
        
        sql_conn.close()
        
        return {
            "status": "success",
            "message": "Data migration completed successfully",
            "source_records": len(grants_data),
            "inserted_records": inserted_count,
            "failed_records": failed_count,
            "final_count_in_sql": final_count,
            "success_rate": round((inserted_count / len(grants_data)) * 100, 2) if grants_data else 0,
            "database": SQL_DATABASE,
            "table": "Grants",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logging.error(f"Migration failed: {str(e)}")
        return {
            "status": "error",
            "error": f"Migration failed: {str(e)}",
            "database": SQL_DATABASE
        }

def check_sql_tables() -> Dict:
    """Check what tables exist in SQL Database"""
    
    try:
        sql_conn = get_sql_connection()
        cursor = sql_conn.cursor()
        
        # Get all tables
        cursor.execute("""
            SELECT TABLE_NAME, TABLE_TYPE 
            FROM INFORMATION_SCHEMA.TABLES 
            ORDER BY TABLE_NAME
        """)
        tables = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        
        # Check Grants table specifically
        grants_info = None
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Grants'")
        if cursor.fetchone()[0] > 0:
            cursor.execute("SELECT COUNT(*) FROM Grants")
            record_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT TOP 5 Title, AgencyName FROM Grants ORDER BY ID")
            sample_records = [{"title": row[0], "agency": row[1]} for row in cursor.fetchall()]
            
            grants_info = {
                "exists": True,
                "record_count": record_count,
                "sample_records": sample_records
            }
        else:
            grants_info = {"exists": False}
        
        sql_conn.close()
        
        return {
            "status": "success",
            "database": SQL_DATABASE,
            "all_tables": tables,
            "grants_table": grants_info,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": f"Failed to check SQL tables: {str(e)}",
            "database": SQL_DATABASE
        }

def test_connection() -> Dict:
    """Test both storage and SQL connections"""
    
    result = {"status": "success", "tests": {}}
    
    # Test Table Storage
    try:
        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        tables = list(table_service.list_tables())
        table_names = [table.name for table in tables]
        
        result["tests"]["table_storage"] = {
            "status": "success",
            "storage_account": "grantsgov225756",
            "tables_found": table_names,
            "grant_details_exists": "GrantDetails" in table_names
        }
        
    except Exception as e:
        result["tests"]["table_storage"] = {
            "status": "error",
            "error": str(e)
        }
    
    # Test SQL Database
    try:
        sql_conn = get_sql_connection()
        cursor = sql_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        table_count = cursor.fetchone()[0]
        
        # List existing tables
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        sql_conn.close()
        
        result["tests"]["sql_database"] = {
            "status": "success",
            "server": SQL_SERVER,
            "database": SQL_DATABASE,
            "table_count": table_count,
            "existing_tables": existing_tables,
            "grants_table_exists": "Grants" in existing_tables
        }
        
    except Exception as e:
        result["tests"]["sql_database"] = {
            "status": "error",
            "error": str(e),
            "server": SQL_SERVER,
            "database": SQL_DATABASE
        }
    
    # Overall status
    table_storage_ok = result["tests"]["table_storage"]["status"] == "success"
    sql_ok = result["tests"]["sql_database"]["status"] == "success"
    
    if not table_storage_ok or not sql_ok:
        result["status"] = "partial_failure"
    
    result["ready_for_migration"] = table_storage_ok and sql_ok
    result["timestamp"] = datetime.utcnow().isoformat()
    
    return result

def get_grants_data(limit: int) -> Dict:
    """Get grants data from Table Storage"""
    
    try:
        logging.info(f"Retrieving {limit} grants from Table Storage")
        
        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        table_client = table_service.get_table_client("GrantDetails")
        
        # Try filtered query first
        try:
            entities = list(table_client.query_entities(
                query_filter="PartitionKey eq 'Grant'",
                results_per_page=limit
            ))
            query_method = "Filtered by PartitionKey='Grant'"
        except Exception:
            entities = list(table_client.list_entities(results_per_page=limit))
            query_method = "List all entities"
        
        # Clean the data
        grants_data = []
        for entity in entities:
            clean_record = {}
            for key, value in entity.items():
                if key not in ['etag', 'odata_etag', 'odata_metadata', 'Timestamp']:
                    if isinstance(value, datetime):
                        clean_record[key] = value.isoformat()
                    else:
                        clean_record[key] = value
            grants_data.append(clean_record)
        
        grants_data.sort(key=lambda x: x.get('RowKey', ''))
        
        return {
            "status": "success",
            "storage_account": "grantsgov225756",
            "table_name": "GrantDetails",
            "query_method": query_method,
            "total_records": len(grants_data),
            "data": grants_data,
            "timestamp": datetime.utcnow().isoformat(),
            "ready_for_sql_migration": True
        }
        
    except Exception as e:
        logging.error(f"Data retrieval failed: {str(e)}")
        return {
            "status": "error",
            "error": f"Data retrieval failed: {str(e)}",
            "storage_account": "grantsgov225756"
        }

def create_sql_tables() -> Dict:
    """Create tables in SQL Database for grants data"""
    
    try:
        logging.info("Creating SQL tables for grants data")
        
        sql_conn = get_sql_connection()
        cursor = sql_conn.cursor()
        
        # Create main Grants table
        create_grants_table = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Grants' AND xtype='U')
        CREATE TABLE Grants (
            ID int IDENTITY(1,1) PRIMARY KEY,
            RowKey nvarchar(100),
            PartitionKey nvarchar(50),
            Title nvarchar(500),
            AgencyName nvarchar(200),
            AgencyCode nvarchar(50),
            Description ntext,
            PostedDate nvarchar(50),
            CloseDate nvarchar(50),
            AwardCeiling nvarchar(50),
            AwardFloor nvarchar(50),
            Category nvarchar(200),
            FundingType nvarchar(100),
            EligibleApplicants ntext,
            AdditionalInfoURL nvarchar(500),
            CFDANumbers nvarchar(200),
            ExpectedAwards nvarchar(50),
            EstimatedTotalFunding nvarchar(50),
            GrantorEmail nvarchar(200),
            LastUpdated nvarchar(50),
            ImportDate datetime2 DEFAULT GETUTCDATE(),
            DataSource nvarchar(100) DEFAULT 'Azure_Table_Storage'
        )
        """
        
        cursor.execute(create_grants_table)
        sql_conn.commit()
        
        # Check if table was created
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Grants'")
        grants_table_exists = cursor.fetchone()[0] > 0
        
        sql_conn.close()
        
        return {
            "status": "success",
            "message": "SQL tables created successfully",
            "grants_table_created": grants_table_exists,
            "database": SQL_DATABASE,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logging.error(f"Table creation failed: {str(e)}")
        return {
            "status": "error",
            "error": f"Table creation failed: {str(e)}",
            "database": SQL_DATABASE
        }

def migrate_data_to_sql(limit: int) -> Dict:
    """Migrate data from Table Storage to SQL Database"""
    
    try:
        logging.info(f"Starting migration of {limit} records to SQL Database")
        
        # First, get the data from Table Storage
        table_data = get_grants_data(limit)
        if table_data["status"] != "success":
            return {
                "status": "error",
                "error": "Failed to retrieve data from Table Storage",
                "table_storage_error": table_data.get("error")
            }
        
        grants_data = table_data["data"]
        if not grants_data:
            return {
                "status": "error",
                "error": "No data found in Table Storage to migrate"
            }
        
        # Connect to SQL Database
        sql_conn = get_sql_connection()
        cursor = sql_conn.cursor()
        
        # Ensure table exists
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Grants'")
        if cursor.fetchone()[0] == 0:
            # Create table first
            create_result = create_sql_tables()
            if create_result["status"] != "success":
                return create_result
        
        # Clear existing data (optional)
        cursor.execute("DELETE FROM Grants WHERE DataSource = 'Azure_Table_Storage'")
        
        # Insert data
        inserted_count = 0
        failed_count = 0
        
        insert_sql = """
        INSERT INTO Grants (
            RowKey, PartitionKey, Title, AgencyName, AgencyCode,
            Description, PostedDate, CloseDate, AwardCeiling, AwardFloor,
            Category, FundingType, EligibleApplicants, AdditionalInfoURL,
            CFDANumbers, ExpectedAwards, EstimatedTotalFunding, GrantorEmail, LastUpdated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        for grant in grants_data:
            try:
                # Better data cleaning and null handling
                cursor.execute(insert_sql, (
                    str(grant.get('RowKey', '') or '')[:100],
                    str(grant.get('PartitionKey', '') or '')[:50],
                    str(grant.get('Title', '') or '')[:500],
                    str(grant.get('AgencyName', '') or '')[:200],
                    str(grant.get('AgencyCode', '') or '')[:50],
                    str(grant.get('Description', '') or ''),  # Full text for ntext
                    str(grant.get('PostedDate', '') or '')[:50],
                    str(grant.get('CloseDate', '') or '')[:50],
                    str(grant.get('AwardCeiling', '') or '')[:50],
                    str(grant.get('AwardFloor', '') or '')[:50],
                    str(grant.get('Category', '') or '')[:200],
                    str(grant.get('FundingType', 'Grant') or 'Grant')[:100],
                    str(grant.get('EligibleApplicants', '') or ''),  # Full text for ntext
                    str(grant.get('AdditionalInfoURL', '') or '')[:500],
                    str(grant.get('CFDANumbers', '') or '')[:200],
                    str(grant.get('ExpectedAwards', '') or '')[:50],
                    str(grant.get('EstimatedTotalFunding', '') or '')[:50],
                    str(grant.get('GrantorEmail', '') or '')[:200],
                    str(grant.get('LastUpdated', '') or '')[:50]
                ))
                inserted_count += 1
                
                # Log progress every 10 records
                if inserted_count % 10 == 0:
                    logging.info(f"Inserted {inserted_count} records...")
                
            except Exception as insert_error:
                logging.warning(f"Failed to insert grant {grant.get('RowKey', 'unknown')}: {insert_error}")
                failed_count += 1
                
                # Log the problematic record for debugging
                if failed_count <= 3:  # Only log first 3 failures to avoid spam
                    logging.warning(f"Problematic record: {grant}")
        
        sql_conn.commit()
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM Grants WHERE DataSource = 'Azure_Table_Storage'")
        final_count = cursor.fetchone()[0]
        
        sql_conn.close()
        
        return {
            "status": "success",
            "message": "Data migration completed successfully",
            "source_records": len(grants_data),
            "inserted_records": inserted_count,
            "failed_records": failed_count,
            "final_count_in_sql": final_count,
            "success_rate": round((inserted_count / len(grants_data)) * 100, 2) if grants_data else 0,
            "database": SQL_DATABASE,
            "table": "Grants",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logging.error(f"Migration failed: {str(e)}")
        return {
            "status": "error",
            "error": f"Migration failed: {str(e)}",
            "database": SQL_DATABASE
        }

def check_sql_tables() -> Dict:
    """Check what tables exist in SQL Database"""
    
    try:
        sql_conn = get_sql_connection()
        cursor = sql_conn.cursor()
        
        # Get all tables
        cursor.execute("""
            SELECT TABLE_NAME, TABLE_TYPE 
            FROM INFORMATION_SCHEMA.TABLES 
            ORDER BY TABLE_NAME
        """)
        tables = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        
        # Check Grants table specifically
        grants_info = None
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Grants'")
        if cursor.fetchone()[0] > 0:
            cursor.execute("SELECT COUNT(*) FROM Grants")
            record_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT TOP 5 Title, AgencyName FROM Grants ORDER BY ID")
            sample_records = [{"title": row[0], "agency": row[1]} for row in cursor.fetchall()]
            
            grants_info = {
                "exists": True,
                "record_count": record_count,
                "sample_records": sample_records
            }
        else:
            grants_info = {"exists": False}
        
        sql_conn.close()
        
        return {
            "status": "success",
            "database": SQL_DATABASE,
            "all_tables": tables,
            "grants_table": grants_info,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": f"Failed to check SQL tables: {str(e)}",
            "database": SQL_DATABASE
        }

def test_connection() -> Dict:
    """Test both storage and SQL connections"""
    
    result = {"status": "success", "tests": {}}
    
    # Test Table Storage
    try:
        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        tables = list(table_service.list_tables())
        table_names = [table.name for table in tables]
        
        result["tests"]["table_storage"] = {
            "status": "success",
            "storage_account": "grantsgov225756",
            "tables_found": table_names,
            "grant_details_exists": "GrantDetails" in table_names
        }
        
    except Exception as e:
        result["tests"]["table_storage"] = {
            "status": "error",
            "error": str(e)
        }
    
    # Test SQL Database
    try:
        sql_conn = get_sql_connection()
        cursor = sql_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        table_count = cursor.fetchone()[0]
        
        # List existing tables
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        sql_conn.close()
        
        result["tests"]["sql_database"] = {
            "status": "success",
            "server": SQL_SERVER,
            "database": SQL_DATABASE,
            "table_count": table_count,
            "existing_tables": existing_tables,
            "grants_table_exists": "Grants" in existing_tables
        }
        
    except Exception as e:
        result["tests"]["sql_database"] = {
            "status": "error",
            "error": str(e),
            "server": SQL_SERVER,
            "database": SQL_DATABASE
        }
    
    # Overall status
    table_storage_ok = result["tests"]["table_storage"]["status"] == "success"
    sql_ok = result["tests"]["sql_database"]["status"] == "success"
    
    if not table_storage_ok or not sql_ok:
        result["status"] = "partial_failure"
    
    result["ready_for_migration"] = table_storage_ok and sql_ok
    result["timestamp"] = datetime.utcnow().isoformat()
    
    return result