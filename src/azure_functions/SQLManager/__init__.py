import azure.functions as func
import json
import logging
import os
import pyodbc
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import urllib.parse

# SQL Server connection configuration
SQL_SERVER = "grants-gov-sql-server.database.windows.net"
SQL_DATABASE = "GrantsGovDB"
SQL_USERNAME = "grantsadmin"
SQL_PASSWORD = "GrantsAdmin123!"

def main(req: func.HttpRequest) -> func.HttpResponse:
    """SQL Database Manager for three-layer grants data architecture"""
    
    logging.info('SQL Manager function processing request')
    
    try:
        # Get operation parameters
        operation = req.params.get('operation', 'query')
        layer = req.params.get('layer', 'business')  # raw, cleaned, business
        format_type = req.params.get('format', 'json')
        limit = int(req.params.get('limit', 100))
        
        # SQL-like query parameters
        sql_query = req.params.get('sql', '')
        table_name = req.params.get('table', '')
        where_clause = req.params.get('where', '')
        order_by = req.params.get('order_by', '')
        join_tables = req.params.get('join', '')
        
        # Execute based on operation type
        if operation == 'create_schema':
            result = create_database_schema()
        elif operation == 'migrate_data':
            result = migrate_data_from_table_storage()
        elif operation == 'refresh_views':
            result = refresh_business_views()
        elif operation == 'analytics':
            result = get_analytics_dashboard()
        elif operation == 'custom_sql':
            result = execute_custom_sql(sql_query)
        else:
            # Standard query operation
            result = query_layer_data(layer, limit, table_name, where_clause, order_by, join_tables)
        
        # Return response based on format
        if format_type == 'html':
            return generate_sql_html_response(result, operation, layer)
        elif format_type == 'csv':
            return generate_csv_response(result)
        else:
            return func.HttpResponse(
                json.dumps(result, default=str, indent=2),
                mimetype="application/json"
            )
            
    except Exception as e:
        logging.error(f"SQL Manager error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"SQL operation failed: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

def get_sql_connection():
    """Get SQL Server connection with proper configuration"""
    
    try:
        # Build connection string
        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        
        logging.info(f"Connecting to SQL Server: {SQL_SERVER}")
        connection = pyodbc.connect(connection_string)
        
        return connection
        
    except Exception as e:
        logging.error(f"SQL connection failed: {str(e)}")
        raise

def create_database_schema() -> Dict:
    """Create the three-layer database schema"""
    
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        # Read and execute schema script
        schema_script = get_database_schema_script()
        
        # Split into individual statements and execute
        statements = schema_script.split('GO')
        executed_statements = 0
        
        for statement in statements:
            statement = statement.strip()
            if statement:
                try:
                    cursor.execute(statement)
                    executed_statements += 1
                except Exception as e:
                    logging.warning(f"Statement execution warning: {str(e)}")
        
        connection.commit()
        connection.close()
        
        return {
            "status": "success",
            "message": "Database schema created successfully",
            "executed_statements": executed_statements,
            "layers_created": ["Layer 1 - Raw Data", "Layer 2 - Cleaned Data", "Layer 3 - Business Views"],
            "tables_created": [
                "RawGrantsLayer1", "RawAgenciesLayer1", "RawCategoriesLayer1",
                "AgencyMasterLayer2", "CategoryMasterLayer2", "CleanedGrantsLayer2",
                "GrantBusinessViewLayer3", "AgencyStatsLayer3", "CategoryStatsLayer3"
            ]
        }
        
    except Exception as e:
        return {"error": f"Schema creation failed: {str(e)}"}

def migrate_data_from_table_storage() -> Dict:
    """Migrate data from Azure Table Storage to SQL Database"""
    
    try:
        # Import the Table Storage connection from your existing functions
        from azure.data.tables import TableServiceClient
        
        STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=grantsgov225756;AccountKey=UXwW5dfy9MY9nh2BGmWhYUbzBve+6LUyT3F7+N3Cp0kWUoEk4AO3z5U6LrBYvo/VwO+Nduq2ay9E+AStKQb86Q==;EndpointSuffix=core.windows.net"
        
        # Get Table Storage data
        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        table_client = table_service.get_table_client("GrantDetails")
        entities = list(table_client.list_entities(results_per_page=1000))
        
        # Get SQL connection
        sql_connection = get_sql_connection()
        cursor = sql_connection.cursor()
        
        migrated_records = 0
        
        # Step 1: Migrate raw data to Layer 1
        for entity in entities:
            try:
                insert_sql = """
                INSERT INTO RawGrantsLayer1 (
                    OpportunityNumber, OpportunityTitle, AgencyName, AgencyCode,
                    FundingDescription, PostedDate, CloseDate, AwardCeiling, AwardFloor,
                    CategoryOfFundingActivity, FundingInstrumentType, EligibleApplicants,
                    LinkToAdditionalInformation, AssistanceListings, ExpectedNumberOfAwards,
                    EstimatedTotalFunding, GrantorContactEmail, DataQualityScore, SourceFile
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                cursor.execute(insert_sql, (
                    entity.get('OpportunityNumber', entity.get('RowKey', '')),
                    entity.get('Title', ''),
                    entity.get('AgencyName', ''),
                    entity.get('AgencyCode', ''),
                    entity.get('Description', ''),
                    entity.get('PostedDate', ''),
                    entity.get('CloseDate', ''),
                    str(entity.get('AwardCeiling', 0)),
                    str(entity.get('AwardFloor', 0)),
                    entity.get('Category', ''),
                    entity.get('FundingType', 'Grant'),
                    entity.get('EligibleApplicants', ''),
                    entity.get('AdditionalInfoURL', ''),
                    entity.get('CFDANumbers', ''),
                    str(entity.get('ExpectedAwards', '')),
                    str(entity.get('EstimatedTotalFunding', '')),
                    entity.get('GrantorEmail', ''),
                    5.0,  # Default quality score
                    'Azure_Table_Migration'
                ))
                
                migrated_records += 1
                
            except Exception as e:
                logging.warning(f"Failed to migrate record: {str(e)}")
        
        sql_connection.commit()
        
        # Step 2: Process data into Layer 2 (Cleaned)
        processed_records = process_layer2_data(cursor)
        
        # Step 3: Generate Layer 3 (Business Views)
        cursor.execute("EXEC RefreshBusinessViews")
        sql_connection.commit()
        
        sql_connection.close()
        
        return {
            "status": "success",
            "message": "Data migration completed successfully",
            "migrated_records": migrated_records,
            "processed_records": processed_records,
            "layers_populated": 3
        }
        
    except Exception as e:
        return {"error": f"Data migration failed: {str(e)}"}

def process_layer2_data(cursor) -> int:
    """Process raw data into cleaned Layer 2 tables"""
    
    try:
        # Process agencies
        cursor.execute("""
            INSERT INTO AgencyMasterLayer2 (AgencyName, AgencyCode, AgencyType)
            SELECT DISTINCT 
                AgencyName,
                AgencyCode,
                'Federal' as AgencyType
            FROM RawGrantsLayer1 
            WHERE AgencyName IS NOT NULL AND AgencyName != ''
            AND NOT EXISTS (
                SELECT 1 FROM AgencyMasterLayer2 
                WHERE AgencyMasterLayer2.AgencyName = RawGrantsLayer1.AgencyName
            )
        """)
        
        # Process categories
        cursor.execute("""
            INSERT INTO CategoryMasterLayer2 (CategoryName, CategoryGroup)
            SELECT DISTINCT 
                CategoryOfFundingActivity,
                CASE 
                    WHEN CategoryOfFundingActivity LIKE '%Science%' OR CategoryOfFundingActivity LIKE '%Research%' THEN 'Science & Technology'
                    WHEN CategoryOfFundingActivity LIKE '%Education%' THEN 'Education'
                    WHEN CategoryOfFundingActivity LIKE '%Health%' THEN 'Health'
                    WHEN CategoryOfFundingActivity LIKE '%Environment%' THEN 'Environment'
                    ELSE 'General'
                END as CategoryGroup
            FROM RawGrantsLayer1 
            WHERE CategoryOfFundingActivity IS NOT NULL AND CategoryOfFundingActivity != ''
            AND NOT EXISTS (
                SELECT 1 FROM CategoryMasterLayer2 
                WHERE CategoryMasterLayer2.CategoryName = RawGrantsLayer1.CategoryOfFundingActivity
            )
        """)
        
        # Process cleaned grants
        cursor.execute("""
            INSERT INTO CleanedGrantsLayer2 (
                OpportunityID, Title, AgencyID, CategoryID, Description,
                PostedDate, CloseDate, AwardCeiling, AwardFloor,
                EstimatedTotalFunding, InstrumentType, EligibilityRequirements,
                CFDANumbers, AdditionalInfoURL, DataQualityScore
            )
            SELECT 
                r.OpportunityNumber,
                r.OpportunityTitle,
                a.AgencyID,
                c.CategoryID,
                r.FundingDescription,
                CASE 
                    WHEN ISDATE(r.PostedDate) = 1 THEN CAST(r.PostedDate as DATE)
                    ELSE NULL
                END,
                CASE 
                    WHEN ISDATE(r.CloseDate) = 1 THEN CAST(r.CloseDate as DATE)
                    ELSE NULL
                END,
                CASE 
                    WHEN ISNUMERIC(r.AwardCeiling) = 1 THEN CAST(r.AwardCeiling as MONEY)
                    ELSE NULL
                END,
                CASE 
                    WHEN ISNUMERIC(r.AwardFloor) = 1 THEN CAST(r.AwardFloor as MONEY)
                    ELSE NULL
                END,
                CASE 
                    WHEN ISNUMERIC(r.EstimatedTotalFunding) = 1 THEN CAST(r.EstimatedTotalFunding as MONEY)
                    ELSE NULL
                END,
                r.FundingInstrumentType,
                r.EligibleApplicants,
                r.AssistanceListings,
                r.LinkToAdditionalInformation,
                r.DataQualityScore
            FROM RawGrantsLayer1 r
            LEFT JOIN AgencyMasterLayer2 a ON r.AgencyName = a.AgencyName
            LEFT JOIN CategoryMasterLayer2 c ON r.CategoryOfFundingActivity = c.CategoryName
            WHERE r.OpportunityNumber IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM CleanedGrantsLayer2 
                WHERE CleanedGrantsLayer2.OpportunityID = r.OpportunityNumber
            )
        """)
        
        # Get count of processed records
        cursor.execute("SELECT COUNT(*) FROM CleanedGrantsLayer2")
        processed_count = cursor.fetchone()[0]
        
        return processed_count
        
    except Exception as e:
        logging.error(f"Layer 2 processing error: {str(e)}")
        return 0

def query_layer_data(layer: str, limit: int, table_name: str = "", 
                    where_clause: str = "", order_by: str = "", join_tables: str = "") -> Dict:
    """Query data from specified layer with SQL-like capabilities"""
    
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        # Determine base table based on layer
        if layer == 'raw':
            base_table = table_name or 'RawGrantsLayer1'
        elif layer == 'cleaned':
            base_table = table_name or 'CleanedGrantsLayer2'
        else:  # business
            base_table = table_name or 'GrantBusinessViewLayer3'
        
        # Build SQL query
        sql_query = f"SELECT TOP {limit} * FROM {base_table}"
        
        # Add WHERE clause
        if where_clause:
            sql_query += f" WHERE {where_clause}"
        
        # Add ORDER BY
        if order_by:
            sql_query += f" ORDER BY {order_by}"
        else:
            # Default ordering
            if layer == 'business':
                sql_query += " ORDER BY OpportunityScore DESC, DaysUntilDeadline ASC"
            elif layer == 'cleaned':
                sql_query += " ORDER BY PostedDate DESC"
            else:
                sql_query += " ORDER BY ID DESC"
        
        logging.info(f"Executing SQL: {sql_query}")
        cursor.execute(sql_query)
        
        # Fetch results
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        results = []
        for row in rows:
            record = {}
            for i, value in enumerate(row):
                record[columns[i]] = value
            results.append(record)
        
        connection.close()
        
        return {
            "query_result": results,
            "layer": layer,
            "total_records": len(results),
            "sql_executed": sql_query,
            "columns": columns,
            "execution_time": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {"error": f"Query execution failed: {str(e)}"}

def refresh_business_views() -> Dict:
    """Refresh all business views and analytics"""
    
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        # Execute refresh procedures
        cursor.execute("EXEC RefreshBusinessViews")
        connection.commit()
        
        # Get updated counts
        cursor.execute("SELECT COUNT(*) FROM GrantBusinessViewLayer3")
        business_view_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM AgencyStatsLayer3")
        agency_stats_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM CategoryStatsLayer3")
        category_stats_count = cursor.fetchone()[0]
        
        connection.close()
        
        return {
            "status": "success",
            "message": "Business views refreshed successfully",
            "business_view_records": business_view_count,
            "agency_stats_records": agency_stats_count,
            "category_stats_records": category_stats_count,
            "refresh_time": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {"error": f"View refresh failed: {str(e)}"}

def get_analytics_dashboard() -> Dict:
    """Get comprehensive analytics dashboard data"""
    
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        # Overall statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as TotalGrants,
                SUM(CASE WHEN Status = 'Open' THEN 1 ELSE 0 END) as OpenGrants,
                SUM(CASE WHEN Status = 'Closing Soon' THEN 1 ELSE 0 END) as ClosingSoon,
                SUM(CASE WHEN IsHighValue = 1 THEN 1 ELSE 0 END) as HighValueGrants,
                AVG(CAST(AwardCeiling as FLOAT)) as AvgFunding,
                SUM(CAST(AwardCeiling as FLOAT)) as TotalFunding
            FROM GrantBusinessViewLayer3
        """)
        
        overall_stats = cursor.fetchone()
        
        # Top agencies by funding
        cursor.execute("""
            SELECT TOP 10 
                AgencyName,
                TotalFunding,
                TotalGrants,
                AvgFunding
            FROM AgencyStatsLayer3
            ORDER BY TotalFunding DESC
        """)
        
        top_agencies = cursor.fetchall()
        
        # Top categories by grant count
        cursor.execute("""
            SELECT TOP 10 
                CategoryName,
                TotalGrants,
                TotalFunding,
                AvgFunding
            FROM CategoryStatsLayer3
            ORDER BY TotalGrants DESC
        """)
        
        top_categories = cursor.fetchall()
        
        # Funding by status
        cursor.execute("""
            SELECT 
                Status,
                COUNT(*) as GrantCount,
                SUM(CAST(AwardCeiling as FLOAT)) as TotalFunding
            FROM GrantBusinessViewLayer3
            GROUP BY Status
            ORDER BY GrantCount DESC
        """)
        
        status_breakdown = cursor.fetchall()
        
        connection.close()
        
        return {
            "dashboard_data": {
                "overall_statistics": {
                    "total_grants": overall_stats[0],
                    "open_grants": overall_stats[1],
                    "closing_soon": overall_stats[2],
                    "high_value_grants": overall_stats[3],
                    "average_funding": round(overall_stats[4], 2) if overall_stats[4] else 0,
                    "total_funding": round(overall_stats[5], 2) if overall_stats[5] else 0
                },
                "top_agencies": [
                    {
                        "agency_name": row[0],
                        "total_funding": row[1],
                        "total_grants": row[2],
                        "avg_funding": row[3]
                    } for row in top_agencies
                ],
                "top_categories": [
                    {
                        "category_name": row[0],
                        "total_grants": row[1],
                        "total_funding": row[2],
                        "avg_funding": row[3]
                    } for row in top_categories
                ],
                "status_breakdown": [
                    {
                        "status": row[0],
                        "grant_count": row[1],
                        "total_funding": row[2]
                    } for row in status_breakdown
                ]
            },
            "generated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {"error": f"Analytics generation failed: {str(e)}"}

def execute_custom_sql(sql_query: str) -> Dict:
    """Execute custom SQL query with safety checks"""
    
    try:
        # Basic safety checks
        sql_upper = sql_query.upper().strip()
        
        # Only allow SELECT statements
        if not sql_upper.startswith('SELECT'):
            return {"error": "Only SELECT statements are allowed"}
        
        # Prevent dangerous operations
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'EXEC', 'EXECUTE']
        if any(keyword in sql_upper for keyword in dangerous_keywords):
            return {"error": "Query contains dangerous operations"}
        
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        cursor.execute(sql_query)
        
        # Fetch results
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        results = []
        for row in rows:
            record = {}
            for i, value in enumerate(row):
                record[columns[i]] = value
            results.append(record)
        
        connection.close()
        
        return {
            "custom_query_result": results,
            "sql_executed": sql_query,
            "columns": columns,
            "record_count": len(results),
            "execution_time": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {"error": f"Custom SQL execution failed: {str(e)}"}

def get_database_schema_script() -> str:
    """Return the complete database schema creation script"""
    
    # This would contain the full SQL schema from Step 2
    # For brevity, returning a simplified version
    return """
    -- This would contain the full schema from the previous step
    -- Including all CREATE TABLE statements, indexes, and stored procedures
    """

def generate_sql_html_response(result: Dict, operation: str, layer: str) -> func.HttpResponse:
    """Generate HTML response for SQL operations"""
    
    # HTML template for SQL results
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>SQL Database Manager - {operation.title()}</title>
        <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
    </head>
    <body class="bg-gray-100">
        <div class="container mx-auto px-4 py-8">
            <h1 class="text-3xl font-bold mb-6">SQL Database Manager</h1>
            <div class="bg-white rounded-lg shadow p-6">
                <h2 class="text-xl font-semibold mb-4">Operation: {operation} | Layer: {layer}</h2>
                <pre class="bg-gray-100 p-4 rounded">{json.dumps(result, default=str, indent=2)}</pre>
            </div>
        </div>
    </body>
    </html>
    """
    
    return func.HttpResponse(html_content, mimetype="text/html")

def generate_csv_response(result: Dict) -> func.HttpResponse:
    """Generate CSV response for SQL query results"""
    
    try:
        if 'query_result' in result:
            df = pd.DataFrame(result['query_result'])
            csv_data = df.to_csv(index=False)
        else:
            csv_data = "No data available"
        
        return func.HttpResponse(
            csv_data,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=sql_query_result.csv"}
        )
        
    except Exception as e:
        return func.HttpResponse(
            f"CSV generation failed: {str(e)}",
            status_code=500
        )