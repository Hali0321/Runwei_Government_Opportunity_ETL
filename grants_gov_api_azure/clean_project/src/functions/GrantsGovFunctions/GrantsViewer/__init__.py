import logging
import azure.functions as func
import os
import json
from azure.data.tables import TableServiceClient
from datetime import datetime

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('GrantsViewer HTTP trigger function processed a request.')
    
    try:
        # Get query parameters with defaults
        format_type = req.params.get('format', 'json').lower()
        search_term = req.params.get('search', '').lower()
        agency_filter = req.params.get('agency', '').lower()
        category_filter = req.params.get('category', '').lower()
        
        # Properly parse the limit parameter - allow larger limits
        try:
            limit = int(req.params.get('limit', '100'))
            # Make sure limit is reasonable but allow large limits for data export
            if limit <= 0:
                limit = 100
            elif limit > 10000:  # Cap at 10,000 for performance reasons
                limit = 10000
        except ValueError:
            limit = 100
            
        # Properly parse the page parameter
        try:
            page = max(1, int(req.params.get('page', '1')))
        except ValueError:
            page = 1
            
        # Calculate offset based on page
        offset = (page - 1) * limit
        
        # Get connection string from environment
        connection_string = os.environ.get("AzureWebJobsStorage")
        
        if not connection_string:
            return func.HttpResponse(
                "AzureWebJobsStorage connection string not found in environment variables",
                status_code=500
            )
            
        # Initialize table client
        table_service = TableServiceClient.from_connection_string(connection_string)
        table_client = table_service.get_table_client("GrantDetails")
        
        # For performance with large tables, use server-side filtering when possible
        # and only client-side filtering for complex operations
        
        # First, count the total records (this gives us the maximum possible results)
        query_filter = "PartitionKey eq 'Grant'"
        
        # Add server-side filters if possible
        if search_term and len(search_term) >= 3:
            # We can only do server-side filtering on exact matches
            # For partial matches, we'll need to filter in memory
            pass
        
        # Get total count first (important for pagination)
        total_records = 0
        try:
            # This could be slow for very large tables but gives accurate pagination
            total_records = sum(1 for _ in table_client.query_entities(query_filter))
            logging.info(f"Total grants in table: {total_records}")
        except Exception as e:
            logging.error(f"Error counting records: {str(e)}")
            total_records = 10000  # Assume a large number
        
        # For very large tables, we can skip the counting step and use a query with a limit
        # query_results = list(table_client.query_entities(query_filter, results_per_page=limit))
        
        # Query entities with paging - this is more efficient
        logging.info(f"Querying grants with offset {offset} and limit {limit}")
        
        # Azure Table doesn't support direct offset, so we need to implement it in code
        # Get one page of results that we'll filter and paginate in memory
        query_results = list(table_client.query_entities(query_filter))
        
        # Apply client-side filtering
        filtered_grants = []
        for grant in query_results:
            # Apply search filter if provided
            if search_term:
                search_fields = ["Title", "Description", "AgencyName", "Number", "CFDANumbers"]
                search_match = any(search_term in str(grant.get(field, "")).lower() for field in search_fields)
                if not search_match:
                    continue
                    
            # Apply agency filter if provided
            if agency_filter and agency_filter not in str(grant.get("AgencyName", "")).lower():
                continue
                
            # Apply category filter if provided
            if category_filter and category_filter not in str(grant.get("Category", "")).lower():
                continue
                
            # Convert to a simplified format for response
            simplified_grant = {
                "ID": grant.get("RowKey", ""),
                "Title": grant.get("Title", ""),
                "Agency": grant.get("AgencyName", ""),
                "Description": grant.get("Description", ""),
                "PostDate": grant.get("OpenDate", ""),
                "CloseDate": grant.get("CloseDate", ""),
                "CFDANumbers": grant.get("CFDANumbers", ""),
                "AwardRange": f"{grant.get('AwardFloor', '')} - {grant.get('AwardCeiling', '')}".strip(" -"),
                "Category": grant.get("Category", ""),
                "OpportunityURL": grant.get("OpportunityURL", "")
            }
            filtered_grants.append(simplified_grant)
        
        logging.info(f"After filtering: {len(filtered_grants)} grants match criteria")
        
        # Apply pagination
        total_count = len(filtered_grants)
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        
        paginated_grants = filtered_grants[offset:offset+limit]
        logging.info(f"Returning page {page} with {len(paginated_grants)} grants (limit={limit}, offset={offset})")
        
        # Return as HTML or JSON based on format parameter
        if format_type == 'html':
            # Create an HTML response
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Grants.gov Opportunities</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    h1 {{ color: #2c3e50; }}
                    table {{ border-collapse: collapse; width: 100%; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; position: sticky; top: 0; }}
                    tr:nth-child(even) {{ background-color: #f9f9f9; }}
                    tr:hover {{ background-color: #f1f1f1; }}
                    a {{ color: #3498db; text-decoration: none; }}
                    a:hover {{ text-decoration: underline; }}
                    .search-box {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; background-color: #f9f9f9; }}
                    .pagination {{ margin-top: 20px; text-align: center; }}
                    .pagination a {{ padding: 8px 16px; text-decoration: none; color: black; margin: 0 5px; }}
                    .pagination a.active {{ background-color: #3498db; color: white; }}
                    .pagination a:hover:not(.active) {{ background-color: #ddd; }}
                    .status-bar {{ background-color: #e8f4f8; padding: 10px; margin: 10px 0; border-radius: 5px; }}
                    .export-links {{ margin: 10px 0; }}
                    .export-links a {{ margin-right: 10px; background-color: #4CAF50; color: white; padding: 10px 15px; text-decoration: none; border-radius: 4px; }}
                    .export-links a:hover {{ background-color: #45a049; }}
                </style>
            </head>
            <body>
                <h1>Grants.gov Opportunities</h1>
                
                <div class="search-box">
                    <form method="get">
                        <input type="hidden" name="format" value="html">
                        <input type="text" name="search" placeholder="Search terms..." value="{search_term}">
                        <input type="text" name="agency" placeholder="Agency filter..." value="{agency_filter}">
                        <input type="text" name="category" placeholder="Category filter..." value="{category_filter}">
                        <select name="limit">
                            <option value="50" {"selected" if limit == 50 else ""}>50 per page</option>
                            <option value="100" {"selected" if limit == 100 else ""}>100 per page</option>
                            <option value="250" {"selected" if limit == 250 else ""}>250 per page</option>
                            <option value="500" {"selected" if limit == 500 else ""}>500 per page</option>
                            <option value="1000" {"selected" if limit == 1000 else ""}>1000 per page</option>
                        </select>
                        <input type="submit" value="Search">
                    </form>
                </div>
                
                <div class="status-bar">
                    <p>Total grants in database: {total_records}</p>
                    <p>Grants matching search criteria: {total_count}</p>
                    <p>Page {page} of {total_pages} | Showing {len(paginated_grants)} grants (limit: {limit})</p>
                </div>
                
                <div class="export-links">
                    <a href="?format=json&search={search_term}&agency={agency_filter}&category={category_filter}&limit=1000" target="_blank">Export to JSON (1000 records)</a>
                    <a href="?format=json&search={search_term}&agency={agency_filter}&category={category_filter}&limit=10000" target="_blank">Export All Matching (JSON)</a>
                </div>
                
                {f"""
                <table>
                    <tr>
                        <th>ID</th>
                        <th>Title</th>
                        <th>Agency</th>
                        <th>Description</th>
                        <th>Post Date</th>
                        <th>Close Date</th>
                        <th>CFDA Numbers</th>
                        <th>Award Range</th>
                        <th>Category</th>
                        <th>Actions</th>
                    </tr>
                    {"".join(f'''
                    <tr>
                        <td>{grant["ID"]}</td>
                        <td>{grant["Title"]}</td>
                        <td>{grant["Agency"]}</td>
                        <td>{grant["Description"][:100] + "..." if len(grant["Description"]) > 100 else grant["Description"]}</td>
                        <td>{grant["PostDate"]}</td>
                        <td>{grant["CloseDate"]}</td>
                        <td>{grant["CFDANumbers"]}</td>
                        <td>{grant["AwardRange"]}</td>
                        <td>{grant["Category"]}</td>
                        <td><a href="{grant["OpportunityURL"]}" target="_blank">View on Grants.gov</a></td>
                    </tr>
                    ''' for grant in paginated_grants)}
                </table>
                """ if paginated_grants else "<p>No grants found matching your criteria.</p>"}
                
                <div class="pagination">
                    {f'<a href="?format={format_type}&search={search_term}&agency={agency_filter}&category={category_filter}&limit={limit}&page=1">First</a>' if page > 1 else ''}
                    {f'<a href="?format={format_type}&search={search_term}&agency={agency_filter}&category={category_filter}&limit={limit}&page={page-1}">Previous</a>' if page > 1 else ''}
                    
                    {f'<a href="?format={format_type}&search={search_term}&agency={agency_filter}&category={category_filter}&limit={limit}&page={page}" class="active">{page}</a>'}
                    
                    {f'<a href="?format={format_type}&search={search_term}&agency={agency_filter}&category={category_filter}&limit={limit}&page={page+1}">Next</a>' if page < total_pages else ''}
                    {f'<a href="?format={format_type}&search={search_term}&agency={agency_filter}&category={category_filter}&limit={limit}&page={total_pages}">Last</a>' if page < total_pages else ''}
                </div>
                
                <p><small>Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</small></p>
            </body>
            </html>
            """
            return func.HttpResponse(html_content, mimetype="text/html")
        else:
            # Create a JSON response with pagination info
            return func.HttpResponse(
                json.dumps({
                    "pagination": {
                        "total": total_count,
                        "page": page,
                        "limit": limit,
                        "pages": total_pages
                    },
                    "grants": paginated_grants
                }, indent=2),
                mimetype="application/json"
            )
    except Exception as e:
        logging.error(f"Error in GrantsViewer function: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        
        if format_type == 'html':
            html_error = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Error</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    h1 {{ color: #e74c3c; }}
                    pre {{ background-color: #f9f9f9; padding: 10px; border: 1px solid #ddd; }}
                </style>
            </head>
            <body>
                <h1>Error Occurred</h1>
                <p>{str(e)}</p>
                <pre>{traceback.format_exc()}</pre>
            </body>
            </html>
            """
            return func.HttpResponse(html_error, mimetype="text/html", status_code=500)
        else:
            return func.HttpResponse(
                json.dumps({
                    "error": str(e),
                    "traceback": traceback.format_exc()
                }),
                mimetype="application/json",
                status_code=500
            )
