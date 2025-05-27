import logging
import azure.functions as func
import os
from azure.data.tables import TableServiceClient
import json
import html

def format_currency(value):
    """Format a number as currency"""
    try:
        if value is None or value == 0:
            return "$0"
        return f"${float(value):,.2f}"
    except (ValueError, TypeError):
        return "$0"
        
def format_award_range(floor, ceiling):
    """Format award range nicely"""
    try:
        if floor is None or floor == 0:
            if ceiling is None or ceiling == 0:
                return "Not specified"
            return f"Up to {format_currency(ceiling)}"
        else:
            if ceiling is None or ceiling == 0 or ceiling == floor:
                return format_currency(floor)
            return f"{format_currency(floor)} - {format_currency(ceiling)}"
    except (ValueError, TypeError):
        return "Not specified"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    try:
        # Get query parameters
        query = req.params.get('query', '')
        limit = int(req.params.get('limit', '100'))
        format_type = req.params.get('format', 'json').lower()
        
        # Connect to Azure Table
        connection_string = os.environ["AzureWebJobsStorage"]
        table_service = TableServiceClient.from_connection_string(connection_string)
        table_client = table_service.get_table_client("GrantDetails")
        
        # Build query
        filter_query = "PartitionKey eq 'Grant'"
        if query:
            keywords = query.split()
            for keyword in keywords:
                filter_query += f" and (contains(Title, '{keyword}') or contains(Description, '{keyword}'))"
        
        # Execute query
        entities = list(table_client.query_entities(filter_query, results_per_page=limit))
        
        # Sort by close date (most recent first)
        entities.sort(key=lambda x: x.get('CloseDate', ''), reverse=True)
        
        # Limit results
        entities = entities[:limit]
        
        # Return as JSON
        if format_type == 'json':
            return func.HttpResponse(
                json.dumps(entities, default=str),
                status_code=200,
                mimetype="application/json"
            )
        # Return as HTML table
        elif format_type == 'html':
            html_content = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Grants.gov Opportunities</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; }
                    table { border-collapse: collapse; width: 100%; }
                    th, td { text-align: left; padding: 8px; border: 1px solid #ddd; }
                    tr:nth-child(even) { background-color: #f2f2f2; }
                    th { background-color: #4CAF50; color: white; }
                    .award-range { text-align: right; }
                    .link { color: blue; text-decoration: underline; }
                </style>
            </head>
            <body>
                <h1>Grants.gov Opportunities</h1>
                <p>Showing {count} results</p>
                <table>
                    <tr>
                        <th>ID</th>
                        <th>Title</th>
                        <th>Agency</th>
                        <th>Description</th>
                        <th>Award Range</th>
                        <th>Expected Awards</th>
                        <th>Type</th>
                        <th>Open Date</th>
                        <th>Close Date</th>
                        <th>Link</th>
                    </tr>
            """.format(count=len(entities))
            
            for entity in entities:
                # Get field values safely
                grant_id = entity.get('RowKey', '')
                title = html.escape(entity.get('Title', ''))
                agency = html.escape(entity.get('AgencyName', ''))
                description = html.escape(entity.get('Description', '')[:200] + '...' if entity.get('Description', '') else '')
                award_floor = entity.get('AwardFloor')
                award_ceiling = entity.get('AwardCeiling')
                expected_awards = entity.get('ExpectedNumberofAwards', entity.get('ExpectedAwards', ''))
                funding_type = html.escape(entity.get('FundingType', 'Other'))
                open_date = entity.get('OpenDate', '')
                close_date = entity.get('CloseDate', '')
                url = entity.get('OpportunityURL', f"https://www.grants.gov/search-results-detail/{grant_id}")
                
                # Format the award range
                award_range = format_award_range(award_floor, award_ceiling)
                
                # Add row
                html_content += f"""
                    <tr>
                        <td>{grant_id}</td>
                        <td>{title}</td>
                        <td>{agency}</td>
                        <td>{description}</td>
                        <td class="award-range">{award_range}</td>
                        <td>{expected_awards}</td>
                        <td>{funding_type}</td>
                        <td>{open_date}</td>
                        <td>{close_date}</td>
                        <td><a href="{url}" target="_blank" class="link">View on Grants.gov</a></td>
                    </tr>
                """
            
            html_content += """
                </table>
            </body>
            </html>
            """
            
            return func.HttpResponse(
                html_content,
                status_code=200,
                mimetype="text/html"
            )
        else:
            return func.HttpResponse(
                "Invalid format specified. Use 'json' or 'html'.",
                status_code=400
            )
    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")
        return func.HttpResponse(
            f"An error occurred: {str(e)}",
            status_code=500
        )
