import azure.functions as func
import json
import logging
import os
from typing import Dict, List, Optional
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceNotFoundError

# Direct connection string - replace with your actual connection string
STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=grantsgov225756;AccountKey=UXwW5dfy9MY9nh2BGmWhYUbzBve+6LUyT3F7+N3Cp0kWUoEk4AO3z5U6LrBYvo/VwO+Nduq2ay9E+AStKQb86Q==;EndpointSuffix=core.windows.net"

def main(req: func.HttpRequest) -> func.HttpResponse:
    """Clean grants viewer focusing on original data source"""
    
    logging.info('Grants viewer function processed a request.')
    
    try:
        # Get query parameters
        format_type = req.params.get('format', 'json').lower()
        search_query = req.params.get('search', '')
        agency_filter = req.params.get('agency', '')
        category_filter = req.params.get('category', '')
        limit = int(req.params.get('limit', 100))
        page = int(req.params.get('page', 1))
        sort_by = req.params.get('sort', 'PostedDate')
        sort_order = req.params.get('order', 'desc')
        
        # Calculate offset for pagination
        offset = (page - 1) * limit
        
        # Get grants data from original source
        grants_data = get_grants_from_azure_table(
            search_query=search_query,
            agency_filter=agency_filter,
            category_filter=category_filter,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_order=sort_order
        )
        
        # Process data for display
        processed_grants = process_grants_for_display(grants_data)
        
        if format_type == 'html':
            return generate_html_response(processed_grants, {
                'search': search_query,
                'agency': agency_filter,
                'category': category_filter,
                'limit': limit,
                'page': page,
                'total_count': len(processed_grants),
                'sort_by': sort_by,
                'sort_order': sort_order
            })
        else:
            return func.HttpResponse(
                json.dumps({
                    "grants": processed_grants,
                    "metadata": {
                        "total_count": len(processed_grants),
                        "page": page,
                        "limit": limit,
                        "search": search_query,
                        "agency_filter": agency_filter,
                        "category_filter": category_filter
                    }
                }, default=str, indent=2),
                mimetype="application/json",
                status_code=200
            )
            
    except Exception as e:
        logging.error(f"Error in grants viewer: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Error processing request: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

def get_grants_from_azure_table(search_query: str = "", agency_filter: str = "", 
                               category_filter: str = "", limit: int = 100, 
                               offset: int = 0, sort_by: str = "PostedDate", 
                               sort_order: str = "desc") -> List[Dict]:
    """Get grants from Azure Table Storage with enhanced filtering and sorting"""
    
    try:
        # Get connection string with fallback
        connection_string = os.environ.get("AzureWebJobsStorage") or STORAGE_CONNECTION_STRING
        
        if not connection_string:
            logging.error("Azure Storage connection string not found")
            return []
        
        # Initialize table client
        table_service = TableServiceClient.from_connection_string(connection_string)
        table_client = table_service.get_table_client("GrantDetails")
        
        # Build filter query
        filter_parts = ["PartitionKey eq 'Grant'"]
        
        if search_query:
            # Search in title and description
            search_filter = f"(contains(Title, '{search_query}') or contains(Description, '{search_query}'))"
            filter_parts.append(search_filter)
        
        if agency_filter:
            filter_parts.append(f"AgencyName eq '{agency_filter}'")
        
        if category_filter:
            filter_parts.append(f"Category eq '{category_filter}'")
        
        filter_query = " and ".join(filter_parts)
        
        # Query entities
        if filter_query:
            entities = list(table_client.query_entities(
                query_filter=filter_query,
                results_per_page=limit + offset
            ))
        else:
            entities = list(table_client.list_entities(results_per_page=limit + offset))
        
        # Convert to list of dicts
        grants_list = []
        for entity in entities:
            clean_entity = {}
            for key, value in entity.items():
                # Skip Azure Table metadata
                if key not in ['etag', 'odata_etag', 'odata_metadata', 'Timestamp']:
                    clean_entity[key] = value
            grants_list.append(clean_entity)
        
        # Sort results
        reverse_sort = sort_order.lower() == 'desc'
        if sort_by in ['PostedDate', 'CloseDate', 'LastUpdated']:
            grants_list.sort(key=lambda x: x.get(sort_by, ''), reverse=reverse_sort)
        elif sort_by == 'AwardCeiling':
            grants_list.sort(key=lambda x: float(x.get('AwardCeiling', 0) or 0), reverse=reverse_sort)
        else:
            grants_list.sort(key=lambda x: str(x.get(sort_by, '')), reverse=reverse_sort)
        
        # Apply pagination
        return grants_list[offset:offset + limit]
        
    except Exception as e:
        logging.error(f"Error querying Azure Table Storage: {str(e)}")
        return []

def process_grants_for_display(grants_data: List[Dict]) -> List[Dict]:
    """Process grants data for enhanced display"""
    
    processed_grants = []
    
    for grant in grants_data:
        # Extract opportunity ID for the hyperlink
        opportunity_id = grant.get('RowKey', '')
        
        # Create the Grants.gov URL for title link
        grants_gov_url = f"https://www.grants.gov/search-results-detail/{opportunity_id}"
        
        # Enhanced award range formatting
        award_ceiling = grant.get('AwardCeiling', 0) or 0
        award_floor = grant.get('AwardFloor', 0) or 0
        
        # Better logic for award range display
        if award_ceiling == 0 and award_floor == 0:
            award_range = "Not Disclosed"
        elif award_ceiling == 0 and award_floor > 0:
            award_range = f"Min: ${award_floor:,.0f}"
        elif award_floor == 0 and award_ceiling > 0:
            award_range = f"Up to ${award_ceiling:,.0f}"
        elif award_ceiling == award_floor and award_ceiling > 0:
            award_range = f"${award_ceiling:,.0f}"
        elif award_ceiling > 0 and award_floor > 0:
            award_range = f"${award_floor:,.0f} - ${award_ceiling:,.0f}"
        else:
            award_range = "Varies"
        
        # Determine grant status based on close date
        close_date = grant.get('CloseDate', '')
        status = determine_grant_status(close_date)
        
        # Clean title
        title = grant.get('Title', '')
        clean_title = clean_title_text(title)
        
        # Clean and format description
        description = grant.get('Description', '')
        short_description = description[:120] + "..." if len(description) > 120 else description
        
        processed_grant = {
            'ID': opportunity_id,
            'Title': clean_title,
            'TitleLink': grants_gov_url,
            'Agency': grant.get('AgencyName', ''),
            'Description': short_description,
            'FullDescription': description,
            'PostDate': format_date(grant.get('PostedDate', '')),
            'CloseDate': format_date(grant.get('CloseDate', '')),
            'AwardRange': award_range,
            'AwardCeiling': award_ceiling,
            'Category': grant.get('Category', ''),
            'Status': status,
            'AgencyCode': grant.get('AgencyCode', ''),
            'FundingType': grant.get('FundingType', ''),
            'LastUpdated': format_date(grant.get('LastUpdated', '')),
            'EligibleApplicants': grant.get('EligibleApplicants', ''),
            'EstimatedTotalFunding': grant.get('EstimatedTotalFunding', ''),
            'ExpectedAwards': grant.get('ExpectedAwards', '')
        }
        
        processed_grants.append(processed_grant)
    
    return processed_grants

def clean_title_text(title: str) -> str:
    """Clean title by removing update tags"""
    import re
    
    # Remove common update indicators
    update_patterns = [
        r'\[UPDATED\]',
        r'\(UPDATED\)',
        r'- UPDATED',
        r'UPDATED:',
        r'\[UPDATE\]',
        r'\(UPDATE\)'
    ]
    
    clean_title = title
    for pattern in update_patterns:
        clean_title = re.sub(pattern, '', clean_title, flags=re.IGNORECASE).strip()
        clean_title = re.sub(r'\s+', ' ', clean_title).strip(' -:')
    
    return clean_title

def determine_grant_status(close_date: str) -> str:
    """Determine grant status based on close date"""
    from datetime import datetime, timedelta
    
    if not close_date:
        return 'No Close Date'
    
    try:
        close_dt = parse_date_flexible(close_date)
        now = datetime.now()
        days_until_close = (close_dt - now).days
        
        if days_until_close < 0:
            return 'Closed'
        elif days_until_close <= 7:
            return 'Closing Soon'
        elif days_until_close <= 30:
            return 'Closing This Month'
        else:
            return 'Open'
    except:
        return 'Unknown'

def parse_date_flexible(date_str: str):
    """Parse date with multiple format support"""
    from datetime import datetime
    
    formats = [
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%d/%m/%Y',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str[:len(fmt)], fmt)
        except:
            continue
    
    raise ValueError(f"Unable to parse date: {date_str}")

def format_date(date_str: str) -> str:
    """Format date for display"""
    if not date_str:
        return ""
    
    try:
        dt = parse_date_flexible(date_str)
        return dt.strftime('%m/%d/%Y')
    except:
        return date_str

def generate_html_response(grants_data: List[Dict], params: Dict) -> func.HttpResponse:
    """Generate clean HTML response"""
    
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Grants Viewer - Azure Functions</title>
    <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        .grant-status-open {{ background-color: #10B981; color: white; }}
        .grant-status-closing-soon {{ background-color: #F59E0B; color: white; }}
        .grant-status-closing-this-month {{ background-color: #F97316; color: white; }}
        .grant-status-closed {{ background-color: #EF4444; color: white; }}
        .grant-status-no-close-date {{ background-color: #6B7280; color: white; }}
        .grant-status-unknown {{ background-color: #6B7280; color: white; }}
        
        .table-row:hover {{ background-color: #F3F4F6; }}
        .table-row:nth-child(even) {{ background-color: #F9FAFB; }}
        
        .grant-title-link {{
            color: #1D4ED8;
            text-decoration: none;
            font-weight: 500;
        }}
        
        .grant-title-link:hover {{
            color: #1E40AF;
            text-decoration: underline;
        }}
        
        .table-container {{ 
            overflow-x: auto;
            border-radius: 8px;
        }}
    </style>
</head>
<body class="bg-gray-50 font-sans">
    <div class="container mx-auto px-4 py-8 max-w-7xl">
        <!-- Header -->
        <div class="bg-white rounded-lg shadow-sm border border-gray-200 mb-6">
            <div class="px-6 py-4 border-b border-gray-200">
                <div class="flex flex-col md:flex-row md:items-center md:justify-between">
                    <div>
                        <h1 class="text-2xl font-bold text-gray-900 flex items-center">
                            <i class="fas fa-search mr-2 text-blue-600"></i>
                            Grants Viewer
                        </h1>
                        <p class="text-gray-600 mt-1">
                            Showing {len(grants_data)} grants • Page {params['page']}
                        </p>
                    </div>
                    <div class="flex items-center space-x-2 mt-4 md:mt-0">
                        <button onclick="exportToJSON(1000)" 
                                class="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg flex items-center">
                            <i class="fas fa-download mr-2"></i>Export JSON
                        </button>
                    </div>
                </div>
            </div>
            
            <!-- Search and Filter Bar -->
            <div class="px-6 py-4">
                <form onsubmit="applyFilters(); return false;">
                    <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
                        <div>
                            <label for="searchInput" class="block text-sm font-medium text-gray-700 mb-1">Search Keywords</label>
                            <input type="text" 
                                   id="searchInput" 
                                   value="{params['search']}" 
                                   placeholder="Search titles, descriptions..." 
                                   class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500">
                        </div>
                        <div>
                            <label for="agencyFilter" class="block text-sm font-medium text-gray-700 mb-1">Agency</label>
                            <select id="agencyFilter" 
                                    class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500">
                                <option value="">All Agencies</option>
                                {generate_agency_options(grants_data, params['agency'])}
                            </select>
                        </div>
                        <div>
                            <label for="categoryFilter" class="block text-sm font-medium text-gray-700 mb-1">Category</label>
                            <select id="categoryFilter" 
                                    class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500">
                                <option value="">All Categories</option>
                                {generate_category_options(grants_data, params['category'])}
                            </select>
                        </div>
                        <div class="flex items-end">
                            <button type="submit" 
                                    class="w-full bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-md">
                                <i class="fas fa-search mr-2"></i>Apply Filters
                            </button>
                        </div>
                    </div>
                </form>
            </div>
        </div>

        <!-- Data Table -->
        <div class="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
            <div class="table-container">
                <table class="min-w-full divide-y divide-gray-200">
                    <thead class="bg-gray-50">
                        <tr>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">ID</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Title</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Agency</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Posted Date</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Close Date</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Award Range</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                        </tr>
                    </thead>
                    <tbody class="bg-white divide-y divide-gray-200">
                        {generate_table_rows(grants_data)}
                    </tbody>
                </table>
            </div>
        </div>
        
        <!-- Pagination -->
        <nav class="mt-6 flex items-center justify-between">
            <button onclick="changePage({params['page'] - 1})" 
                    class="bg-gray-100 hover:bg-gray-200 text-gray-700 px-3 py-2 rounded-md"
                    {'disabled' if params['page'] <= 1 else ''}>
                Previous
            </button>
            <span class="px-3 py-2">Page {params['page']}</span>
            <button onclick="changePage({params['page'] + 1})" 
                    class="bg-gray-100 hover:bg-gray-200 text-gray-700 px-3 py-2 rounded-md">
                Next
            </button>
        </nav>
    </div>

    <script>
        function applyFilters() {{
            const search = document.getElementById('searchInput').value;
            const agency = document.getElementById('agencyFilter').value;
            const category = document.getElementById('categoryFilter').value;
            
            const params = new URLSearchParams({{
                search: search,
                agency: agency,
                category: category,
                format: 'html',
                limit: {params['limit']},
                page: 1
            }});
            
            window.location.href = '?' + params.toString();
        }}
        
        function changePage(page) {{
            const params = new URLSearchParams(window.location.search);
            params.set('page', page);
            window.location.href = '?' + params.toString();
        }}
        
        function exportToJSON(limit) {{
            const params = new URLSearchParams(window.location.search);
            params.set('format', 'json');
            params.set('limit', limit);
            window.open('?' + params.toString(), '_blank');
        }}
    </script>
</body>
</html>
"""
    
    return func.HttpResponse(html_content, mimetype="text/html", status_code=200)

def generate_table_rows(grants_data: List[Dict]) -> str:
    """Generate table rows HTML"""
    
    rows_html = ""
    
    for grant in grants_data:
        status_class = f"grant-status-{grant['Status'].lower().replace(' ', '-')}"
        
        rows_html += f"""
        <tr class="table-row">
            <td class="px-6 py-4 text-sm">{grant['ID']}</td>
            <td class="px-6 py-4 text-sm">
                <a href="{grant['TitleLink']}" target="_blank" class="grant-title-link">
                    {grant['Title']}
                </a>
                <p class="text-gray-500 text-xs mt-1">{grant['Description']}</p>
            </td>
            <td class="px-6 py-4 text-sm">{grant['Agency']}</td>
            <td class="px-6 py-4 text-sm">{grant['PostDate']}</td>
            <td class="px-6 py-4 text-sm">{grant['CloseDate']}</td>
            <td class="px-6 py-4 text-sm">{grant['AwardRange']}</td>
            <td class="px-6 py-4 text-sm">
                <span class="px-2 py-1 text-xs rounded-full {status_class}">
                    {grant['Status']}
                </span>
            </td>
        </tr>
        """
    
    return rows_html

def generate_agency_options(grants_data: List[Dict], selected_agency: str) -> str:
    """Generate agency filter options"""
    agencies = sorted(set(grant['Agency'] for grant in grants_data if grant['Agency']))
    options = ""
    
    for agency in agencies:
        selected = 'selected' if agency == selected_agency else ''
        options += f'<option value="{agency}" {selected}>{agency}</option>'
    
    return options

def generate_category_options(grants_data: List[Dict], selected_category: str) -> str:
    """Generate category filter options"""
    categories = sorted(set(grant['Category'] for grant in grants_data if grant['Category']))
    options = ""
    
    for category in categories:
        selected = 'selected' if category == selected_category else ''
        options += f'<option value="{category}" {selected}>{category}</option>'
    
    return options