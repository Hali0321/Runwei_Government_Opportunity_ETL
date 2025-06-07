import azure.functions as func
import json
import logging
import os
from typing import Dict, List, Optional
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceNotFoundError

def main(req: func.HttpRequest) -> func.HttpResponse:
    """Enhanced grants viewer with improved Actions column and UI optimizations"""
    
    logging.info('Enhanced Grants viewer function processed a request.')
    
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
        
        # Get grants data from Azure Table Storage
        grants_data = get_grants_from_azure_table(
            search_query=search_query,
            agency_filter=agency_filter,
            category_filter=category_filter,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_order=sort_order
        )
        
        # Process data for enhanced display
        processed_grants = process_grants_for_display(grants_data)
        
        if format_type == 'html':
            return generate_enhanced_html_response(processed_grants, {
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
                json.dumps(processed_grants, default=str, indent=2),
                mimetype="application/json",
                status_code=200
            )
            
    except Exception as e:
        logging.error(f"Error in grants viewer: {str(e)}")
        return func.HttpResponse(
            f"Error processing request: {str(e)}",
            status_code=500
        )



def process_grants_for_display(grants_data: List[Dict]) -> List[Dict]:
    """Process grants data for enhanced display with improved UX elements"""
    
    processed_grants = []
    
    for grant in grants_data:
        # Extract opportunity ID for the hyperlink
        opportunity_id = grant.get('RowKey', '')
        opportunity_number = grant.get('OpportunityNumber', opportunity_id)
        
        # Create the Grants.gov URL for title link
        grants_gov_url = f"https://www.grants.gov/search-results-detail/{opportunity_id}"
        
        # Enhanced award range formatting - fix the "0.0 - 0.0" issue
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
            if award_ceiling == award_floor:
                award_range = f"${award_ceiling:,.0f}"
            else:
                award_range = f"${award_floor:,.0f} - ${award_ceiling:,.0f}"
        else:
            award_range = "Varies"
        
        # Determine grant status based on close date
        close_date = grant.get('CloseDate', '')
        status = determine_grant_status(close_date)
        
        # Enhanced CFDA processing with truncation
        cfda_full = grant.get('CFDANumbers', '')
        cfda_short = truncate_cfda_display(cfda_full)
        
        # Clean title - remove [UPDATED] tag and extract update info
        raw_title = grant.get('Title', '')
        clean_title, is_updated = clean_title_and_extract_update(raw_title)
        
        # Get additional info URL for actions column
        additional_info_url = grant.get('AdditionalInfoURL', '') or grant.get('LinkToAdditionalInformation', '')
        
        # Clean and format description with length limits
        description = grant.get('Description', '')
        short_description = description[:120] + "..." if len(description) > 120 else description
        
        processed_grant = {
            'ID': opportunity_id,
            'Title': clean_title,
            'TitleLink': grants_gov_url,  # Main grants.gov link for title
            'Agency': grant.get('AgencyName', ''),
            'Description': short_description,
            'FullDescription': description,  # For tooltip/modal
            'PostDate': format_date(grant.get('PostedDate', '')),
            'CloseDate': format_date(grant.get('CloseDate', '')),
            'CFDANumbers': cfda_short,
            'CFDANumbersFull': cfda_full,  # For tooltip
            'AwardRange': award_range,
            'Category': grant.get('Category', ''),
            'Status': status,
            'IsUpdated': is_updated,  # Extracted from title
            'UpdatedDate': format_date(grant.get('LastUpdated', '')) if is_updated else '',
            'AdditionalInfoURL': additional_info_url,  # For actions column
            'AgencyCode': grant.get('AgencyCode', ''),
            'FundingType': grant.get('FundingType', ''),
            'LastUpdated': format_date(grant.get('LastUpdated', '')),
            # Additional fields for responsive display
            'EligibleApplicants': grant.get('EligibleApplicants', ''),
            'EstimatedTotalFunding': grant.get('EstimatedTotalFunding', ''),
            'ExpectedAwards': grant.get('ExpectedAwards', '')
        }
        
        processed_grants.append(processed_grant)
    
    return processed_grants

def clean_title_and_extract_update(title: str) -> tuple[str, bool]:
    """Clean title by removing [UPDATED] tags and return update status"""
    import re
    
    # Check if title contains update indicators
    update_patterns = [
        r'\[UPDATED\]',
        r'\(UPDATED\)',
        r'- UPDATED',
        r'UPDATED:',
        r'\[UPDATE\]',
        r'\(UPDATE\)'
    ]
    
    is_updated = False
    clean_title = title
    
    for pattern in update_patterns:
        if re.search(pattern, title, re.IGNORECASE):
            is_updated = True
            clean_title = re.sub(pattern, '', title, flags=re.IGNORECASE).strip()
            # Clean up any double spaces or leading/trailing punctuation
            clean_title = re.sub(r'\s+', ' ', clean_title).strip(' -:')
    
    return clean_title, is_updated

def truncate_cfda_display(cfda_full: str) -> str:
    """Truncate CFDA numbers for display while preserving important info"""
    if not cfda_full:
        return ""
    
    # If it's short enough, return as-is
    if len(cfda_full) <= 30:
        return cfda_full
    
    # Split by common delimiters and show first few
    delimiters = [';', ',', '|', '\n']
    for delimiter in delimiters:
        if delimiter in cfda_full:
            parts = [part.strip() for part in cfda_full.split(delimiter) if part.strip()]
            if len(parts) > 1:
                return f"{parts[0]} (+{len(parts)-1} more)"
    
    # If no delimiters, just truncate
    return cfda_full[:27] + "..."

def generate_enhanced_html_response(grants_data: List[Dict], params: Dict) -> func.HttpResponse:
    """Generate enhanced HTML response with improved UX elements"""
    
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Enhanced Grants Viewer - Azure Functions</title>
    <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        /* Enhanced responsive and accessibility styles */
        .grant-status-normal {{ background-color: #10B981; color: white; }}
        .grant-status-moderate {{ background-color: #F59E0B; color: white; }}
        .grant-status-urgent {{ background-color: #F97316; color: white; }}
        .grant-status-closed {{ background-color: #EF4444; color: white; }}
        .grant-status-none {{ background-color: #6B7280; color: white; }}
        
        /* Enhanced table accessibility */
        .table-row:hover {{ background-color: #F3F4F6; }}
        .table-row:nth-child(even) {{ background-color: #F9FAFB; }}
        .table-row:focus-within {{ 
            background-color: #DBEAFE; 
            outline: 2px solid #3B82F6; 
            outline-offset: -2px;
        }}
        
        /* Clickable title styles */
        .grant-title-link {{
            color: #1D4ED8;
            text-decoration: none;
            font-weight: 500;
            transition: color 0.2s;
        }}
        
        .grant-title-link:hover {{
            color: #1E40AF;
            text-decoration: underline;
        }}
        
        .grant-title-link:focus {{
            outline: 2px solid #3B82F6;
            outline-offset: 2px;
            border-radius: 4px;
        }}
        
        /* Updated badge styles */
        .updated-badge {{
            background-color: #FBBF24;
            color: #92400E;
            font-size: 0.75rem;
            padding: 0.125rem 0.375rem;
            border-radius: 0.375rem;
            font-weight: 500;
            margin-left: 0.5rem;
        }}
        
        /* Enhanced tooltips */
        .tooltip-container {{
            position: relative;
            cursor: help;
        }}
        
        .tooltip-container:hover .tooltip-content,
        .tooltip-container:focus .tooltip-content {{
            visibility: visible;
            opacity: 1;
        }}
        
        .tooltip-content {{
            visibility: hidden;
            opacity: 0;
            position: absolute;
            z-index: 1000;
            bottom: 125%;
            left: 50%;
            margin-left: -150px;
            width: 300px;
            max-width: 90vw;
            background-color: #1F2937;
            color: white;
            text-align: left;
            border-radius: 8px;
            padding: 12px;
            transition: opacity 0.3s;
            font-size: 0.875rem;
            line-height: 1.4;
            box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.25);
        }}
        
        /* CFDA tooltip specific */
        .cfda-tooltip {{
            width: 400px;
            margin-left: -200px;
        }}
        
        /* Action icon styles */
        .action-icon {{
            color: #6B7280;
            font-size: 1.1rem;
            transition: color 0.2s;
        }}
        
        .action-icon:hover {{
            color: #4B5563;
        }}
        
        /* Responsive table improvements */
        .table-container {{ 
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
            border-radius: 8px;
        }}
        
        .table-container::-webkit-scrollbar {{
            height: 8px;
        }}
        
        .table-container::-webkit-scrollbar-track {{
            background: #F3F4F6;
            border-radius: 4px;
        }}
        
        .table-container::-webkit-scrollbar-thumb {{
            background: #9CA3AF;
            border-radius: 4px;
        }}
        
        .table-container::-webkit-scrollbar-thumb:hover {{
            background: #6B7280;
        }}
        
        /* Collapsible columns for responsive design */
        .column-priority-1 {{ display: table-cell; }}
        .column-priority-2 {{ display: table-cell; }}
        .column-priority-3 {{ display: table-cell; }}
        .column-priority-4 {{ display: table-cell; }}
        
        @media (max-width: 1200px) {{
            .column-priority-4 {{ display: none; }}
        }}
        
        @media (max-width: 1024px) {{
            .column-priority-3 {{ display: none; }}
        }}
        
        @media (max-width: 900px) {{
            .column-priority-2 {{ display: none; }}
        }}
        
        @media (max-width: 768px) {{
            .desktop-table {{ display: none; }}
            .mobile-cards {{ display: block; }}
            .column-toggle {{ display: inline-flex; }}
        }}
        
        @media (min-width: 769px) {{
            .mobile-cards {{ display: none; }}
            .column-toggle {{ display: none; }}
        }}
        
        /* Keyboard navigation styles */
        .focusable:focus {{
            outline: 2px solid #3B82F6;
            outline-offset: 2px;
            border-radius: 4px;
        }}
        
        /* Mobile card enhancements */
        .mobile-card {{
            border: 1px solid #E5E7EB;
            border-radius: 12px;
            margin-bottom: 1rem;
            padding: 1.25rem;
            background: white;
            box-shadow: 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            transition: box-shadow 0.2s;
        }}
        
        .mobile-card:hover {{
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        }}
        
        .mobile-card:focus-within {{
            box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.15);
            border-color: #3B82F6;
        }}
        
        /* Screen reader only content */
        .sr-only {{
            position: absolute;
            width: 1px;
            height: 1px;
            padding: 0;
            margin: -1px;
            overflow: hidden;
            clip: rect(0, 0, 0, 0);
            white-space: nowrap;
            border: 0;
        }}
        
        /* Enhanced button states */
        .btn-primary {{
            background-color: #3B82F6;
            color: white;
            transition: all 0.2s;
        }}
        
        .btn-primary:hover {{
            background-color: #2563EB;
            transform: translateY(-1px);
        }}
        
        .btn-primary:focus {{
            outline: 2px solid #93C5FD;
            outline-offset: 2px;
        }}
        
        .btn-primary:active {{
            transform: translateY(0);
        }}
        
        /* Loading states */
        .loading {{
            opacity: 0.6;
            pointer-events: none;
        }}
        
        /* More columns toggle */
        .more-columns-panel {{
            background: #F9FAFB;
            border-top: 1px solid #E5E7EB;
            padding: 1rem;
            margin-top: 0.5rem;
        }}
        
        .hidden {{
            display: none !important;
        }}
    </style>
</head>
<body class="bg-gray-50 font-sans">
    <!-- Skip to main content for screen readers -->
    <a href="#main-content" class="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 bg-blue-600 text-white px-4 py-2 rounded-md z-50">
        Skip to main content
    </a>
    
    <div class="container mx-auto px-4 py-8 max-w-7xl">
        <!-- Enhanced Header with Toolbar -->
        <div class="bg-white rounded-lg shadow-sm border border-gray-200 mb-6">
            <div class="px-6 py-4 border-b border-gray-200">
                <div class="flex flex-col md:flex-row md:items-center md:justify-between">
                    <div>
                        <h1 class="text-2xl font-bold text-gray-900" id="page-title">
                            <i class="fas fa-search mr-2 text-blue-600" aria-hidden="true"></i>
                            Enhanced Grants Viewer
                        </h1>
                        <p class="text-gray-600 mt-1" id="results-summary" aria-live="polite">
                            Showing {len(grants_data)} grants • Page {params['page']} • Azure Functions Powered
                        </p>
                    </div>
                    <div class="flex items-center space-x-2 mt-4 md:mt-0" role="toolbar" aria-label="Export and view options">
                        <button onclick="exportToJSON(1000)" 
                                class="btn-primary px-4 py-2 rounded-lg flex items-center focusable"
                                aria-label="Export up to 1000 records as JSON">
                            <i class="fas fa-download mr-2" aria-hidden="true"></i>
                            Export JSON (1000)
                        </button>
                        <button onclick="exportAllToJSON()" 
                                class="btn-primary px-4 py-2 rounded-lg flex items-center focusable"
                                aria-label="Export all records as JSON">
                            <i class="fas fa-file-export mr-2" aria-hidden="true"></i>
                            Export All JSON
                        </button>
                        <button onclick="toggleColumnSelector()" 
                                class="bg-gray-600 hover:bg-gray-700 text-white px-4 py-2 rounded-lg transition-colors duration-200 flex items-center focusable"
                                aria-label="Select which columns to display">
                            <i class="fas fa-columns mr-2" aria-hidden="true"></i>
                            Columns
                        </button>
                        <button onclick="toggleMoreColumns()" 
                                class="column-toggle bg-purple-600 hover:bg-purple-700 text-white px-4 py-2 rounded-lg transition-colors duration-200 flex items-center focusable"
                                aria-label="Show additional columns">
                            <i class="fas fa-plus mr-2" aria-hidden="true"></i>
                            + More
                        </button>
                    </div>
                </div>
            </div>
            
            <!-- Enhanced Search and Filter Bar -->
            <div class="px-6 py-4">
                <form onsubmit="applyFilters(); return false;" role="search" aria-label="Filter grants">
                    <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
                        <div>
                            <label for="searchInput" class="block text-sm font-medium text-gray-700 mb-1">Search Keywords</label>
                            <input type="text" 
                                   id="searchInput" 
                                   name="search"
                                   value="{params['search']}" 
                                   placeholder="Search titles, descriptions..." 
                                   class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focusable"
                                   aria-describedby="search-help">
                            <div id="search-help" class="sr-only">Enter keywords to search grant titles and descriptions</div>
                        </div>
                        <div>
                            <label for="agencyFilter" class="block text-sm font-medium text-gray-700 mb-1">Agency</label>
                            <select id="agencyFilter" 
                                    name="agency"
                                    class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focusable"
                                    aria-describedby="agency-help">
                                <option value="">All Agencies</option>
                                {generate_agency_options(grants_data, params['agency'])}
                            </select>
                            <div id="agency-help" class="sr-only">Filter grants by issuing agency</div>
                        </div>
                        <div>
                            <label for="categoryFilter" class="block text-sm font-medium text-gray-700 mb-1">Category</label>
                            <select id="categoryFilter" 
                                    name="category"
                                    class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focusable"
                                    aria-describedby="category-help">
                                <option value="">All Categories</option>
                                {generate_category_options(grants_data, params['category'])}
                            </select>
                            <div id="category-help" class="sr-only">Filter grants by funding category</div>
                        </div>
                        <div class="flex items-end">
                            <button type="submit" 
                                    class="w-full btn-primary px-4 py-2 rounded-md focusable"
                                    aria-label="Apply the selected filters to search results">
                                <i class="fas fa-search mr-2" aria-hidden="true"></i>Apply Filters
                            </button>
                        </div>
                    </div>
                </form>
            </div>
        </div>

        <!-- Enhanced Data Table for Desktop with Improved UX -->
        <div class="desktop-table bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden" 
             id="main-content" 
             role="region" 
             aria-labelledby="page-title"
             aria-describedby="results-summary">
            
            <div class="table-container" role="table" aria-label="Grants listing">
                <table class="min-w-full divide-y divide-gray-200">
                    <thead class="bg-gray-50" role="rowgroup">
                        <tr role="row">
                            <th class="column-priority-1 px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 focusable" 
                                onclick="sortBy('ID')" 
                                onkeydown="handleKeyPress(event, () => sortBy('ID'))"
                                role="columnheader" 
                                aria-sort="{get_aria_sort('ID', params['sort_by'], params['sort_order'])}"
                                tabindex="0">
                                ID <i class="fas fa-sort ml-1" aria-hidden="true"></i>
                                <span class="sr-only">Sort by ID</span>
                            </th>
                            <th class="column-priority-1 px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 focusable" 
                                onclick="sortBy('Title')" 
                                onkeydown="handleKeyPress(event, () => sortBy('Title'))"
                                role="columnheader" 
                                aria-sort="{get_aria_sort('Title', params['sort_by'], params['sort_order'])}"
                                tabindex="0">
                                Title <i class="fas fa-sort ml-1" aria-hidden="true"></i>
                                <span class="sr-only">Sort by Title</span>
                            </th>
                            <th class="column-priority-2 px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 focusable" 
                                onclick="sortBy('Agency')" 
                                onkeydown="handleKeyPress(event, () => sortBy('Agency'))"
                                role="columnheader" 
                                aria-sort="{get_aria_sort('Agency', params['sort_by'], params['sort_order'])}"
                                tabindex="0">
                                Agency <i class="fas fa-sort ml-1" aria-hidden="true"></i>
                                <span class="sr-only">Sort by Agency</span>
                            </th>
                            <th class="column-priority-3 px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                                role="columnheader">
                                Description
                            </th>
                            <th class="column-priority-2 px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 focusable" 
                                onclick="sortBy('PostDate')" 
                                onkeydown="handleKeyPress(event, () => sortBy('PostDate'))"
                                role="columnheader" 
                                aria-sort="{get_aria_sort('PostDate', params['sort_by'], params['sort_order'])}"
                                tabindex="0">
                                Post Date <i class="fas fa-sort ml-1" aria-hidden="true"></i>
                                <span class="sr-only">Sort by Post Date</span>
                            </th>
                            <th class="column-priority-1 px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 focusable" 
                                onclick="sortBy('CloseDate')" 
                                onkeydown="handleKeyPress(event, () => sortBy('CloseDate'))"
                                role="columnheader" 
                                aria-sort="{get_aria_sort('CloseDate', params['sort_by'], params['sort_order'])}"
                                tabindex="0">
                                Close Date <i class="fas fa-sort ml-1" aria-hidden="true"></i>
                                <span class="sr-only">Sort by Close Date</span>
                            </th>
                            <th class="column-priority-4 px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                                role="columnheader">
                                CFDA Numbers
                            </th>
                            <th class="column-priority-2 px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 focusable" 
                                onclick="sortBy('AwardRange')" 
                                onkeydown="handleKeyPress(event, () => sortBy('AwardRange'))"
                                role="columnheader" 
                                aria-sort="{get_aria_sort('AwardRange', params['sort_by'], params['sort_order'])}"
                                tabindex="0">
                                Award Range <i class="fas fa-sort ml-1" aria-hidden="true"></i>
                                <span class="sr-only">Sort by Award Range</span>
                            </th>
                            <th class="column-priority-3 px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                                role="columnheader">
                                Category
                            </th>
                            <th class="column-priority-1 px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                                role="columnheader">
                                Status
                            </th>
                            <th class="column-priority-3 px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                                role="columnheader">
                                Updated
                            </th>
                            <th class="column-priority-2 px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                                role="columnheader">
                                Actions
                            </th>
                        </tr>
                    </thead>
                    <tbody class="bg-white divide-y divide-gray-200" role="rowgroup">
                        {generate_improved_table_rows(grants_data)}
                    </tbody>
                </table>
            </div>
            
            <!-- More Columns Panel (Collapsible) -->
            <div id="moreColumnsPanel" class="more-columns-panel hidden">
                <h3 class="text-sm font-medium text-gray-700 mb-3">Additional Grant Information</h3>
                <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 text-sm">
                    <!-- Additional fields will be populated by JavaScript -->
                </div>
            </div>
        </div>
        
        <!-- Enhanced Mobile Card Layout -->
        <div class="mobile-cards" role="region" aria-label="Grants listing for mobile devices">
            {generate_improved_mobile_cards(grants_data)}
        </div>
        
        <!-- Enhanced Pagination with Accessibility -->
        <nav class="mt-6 flex flex-col md:flex-row md:items-center md:justify-between bg-white px-6 py-3 rounded-lg shadow-sm border border-gray-200"
             role="navigation" 
             aria-label="Pagination Navigation">
            <div class="flex items-center text-sm text-gray-700" aria-live="polite">
                <span>Showing page {params['page']} of grants</span>
            </div>
            <div class="flex items-center space-x-2 mt-3 md:mt-0">
                <button onclick="changePage({params['page'] - 1})" 
                        class="bg-gray-100 hover:bg-gray-200 text-gray-700 px-3 py-2 rounded-md transition-colors duration-200 focusable {'cursor-not-allowed opacity-50' if params['page'] <= 1 else ''}"
                        {'disabled' if params['page'] <= 1 else ''}
                        aria-label="Go to previous page"
                        {'aria-disabled="true"' if params['page'] <= 1 else ''}>
                    <i class="fas fa-chevron-left mr-1" aria-hidden="true"></i>Previous
                </button>
                <span class="px-3 py-2 bg-blue-600 text-white rounded-md" aria-current="page">
                    Page {params['page']}
                </span>
                <button onclick="changePage({params['page'] + 1})" 
                        class="bg-gray-100 hover:bg-gray-200 text-gray-700 px-3 py-2 rounded-md transition-colors duration-200 focusable"
                        aria-label="Go to next page">
                    Next<i class="fas fa-chevron-right ml-1" aria-hidden="true"></i>
                </button>
            </div>
        </nav>
    </div>

    <!-- Rest of the existing modals and JavaScript -->
    {generate_column_selector_modal()}

    <script>
        {generate_enhanced_javascript(params)}
    </script>
</body>
</html>
"""
    
    return func.HttpResponse(html_content, mimetype="text/html", status_code=200)

def generate_improved_table_rows(grants_data: List[Dict]) -> str:
    """Generate enhanced table rows with improved UX elements"""
    rows_html = ""
    
    for index, grant in enumerate(grants_data):
        status = grant['Status']
        status_class = f"grant-status-{status['urgency']}"
        
        # Create clickable title with update badge
        title_cell = f"""
        <div class="flex items-center">
            <a href="{grant['TitleLink']}" 
               target="_blank" 
               class="grant-title-link"
               aria-label="View grant details: {grant['Title']}">
                {grant['Title']}
            </a>
            {f'<span class="updated-badge" title="Updated on {grant["UpdatedDate"]}">NEW</span>' if grant['IsUpdated'] else ''}
        </div>
        """
        
        # Create CFDA cell with tooltip
        cfda_cell = f"""
        <div class="tooltip-container" tabindex="0">
            <span class="text-gray-900">{grant['CFDANumbers']}</span>
            <div class="tooltip-content cfda-tooltip" role="tooltip">
                <strong>Full CFDA Numbers:</strong><br>
                {grant['CFDANumbersFull'] or 'No CFDA numbers specified'}
            </div>
        </div>
        """ if grant['CFDANumbers'] else "N/A"
        
        # Create description cell with tooltip
        description_cell = f"""
        <div class="tooltip-container" 
             tabindex="0" 
             aria-describedby="desc-{grant['ID']}"
             role="button"
             aria-label="Show full description">
            <span class="text-gray-900">{grant['Description']}</span>
            <div id="desc-{grant['ID']}" class="tooltip-content" role="tooltip">
                {grant['FullDescription']}
            </div>
        </div>
        """
        
        # Create updated column
        updated_cell = f"""
        <div class="text-center">
            {f'<span class="updated-badge" title="Updated on {grant["UpdatedDate"]}">✓</span>' if grant['IsUpdated'] else ''}
        </div>
        """
        
        # Create simplified actions column with additional info link
        actions_cell = ""
        if grant['AdditionalInfoURL']:
            actions_cell = f"""
            <div class="flex items-center justify-center">
                <a href="{grant['AdditionalInfoURL']}" 
                   target="_blank" 
                   class="action-icon focusable"
                   aria-label="View additional information"
                   title="Additional Information">
                    <i class="fas fa-paperclip"></i>
                </a>
            </div>
            """
        else:
            actions_cell = '<div class="text-center text-gray-300">—</div>'
        
        rows_html += f"""
        <tr class="table-row" 
            role="row"
            aria-rowindex="{index + 2}"
            data-grant-id="{grant['ID']}">
            <td class="column-priority-1 px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900" 
                role="gridcell">{grant['ID']}</td>
            <td class="column-priority-1 px-6 py-4 text-sm text-gray-900 max-w-xs" 
                role="gridcell">{title_cell}</td>
            <td class="column-priority-2 px-6 py-4 whitespace-nowrap text-sm text-gray-900" 
                role="gridcell">{grant['Agency']}</td>
            <td class="column-priority-3 px-6 py-4 text-sm text-gray-900 max-w-md" 
                role="gridcell">{description_cell}</td>
            <td class="column-priority-2 px-6 py-4 whitespace-nowrap text-sm text-gray-900" 
                role="gridcell">{grant['PostDate']}</td>
            <td class="column-priority-1 px-6 py-4 whitespace-nowrap text-sm text-gray-900" 
                role="gridcell">{grant['CloseDate']}</td>
            <td class="column-priority-4 px-6 py-4 text-sm text-gray-900" 
                role="gridcell">{cfda_cell}</td>
            <td class="column-priority-2 px-6 py-4 whitespace-nowrap text-sm text-gray-900" 
                role="gridcell">{grant['AwardRange']}</td>
            <td class="column-priority-3 px-6 py-4 text-sm text-gray-900" 
                role="gridcell">{grant['Category']}</td>
            <td class="column-priority-1 px-6 py-4 whitespace-nowrap" 
                role="gridcell">
                <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium {status_class}"
                      aria-label="Grant status: {status['status']}">
                    {status['status']}
                </span>
            </td>
            <td class="column-priority-3 px-6 py-4 whitespace-nowrap text-sm" 
                role="gridcell">{updated_cell}</td>
            <td class="column-priority-2 px-6 py-4 whitespace-nowrap text-sm" 
                role="gridcell">{actions_cell}</td>
        </tr>
        """
    
    return rows_html

def generate_improved_mobile_cards(grants_data: List[Dict]) -> str:
    """Generate improved mobile-friendly card layout"""
    cards_html = ""
    
    for index, grant in enumerate(grants_data):
        status = grant['Status']
        status_class = f"grant-status-{status['urgency']}"
        
        # Additional info link for mobile
        additional_link = ""
        if grant['AdditionalInfoURL']:
            additional_link = f"""
            <a href="{grant['AdditionalInfoURL']}" 
               target="_blank" 
               class="text-gray-500 hover:text-gray-700 ml-2"
               aria-label="Additional information">
                <i class="fas fa-paperclip"></i>
            </a>
            """
        
        cards_html += f"""
        <article class="mobile-card" 
                 tabindex="0"
                 role="article"
                 aria-labelledby="card-title-{grant['ID']}"
                 aria-describedby="card-desc-{grant['ID']}">
            <div class="flex items-start justify-between mb-3">
                <div class="flex-1">
                    <h3 id="card-title-{grant['ID']}" class="mb-1">
                        <a href="{grant['TitleLink']}" 
                           target="_blank" 
                           class="grant-title-link">
                            {grant['Title']}
                        </a>
                        {f'<span class="updated-badge ml-2">NEW</span>' if grant['IsUpdated'] else ''}
                    </h3>
                    <p class="text-sm text-gray-600 flex items-center">
                        {grant['Agency']}
                        {additional_link}
                    </p>
                </div>
                <span class="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium {status_class} ml-2"
                      aria-label="Status: {status['status']}">
                    {status['status']}
                </span>
            </div>
            
            <div class="grid grid-cols-2 gap-3 text-sm mb-4">
                <div>
                    <span class="font-medium text-gray-700">Post Date:</span>
                    <span class="text-gray-900">{grant['PostDate']}</span>
                </div>
                <div>
                    <span class="font-medium text-gray-700">Close Date:</span>
                    <span class="text-gray-900">{grant['CloseDate']}</span>
                </div>
                <div>
                    <span class="font-medium text-gray-700">Award Range:</span>
                    <span class="text-gray-900">{grant['AwardRange']}</span>
                </div>
                <div>
                    <span class="font-medium text-gray-700">Category:</span>
                    <span class="text-gray-900">{grant['Category']}</span>
                </div>
                {f'''
                <div class="col-span-2">
                    <span class="font-medium text-gray-700">CFDA:</span>
                    <span class="text-gray-900">{grant['CFDANumbers']}</span>
                </div>
                ''' if grant['CFDANumbers'] else ''}
            </div>
            
            <div class="mb-3">
                <p id="card-desc-{grant['ID']}" class="text-sm text-gray-700">{grant['Description']}</p>
            </div>
            
            <div class="flex items-center justify-between text-xs text-gray-500">
                <span>ID: {grant['ID']}</span>
                {f'<span class="updated-badge text-xs">Updated {grant["UpdatedDate"]}</span>' if grant['IsUpdated'] else ''}
            </div>
        </article>
        """
    
    return cards_html

def generate_column_selector_modal() -> str:
    """Generate the column selector modal HTML"""
    return """
    <!-- Enhanced Column Selector Modal with Accessibility -->
    <div id="columnSelector" 
         class="fixed inset-0 bg-gray-600 bg-opacity-50 hidden z-50" 
         role="dialog" 
         aria-labelledby="column-selector-title" 
         aria-modal="true">
        <div class="flex items-center justify-center min-h-screen p-4">
            <div class="bg-white rounded-lg shadow-xl max-w-md w-full">
                <div class="px-6 py-4 border-b border-gray-200">
                    <div class="flex items-center justify-between">
                        <h3 id="column-selector-title" class="text-lg font-medium text-gray-900">
                            <i class="fas fa-columns mr-2" aria-hidden="true"></i>Select Columns
                        </h3>
                        <button onclick="toggleColumnSelector()" 
                                class="text-gray-400 hover:text-gray-600 focusable"
                                aria-label="Close column selector">
                            <i class="fas fa-times" aria-hidden="true"></i>
                        </button>
                    </div>
                    <div class="mt-2">
                        <label for="columnSearch" class="sr-only">Search columns</label>
                        <input type="text" 
                               id="columnSearch" 
                               placeholder="Search columns..." 
                               class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm focusable">
                    </div>
                </div>
                <div class="px-6 py-4 max-h-96 overflow-y-auto">
                    <div class="space-y-2">
                        <div class="flex items-center justify-between border-b pb-2 mb-3">
                            <button onclick="selectAllColumns()" 
                                    class="text-blue-600 hover:text-blue-800 text-sm focusable"
                                    aria-label="Select all columns">
                                <i class="fas fa-check-square mr-1" aria-hidden="true"></i>Select All
                            </button>
                            <button onclick="deselectAllColumns()" 
                                    class="text-red-600 hover:text-red-800 text-sm focusable"
                                    aria-label="Deselect all columns">
                                <i class="fas fa-square mr-1" aria-hidden="true"></i>Deselect All
                            </button>
                        </div>
                        
                        <!-- Currently Visible Columns -->
                        <div class="mb-4">
                            <h4 class="text-sm font-medium text-gray-700 mb-2">
                                <i class="fas fa-eye text-green-500 mr-1" aria-hidden="true"></i>Currently Visible
                            </h4>
                            <div class="space-y-2">
                                <div class="flex items-center column-option">
                                    <input type="checkbox" id="col_ID" checked class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded focusable">
                                    <label for="col_ID" class="ml-2 text-sm text-gray-900">ID</label>
                                </div>
                                <div class="flex items-center column-option">
                                    <input type="checkbox" id="col_Title" checked class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded focusable">
                                    <label for="col_Title" class="ml-2 text-sm text-gray-900">Title (Clickable)</label>
                                </div>
                                <div class="flex items-center column-option">
                                    <input type="checkbox" id="col_Agency" checked class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded focusable">
                                    <label for="col_Agency" class="ml-2 text-sm text-gray-900">Agency</label>
                                </div>
                                <div class="flex items-center column-option">
                                    <input type="checkbox" id="col_Status" checked class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded focusable">
                                    <label for="col_Status" class="ml-2 text-sm text-gray-900">Status</label>
                                </div>
                                <div class="flex items-center column-option">
                                    <input type="checkbox" id="col_CloseDate" checked class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded focusable">
                                    <label for="col_CloseDate" class="ml-2 text-sm text-gray-900">Close Date</label>
                                </div>
                                <div class="flex items-center column-option">
                                    <input type="checkbox" id="col_Updated" checked class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded focusable">
                                    <label for="col_Updated" class="ml-2 text-sm text-gray-900">Updated</label>
                                </div>
                                <div class="flex items-center column-option">
                                    <input type="checkbox" id="col_Actions" checked class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded focusable">
                                    <label for="col_Actions" class="ml-2 text-sm text-gray-900">Actions (Additional Info)</label>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Available Columns -->
                        <div>
                            <h4 class="text-sm font-medium text-gray-700 mb-2">
                                <i class="fas fa-eye-slash text-gray-400 mr-1" aria-hidden="true"></i>Available Columns
                            </h4>
                            <div class="space-y-2">
                                <div class="flex items-center column-option">
                                    <input type="checkbox" id="col_Description" class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded focusable">
                                    <label for="col_Description" class="ml-2 text-sm text-gray-900">Description</label>
                                </div>
                                <div class="flex items-center column-option">
                                    <input type="checkbox" id="col_PostDate" class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded focusable">
                                    <label for="col_PostDate" class="ml-2 text-sm text-gray-900">Post Date</label>
                                </div>
                                <div class="flex items-center column-option">
                                    <input type="checkbox" id="col_CFDA" class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded focusable">
                                    <label for="col_CFDA" class="ml-2 text-sm text-gray-900">CFDA Numbers</label>
                                </div>
                                <div class="flex items-center column-option">
                                    <input type="checkbox" id="col_AwardRange" class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded focusable">
                                    <label for="col_AwardRange" class="ml-2 text-sm text-gray-900">Award Range</label>
                                </div>
                                <div class="flex items-center column-option">
                                    <input type="checkbox" id="col_Category" class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded focusable">
                                    <label for="col_Category" class="ml-2 text-sm text-gray-900">Category</label>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="px-6 py-4 border-t border-gray-200">
                    <button onclick="applyColumnChanges()" 
                            class="w-full btn-primary px-4 py-2 rounded-md focusable">
                        Apply Changes
                    </button>
                </div>
            </div>
        </div>
    </div>
    """

def generate_enhanced_javascript(params: Dict) -> str:
    """Generate enhanced JavaScript with all the improved functionality"""
    return f"""
        // Enhanced JavaScript with improved UX
        let currentSort = '{params['sort_by']}';
        let currentOrder = '{params['sort_order']}';
        let moreColumnsVisible = false;
        
        // Keyboard navigation handler
        function handleKeyPress(event, callback) {{
            if (event.key === 'Enter' || event.key === ' ') {{
                event.preventDefault();
                callback();
            }}
        }}
        
        // Enhanced sorting with accessibility
        function sortBy(column) {{
            if (currentSort === column) {{
                currentOrder = currentOrder === 'asc' ? 'desc' : 'asc';
            }} else {{
                currentSort = column;
                currentOrder = 'asc';
            }}
            
            // Announce sort change to screen readers
            announceToScreenReader(`Sorting by ${{column}} in ${{currentOrder}}ending order`);
            
            applyFilters();
        }}
        
        // Enhanced filter application with loading state
        function applyFilters() {{
            const form = document.querySelector('form[role="search"]');
            if (form) form.classList.add('loading');
            
            const search = document.getElementById('searchInput').value;
            const agency = document.getElementById('agencyFilter').value;
            const category = document.getElementById('categoryFilter').value;
            
            const params = new URLSearchParams({{
                search: search,
                agency: agency,
                category: category,
                sort: currentSort,
                order: currentOrder,
                format: 'html',
                limit: {params['limit']},
                page: 1
            }});
            
            // Announce filter application
            announceToScreenReader('Applying filters and refreshing results');
            
            window.location.href = '?' + params.toString();
        }}
        
        // Enhanced pagination with accessibility
        function changePage(page) {{
            if (page < 1) return;
            
            announceToScreenReader(`Navigating to page ${{page}}`);
            
            const params = new URLSearchParams(window.location.search);
            params.set('page', page);
            window.location.href = '?' + params.toString();
        }}
        
        // Export functions with accessibility
        function exportToJSON(limit) {{
            announceToScreenReader(`Exporting up to ${{limit}} records`);
            const params = new URLSearchParams(window.location.search);
            params.set('format', 'json');
            params.set('limit', limit);
            window.open('?' + params.toString(), '_blank');
        }}
        
        function exportAllToJSON() {{
            announceToScreenReader('Exporting all records');
            const params = new URLSearchParams(window.location.search);
            params.set('format', 'json');
            params.set('limit', 10000);
            window.open('?' + params.toString(), '_blank');
        }}
        
        // Enhanced column selector with accessibility
        function toggleColumnSelector() {{
            const modal = document.getElementById('columnSelector');
            const isHidden = modal.classList.contains('hidden');
            
            modal.classList.toggle('hidden');
            
            if (!isHidden) {{
                // Closing modal
                document.body.style.overflow = '';
                announceToScreenReader('Column selector closed');
            }} else {{
                // Opening modal
                document.body.style.overflow = 'hidden';
                announceToScreenReader('Column selector opened');
                
                // Focus the first interactive element
                setTimeout(() => {{
                    const firstInput = modal.querySelector('input, button');
                    if (firstInput) firstInput.focus();
                }}, 100);
            }}
        }}
        
        // More columns toggle for responsive design
        function toggleMoreColumns() {{
            const panel = document.getElementById('moreColumnsPanel');
            const button = document.querySelector('.column-toggle');
            
            moreColumnsVisible = !moreColumnsVisible;
            
            if (moreColumnsVisible) {{
                panel.classList.remove('hidden');
                button.innerHTML = '<i class="fas fa-minus mr-2" aria-hidden="true"></i>- Less';
                button.setAttribute('aria-label', 'Hide additional columns');
                announceToScreenReader('Additional columns shown');
                
                // Populate additional data
                populateMoreColumnsData();
            }} else {{
                panel.classList.add('hidden');
                button.innerHTML = '<i class="fas fa-plus mr-2" aria-hidden="true"></i>+ More';
                button.setAttribute('aria-label', 'Show additional columns');
                announceToScreenReader('Additional columns hidden');
            }}
        }}
        
        // Populate additional columns data
        function populateMoreColumnsData() {{
            const panel = document.getElementById('moreColumnsPanel');
            const grid = panel.querySelector('.grid');
            
            // This would be populated with actual grant data
            grid.innerHTML = `
                <div>
                    <span class="font-medium text-gray-700">Funding Type:</span>
                    <span class="text-gray-900">Grant</span>
                </div>
                <div>
                    <span class="font-medium text-gray-700">Eligible Applicants:</span>
                    <span class="text-gray-900">Varies by grant</span>
                </div>
                <div>
                    <span class="font-medium text-gray-700">Last Updated:</span>
                    <span class="text-gray-900">Recently</span>
                </div>
            `;
        }}
        
        function selectAllColumns() {{
            document.querySelectorAll('#columnSelector input[type="checkbox"]').forEach(cb => {{
                cb.checked = true;
                cb.dispatchEvent(new Event('change'));
            }});
            announceToScreenReader('All columns selected');
        }}
        
        function deselectAllColumns() {{
            document.querySelectorAll('#columnSelector input[type="checkbox"]').forEach(cb => {{
                cb.checked = false;
                cb.dispatchEvent(new Event('change'));
            }});
            announceToScreenReader('All columns deselected');
        }}
        
        function applyColumnChanges() {{
            // This would implement the column visibility logic
            announceToScreenReader('Column changes applied');
            toggleColumnSelector();
        }}
        
        // Accessibility announcement function
        function announceToScreenReader(message) {{
            const announcement = document.createElement('div');
            announcement.setAttribute('aria-live', 'polite');
            announcement.setAttribute('aria-atomic', 'true');
            announcement.className = 'sr-only';
            announcement.textContent = message;
            
            document.body.appendChild(announcement);
            
            setTimeout(() => {{
                document.body.removeChild(announcement);
            }}, 1000);
        }}
        
        // Toast notification for visual feedback
        function showToast(message) {{
            const toast = document.createElement('div');
            toast.className = 'fixed top-4 right-4 bg-green-600 text-white px-6 py-3 rounded-lg shadow-lg z-50 transition-all duration-300';
            toast.textContent = message;
            
            document.body.appendChild(toast);
            
            setTimeout(() => {{
                toast.style.opacity = '0';
                toast.style.transform = 'translateY(-20px)';
                setTimeout(() => {{
                    if (document.body.contains(toast)) {{
                        document.body.removeChild(toast);
                    }}
                }}, 300);
            }}, 3000);
        }}
        
        // Enhanced search functionality for column selector
        document.addEventListener('DOMContentLoaded', function() {{
            const columnSearch = document.getElementById('columnSearch');
            if (columnSearch) {{
                columnSearch.addEventListener('input', function() {{
                    const searchTerm = this.value.toLowerCase();
                    const options = document.querySelectorAll('#columnSelector .column-option');
                    let visibleCount = 0;
                    
                    options.forEach(option => {{
                        const text = option.textContent.toLowerCase();
                        const isVisible = text.includes(searchTerm);
                        option.style.display = isVisible ? 'flex' : 'none';
                        if (isVisible) visibleCount++;
                    }});
                    
                    announceToScreenReader(`${{visibleCount}} columns match search`);
                }});
            }}
            
            // Escape key handling for modal
            document.addEventListener('keydown', function(event) {{
                if (event.key === 'Escape') {{
                    const modal = document.getElementById('columnSelector');
                    if (modal && !modal.classList.contains('hidden')) {{
                        toggleColumnSelector();
                    }}
                }}
            }});
            
            // Trap focus in modal
            const modal = document.getElementById('columnSelector');
            if (modal) {{
                modal.addEventListener('keydown', function(event) {{
                    if (event.key === 'Tab') {{
                        const focusableElements = modal.querySelectorAll('button, input, select, textarea, [tabindex]:not([tabindex="-1"])');
                        const firstElement = focusableElements[0];
                        const lastElement = focusableElements[focusableElements.length - 1];
                        
                        if (event.shiftKey && document.activeElement === firstElement) {{
                            event.preventDefault();
                            lastElement.focus();
                        }} else if (!event.shiftKey && document.activeElement === lastElement) {{
                            event.preventDefault();
                            firstElement.focus();
                        }}
                    }}
                }});
            }}
        }});
    """

def generate_accessible_column_checkboxes(columns: List[str], checked: bool) -> str:
    """Generate accessible column selector checkboxes"""
    checkboxes_html = ""
    
    for column in columns:
        checked_attr = 'checked' if checked else ''
        column_label = column.replace('_', ' ').replace('ViewOnGrantsGov', 'View on Grants.gov').title()
        
        checkboxes_html += f"""
        <div class="flex items-center column-option">
            <input type="checkbox" 
                   id="col_{column}" 
                   {checked_attr} 
                   class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded focusable"
                   aria-describedby="col_{column}_desc">
            <label for="col_{column}" class="ml-2 text-sm text-gray-900">{column_label}</label>
            <div id="col_{column}_desc" class="sr-only">Toggle visibility of {column_label} column</div>
        </div>
        """
    
    return checkboxes_html

def get_aria_sort(column: str, current_sort: str, current_order: str) -> str:
    """Get appropriate aria-sort value for accessibility"""
    if column == current_sort:
        return 'ascending' if current_order == 'asc' else 'descending'
    return 'none'

# ...rest of existing helper functions...

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

def determine_grant_status(close_date: str) -> Dict[str, str]:
    """Determine grant status based on close date"""
    from datetime import datetime, timedelta
    
    if not close_date:
        return {
            'status': 'No Close Date',
            'urgency': 'none'
        }
    
    try:
        close_dt = parse_date_flexible(close_date)
        now = datetime.now()
        days_until_close = (close_dt - now).days
        
        if days_until_close < 0:
            return {
                'status': 'Closed',
                'urgency': 'closed'
            }
        elif days_until_close <= 7:
            return {
                'status': 'Closing Soon',
                'urgency': 'urgent'
            }
        elif days_until_close <= 30:
            return {
                'status': 'Closing This Month',
                'urgency': 'moderate'
            }
        else:
            return {
                'status': 'Open',
                'urgency': 'normal'
            }
    except:
        return {
            'status': 'Unknown',
            'urgency': 'none'
        }

def generate_column_checkboxes(columns: List[str], checked: bool) -> str:
    """Generate column selector checkboxes"""
    checkboxes_html = ""
    
    for column in columns:
        checked_attr = 'checked' if checked else ''
        checkboxes_html += f"""
        <div class="flex items-center column-option">
            <input type="checkbox" id="col_{column}" {checked_attr} 
                   class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded">
            <label for="col_{column}" class="ml-2 text-sm text-gray-900">{column.replace('_', ' ').title()}</label>
        </div>
        """
    
    return checkboxes_html

# Helper functions
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

def get_grants_from_azure_table(search_query: str = "", agency_filter: str = "", 
                               category_filter: str = "", limit: int = 100, 
                               offset: int = 0, sort_by: str = "PostedDate", 
                               sort_order: str = "desc") -> List[Dict]:
    """Get grants from Azure Table Storage with enhanced filtering and sorting"""
    
    try:
        # Get connection string
        connection_string = os.environ.get("AzureWebJobsStorage")
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
        entities = list(table_client.query_entities(
            query_filter=filter_query,
            results_per_page=limit + offset
        ))
        
        # Convert to list of dicts and apply sorting
        grants_list = [dict(entity) for entity in entities]
        
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