import azure.functions as func
import json
import logging
import os
from typing import Dict, List, Optional
from azure.data.tables import TableServiceClient
from datetime import datetime

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Unified Grants API with integrated Azure Table Storage data
    """
    logging.info('Unified Grants API processed a request.')
    
    try:
        # Get route parameters
        format_type = req.route_params.get('format', 'innovative')
        
        # Get query parameters with defaults
        search = req.params.get('search', '')
        agency = req.params.get('agency', '')
        domain = req.params.get('domain', '')
        funding_min = int(req.params.get('funding_min', 0))
        funding_max = int(req.params.get('funding_max', 999999999))
        limit = int(req.params.get('limit', 10))
        sort_by = req.params.get('sort', 'relevance')
        output = req.params.get('output', 'json')
        
        # Validate format
        if format_type not in ['innovative', 'competitor']:
            format_type = 'innovative'
        
        # Get data based on format type
        if format_type == 'competitor':
            grants_data = get_competitor_format_data(search, agency, funding_min, funding_max, limit, sort_by)
        else:
            grants_data = get_innovative_format_data(search, agency, domain, funding_min, funding_max, limit, sort_by)
        
        # Return appropriate response format
        if output.lower() == 'html':
            return generate_html_response(grants_data, format_type)
        else:
            # JSON response with enhanced metadata
            response_data = {
                'format_type': format_type,
                'total_results': len(grants_data),
                'grants': grants_data,
                'filters_applied': {
                    'search': search,
                    'agency': agency,
                    'domain': domain if format_type == 'innovative' else '',
                    'funding_range': [funding_min, funding_max]
                },
                'metadata': {
                    'generated_at': datetime.utcnow().isoformat(),
                    'data_source': 'azure_table_storage',
                    'api_version': '2.0'
                }
            }
            
            return func.HttpResponse(
                json.dumps(response_data, indent=2),
                status_code=200,
                headers={"Content-Type": "application/json"}
            )
    
    except Exception as e:
        logging.error(f"Unified Grants API error: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                'error': str(e),
                'message': 'Failed to retrieve grants data'
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )

def get_competitor_format_data(search, agency, funding_min, funding_max, limit, sort_by):
    """Get data in competitor format from Azure Table Storage"""
    try:
        # Get data from Azure Table Storage
        raw_grants = fetch_grants_from_table(search, agency, limit)
        
        # Transform to competitor format
        competitor_grants = []
        for grant in raw_grants:
            competitor_grant = transform_to_competitor_format(grant)
            competitor_grants.append(competitor_grant)
        
        return competitor_grants
    
    except Exception as e:
        logging.error(f"Error fetching competitor data: {str(e)}")
        # Fallback to sample data
        return load_sample_competitor_data()[:limit]

def get_innovative_format_data(search, agency, domain, funding_min, funding_max, limit, sort_by):
    """Get data in innovative format with advanced filtering from Azure Table Storage"""
    try:
        # Get data from Azure Table Storage
        raw_grants = fetch_grants_from_table(search, agency, limit)
        
        # Transform to innovative format with AI enhancements
        innovative_grants = []
        for grant in raw_grants:
            innovative_grant = transform_to_innovative_format(grant)
            innovative_grants.append(innovative_grant)
        
        return innovative_grants
    
    except Exception as e:
        logging.error(f"Error fetching innovative data: {str(e)}")
        # Fallback to sample data
        return load_sample_innovative_data()[:limit]

def fetch_grants_from_table(search: str = '', agency: str = '', limit: int = 10) -> List[Dict]:
    """Fetch grants from Azure Table Storage with enhanced filtering and error handling"""
    try:
        # Get connection string
        connection_string = os.environ.get('STORAGE_CONNECTION_STRING')
        if not connection_string or connection_string == 'UseDevelopmentStorage=true':
            logging.warning("Azure Table Storage not configured, using sample data")
            return []
        
        # Initialize table client
        table_service = TableServiceClient.from_connection_string(connection_string)
        table_name = os.environ.get('GRANTS_TABLE_NAME', 'GrantsData')
        table_client = table_service.get_table_client(table_name)
        
        # Build filter query with better escaping
        filters = []
        
        # Filter by agency if specified
        if agency:
            agency_escaped = agency.replace("'", "''")  # Escape single quotes
            filters.append(f"(AgencyCode eq '{agency_escaped}' or contains(AgencyName, '{agency_escaped}'))")
        
        # Filter by search term in multiple fields
        if search:
            search_escaped = search.replace("'", "''")  # Escape single quotes
            search_filters = [
                f"contains(Title, '{search_escaped}')",
                f"contains(Description, '{search_escaped}')",
                f"contains(Category, '{search_escaped}')",
                f"contains(AgencyName, '{search_escaped}')",
                f"contains(OpportunityId, '{search_escaped}')"
            ]
            filters.append(f"({' or '.join(search_filters)})")
        
        # Combine filters
        filter_query = ' and '.join(filters) if filters else None
        
        logging.info(f"Querying table {table_name} with filter: {filter_query}")
        
        # Query entities with error handling
        try:
            if filter_query:
                entities = table_client.query_entities(
                    query_filter=filter_query,
                    results_per_page=limit * 3  # Get more to allow for filtering
                )
            else:
                entities = table_client.query_entities(
                    results_per_page=limit * 3
                )
        except Exception as query_error:
            logging.error(f"Query failed: {str(query_error)}")
            # Try simple query without filters
            entities = table_client.query_entities(results_per_page=limit)
        
        # Convert to list and limit results
        grants_list = []
        count = 0
        
        for entity in entities:
            if count >= limit:
                break
            
            try:
                # Convert table entity to grant dictionary with null checks
                grant = {
                    'opportunity_id': str(entity.get('OpportunityId', entity.get('RowKey', ''))),
                    'opportunity_title': str(entity.get('Title', 'Grant Opportunity')),
                    'agency_code': str(entity.get('AgencyCode', '')),
                    'agency_name': str(entity.get('AgencyName', 'Federal Agency')),
                    'category_of_funding_activity': str(entity.get('Category', '')),
                    'funding_instrument_type': str(entity.get('FundingInstrumentType', '')),
                    'estimated_total_funding': str(entity.get('EstimatedTotalFunding', '')),
                    'expected_number_of_awards': str(entity.get('ExpectedNumberOfAwards', '1')),
                    'award_ceiling': str(entity.get('AwardCeiling', '')),
                    'award_floor': str(entity.get('AwardFloor', '')),
                    'cost_sharing_match_requirement': str(entity.get('CostSharingRequired', '')),
                    'link_to_additional_information': str(entity.get('AdditionalInfoLink', '')),
                    'grantor_contact': str(entity.get('GrantorContact', '')),
                    'grantor_contact_email': str(entity.get('GrantorEmail', '')),
                    'posted_date': str(entity.get('PostedDate', '')),
                    'close_date': str(entity.get('CloseDate', '')),
                    'opportunity_status': str(entity.get('Status', 'Active')),
                    'funding_description': str(entity.get('Description', '')),
                    'eligible_applicants': str(entity.get('EligibleApplicants', '')),
                    'last_updated': entity.get('LastUpdated', datetime.utcnow()),
                    'data_source': str(entity.get('DataSource', 'grants.gov'))
                }
                
                # Only add grants with valid opportunity IDs
                if grant['opportunity_id'] and grant['opportunity_id'] not in ['', 'None', 'nan']:
                    grants_list.append(grant)
                    count += 1
                    
            except Exception as entity_error:
                logging.warning(f"Error processing entity: {str(entity_error)}")
                continue
        
        if grants_list:
            logging.info(f"Successfully fetched {len(grants_list)} grants from Azure Table Storage")
        else:
            logging.warning("No grants found in Azure Table Storage")
            
        return grants_list
        
    except Exception as e:
        logging.error(f"Error fetching from Azure Table Storage: {str(e)}")
        return []

def transform_to_competitor_format(grant: Dict) -> Dict:
    """Transform raw grant data to competitor format"""
    try:
        # Extract funding amount for display
        award_ceiling = grant.get('award_ceiling', '')
        funding_amount = extract_funding_amount(award_ceiling)
        
        return {
            'id': grant.get('opportunity_id', ''),
            'title': grant.get('opportunity_title', 'Grant Opportunity'),
            'funder': grant.get('agency_name', 'Federal Agency'),
            'amount': f"${funding_amount:,}" if funding_amount > 0 else 'Amount varies',
            'deadline': format_deadline(grant.get('close_date', '')),
            'status': grant.get('opportunity_status', 'Active'),
            'applicants': grant.get('eligible_applicants', 'See details'),
            'grant_overview': grant.get('funding_description', 'Grant details will be available soon.')[:300],
            'direct_link': grant.get('link_to_additional_information', '#'),
            'category': grant.get('category_of_funding_activity', ''),
            'instrument_type': grant.get('funding_instrument_type', ''),
            'contact': grant.get('grantor_contact', ''),
            'contact_email': grant.get('grantor_contact_email', '')
        }
    except Exception as e:
        logging.error(f"Error transforming to competitor format: {str(e)}")
        return {
            'id': grant.get('opportunity_id', 'unknown'),
            'title': 'Error loading grant details',
            'funder': 'Unknown',
            'amount': 'N/A',
            'deadline': 'N/A',
            'status': 'Unknown'
        }

def transform_to_innovative_format(grant: Dict) -> Dict:
    """Transform raw grant data to innovative format with AI enhancements"""
    try:
        # Extract and calculate funding intelligence
        award_ceiling = extract_funding_amount(grant.get('award_ceiling', ''))
        award_floor = extract_funding_amount(grant.get('award_floor', ''))
        estimated_awards = extract_number(grant.get('expected_number_of_awards', '1'))
        
        # Calculate AI insights
        competitiveness_score = calculate_competitiveness_score(grant)
        success_probability = calculate_success_probability(grant)
        urgency_level = calculate_urgency_level(grant)
        domain_tags = extract_domain_tags(grant)
        
        # Calculate timeline analysis
        timeline_analysis = calculate_timeline_analysis(grant.get('close_date', ''))
        
        # Determine funding tier
        funding_tier = determine_funding_tier(award_ceiling)
        
        # Build eligibility matrix
        eligibility_matrix = build_eligibility_matrix(grant.get('eligible_applicants', ''))
        
        # Generate application strategy
        application_strategy = generate_application_strategy(grant)
        
        return {
            'grant_id': grant.get('opportunity_id', ''),
            'title': grant.get('opportunity_title', 'Advanced Grant Opportunity'),
            'agency_full': grant.get('agency_name', 'Federal Agency'),
            'agency_short': grant.get('agency_code', 'FEDERAL'),
            'funding_intelligence': {
                'total_program_funding': award_ceiling * estimated_awards if award_ceiling and estimated_awards else 0,
                'individual_award_range': {
                    'min': award_floor if award_floor > 0 else award_ceiling // 2 if award_ceiling else 0,
                    'max': award_ceiling if award_ceiling > 0 else 100000,
                    'average': (award_ceiling + award_floor) // 2 if award_ceiling and award_floor else award_ceiling if award_ceiling else 50000
                },
                'estimated_awards': estimated_awards,
                'funding_tier': funding_tier,
                'budget_flexibility': 'High' if not grant.get('cost_sharing_match_requirement') else 'Medium'
            },
            'competitiveness_score': competitiveness_score,
            'success_probability': success_probability,
            'timeline_analysis': timeline_analysis,
            'urgency_level': urgency_level,
            'domain_tags': domain_tags,
            'eligibility_matrix': eligibility_matrix,
            'application_strategy': application_strategy,
            'preparation_time': estimate_preparation_time(grant),
            'original_data': {
                'opportunity_number': grant.get('opportunity_id', ''),
                'close_date': grant.get('close_date', ''),
                'link_to_additional_information': grant.get('link_to_additional_information', '')
            }
        }
    except Exception as e:
        logging.error(f"Error transforming to innovative format: {str(e)}")
        return {
            'grant_id': grant.get('opportunity_id', 'unknown'),
            'title': 'Error loading grant details',
            'agency_full': 'Unknown',
            'competitiveness_score': 50,
            'success_probability': 25.0,
            'urgency_level': 'Medium'
        }

def extract_funding_amount(amount_str: str) -> int:
    """Extract numeric amount from funding string"""
    try:
        if not amount_str or amount_str.lower() in ['nan', 'none', '']:
            return 0
        
        # Remove common prefixes and clean
        amount_str = str(amount_str).replace('$', '').replace(',', '').strip()
        
        # Extract first number found
        import re
        numbers = re.findall(r'\d+', amount_str)
        if numbers:
            return int(numbers[0])
        return 0
    except:
        return 0

def extract_number(number_str: str) -> int:
    """Extract number from string"""
    try:
        if not number_str or str(number_str).lower() in ['nan', 'none', '']:
            return 1
        
        import re
        numbers = re.findall(r'\d+', str(number_str))
        if numbers:
            return int(numbers[0])
        return 1
    except:
        return 1

def calculate_competitiveness_score(grant: Dict) -> int:
    """Calculate AI-based competitiveness score"""
    try:
        score = 50  # Base score
        
        # Adjust based on funding amount (higher = more competitive)
        funding = extract_funding_amount(grant.get('award_ceiling', ''))
        if funding > 1000000:
            score += 30
        elif funding > 500000:
            score += 20
        elif funding > 100000:
            score += 10
        
        # Adjust based on application requirements
        if 'research' in grant.get('funding_description', '').lower():
            score += 15
        if 'innovation' in grant.get('funding_description', '').lower():
            score += 10
        
        # Ensure score is within bounds
        return min(100, max(0, score))
    except:
        return 50

def calculate_success_probability(grant: Dict) -> float:
    """Calculate estimated success probability"""
    try:
        estimated_awards = extract_number(grant.get('expected_number_of_awards', '1'))
        if estimated_awards >= 10:
            return 45.0
        elif estimated_awards >= 5:
            return 30.0
        elif estimated_awards >= 3:
            return 20.0
        else:
            return 15.0
    except:
        return 25.0

def calculate_urgency_level(grant: Dict) -> str:
    """Calculate urgency level based on deadline"""
    try:
        close_date = grant.get('close_date', '')
        if not close_date:
            return 'Medium'
        
        # Simple date parsing and comparison
        from datetime import datetime, timedelta
        
        # Try to parse common date formats
        date_formats = ['%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%d/%m/%Y']
        deadline = None
        
        for fmt in date_formats:
            try:
                deadline = datetime.strptime(close_date.strip(), fmt)
                break
            except:
                continue
        
        if deadline:
            days_until = (deadline - datetime.now()).days
            if days_until <= 30:
                return 'High'
            elif days_until <= 60:
                return 'Medium'
            else:
                return 'Low'
        
        return 'Medium'
    except:
        return 'Medium'

def calculate_timeline_analysis(close_date: str) -> Dict:
    """Calculate timeline analysis"""
    try:
        from datetime import datetime
        
        date_formats = ['%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%d/%m/%Y']
        deadline = None
        
        for fmt in date_formats:
            try:
                deadline = datetime.strptime(close_date.strip(), fmt)
                break
            except:
                continue
        
        if deadline:
            days_remaining = (deadline - datetime.now()).days
            status = 'Active (≤90 days)' if days_remaining <= 90 else 'Active'
            
            return {
                'days_remaining': max(0, days_remaining),
                'status': status,
                'application_window': max(0, days_remaining)
            }
        
        return {
            'days_remaining': 60,
            'status': 'Active',
            'application_window': 60
        }
    except:
        return {
            'days_remaining': 60,
            'status': 'Active',
            'application_window': 60
        }

def determine_funding_tier(amount: int) -> str:
    """Determine funding tier based on amount"""
    if amount >= 5000000:
        return 'Major Program (≥$5M)'
    elif amount >= 1000000:
        return 'Large Grant ($1M-$5M)'
    elif amount >= 500000:
        return 'Standard Grant ($500K-$1M)'
    elif amount >= 100000:
        return 'Standard Grant ($100K-$500K)'
    elif amount > 0:
        return 'Small Grant (<$100K)'
    else:
        return 'Amount varies'

def extract_domain_tags(grant: Dict) -> List[str]:
    """Extract domain tags from grant description"""
    try:
        description = grant.get('funding_description', '').lower()
        category = grant.get('category_of_funding_activity', '').lower()
        
        # Common domain mappings
        domain_mapping = {
            'education': ['education', 'school', 'student', 'learning', 'teaching'],
            'healthcare': ['health', 'medical', 'healthcare', 'medicine', 'clinical'],
            'research': ['research', 'study', 'investigation', 'analysis'],
            'technology': ['technology', 'digital', 'innovation', 'tech', 'software'],
            'environment': ['environment', 'climate', 'sustainability', 'green'],
            'social_services': ['social', 'community', 'welfare', 'services'],
            'arts': ['arts', 'culture', 'cultural', 'creative', 'artistic'],
            'infrastructure': ['infrastructure', 'construction', 'transportation'],
            'business': ['business', 'economic', 'entrepreneurship', 'commerce']
        }
        
        tags = []
        text = f"{description} {category}"
        
        for domain, keywords in domain_mapping.items():
            if any(keyword in text for keyword in keywords):
                tags.append(domain)
        
        return tags[:5]  # Limit to 5 tags
    except:
        return ['general']

def build_eligibility_matrix(eligible_applicants: str) -> Dict:
    """Build eligibility matrix from applicants description"""
    try:
        text = eligible_applicants.lower()
        
        return {
            'academic_institutions': {
                'eligible': any(word in text for word in ['university', 'college', 'academic', 'institution', 'school'])
            },
            'government_entities': {
                'eligible': any(word in text for word in ['government', 'federal', 'state', 'local', 'municipal'])
            },
            'nonprofit_organizations': {
                'eligible': any(word in text for word in ['nonprofit', 'non-profit', 'organization', 'ngo'])
            },
            'private_sector': {
                'eligible': any(word in text for word in ['private', 'business', 'company', 'corporation', 'commercial'])
            }
        }
    except:
        return {
            'academic_institutions': {'eligible': True},
            'government_entities': {'eligible': True},
            'nonprofit_organizations': {'eligible': True},
            'private_sector': {'eligible': False}
        }

def generate_application_strategy(grant: Dict) -> List[str]:
    """Generate AI-powered application strategy"""
    try:
        strategies = []
        
        funding_amount = extract_funding_amount(grant.get('award_ceiling', ''))
        description = grant.get('funding_description', '').lower()
        
        # Base strategies
        if funding_amount > 1000000:
            strategies.append('Form strong consortium with complementary expertise')
            strategies.append('Include detailed implementation and sustainability plan')
        elif funding_amount > 500000:
            strategies.append('Develop comprehensive project timeline with milestones')
            strategies.append('Demonstrate institutional support and commitment')
        else:
            strategies.append('Focus on clear, achievable objectives')
            strategies.append('Emphasize cost-effectiveness and impact')
        
        # Content-specific strategies
        if 'research' in description:
            strategies.append('Include robust methodology and evaluation plan')
        if 'community' in description:
            strategies.append('Demonstrate community engagement and partnerships')
        if 'innovation' in description:
            strategies.append('Highlight innovative approaches and technologies')
        
        return strategies[:4]  # Limit to 4 strategies
    except:
        return [
            'Develop clear project objectives and outcomes',
            'Include detailed budget justification',
            'Demonstrate organizational capacity and experience'
        ]

def estimate_preparation_time(grant: Dict) -> str:
    """Estimate preparation time needed"""
    try:
        funding_amount = extract_funding_amount(grant.get('award_ceiling', ''))
        
        if funding_amount > 2000000:
            return '12-16 weeks'
        elif funding_amount > 1000000:
            return '8-12 weeks'
        elif funding_amount > 500000:
            return '6-8 weeks'
        else:
            return '4-6 weeks'
    except:
        return '6-8 weeks'

def format_deadline(date_str: str) -> str:
    """Format deadline for display"""
    try:
        if not date_str or date_str.lower() in ['nan', 'none', '']:
            return 'TBD'
        
        from datetime import datetime
        
        # Try to parse and reformat
        date_formats = ['%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%d/%m/%Y']
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str.strip(), fmt)
                return parsed_date.strftime('%B %d, %Y')
            except:
                continue
        
        return date_str
    except:
        return 'TBD'

# Keep your existing HTML generation functions and sample data as fallbacks
def generate_html_response(grants_data, format_type):
    """Generate enhanced HTML response"""
    if format_type == 'competitor':
        return generate_competitor_html(grants_data)
    else:
        return generate_innovative_html(grants_data)

def generate_competitor_html(grants_data):
    """Generate HTML exactly like competitor"""
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Grant Discovery Platform - Competitor Format</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background: #f5f7fa; }}
            .header {{ background: white; padding: 20px; border-bottom: 1px solid #e1e8ed; }}
            .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
            .grant-card {{ 
                background: white; 
                border: 1px solid #e1e8ed; 
                border-radius: 8px; 
                padding: 24px; 
                margin-bottom: 16px;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            }}
            .grant-title {{ 
                font-size: 20px; 
                font-weight: 600; 
                color: #1da1f2; 
                margin-bottom: 12px;
                line-height: 1.3;
            }}
            .grant-amount {{ 
                font-size: 24px; 
                font-weight: 700; 
                color: #17bf63; 
                margin-bottom: 8px; 
            }}
            .grant-funder {{ 
                color: #657786; 
                font-size: 14px;
                margin-bottom: 16px; 
            }}
            .grant-meta {{ 
                display: flex; 
                gap: 24px; 
                margin-bottom: 16px;
                font-size: 14px;
            }}
            .meta-item {{ color: #657786; }}
            .meta-label {{ font-weight: 600; color: #14171a; }}
            .grant-overview {{ 
                color: #14171a; 
                line-height: 1.5;
                margin-top: 16px;
            }}
            .open-btn {{
                background: #1da1f2;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                cursor: pointer;
                font-weight: 600;
                float: right;
                margin-top: 16px;
            }}
            .stats {{ 
                background: white; 
                padding: 20px; 
                border-radius: 8px; 
                margin-bottom: 20px;
                text-align: center;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Grant Discovery Platform</h1>
            <p>Competitor Format View</p>
        </div>
        <div class="container">
            <div class="stats">
                <h2>Total Results: {len(grants_data)}</h2>
            </div>
    """
    
    for grant in grants_data:
        html += f"""
        <div class="grant-card">
            <div class="grant-title">{grant.get('title', 'Grant Opportunity')}</div>
            <div class="grant-amount">{grant.get('funding_amount', 'Amount not specified')}</div>
            <div class="grant-funder">{grant.get('funder', 'Federal Agency')}</div>
            
            <div class="grant-meta">
                <div class="meta-item">
                    <span class="meta-label">Eligible Region:</span> {grant.get('eligible_region', 'Federal')}
                </div>
                <div class="meta-item">
                    <span class="meta-label">Activities:</span> {grant.get('eligible_activities', 'Various')}
                </div>
                <div class="meta-item">
                    <span class="meta-label">Applicants:</span> {grant.get('eligible_applicants', 'See details')}
                </div>
            </div>
            
            <div class="grant-overview">
                {grant.get('grant_overview', 'Grant details will be available soon.')[:300]}...
            </div>
            
            <button class="open-btn" onclick="window.open('{grant.get('direct_link', '#')}', '_blank')">
                Open Original
            </button>
        </div>
        """
    
    html += """
        </div>
    </body>
    </html>
    """
    
    return func.HttpResponse(html, status_code=200, headers={"Content-Type": "text/html"})

def generate_innovative_html(grants_data):
    """Generate advanced HTML with innovative features"""
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Advanced Grant Intelligence Platform</title>
        <style>
            body {{ 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; 
                margin: 0; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }}
            .header {{ 
                background: rgba(255,255,255,0.95); 
                backdrop-filter: blur(10px);
                padding: 24px; 
                border-bottom: 1px solid rgba(255,255,255,0.2);
                position: sticky;
                top: 0;
                z-index: 100;
            }}
            .container {{ max-width: 1400px; margin: 0 auto; padding: 24px; }}
            .grant-card {{ 
                background: rgba(255,255,255,0.95); 
                backdrop-filter: blur(10px);
                border: 1px solid rgba(255,255,255,0.2); 
                border-radius: 16px; 
                padding: 28px; 
                margin-bottom: 20px;
                box-shadow: 0 8px 32px rgba(0,0,0,0.1);
                transition: transform 0.2s ease;
            }}
            .grant-card:hover {{ transform: translateY(-2px); }}
            .grant-header {{
                display: flex;
                justify-content: space-between;
                align-items: flex-start;
                margin-bottom: 20px;
            }}
            .grant-title {{ 
                font-size: 22px; 
                font-weight: 700; 
                color: #2d3748; 
                margin-bottom: 8px;
                line-height: 1.3;
                flex: 1;
            }}
            .priority-badge {{
                background: linear-gradient(45deg, #ff6b6b, #ee5a24);
                color: white;
                padding: 4px 12px;
                border-radius: 20px;
                font-size: 12px;
                font-weight: 600;
                margin-left: 16px;
            }}
            .grant-intelligence {{
                display: grid;
                grid-template-columns: 1fr 1fr 1fr;
                gap: 20px;
                margin-bottom: 20px;
            }}
            .intel-card {{
                background: rgba(255,255,255,0.6);
                padding: 16px;
                border-radius: 12px;
                text-align: center;
            }}
            .intel-value {{
                font-size: 24px;
                font-weight: 700;
                color: #2d3748;
                margin-bottom: 4px;
            }}
            .intel-label {{
                font-size: 12px;
                color: #718096;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }}
            .funding-intelligence {{
                background: linear-gradient(45deg, #48bb78, #38a169);
                color: white;
                padding: 20px;
                border-radius: 12px;
                margin-bottom: 16px;
            }}
            .match-scores {{
                display: flex;
                gap: 12px;
                margin-bottom: 16px;
            }}
            .match-score {{
                background: rgba(72, 187, 120, 0.1);
                border: 1px solid #48bb78;
                padding: 8px 12px;
                border-radius: 20px;
                font-size: 12px;
                font-weight: 600;
                color: #2d3748;
            }}
            .domain-tags {{
                display: flex;
                flex-wrap: wrap;
                gap: 8px;
                margin-bottom: 16px;
            }}
            .domain-tag {{
                background: rgba(102, 126, 234, 0.1);
                border: 1px solid #667eea;
                padding: 4px 10px;
                border-radius: 16px;
                font-size: 11px;
                color: #4c51bf;
                font-weight: 500;
            }}
            .advanced-actions {{
                display: flex;
                gap: 12px;
                margin-top: 20px;
            }}
            .action-btn {{
                padding: 10px 20px;
                border: none;
                border-radius: 8px;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.2s ease;
            }}
            .primary-btn {{
                background: linear-gradient(45deg, #667eea, #764ba2);
                color: white;
            }}
            .secondary-btn {{
                background: rgba(255,255,255,0.8);
                color: #4a5568;
                border: 1px solid rgba(255,255,255,0.3);
            }}
            .action-btn:hover {{ transform: translateY(-1px); }}
            .stats-dashboard {{ 
                background: rgba(255,255,255,0.95); 
                backdrop-filter: blur(10px);
                padding: 24px; 
                border-radius: 16px; 
                margin-bottom: 24px;
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 20px;
            }}
            .risk-indicator {{
                display: inline-block;
                width: 12px;
                height: 12px;
                border-radius: 50%;
                margin-right: 8px;
            }}
            .risk-low {{ background: #48bb78; }}
            .risk-medium {{ background: #ed8936; }}
            .risk-high {{ background: #f56565; }}
            .timeline-bar {{
                width: 100%;
                height: 6px;
                background: rgba(255,255,255,0.3);
                border-radius: 3px;
                overflow: hidden;
                margin: 8px 0;
            }}
            .timeline-progress {{
                height: 100%;
                background: linear-gradient(90deg, #48bb78, #ed8936, #f56565);
                border-radius: 3px;
            }}
            .ai-insights {{
                background: rgba(102, 126, 234, 0.1);
                border: 1px solid #667eea;
                border-radius: 12px;
                padding: 16px;
                margin-bottom: 16px;
            }}
            .insight-item {{
                display: flex;
                align-items: center;
                margin-bottom: 8px;
            }}
            .insight-icon {{
                width: 16px;
                height: 16px;
                margin-right: 8px;
                border-radius: 50%;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🚀 Advanced Grant Intelligence Platform</h1>
            <p>AI-Powered Grant Discovery with Competitive Intelligence</p>
        </div>
        <div class="container">
            <div class="stats-dashboard">
                <div class="intel-card">
                    <div class="intel-value">{len(grants_data)}</div>
                    <div class="intel-label">Total Opportunities</div>
                </div>
                <div class="intel-card">
                    <div class="intel-value">${sum([g.get('funding_intelligence', {}).get('individual_award_range', {}).get('max', 0) for g in grants_data]):,}</div>
                    <div class="intel-label">Total Available Funding</div>
                </div>
                <div class="intel-card">
                    <div class="intel-value">{len([g for g in grants_data if g.get('urgency_level') == 'High'])}</div>
                    <div class="intel-label">High Priority</div>
                </div>
                <div class="intel-card">
                    <div class="intel-value">{len([g for g in grants_data if g.get('competitiveness_score', 0) < 30])}</div>
                    <div class="intel-label">Low Competition</div>
                </div>
            </div>
    """
    
    for grant in grants_data:
        funding_intel = grant.get('funding_intelligence', {})
        timeline_analysis = grant.get('timeline_analysis', {})
        domain_tags = grant.get('domain_tags', [])
        eligibility_matrix = grant.get('eligibility_matrix', {})
        application_strategy = grant.get('application_strategy', [])
        
        # Calculate timeline progress (days remaining)
        days_remaining = timeline_analysis.get('days_remaining', 0)
        timeline_progress = max(0, min(100, 100 - (days_remaining / 90 * 100))) if days_remaining else 0
        
        # Determine risk level
        risk_level = 'low' if grant.get('competitiveness_score', 50) < 30 else 'medium' if grant.get('competitiveness_score', 50) < 70 else 'high'
        
        html += f"""
        <div class="grant-card">
            <div class="grant-header">
                <div class="grant-title">{grant.get('title', 'Advanced Grant Opportunity')}</div>
                {f'<div class="priority-badge">HIGH PRIORITY</div>' if grant.get('urgency_level') == 'High' else ''}
            </div>
            
            <div class="funding-intelligence">
                <h3>💰 Funding Intelligence</h3>
                <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; margin-top: 12px;">
                    <div>
                        <div style="font-size: 18px; font-weight: 700;">${funding_intel.get('individual_award_range', {}).get('max', 0):,}</div>
                        <div style="font-size: 12px; opacity: 0.8;">Max Award</div>
                    </div>
                    <div>
                        <div style="font-size: 18px; font-weight: 700;">{grant.get('success_probability', 0):.1f}%</div>
                        <div style="font-size: 12px; opacity: 0.8;">Success Rate</div>
                    </div>
                    <div>
                        <div style="font-size: 18px; font-weight: 700;">{funding_intel.get('funding_tier', 'Unknown')}</div>
                        <div style="font-size: 12px; opacity: 0.8;">Funding Tier</div>
                    </div>
                </div>
            </div>
            
            <div class="grant-intelligence">
                <div class="intel-card">
                    <div class="intel-value">{grant.get('competitiveness_score', 0)}</div>
                    <div class="intel-label">Competition Score</div>
                </div>
                <div class="intel-card">
                    <div class="intel-value">{timeline_analysis.get('days_remaining', 'N/A')}</div>
                    <div class="intel-label">Days Remaining</div>
                </div>
                <div class="intel-card">
                    <div class="intel-value">
                        <span class="risk-indicator risk-{risk_level}"></span>
                        {risk_level.capitalize()}
                    </div>
                    <div class="intel-label">Risk Level</div>
                </div>
            </div>
            
            <div class="ai-insights">
                <h4 style="margin: 0 0 12px 0; color: #4c51bf;">🤖 AI Insights</h4>
                <div class="insight-item">
                    <div class="insight-icon" style="background: #48bb78;"></div>
                    <span style="font-size: 14px;">Match Score: {grant.get('competitiveness_score', 0)}/100 based on funding amount and competition</span>
                </div>
                <div class="insight-item">
                    <div class="insight-icon" style="background: #667eea;"></div>
                    <span style="font-size: 14px;">Estimated prep time: {grant.get('preparation_time', '4-6 weeks')}</span>
                </div>
                <div class="insight-item">
                    <div class="insight-icon" style="background: #ed8936;"></div>
                    <span style="font-size: 14px;">Timeline status: {timeline_analysis.get('status', 'Active')}</span>
                </div>
            </div>
            
            <div class="timeline-bar">
                <div class="timeline-progress" style="width: {timeline_progress}%;"></div>
            </div>
            <div style="font-size: 12px; color: #718096; margin-bottom: 16px;">
                Application Timeline Progress: {timeline_progress:.0f}% elapsed
            </div>
            
            <div class="match-scores">
                <span class="match-score">Match: {grant.get("competitiveness_score", 0)}/100</span>
                <span class="match-score">Success: {grant.get("success_probability", 0):.1f}%</span>
                <span class="match-score">Prep: {grant.get("preparation_time", "6 weeks")}</span>
            </div>
            
            <div class="domain-tags">
                {''.join([f'<span class="domain-tag">{tag.replace("_", " ").title()}</span>' for tag in domain_tags[:5]])}
            </div>
            
            <div style="background: rgba(255,255,255,0.6); padding: 16px; border-radius: 12px; margin-bottom: 16px;">
                <h4 style="margin: 0 0 8px 0; color: #2d3748;">📋 Eligibility Overview</h4>
                <div style="font-size: 14px; color: #4a5568;">
                    <div>• Academic: {'✅' if eligibility_matrix.get('academic_institutions', {}).get('eligible') else '❌'}</div>
                    <div>• Government: {'✅' if eligibility_matrix.get('government_entities', {}).get('eligible') else '❌'}</div>
                    <div>• Nonprofit: {'✅' if eligibility_matrix.get('nonprofit_organizations', {}).get('eligible') else '❌'}</div>
                    <div>• Private: {'✅' if eligibility_matrix.get('private_sector', {}).get('eligible') else '❌'}</div>
                </div>
            </div>
            
            <div style="background: rgba(255,255,255,0.6); padding: 16px; border-radius: 12px; margin-bottom: 16px;">
                <h4 style="margin: 0 0 8px 0; color: #2d3748;">🎯 Application Strategy</h4>
                <div style="font-size: 14px; color: #4a5568;">
                    {''.join([f'<div>• {strategy}</div>' for strategy in application_strategy[:3]])}
                </div>
            </div>
            
            <div class="advanced-actions">
                <button class="action-btn primary-btn" onclick="window.open('{grant.get('original_data', {}).get('link_to_additional_information', '#')}', '_blank')">
                    📖 View Full Details
                </button>
                <button class="action-btn secondary-btn" onclick="alert('AI Analysis: This grant shows {grant.get('competitiveness_score', 0)}/100 match score with {grant.get('success_probability', 0):.1f}% estimated success rate.')">
                    🤖 AI Analysis
                </button>
                <button class="action-btn secondary-btn" onclick="alert('Bookmark feature coming soon!')">
                    🔖 Bookmark
                </button>
                <button class="action-btn secondary-btn" onclick="alert('Share feature coming soon!')">
                    📤 Share
                </button>
            </div>
        </div>
        """
    
    html += """
        </div>
        
        <div style="background: rgba(255,255,255,0.95); padding: 24px; margin: 24px; border-radius: 16px; text-align: center;">
            <h3>🚀 Powered by Advanced Grant Intelligence</h3>
            <p style="color: #718096; margin: 0;">AI-driven analysis • Real-time competitive intelligence • Smart matching algorithms</p>
        </div>
        
        <script>
            // Add interactive features
            document.addEventListener('DOMContentLoaded', function() {
                // Animate cards on scroll
                const cards = document.querySelectorAll('.grant-card');
                const observer = new IntersectionObserver((entries) => {
                    entries.forEach(entry => {
                        if (entry.isIntersecting) {
                            entry.target.style.opacity = '1';
                            entry.target.style.transform = 'translateY(0)';
                        }
                    });
                });
                
                cards.forEach(card => {
                    card.style.opacity = '0';
                    card.style.transform = 'translateY(20px)';
                    card.style.transition = 'opacity 0.6s ease, transform 0.6s ease';
                    observer.observe(card);
                });
                
                // Add click analytics
                document.querySelectorAll('.action-btn').forEach(btn => {
                    btn.addEventListener('click', function() {
                        console.log('Action clicked:', this.textContent);
                    });
                });
            });
        </script>
    </body>
    </html>
    """
    
    return func.HttpResponse(html, status_code=200, headers={"Content-Type": "text/html"})

def load_sample_competitor_data():
    """Load sample data in competitor format"""
    return [
        {
            'title': 'Research and Development (RAD) Directed Energy (RD) University Assistance Instruments',
            'funding_amount': '$49,000,000',
            'funder': 'U.S. Department of Defense (DOD)',
            'eligible_region': 'Federal',
            'eligible_activities': 'Purchase Materials',
            'eligible_applicants': 'Unrestricted',
            'grant_overview': 'Original Solicitation:Closing Date of the FOAFROM: 18 July 2024TO: 18 July 2029Cost Ceiling$49MContracting/Agreements Points of Contact (POC)Agreements OfficerMariah SalazarAgreements SpecialistMonique Esquibel-SenaEmails: mariah.salazar@us.af.mil...',
            'direct_link': 'https://www.grants.gov/search-results-detail/example'
        },
        {
            'title': 'Promoting American Excellence to Moroccan Youth 2025',
            'funding_amount': '$123,000',
            'funder': 'U.S. Mission to Morocco',
            'eligible_region': 'Federal',
            'eligible_activities': 'Education',
            'eligible_applicants': 'Nonprofits having a 501(c)(3) status',
            'grant_overview': 'For Fiscal Year 2025, American Spaces Morocco aims to continue delivering impactful programs that promote American excellence and encourage the best and brightest young Moroccans...',
            'direct_link': 'https://www.grants.gov/search-results-detail/359212'
        }
    ]

def load_sample_innovative_data():
    """Load sample data in innovative format"""
    return [
        {
            'grant_id': 'OFOP0001937',
            'title': 'Promoting American Excellence to Moroccan Youth 2025',
            'agency_full': 'U.S. Mission to Morocco',
            'agency_short': 'DOS-MAR',
            'funding_intelligence': {
                'total_program_funding': 123000,
                'individual_award_range': {'min': 40000, 'max': 120000, 'average': 80000},
                'estimated_awards': 3,
                'funding_tier': 'Standard Grant ($100K-$500K)',
                'budget_flexibility': 'Medium'
            },
            'competitiveness_score': 65,
            'success_probability': 33.3,
            'timeline_analysis': {
                'days_remaining': 62,
                'status': 'Active (≤90 days)',
                'application_window': 62
            },
            'urgency_level': 'Medium',
            'domain_tags': ['education', 'cultural_exchange', 'digital_transformation'],
            'eligibility_matrix': {
                'academic_institutions': {'eligible': False},
                'government_entities': {'eligible': False},
                'nonprofit_organizations': {'eligible': True},
                'private_sector': {'eligible': False}
            },
            'application_strategy': [
                'Focus on cultural exchange components',
                'Include detailed budget breakdown for youth programs',
                'Emphasize measurable outcomes and impact metrics'
            ],
            'preparation_time': '4-6 weeks',
            'original_data': {
                'opportunity_number': 'OFOP0001937',
                'close_date': '07/31/2025',
                'link_to_additional_information': 'https://mygrants.servicenowservices.com/mygrants'
            }
        },
        {
            'grant_id': 'HHS-2025-ACL-CIP-AA-0049',
            'title': 'Research to Spread and Scale the Impact of Evidence-Based Falls Prevention Programs',
            'agency_full': 'Administration for Community Living',
            'agency_short': 'HHS-ACL',
            'funding_intelligence': {
                'total_program_funding': 4650000,
                'individual_award_range': {'min': 1450000, 'max': 4650000, 'average': 3050000},
                'estimated_awards': 3,
                'funding_tier': 'Major Program (≥$5M)',
                'budget_flexibility': 'High'
            },
            'competitiveness_score': 85,
            'success_probability': 12.5,
            'timeline_analysis': {
                'days_remaining': 73,
                'status': 'Active (≤90 days)',
                'application_window': 46
            },
            'urgency_level': 'High',
            'domain_tags': ['healthcare', 'aging', 'prevention', 'research'],
            'eligibility_matrix': {
                'academic_institutions': {'eligible': True},
                'government_entities': {'eligible': True},
                'nonprofit_organizations': {'eligible': True},
                'private_sector': {'eligible': True}
            },
            'application_strategy': [
                'Form strong research consortium with complementary expertise',
                'Include detailed implementation science methodology',
                'Demonstrate experience with aging network partnerships'
            ],
            'preparation_time': '8-12 weeks',
            'original_data': {
                'opportunity_number': 'HHS-2025-ACL-CIP-AA-0049',
                'close_date': '08/12/2025',
                'link_to_additional_information': 'https://acl.gov/grants/open-opportunities'
            }
        }
    ]