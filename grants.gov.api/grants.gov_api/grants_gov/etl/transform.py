import logging
import json
from datetime import datetime
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class GrantDataTransformer:
    """Transforms data from Grants.gov API to match our database schema."""
    
    def transform_opportunities(self, opportunities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform Grants.gov API data to match our database schema.
        
        Args:
            opportunities: List of raw opportunity dictionaries from API
            
        Returns:
            List of transformed opportunity dictionaries matching our schema
        """
        transformed_opportunities = []
        
        for opp in opportunities:
            # Skip opportunities missing critical fields
            if not opp.get('id') or not opp.get('title'):
                logger.warning(f"Skipping opportunity with missing critical fields: {opp}")
                continue
                
            # Create a new dict with mapped data
            transformed_opp = {
                'id': opp.get('id'),
                
                # General
                'opportunity_url': f"https://www.grants.gov/search-results-detail/{opp.get('id')}",
                'title': opp.get('title'),
                'deadline': opp.get('closeDate'),  # May need date parsing
                'time_zone': "Eastern Time (ET)",  # Grants.gov uses ET
                
                # Financial Information
                'award_value': None,  # Not directly in basic API response
                'cash_award': None,   # Not directly in basic API response
                
                # Application Details
                'direct_link_to_apply_url': f"https://www.grants.gov/apply-for-grants/opportunities/{opp.get('id')}",
                
                # Opportunity Details
                'opportunity_gap': None,  # Would need custom mapping
                'type': "Grant",  # All entries from Grants.gov are grants
                
                # Geographic Eligibility
                'global_opportunity': False,  # Default for US grants
                'global_locations': "North America",
                'countries_eligible': "United States",
                'location_details': "United States",
                
                # Detailed Information
                'short_description': opp.get('description', '')[:500],  # Truncate if exists
                'eligibility': None,  # Would need to extract from detailed response
                'long_description': opp.get('description'),
                'target_community': None,  # Not directly in API
                'opportunity_logo_url': None,  # Not in API
                'date_posted': opp.get('openDate'),  # May need date parsing
                'industry': None,  # Not directly in API
                
                # Additional Information
                'service_provider': opp.get('agency'),
                'eso_website': None,  # Would need agency website mapping
                'contact_email': None,  # Not in basic API response
                
                # Additional fields from Grants.gov API
                'agency': opp.get('agency'),
                'agency_code': opp.get('agencyCode'),
                'opportunity_number': opp.get('number'),
                'opportunity_status': opp.get('oppStatus'),
                'open_date': opp.get('openDate'),
                'close_date': opp.get('closeDate'),
                
                # Store original data
                'raw_data': json.dumps(opp)
            }
            
            # Any additional transformations or field derivations
            
            transformed_opportunities.append(transformed_opp)
            
        logger.info(f"Transformed {len(transformed_opportunities)} opportunities")
        return transformed_opportunities
    
    def enrich_with_details(self, opportunities: List[Dict[str, Any]], api_client) -> List[Dict[str, Any]]:
        """
        Enrich basic opportunity data with detailed information.
        
        Args:
            opportunities: List of basic transformed opportunities
            api_client: GrantsGovAPI client instance
            
        Returns:
            List of enriched opportunities
        """
        enriched_opportunities = []
        
        for opp in opportunities:
            try:
                # Fetch detailed information
                details = api_client.fetch_opportunity(opp['id'])
                
                if 'data' in details and details['data']:
                    detail_data = details['data']
                    
                    # Extract additional information from detail response
                    if 'synopsis' in detail_data:
                        synopsis = detail_data['synopsis']
                        
                        # Update fields with detailed information
                        opp['eligibility'] = synopsis.get('eligibility', '')
                        opp['award_value'] = self._extract_award_amount(synopsis)
                        opp['long_description'] = synopsis.get('description', opp['long_description'])
                        
                        # Try to extract contact information
                        if 'agencyContactInfo' in synopsis:
                            contact_info = synopsis['agencyContactInfo']
                            opp['contact_email'] = contact_info.get('email', '')
                    
                    # Store updated raw data
                    opp['raw_data'] = json.dumps(details)
                
            except Exception as e:
                logger.warning(f"Error enriching opportunity {opp['id']}: {e}")
                
            enriched_opportunities.append(opp)
            
        return enriched_opportunities
    
    def _extract_award_amount(self, synopsis: Dict) -> float:
        """Extract award amount from synopsis if available."""
        try:
            award_info = synopsis.get('awardInfo', {})
            if 'awardAmount' in award_info:
                # Remove currency symbols and commas, then convert to float
                amount_str = award_info['awardAmount'].replace('$', '').replace(',', '')
                return float(amount_str)
        except Exception:
            pass
        return None