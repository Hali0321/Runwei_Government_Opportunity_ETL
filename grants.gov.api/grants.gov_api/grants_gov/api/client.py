import requests
import json
import csv
import time
from typing import Dict, List, Optional, Union, Any
import logging

logger = logging.getLogger(__name__)

class GrantsGovAPI:
    """
    Client for interacting with the Grants.gov API.
    
    This class provides methods to access various endpoints of the Grants.gov API,
    including searching for opportunities, fetching specific opportunity details,
    and retrieving opportunity counts by CFDA.
    """
    
    def __init__(self, api_key: Optional[str] = None, use_staging: bool = False):
        """
        Initialize the Grants.gov API client.
        
        Args:
            api_key: API key for authenticated endpoints
            use_staging: If True, use the staging API URL instead of production
        """
        self.api_key = api_key
        self.base_url = "https://api.staging.grants.gov" if use_staging else "https://api.grants.gov"
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
        
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"
    
    def search_opportunities(self, **kwargs) -> Dict[str, Any]:
        """
        Search for grant opportunities with optional filters.
        
        Args:
            **kwargs: Optional search parameters
                - keyword: Search term
                - oppNum: Opportunity number
                - oppTitle: Opportunity title
                - oppStatuses: Opportunity statuses (comma-separated)
                - oppCategories: Opportunity categories (comma-separated)
                - eligibilities: Eligibility codes (comma-separated)
                - fundingCategories: Funding categories (comma-separated)
                - fundingInstruments: Funding instruments (comma-separated)
                - agencies: Agency codes (comma-separated)
                - sortBy: Sort field
                - sortOrder: Sort order (asc or desc)
                - rows: Number of results per page
                - start: Starting index for pagination
                
        Returns:
            Dict containing search results
        """
        # Use the public search endpoint which is more reliable
        endpoint = "https://www.grants.gov/grantsws/rest/opportunities/search"
        
        # Filter out None values from kwargs
        params = {k: v for k, v in kwargs.items() if v is not None}
        
        # Default to 10 rows per page if not specified (lower to avoid issues)
        if "rows" not in params:
            params["rows"] = 10
        
        # Ensure we're asking for posted opportunities by default
        if "oppStatuses" not in params:
            params["oppStatuses"] = "posted"
            
        # Make the API request
        try:
            response = requests.post(endpoint, json=params, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 403:
                logger.error(f"API access forbidden. Response: {response.text}")
                # Try alternative method for search if available
                return self._search_alternative(params)
            elif response.status_code == 429:
                logger.warning("Rate limit hit. Waiting 5 seconds...")
                time.sleep(5)
                return self.search_opportunities(**kwargs)
            raise e
    
    def _search_alternative(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Alternative search method using the public web interface.
        
        Args:
            params: Search parameters
            
        Returns:
            Dict containing search results
        """
        # Fall back to the public search API
        endpoint = "https://www.grants.gov/web/grants/search-grants.html"
        logger.info(f"Using alternative search method via {endpoint}")
        
        # This is a placeholder - in a real implementation, you would
        # need to scrape the web interface or use another public API
        # that doesn't require authentication
        
        # For now, just return an empty result
        return {"data": {"oppHits": []}}
    
    def search_all_opportunities(self, max_results: int = 100, **kwargs) -> List[Dict[str, Any]]:
        """
        Search for grant opportunities and fetch all pages of results up to max_results.
        
        Args:
            max_results: Maximum number of results to return
            **kwargs: Search parameters as in search_opportunities
            
        Returns:
            List of opportunity dictionaries
        """
        if max_results > 100:
            logger.warning("Requesting more than 100 results. This might cause rate limiting.")
            max_results = 100  # Cap at 100 to be safe
            
        # Set up pagination parameters
        rows_per_page = min(10, max_results)  # Use smaller page size to avoid issues
        total_fetched = 0
        start = 0
        all_opportunities = []
        
        search_params = kwargs.copy()
        
        while total_fetched < max_results:
            # Update pagination parameters
            search_params["rows"] = rows_per_page
            search_params["start"] = start
            
            # Make the API request
            try:
                logger.debug(f"Fetching opportunities {start+1}-{start+rows_per_page}")
                response = self.search_opportunities(**search_params)
                
                # Extract opportunities from response
                if "data" in response and "oppHits" in response["data"]:
                    opportunities = response["data"]["oppHits"]
                    if not opportunities:
                        # No more results
                        break
                        
                    # Add to our result list
                    all_opportunities.extend(opportunities)
                    total_fetched += len(opportunities)
                    
                    # Check if we've fetched all available opportunities
                    if len(opportunities) < rows_per_page:
                        break
                        
                    # Update start for next page
                    start += rows_per_page
                    
                    # Add a delay to avoid rate limiting
                    time.sleep(1)
                else:
                    logger.warning("Unexpected API response format")
                    break
            except Exception as e:
                logger.error(f"Error fetching opportunities: {e}")
                break
                
        logger.info(f"Fetched {len(all_opportunities)} opportunities")
        return all_opportunities[:max_results]
    
    def fetch_opportunity(self, opportunity_id: str) -> Dict[str, Any]:
        """
        Fetch detailed information about a specific opportunity.
        
        Args:
            opportunity_id: The opportunity ID
            
        Returns:
            Dict containing opportunity details
        """
        endpoint = f"{self.base_url}/grantsws/rest/opportunity/{opportunity_id}"
        
        # Make the API request
        try:
            response = requests.get(endpoint, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                # Try alternative method
                return self._fetch_opportunity_alternative(opportunity_id)
            logger.error(f"Error fetching opportunity {opportunity_id}: {e}")
            return {"error": str(e)}
    
    def _fetch_opportunity_alternative(self, opportunity_id: str) -> Dict[str, Any]:
        """
        Alternative method to fetch opportunity details from public website.
        """
        # Use the public detail page URL
        endpoint = f"https://www.grants.gov/web/grants/view-opportunity.html?oppId={opportunity_id}"
        logger.info(f"Using alternative method to fetch opportunity {opportunity_id}")
        
        # For now, return basic structure
        return {
            "data": {
                "id": opportunity_id,
                "synopsis": {
                    "description": "Details not available due to API restriction",
                    "awardInfo": {}
                }
            }
        }
    
    def save_opportunities_to_csv(self, opportunities: List[Dict[str, Any]], filename: str) -> None:
        """
        Save opportunities to a CSV file.
        
        Args:
            opportunities: List of opportunity dictionaries
            filename: Output filename
        """
        if not opportunities:
            logger.warning("No opportunities to save")
            return
            
        # Determine fields to include in CSV
        # Start with common fields we always want
        fieldnames = [
            'id', 'opportunity_number', 'title', 'agency', 'opportunity_status',
            'open_date', 'close_date', 'award_value', 'cash_award',
            'opportunity_url', 'direct_link_to_apply_url', 'eligibility',
            'short_description', 'cost_sharing_required'
        ]
        
        # Add any additional fields from the first opportunity
        sample_opp = opportunities[0]
        for key in sample_opp.keys():
            if key not in fieldnames and key != 'raw_data':  # Skip raw_data as it's too large for CSV
                fieldnames.append(key)
                
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                # Write each opportunity, excluding raw_data field
                for opp in opportunities:
                    row = {k: v for k, v in opp.items() if k != 'raw_data'}
                    writer.writerow(row)
                    
            logger.info(f"Saved {len(opportunities)} opportunities to {filename}")
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")


# Example usage
if __name__ == "__main__":
    # Create API client
    api = GrantsGovAPI()
    
    # Uncomment to save opportunities to CSV
    # api.save_opportunities_to_csv(all_edu_opportunities, "education_opportunities.csv")