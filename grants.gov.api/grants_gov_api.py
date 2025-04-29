import requests
import json
from typing import Dict, List, Optional, Union, Any


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
            "Content-Type": "application/json"
        }
        
        if api_key:
            self.auth_headers = {
                "Authorization": f"APIKEY={api_key}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
    
        # Add these methods to your GrantsGovAPI class:
    
    def search_by_cfda(self, cfda_number: str, **kwargs) -> Dict[str, Any]:
        """
        Search for grant opportunities by CFDA number.
        
        Args:
            cfda_number: The CFDA (Catalog of Federal Domestic Assistance) number
            **kwargs: Additional search parameters
            
        Returns:
            Dict containing search results
        """
        return self.search_opportunities(cfda=cfda_number, **kwargs)
    
    def search_by_agency(self, agency_code: str, **kwargs) -> Dict[str, Any]:
        """
        Search for grant opportunities by agency code.
        
        Args:
            agency_code: The agency code (e.g., "NSF", "HHS")
            **kwargs: Additional search parameters
            
        Returns:
            Dict containing search results
        """
        return self.search_opportunities(agencies=agency_code, **kwargs)
    
    def get_eligibility_types(self) -> List[Dict[str, str]]:
        """
        Get all eligibility types available in the Grants.gov system.
        This is a static/reference endpoint that returns metadata.
        
        Returns:
            List of dictionaries containing eligibility type codes and descriptions
        """
        endpoint = f"{self.base_url}/grantsws/rest/eligibilities"
        response = requests.get(endpoint, headers=self.headers)
        response.raise_for_status()
        
        return response.json()
    
    def save_opportunities_to_csv(self, opportunities: List[Dict[str, Any]], 
                                 filename: str = "opportunities.csv") -> None:
        """
        Save a list of opportunities to a CSV file.
        
        Args:
            opportunities: List of opportunity dictionaries
            filename: Output filename
        """
        import csv
        
        if not opportunities:
            print("No opportunities to save.")
            return
            
        # Get field names from the first opportunity (assuming all have similar structure)
        fieldnames = opportunities[0].keys()
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for opp in opportunities:
                writer.writerow(opp)
                
        print(f"Saved {len(opportunities)} opportunities to {filename}")

    def search_opportunities(self, 
                           keyword: Optional[str] = None,
                           rows: int = 10,
                           opp_statuses: Optional[str] = None,
                           **kwargs) -> Dict[str, Any]:
        """
        Search for grant opportunities based on specific criteria.
        
        Args:
            keyword: Search term
            rows: Number of results to return
            opp_statuses: Opportunity statuses (e.g., "posted|forecasted")
            **kwargs: Additional search parameters
            
        Returns:
            Dict containing search results
        """
        endpoint = f"{self.base_url}/v1/api/search2"
        
        # Build request payload
        payload = {}
        if keyword:
            payload["keyword"] = keyword
        if rows:
            payload["rows"] = rows
        if opp_statuses:
            payload["oppStatuses"] = opp_statuses
            
        # Add any additional parameters
        payload.update(kwargs)
        
        response = requests.post(endpoint, headers=self.headers, json=payload)
        response.raise_for_status()
        
        return response.json()
    
        # Add this method to your GrantsGovAPI class:
    
    def search_all_opportunities(self, 
                               keyword: Optional[str] = None,
                               opp_statuses: Optional[str] = None,
                               max_results: int = 100,
                               **kwargs) -> List[Dict[str, Any]]:
        """
        Search for grant opportunities and fetch multiple pages of results.
        
        Args:
            keyword: Search term
            opp_statuses: Opportunity statuses (e.g., "posted|forecasted")
            max_results: Maximum number of results to return across all pages
            **kwargs: Additional search parameters
            
        Returns:
            List of opportunities (dictionaries)
        """
        all_opportunities = []
        page_size = min(25, max_results)  # Use reasonable page size
        start_record = 0
        
        while len(all_opportunities) < max_results:
            # Update the start record for pagination
            kwargs['startRecordNum'] = start_record
            
            # Fetch a page of results
            results = self.search_opportunities(
                keyword=keyword,
                rows=page_size,
                opp_statuses=opp_statuses,
                **kwargs
            )
            
            # Extract opportunities from this page
            opportunities = results.get('data', {}).get('oppHits', [])
            if not opportunities:
                break  # No more results
                
            all_opportunities.extend(opportunities)
            
            # Check if we've reached the end of available results
            hit_count = results.get('data', {}).get('hitCount', 0)
            if start_record + page_size >= hit_count:
                break
                
            # Move to next page
            start_record += page_size
        
        # Trim to max_results if we got more
        return all_opportunities[:max_results]
    
    def fetch_opportunity(self, opportunity_id: str) -> Dict[str, Any]:
        """
        Retrieve detailed information about a specific opportunity.
        
        Args:
            opportunity_id: The ID of the opportunity to fetch
            
        Returns:
            Dict containing opportunity details
        """
        endpoint = f"{self.base_url}/v1/api/fetchOpportunity"
        
        payload = {
            "opportunityId": opportunity_id
        }
        
        response = requests.post(endpoint, headers=self.headers, json=payload)
        response.raise_for_status()
        
        return response.json()
    
    def get_opportunity_totals_by_cfda(self) -> Dict[str, Any]:
        """
        Retrieve counts of opportunities categorized by CFDA numbers and their statuses.
        
        Note: This endpoint requires an API key.
        
        Returns:
            Dict containing CFDA numbers and counts of opportunities by status
        """
        if not self.api_key:
            raise ValueError("API key is required for this endpoint")
            
        endpoint = f"{self.base_url}/grantsws/rest/opportunities/search/cfda/totals"
        
        response = requests.post(endpoint, headers=self.auth_headers)
        response.raise_for_status()
        
        return response.json()

    def __repr__(self) -> str:
        """Return string representation of the API client."""
        api_mode = "Staging" if "staging" in self.base_url else "Production"
        auth_status = "With API Key" if self.api_key else "Without API Key"
        return f"GrantsGovAPI({api_mode}, {auth_status})"


# ...existing code...

# Example usage
if __name__ == "__main__":
    # Create API client without API key for public endpoints
    api = GrantsGovAPI()
    
    # Search for opportunities related to education
    try:
        results = api.search_opportunities(keyword="education", rows=5, oppStatuses="posted")
        
        # Print the entire response for debugging
        print("API Response:")
        print(json.dumps(results, indent=2)[:1000] + "..." if len(json.dumps(results)) > 1000 else json.dumps(results, indent=2))
        
        # The opportunities are in the data.oppHits field, not directly in 'opportunities'
        opportunities = results.get('data', {}).get('oppHits', [])
        print(f"\nFound {len(opportunities)} education opportunities")
        
        # If opportunities exist, display the first one
        if opportunities:
            first_opp = opportunities[0]
            print("\nFirst opportunity details:")
            print(f"ID: {first_opp.get('id')}")
            print(f"Title: {first_opp.get('title')}")
            print(f"Agency: {first_opp.get('agency')}")
            print(f"Status: {first_opp.get('oppStatus')}")  # Note: it's 'oppStatus', not 'opportunityStatus'
            print(f"Number: {first_opp.get('number')}")
            print(f"Open Date: {first_opp.get('openDate')}")
            print(f"Close Date: {first_opp.get('closeDate') or 'Not specified'}")
        
        # Try a different search term
        print("\nTrying a different search term...")
        broader_results = api.search_opportunities(keyword="research", rows=5, oppStatuses="posted")
        broader_opportunities = broader_results.get('data', {}).get('oppHits', [])
        print(f"Found {len(broader_opportunities)} research opportunities")
        
        # Display the total hit count from the API
        hit_count = results.get('data', {}).get('hitCount', 0)
        print(f"\nTotal education opportunities available: {hit_count}")
        
        hit_count_research = broader_results.get('data', {}).get('hitCount', 0)
        print(f"Total research opportunities available: {hit_count_research}")
        
    except requests.exceptions.RequestException as e:
        print(f"API request error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    # Example usage in your __main__ block:
    all_edu_opportunities = api.search_all_opportunities(
        keyword="education", 
        opp_statuses="posted",
        max_results=50
    )
    print(f"\nFetched {len(all_edu_opportunities)} education opportunities in total")

    # Add to your __main__ block for testing the new methods
    print("\nTesting agency-specific search...")
    nsf_opportunities = api.search_by_agency("NSF", rows=5, oppStatuses="posted")
    nsf_hits = nsf_opportunities.get('data', {}).get('oppHits', [])
    print(f"Found {len(nsf_hits)} NSF opportunities")
    
    # Uncomment to save opportunities to CSV
    # api.save_opportunities_to_csv(all_edu_opportunities, "education_opportunities.csv")

    # Create API client with API key for authenticated endpoints
    # api_with_key = GrantsGovAPI(api_key="your-api-key")
    # cfda_totals = api_with_key.get_opportunity_totals_by_cfda()
    # print(f"Retrieved {len(cfda_totals)} CFDA entries")
