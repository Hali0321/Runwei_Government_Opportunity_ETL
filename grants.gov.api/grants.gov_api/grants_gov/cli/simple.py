import logging
import argparse
import json
import time
import os
from grants_gov_api import GrantsGovAPI
from etl.transform import GrantDataTransformer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_sample_data():
    """Return sample grant opportunities for testing."""
    sample_data = [
        {
            "id": "350230",
            "number": "PD-22-1762",
            "title": "Research in the Formation of Engineers",
            "agency": "U.S. National Science Foundation",
            "agencyCode": "NSF",
            "description": "The NSF Engineering (ENG) Directorate has established the Research in the Formation of Engineers (RFE) program.  RFE supports research in science, technology, engineering, and mathematics (STEM) education with a particular interest in engineering education research.",
            "oppStatus": "posted",
            "openDate": "2022-01-13",
            "closeDate": "2025-05-15",
            "awardCeiling": 500000,
            "costSharing": False
        },
        {
            "id": "344124",
            "number": "PD-22-1998",
            "title": "Improving Undergraduate STEM Education: Directorate for STEM Education",
            "agency": "U.S. National Science Foundation",
            "agencyCode": "NSF",
            "description": "The Improving Undergraduate Science, Technology, Engineering, and Mathematics (STEM) Education (IUSE) program is a core NSF STEM education program that seeks to promote novel, creative, and transformative approaches to generating and using new knowledge about STEM teaching and learning to improve STEM education for undergraduate students.",
            "oppStatus": "posted",
            "openDate": "2022-02-03",
            "closeDate": "2025-12-10",
            "awardCeiling": 1000000,
            "costSharing": True
        },
        {
            "id": "358568",
            "number": "PD-23-245",
            "title": "Fire Science Innovations through Research and Education",
            "agency": "U.S. National Science Foundation",
            "agencyCode": "NSF",
            "description": "The Fire Science Innovations through Research and Education (FIRE) program is focused on understanding and mitigating the impacts of fires on society through fundamental research, education, and capacity building.",
            "oppStatus": "posted",
            "openDate": "2023-06-01",
            "closeDate": "2025-09-30",
            "awardCeiling": 750000,
            "costSharing": False
        }
    ]
    return sample_data

def main():
    """Simple script to test functionality without API dependencies."""
    parser = argparse.ArgumentParser(description="Test grants functionality with sample data")
    parser.add_argument("--use-api", action="store_true", help="Attempt to use the API instead of sample data")
    parser.add_argument("--keyword", help="Search keyword", default="education")
    parser.add_argument("--max-results", type=int, help="Maximum results to retrieve", default=10)
    parser.add_argument("--enrich", action="store_true", help="Simulate enrichment")
    parser.add_argument("--only-free", action="store_true", help="Only return free grants")
    args = parser.parse_args()
    
    try:
        # Initialize components
        api_client = GrantsGovAPI()
        transformer = GrantDataTransformer()
        
        # Get opportunities (either from API or sample data)
        if args.use_api:
            try:
                logger.info(f"Attempting to use API with keyword '{args.keyword}'")
                opportunities = api_client.search_all_opportunities(
                    keyword=args.keyword,
                    opp_statuses="posted",
                    max_results=args.max_results
                )
            except Exception as e:
                logger.warning(f"API access failed: {e}. Using sample data instead.")
                opportunities = get_sample_data()
        else:
            logger.info("Using sample data")
            opportunities = get_sample_data()
        
        logger.info(f"Retrieved {len(opportunities)} opportunities")
        
        # Transform opportunities to match our schema
        transformed_opps = transformer.transform_opportunities(opportunities)
        
        # Simulate enrichment if requested
        if args.enrich:
            logger.info("Simulating enrichment with opportunity details")
            # In real implementation, this would call the API for each opportunity
            for opp in transformed_opps:
                # Simulate enrichment data
                opp['award_value'] = opp.get('award_value', 500000)  # Default value for testing
                opp['cash_award'] = opp.get('award_value')
                opp['eligibility'] = "Various institutions may apply"
                opp['cost_sharing_required'] = opp.get('id') == '344124'  # Just for sample variety
        
        # Print some information about the opportunities
        print(f"\nProcessed {len(transformed_opps)} opportunities")
        for i, opp in enumerate(transformed_opps[:5], 1):
            print(f"\nOpportunity {i}:")
            print(f"ID: {opp.get('id')}")
            print(f"Title: {opp.get('title')}")
            print(f"Agency: {opp.get('agency')}")
            print(f"Status: {opp.get('opportunity_status')}")
            print(f"Award value: {opp.get('award_value')}")
            print(f"Cost sharing required: {opp.get('cost_sharing_required')}")
        
        # Filter for free active grants if requested
        if args.only_free:
            # Use local implementation since there might be issues with class method
            filtered_opps = [opp for opp in transformed_opps 
                           if opp.get('opportunity_status') == 'posted' and 
                              not opp.get('cost_sharing_required', False)]
            logger.info(f"Found {len(filtered_opps)} free active grants")
            output_opps = filtered_opps
        else:
            output_opps = transformed_opps
            
        # Save to CSV
        if output_opps:
            csv_filename = "free_active_grants.csv" if args.only_free else "grants.csv"
            print(f"\nSaving {len(output_opps)} opportunities to {csv_filename}")
            api_client.save_opportunities_to_csv(output_opps, csv_filename)
        
    except Exception as e:
        logger.error(f"Error in main process: {e}", exc_info=True)
        
if __name__ == "__main__":
    main()