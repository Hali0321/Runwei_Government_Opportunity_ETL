import logging
import argparse
import json
import os
import sys
import importlib
from datetime import datetime
from typing import List, Dict, Any

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Force reload modules with import issues
if 'grants_gov.utils.data_generator' in sys.modules:
    importlib.reload(sys.modules['grants_gov.utils.data_generator'])

# Import components
from grants_gov.api.client import GrantsGovAPI
from grants_gov.etl.transform import GrantDataTransformer
from grants_gov.db.models import Base
from grants_gov.utils.config import get_connection_string
from grants_gov.db.postgresql import PostgreSQLClient

# Define a local version of the function as a fallback
def local_generate_sample_opportunities(count=100):
    """Fallback sample data generator if the import fails."""
    import random
    from datetime import datetime, timedelta
    
    logger.info(f"Using local fallback to generate {count} sample opportunities")
    
    # Simplified implementation
    agencies = [
        {"name": "U.S. National Science Foundation", "code": "NSF"},
        {"name": "Department of Education", "code": "ED"},
        {"name": "Department of Energy", "code": "DOE"},
        {"name": "Department of Health and Human Services", "code": "HHS-NIH"}
    ]
    
    samples = []
    base_id = 400000
    
    for i in range(count):
        agency = random.choice(agencies)
        today = datetime.now()
        
        opportunity = {
            "id": str(base_id + i),
            "number": f"{agency['code']}-{today.year}-{random.randint(1000, 9999)}",
            "title": f"Sample Opportunity {i+1}",
            "agency": agency["name"],
            "agencyCode": agency["code"],
            "description": f"This is a sample opportunity #{i+1}",
            "oppStatus": "posted",
            "openDate": (today - timedelta(days=random.randint(30, 365))).strftime("%Y-%m-%d"),
            "closeDate": (today + timedelta(days=random.randint(30, 730))).strftime("%Y-%m-%d"),
            "awardCeiling": random.randint(50, 5000) * 1000,
            "costSharing": random.choice([True, False, False, False])
        }
        
        samples.append(opportunity)
    
    return samples

# Configure logger early
logger = logging.getLogger(__name__)

# Try to import the generate_sample_opportunities function, but use local fallback if it fails
try:
    from grants_gov.utils.data_generator import generate_sample_opportunities
    logger.info("Successfully imported generate_sample_opportunities")
except ImportError:
    logger.warning("Could not import generate_sample_opportunities, using local fallback")
    generate_sample_opportunities = local_generate_sample_opportunities

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"grants_etl_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_database():
    """Set up the database and create tables if they don't exist."""
    from sqlalchemy import create_engine
    
    connection_string = get_connection_string()
    engine = create_engine(connection_string)
    
    # Try to create tables if they don't exist
    try:
        Base.metadata.create_all(engine)
        logger.info("Database tables created or verified")
    except Exception as e:
        logger.warning(f"Could not create tables using SQLAlchemy Base: {e}")
        logger.info("Tables may have been created manually already")
    
    return PostgreSQLClient(connection_string)

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
        },
        {
            "id": "351423",
            "number": "ED-GRANTS-041822-001",
            "title": "Office of Elementary and Secondary Education (OESE): Well-Rounded Education Through Student-Centered Funding",
            "agency": "Department of Education",
            "agencyCode": "ED",
            "description": "The Well-Rounded Education through Student-Centered Funding program provides competitive grants to local educational agencies (LEAs) to develop and implement student-centered funding systems.",
            "oppStatus": "posted",
            "openDate": "2022-04-18",
            "closeDate": "2025-07-30",
            "awardCeiling": 3000000,
            "costSharing": False,
            "fundingCategories": ["ED", "ISS"]
        },
        {
            "id": "346782",
            "number": "PA-21-145",
            "title": "Biomedical Technology Development and Dissemination Center (RM1)",
            "agency": "Department of Health and Human Services",
            "agencyCode": "HHS-NIH",
            "description": "This funding opportunity announcement (FOA) encourages grant applications for national Biomedical Technology Development and Dissemination Centers (BTDDCs). The BTDDCs are charged to develop and disseminate technologies that will have a significant impact on a broad community of biomedical researchers. New technologies are expected to transform research capabilities, enhancing the molecular-level understanding of complex biological processes.",
            "oppStatus": "posted",
            "openDate": "2021-02-25",
            "closeDate": "2024-09-07",
            "awardCeiling": 5000000,
            "costSharing": False,
            "fundingCategories": ["HL", "ST"]
        }
    ]
    
    # Import and use the generator for more data
    try:
        additional_data = generate_sample_opportunities(95)  # Generate 95 more for a total of 100
        sample_data.extend(additional_data)
        logger.info(f"Generated {len(additional_data)} additional sample opportunities")
    except Exception as e:
        logger.warning(f"Could not generate additional sample data: {e}")
    
    return sample_data

def extract_opportunities(api_client, keyword=None, agency=None, max_results=5000, use_api=True):
    """Extract opportunities from the API or use sample data."""
    if not use_api:
        logger.info("Using sample data instead of API")
        return get_sample_data()
        
    try:
        if agency:
            logger.info(f"Searching for {max_results} opportunities from agency {agency}")
            return api_client.search_all_opportunities(
                opp_statuses="posted",
                max_results=max_results,
                agencies=agency
            )
        elif keyword:
            logger.info(f"Searching for {max_results} opportunities with keyword {keyword}")
            return api_client.search_all_opportunities(
                keyword=keyword,
                opp_statuses="posted",
                max_results=max_results
            )
        else:
            logger.info(f"Searching for {max_results} recent opportunities")
            return api_client.search_all_opportunities(
                opp_statuses="posted",
                max_results=max_results
            )
    except Exception as e:
        logger.error(f"Error fetching opportunities: {e}")
        logger.info("Falling back to sample data")
        return get_sample_data()

def main():
    parser = argparse.ArgumentParser(description="ETL process for Grants.gov data")
    parser.add_argument("--keyword", help="Search keyword")
    parser.add_argument("--agency", help="Agency code")
    parser.add_argument("--max-results", type=int, help="Maximum results to retrieve", default=1000)
    parser.add_argument("--enrich", action="store_true", help="Fetch detailed data for each opportunity")
    parser.add_argument("--use-api", action="store_true", help="Try to use the API (defaults to sample data)")
    parser.add_argument("--full-load", action="store_true", help="Attempt to load all available opportunities")
    args = parser.parse_args()
    
    try:
        # Initialize components
        api_client = GrantsGovAPI()
        transformer = GrantDataTransformer()
        db_client = setup_database()
        
        # Extract opportunities
        if args.full_load:
            logger.info("Full load mode: Attempting to fetch all available opportunities")
            all_opportunities = []
            
            # Fetch opportunities in chunks to avoid timeout
            max_per_chunk = 500
            agencies = ["NSF", "ED", "DOE", "HHS-NIH", "DOD", "USDA", "DOC", "NASA"]
            
            # First try without agency filter
            logger.info("Fetching opportunities with no agency filter...")
            opps = extract_opportunities(api_client, max_results=max_per_chunk, use_api=True)
            all_opportunities.extend(opps)
            logger.info(f"Fetched {len(opps)} opportunities (no filter)")
            
            # Then try by agency for more complete coverage
            for agency in agencies:
                logger.info(f"Fetching opportunities for agency {agency}...")
                try:
                    agency_opps = extract_opportunities(api_client, agency=agency, max_results=max_per_chunk, use_api=True)
                    # Deduplicate based on ID
                    existing_ids = {opp["id"] for opp in all_opportunities}
                    new_opps = [opp for opp in agency_opps if opp["id"] not in existing_ids]
                    
                    all_opportunities.extend(new_opps)
                    logger.info(f"Fetched {len(new_opps)} new opportunities from {agency}")
                    
                except Exception as e:
                    logger.error(f"Error fetching opportunities for {agency}: {e}")
            
            opportunities = all_opportunities
        else:
            # Standard extraction as before
            opportunities = extract_opportunities(
                api_client, 
                args.keyword, 
                args.agency, 
                args.max_results, 
                use_api=args.use_api or args.full_load  # Use API if full load or use_api is specified
            )
        
        logger.info(f"Extracted {len(opportunities)} opportunities")
        
        # Transform
        transformed_opps = transformer.transform_opportunities(opportunities)
        
        # Enrich with details if requested
        if args.enrich:
            logger.info("Enriching opportunities with detailed information")
            # For sample data, simulate enrichment
            if not args.use_api:
                for opp in transformed_opps:
                    # Add category based on ID to simulate mapping
                    if opp['id'] == '351423':
                        opp['category'] = 'Education, Income Security and Social Services'
                    elif opp['id'] == '346782':
                        opp['category'] = 'Health, Science and Technology'
                    else:
                        opp['category'] = 'Science and Technology'
                    
                    # Ensure cost_sharing_required is set
                    opp['cost_sharing_required'] = opp['id'] == '344124'
                    
                logger.info("Used simulated enrichment for sample data")
            else:
                # Try real enrichment
                try:
                    transformed_opps = transformer.enrich_with_details(transformed_opps, api_client)
                except Exception as e:
                    logger.error(f"Enrichment failed: {e}")
                    logger.info("Using basic opportunity data")
        
        # Load
        if transformed_opps:
            inserted_count = db_client.insert_opportunities(transformed_opps)
            logger.info(f"Loaded {inserted_count} opportunities into the database")
            
            # Print what was loaded to verify
            print(f"\nSuccessfully loaded {inserted_count} opportunities into the database.")
            print("\nSample of opportunities loaded:")
            for i, opp in enumerate(transformed_opps[:3]):
                print(f"\n{i+1}. {opp.get('title')}")
                print(f"   Agency: {opp.get('agency')}")
                print(f"   Award: ${opp.get('award_value', 'N/A')}")
                print(f"   Category: {opp.get('category', 'Not categorized')}")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}", exc_info=True)
        print(f"Error: {e}")

if __name__ == "__main__":
    main()