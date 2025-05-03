import random
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def generate_sample_opportunities(count=100):
    """Generate a larger set of sample opportunities for testing."""
    logger.info(f"Generating {count} sample opportunities")
    
    agencies = [
        {"name": "U.S. National Science Foundation", "code": "NSF"},
        {"name": "Department of Education", "code": "ED"},
        {"name": "Department of Energy", "code": "DOE"},
        {"name": "Department of Health and Human Services", "code": "HHS-NIH"},
        {"name": "National Aeronautics and Space Administration", "code": "NASA"}
    ]
    
    categories = [
        ["ED", "ST"],  # Education, Science and Technology
        ["HL", "ST"],  # Health, Science and Technology
        ["EN", "ENV"],  # Energy, Environment
        ["ST", "RD"],  # Science and Technology, Regional Development
        ["ED", "ISS"],  # Education, Income Security and Social Services
    ]
    
    titles = [
        "Research in {field} for {purpose}",
        "Advancing {field} through Innovative Research",
        "{field} Development Program",
        "Collaborative Research in {field}",
        "Building Capacity for {purpose}",
        "{field} Education and Training Initiative",
        "{field} Infrastructure Improvement Program"
    ]
    
    fields = [
        "Artificial Intelligence", "Quantum Computing", "Climate Science", 
        "Materials Science", "Renewable Energy", "STEM Education", "Public Health",
        "Precision Agriculture", "Sustainable Development", "Space Exploration"
    ]
    
    purposes = [
        "Societal Impact", "Educational Advancement", "Technological Innovation",
        "Scientific Discovery", "Community Development", "Economic Growth",
        "Environmental Sustainability", "Public Welfare", "National Security"
    ]
    
    # Generate sample opportunities
    samples = []
    base_id = 400000
    
    for i in range(count):
        # Select random values
        agency = random.choice(agencies)
        field = random.choice(fields)
        purpose = random.choice(purposes)
        
        # Generate random dates
        today = datetime.now()
        open_date = (today - timedelta(days=random.randint(30, 365))).strftime("%Y-%m-%d")
        close_date = (today + timedelta(days=random.randint(30, 730))).strftime("%Y-%m-%d")
        
        # Generate random award amount
        award = random.randint(50, 5000) * 1000
        
        # Generate title
        title_template = random.choice(titles)
        title = title_template.format(field=field, purpose=purpose)
        
        # Generate opportunity number
        opp_num = f"{agency['code']}-{today.year}-{random.randint(1000, 9999)}"
        
        # Create opportunity object
        opportunity = {
            "id": str(base_id + i),
            "number": opp_num,
            "title": title,
            "agency": agency["name"],
            "agencyCode": agency["code"],
            "description": f"This program supports research and development in {field} to advance {purpose.lower()}. Projects should demonstrate significant innovation and potential for broad impact.",
            "oppStatus": "posted",
            "openDate": open_date,
            "closeDate": close_date,
            "awardCeiling": award,
            "costSharing": random.choice([True, False, False, False]),  # 25% chance of cost sharing
            "fundingCategories": random.choice(categories)
        }
        
        samples.append(opportunity)
    
    logger.info(f"Generated {len(samples)} sample opportunities")
    return samples