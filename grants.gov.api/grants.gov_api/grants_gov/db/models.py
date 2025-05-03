import logging
from sqlalchemy import Column, Integer, String, Date, Text, Boolean, Float, JSON
from sqlalchemy.ext.declarative import declarative_base

# Create base class for declarative models
Base = declarative_base()

logger = logging.getLogger(__name__)

class Opportunity(Base):
    """Grant opportunity data model based on the provided schema."""
    __tablename__ = 'opportunities'
    
    # Using id as primary key from the API
    id = Column(String, primary_key=True)
    
    # Basic information
    opportunity_number = Column(String)
    title = Column(String)
    opportunity_url = Column(String)
    direct_link_to_apply_url = Column(String)
    
    # Status and dates
    opportunity_status = Column(String)
    open_date = Column(Date)
    close_date = Column(Date)
    deadline = Column(Date)
    time_zone = Column(String)
    date_posted = Column(Date)
    
    # Financial information
    award_value = Column(Float)
    cash_award = Column(Float)
    award_floor = Column(Float, nullable=True)
    total_funding = Column(Float, nullable=True)
    cost = Column(Float, nullable=True)
    cost_sharing_required = Column(Boolean, default=False)
    
    # Agency information
    agency = Column(String)
    agency_code = Column(String)
    service_provider = Column(String)
    eso_website = Column(String, nullable=True)
    
    # Description and eligibility
    short_description = Column(Text)
    long_description = Column(Text, nullable=True)
    eligibility = Column(Text, nullable=True)
    
    # Categorization
    type = Column(String)
    category = Column(String)
    industry = Column(String, nullable=True)
    target_community = Column(String, nullable=True)
    opportunity_gap = Column(String, nullable=True)
    
    # Geographic information
    global_opportunity = Column(Boolean, default=False)
    global_locations = Column(String, nullable=True)
    countries_eligible = Column(String, nullable=True)
    location_details = Column(String, nullable=True)
    
    # Additional data
    opportunity_logo_url = Column(String, nullable=True)
    contact_email = Column(String, nullable=True)
    
    # Raw data for future reference
    raw_data = Column(JSON, nullable=True)

    # Category field for funding categories
    category = Column(String, nullable=True)
    
    def __repr__(self):
        return f"<Opportunity(id='{self.id}', title='{self.title}')>"
    
    @classmethod
    def from_api_response(cls, data):
        """Create an opportunity instance from API response data."""
        return cls(
            id=data.get('id'),
            number=data.get('number'),
            title=data.get('title'),
            agency_code=data.get('agencyCode'),
            agency=data.get('agency'),
            open_date=data.get('openDate'),  # You may need to parse date strings
            close_date=data.get('closeDate'),
            status=data.get('oppStatus'),
            doc_type=data.get('docType'),
            description=data.get('description', ''),
            last_updated=data.get('lastUpdatedDate')
        )