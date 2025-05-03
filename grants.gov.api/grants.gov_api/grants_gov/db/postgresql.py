import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from typing import List, Dict, Any
import json
from datetime import datetime

from database.models import Base, Opportunity

logger = logging.getLogger(__name__)

class PostgreSQLClient:
    """Client for PostgreSQL database operations."""
    
    def __init__(self, connection_string: str):
        """
        Initialize PostgreSQL client.
        
        Args:
            connection_string: SQLAlchemy connection string
        """
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
    
    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            session.close()
    
    def insert_opportunities(self, opportunities: List[Dict[str, Any]]) -> int:
        """
        Insert opportunities into the database.
        
        Args:
            opportunities: List of transformed opportunity dictionaries
            
        Returns:
            Number of opportunities inserted/updated
        """
        count = 0
        with self.session_scope() as session:
            for opp_data in opportunities:
                try:
                    # Check if we already have this opportunity
                    existing = session.query(Opportunity).filter_by(id=opp_data["id"]).first()
                    
                    if existing:
                        # Update existing record
                        for key, value in opp_data.items():
                            if hasattr(existing, key):
                                setattr(existing, key, value)
                        logger.debug(f"Updated opportunity {opp_data['id']}")
                    else:
                        # Create new record
                        opportunity = Opportunity(**opp_data)
                        session.add(opportunity)
                        logger.debug(f"Added new opportunity {opp_data['id']}")
                    
                    count += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting opportunity {opp_data.get('id', 'unknown')}: {e}")
                    
        return count
    
    def get_opportunity_by_id(self, opportunity_id: str) -> Dict[str, Any]:
        """
        Retrieve an opportunity by ID.
        
        Args:
            opportunity_id: The opportunity ID to retrieve
            
        Returns:
            Opportunity data as dictionary
        """
        with self.session_scope() as session:
            opportunity = session.query(Opportunity).filter_by(id=opportunity_id).first()
            if opportunity:
                # Convert to dictionary - you might want a more sophisticated serializer
                return {c.name: getattr(opportunity, c.name) for c in opportunity.__table__.columns}
        return None